import { mkdir, readFile, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import path from "node:path";
import { text } from "node:stream/consumers";

import { createORPCClient } from "@orpc/client";
import { RetryLinkPlugin, type RetryLinkPluginContext } from "@orpc/client/plugins";
import { RPCLink } from "@orpc/client/websocket";
import type { ContractRouterClient } from "@orpc/contract";
import type { JsonifiedClient } from "@orpc/openapi";
import { OpenAPILink } from "@orpc/openapi/fetch";
import {
  HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_PING,
  HEARTBEAT_PONG,
  HEARTBEAT_TIMEOUT_MS,
  contract,
  RUNNER_CONNECT_PATH,
  RUNNER_NOT_FOUND,
  type RegisteredRunner,
  type runnerProtocol,
} from "contract";
import WebSocket from "ws";

type Hub = ContractRouterClient<typeof runnerProtocol, RetryLinkPluginContext>;

// TODO: remove once `@types/ws` declares `closeTimeout`, added in `ws` 8.19.
declare module "ws" {
  interface ClientOptions {
    closeTimeout?: number;
  }
}

// Connection timings follow partysocket's defaults (ADR 0008). Opening a socket may take 4 s. The
// first attempt goes at once and each retry waits 3 s, growing 1.3 times per attempt up to 10 s; only
// a connection that stays open for 5 s starts the delays over, so a hub that accepts and then drops
// Runners is not redialled at a fixed pace.
const CONNECT_TIMEOUT_MS = 4_000;
/** How long a closing socket waits for the hub's answer before dropping the connection itself. */
const CLOSE_TIMEOUT_MS = 4_000;
const RECONNECT_DELAY_MS = 3_000;
const RECONNECT_DELAY_GROWTH = 1.3;
const MAX_RECONNECT_DELAY_MS = 10_000;
const MIN_UPTIME_MS = 5_000;

export interface RunnerOptions {
  /** The hub's URL; the Runner opens its WebSocket there, over `wss:` for `https:`. */
  hubUrl: string | URL;
  /** The key Runners share to register; a Runner uses it once, to receive a key of its own. */
  registrationKey: string;
  /** Where the Runner keeps what must survive restarts, such as its own key. */
  dataDir: string;
  /** How often to send a heartbeat; tests set it shorter. */
  heartbeatIntervalMs?: number;
  /** Discards the Runner's own key and registers again, as the `register` command does. */
  register?: boolean;
  /**
   * Called when the hub answers that it does not know the Runner's own key, because the Runner was
   * removed or the hub was reset; the Runner has stopped by then, since retrying cannot succeed.
   */
  onRemoved?: () => void;
}

export interface Runner {
  /** Closes the connection to the hub, so the hub drops it at once, and stops reconnecting. */
  stop(): Promise<void>;
}

/** Starts a Runner that stays connected to the hub until stopped, registering it on its first start. */
export async function startRunner(options: RunnerOptions): Promise<Runner> {
  const hubUrl = new URL(options.hubUrl);
  const registration = await loadOrRegister(hubUrl, options);
  const connection = new HubConnection(hubUrl, registration.key, options);
  connection.start();
  return connection;
}

/**
 * The Runner's own key, kept in its data directory, or, on its first start, a new one from
 * registering with the shared key, as GitHub Actions and Buildkite runners do.
 */
async function loadOrRegister(hubUrl: URL, options: RunnerOptions): Promise<RegisteredRunner> {
  const file = path.join(options.dataDir, "runner.json");
  if (!options.register) {
    try {
      return JSON.parse(await readFile(file, "utf8")) as RegisteredRunner;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  const hub: JsonifiedClient<ContractRouterClient<typeof contract>> = createORPCClient(
    new OpenAPILink(contract, {
      origin: hubUrl.origin,
      headers: { "x-api-key": options.registrationKey },
    }),
  );
  const registration = await hub.runners.register({ name: hostname() });
  await mkdir(options.dataDir, { recursive: true });
  // Only the operating-system user running the Runner may read its key.
  await writeFile(file, JSON.stringify(registration), { mode: 0o600 });
  return registration;
}

/**
 * Keeps this Runner subscribed to the hub's commands, which also registers it, and sends heartbeats.
 * oRPC reopens the socket and subscribes again whenever the stream ends; the heartbeat timer also
 * restarts the subscription if it ended, and drops the connection if the hub stopped answering.
 *
 * The heartbeat is our own because nothing below provides one: the hub's runtime answers and records
 * only a text message without waking the hub, and oRPC neither sends heartbeats nor notices a silent
 * peer.
 */
class HubConnection implements Runner {
  readonly #hubUrl: URL;
  /** The Runner's own key, from which the hub takes its identity. */
  readonly #key: string;
  readonly #heartbeatIntervalMs: number;
  readonly #onRemoved: (() => void) | undefined;

  #heartbeats: ReturnType<typeof setInterval> | undefined;
  #hub: Hub | undefined;
  #socket: WebSocket | undefined;
  #subscription: AbortController | undefined;
  #lastPongAt = 0;
  /** Connection attempts since the last one that stayed open long enough; -1 before the first. */
  #retryCount = -1;

  constructor(
    hubUrl: URL,
    key: string,
    options: Pick<RunnerOptions, "heartbeatIntervalMs" | "onRemoved">,
  ) {
    this.#hubUrl = hubUrl;
    this.#key = key;
    this.#heartbeatIntervalMs = options.heartbeatIntervalMs ?? HEARTBEAT_INTERVAL_MS;
    this.#onRemoved = options.onRemoved;
  }

  start(): void {
    this.#heartbeats = setInterval(() => {
      this.#sendHeartbeat();
      this.#subscribe();
    }, this.#heartbeatIntervalMs);
    this.#subscribe();
  }

  async stop(): Promise<void> {
    clearInterval(this.#heartbeats);
    const socket = this.#socket;
    this.#disconnect();
    // Closing waits for the hub's answer for at most `CLOSE_TIMEOUT_MS`, then drops the connection.
    socket?.close(1000, "Runner stopped");
    if (socket && socket.readyState !== WebSocket.CLOSED) {
      await new Promise((resolve) => socket.addEventListener("close", resolve, { once: true }));
    }
  }

  /** Sends a heartbeat, or drops the connection if the hub stopped answering them. */
  #sendHeartbeat(): void {
    if (this.#socket?.readyState !== WebSocket.OPEN) return;
    if (Date.now() - this.#lastPongAt <= HEARTBEAT_TIMEOUT_MS) {
      this.#socket.send(HEARTBEAT_PING);
      return;
    }
    console.error("The hub stopped answering heartbeats; reconnecting");
    // The hub is not answering, so the connection is dropped at once rather than closed politely.
    // Dropping the client too lets the next subscription start afresh.
    const socket = this.#socket;
    this.#disconnect();
    socket.terminate();
  }

  /** Ends the subscription and drops the client and its socket, leaving the socket to the caller. */
  #disconnect(): void {
    this.#subscription?.abort();
    this.#subscription = undefined;
    // Dropping the client means nothing calls through it again, so it never reconnects.
    this.#hub = undefined;
    this.#socket = undefined;
  }

  #client(): Hub {
    if (this.#hub) return this.#hub;
    const hub: Hub = createORPCClient(
      new RPCLink({
        connect: () => this.#openSocket(hub),
        // One attempt per call, although oRPC's docs suggest more than 1: its transport keeps
        // retrying until it connects, even for a call that was given up, so a client dropped after a
        // heartbeat timeout would keep dialling in the background. With one attempt, the retry plugin
        // does all the waiting, and a given-up call stops connecting.
        reconnect: { enabled: true, maxAttempt: 1 },
        plugins: [new RetryLinkPlugin({ default: { retryDelay: () => this.#retryDelay() } })],
      }),
    );
    this.#hub = hub;
    return hub;
  }

  /** How long to wait before the next connection attempt, as partysocket computes it. */
  #retryDelay(): number {
    const attempt = this.#retryCount + 1;
    if (attempt <= 0) return 0;
    return Math.min(
      RECONNECT_DELAY_MS * RECONNECT_DELAY_GROWTH ** (attempt - 1),
      MAX_RECONNECT_DELAY_MS,
    );
  }

  /** Subscribes to the hub's commands unless already subscribed, retrying until stopped. */
  #subscribe(): void {
    if (this.#subscription) return;
    const subscription = new AbortController();
    this.#subscription = subscription;
    const consume = async () => {
      const commands = await this.#client().commands(
        { name: hostname() },
        { signal: subscription.signal, context: { retry: Number.POSITIVE_INFINITY } },
      );
      // No commands exist yet; jobs will be the first.
      for await (const _command of commands);
    };
    consume()
      .catch((error: unknown) => {
        if (!subscription.signal.aborted) console.error("Commands subscription ended", error);
      })
      .finally(() => {
        if (this.#subscription === subscription) this.#subscription = undefined;
      });
  }

  /** Opens a socket to the hub for `client`, unless the client was dropped. */
  #openSocket(client: Hub): WebSocket {
    if (this.#hub !== client) throw new Error("The connection was dropped");
    this.#retryCount += 1;
    const url = new URL(RUNNER_CONNECT_PATH, this.#hubUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    // A socket that does not open in time closes, which ends the attempt, and oRPC tries again.
    const socket = new WebSocket(url, {
      headers: { "x-api-key": this.#key },
      handshakeTimeout: CONNECT_TIMEOUT_MS,
      closeTimeout: CLOSE_TIMEOUT_MS,
    });
    // The hub answers 401 with `RUNNER_NOT_FOUND` to a key it no longer knows: the Runner was
    // removed, or the hub was reset. Retrying cannot succeed, so the Runner stops, as a GitHub Actions
    // runner does only on its server's `invalid_client`. Other refusals, such as a proxy's or a hub's
    // that cannot reach its database, end the attempt, and oRPC tries again.
    socket.on("unexpected-response", (_request, response) => {
      // The body is read before the socket is dropped, which would discard it.
      void text(response)
        .catch(() => "")
        .then((body) => {
          socket.terminate();
          if (response.statusCode === 401 && isRunnerNotFound(body)) {
            void this.stop().then(() => this.#onRemoved?.());
          }
        });
    });
    let minUptimeTimer: ReturnType<typeof setTimeout> | undefined;
    socket.addEventListener("open", () => {
      this.#lastPongAt = Date.now();
      minUptimeTimer = setTimeout(() => (this.#retryCount = 0), MIN_UPTIME_MS);
    });
    socket.addEventListener("close", () => clearTimeout(minUptimeTimer));
    socket.addEventListener("message", (event) => {
      if (event.data === HEARTBEAT_PONG) this.#lastPongAt = Date.now();
    });
    this.#socket = socket;
    return socket;
  }
}

/** Whether a refusal's body is the hub's answer that it does not know the Runner. */
function isRunnerNotFound(body: string): boolean {
  try {
    return (JSON.parse(body) as { code?: unknown }).code === RUNNER_NOT_FOUND;
  } catch {
    return false;
  }
}
