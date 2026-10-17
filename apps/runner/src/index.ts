import { mkdir, readFile, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import path from "node:path";
import { text } from "node:stream/consumers";

import { createORPCClient } from "@orpc/client";
import type { ContractRouterClient } from "@orpc/contract";
import {
  HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_PING,
  HEARTBEAT_PONG,
  HEARTBEAT_TIMEOUT_MS,
  contract,
  createProblemDetailsLink,
  createProtocolClient,
  hubNotifications,
  PROTOCOL_UNSUPPORTED,
  RUNNER_CONNECT_PATH,
  RUNNER_NOT_FOUND,
  RUNNER_PROTOCOL_VERSIONS,
  runnerMethods,
  type DesiredState,
  type ProtocolClient,
  type RegisteredRunner,
  registeredRunnerSchema,
  socketRefusalSchema,
  type SocketRefusal,
} from "contract";
import WebSocket from "ws";
import * as z from "zod/mini";

import { JobRunner } from "./jobs.ts";

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
/** How long the hub may take to answer a request before the Runner treats the connection as dead. */
const REQUEST_TIMEOUT_MS = 30_000;
/** How far either side of the sync interval a sync may fall, so Runners' syncs spread out. */
const SYNC_JITTER = 0.1;

export interface RunnerOptions {
  /** The hub's URL; the Runner opens its WebSocket there, over `wss:` for `https:`. */
  hubUrl: string | URL;
  /** The key Runners share to register; a Runner uses it once, to receive a key of its own. */
  registrationKey: string;
  /** Where the Runner keeps what must survive restarts, such as its own key. */
  dataDir: string;
  /** How many jobs the Runner runs at once. */
  maxJobs?: number;
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

/**
 * How many jobs a Runner runs at once unless `RUNNER_MAX_JOBS` says otherwise. A low placeholder:
 * an agent loop mostly waits on a model, so a machine holds many, and the Runner is to derive its
 * allowance from the host's memory and a measured loop's footprint, admitting less under memory
 * pressure, once a real loop runs.
 */
const DEFAULT_MAX_JOBS = 8;

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
      return z.parse(registeredRunnerSchema, JSON.parse(await readFile(file, "utf8")));
    } catch (error) {
      const absent = error instanceof Error && "code" in error && error.code === "ENOENT";
      if (!absent) throw error;
    }
  }
  const hub: ContractRouterClient<typeof contract> = createORPCClient(
    createProblemDetailsLink(contract, {
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
 * Keeps this Runner connected to the hub and in sync with it (ADR 0026): on each new socket it says
 * hello, then syncs, and from then on syncs at the interval the hub gave it, when the hub hints, and
 * when a job ends, reporting what became of its jobs. Every answer is the desired state of its jobs,
 * which the job runner is brought to. The heartbeat timer sends pings and drops a connection whose
 * pongs stopped; the socket is reopened with growing delays.
 *
 * The heartbeat is our own because nothing below provides one: on Workers, the hub's runtime answers
 * and records only a text message without waking the hub, and the protocol has no request the hub
 * makes of an idle Runner.
 */
class HubConnection implements Runner {
  readonly #hubUrl: URL;
  /** The Runner's own key, from which the hub takes its identity. */
  readonly #key: string;
  readonly #heartbeatIntervalMs: number;
  readonly #onRemoved: (() => void) | undefined;
  readonly #jobs: JobRunner;

  #heartbeats: ReturnType<typeof setInterval> | undefined;
  #socket: WebSocket | undefined;
  #hub: ProtocolClient<typeof runnerMethods> | undefined;
  #nextSync: ReturnType<typeof setTimeout> | undefined;
  #reconnect: ReturnType<typeof setTimeout> | undefined;
  #syncIntervalMs = 0;
  /** Whether a sync or report is in flight; another asked for meanwhile follows it. */
  #syncing = false;
  #syncAgain = false;
  #lastPongAt = 0;
  #stopped = false;
  /** Connection attempts since the last one that stayed open long enough; -1 before the first. */
  #retryCount = -1;

  constructor(
    hubUrl: URL,
    key: string,
    options: Pick<RunnerOptions, "heartbeatIntervalMs" | "onRemoved" | "maxJobs">,
  ) {
    this.#hubUrl = hubUrl;
    this.#key = key;
    this.#heartbeatIntervalMs = options.heartbeatIntervalMs ?? HEARTBEAT_INTERVAL_MS;
    this.#onRemoved = options.onRemoved;
    this.#jobs = new JobRunner(options.maxJobs ?? DEFAULT_MAX_JOBS, () => this.#requestSync());
  }

  start(): void {
    this.#heartbeats = setInterval(() => this.#sendHeartbeat(), this.#heartbeatIntervalMs);
    this.#connect();
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    clearInterval(this.#heartbeats);
    clearTimeout(this.#reconnect);
    const socket = this.#socket;
    this.#disconnect();
    await this.#jobs.stop();
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
    const socket = this.#socket;
    this.#disconnect();
    socket.terminate();
    this.#scheduleReconnect();
  }

  /** Drops the client and its socket, leaving the socket to the caller, and stops syncing on it. */
  #disconnect(): void {
    clearTimeout(this.#nextSync);
    this.#nextSync = undefined;
    this.#hub?.rejectAllPendingRequests("The connection closed");
    this.#hub = undefined;
    this.#socket = undefined;
    this.#syncing = false;
    this.#syncAgain = false;
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

  #scheduleReconnect(): void {
    if (this.#stopped || this.#reconnect) return;
    this.#reconnect = setTimeout(() => {
      this.#reconnect = undefined;
      this.#connect();
    }, this.#retryDelay());
  }

  /** Opens a socket to the hub, offering the protocol versions this Runner speaks. */
  #connect(): void {
    if (this.#stopped || this.#socket) return;
    this.#retryCount += 1;
    const url = new URL(RUNNER_CONNECT_PATH, this.#hubUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    // A socket that does not open in time closes, which ends the attempt.
    const socket = new WebSocket(url, [...RUNNER_PROTOCOL_VERSIONS], {
      headers: { "x-api-key": this.#key },
      handshakeTimeout: CONNECT_TIMEOUT_MS,
      closeTimeout: CLOSE_TIMEOUT_MS,
    });
    this.#socket = socket;
    // The hub answers 401 with `RUNNER_NOT_FOUND` to a key it no longer knows: the Runner was
    // removed, or the hub was reset. Retrying cannot succeed, so the Runner stops, as a GitHub Actions
    // runner does only on its server's `invalid_client`. A hub supporting none of the versions
    // offered says which it supports; either side upgrading resolves that, so the Runner says so and
    // retries. Other refusals, such as a proxy's or a hub's that cannot reach its database, end the
    // attempt, and the Runner tries again.
    socket.on("unexpected-response", (_request, response) => {
      // The body is read before the socket is dropped, which would discard it.
      void text(response)
        .catch(() => "")
        .then((body) => {
          socket.terminate();
          const refusal = parseRefusal(body);
          if (response.statusCode === 401 && refusal?.code === RUNNER_NOT_FOUND) {
            void this.stop().then(() => this.#onRemoved?.());
            return;
          }
          if (refusal?.code === PROTOCOL_UNSUPPORTED) {
            console.error(
              `The hub supports none of this Runner's protocol versions (${RUNNER_PROTOCOL_VERSIONS.join(", ")}); it supports ${refusal.supported?.join(", ") ?? "none it names"}. Upgrade whichever is older.`,
            );
          }
        });
    });
    let minUptimeTimer: ReturnType<typeof setTimeout> | undefined;
    socket.addEventListener("open", () => {
      this.#lastPongAt = Date.now();
      minUptimeTimer = setTimeout(() => (this.#retryCount = 0), MIN_UPTIME_MS);
      this.#hub = createProtocolClient(
        runnerMethods,
        hubNotifications,
        { syncNow: () => this.#requestSync() },
        (message) => socket.send(message),
      );
      void this.#hello();
    });
    socket.addEventListener("message", (event) => {
      if (event.data === HEARTBEAT_PONG) this.#lastPongAt = Date.now();
      else if (typeof event.data === "string") this.#hub?.receive(event.data);
    });
    socket.addEventListener("error", (event) => {
      if (!this.#stopped) console.error("The connection to the hub failed", event.message);
    });
    socket.addEventListener("close", () => {
      clearTimeout(minUptimeTimer);
      if (this.#socket === socket) {
        this.#disconnect();
        this.#scheduleReconnect();
      }
    });
  }

  /** Says hello on a new socket, learning the sync interval, then syncs. */
  async #hello(): Promise<void> {
    const hub = this.#hub;
    if (!hub) return;
    try {
      const { syncIntervalMs } = await hub.request(
        "hello",
        { name: hostname() },
        REQUEST_TIMEOUT_MS,
      );
      this.#syncIntervalMs = syncIntervalMs;
    } catch (error) {
      this.#fail("Hello to the hub failed", error);
      return;
    }
    this.#requestSync();
  }

  /** Syncs as soon as the sync or report in flight, if any, is answered. */
  #requestSync(): void {
    if (!this.#hub) return;
    if (this.#syncing) {
      this.#syncAgain = true;
      return;
    }
    void this.#sync();
  }

  /**
   * Reports what became of jobs, if anything did, or syncs, and brings the job runner to the desired
   * state the hub answers with; then schedules the next sync.
   */
  async #sync(): Promise<void> {
    const hub = this.#hub;
    if (!hub) return;
    this.#syncing = true;
    clearTimeout(this.#nextSync);
    const reports = this.#jobs.takeReports();
    const load = this.#jobs.load();
    try {
      const desired: DesiredState =
        reports.length > 0
          ? await hub.request("report", { ...load, jobs: reports }, REQUEST_TIMEOUT_MS)
          : await hub.request("sync", load, REQUEST_TIMEOUT_MS);
      this.#jobs.reconcile(desired);
    } catch (error) {
      // The reports go again with the next sync on the next socket.
      this.#jobs.returnReports(reports);
      this.#fail("Syncing with the hub failed", error);
      return;
    } finally {
      if (this.#hub === hub) this.#syncing = false;
    }
    if (this.#hub !== hub) return;
    if (this.#syncAgain) {
      this.#syncAgain = false;
      void this.#sync();
      return;
    }
    const jitter = 1 + (Math.random() * 2 - 1) * SYNC_JITTER;
    this.#nextSync = setTimeout(() => this.#requestSync(), this.#syncIntervalMs * jitter);
  }

  /** Drops a connection the hub stopped answering on, and reconnects. */
  #fail(what: string, error: unknown): void {
    if (this.#stopped) return;
    console.error(what, error instanceof Error ? error.message : error);
    const socket = this.#socket;
    this.#disconnect();
    socket?.terminate();
    this.#scheduleReconnect();
  }
}

/** The hub's refusal in `body`, if it is one the Runner knows how to read. */
function parseRefusal(body: string): SocketRefusal | undefined {
  try {
    const parsed = z.safeParse(socketRefusalSchema, JSON.parse(body));
    return parsed.success ? parsed.data : undefined;
  } catch {
    return undefined;
  }
}
