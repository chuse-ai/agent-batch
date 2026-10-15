import { createORPCClient } from "@orpc/client";
import { RetryLinkPlugin, type RetryLinkPluginContext } from "@orpc/client/plugins";
import { RPCLink } from "@orpc/client/websocket";
import type { ContractRouterClient } from "@orpc/contract";
import { DurableObject } from "cloudflare:workers";
import {
  HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_PING,
  HEARTBEAT_PONG,
  HEARTBEAT_TIMEOUT_MS,
  type runnerProtocol,
} from "contract";

/** Variables only tests set, to shorten the protocol's timings; the deployed config declares none. */
type TestEnv = Env & { HEARTBEAT_INTERVAL_MS?: string };

type Hub = ContractRouterClient<typeof runnerProtocol, RetryLinkPluginContext>;

// Reconnect timing follows partysocket's defaults: the first retry after 3 s, each further attempt
// waiting 1.3 times longer up to 10 s. There is no limit on opening a socket: a service binding
// call reaches the hub inside Cloudflare and fails rather than hangs. A Runner that dials the hub by
// URL needs one, bounding only the opening, as partysocket's connection timeout does (ADR 0008).
const RECONNECT_DELAY_MS = 3_000;
const RECONNECT_DELAY_GROWTH = 1.3;
const MAX_RECONNECT_DELAY_MS = 10_000;

/**
 * Keeps this Runner subscribed to the hub's commands, which also registers it. oRPC reopens the socket
 * and subscribes again whenever the stream ends; the alarm sends heartbeats and restarts the
 * subscription if the object restarted or the hub stopped answering.
 *
 * The heartbeat is our own because nothing below provides one: Workers WebSockets cannot send
 * protocol pings, and neither oRPC nor partysocket sends heartbeats or notices a silent peer.
 */
export class HubConnection extends DurableObject<Env> {
  /** How often to send a heartbeat; tests set `HEARTBEAT_INTERVAL_MS` shorter. */
  readonly #heartbeatIntervalMs =
    Number((this.env as TestEnv).HEARTBEAT_INTERVAL_MS) || HEARTBEAT_INTERVAL_MS;

  #hub: Hub | undefined;
  #socket: WebSocket | undefined;
  #subscription: AbortController | undefined;
  #lastPongAt = 0;

  async start(): Promise<void> {
    await this.ctx.storage.put("started", true);
    await this.ctx.storage.setAlarm(Date.now() + this.#heartbeatIntervalMs);
    this.#subscribe();
  }

  async stop(): Promise<void> {
    await this.ctx.storage.delete("started");
    await this.ctx.storage.deleteAlarm();
    this.#disconnect("Runner stopped");
  }

  async alarm(): Promise<void> {
    if (!(await this.ctx.storage.get("started"))) return;
    // Scheduled first: an alarm that throws is retried only a few times, then never runs again.
    await this.ctx.storage.setAlarm(Date.now() + this.#heartbeatIntervalMs);
    try {
      this.#sendHeartbeat();
    } catch (error) {
      console.error("Heartbeat to the hub failed", error);
    }
    this.#subscribe();
  }

  /** Sends a heartbeat, or drops the connection if the hub stopped answering them. */
  #sendHeartbeat(): void {
    if (this.#socket?.readyState !== WebSocket.OPEN) return;
    if (Date.now() - this.#lastPongAt <= HEARTBEAT_TIMEOUT_MS) {
      this.#socket.send(HEARTBEAT_PING);
      return;
    }
    console.error("The hub stopped answering heartbeats; reconnecting");
    // Closing the socket alone is not enough: its close event waits for the hub, which may never
    // answer, so oRPC would not notice. Dropping the client lets the next subscription start afresh.
    this.#disconnect("No heartbeat answer");
  }

  /** Ends the subscription and drops the client and its socket. */
  #disconnect(reason: string): void {
    this.#subscription?.abort();
    this.#subscription = undefined;
    // Dropping the client means nothing calls through it again, so it never reconnects.
    this.#hub = undefined;
    this.#socket?.close(1000, reason);
    this.#socket = undefined;
  }

  #client(): Hub {
    if (this.#hub) return this.#hub;
    const hub: Hub = createORPCClient(
      new RPCLink({
        connect: () => this.#openSocket(hub),
        reconnect: {
          enabled: true,
          // The first attempt goes at once: the subscription's retry has already waited. The count
          // restarts after each successful connect.
          delay: ({ attempt }) =>
            attempt === 1
              ? 0
              : Math.min(
                  RECONNECT_DELAY_MS * RECONNECT_DELAY_GROWTH ** (attempt - 1),
                  MAX_RECONNECT_DELAY_MS,
                ),
        },
        plugins: [new RetryLinkPlugin({ default: { retryDelay: RECONNECT_DELAY_MS } })],
      }),
    );
    this.#hub = hub;
    return hub;
  }

  /** Subscribes to the hub's commands unless already subscribed, retrying until stopped. */
  #subscribe(): void {
    if (this.#subscription) return;
    const subscription = new AbortController();
    this.#subscription = subscription;
    const consume = async () => {
      const commands = await this.#client().commands(
        { name: this.env.RUNNER_NAME },
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

  /** Opens a socket for `client`, or closes it if the client was dropped while it opened. */
  async #openSocket(client: Hub): Promise<WebSocket> {
    const path = `/runners/${encodeURIComponent(this.env.RUNNER_ID)}/connect`;
    const response = await this.env.HUB.fetch(`https://hub${path}`, {
      headers: { Upgrade: "websocket" },
    });
    const socket = response.webSocket;
    if (!socket) throw new Error(`Hub refused the WebSocket: ${response.status}`);
    socket.accept();
    if (this.#hub !== client) {
      socket.close(1000, "Runner stopped");
      throw new Error("The connection was dropped while the socket opened");
    }
    socket.addEventListener("message", (event) => {
      if (event.data === HEARTBEAT_PONG) this.#lastPongAt = Date.now();
    });
    this.#lastPongAt = Date.now();
    this.#socket = socket;
    return socket;
  }
}
