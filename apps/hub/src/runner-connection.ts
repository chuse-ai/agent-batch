import type { WithEffectContext } from "@orpc/experimental-effect";
import "@orpc/experimental-effect/extensions/effect";
import { HibernationAsyncIteratorClass, HibernationHandlerPlugin } from "@orpc/hibernation";
import { implement } from "@orpc/server";
import { RPCHandler } from "@orpc/server/websocket";
import { DurableObject } from "cloudflare:workers";
import { HEARTBEAT_PING, HEARTBEAT_PONG, runnerProtocol, type Command } from "contract";
import { type Context, Effect } from "effect";

import { Db, databaseLayer } from "./db/database.ts";
import type { RunnerId } from "./db/types.ts";
import {
  nextHeartbeatCheck,
  recordRunnerSeen,
  recordRunnerSubscribed,
  SILENT_RUNNER_CLOSE,
} from "./runners.ts";
import { heartbeatTimeoutMs, readConfig } from "./settings.ts";

interface ProtocolContext extends WithEffectContext<Db> {
  runnerId: RunnerId;
  ws: WebSocket;
}

/** What the hub keeps with each socket, surviving hibernation. */
interface SocketAttachment {
  /** The commands subscription to send this Runner's commands to. */
  commandsId: string;
}

const os = implement<typeof runnerProtocol, ProtocolContext>(runnerProtocol);

const protocol = new RPCHandler(
  os.router({
    commands: os.commands.effect(function* ({ input, context }) {
      yield* recordRunnerSubscribed(context.runnerId, input.name, new Date());
      return new HibernationAsyncIteratorClass<Command>((id) => {
        context.ws.serializeAttachment({ commandsId: id } satisfies SocketAttachment);
      });
    }),
  }),
  { plugins: [new HibernationHandlerPlugin()] },
);

/**
 * Holds one Runner's WebSocket, hibernating while the Runner is idle. The runtime answers the
 * Runner's heartbeats and records when they arrive without waking this object; its alarm wakes it
 * only to record the latest heartbeat as when the Runner was last seen, and to drop the Runner's
 * sockets once heartbeats stop.
 *
 * This object only holds the connection: sockets, the alarm and hibernation. What the hub records
 * about Runners lives in plain modules, so a hub outside Workers replaces only this class. Only
 * Workers bound to its namespace, which only the hub's configuration declares, can reach it: `fetch`
 * accepts the Runner's WebSocket, and `dropConnections` closes it when the Runner is removed.
 *
 * It must stay able to hibernate between messages. A Durable Object is billed for every second it
 * sits in memory unable to hibernate, and a Runner's socket stays open for days, so a pending
 * `setTimeout` or `setInterval`, unfinished I/O, or an outbound connection left open, such as a
 * database connection kept between messages, would bill it around the clock while nothing visibly
 * breaks. So sockets are accepted with `acceptWebSocket`, never `accept()`; each database connection
 * is closed when its work ends; and lint forbids timers in this module, where the storage alarm
 * schedules work instead.
 */
export class RunnerConnection extends DurableObject<Env> {
  /** How long the Runner may go without a heartbeat; tests set `HEARTBEAT_TIMEOUT_MS` shorter. */
  readonly #heartbeatTimeoutMs: number;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#heartbeatTimeoutMs = readConfig(heartbeatTimeoutMs, env);
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair(HEARTBEAT_PING, HEARTBEAT_PONG));
  }

  async fetch(): Promise<Response> {
    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);
    // The alarm drops the Runner's sockets if no heartbeat arrives before the timeout.
    await this.ctx.storage.setAlarm(Date.now() + this.#heartbeatTimeoutMs);
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Closes the Runner's sockets, for a Runner the hub has removed. */
  async dropConnections(): Promise<void> {
    for (const ws of this.ctx.getWebSockets()) ws.close(1008, "Runner removed");
    await this.ctx.storage.deleteAlarm();
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const runnerId = this.#runnerId;
    await this.#withDatabase(
      Effect.contextWith((services: Context.Context<Db>) =>
        Effect.promise(() =>
          protocol.message(ws, message, { context: { "effect/context": services, runnerId, ws } }),
        ),
      ),
    );
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    // Answers the Runner's close frame, as Cloudflare's example does: from compatibility date
    // 2026-04-07 the runtime should do so itself, but local workerd does not, and the Runner waits
    // for the answer before its socket counts as closed. Codes 1005, 1006 and 1015 only describe a
    // close locally (no status, a dropped connection, a failed TLS handshake) and cannot be sent, so
    // those are answered without a status; closing a socket that is already gone does nothing.
    if (code === 1005 || code === 1006 || code === 1015) ws.close();
    else ws.close(code, reason);
    await protocol.close(ws);
    // The alarm watches heartbeats on every socket, so it stays while another socket is open.
    const stillOpen = this.ctx
      .getWebSockets()
      .some((other) => other !== ws && other.readyState === WebSocket.OPEN);
    if (!stillOpen) await this.ctx.storage.deleteAlarm();
  }

  async alarm(): Promise<void> {
    const sockets = this.ctx.getWebSockets();
    const lastHeartbeat = Math.max(
      0,
      ...sockets.map((ws) => this.ctx.getWebSocketAutoResponseTimestamp(ws)?.getTime() ?? 0),
    );
    const next = nextHeartbeatCheck(lastHeartbeat, this.#heartbeatTimeoutMs, Date.now());
    if (next !== undefined) await this.ctx.storage.setAlarm(next);
    else for (const ws of sockets) ws.close(SILENT_RUNNER_CLOSE.code, SILENT_RUNNER_CLOSE.reason);
    if (lastHeartbeat === 0) return;
    // Recorded after the next alarm is set: a failed write is made good by a later one, while an
    // alarm that throws is retried only a few times.
    try {
      await this.#withDatabase(recordRunnerSeen(this.#runnerId, new Date(lastHeartbeat)));
    } catch (error) {
      console.error("Recording the Runner's last heartbeat failed", error);
    }
  }

  /**
   * Runs `effect` with a database connection of its own, closed when it ends: Workers forbid using a
   * connection in any request but the one that opened it, and one left open would keep this object
   * from hibernating.
   */
  #withDatabase<A, E>(effect: Effect.Effect<A, E, Db>): Promise<A> {
    return Effect.runPromise(
      Effect.provide(effect, databaseLayer(this.env.HYPERDRIVE.connectionString)),
    );
  }

  get #runnerId(): RunnerId {
    const name = this.ctx.id.name;
    if (name === undefined) throw new Error("RunnerConnection must be addressed by Runner ID");
    return name as RunnerId;
  }
}
