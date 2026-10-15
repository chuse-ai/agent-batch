import { HibernationAsyncIteratorClass, HibernationHandlerPlugin } from "@orpc/hibernation";
import { implement } from "@orpc/server";
import { RPCHandler } from "@orpc/server/websocket";
import { DurableObject } from "cloudflare:workers";
import {
  HEARTBEAT_PING,
  HEARTBEAT_PONG,
  HEARTBEAT_TIMEOUT_MS,
  runnerProtocol,
  type Command,
} from "contract";

import { withDatabase } from "./db/database.ts";
import type { RunnerId } from "./db/types.ts";
import { recordRunnerSeen, recordRunnerSubscribed } from "./runners.ts";

interface ProtocolContext {
  runnerId: RunnerId;
  ws: WebSocket;
  connectionString: string;
}

/** What the hub keeps with each socket, surviving hibernation. */
interface SocketAttachment {
  /** The commands subscription to send this Runner's commands to. */
  commandsId: string;
}

/** Variables only tests set, to shorten the protocol's timings; the deployed config declares none. */
type TestEnv = Env & { HEARTBEAT_TIMEOUT_MS?: string };

const os = implement<typeof runnerProtocol, ProtocolContext>(runnerProtocol);

const protocol = new RPCHandler(
  os.router({
    commands: os.commands.handler(async ({ input, context }) => {
      await withDatabase(context.connectionString, (db) =>
        recordRunnerSubscribed(db, context.runnerId, input.name, new Date()),
      );
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
 */
export class RunnerConnection extends DurableObject<Env> {
  /** How long the Runner may go without a heartbeat; tests set `HEARTBEAT_TIMEOUT_MS` shorter. */
  readonly #heartbeatTimeoutMs: number;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#heartbeatTimeoutMs =
      Number((env as TestEnv).HEARTBEAT_TIMEOUT_MS) || HEARTBEAT_TIMEOUT_MS;
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
    await protocol.message(ws, message, {
      context: {
        runnerId: this.#runnerId,
        ws,
        connectionString: this.env.HYPERDRIVE.connectionString,
      },
    });
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
    const deadline = lastHeartbeat + this.#heartbeatTimeoutMs;
    if (deadline > Date.now()) await this.ctx.storage.setAlarm(deadline);
    else for (const ws of sockets) ws.close(1001, "No heartbeat");
    if (lastHeartbeat === 0) return;
    // Recorded after the next alarm is set: a failed write is made good by a later one, while an
    // alarm that throws is retried only a few times.
    try {
      await withDatabase(this.env.HYPERDRIVE.connectionString, (db) =>
        recordRunnerSeen(db, this.#runnerId, new Date(lastHeartbeat)),
      );
    } catch (error) {
      console.error("Recording the Runner's last heartbeat failed", error);
    }
  }

  get #runnerId(): RunnerId {
    const name = this.ctx.id.name;
    if (name === undefined) throw new Error("RunnerConnection must be addressed by Runner ID");
    return name as RunnerId;
  }
}
