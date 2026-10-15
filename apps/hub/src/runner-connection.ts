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

import type { Kysely } from "kysely";

import { DB_SCHEMA } from "./db/config.ts";
import type { Database } from "./db/entities.ts";
import { withEntityManager } from "./db/orm.ts";
import { recordRunnerSeen, recordRunnerSubscribed } from "./runners.ts";

interface ProtocolContext {
  runnerId: string;
  ws: WebSocket;
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
      await withDatabase((db) =>
        recordRunnerSubscribed(db, context.runnerId, input.name, new Date()),
      );
      return new HibernationAsyncIteratorClass<Command>((id) => {
        context.ws.serializeAttachment({ commandsId: id } satisfies SocketAttachment);
      });
    }),
  }),
  { plugins: [new HibernationHandlerPlugin()] },
);

function withDatabase<T>(work: (db: Kysely<Database>) => Promise<T>): Promise<T> {
  return withEntityManager((em) => work(em.getKysely<Database>().withSchema(DB_SCHEMA)));
}

/**
 * Holds one Runner's WebSocket, hibernating while the Runner is idle. The runtime answers the
 * Runner's heartbeats and records when they arrive without waking this object; its alarm wakes it
 * only to record the latest heartbeat as when the Runner was last seen, and to drop the Runner's
 * sockets once heartbeats stop.
 *
 * This object only holds the connection: sockets, the alarm and hibernation. What the hub records
 * about Runners lives in plain modules, so a hub outside Workers replaces only this class. It has no
 * public methods of its own, so other Workers cannot call into it over RPC.
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

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await protocol.message(ws, message, { context: { runnerId: this.#runnerId, ws } });
  }

  async webSocketClose(ws: WebSocket): Promise<void> {
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
      await withDatabase((db) => recordRunnerSeen(db, this.#runnerId, new Date(lastHeartbeat)));
    } catch (error) {
      console.error("Recording the Runner's last heartbeat failed", error);
    }
  }

  get #runnerId(): string {
    const name = this.ctx.id.name;
    if (name === undefined) throw new Error("RunnerConnection must be addressed by Runner ID");
    return name;
  }
}
