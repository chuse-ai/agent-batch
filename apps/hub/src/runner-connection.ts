import { DurableObject } from "cloudflare:workers";
import { HEARTBEAT_PING, HEARTBEAT_PONG, selectProtocolVersion } from "contract";
import { type Context, Effect } from "effect";
import * as z from "zod/mini";

import { Db, databaseLayer } from "./db/database.ts";
import { RunnerId } from "./db/types.ts";
import { handleRunnerMessage, SYNC_NOW } from "./runner-protocol.ts";
import { SYNC_INTERVAL_MS } from "./runners.ts";
import { hubSettings, readConfig } from "./settings.ts";

/** What the hub keeps with each socket, surviving hibernation. */
/**
 * What the hub keeps on each Runner's socket across hibernation: when the Runner was last recorded
 * as seen through it, 0 before the first time.
 */
const socketAttachmentSchema = z.object({ lastSeenWrittenAt: z.number() });
type SocketAttachment = z.infer<typeof socketAttachmentSchema>;

/**
 * Holds one Runner's WebSocket, hibernating while the Runner is idle. The runtime answers the
 * Runner's heartbeats without waking this object; a message from the Runner wakes it to answer
 * (ADR 0026), and a job for the Runner wakes it to send the hint to sync.
 *
 * This object only holds the connection: sockets and hibernation. What the hub records about
 * Runners lives in plain modules, so a hub outside Workers replaces only this class. Only Workers
 * bound to its namespace, which only the hub's configuration declares, can reach it: `fetch` accepts
 * the Runner's WebSocket, `requestSync` hints, and `dropConnections` closes it when the Runner is
 * removed.
 *
 * It must stay able to hibernate between messages. A Durable Object is billed for every second it
 * sits in memory unable to hibernate, and a Runner's socket stays open for days, so a pending
 * `setTimeout` or `setInterval`, unfinished I/O, or an outbound connection left open, such as a
 * database connection kept between messages, would bill it around the clock while nothing visibly
 * breaks. So sockets are accepted with `acceptWebSocket`, never `accept()`; each database connection
 * is closed when its work ends; and lint forbids timers in this module, which schedules nothing
 * itself: Runners' syncs bring the work (ADR 0026).
 */
export class RunnerConnection extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair(HEARTBEAT_PING, HEARTBEAT_PONG));
  }

  async fetch(request: Request): Promise<Response> {
    // The app refused any Runner offering no supported version, so one is selected here.
    const protocol = selectProtocolVersion(request.headers.get("sec-websocket-protocol"));
    const { 0: client, 1: server } = new WebSocketPair();
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ lastSeenWrittenAt: 0 } satisfies SocketAttachment);
    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: protocol === undefined ? {} : { "sec-websocket-protocol": protocol },
    });
  }

  /** Closes the Runner's sockets, for a Runner the hub has removed. */
  dropConnections(): void {
    for (const ws of this.ctx.getWebSockets()) ws.close(1008, "Runner removed");
  }

  /** Tells the Runner to sync now, on each of its open sockets. */
  requestSync(): void {
    for (const ws of this.ctx.getWebSockets()) {
      if (ws.readyState === WebSocket.OPEN) ws.send(SYNC_NOW);
    }
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    // The protocol is text; a binary message is not part of it.
    if (typeof message !== "string") return;
    const runnerId = this.#runnerId;
    const attachment = z.parse(socketAttachmentSchema, ws.deserializeAttachment());
    const now = Date.now();
    const recordSeen = now - attachment.lastSeenWrittenAt >= SYNC_INTERVAL_MS;
    const reply = await this.#withDatabase(
      Effect.contextWith((services: Context.Context<Db>) =>
        Effect.promise(() =>
          handleRunnerMessage(message, {
            runnerId,
            services,
            recordSeen,
            reportIntervalMs: readConfig(hubSettings).reportIntervalMs,
          }),
        ),
      ),
    );
    if (recordSeen) ws.serializeAttachment({ lastSeenWrittenAt: now } satisfies SocketAttachment);
    if (reply !== undefined) ws.send(reply);
  }

  webSocketClose(ws: WebSocket, code: number, reason: string): void {
    // Answers the Runner's close frame, as Cloudflare's example does: from compatibility date
    // 2026-04-07 the runtime should do so itself, but local workerd does not, and the Runner waits
    // for the answer before its socket counts as closed. Codes 1005, 1006 and 1015 only describe a
    // close locally (no status, a dropped connection, a failed TLS handshake) and cannot be sent, so
    // those are answered without a status; closing a socket that is already gone does nothing.
    if (code === 1005 || code === 1006 || code === 1015) ws.close();
    else ws.close(code, reason);
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
    return RunnerId(name);
  }
}
