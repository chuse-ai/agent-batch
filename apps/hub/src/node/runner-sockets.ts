import { HEARTBEAT_PING, HEARTBEAT_PONG } from "contract";
import type { Message, Peer } from "crossws";
import type { Context } from "effect";

import type { Db } from "../db/database.ts";
import { RunnerId } from "../db/types.ts";
import { handleRunnerMessage, SYNC_NOW } from "../runner-protocol.ts";
import { SYNC_INTERVAL_MS } from "../runners.ts";

/**
 * Holds every Runner's WebSocket in the hub's one process, as a Durable Object per Runner does on
 * Workers: it answers heartbeats and the Runner's messages, and sends the hint to sync. The Docker
 * hub runs as a single process (ADR 0014), so every Runner's sockets are here.
 */
export class RunnerSockets {
  readonly #runners = new Map<RunnerId, Set<Peer>>();
  /** When the Runner was last recorded as seen through each socket. */
  readonly #lastSeenWrittenAt = new Map<Peer, number>();
  /** The services with the hub's database, for the Runners' protocol. */
  readonly #services: Context.Context<Db>;

  constructor(services: Context.Context<Db>) {
    this.#services = services;
  }

  /** Takes over the open socket of the Runner the upgrade authenticated, in its `runnerId`. */
  open(peer: Peer): void {
    const runnerId = runnerOf(peer);
    let sockets = this.#runners.get(runnerId);
    if (!sockets) {
      sockets = new Set();
      this.#runners.set(runnerId, sockets);
    }
    sockets.add(peer);
    this.#lastSeenWrittenAt.set(peer, 0);
  }

  /** Answers a heartbeat, or a message of the protocol. */
  async message(peer: Peer, message: Message): Promise<void> {
    const text = message.text();
    if (text === HEARTBEAT_PING) {
      peer.send(HEARTBEAT_PONG);
      return;
    }
    const now = Date.now();
    const recordSeen = now - (this.#lastSeenWrittenAt.get(peer) ?? 0) >= SYNC_INTERVAL_MS;
    const reply = await handleRunnerMessage(text, {
      runnerId: runnerOf(peer),
      services: this.#services,
      recordSeen,
    });
    if (recordSeen) this.#lastSeenWrittenAt.set(peer, now);
    if (reply !== undefined) peer.send(reply);
  }

  /** Forgets a closed socket. */
  close(peer: Peer): void {
    const runnerId = runnerOf(peer);
    const sockets = this.#runners.get(runnerId);
    sockets?.delete(peer);
    if (sockets?.size === 0) this.#runners.delete(runnerId);
    this.#lastSeenWrittenAt.delete(peer);
  }

  /** Tells the Runner to sync now, on each of its open sockets. */
  requestSync(runnerId: RunnerId): void {
    for (const peer of this.#runners.get(runnerId) ?? []) peer.send(SYNC_NOW);
  }

  /** Closes the Runner's sockets, for a Runner the hub has removed. */
  drop(runnerId: RunnerId): void {
    for (const peer of this.#runners.get(runnerId) ?? []) peer.close(1008, "Runner removed");
  }

  /** Closes every Runner's sockets, for a hub that is stopping; Runners reconnect to its successor. */
  closeAll(): void {
    for (const sockets of this.#runners.values()) {
      for (const peer of sockets) peer.close(1001, "Hub stopping");
    }
  }
}

/** The Runner whose socket `peer` is, as the upgrade authenticated it. */
function runnerOf(peer: Peer): RunnerId {
  return RunnerId(String(peer.context.runnerId));
}
