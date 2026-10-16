import type { WithEffectContext } from "@orpc/experimental-effect";
import "@orpc/experimental-effect/extensions/effect";
import { implement } from "@orpc/server";
import { experimental_RPCHandler as RPCHandler } from "@orpc/server/crossws";
import { HEARTBEAT_PING, HEARTBEAT_PONG, runnerProtocol, type Command } from "contract";
import type { Message, Peer } from "crossws";
import { Clock, type Context, Effect } from "effect";

import type { Db } from "../db/database.ts";
import type { RunnerId } from "../db/types.ts";
import {
  nextHeartbeatCheck,
  recordRunnerSeen,
  recordRunnerSubscribed,
  SILENT_RUNNER_CLOSE,
} from "../runners.ts";

interface ProtocolContext extends WithEffectContext<Db> {
  runnerId: RunnerId;
}

const os = implement<typeof runnerProtocol, ProtocolContext>(runnerProtocol);

const protocol = new RPCHandler(
  os.router({
    commands: os.commands.effect(function* ({ input, context, signal }) {
      yield* recordRunnerSubscribed(context.runnerId, input.name, new Date());
      return commandsUntil(signal);
    }),
  }),
);

/** The Runner's commands, a stream that stays open until the Runner ends it or its socket closes. */
// oxlint-disable-next-line require-yield -- No commands exist yet; jobs will be the first.
async function* commandsUntil(signal: AbortSignal | undefined): AsyncGenerator<Command> {
  await new Promise((resolve) => signal?.addEventListener("abort", resolve, { once: true }));
}

/** One Runner's open sockets, each with when its latest heartbeat arrived (0 before the first). */
type Sockets = Map<Peer, number>;

/** Starts `watch` as the Runner `runnerId`'s one watching fiber, replacing any it had. */
type Watch = (runnerId: RunnerId, watch: Effect.Effect<void>) => void;

/**
 * Holds every Runner's WebSocket in the hub's one process, as a Durable Object per Runner does on
 * Workers: it answers heartbeats, drops a Runner's sockets once heartbeats stop, and records the
 * latest heartbeat as when the Runner was last seen. The Docker hub runs as a single process (ADR
 * 0014), so every Runner's sockets are here.
 */
export class RunnerSockets {
  readonly #runners = new Map<RunnerId, Sockets>();
  readonly #watch: Watch;
  /** The services with the hub's database, for the Runners' protocol and the watching fibers. */
  readonly #services: Context.Context<Db>;
  /** The runtime's clock, which the watching fibers also sleep on. */
  readonly #clock: Clock.Clock;
  readonly #heartbeatTimeoutMs: number;

  /**
   * `watch` runs each Runner's watching fiber in a `FiberMap`, which replaces a Runner's fiber when
   * it starts another and interrupts them all when the hub stops.
   */
  constructor(
    watch: Watch,
    services: Context.Context<Db>,
    clock: Clock.Clock,
    heartbeatTimeoutMs: number,
  ) {
    this.#watch = watch;
    this.#services = services;
    this.#clock = clock;
    this.#heartbeatTimeoutMs = heartbeatTimeoutMs;
  }

  /** Takes over the open socket of the Runner the upgrade authenticated, in its `runnerId`. */
  open(peer: Peer): void {
    const runnerId = runnerOf(peer);
    let sockets = this.#runners.get(runnerId);
    if (!sockets) {
      sockets = new Map();
      this.#runners.set(runnerId, sockets);
    }
    sockets.set(peer, 0);
    this.#watch(runnerId, this.#watchHeartbeats(runnerId, sockets));
  }

  /** Answers a heartbeat, or passes an oRPC message on. */
  async message(peer: Peer, message: Message): Promise<void> {
    const runnerId = runnerOf(peer);
    if (message.rawData === HEARTBEAT_PING) {
      peer.send(HEARTBEAT_PONG);
      this.#runners.get(runnerId)?.set(peer, this.#clock.currentTimeMillisUnsafe());
      return;
    }
    await protocol.message(peer, message, {
      context: { "effect/context": this.#services, runnerId },
    });
  }

  /** Forgets a closed socket. */
  async close(peer: Peer): Promise<void> {
    const runnerId = runnerOf(peer);
    const sockets = this.#runners.get(runnerId);
    sockets?.delete(peer);
    if (sockets?.size === 0) this.#runners.delete(runnerId);
    await protocol.close(peer);
  }

  /** Closes the Runner's sockets, for a Runner the hub has removed. */
  drop(runnerId: RunnerId): void {
    for (const peer of this.#runners.get(runnerId)?.keys() ?? [])
      peer.close(1008, "Runner removed");
  }

  /** Closes every Runner's sockets, for a hub that is stopping; Runners reconnect to its successor. */
  closeAll(): void {
    for (const sockets of this.#runners.values()) {
      for (const peer of sockets.keys()) peer.close(1001, "Hub stopping");
    }
  }

  /**
   * Waits until the Runner's heartbeats are due, then drops its sockets if none arrived in time, and
   * records the latest as when it was last seen; as the Durable Object's alarm does on Workers. Ends
   * once it has dropped the Runner's sockets or none is open.
   */
  #watchHeartbeats(runnerId: RunnerId, sockets: Sockets): Effect.Effect<void> {
    const timeoutMs = this.#heartbeatTimeoutMs;
    const recordSeen = (seenAt: number) =>
      recordRunnerSeen(runnerId, new Date(seenAt)).pipe(
        Effect.provide(this.#services),
        Effect.tapError((error) =>
          Effect.sync(() => console.error("Recording the Runner's last heartbeat failed", error)),
        ),
        Effect.ignore,
      );
    return Effect.gen(function* () {
      // As on Workers, a new socket gives the Runner a full timeout before its first heartbeat.
      let next: number | undefined = (yield* Clock.currentTimeMillis) + timeoutMs;
      while (next !== undefined && sockets.size > 0) {
        yield* Effect.sleep(Math.max(0, next - (yield* Clock.currentTimeMillis)));
        const lastHeartbeat = Math.max(0, ...sockets.values());
        next = nextHeartbeatCheck(lastHeartbeat, timeoutMs, yield* Clock.currentTimeMillis);
        if (next === undefined) {
          for (const peer of sockets.keys()) {
            peer.close(SILENT_RUNNER_CLOSE.code, SILENT_RUNNER_CLOSE.reason);
          }
        }
        // Recorded without waiting, as on Workers: a slow database must not delay the next check,
        // and writes only move the time forward, so their order does not matter. Detached, so the
        // last one still lands after the watch ends.
        if (lastHeartbeat > 0) yield* Effect.forkDetach(recordSeen(lastHeartbeat));
      }
    });
  }
}

/** The Runner whose socket `peer` is, as the upgrade authenticated it. */
function runnerOf(peer: Peer): RunnerId {
  return peer.context.runnerId as RunnerId;
}
