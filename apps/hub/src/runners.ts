import type { Runner } from "contract";
import { Effect } from "effect";

import { Db } from "./db/database.ts";
import type { RunnerId } from "./db/types.ts";

// Runners are recorded only by when the hub last heard from them. These writes may arrive late or out
// of order, so each one moves `lastSeenAt` forward or does nothing, and their order never matters.

/** Records that the Runner said hello under `name` at `seenAt`. */
export function recordRunnerHello(runnerId: RunnerId, name: string, seenAt: Date) {
  return Db.use((database) =>
    database.query((db) =>
      db
        .updateTable("runner")
        .set({ name, lastSeenAt: seenAt })
        .where("id", "=", runnerId)
        .where("lastSeenAt", "<", seenAt)
        .execute(),
    ),
  ).pipe(Effect.asVoid);
}

/** Records that the hub heard from the Runner at `seenAt`: a sync or a report. */
export function recordRunnerSeen(runnerId: RunnerId, seenAt: Date) {
  return Db.use((database) =>
    database.query((db) =>
      db
        .updateTable("runner")
        .set({ lastSeenAt: seenAt })
        .where("id", "=", runnerId)
        .where("lastSeenAt", "<", seenAt)
        .execute(),
    ),
  ).pipe(Effect.asVoid);
}

/** Lists every Runner the hub has recorded, as its API returns them. */
export const listRunners = Db.use((database) =>
  database.query((db): Promise<Runner[]> =>
    db.selectFrom("runner").select(["id", "name", "lastSeenAt"]).execute(),
  ),
);

/**
 * How often a Runner syncs when nothing prompts it sooner, told to it in the hub's answer to its
 * hello (ADR 0026): three minutes, for every deployment until one shows a reason to tune it.
 */
export const SYNC_INTERVAL_MS = 180_000;

/**
 * How long a Runner may go unheard before it counts as gone: a sync it missed, the next one, and a
 * minute for it to arrive. Only Runners heard from more recently are told to sync.
 */
export const GONE_AFTER_MS = 2 * SYNC_INTERVAL_MS + 60_000;

/**
 * Tells every Runner heard from within the last `GONE_AFTER_MS` to sync now, through `requestSync`,
 * which reaches the Runner's connection on this platform. A Runner that is not connected after all
 * misses nothing: its next sync finds the same jobs.
 */
export function hintLiveRunners(requestSync: (runnerId: RunnerId) => Promise<void>) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const since = new Date(Date.now() - GONE_AFTER_MS);
    const runners = yield* database.query((db) =>
      db.selectFrom("runner").select("id").where("lastSeenAt", ">", since).execute(),
    );
    yield* Effect.tryPromise(() => Promise.all(runners.map((runner) => requestSync(runner.id))));
  }).pipe(Effect.asVoid);
}
