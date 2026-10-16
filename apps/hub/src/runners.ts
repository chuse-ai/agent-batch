import type { Runner } from "contract";
import { Effect } from "effect";

import { Db } from "./db/database.ts";
import type { RunnerId } from "./db/types.ts";

// Runners are recorded only by when the hub last heard from them. These writes may arrive late or out
// of order, so each one moves `lastSeenAt` forward or does nothing, and their order never matters.

/** Records that the Runner subscribed to commands under `name` at `seenAt`. */
export function recordRunnerSubscribed(runnerId: RunnerId, name: string, seenAt: Date) {
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

/** Records that the hub heard from the Runner at `seenAt`, such as its latest heartbeat. */
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
 * When the Runner's next heartbeat check is due, given when its latest heartbeat arrived (0 before
 * the first) and the timeout, or `undefined` if it has gone silent and its sockets are to be dropped.
 * Both hubs apply it, each scheduling the checks its own way.
 */
export function nextHeartbeatCheck(
  lastHeartbeat: number,
  timeoutMs: number,
  now: number,
): number | undefined {
  const deadline = lastHeartbeat + timeoutMs;
  return deadline > now ? deadline : undefined;
}

/** How the hub closes the sockets of a Runner that has gone silent. */
export const SILENT_RUNNER_CLOSE = { code: 1001, reason: "No heartbeat" } as const;
