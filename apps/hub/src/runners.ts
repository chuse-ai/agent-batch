import type { Runner } from "contract";
import type { Kysely } from "kysely";

import type { Database, RunnerId } from "./db/types.ts";

// Runners are recorded only by when the hub last heard from them. These writes may arrive late or out
// of order, so each one moves `lastSeenAt` forward or does nothing, and their order never matters.

/** Records that the Runner subscribed to commands under `name` at `seenAt`. */
export async function recordRunnerSubscribed(
  db: Kysely<Database>,
  runnerId: RunnerId,
  name: string,
  seenAt: Date,
): Promise<void> {
  await db
    .updateTable("runner")
    .set({ name, lastSeenAt: seenAt })
    .where("id", "=", runnerId)
    .where("lastSeenAt", "<", seenAt)
    .execute();
}

/** Records that the hub heard from the Runner at `seenAt`, such as its latest heartbeat. */
export async function recordRunnerSeen(
  db: Kysely<Database>,
  runnerId: RunnerId,
  seenAt: Date,
): Promise<void> {
  await db
    .updateTable("runner")
    .set({ lastSeenAt: seenAt })
    .where("id", "=", runnerId)
    .where("lastSeenAt", "<", seenAt)
    .execute();
}

/** Lists every Runner the hub has recorded, as its API returns them. */
export function listRunners(db: Kysely<Database>): Promise<Runner[]> {
  return db.selectFrom("runner").select(["id", "name", "lastSeenAt"]).execute();
}
