import type { Runner } from "contract";
import type { Kysely } from "kysely";

import type { Database, RunnersId } from "./db/types.ts";

// Runners are recorded only by when the hub last heard from them. These writes may arrive late or out
// of order, so each one moves `last_seen_at` forward or does nothing, and their order never matters.

/** Records that the Runner subscribed to commands under `name` at `seenAt`, creating its row. */
export async function recordRunnerSubscribed(
  db: Kysely<Database>,
  runnerId: RunnersId,
  name: string,
  seenAt: Date,
): Promise<void> {
  await db
    .insertInto("runners")
    .values({ id: runnerId, name, last_seen_at: seenAt })
    .onConflict((oc) =>
      oc
        .column("id")
        .doUpdateSet((eb) => ({
          name: eb.ref("excluded.name"),
          last_seen_at: eb.ref("excluded.last_seen_at"),
        }))
        .where((eb) => eb("runners.last_seen_at", "<", eb.ref("excluded.last_seen_at"))),
    )
    .execute();
}

/** Records that the hub heard from the Runner at `seenAt`, such as its latest heartbeat. */
export async function recordRunnerSeen(
  db: Kysely<Database>,
  runnerId: RunnersId,
  seenAt: Date,
): Promise<void> {
  await db
    .updateTable("runners")
    .set({ last_seen_at: seenAt })
    .where("id", "=", runnerId)
    .where("last_seen_at", "<", seenAt)
    .execute();
}

/** Lists every Runner the hub has recorded, as its API returns them. */
export async function listRunners(db: Kysely<Database>): Promise<Runner[]> {
  const runners = await db.selectFrom("runners").select(["id", "name", "last_seen_at"]).execute();
  return runners.map((runner) => ({
    id: runner.id,
    name: runner.name,
    lastSeenAt: runner.last_seen_at.toISOString(),
  }));
}
