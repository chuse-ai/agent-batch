import { type Kysely, sql } from "kysely";

// A job's current placement and what came of it: the Runner, the placement's generation (ADR 0026),
// when its agent's loop started and ended, and its result. The Runner is copied, not referenced, so
// a removed Runner stays on the jobs it ran: it identifies where they ran. The loop's times
// are the Runner's clock, as its reports give them, since a report may arrive a sync interval late;
// an end the hub makes itself, as when it cancels a job no Runner was sent, is the hub's clock.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema
    .alterTable("job")
    .addColumn("runnerId", "text")
    // 0 until the job is first placed; each placement raises it by one.
    .addColumn("generation", "integer", (col) => col.notNull().defaultTo(0))
    // The hub's clock at placement; the deadline for the Runner to confirm it reads it.
    .addColumn("placedAt", "timestamptz")
    .addColumn("startedAt", "timestamptz")
    .addColumn("endedAt", "timestamptz")
    .addColumn("result", "jsonb")
    .execute();
  // A Runner's sync reads its unfinished jobs; placement reads the queued jobs without a Runner in
  // the order they were accepted, which their IDs (UUID v7) follow.
  await schema
    .createIndex("job_runnerId_unfinished_idx")
    .on("job")
    .columns(["runnerId"])
    .where(sql.ref("status"), "in", sql`('queued', 'running')`)
    .execute();
  await schema
    .createIndex("job_queued_unplaced_idx")
    .on("job")
    .columns(["id"])
    .where(sql.ref("status"), "=", "queued")
    .where(sql.ref("runnerId"), "is", null)
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.dropIndex("job_queued_unplaced_idx").execute();
  await schema.dropIndex("job_runnerId_unfinished_idx").execute();
  await schema
    .alterTable("job")
    .dropColumn("result")
    .dropColumn("endedAt")
    .dropColumn("startedAt")
    .dropColumn("placedAt")
    .dropColumn("generation")
    .dropColumn("runnerId")
    .execute();
}
