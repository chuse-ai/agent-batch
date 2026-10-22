import { type Kysely, sql } from "kysely";

// What outlives a job's content. A Usage record per end of a job: a job a resume
// carries on can end more than once, so each end records what the job used since its previous end,
// counted in the month it ends, and the job's totals at that end; it is written once, at the end,
// and copies what it reports rather than referencing it, since the job's row goes when the job is
// deleted. Deleting the job marks its latest record, which keeps what became of the job's files on
// its host. What a job's next end counts from lives on the job's row, and a deleted job's view
// while its files wait to be deleted lives on the deletion its Runner owes, so neither depends on a
// record whose month may be dropped. The deletion is read by each of that Runner's syncs until the
// Runner confirms it.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.createType("hostDeletionOutcome").asEnum(["confirmed", "notRequired"]).execute();
  await schema
    .createTable("usageRecord")
    .addColumn("jobId", "uuid", (col) => col.notNull())
    // The placement that ended, 0 for a job that ended before any.
    .addColumn("generation", "integer", (col) => col.notNull())
    .addColumn("organizationId", "text", (col) => col.notNull())
    .addColumn("status", sql`agent_batch."jobStatus"`, (col) => col.notNull())
    // Null for a job that ended before a Runner was sent it or started it.
    .addColumn("runnerId", "text")
    .addColumn("submitterKeyId", "text", (col) => col.notNull())
    // Null when the key has no name, which Better Auth allows.
    .addColumn("submitterKeyName", "text")
    .addColumn("submitterMemberId", "text", (col) => col.notNull())
    // The job's own times: when it was accepted (the hub's clock), when it started, null for one
    // that ended before it started, and this end, as the job's row records them.
    .addColumn("createdAt", "timestamptz", (col) => col.notNull())
    .addColumn("startedAt", "timestamptz")
    .addColumn("endedAt", "timestamptz", (col) => col.notNull())
    // What the job used since its previous end, as pi counts it; read by cost reports.
    .addColumn("totalTokens", "bigint", (col) => col.notNull())
    .addColumn("costUsd", "numeric", (col) => col.notNull())
    // The job's totals at this end; read by cost reports as the job's use over its life,
    // whose earlier months may be gone.
    .addColumn("jobTotalTokens", "bigint", (col) => col.notNull())
    .addColumn("jobCostUsd", "numeric", (col) => col.notNull())
    // When the job was deleted, by the hub's clock, on its latest record only; null before. Read by
    // cost reports, which keep a deleted job's usage.
    .addColumn("deletedAt", "timestamptz")
    // What became of the deleted job's files on its host, and when, by the hub's clock; null while
    // its Runner still owes their deletion, and before the job is deleted. Read by the Usage records
    // an admin reads, to show whether a deleted job's files left its host.
    .addColumn("hostDeletionOutcome", sql`agent_batch."hostDeletionOutcome"`)
    .addColumn("hostDeletionOutcomeAt", "timestamptz")
    .addPrimaryKeyConstraint("usageRecord_pkey", ["jobId", "generation"])
    .addForeignKeyConstraint(
      "usageRecord_organizationId_fkey",
      ["organizationId"],
      "organization",
      ["id"],
      (constraint) => constraint.onDelete("cascade"),
    )
    .addCheckConstraint(
      "usageRecord_status_check",
      sql`"status" in ('succeeded', 'failed', 'cancelled')`,
    )
    .addCheckConstraint(
      "usageRecord_hostDeletionOutcome_check",
      sql`("hostDeletionOutcome" is null) = ("hostDeletionOutcomeAt" is null)`,
    )
    .execute();
  await schema
    .createTable("jobDeletion")
    .addColumn("jobId", "uuid", (col) => col.primaryKey())
    .addColumn("organizationId", "text", (col) => col.notNull())
    .addColumn("runnerId", "text", (col) => col.notNull())
    // The job's last placement, under which the Runner holds its files.
    .addColumn("generation", "integer", (col) => col.notNull())
    // When the job was deleted, by the hub's clock.
    .addColumn("requestedAt", "timestamptz", (col) => col.notNull())
    // The deleted job as it reads while the deletion waits, which can outlast its Usage records'
    // months: its final status, its times, null for a start it never had, and its totals.
    .addColumn("status", sql`agent_batch."jobStatus"`, (col) => col.notNull())
    .addColumn("createdAt", "timestamptz", (col) => col.notNull())
    .addColumn("startedAt", "timestamptz")
    .addColumn("endedAt", "timestamptz", (col) => col.notNull())
    .addColumn("totalTokens", "bigint", (col) => col.notNull())
    .addColumn("costUsd", "numeric", (col) => col.notNull())
    .addForeignKeyConstraint(
      "jobDeletion_organizationId_fkey",
      ["organizationId"],
      "organization",
      ["id"],
      (constraint) => constraint.onDelete("cascade"),
    )
    .execute();
  await schema
    .createIndex("jobDeletion_runnerId_idx")
    .on("jobDeletion")
    .column("runnerId")
    .execute();
  // What the job's Usage records already count: its totals at its last end, 0 before any. Its next
  // end counts what it used from them, and the monthly limit counts what an unfinished job has
  // used beyond them. On the job's row, which each end updates in the statement that writes its
  // record.
  await schema
    .alterTable("job")
    .addColumn("recordedTotalTokens", "bigint", (col) => col.notNull().defaultTo(0))
    .addColumn("recordedCostUsd", "numeric", (col) => col.notNull().defaultTo(0))
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema
    .alterTable("job")
    .dropColumn("recordedCostUsd")
    .dropColumn("recordedTotalTokens")
    .execute();
  await schema.dropTable("jobDeletion").execute();
  await schema.dropTable("usageRecord").execute();
  await schema.dropType("hostDeletionOutcome").execute();
}
