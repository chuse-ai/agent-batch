import { type Kysely, sql } from "kysely";

// What a caller's control of a job leaves on it. A control the job's Runner has not yet acted
// on: which, and when it was asked for. The job's summary reads both as `pendingControl`, and each
// sync of the job's Runner reads which as the placement's intent. Both are null while none waits: a
// control the hub applies itself, as a pause or cancel of a job no Runner was sent, never waits,
// and one that waits is cleared once the job's status confirms it or the job ends first. And why the
// job stopped, read by the job's summary.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.createType("jobControl").asEnum(["pause", "cancel"]).execute();
  await schema
    .createType("jobReason")
    .asEnum(["runnerRemoved", "runTimedOut", "modelBudgetExceeded"])
    .execute();
  await schema
    .alterTable("job")
    .addColumn("pendingControl", sql`agent_batch."jobControl"`)
    // The hub's clock.
    .addColumn("pendingControlRequestedAt", "timestamptz")
    // Null unless the job failed because its Runner was removed or a limit stopped it; a failure
    // of the job's own agent has no reason.
    .addColumn("reason", sql`agent_batch."jobReason"`)
    .execute();
  await schema
    .alterTable("job")
    .addCheckConstraint(
      "job_pendingControl_check",
      sql`("pendingControl" is null) = ("pendingControlRequestedAt" is null)`,
    )
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.alterTable("job").dropConstraint("job_pendingControl_check").execute();
  await schema
    .alterTable("job")
    .dropColumn("reason")
    .dropColumn("pendingControlRequestedAt")
    .dropColumn("pendingControl")
    .execute();
  await schema.dropType("jobReason").execute();
  await schema.dropType("jobControl").execute();
}
