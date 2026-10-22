import { type Kysely, sql } from "kysely";

// A control a caller asked for that the job's Runner has not yet acted on: which, and when it
// was asked for. The job's summary reads both as `pendingControl`, and each sync of the job's Runner
// reads which as the placement's intent. Both are null while none waits: a control the hub applies
// itself, as a cancel of a job no Runner was sent, never waits, and one that waits is cleared once
// the job's status confirms it or the job ends first.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.createType("jobControl").asEnum(["cancel"]).execute();
  await schema
    .alterTable("job")
    .addColumn("pendingControl", sql`agent_batch."jobControl"`)
    // The hub's clock.
    .addColumn("pendingControlRequestedAt", "timestamptz")
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
    .dropColumn("pendingControlRequestedAt")
    .dropColumn("pendingControl")
    .execute();
  await schema.dropType("jobControl").execute();
}
