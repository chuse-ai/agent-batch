import { type Kysely, sql } from "kysely";

// What limits a job, resolved when it is submitted from what it gives or the hub's defaults, so a
// later change of defaults never moves it, and given again by a resume that names new ones. Each sync
// of the job's Runner sends both with the job's run; the job's summary reads both.

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .withSchema("agent_batch")
    .alterTable("job")
    // How long each run may work on its Runner, in seconds: the run's `runTimeoutSeconds`.
    .addColumn("runTimeoutSeconds", "integer", (col) =>
      col.notNull().check(sql`"runTimeoutSeconds" > 0`),
    )
    // The most the job's model calls may cost over its life, in US dollars, compared exactly with its
    // cost: the run's `modelBudgetUsd`.
    .addColumn("modelBudgetUsd", "numeric", (col) => col.notNull().check(sql`"modelBudgetUsd" > 0`))
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .withSchema("agent_batch")
    .alterTable("job")
    .dropColumn("modelBudgetUsd")
    .dropColumn("runTimeoutSeconds")
    .execute();
}
