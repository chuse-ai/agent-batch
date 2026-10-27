import { type Kysely, sql } from "kysely";

// Batches and labels: a Batch holds the jobs submitted together, which are released, paused and
// cancelled as one, and a job carries labels, a key and value each, to find it, group it and
// attribute its cost by.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  // Who held a Batch's members back: a caller's pause, or the failure breaker.
  await schema.createType("batchPausedBy").asEnum(["caller", "failFast"]).execute();

  // A Batch keeps only what its members cannot tell: its own settings, its hold and its cancel, the
  // breaker's counts, and what its deleted members leave behind. Its members' counts by status and
  // their cost are read from the members themselves.
  await schema
    .createTable("batch")
    .addColumn("id", "uuid", (col) => col.primaryKey())
    .addColumn("organizationId", "text", (col) =>
      col.notNull().references("organization.id").onDelete("cascade"),
    )
    // Null when its creation carried none; the unique constraint below ignores nulls.
    .addColumn("idempotencyKey", "text")
    // Labels every member gets, beside its own, which win where both name a key.
    .addColumn("labels", "jsonb", (col) => col.notNull())
    // The failure breaker: how many of its first members form the Canary, which runs before the rest
    // are released, and the share of failed members among finished ones, as a percentage, past
    // which the Batch pauses. Both are null when the Batch has no breaker.
    .addColumn("canarySize", "integer", (col) => col.check(sql`"canarySize" > 0`))
    .addColumn("tolerancePercent", "numeric", (col) =>
      col.check(sql`"tolerancePercent" >= 0 and "tolerancePercent" < 100`),
    )
    .addColumn("submitterKeyId", "text", (col) => col.notNull())
    // Null when the key has no name, which Better Auth allows.
    .addColumn("submitterKeyName", "text")
    .addColumn("submitterMemberId", "text", (col) => col.notNull())
    .addColumn("createdAt", "timestamptz", (col) => col.notNull())
    // Who paused the Batch and when, by the hub's clock; both null while it is not paused.
    .addColumn("pausedBy", sql`agent_batch."batchPausedBy"`)
    .addColumn("pausedAt", "timestamptz")
    // When the Batch was cancelled, by the hub's clock; null unless it was, and cleared when a
    // member's own resume carries the cancelled Batch on.
    .addColumn("cancelledAt", "timestamptz")
    // The breaker's counts since its trial began, at the Batch's creation or at the last resume
    // of a pause the breaker made: members' ends that count, succeeded or failed, and of them the
    // failures.
    .addColumn("trialEnds", "integer", (col) => col.notNull().defaultTo(0))
    .addColumn("trialFailures", "integer", (col) => col.notNull().defaultTo(0))
    // What its deleted members leave behind: how many, and their totals as they were deleted.
    .addColumn("deletedJobs", "integer", (col) => col.notNull().defaultTo(0))
    .addColumn("deletedTotalTokens", "bigint", (col) => col.notNull().defaultTo(0))
    .addColumn("deletedCostUsd", "numeric", (col) => col.notNull().defaultTo(0))
    .addUniqueConstraint("batch_organizationId_idempotencyKey_key", [
      "organizationId",
      "idempotencyKey",
    ])
    .addCheckConstraint(
      "batch_failFast_check",
      sql`("canarySize" is null) = ("tolerancePercent" is null)`,
    )
    .addCheckConstraint("batch_pausedBy_check", sql`("pausedBy" is null) = ("pausedAt" is null)`)
    .execute();

  await schema
    .alterTable("job")
    // The job's Batch; null for a job submitted alone. A Batch outlives its members, and nothing
    // deletes one yet, so a Batch with members cannot be deleted.
    .addColumn("batchId", "uuid", (col) => col.references("batch.id").onDelete("restrict"))
    // Whether the job is one of its Batch's Canary, which runs before the rest are released.
    .addColumn("inCanary", "boolean", (col) => col.notNull().defaultTo(false))
    .addColumn("labels", "jsonb", (col) => col.notNull().defaultTo(sql`'{}'::jsonb`))
    .execute();
  // A job submitted alone has an idempotency key unique in the Organization; a member, a key unique
  // in its Batch, the way a caller names each job of the Batch it builds.
  await schema.alterTable("job").dropConstraint("job_organizationId_idempotencyKey_key").execute();
  await schema
    .createIndex("job_organizationId_idempotencyKey_key")
    .on("job")
    .columns(["organizationId", "idempotencyKey"])
    .unique()
    .where(sql.ref("batchId"), "is", null)
    .execute();
  await schema
    .alterTable("job")
    .addUniqueConstraint("job_batchId_idempotencyKey_key", ["batchId", "idempotencyKey"])
    .execute();
  // A member is named by its key in its Batch, and only a member can be in a Canary.
  await schema
    .alterTable("job")
    .addCheckConstraint("job_batchId_check", sql`"batchId" is null or "idempotencyKey" is not null`)
    .execute();
  await schema
    .alterTable("job")
    .addCheckConstraint("job_inCanary_check", sql`not "inCanary" or "batchId" is not null`)
    .execute();
  // Listing jobs by label reads `labels @> …`, which this operator class serves.
  await sql`create index "job_labels_idx" on agent_batch.job using gin (labels jsonb_path_ops)`.execute(
    db,
  );
  // A Batch's summary and its controls read its unfinished members; placement, its unfinished Canary.
  await schema
    .createIndex("job_batchId_unfinished_idx")
    .on("job")
    .columns(["batchId", "inCanary"])
    .where(sql.ref("status"), "in", sql`('queued', 'running', 'paused')`)
    .execute();
  // Listing a Batch's members, newest first.
  await schema.createIndex("job_batchId_id_idx").on("job").columns(["batchId", "id"]).execute();

  // A Usage record copies the job's labels and Batch as the job ended, to group cost by after the
  // job is deleted. The Batch is copied rather than referenced, as the Runner is.
  await schema
    .alterTable("usageRecord")
    // Null for a job submitted alone.
    .addColumn("batchId", "uuid")
    .addColumn("labels", "jsonb", (col) => col.notNull().defaultTo(sql`'{}'::jsonb`))
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.alterTable("usageRecord").dropColumn("batchId").dropColumn("labels").execute();
  await schema.dropIndex("job_batchId_id_idx").execute();
  await schema.dropIndex("job_batchId_unfinished_idx").execute();
  await schema.dropIndex("job_labels_idx").execute();
  await schema.alterTable("job").dropConstraint("job_inCanary_check").execute();
  await schema.alterTable("job").dropConstraint("job_batchId_check").execute();
  await schema.alterTable("job").dropConstraint("job_batchId_idempotencyKey_key").execute();
  await schema.dropIndex("job_organizationId_idempotencyKey_key").execute();
  await schema
    .alterTable("job")
    .dropColumn("batchId")
    .dropColumn("inCanary")
    .dropColumn("labels")
    .execute();
  await schema
    .alterTable("job")
    .addUniqueConstraint("job_organizationId_idempotencyKey_key", [
      "organizationId",
      "idempotencyKey",
    ])
    .execute();
  await schema.dropTable("batch").execute();
  await schema.dropType("batchPausedBy").execute();
}
