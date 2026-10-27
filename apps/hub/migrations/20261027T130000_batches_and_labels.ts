import { type Kysely, sql } from "kysely";

// Batches and labels: every job belongs to a Batch, the jobs added together, which run one agent
// definition, are released, paused and cancelled as one, and carry the failure breaker; a job
// carries labels, a key and value each, to find it, group it and attribute its cost by.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  // Who paused a Batch: a caller, or its failure breaker.
  await schema.createType("batchPausedBy").asEnum(["caller", "breaker"]).execute();

  // A Batch keeps only what its jobs cannot tell: its agent, labels and breaker, its pause and its
  // cancel, and the breaker's counts and Canary. Its jobs' counts by status and their cost are read
  // from the jobs themselves.
  await schema
    .createTable("batch")
    .addColumn("id", "uuid", (col) => col.primaryKey())
    .addColumn("organizationId", "text", (col) =>
      col.notNull().references("organization.id").onDelete("cascade"),
    )
    // The caller's own ID for the Batch, unique in the Organization; null when its creation gave
    // none, which the unique constraint below ignores.
    .addColumn("customId", "text")
    // SHA-256 of the creation's RFC 8785 canonical JSON without the custom ID, in hex, which a
    // creation naming the same custom ID compares; null when there is none to compare under.
    .addColumn("contentHash", "text")
    // The agent every job of the Batch runs, as the creation gave it, with the hub's default for
    // each limit it left out. Its Input files are `batchInputFile`'s.
    // What the agent is told beside the Runner's own instructions; null when the agent has none.
    .addColumn("instructions", "text")
    .addColumn("resultContract", "jsonb", (col) => col.notNull())
    // Each job takes these limits when it is added; a job's resume may give it others.
    .addColumn("runTimeoutSeconds", "integer", (col) =>
      col.notNull().check(sql`"runTimeoutSeconds" > 0`),
    )
    .addColumn("modelBudgetUsd", "numeric", (col) => col.notNull().check(sql`"modelBudgetUsd" > 0`))
    // Labels every job gets, beside its own, which win where both name a key.
    .addColumn("labels", "jsonb", (col) => col.notNull())
    // The failure breaker: how many jobs each Canary holds, and the share of failed jobs among those
    // ended since the Canary began, as a percentage, past which the Batch pauses. Both are null when
    // the Batch has no breaker.
    .addColumn("canary", "integer", (col) => col.check(sql`canary >= 0`))
    .addColumn("maxFailurePercent", "numeric", (col) =>
      col.check(sql`"maxFailurePercent" between 0 and 100`),
    )
    .addColumn("submitterKeyId", "text", (col) => col.notNull())
    // Null when the key has no name, which Better Auth allows.
    .addColumn("submitterKeyName", "text")
    .addColumn("submitterMemberId", "text", (col) => col.notNull())
    .addColumn("createdAt", "timestamptz", (col) => col.notNull())
    // Who paused the Batch and when, by the hub's clock; both null while it is not paused.
    .addColumn("pausedBy", sql`agent_batch."batchPausedBy"`)
    .addColumn("pausedAt", "timestamptz")
    // When the breaker's rule tripped while a caller had the Batch paused, by the hub's clock; null
    // otherwise. The caller's resume then starts a new Canary, as a resume of the breaker's pause
    // does, and clears it.
    .addColumn("breakerTrippedAt", "timestamptz")
    // When the Batch was cancelled, by the hub's clock; null unless it was. A cancel is final.
    .addColumn("cancelledAt", "timestamptz")
    // The breaker's counts since its current Canary began: the jobs whose counted end, success or
    // failure, has a Usage record after `countedAfterUsageRecordId`, and of them those that failed.
    // That ID is the latest record's when the Canary began, and 0 for the Batch's first Canary.
    .addColumn("countedAfterUsageRecordId", "bigint", (col) => col.notNull().defaultTo(0))
    .addColumn("countedEnds", "integer", (col) => col.notNull().defaultTo(0))
    .addColumn("countedFailures", "integer", (col) => col.notNull().defaultTo(0))
    // The places of its current Canary that no job has taken yet, which the jobs added next take.
    .addColumn("canaryPlacesLeft", "integer", (col) => col.notNull().defaultTo(0))
    .addUniqueConstraint("batch_organizationId_customId_key", ["organizationId", "customId"])
    .addCheckConstraint(
      "batch_contentHash_check",
      sql`("customId" is null) = ("contentHash" is null)`,
    )
    .addCheckConstraint(
      "batch_breaker_check",
      sql`(canary is null) = ("maxFailurePercent" is null)`,
    )
    .addCheckConstraint("batch_pausedBy_check", sql`("pausedBy" is null) = ("pausedAt" is null)`)
    .addCheckConstraint(
      "batch_breakerTrippedAt_check",
      sql`"breakerTrippedAt" is null or "pausedBy" = 'caller'`,
    )
    .execute();

  // The agent's Input files, which every job of the Batch gets beside its own. A Batch's files go
  // with it; a blob cannot be deleted while a Batch's file refers to it.
  await schema
    .createTable("batchInputFile")
    .addColumn("batchId", "uuid", (col) => col.notNull().references("batch.id").onDelete("cascade"))
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("sha256", "bytea", (col) =>
      col.notNull().references("blob.sha256").onDelete("restrict"),
    )
    .addPrimaryKeyConstraint("batchInputFile_pkey", ["batchId", "name"])
    .execute();
  // For the reference check of deleting a blob, and for finding whether any Batch still uses one.
  await schema
    .createIndex("batchInputFile_sha256_idx")
    .on("batchInputFile")
    .column("sha256")
    .execute();

  // Each job accepted before Batches existed becomes a Batch of one, without a breaker, whose agent
  // is the job's Result contract and limits, and whose ID is the job's.
  await sql`
    insert into agent_batch.batch (
      id, "organizationId", "resultContract", "runTimeoutSeconds", "modelBudgetUsd", labels,
      "submitterKeyId", "submitterKeyName", "submitterMemberId", "createdAt"
    )
    select
      id, "organizationId", "resultContract", "runTimeoutSeconds", "modelBudgetUsd", '{}'::jsonb,
      "submitterKeyId", "submitterKeyName", "submitterMemberId", "createdAt"
    from agent_batch.job
  `.execute(db);

  await schema
    .alterTable("job")
    // The job's Batch. Nothing deletes a Batch yet; the Organization's deletion deletes its Batches
    // and jobs in the same statement, by which point no job refers to a Batch.
    .addColumn("batchId", "uuid", (col) => col.references("batch.id").onDelete("no action"))
    // Whether the job is in its Batch's current Canary, which runs before the rest are released.
    .addColumn("inCanary", "boolean", (col) => col.notNull().defaultTo(false))
    .addColumn("labels", "jsonb", (col) => col.notNull().defaultTo(sql`'{}'::jsonb`))
    .execute();
  // What the job gives its Batch's agent, which was its Prompt; its Result contract is the Batch's.
  await schema.alterTable("job").renameColumn("prompt", "input").execute();
  // The caller's own ID for the job, unique in its Batch, which was its idempotency key, unique in
  // the Organization. A job accepted before has its key, or else its own ID.
  await schema.alterTable("job").renameColumn("idempotencyKey", "customId").execute();
  await sql`
    update agent_batch.job set "batchId" = id, "customId" = coalesce("customId", id::text)
  `.execute(db);
  await schema
    .alterTable("job")
    .alterColumn("batchId", (col) => col.setNotNull())
    .alterColumn("customId", (col) => col.setNotNull())
    .alterColumn("labels", (col) => col.dropDefault())
    .dropColumn("resultContract")
    .execute();
  await schema.alterTable("job").dropConstraint("job_organizationId_idempotencyKey_key").execute();
  // Adding a job inserts it under this constraint, so a concurrent duplicate waits for the first.
  await schema
    .alterTable("job")
    .addUniqueConstraint("job_batchId_customId_key", ["batchId", "customId"])
    .execute();
  // Listing jobs by custom ID across Batches.
  await schema.createIndex("job_customId_idx").on("job").column("customId").execute();
  // Listing jobs by label reads `labels @> …`, which this operator class serves.
  await sql`create index "job_labels_idx" on agent_batch.job using gin (labels jsonb_path_ops)`.execute(
    db,
  );
  // A Batch's summary and its controls read its unfinished jobs; placement, its unfinished Canary.
  await schema
    .createIndex("job_batchId_unfinished_idx")
    .on("job")
    .columns(["batchId", "inCanary"])
    .where(sql.ref("status"), "in", sql`('queued', 'running', 'paused')`)
    .execute();
  // Listing a Batch's jobs, newest first.
  await schema.createIndex("job_batchId_id_idx").on("job").columns(["batchId", "id"]).execute();

  // A Usage record copies the job's labels and Batch as the job ended, to group cost by after the
  // job is deleted. The Batch is copied rather than referenced, as the Runner is. A record of an
  // end before Batches existed has its job's Batch of one, whose ID is the job's.
  await schema
    .alterTable("usageRecord")
    .addColumn("batchId", "uuid")
    .addColumn("labels", "jsonb", (col) => col.notNull().defaultTo(sql`'{}'::jsonb`))
    .execute();
  await sql`update agent_batch."usageRecord" set "batchId" = "jobId"`.execute(db);
  await schema
    .alterTable("usageRecord")
    .alterColumn("batchId", (col) => col.setNotNull())
    .alterColumn("labels", (col) => col.dropDefault())
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.alterTable("usageRecord").dropColumn("batchId").dropColumn("labels").execute();
  await schema.dropIndex("job_batchId_id_idx").execute();
  await schema.dropIndex("job_batchId_unfinished_idx").execute();
  await schema.dropIndex("job_labels_idx").execute();
  await schema.dropIndex("job_customId_idx").execute();
  await schema.alterTable("job").dropConstraint("job_batchId_customId_key").execute();
  await schema
    .alterTable("job")
    .addColumn("resultContract", "jsonb")
    .alterColumn("customId", (col) => col.dropNotNull())
    .execute();
  // A job takes its Batch's Result contract, and keeps its custom ID as its idempotency key where
  // that is not its own ID and no other job of the Organization has it.
  await sql`
    update agent_batch.job j set
      "resultContract" = b."resultContract",
      "customId" = case
        when j."customId" = j.id::text or exists (
          select from agent_batch.job other
          where other."organizationId" = j."organizationId"
            and other."customId" = j."customId" and other.id <> j.id
        ) then null
        else j."customId"
      end
    from agent_batch.batch b
    where b.id = j."batchId"
  `.execute(db);
  await schema
    .alterTable("job")
    .alterColumn("resultContract", (col) => col.setNotNull())
    .dropColumn("batchId")
    .dropColumn("inCanary")
    .dropColumn("labels")
    .execute();
  await schema.alterTable("job").renameColumn("customId", "idempotencyKey").execute();
  await schema.alterTable("job").renameColumn("input", "prompt").execute();
  await schema
    .alterTable("job")
    .addUniqueConstraint("job_organizationId_idempotencyKey_key", [
      "organizationId",
      "idempotencyKey",
    ])
    .execute();
  await schema.dropTable("batchInputFile").execute();
  await schema.dropTable("batch").execute();
  await schema.dropType("batchPausedBy").execute();
}
