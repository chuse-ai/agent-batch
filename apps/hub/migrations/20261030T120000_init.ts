import { type Kysely, sql } from "kysely";

// Migrations stay as written once released, so they name the schema here instead of importing it.
// Names follow Better Auth's conventions, whose tables share the schema: singular table names and
// camelCase columns (ADR 0020).

// Better Auth's tables, as drafted by its CLI (`vp run hub#db:draft-auth-migration`) for Better Auth
// 1.7.8 with the hub's plugins, copied here rather than generated at run time. The driver runs one
// statement per query.
const betterAuthStatements = [
  'create table "agent_batch"."user" ("id" text not null primary key, "name" text not null, "email" text not null unique, "emailVerified" boolean not null, "image" text, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz default CURRENT_TIMESTAMP not null)',
  'create table "agent_batch"."session" ("id" text not null primary key, "expiresAt" timestamptz not null, "token" text not null unique, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz not null, "ipAddress" text, "userAgent" text, "userId" text not null references "agent_batch"."user" ("id") on delete cascade, "activeOrganizationId" text)',
  'create table "agent_batch"."account" ("id" text not null primary key, "accountId" text not null, "providerId" text not null, "userId" text not null references "agent_batch"."user" ("id") on delete cascade, "accessToken" text, "refreshToken" text, "idToken" text, "accessTokenExpiresAt" timestamptz, "refreshTokenExpiresAt" timestamptz, "scope" text, "password" text, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz not null)',
  'create table "agent_batch"."verification" ("id" text not null primary key, "identifier" text not null, "value" text not null, "expiresAt" timestamptz not null, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz default CURRENT_TIMESTAMP not null)',
  'create table "agent_batch"."organization" ("id" text not null primary key, "name" text not null, "slug" text not null unique, "logo" text, "createdAt" timestamptz not null, "metadata" text)',
  'create table "agent_batch"."member" ("id" text not null primary key, "organizationId" text not null references "agent_batch"."organization" ("id") on delete cascade, "userId" text not null references "agent_batch"."user" ("id") on delete cascade, "role" text not null, "createdAt" timestamptz not null)',
  'create table "agent_batch"."invitation" ("id" text not null primary key, "organizationId" text not null references "agent_batch"."organization" ("id") on delete cascade, "email" text not null, "role" text, "status" text not null, "expiresAt" timestamptz not null, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "inviterId" text not null references "agent_batch"."user" ("id") on delete cascade)',
  'create table "agent_batch"."apikey" ("id" text not null primary key, "configId" text not null, "name" text, "start" text, "referenceId" text not null, "prefix" text, "key" text not null, "refillInterval" integer, "refillAmount" integer, "lastRefillAt" timestamptz, "enabled" boolean, "rateLimitEnabled" boolean, "rateLimitTimeWindow" integer, "rateLimitMax" integer, "requestCount" integer, "remaining" integer, "lastRequest" timestamptz, "expiresAt" timestamptz, "createdAt" timestamptz not null, "updatedAt" timestamptz not null, "permissions" text, "metadata" text)',
  'create table "agent_batch"."rateLimit" ("id" text not null primary key, "key" text not null unique, "count" integer not null, "lastRequest" bigint not null)',
  'create index "session_userId_idx" on "agent_batch"."session" ("userId")',
  'create index "account_userId_idx" on "agent_batch"."account" ("userId")',
  'create index "verification_identifier_idx" on "agent_batch"."verification" ("identifier")',
  'create index "member_organizationId_idx" on "agent_batch"."member" ("organizationId")',
  'create index "member_userId_idx" on "agent_batch"."member" ("userId")',
  'create index "invitation_organizationId_idx" on "agent_batch"."invitation" ("organizationId")',
  'create index "invitation_email_idx" on "agent_batch"."invitation" ("email")',
  'create index "apikey_configId_idx" on "agent_batch"."apikey" ("configId")',
  'create index "apikey_referenceId_idx" on "agent_batch"."apikey" ("referenceId")',
  'create index "apikey_key_idx" on "agent_batch"."apikey" ("key")',
];

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema.createSchema("agent_batch").ifNotExists().execute();
  for (const statement of betterAuthStatements) await sql.raw(statement).execute(db);

  const schema = db.schema.withSchema("agent_batch");
  await schema
    .createType("jobStatus")
    .asEnum(["queued", "running", "paused", "succeeded", "failed", "cancelled"])
    .execute();
  await schema.createType("jobControl").asEnum(["pause", "cancel"]).execute();
  await schema
    .createType("jobFailureReason")
    .asEnum([
      "noOutput",
      "runTimedOut",
      "modelBudgetExceeded",
      "modelFailed",
      "runFaulted",
      "runnerRemoved",
    ])
    .execute();
  await schema
    .createType("hostDeletionOutcome")
    .asEnum(["confirmed", "notRequired", "abandoned"])
    .execute();
  // Who paused a Batch: a caller, or its failure breaker.
  await schema.createType("batchPausedBy").asEnum(["caller", "breaker"]).execute();

  // A Runner's row goes with its own key: deleting the key, through the hub or through Better Auth,
  // deletes the Runner in the same statement.
  await schema
    .createTable("runner")
    .addColumn("id", "text", (col) => col.primaryKey())
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("lastSeenAt", "timestamptz", (col) => col.notNull())
    // The Runner's own API key, from which the hub takes the Runner's identity.
    .addColumn("keyId", "text", (col) => col.notNull().unique())
    .addForeignKeyConstraint("runner_keyId_fkey", ["keyId"], "apikey", ["id"], (constraint) =>
      constraint.onDelete("cascade"),
    )
    .execute();

  // Input files' content is stored once per SHA-256 as a blob: a file many jobs share is stored
  // once, and its bytes sit in chunk rows of original bytes, each compressed as one zstd frame,
  // which a Runner downloads one at a time. A blob's bytes belong to the upload that wrote them, and
  // the blob is reached through the upload: so an upload writes its chunks before its blob is known
  // and verified, and publishing it copies no bytes. Today each upload is one request's body.
  await schema
    .createTable("blob")
    // The SHA-256 of the original bytes, as 32 raw bytes: the address of the blob, which a Runner
    // checks what it downloads against.
    .addColumn("sha256", "bytea", (col) => col.primaryKey().check(sql`octet_length(sha256) = 32`))
    .addColumn("originalSizeBytes", "bigint", (col) =>
      col.notNull().check(sql`"originalSizeBytes" >= 0`),
    )
    .execute();

  await schema
    .createTable("upload")
    .addColumn("id", "uuid", (col) => col.primaryKey())
    // The verified SHA-256 of the upload's bytes, the blob it published; null until its size and
    // SHA-256 are verified. Deleting the blob deletes its upload, and with it the bytes.
    .addColumn("sha256", "bytea", (col) => col.references("blob.sha256").onDelete("cascade"))
    // How many original bytes each of its chunks holds but the last, chosen as the upload starts,
    // so the hub can choose another for new blobs without rewriting what it stored.
    .addColumn("chunkBytes", "integer", (col) => col.notNull().check(sql`"chunkBytes" > 0`))
    // When an upload not yet published is removed with its chunks: one whose bytes failed
    // verification or whose request was cut off. Ignored once published.
    .addColumn("expiresAt", "timestamptz", (col) => col.notNull())
    // A blob's bytes are those of one upload, through which it is read.
    .addUniqueConstraint("upload_sha256_key", ["sha256"])
    .execute();
  // For the sweep of uploads that were never published.
  await schema
    .createIndex("upload_expiresAt_idx")
    .on("upload")
    .column("expiresAt")
    .where(sql.ref("sha256"), "is", null)
    .execute();

  await schema
    .createTable("uploadChunk")
    .addColumn("uploadId", "uuid", (col) =>
      col.notNull().references("upload.id").onDelete("cascade"),
    )
    // The chunk's position in the file: chunk `index` holds the original bytes from `index` times
    // the upload's `chunkBytes`.
    .addColumn("index", "integer", (col) => col.notNull().check(sql`index >= 0`))
    // One zstd frame.
    .addColumn("bytes", "bytea", (col) => col.notNull())
    .addPrimaryKeyConstraint("uploadChunk_pkey", ["uploadId", "index"])
    .execute();
  // zstd's output does not compress further, so Postgres stores it as it is rather than trying
  // its own compression on every value first.
  await sql`alter table agent_batch."uploadChunk" alter column bytes set storage external`.execute(
    db,
  );

  // Content an organization uploaded, which its jobs may reference: a verified upload adds or renews
  // the row, also when the bytes were already stored for another organization. An organization
  // holds content while a row here has not expired, or while a job or Batch of its own references
  // it; only content it holds is answered as stored, or accepted in its jobs.
  await schema
    .createTable("organizationBlob")
    .addColumn("organizationId", "text", (col) =>
      col.notNull().references("organization.id").onDelete("cascade"),
    )
    // The content the organization uploaded.
    .addColumn("sha256", "bytea", (col) =>
      col.notNull().references("blob.sha256").onDelete("cascade"),
    )
    // Seven days after the organization last uploaded the content. Until then the content is the
    // organization's to reference, and is kept even when no job uses it.
    .addColumn("expiresAt", "timestamptz", (col) => col.notNull())
    .addPrimaryKeyConstraint("organizationBlob_pkey", ["organizationId", "sha256"])
    .execute();
  // For deleting a blob, and for finding whether anything still uses one.
  await schema
    .createIndex("organizationBlob_sha256_idx")
    .on("organizationBlob")
    .column("sha256")
    .execute();
  // For the sweep of expired rows.
  await schema
    .createIndex("organizationBlob_expiresAt_idx")
    .on("organizationBlob")
    .column("expiresAt")
    .execute();

  // Every job belongs to a Batch: the jobs added to it run one agent, are released, paused and
  // cancelled as one, and are watched by its failure breaker. A Batch keeps only what its jobs
  // cannot tell: its agent, labels and breaker, its pause and its cancel, and the breaker's counts
  // and Canary. Its jobs' counts by status and their cost are read from the jobs themselves.
  await schema
    .createTable("batch")
    // The hub's own key for the row, a UUIDv7, which other tables refer to and locks are ordered by;
    // the API never shows it (ADR 0036).
    .addColumn("rowId", "uuid", (col) => col.primaryKey())
    .addColumn("organizationId", "text", (col) =>
      col.notNull().references("organization.id").onDelete("cascade"),
    )
    // The Batch's ID in the API, unique in the Organization: the one its creation gave, or a UUIDv4
    // the hub made.
    .addColumn("id", "text", (col) => col.notNull())
    // SHA-256 of the creation's RFC 8785 canonical JSON without the ID, in hex, which a creation
    // naming the same ID compares.
    .addColumn("contentHash", "text", (col) => col.notNull())
    // The agent every job of the Batch runs, as the creation gave it, with the hub's default for
    // each limit it left out. Its Input files are `batchInputFile`'s.
    // What the agent is told beside the Runner's own instructions; null when the agent has none.
    .addColumn("instructions", "text")
    .addColumn("outputContract", "jsonb", (col) => col.notNull())
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
    // The ID first, so a lookup by the ID alone, as each call makes while a deployment serves one
    // Organization (ADR 0007), uses the constraint's index.
    .addUniqueConstraint("batch_id_organizationId_key", ["id", "organizationId"])
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
    .addColumn("batchRowId", "uuid", (col) =>
      col.notNull().references("batch.rowId").onDelete("cascade"),
    )
    // The file's path under each of the Batch's jobs' `inputs` folder, relative, with folders.
    .addColumn("path", "text", (col) => col.notNull())
    .addColumn("sha256", "bytea", (col) =>
      col.notNull().references("blob.sha256").onDelete("restrict"),
    )
    .addPrimaryKeyConstraint("batchInputFile_pkey", ["batchRowId", "path"])
    .execute();
  // For the reference check of deleting a blob, and for finding whether any Batch still uses one.
  await schema
    .createIndex("batchInputFile_sha256_idx")
    .on("batchInputFile")
    .column("sha256")
    .execute();

  // A job belongs to the Organization (ADR 0007). Its submitter and Runner are copied rather than
  // referenced: Better Auth deletes a key's row when the key is revoked or expires, and a removed
  // Runner stays on the jobs it ran, whose execution history identifies the host.
  await schema
    .createTable("job")
    .addColumn("id", "uuid", (col) => col.primaryKey())
    .addColumn("organizationId", "text", (col) => col.notNull())
    .addColumn("status", sql`agent_batch."jobStatus"`, (col) => col.notNull())
    // What the job gives its Batch's agent. Null when the job gives its agent no text, only its own
    // files.
    .addColumn("input", "text")
    // The caller's own ID for the job, unique in its Batch.
    .addColumn("customId", "text", (col) => col.notNull())
    // SHA-256 of the job's RFC 8785 canonical JSON as added, without its custom ID, in hex, which an
    // add naming the same custom ID compares.
    .addColumn("contentHash", "text", (col) => col.notNull())
    .addColumn("submitterKeyId", "text", (col) => col.notNull())
    // Null when the key has no name, which Better Auth allows.
    .addColumn("submitterKeyName", "text")
    .addColumn("submitterMemberId", "text", (col) => col.notNull())
    .addColumn("createdAt", "timestamptz", (col) => col.notNull())
    // The job's current run (ADR 0026): its Runner, when its agent's loop started and ended, and its
    // output. The loop's times are the Runner's clock, as its reports give them, since a report may
    // arrive a sync interval late; an end the hub makes itself, as when it cancels a job no Runner
    // was sent, is the hub's clock.
    .addColumn("runnerId", "text")
    // The run's ID, null until the job is first placed; a resume mints the next.
    .addColumn("runId", "uuid")
    // The hub's clock when the run was sent to its Runner, null until then; the deadline for the
    // Runner to confirm it reads it.
    .addColumn("placedAt", "timestamptz")
    .addColumn("startedAt", "timestamptz")
    .addColumn("endedAt", "timestamptz")
    // Null until the job succeeds: an output of JSON `null` is stored as the jsonb value `null`.
    .addColumn("output", "jsonb")
    // A control the job's Runner has not yet acted on: which, and when it was asked for, by the
    // hub's clock. The job's summary reads both as `pendingControl`, and each sync of the job's
    // Runner reads which as the placement's intent. Both are null while none waits: a control the hub
    // applies itself, as a pause or cancel of a job no Runner was sent, never waits, and one that
    // waits is cleared once the job's status confirms it or the job ends first.
    .addColumn("pendingControl", sql`agent_batch."jobControl"`)
    .addColumn("pendingControlRequestedAt", "timestamptz")
    // Why the job failed: null unless it failed, and cleared when it is resumed.
    .addColumn("failureReason", sql`agent_batch."jobFailureReason"`)
    // What the cause of the job's failure said: null when the cause has no text and when the job
    // has not failed, and cleared when it is resumed. It is job content, so a Usage record does not
    // copy it and it goes with the job's row when the job is deleted.
    .addColumn("failureMessage", "text")
    // What the job's Usage records already count: its totals at its last end, 0 before any. Its next
    // end counts what it used from them, and updates them in the statement that writes its record.
    .addColumn("recordedTotalTokens", "bigint", (col) => col.notNull().defaultTo(0))
    .addColumn("recordedCostUsd", "numeric", (col) => col.notNull().defaultTo(0))
    // The job's limits, its Batch's when it is added, so a later change of the hub's defaults never
    // moves them, and given again by a resume that names new ones.
    // Each sync of the job's Runner sends both with the job's run.
    // How long each run may work on its Runner, in seconds.
    .addColumn("runTimeoutSeconds", "integer", (col) =>
      col.notNull().check(sql`"runTimeoutSeconds" > 0`),
    )
    // The most the job's model calls may cost over its life, in US dollars, compared exactly with its
    // cost.
    .addColumn("modelBudgetUsd", "numeric", (col) => col.notNull().check(sql`"modelBudgetUsd" > 0`))
    // The job's Batch. Nothing deletes a Batch yet; the Organization's deletion deletes its Batches
    // and jobs in the same statement, by which point no job refers to a Batch.
    .addColumn("batchRowId", "uuid", (col) =>
      col.notNull().references("batch.rowId").onDelete("no action"),
    )
    // Whether the job is in its Batch's current Canary, which runs before the rest are released.
    .addColumn("inCanary", "boolean", (col) => col.notNull().defaultTo(false))
    // What finds the job, groups it and attributes its cost: its Batch's labels with its own, which
    // win where both name a key.
    .addColumn("labels", "jsonb", (col) => col.notNull())
    .addForeignKeyConstraint(
      "job_organizationId_fkey",
      ["organizationId"],
      "organization",
      ["id"],
      (constraint) => constraint.onDelete("cascade"),
    )
    // Adds to a Batch take its lock, so each finds the jobs those before it made; the constraint
    // backs that up.
    .addUniqueConstraint("job_batchRowId_customId_key", ["batchRowId", "customId"])
    // A placement is sent to a Runner, so a job sent has one; a resumed job has its Runner, the one
    // holding its journal, before it is sent.
    .addCheckConstraint("job_placedAt_check", sql`"placedAt" is null or "runnerId" is not null`)
    .addCheckConstraint(
      "job_pendingControl_check",
      sql`("pendingControl" is null) = ("pendingControlRequestedAt" is null)`,
    )
    .addCheckConstraint(
      "job_failureReason_check",
      sql`("failureReason" is not null) = ("status" = 'failed')`,
    )
    .addCheckConstraint(
      "job_failureMessage_check",
      sql`"failureMessage" is null or "failureReason" is not null`,
    )
    .execute();
  // A Runner's sync reads its unfinished jobs; placement reads the queued jobs not yet sent to a
  // Runner in the order they were accepted, which their IDs (UUID v7) follow.
  await schema
    .createIndex("job_runnerId_unfinished_idx")
    .on("job")
    .columns(["runnerId"])
    .where(sql.ref("status"), "in", ["queued", "running"])
    .execute();
  await schema
    .createIndex("job_queued_unsent_idx")
    .on("job")
    .columns(["id"])
    .where(sql.ref("status"), "=", "queued")
    .where(sql.ref("placedAt"), "is", null)
    .execute();
  // Listing jobs by custom ID across Batches.
  await schema.createIndex("job_customId_idx").on("job").column("customId").execute();
  // Listing jobs by label reads `labels @> …`, which this operator class serves.
  await schema
    .createIndex("job_labels_idx")
    .on("job")
    .using("gin")
    .column(sql`${sql.ref("labels")} jsonb_path_ops`)
    .execute();
  // A Batch's summary and its controls read its unfinished jobs; placement, its unfinished Canary.
  await schema
    .createIndex("job_batchRowId_unfinished_idx")
    .on("job")
    .columns(["batchRowId", "inCanary"])
    .where(sql.ref("status"), "in", ["queued", "running", "paused"])
    .execute();
  // Listing a Batch's jobs, newest first.
  await schema
    .createIndex("job_batchRowId_id_idx")
    .on("job")
    .columns(["batchRowId", "id"])
    .execute();

  // A job's own Input files.
  await schema
    .createTable("inputFile")
    .addColumn("jobId", "uuid", (col) => col.notNull())
    // The file's path under the job's `inputs` folder, relative, with folders.
    .addColumn("path", "text", (col) => col.notNull())
    // A job's file refers to its blob, which cannot be deleted while any job's file refers to it.
    .addColumn("sha256", "bytea", (col) =>
      col.notNull().references("blob.sha256").onDelete("restrict"),
    )
    .addPrimaryKeyConstraint("inputFile_pkey", ["jobId", "path"])
    .addForeignKeyConstraint("inputFile_jobId_fkey", ["jobId"], "job", ["id"], (constraint) =>
      constraint.onDelete("cascade"),
    )
    .execute();
  // For the reference check of deleting a blob, and for finding whether any job still uses one.
  await schema.createIndex("inputFile_sha256_idx").on("inputFile").column("sha256").execute();

  // What a job has used so far and what it is doing: one row per job, inserted when the job is
  // accepted and rewritten by every report that changes it. A rewrite avoids new index entries only
  // when the new version fits on the same page, so half of each page is left free: in a probe of
  // 20,000 rows updated 30 times each, 0.2% of updates stayed on their page with Postgres's default
  // of 100 against 97% at 50, and the heap was fifteen times as large.
  await schema
    .createTable("jobProgress")
    .addColumn("jobId", "uuid", (col) => col.primaryKey())
    // The ID of the last journal record stored, 0 before any: a report's events apply only from
    // here, and every answer to a sync or a report carries it.
    .addColumn("position", "bigint", (col) => col.notNull().defaultTo(0))
    // pi's totals per model and per tool, as `Harness.usage()` reports them; read by the report
    // statement, which keeps the larger. Only names and numbers, so `jsonb` holds them, unlike
    // events.
    .addColumn("usage", "jsonb", (col) =>
      col.notNull().defaultTo(sql`'{"models": {}, "tools": {}}'::jsonb`),
    )
    // The sums of `usage`, read by the job's summary and by each end's Usage record. pi computes
    // cost as a float from per-million-token prices, below a microdollar per token, so cost is a
    // `numeric` without a fixed scale: the statement that applies a report adds up its costs per
    // model and per tool as `numeric`, so the total equals the sum of the stored ones, and sums
    // over jobs add no error.
    .addColumn("totalTokens", "bigint", (col) => col.notNull().defaultTo(0))
    .addColumn("costUsd", "numeric", (col) => col.notNull().defaultTo(0))
    // What the job's conversations are doing, as the last report that said gave it; null before
    // any report has given it, and once the job has ended. Read by the job's summary while the job
    // runs. Times inside are the Runner's clock.
    .addColumn("activity", "jsonb")
    .addForeignKeyConstraint("jobProgress_jobId_fkey", ["jobId"], "job", ["id"], (constraint) =>
      constraint.onDelete("cascade"),
    )
    .modifyEnd(sql`with (fillfactor = 50)`)
    .execute();

  // A job's events as the hub stores them from its Runner's reports: one row per job per report
  // applied, holding that report's events for the job as JSON Lines, one `{id, type, record}` per
  // line, each line ending in `\n`, so a page is the rows' text joined. `JSON.stringify` writes no
  // raw newline inside a string, so splitting on `\n` is safe, and writes NUL as `\u0000`, which
  // `text` holds and `jsonb` would refuse. Rows never overlap, so `lastId` orders them, and
  // `lastId > after` also finds the row the position `after` falls inside. IDs have gaps, so a row's
  // events are counted by its lines, not from its IDs; its size is `octet_length(events)`.
  await schema
    .createTable("jobEventChunk")
    .addColumn("jobId", "uuid", (col) => col.notNull())
    .addColumn("lastId", "bigint", (col) => col.notNull())
    // Times inside the events are the Runner's clock.
    .addColumn("events", "text", (col) => col.notNull())
    // The hub's clock when the row was stored.
    .addColumn("storedAt", "timestamptz", (col) => col.notNull())
    .addPrimaryKeyConstraint("jobEventChunk_pkey", ["jobId", "lastId"])
    .addForeignKeyConstraint("jobEventChunk_jobId_fkey", ["jobId"], "job", ["id"], (constraint) =>
      constraint.onDelete("cascade"),
    )
    .execute();

  // What outlives a job's content: a Usage record per end of a job. A job a resume carries on can
  // end more than once, so each end records what the job used since its previous end and the job's
  // totals at that end. It is written once, at the end, and copies what it reports rather than
  // referencing it, since the job's row goes when the job is deleted. Deleting the job marks its
  // latest record, which keeps what became of the job's files on its host.
  await schema
    .createTable("usageRecord")
    // Ends are written one at a time under their job's row lock, so a job's records follow their IDs.
    .addColumn("id", "bigint", (col) => col.primaryKey().generatedAlwaysAsIdentity())
    .addColumn("jobId", "uuid", (col) => col.notNull())
    .addColumn("organizationId", "text", (col) => col.notNull())
    .addColumn("status", sql`agent_batch."jobStatus"`, (col) => col.notNull())
    // Why the job failed: null unless this end is a failure.
    .addColumn("failureReason", sql`agent_batch."jobFailureReason"`)
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
    // What the job used since its previous end, as pi counts it.
    .addColumn("totalTokens", "bigint", (col) => col.notNull())
    .addColumn("costUsd", "numeric", (col) => col.notNull())
    // The job's totals at this end.
    .addColumn("jobTotalTokens", "bigint", (col) => col.notNull())
    .addColumn("jobCostUsd", "numeric", (col) => col.notNull())
    // When the job was deleted, by the hub's clock, on its latest record only; null before.
    .addColumn("deletedAt", "timestamptz")
    // What became of the deleted job's files on its host, and when, by the hub's clock; null while
    // its Runner still owes their deletion, and before the job is deleted.
    .addColumn("hostDeletionOutcome", sql`agent_batch."hostDeletionOutcome"`)
    .addColumn("hostDeletionOutcomeAt", "timestamptz")
    // The job's Batch and labels as it ended, to group cost by after the job is deleted. The Batch
    // is copied rather than referenced, as the Runner is.
    .addColumn("batchRowId", "uuid", (col) => col.notNull())
    .addColumn("labels", "jsonb", (col) => col.notNull())
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
      "usageRecord_failureReason_check",
      sql`("failureReason" is not null) = ("status" = 'failed')`,
    )
    .addCheckConstraint(
      "usageRecord_hostDeletionOutcome_check",
      sql`("hostDeletionOutcome" is null) = ("hostDeletionOutcomeAt" is null)`,
    )
    .execute();
  // A job's Usage records in the order of their ends, for its latest, which a delete marks.
  await schema
    .createIndex("usageRecord_jobId_id_idx")
    .on("usageRecord")
    .columns(["jobId", "id"])
    .execute();
  // A Runner's removal lists its deleted jobs whose files were abandoned.
  await schema
    .createIndex("usageRecord_abandoned_runnerId_idx")
    .on("usageRecord")
    .column("runnerId")
    .where(sql.ref("hostDeletionOutcome"), "=", "abandoned")
    .execute();

  // A deleted job whose files its Runner still owes the deletion of, read by each of that Runner's
  // syncs until the Runner confirms it. It holds the deleted job as it reads meanwhile, so the job's
  // summary needs no Usage record.
  await schema
    .createTable("jobDeletion")
    .addColumn("jobId", "uuid", (col) => col.primaryKey())
    .addColumn("organizationId", "text", (col) => col.notNull())
    .addColumn("runnerId", "text", (col) => col.notNull())
    // The Usage record the delete marked, where the Runner's confirmation records what became of
    // the files.
    .addColumn("usageRecordId", "bigint", (col) => col.notNull())
    // When the job was deleted, by the hub's clock.
    .addColumn("requestedAt", "timestamptz", (col) => col.notNull())
    // The deleted job as it reads while the deletion waits: its final status, why it failed, null
    // unless it did, its times, null for a start it never had, and its totals.
    .addColumn("status", sql`agent_batch."jobStatus"`, (col) => col.notNull())
    .addColumn("failureReason", sql`agent_batch."jobFailureReason"`)
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
    .addCheckConstraint(
      "jobDeletion_failureReason_check",
      sql`("failureReason" is not null) = ("status" = 'failed')`,
    )
    .execute();
  await schema
    .createIndex("jobDeletion_runnerId_idx")
    .on("jobDeletion")
    .column("runnerId")
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  for (const table of [
    "jobDeletion",
    "usageRecord",
    "jobEventChunk",
    "jobProgress",
    "inputFile",
    "job",
    "batchInputFile",
    "batch",
    "organizationBlob",
    "uploadChunk",
    "upload",
    "blob",
    "runner",
    "rateLimit",
    "apikey",
    "invitation",
    "member",
    "organization",
    "verification",
    "account",
    "session",
    "user",
  ]) {
    await schema.dropTable(table).execute();
  }
  for (const type of [
    "batchPausedBy",
    "hostDeletionOutcome",
    "jobFailureReason",
    "jobControl",
    "jobStatus",
  ]) {
    await schema.dropType(type).execute();
  }
}
