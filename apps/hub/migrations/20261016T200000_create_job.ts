import { type Kysely, sql } from "kysely";

// Jobs and their input files. A job belongs to the Organization (ADR 0007). Its submitter is copied
// rather than referenced: Better Auth deletes a key's row when the key is revoked or expires.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema
    .createType("jobState")
    .asEnum(["queued", "running", "paused", "succeeded", "failed", "cancelled"])
    .execute();
  await schema
    .createTable("job")
    .addColumn("id", "uuid", (col) => col.primaryKey())
    .addColumn("organizationId", "text", (col) => col.notNull())
    .addColumn("state", sql`agent_batch."jobState"`, (col) => col.notNull())
    .addColumn("prompt", "text", (col) => col.notNull())
    .addColumn("outputContract", "jsonb", (col) => col.notNull())
    .addColumn("idempotencyKey", "text")
    // SHA-256 of the submission's RFC 8785 canonical JSON without the key, in hex: replays compare it.
    .addColumn("contentHash", "text", (col) => col.notNull())
    .addColumn("submitterKeyId", "text", (col) => col.notNull())
    .addColumn("submitterKeyName", "text")
    .addColumn("submitterMemberId", "text", (col) => col.notNull())
    .addColumn("createdAt", "timestamptz", (col) => col.notNull())
    .addForeignKeyConstraint(
      "job_organizationId_fkey",
      ["organizationId"],
      "organization",
      ["id"],
      (constraint) => constraint.onDelete("cascade"),
    )
    // Accepting a job inserts it under this constraint, so a concurrent duplicate waits for the first.
    .addUniqueConstraint("job_organizationId_idempotencyKey_key", [
      "organizationId",
      "idempotencyKey",
    ])
    .execute();
  await schema
    .createTable("inputFile")
    .addColumn("jobId", "uuid", (col) => col.notNull())
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("content", "text", (col) => col.notNull())
    .addPrimaryKeyConstraint("inputFile_pkey", ["jobId", "name"])
    .addForeignKeyConstraint("inputFile_jobId_fkey", ["jobId"], "job", ["id"], (constraint) =>
      constraint.onDelete("cascade"),
    )
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.dropTable("inputFile").execute();
  await schema.dropTable("job").execute();
  await schema.dropType("jobState").execute();
}
