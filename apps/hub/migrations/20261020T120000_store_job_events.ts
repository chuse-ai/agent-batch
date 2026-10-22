import { type Kysely, sql } from "kysely";

// A job's events as the hub stores them from its Runner's reports, and what the job has used so far.
// Both go with their job: deleting the job deletes them, and the Usage record that outlives it copies
// the totals rather than referencing them.

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  // One row per job, inserted when the job is accepted and rewritten by every report that changes
  // it. A rewrite avoids new index entries only when the new version fits on the same page, so half
  // of each page is left free: in a probe of 20,000 rows updated 30 times each, 0.2% of updates
  // stayed on their page with Postgres's default of 100 against 97% at 50, and the heap was fifteen
  // times as large.
  await schema
    .createTable("jobProgress")
    .addColumn("jobId", "uuid", (col) => col.primaryKey())
    // The ID of the last journal record stored, 0 before any: a report's events apply only from
    // here, and every answer to a sync or a report carries it.
    .addColumn("position", "bigint", (col) => col.notNull().defaultTo(0))
    // pi's totals per model and per tool, as `Harness.usage()` reports them; read by the report
    // statement, which keeps the larger, and by the Usage record. Only names and numbers, so
    // `jsonb` holds them, unlike events.
    .addColumn("usage", "jsonb", (col) =>
      col.notNull().defaultTo(sql`'{"models": {}, "tools": {}}'::jsonb`),
    )
    // The sums of `usage`, read by the job's summary, Batch totals and the monthly limit. pi
    // computes cost as a float from per-million-token prices, below a microdollar per token, so cost
    // is a `numeric` without a fixed scale: the statement that applies a report adds up its costs per
    // model and per tool as `numeric`, so the total equals the sum of the stored ones, and sums over
    // jobs add no error.
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
  // One row per job per report applied, holding that report's events for the job as JSON Lines: one
  // `{id, type, record}` per line, each line ending in `\n`, so a page or a segment is the rows'
  // text joined. `JSON.stringify` writes no raw newline inside a string, so splitting on `\n` is
  // safe, and writes NUL as `\u0000`, which `text` holds and `jsonb` would refuse. Rows never
  // overlap, so `lastId` orders them as `firstId` would, and `lastId > after` also finds the row the
  // position `after` falls inside. IDs have gaps, so a row's events are counted by its lines, not
  // from its IDs; its size is `octet_length(events)`.
  await schema
    .createTable("jobEventChunk")
    .addColumn("jobId", "uuid", (col) => col.notNull())
    // A segment written from rows records the first row's as its own.
    .addColumn("firstId", "bigint", (col) => col.notNull())
    .addColumn("lastId", "bigint", (col) => col.notNull())
    // Times inside the events are the Runner's clock.
    .addColumn("events", "text", (col) => col.notNull())
    // The hub's clock when the row was stored: planned event compaction writes a job's segment after a while without new
    // events, and watches the age of the oldest event not yet written.
    .addColumn("storedAt", "timestamptz", (col) => col.notNull())
    .addPrimaryKeyConstraint("jobEventChunk_pkey", ["jobId", "lastId"])
    .addForeignKeyConstraint("jobEventChunk_jobId_fkey", ["jobId"], "job", ["id"], (constraint) =>
      constraint.onDelete("cascade"),
    )
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.dropTable("jobEventChunk").execute();
  await schema.dropTable("jobProgress").execute();
}
