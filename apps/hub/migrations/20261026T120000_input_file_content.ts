import { createHash, randomUUID } from "node:crypto";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";

import { type Kysely, sql } from "kysely";

// Input files' content moves out of `inputFile` into blobs stored once by their SHA-256: a file
// many jobs share is stored once, and its bytes sit in chunk rows of 1 MiB of original bytes, each
// compressed as one zstd frame at zstd's default level, which a Runner downloads one at a time.
//
// A blob's bytes belong to the upload that wrote them, and the blob is reached through the upload:
// so an upload can write its chunks before its blob is known and verified, and publishing it
// copies no bytes. Today every upload is a submission's inline file, written whole and published in
// the same transaction.

/** The original bytes in each chunk but a file's last, for the blobs this migration stores. */
const CHUNK_BYTES = 1024 * 1024;

export async function up(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
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
    // The blob the upload published: deleting the blob deletes its upload, and with it the bytes.
    .addColumn("sha256", "bytea", (col) =>
      col.notNull().references("blob.sha256").onDelete("cascade"),
    )
    // How many original bytes each of its chunks holds but the last, chosen as the upload starts,
    // so the hub can choose another for new blobs without rewriting what it stored.
    .addColumn("chunkBytes", "integer", (col) => col.notNull().check(sql`"chunkBytes" > 0`))
    // A blob's bytes are those of one upload, through which it is read.
    .addUniqueConstraint("upload_sha256_key", ["sha256"])
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

  // A job's file refers to its blob, which cannot be deleted while any job's file refers to it.
  await schema
    .alterTable("inputFile")
    .addColumn("sha256", "bytea", (col) => col.references("blob.sha256").onDelete("restrict"))
    .execute();
  await copyContentToBlobs(db);
  await schema
    .alterTable("inputFile")
    .alterColumn("sha256", (col) => col.setNotNull())
    .dropColumn("content")
    .execute();
  // For the reference check of deleting a blob, and for finding whether any job still uses one.
  await schema.createIndex("inputFile_sha256_idx").on("inputFile").column("sha256").execute();
}

/**
 * Stores each job's inline Input file as a blob and refers to it, one file at a time: a row holds a
 * whole file, and node-postgres holds every row a query returns, so a batch of large files could
 * outgrow the hub's memory as it starts.
 */
async function copyContentToBlobs(db: Kysely<unknown>) {
  let after = { jobId: "00000000-0000-0000-0000-000000000000", name: "" };
  for (;;) {
    const { rows } = await sql<{ jobId: string; name: string; content: string }>`
      select "jobId", name, content from agent_batch."inputFile"
      where ("jobId", name) > (${after.jobId}::uuid, ${after.name})
      order by "jobId", name
      limit 1
    `.execute(db);
    const [row] = rows;
    if (!row) return;
    const bytes = Buffer.from(row.content, "utf8");
    const sha256 = createHash("sha256").update(bytes).digest();
    const { rows: inserted } = await sql`
      insert into agent_batch.blob (sha256, "originalSizeBytes")
      values (${sha256}, ${bytes.length})
      on conflict do nothing
      returning sha256
    `.execute(db);
    if (inserted.length > 0) {
      const uploadId = randomUUID();
      await sql`
        insert into agent_batch.upload (id, sha256, "chunkBytes")
        values (${uploadId}, ${sha256}, ${CHUNK_BYTES})
      `.execute(db);
      for (let index = 0; index * CHUNK_BYTES < bytes.length; index++) {
        const chunk = zstdCompressSync(
          bytes.subarray(index * CHUNK_BYTES, (index + 1) * CHUNK_BYTES),
        );
        await sql`
          insert into agent_batch."uploadChunk" ("uploadId", index, bytes)
          values (${uploadId}, ${index}, ${chunk})
        `.execute(db);
      }
    }
    await sql`
      update agent_batch."inputFile" set sha256 = ${sha256}
      where "jobId" = ${row.jobId} and name = ${row.name}
    `.execute(db);
    after = { jobId: row.jobId, name: row.name };
  }
}

export async function down(db: Kysely<unknown>): Promise<void> {
  const schema = db.schema.withSchema("agent_batch");
  await schema.dropIndex("inputFile_sha256_idx").execute();
  await schema.alterTable("inputFile").addColumn("content", "text").execute();
  let after = { jobId: "00000000-0000-0000-0000-000000000000", name: "" };
  for (;;) {
    const { rows } = await sql<{ jobId: string; name: string; uploadId: string }>`
      select f."jobId", f.name, u.id as "uploadId"
      from agent_batch."inputFile" f join agent_batch.upload u on u.sha256 = f.sha256
      where (f."jobId", f.name) > (${after.jobId}::uuid, ${after.name})
      order by f."jobId", f.name
      limit 1
    `.execute(db);
    const [row] = rows;
    if (!row) break;
    const { rows: chunks } = await sql<{ bytes: Buffer }>`
      select bytes from agent_batch."uploadChunk" where "uploadId" = ${row.uploadId} order by index
    `.execute(db);
    const content = Buffer.concat(chunks.map((chunk) => zstdDecompressSync(chunk.bytes)));
    await sql`
      update agent_batch."inputFile" set content = ${content.toString("utf8")}
      where "jobId" = ${row.jobId} and name = ${row.name}
    `.execute(db);
    after = row;
  }
  await schema
    .alterTable("inputFile")
    .alterColumn("content", (col) => col.setNotNull())
    .dropColumn("sha256")
    .execute();
  await schema.dropTable("uploadChunk").execute();
  await schema.dropTable("upload").execute();
  await schema.dropTable("blob").execute();
}
