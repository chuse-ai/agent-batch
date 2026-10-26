import { Buffer } from "node:buffer";
import { createHash, randomUUID } from "node:crypto";
import { constants, zstdCompressSync } from "node:zlib";

import type { JobSubmission } from "contract";
import { Effect, Record } from "effect";
import type { Kysely } from "kysely";

import { Db } from "./db/database.ts";
import {
  BlobSha256,
  type Database,
  InputFileName,
  type JobId,
  type NewUploadChunk,
  type RunnerId,
  UploadChunkIndex,
  UploadId,
} from "./db/types.ts";

// Input files' content, stored once by the SHA-256 of its original bytes however many jobs use it,
// in chunk rows of a fixed number of original bytes each, recorded with the content and listed to
// the Runner, each compressed as one zstd frame. A Runner reads a file a chunk at a time, so serving
// a file holds no more than a chunk on either side, and a sync's answer names the files without
// their bytes.
//
// Content no job uses is deleted with the last job that used it. A submission and a deletion of the
// same content lock its row, so whichever comes second sees what the first did: a submission waits
// for the deletion and stores the content again, and a deletion that waits for a submission finds
// the content used and keeps it.

/**
 * The original bytes in each chunk of new content but its last. Each request for a chunk costs CPU
 * for one chunk's frame, so this bounds the CPU of a request on any plan; content already stored
 * keeps the size it was stored with.
 */
const CHUNK_BYTES = 1024 * 1024;

/** zstd's own default level: as small as gzip's or much smaller, for a fraction of its CPU time. */
const ZSTD_LEVEL = 3;

/**
 * Stores the job `jobId`'s Input files `files` in the transaction `trx`: the content no stored blob
 * has yet, compressed, and the job's reference to each file's content. Content already stored is
 * locked against deletion, rather than compressed and written again.
 */
export async function storeInputFiles(
  trx: Kysely<Database>,
  jobId: JobId,
  files: JobSubmission["inputFiles"],
): Promise<void> {
  // Rows are locked in the order of the content's address, the stored ones by the select and the new
  // ones as they are inserted, so two submissions sharing content do not each wait for the other.
  // A deletion of a job that stored some of the new ones meanwhile can still deadlock with this
  // transaction; Postgres then fails one of them, and that request can be sent again.
  const contents = Record.collect(files, (name, file) => {
    const bytes = Buffer.from(file.content, "utf8");
    return { name, bytes, sha256: BlobSha256(createHash("sha256").update(bytes).digest()) };
  }).toSorted((a, b) => Buffer.compare(a.sha256, b.sha256));
  if (contents.length === 0) return;
  const stored = await trx
    .selectFrom("blob")
    .select("sha256")
    .where("sha256", "=", (eb) => eb.fn.any(eb.val(contents.map((content) => content.sha256))))
    .orderBy("sha256")
    .forKeyShare()
    .execute();
  const known = new Set(stored.map((row) => row.sha256.toString("hex")));
  for (const content of contents) {
    const hex = content.sha256.toString("hex");
    if (known.has(hex)) continue;
    known.add(hex);
    // A concurrent submission of the same new content may store it first; this one then writes
    // none of its own.
    const inserted = await trx
      .insertInto("blob")
      .values({ sha256: content.sha256, originalSizeBytes: content.bytes.length })
      .onConflict((conflict) => conflict.column("sha256").doNothing())
      .returning("sha256")
      .executeTakeFirst();
    if (!inserted) continue;
    const uploadId = UploadId(randomUUID());
    await trx
      .insertInto("upload")
      .values({ id: uploadId, sha256: content.sha256, chunkBytes: CHUNK_BYTES })
      .execute();
    const chunks: NewUploadChunk[] = [];
    for (let start = 0; start < content.bytes.length; start += CHUNK_BYTES) {
      chunks.push({
        uploadId,
        index: UploadChunkIndex(chunks.length),
        bytes: zstdCompressSync(content.bytes.subarray(start, start + CHUNK_BYTES), {
          params: { [constants.ZSTD_c_compressionLevel]: ZSTD_LEVEL },
        }),
      });
    }
    // An empty file has no chunks. pg sends a Buffer parameter as binary, so the bytes cross to
    // Postgres as they are.
    if (chunks.length > 0) await trx.insertInto("uploadChunk").values(chunks).execute();
  }
  await trx
    .insertInto("inputFile")
    .values(
      contents.map((content) => ({
        jobId,
        name: InputFileName(content.name),
        sha256: content.sha256,
      })),
    )
    .execute();
}

/**
 * Locks the content of the job `jobId`'s Input files in the transaction `trx`, before the job is
 * deleted, so no submission can start using it unseen; returns its addresses, for
 * `deleteUnusedContent` once the job is gone.
 */
export async function lockInputFileContent(
  trx: Kysely<Database>,
  jobId: JobId,
): Promise<BlobSha256[]> {
  const rows = await trx
    .selectFrom("blob")
    .select("sha256")
    .where("sha256", "in", (eb) =>
      eb.selectFrom("inputFile").select("inputFile.sha256").where("inputFile.jobId", "=", jobId),
    )
    .orderBy("sha256")
    .forUpdate()
    .execute();
  return rows.map((row) => row.sha256);
}

/**
 * Deletes the content at `addresses`, locked by `lockInputFileContent`, that no job's Input file
 * uses any longer, with its chunks. A statement of its own after the job's deletion: it must see the
 * job's files gone, and a statement sees only what statements before it did.
 */
export async function deleteUnusedContent(
  trx: Kysely<Database>,
  addresses: BlobSha256[],
): Promise<void> {
  if (addresses.length === 0) return;
  await trx
    .deleteFrom("blob")
    .where("sha256", "=", (eb) => eb.fn.any(eb.val(addresses)))
    .where((eb) =>
      eb.not(
        eb.exists(
          eb
            .selectFrom("inputFile")
            .select("inputFile.jobId")
            .whereRef("inputFile.sha256", "=", "blob.sha256"),
        ),
      ),
    )
    .execute();
}

/**
 * Chunk `index` of the job `jobId`'s Input file `name`, one zstd frame, for the Runner `runnerId`
 * the job is placed on; nothing when there is no such chunk, file or job, or the job is not placed
 * on that Runner.
 */
export function readInputFileChunk(
  runnerId: RunnerId,
  jobId: JobId,
  name: InputFileName,
  index: UploadChunkIndex,
) {
  return Db.use((database) =>
    database.query((db) =>
      db
        .selectFrom("inputFile")
        .innerJoin("job", "job.id", "inputFile.jobId")
        .innerJoin("upload", "upload.sha256", "inputFile.sha256")
        .innerJoin("uploadChunk", "uploadChunk.uploadId", "upload.id")
        // A bytea crosses from Postgres as hex text, twice its size; base64 is a third larger.
        .select((eb) =>
          eb.fn<string>("encode", ["uploadChunk.bytes", eb.val("base64")]).as("bytes"),
        )
        .where("inputFile.jobId", "=", jobId)
        .where("inputFile.name", "=", name)
        .where("job.runnerId", "=", runnerId)
        .where("uploadChunk.index", "=", index)
        .executeTakeFirst(),
    ),
  ).pipe(Effect.map((row) => (row ? Buffer.from(row.bytes, "base64") : undefined)));
}
