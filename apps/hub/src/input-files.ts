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

// Input files' blobs: each file's bytes stored once by the SHA-256 of its original bytes however
// many jobs use it, in chunk rows of a fixed number of original bytes each, recorded with the blob's
// upload and listed to the Runner, each compressed as one zstd frame. A Runner reads a file a chunk
// at a time, so serving a file holds no more than a chunk on either side, and a sync's answer names
// the files without their bytes.
//
// A blob no job uses is deleted with the last job that used it. A submission and a deletion lock
// their blobs in the same order, so whichever comes second sees what the first did: a submission
// waits for the deletion and stores the blob again, and a deletion that waits for a submission finds
// the blob used and keeps it.

/**
 * The original bytes in each chunk of a new blob but its last. Each request for a chunk costs CPU
 * for one chunk's frame, so this bounds the CPU of a request on any plan; a blob already stored
 * keeps the size it was stored with.
 */
const CHUNK_BYTES = 1024 * 1024;

/** zstd's own default level: as small as gzip's or much smaller, for a fraction of its CPU time. */
const ZSTD_LEVEL = 3;

/** A submission's Input files, hashed, compressed where the hub held no blob of them when it looked. */
export interface PreparedInputFiles {
  readonly files: readonly { name: InputFileName; sha256: BlobSha256 }[];
  /** Each distinct blob once, in the order of its address, compressed if it was not stored. */
  readonly blobs: readonly { sha256: BlobSha256; bytes: Buffer; chunks?: Buffer[] }[];
}

/**
 * Hashes the Input files `files`, and compresses those whose blob the hub did not hold when it
 * looked, before the submission's transaction, so its locks are not held while it compresses.
 */
export function prepareInputFiles(files: JobSubmission["inputFiles"]) {
  const named = Record.collect(files, (name, file) => {
    const bytes = Buffer.from(file.content, "utf8");
    return {
      name: InputFileName(name),
      bytes,
      sha256: BlobSha256(createHash("sha256").update(bytes).digest()),
    };
  });
  const distinct = new Map(named.map((file) => [file.sha256.toString("hex"), file]));
  const blobs = [...distinct.values()].toSorted((a, b) => Buffer.compare(a.sha256, b.sha256));
  return Effect.gen(function* () {
    if (blobs.length === 0) return { files: [], blobs: [] } satisfies PreparedInputFiles;
    const database = yield* Db;
    // A look without a lock: a blob deleted meanwhile is stored again, and compressed then.
    const stored = yield* database.query((db) =>
      db
        .selectFrom("blob")
        .select("sha256")
        .where("sha256", "=", (eb) => eb.fn.any(eb.val(blobs.map((blob) => blob.sha256))))
        .execute(),
    );
    const known = new Set(stored.map((row) => row.sha256.toString("hex")));
    return {
      files: named.map(({ name, sha256 }) => ({ name, sha256 })),
      blobs: blobs.map(({ sha256, bytes }) =>
        known.has(sha256.toString("hex"))
          ? { sha256, bytes }
          : { sha256, bytes, chunks: compress(bytes) },
      ),
    } satisfies PreparedInputFiles;
  });
}

/** `bytes` in chunks of `CHUNK_BYTES` original bytes, each one zstd frame; none for no bytes. */
function compress(bytes: Buffer): Buffer[] {
  const chunks: Buffer[] = [];
  for (let start = 0; start < bytes.length; start += CHUNK_BYTES) {
    chunks.push(
      zstdCompressSync(bytes.subarray(start, start + CHUNK_BYTES), {
        params: { [constants.ZSTD_c_compressionLevel]: ZSTD_LEVEL },
      }),
    );
  }
  return chunks;
}

/**
 * Stores the job `jobId`'s Input files, `prepared`, in the transaction `trx`: the blobs the hub does
 * not hold yet, and the job's reference to each file's blob. A blob already stored is locked
 * against deletion until the transaction ends, rather than written again.
 */
export async function storeInputFiles(
  trx: Kysely<Database>,
  jobId: JobId,
  prepared: PreparedInputFiles,
): Promise<void> {
  if (prepared.blobs.length === 0) return;
  // One statement takes every lock on the submission's blobs, row by row in the order of their
  // address, as a deletion takes its locks, so the two never wait for each other in turn; Postgres
  // reads the rows of `values` in their order. A stored blob is locked by the update that
  // `where false` keeps from happening: an update's lock on a column no key holds, which a
  // deletion's `for update` waits for, without writing a new version of the row, which
  // `set ... returning (xmax = 0)` would for each reuse. So only blobs it inserted come back. A blob
  // whose deletion it waited for is gone once that commits, and is inserted afresh. The lock also
  // conflicts with itself, so submissions that share a stored blob wait for each other to commit;
  // Postgres 19's `on conflict do select for key share` would lock without that.
  const inserted = await trx
    .insertInto("blob")
    .values(
      prepared.blobs.map(({ sha256, bytes }) => ({ sha256, originalSizeBytes: bytes.length })),
    )
    .onConflict((conflict) =>
      conflict
        .column("sha256")
        .doUpdateSet((eb) => ({ originalSizeBytes: eb.ref("excluded.originalSizeBytes") }))
        .where((eb) => eb.lit(false)),
    )
    .returning("sha256")
    .execute();
  const created = new Set(inserted.map((row) => row.sha256.toString("hex")));
  for (const blob of prepared.blobs) {
    if (!created.has(blob.sha256.toString("hex"))) continue;
    const uploadId = UploadId(randomUUID());
    await trx
      .insertInto("upload")
      .values({ id: uploadId, sha256: blob.sha256, chunkBytes: CHUNK_BYTES })
      .execute();
    // Compressed here only when the blob was stored when the submission looked and deleted since.
    const chunks: NewUploadChunk[] = (blob.chunks ?? compress(blob.bytes)).map((bytes, index) => ({
      uploadId,
      index: UploadChunkIndex(index),
      bytes,
    }));
    // An empty file has no chunks. pg sends a Buffer parameter as binary, so the bytes cross to
    // Postgres as they are.
    if (chunks.length > 0) await trx.insertInto("uploadChunk").values(chunks).execute();
  }
  await trx
    .insertInto("inputFile")
    .values(prepared.files.map(({ name, sha256 }) => ({ jobId, name, sha256 })))
    .execute();
}

/**
 * Locks the blobs of the job `jobId`'s Input files in the transaction `trx`, before the job is
 * deleted, so no submission can start using them unseen; returns their addresses, for
 * `deleteUnusedBlobs` once the job is gone.
 */
export async function lockInputFileBlobs(
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
 * Deletes the blobs at `addresses`, locked by `lockInputFileBlobs`, that no job's Input file uses
 * any longer, with their uploads and chunks. A statement of its own after the job's deletion: it
 * must see the job's files gone, and a statement sees only what statements before it did.
 */
export async function deleteUnusedBlobs(
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
