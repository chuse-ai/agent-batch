import { Buffer } from "node:buffer";
import { createHash, randomUUID } from "node:crypto";
import { zstdCompressSync } from "node:zlib";

import type { JobSubmission } from "contract";
import { Effect, Record } from "effect";
import type { Kysely } from "kysely";

import { Db } from "./db/database.ts";
import {
  type BatchId,
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
// many jobs use it, in chunk rows of a fixed number of original bytes each, recorded with the
// blob's upload and listed to the Runner, each compressed as one zstd frame. A Runner reads a file
// a chunk at a time, so serving a file holds no more than a chunk on either side, and a sync's
// answer names the files without their bytes.
//
// A blob no job uses is deleted with the last job that used it, unless a Batch's agent uses it; a
// Batch keeps its agent's files, which no job's deletion locks or deletes. A submission and a deletion lock
// their blobs in the same order, so whichever comes second sees what the first did: a submission
// waits for the deletion and stores the blob again, and a deletion that waits for a submission
// finds the blob used and keeps it.

/**
 * The original bytes in each chunk of a new blob but its last. Each request for a chunk costs CPU
 * for one chunk's frame, so this bounds the CPU of a request on any plan; a blob already stored
 * keeps the size it was stored with.
 */
const CHUNK_BYTES = 1024 * 1024;

/** A blob a submission's Input files need, compressed if the hub held none of it when it looked. */
interface PreparedBlob {
  readonly sha256: BlobSha256;
  readonly bytes: Buffer;
  readonly chunks?: Buffer[];
}

/** One job's Input files, hashed, each with the blob it needs. */
export interface PreparedInputFiles {
  readonly files: readonly { name: InputFileName; blob: PreparedBlob }[];
}

/**
 * Hashes each job's Input files in `filesOfJobs`, and compresses those whose blob the hub did not
 * hold when it looked, before the submission's transaction, so its locks are not held while it
 * compresses; one look for every job, and each blob compressed once however many jobs share it.
 */
export function prepareInputFiles(filesOfJobs: readonly JobSubmission["inputFiles"][]) {
  // Each distinct blob once, which every file with its bytes shares, compressed below if it was
  // not stored.
  const blobs = new Map<string, { sha256: BlobSha256; bytes: Buffer; chunks?: Buffer[] }>();
  const prepared = filesOfJobs.map((files): PreparedInputFiles => ({
    files: Record.collect(files, (name, file) => {
      const bytes = Buffer.from(file.content, "utf8");
      const sha256 = BlobSha256(createHash("sha256").update(bytes).digest());
      const hex = sha256.toString("hex");
      const blob = blobs.get(hex) ?? { sha256, bytes };
      blobs.set(hex, blob);
      return { name: InputFileName(name), blob };
    }),
  }));
  return Effect.gen(function* () {
    if (blobs.size === 0) return prepared;
    const database = yield* Db;
    // A look without a lock: a blob deleted meanwhile is stored again, and compressed then.
    const stored = yield* database.query((db) =>
      db
        .selectFrom("blob")
        .select("sha256")
        .where("sha256", "=", (eb) => eb.fn.any(eb.val([...blobs.values()].map((b) => b.sha256))))
        .execute(),
    );
    const known = new Set(stored.map((row) => row.sha256.toString("hex")));
    for (const [hex, blob] of blobs) if (!known.has(hex)) blob.chunks = compress(blob.bytes);
    return prepared;
  });
}

/**
 * `bytes` in chunks of `CHUNK_BYTES` original bytes, each one zstd frame at zstd's default level:
 * as small as gzip's or much smaller, for a fraction of its CPU time. None for no bytes.
 */
function compress(bytes: Buffer): Buffer[] {
  const chunks: Buffer[] = [];
  for (let start = 0; start < bytes.length; start += CHUNK_BYTES) {
    chunks.push(zstdCompressSync(bytes.subarray(start, start + CHUNK_BYTES)));
  }
  return chunks;
}

/**
 * Stores the Input files of each job in `jobs` in the transaction `trx`: the blobs the hub does not
 * hold yet, each once however many of the jobs share it, and each job's reference to each of its
 * files' blobs. A blob already stored is locked against deletion until the transaction ends, rather
 * than written again.
 */
export async function storeInputFiles(
  trx: Kysely<Database>,
  jobs: readonly { jobId: JobId; prepared: PreparedInputFiles }[],
): Promise<void> {
  await storeBlobs(
    trx,
    jobs.map(({ prepared }) => prepared),
  );
  const rows = jobs.flatMap(({ jobId, prepared }) =>
    prepared.files.map(({ name, blob }) => ({ jobId, name, sha256: blob.sha256 })),
  );
  if (rows.length > 0) await trx.insertInto("inputFile").values(rows).execute();
}

/**
 * Stores the Input files of the Batch `batchId`'s agent, `prepared`, in the transaction `trx`, as
 * `storeInputFiles` stores a job's: every job of the Batch gets them beside its own.
 */
export async function storeBatchInputFiles(
  trx: Kysely<Database>,
  batchId: BatchId,
  prepared: PreparedInputFiles,
): Promise<void> {
  await storeBlobs(trx, [prepared]);
  const rows = prepared.files.map(({ name, blob }) => ({ batchId, name, sha256: blob.sha256 }));
  if (rows.length > 0) await trx.insertInto("batchInputFile").values(rows).execute();
}

/**
 * Stores, in the transaction `trx`, the blobs the files in `prepared` need that the hub does not
 * hold yet, each once however many files share it, and locks those it holds against deletion until
 * the transaction ends, rather than writing them again.
 */
async function storeBlobs(trx: Kysely<Database>, prepared: readonly PreparedInputFiles[]) {
  const blobs = [
    ...new Map(
      prepared.flatMap(({ files }) =>
        files.map(({ blob }) => [blob.sha256.toString("hex"), blob] as const),
      ),
    ).values(),
  ].toSorted((a, b) => Buffer.compare(a.sha256, b.sha256));
  if (blobs.length === 0) return;
  // One statement takes every lock on the submission's blobs, row by row in the order of their
  // address, as a deletion takes its locks, so the two never wait for each other in turn; Postgres
  // reads the rows of `values` in their order. A stored blob is locked by the update that
  // `where false` keeps from happening: an update's lock on a column no key holds, which a
  // deletion's `for update` waits for, without writing a new version of the row, which
  // `set ... returning (xmax = 0)` would for each reuse. So only blobs it inserted come back. A
  // blob whose deletion it waited for is gone once that commits, and is inserted afresh. The lock
  // also conflicts with itself, so submissions that share a stored blob wait for each other to
  // commit; Postgres 19's `on conflict do select for key share` would lock without that.
  const inserted = await trx
    .insertInto("blob")
    .values(blobs.map(({ sha256, bytes }) => ({ sha256, originalSizeBytes: bytes.length })))
    .onConflict((conflict) =>
      conflict
        .column("sha256")
        .doUpdateSet((eb) => ({ originalSizeBytes: eb.ref("excluded.originalSizeBytes") }))
        .where((eb) => eb.lit(false)),
    )
    .returning("sha256")
    .execute();
  const created = new Set(inserted.map((row) => row.sha256.toString("hex")));
  for (const blob of blobs) {
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
 * Deletes the blobs at `addresses`, locked by `lockInputFileBlobs`, that no Input file of a job or
 * of a Batch's agent uses any longer, with their uploads and chunks. A statement of its own after
 * the job's deletion: it must see the job's files gone, and a statement sees only what statements
 * before it did.
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
        eb.or([
          eb.exists(
            eb
              .selectFrom("inputFile")
              .select("inputFile.jobId")
              .whereRef("inputFile.sha256", "=", "blob.sha256"),
          ),
          eb.exists(
            eb
              .selectFrom("batchInputFile")
              .select("batchInputFile.batchId")
              .whereRef("batchInputFile.sha256", "=", "blob.sha256"),
          ),
        ]),
      ),
    )
    .execute();
}

/**
 * Chunk `index` of the job `jobId`'s Input file `name`, its own or its Batch's agent's, one zstd
 * frame, for the Runner `runnerId` the job is placed on; nothing when there is no such chunk, file
 * or job, or the job is not placed on that Runner.
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
        .selectFrom("job")
        // A job's file names and its Batch's never overlap, as adding a job refuses one that would.
        .innerJoinLateral(
          (eb) =>
            eb
              .selectFrom("inputFile")
              .select("inputFile.sha256")
              .whereRef("inputFile.jobId", "=", "job.id")
              .where("inputFile.name", "=", name)
              .unionAll(
                eb
                  .selectFrom("batchInputFile")
                  .select("batchInputFile.sha256")
                  .whereRef("batchInputFile.batchId", "=", "job.batchId")
                  .where("batchInputFile.name", "=", name),
              )
              .as("file"),
          (join) => join.onTrue(),
        )
        .innerJoin("upload", "upload.sha256", "file.sha256")
        .innerJoin("uploadChunk", "uploadChunk.uploadId", "upload.id")
        // A bytea crosses from Postgres as hex text, twice its size; base64 is a third larger.
        .select((eb) =>
          eb.fn<string>("encode", ["uploadChunk.bytes", eb.val("base64")]).as("bytes"),
        )
        .where("job.id", "=", jobId)
        .where("job.runnerId", "=", runnerId)
        .where("uploadChunk.index", "=", index)
        .executeTakeFirst(),
    ),
  ).pipe(Effect.map((row) => (row ? Buffer.from(row.bytes, "base64") : undefined)));
}
