import { Buffer } from "node:buffer";

import type { JobSubmission } from "contract";
import { Effect } from "effect";
import type { Kysely } from "kysely";

import { digestOf, selectHeldBlobs, sha256Of, sizeProblem } from "./blobs.ts";
import { Db, jsonRows } from "./db/database.ts";
import {
  type BlobSha256,
  type Database,
  InputFilePath,
  type JobId,
  type OrganizationId,
  type RunnerId,
  type UploadChunkIndex,
} from "./db/types.ts";

// Jobs' Input files: each a path in the job's `inputs` folder and the blob it holds, uploaded
// before the job (ADR 0035) and stored once however many jobs use it (ADR 0031). A Runner reads a
// file a chunk at a time, so serving a file holds no more than a chunk on either side, and a sync's
// answer names the files without their bytes.
//
// A submission and a deletion lock their blobs in the same order, so whichever comes second sees
// what the first did: a submission that waits for a deletion finds the blob gone and refuses the
// file, and a deletion that waits for a submission finds the blob used and keeps it.

/**
 * Locks, in the transaction `trx`, the blobs of `files` that the Organization `organizationId`
 * holds, against deletion until the transaction ends, and answers each one's size by digest. Its
 * key-share lock, taken in the order of the blobs' addresses as a deletion takes its own, lets
 * submissions that share a blob go on together.
 */
export async function lockHeldBlobs(
  trx: Kysely<Database>,
  organizationId: OrganizationId,
  files: readonly { digest: string }[],
): Promise<Map<string, number>> {
  if (files.length === 0) return new Map();
  const rows = await selectHeldBlobs(
    trx,
    organizationId,
    files.map(({ digest }) => digest),
  )
    .orderBy("sha256")
    .forKeyShare()
    .execute();
  return new Map(rows.map((row) => [digestOf(row.sha256), row.originalSizeBytes]));
}

/**
 * What is wrong with the files of `jobs`, given the sizes of the blobs their Organization holds,
 * `held`, from `lockHeldBlobs`: a Blob it does not hold, and a size other than its Blob's.
 */
export function unheldFileProblems(
  jobs: readonly JobSubmission[],
  held: ReadonlyMap<string, number>,
) {
  return jobs.flatMap((job, index) =>
    job.files.flatMap(({ digest, sizeBytes }, fileIndex) => {
      const size = held.get(digest);
      if (size === undefined) {
        return [
          {
            message:
              "Must be a Blob the Organization uploaded in the last seven days, or that one of its jobs uses",
            path: ["jobs", index, "files", fileIndex, "digest"],
          },
        ];
      }
      const message = sizeProblem(size, sizeBytes);
      return message === undefined
        ? []
        : [{ message, path: ["jobs", index, "files", fileIndex, "sizeBytes"] }];
    }),
  );
}

/**
 * Records, in the transaction `trx`, each Input file of each job in `jobs`, as a reference to the
 * blob it names, which `lockHeldBlobs` locked.
 */
export async function storeInputFiles(
  trx: Kysely<Database>,
  jobs: readonly { jobId: JobId; files: JobSubmission["files"] }[],
): Promise<void> {
  const rows = jobs.flatMap(({ jobId, files }) =>
    files.map(({ path, digest }) => ({
      jobId,
      path: InputFilePath(path),
      sha256: sha256Of(digest).toString("hex"),
    })),
  );
  if (rows.length === 0) return;
  await trx
    .insertInto("inputFile")
    .columns(["jobId", "path", "sha256"])
    .expression(
      trx
        .selectFrom((eb) => jsonRows(eb, rows))
        .select((eb) => [
          eb.cast<JobId>(eb.ref("row.value", "->>").key("jobId"), "uuid").as("jobId"),
          eb.cast<InputFilePath>(eb.ref("row.value", "->>").key("path"), "text").as("path"),
          eb
            .fn<BlobSha256>("decode", [eb.ref("row.value", "->>").key("sha256"), eb.val("hex")])
            .as("sha256"),
        ]),
    )
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
 * Chunk `index` of the job `jobId`'s Input file at `path`, its own or its Batch's agent's, one zstd
 * frame, for the Runner `runnerId` the job is placed on; nothing when there is no such chunk, file
 * or job, or the job is not placed on that Runner.
 */
export function readInputFileChunk(
  runnerId: RunnerId,
  jobId: JobId,
  path: InputFilePath,
  index: UploadChunkIndex,
) {
  return Db.use((database) =>
    database.query((db) =>
      db
        .selectFrom("job")
        // A Batch's agent has no Input files yet, so a job's paths cannot collide with its Batch's;
        // when it has, adding a job must refuse a path its Batch has.
        .innerJoinLateral(
          (eb) =>
            eb
              .selectFrom("inputFile")
              .select("inputFile.sha256")
              .whereRef("inputFile.jobId", "=", "job.id")
              .where("inputFile.path", "=", path)
              .unionAll(
                eb
                  .selectFrom("batchInputFile")
                  .select("batchInputFile.sha256")
                  .whereRef("batchInputFile.batchId", "=", "job.batchId")
                  .where("batchInputFile.path", "=", path),
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
