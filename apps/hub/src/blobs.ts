import { Buffer } from "node:buffer";
import { createHash, randomUUID } from "node:crypto";
import { zstdCompressSync } from "node:zlib";

import { chunkFromAsync } from "chunk-data";
import { Cause, Effect, Stream } from "effect";
import { type ExpressionBuilder, type Kysely } from "kysely";

import { getOrganization } from "./auth/auth.ts";
import { Db } from "./db/database.ts";
import {
  BlobSha256,
  type Database,
  type OrganizationId,
  UploadChunkIndex,
  UploadId,
} from "./db/types.ts";
import { InvalidInput } from "./invalid-input.ts";

// Blobs, uploaded before the jobs that name them (ADR 0035): a check tells a caller which of its
// files the hub already holds for its Organization, an upload stores one file's bytes from one
// request, and jobs then name their files by digest. Each Blob's bytes are stored once however many
// Organizations upload them (ADR 0031), but an Organization is told a Blob is stored, and may name
// it, only while it holds it: while its upload is under seven days old, or while a job or Batch of
// its own uses it. A Blob nothing holds or uses is deleted by the sweep.

/**
 * The original bytes in each chunk of a new blob but its last. Each request for a chunk costs CPU
 * for one chunk's frame, so this bounds the CPU of a request on any plan; a blob already stored
 * keeps the size it was stored with.
 */
const CHUNK_BYTES = 1024 * 1024;

// Eight chunks use 24 parameters and at most about 8.04 MiB with zstd's compression bound.
// pg's binary serializer allocates about 16.45 MiB for that group, leaving room for the hub within
// Workers' 128 MB. Postgres allows 65,535 parameters and messages just under 1 GiB; Hyperdrive
// documents a 60-second query limit, no query or parameter byte limit.
// https://www.postgresql.org/docs/current/limits.html
// https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/libpq/libpq.h
// https://developers.cloudflare.com/hyperdrive/platform/limits/
// https://developers.cloudflare.com/workers/platform/limits/#memory
const CHUNKS_PER_INSERT = 8;

const DAY_MS = 24 * 60 * 60 * 1000;

/** Until when an upload made now holds its Blob for the Organization, though no job uses it. */
function holdUntil() {
  return new Date(Date.now() + 7 * DAY_MS);
}

/**
 * Until when an upload started now keeps its chunks if it is not published: one request writes
 * it, so an older one failed or was cut off.
 */
function unpublishedUntil() {
  return new Date(Date.now() + DAY_MS);
}

/** How many unused blobs the sweep deletes in each of its transactions. */
const SWEEP_BATCH = 1000;

/** The blob address a digest names: the SHA-256 after its `sha256:`. */
export function sha256Of(digest: string): BlobSha256 {
  return BlobSha256(Buffer.from(digest.slice("sha256:".length), "hex"));
}

/** The digest naming the blob at `sha256`. */
export function digestOf(sha256: Buffer): string {
  return `sha256:${sha256.toString("hex")}`;
}

/**
 * Whether the Organization `organizationId` holds the blob in the row `eb` selects from: its upload
 * of it has not expired, or a job or Batch of its own uses it.
 */
export function heldBy(eb: ExpressionBuilder<Database, "blob">, organizationId: OrganizationId) {
  return eb.or([
    eb.exists(
      eb
        .selectFrom("organizationBlob")
        .select("organizationBlob.sha256")
        .whereRef("organizationBlob.sha256", "=", "blob.sha256")
        .where("organizationBlob.organizationId", "=", organizationId)
        .where("organizationBlob.expiresAt", ">", new Date()),
    ),
    eb.exists(
      eb
        .selectFrom("inputFile")
        .innerJoin("job", "job.id", "inputFile.jobId")
        .select("inputFile.jobId")
        .whereRef("inputFile.sha256", "=", "blob.sha256")
        .where("job.organizationId", "=", organizationId),
    ),
    eb.exists(
      eb
        .selectFrom("batchInputFile")
        .innerJoin("batch", "batch.id", "batchInputFile.batchId")
        .select("batchInputFile.batchId")
        .whereRef("batchInputFile.sha256", "=", "blob.sha256")
        .where("batch.organizationId", "=", organizationId),
    ),
  ]);
}

/**
 * Whether anything keeps the blob in the row `eb` selects from: an Input file of a job or of a
 * Batch's agent, or an Organization's upload that has not expired.
 */
export function inUse(eb: ExpressionBuilder<Database, "blob">) {
  return eb.or([
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
    eb.exists(
      eb
        .selectFrom("organizationBlob")
        .select("organizationBlob.sha256")
        .whereRef("organizationBlob.sha256", "=", "blob.sha256")
        .where("organizationBlob.expiresAt", ">", new Date()),
    ),
  ]);
}

/**
 * Deletes the blobs at `addresses`, locked by the caller, that nothing uses any longer, with their
 * uploads and chunks, and answers how many it deleted. A statement of its own after whatever stopped
 * using them: it must see that done, and a statement sees only what statements before it did.
 */
export async function deleteUnusedBlobs(
  trx: Kysely<Database>,
  addresses: readonly BlobSha256[],
): Promise<number> {
  if (addresses.length === 0) return 0;
  const result = await trx
    .deleteFrom("blob")
    .where("sha256", "=", (eb) => eb.fn.any(eb.val([...addresses])))
    .where((eb) => eb.not(inUse(eb)))
    .executeTakeFirst();
  return Number(result.numDeletedRows);
}

/**
 * The blobs at the digests `digests` that the Organization `organizationId` holds, with their sizes.
 */
export function selectHeldBlobs(
  db: Kysely<Database>,
  organizationId: OrganizationId,
  digests: readonly string[],
) {
  return db
    .selectFrom("blob")
    .select(["sha256", "originalSizeBytes"])
    .where("sha256", "=", (eb) => eb.fn.any(eb.val([...new Set(digests)].map(sha256Of))))
    .where((eb) => heldBy(eb, organizationId));
}

/** The problem with a file of `sizeBytes` whose digest names a Blob of `held` bytes, if any. */
export function sizeProblem(held: number | undefined, sizeBytes: number) {
  return held === undefined || held === sizeBytes
    ? undefined
    : `Must be ${held}, the size of the Blob with this digest`;
}

/**
 * For each of `files`, in order, whether the caller's Organization holds its Blob, or where to
 * upload it; fails with a problem for each file whose size is not that of the Blob it holds.
 */
export function checkBlobs(files: readonly { digest: string; sizeBytes: number }[]) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const organization = yield* getOrganization;
    const held = yield* database.query((db) =>
      selectHeldBlobs(
        db,
        organization.id,
        files.map(({ digest }) => digest),
      ).execute(),
    );
    const sizes = new Map(held.map((row) => [digestOf(row.sha256), row.originalSizeBytes]));
    const problems = files.flatMap(({ digest, sizeBytes }, index) => {
      const message = sizeProblem(sizes.get(digest), sizeBytes);
      return message === undefined ? [] : [{ message, path: ["files", index, "sizeBytes"] }];
    });
    if (problems.length > 0) return yield* Effect.fail(new InvalidInput({ problems }));
    return files.map(({ digest, sizeBytes }) =>
      sizes.has(digest)
        ? { digest, sizeBytes, stored: true as const }
        : {
            digest,
            sizeBytes,
            stored: false as const,
            upload: { transfer: "put" as const, url: `/blobs/${digest}` },
          },
    );
  });
}

/**
 * Stores the file of `sizeBytes` bytes that `body` carries under `digest`, and holds it for the
 * caller's Organization; fails with a problem when its bytes' size or SHA-256 differ, storing
 * nothing. The bytes are hashed and compressed a chunk at a time as they arrive, and written in
 * statements of their own, so no transaction waits on the caller; one transaction at the end
 * publishes them as the blob, or, when the blob is stored already, drops them and keeps the blob.
 */
export function uploadBlob(digest: string, sizeBytes: number, body: ReadableStream<Uint8Array>) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const organization = yield* getOrganization;
    const sha256 = sha256Of(digest);
    const uploadId = UploadId(randomUUID());
    yield* database.query((db) =>
      db
        .insertInto("upload")
        .values({
          id: uploadId,
          sha256: null,
          chunkBytes: CHUNK_BYTES,
          expiresAt: unpublishedUntil(),
        })
        .execute(),
    );
    yield* Effect.gen(function* () {
      const written = yield* writeChunks(uploadId, body);
      const problems = [
        ...(written.sizeBytes === sizeBytes
          ? []
          : [{ message: `Must be ${sizeBytes} bytes, as given`, path: ["body"] }]),
        ...(written.sha256.equals(sha256)
          ? []
          : [
              {
                message: `Must be the digest of the bytes, which is ${digestOf(written.sha256)}`,
                path: ["params", "digest"],
              },
            ]),
      ];
      if (problems.length > 0) return yield* Effect.fail(new InvalidInput({ problems }));
      // Uninterruptible, as a transaction under way runs to its end anyway: the cleanup below then
      // starts once it has committed or rolled back.
      return yield* Effect.uninterruptible(
        database.query((db) =>
          db.transaction().execute(async (trx) => {
            // Inserts the blob, or locks the one stored with an update's lock, which waits for a
            // deletion's: a blob whose deletion this waited for is gone once that commits, and is
            // inserted afresh (ADR 0031).
            const inserted = await trx
              .insertInto("blob")
              .values({ sha256, originalSizeBytes: sizeBytes })
              .onConflict((conflict) =>
                conflict
                  .column("sha256")
                  .doUpdateSet((eb) => ({
                    originalSizeBytes: eb.ref("excluded.originalSizeBytes"),
                  }))
                  .where((eb) => eb.lit(false)),
              )
              .returning("sha256")
              .executeTakeFirst();
            if (inserted) {
              await trx
                .updateTable("upload")
                .set({ sha256 })
                .where("id", "=", uploadId)
                .executeTakeFirstOrThrow();
            } else {
              await trx.deleteFrom("upload").where("id", "=", uploadId).execute();
            }
            const until = holdUntil();
            await trx
              .insertInto("organizationBlob")
              .values({ organizationId: organization.id, sha256, expiresAt: until })
              .onConflict((conflict) =>
                conflict.columns(["organizationId", "sha256"]).doUpdateSet((eb) => ({
                  expiresAt: eb.fn("greatest", [
                    eb.ref("organizationBlob.expiresAt"),
                    eb.val(until),
                  ]),
                })),
              )
              .execute();
          }),
        ),
      );
    }).pipe(
      // Only while unpublished: a published upload holds a blob's bytes.
      Effect.onError(() =>
        database
          .query((db) =>
            db
              .deleteFrom("upload")
              .where("id", "=", uploadId)
              .where("sha256", "is", null)
              .execute(),
          )
          .pipe(Effect.ignore),
      ),
    );
    return { digest, sizeBytes };
  });
}

/**
 * Writes the bytes `body` carries as chunks of the upload `uploadId`, each `CHUNK_BYTES` original
 * bytes compressed as one zstd frame, a few chunks per statement, each in its own transaction, so
 * none waits on the caller. Answers how many bytes it read and their SHA-256. Stopping it cancels
 * the body.
 */
function writeChunks(uploadId: UploadId, body: ReadableStream<Uint8Array>) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const hash = createHash("sha256");
    let sizeBytes = 0;
    yield* Stream.fromAsyncIterable(
      chunkFromAsync(body, CHUNK_BYTES),
      (error) => new Cause.UnknownError(error),
    ).pipe(
      Stream.map((bytes) => {
        hash.update(bytes);
        sizeBytes += bytes.byteLength;
        return zstdCompressSync(bytes);
      }),
      Stream.zipWithIndex,
      Stream.grouped(CHUNKS_PER_INSERT),
      Stream.runForEach((group) =>
        database.query((db) =>
          db
            .insertInto("uploadChunk")
            .values(
              // pg sends a Buffer parameter as binary, so the bytes cross as they are.
              group.map(([bytes, index]) => ({ uploadId, index: UploadChunkIndex(index), bytes })),
            )
            .execute(),
        ),
      ),
    );
    return { sizeBytes, sha256: hash.digest() };
  });
}

/**
 * Deletes what no longer needs keeping: Organizations' expired holds of Blobs, uploads never
 * published, and blobs nothing uses, a few at a time, skipping any a submission or deletion holds.
 * Run every hour by the platform's scheduler.
 */
export const sweepUnusedBlobs = Db.use((database) =>
  database.query(async (db) => {
    const now = new Date();
    await db.deleteFrom("organizationBlob").where("expiresAt", "<=", now).execute();
    await db
      .deleteFrom("upload")
      .where("sha256", "is", null)
      .where("expiresAt", "<=", now)
      .execute();
    for (;;) {
      const deleted = await db.transaction().execute(async (trx) => {
        const unused = await trx
          .selectFrom("blob")
          .select("sha256")
          .where((eb) => eb.not(inUse(eb)))
          .orderBy("sha256")
          .limit(SWEEP_BATCH)
          .forUpdate()
          .skipLocked()
          .execute();
        return await deleteUnusedBlobs(
          trx,
          unused.map((row) => row.sha256),
        );
      });
      if (deleted < SWEEP_BATCH) return;
    }
  }),
);
