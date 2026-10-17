import { createHash } from "node:crypto";

import canonicalize from "canonicalize";
import { IdempotencyKeyReused, NotFound, type Job, type JobSubmission } from "contract";
import { Effect, Struct } from "effect";
import { compileSchema, isJsonError } from "json-schema-library";
import { validate as isUuid, v7 as uuidv7 } from "uuid";

import { getOrganization, keyMetadataSchema } from "./auth/auth.ts";
import { Db } from "./db/database.ts";
import { ApikeyId, InputFileName, JobId, type JobRow, MemberId } from "./db/types.ts";

/** What the hub reports of a job. */
const JOB_COLUMNS = ["id", "state", "createdAt", "startedAt", "endedAt", "result"] as const;

/** A job as the API reports it: its result only once it has succeeded. */
function toApiJob(job: Pick<JobRow, (typeof JOB_COLUMNS)[number]>): Job {
  const { result, ...rest } = job;
  return job.state === "succeeded" ? { ...rest, result } : rest;
}

/**
 * What is wrong with an Output contract's schema, as messages: text that is not Unicode, then
 * json-schema-library's own checks of the schema, then the hub's rules for a result.
 */
export function outputContractProblems(schema: Record<string, unknown>): string[] {
  // Parsed JSON holds no NaN, Infinity or cycles, so canonical JSON refuses it only for a lone
  // surrogate, as RFC 8785 requires; the contract checks the submission's other text itself. NUL in
  // the schema is left to Postgres, which refuses it in `jsonb`: no schema needs one, and finding it
  // would take a walk over every string.
  try {
    canonicalize(schema);
  } catch {
    return ["The schema's text must be Unicode text, without lone surrogates"];
  }
  const root = compileSchema(schema);
  if (root.schemaErrors?.length) return root.schemaErrors.map((error) => error.message);
  const problems: string[] = [];
  // A list of results is submitted as items, one job each (ADR 0009).
  if ([schema.type].flat().includes("array")) {
    problems.push("The schema must describe a single value, not a list");
  }
  // The hub adds no remote schemas, so a reference it cannot resolve points outside the schema. The
  // library checks only references into `$defs` itself; `resolveRef` is what validation follows, and
  // returns an error for any other, though its type leaves the error out.
  for (const node of root.toSchemaNodes()) {
    if (node.schema.$ref === undefined && node.schema.$dynamicRef === undefined) continue;
    const target: unknown = node.resolveRef();
    if (target == null || isJsonError(target)) {
      problems.push(`The reference at ${node.schemaLocation} must point within the schema`);
    }
  }
  return problems;
}

/** Who submitted a job: the API key and the member who created it, copied as they were. */
interface Submitter {
  keyId: ApikeyId;
  keyName: string | null;
  memberId: MemberId;
}

/** The submitter of a job sent with `key`, as Better Auth verified it. */
export function submitterOf(key: { id: string; name: string | null; metadata: unknown }) {
  const { createdBy } = keyMetadataSchema.parse(key.metadata);
  return {
    keyId: ApikeyId(key.id),
    keyName: key.name,
    memberId: MemberId(createdBy),
  } satisfies Submitter;
}

/**
 * The submission's content without its idempotency key, as the hex SHA-256 of its RFC 8785
 * canonical JSON: the same content always hashes the same, however its JSON was written.
 */
function contentHashOf(submission: JobSubmission) {
  // Canonical JSON, like JSON, leaves out properties whose value is `undefined`.
  const content = canonicalize({ ...submission, idempotencyKey: undefined });
  return createHash("sha256")
    .update(content ?? "")
    .digest("hex");
}

/**
 * Accepts the job `submission` as `queued`. A submission with the idempotency key of an earlier job
 * returns that job if their content is the same, and fails otherwise.
 */
export function submitJob(submission: JobSubmission, submitter: Submitter) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const organization = yield* getOrganization;
    const contentHash = contentHashOf(submission);
    const job = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        // A concurrent submission with the same key waits here for the first to commit, then
        // inserts nothing; the select after it sees the committed job.
        const inserted = await trx
          .insertInto("job")
          .values({
            id: JobId(uuidv7()),
            organizationId: organization.id,
            state: "queued",
            prompt: submission.prompt,
            outputContract: submission.outputContract,
            idempotencyKey: submission.idempotencyKey ?? null,
            contentHash,
            submitterKeyId: submitter.keyId,
            submitterKeyName: submitter.keyName,
            submitterMemberId: submitter.memberId,
            createdAt: new Date(),
          })
          .onConflict((conflict) =>
            conflict.constraint("job_organizationId_idempotencyKey_key").doNothing(),
          )
          .returning([...JOB_COLUMNS, "contentHash"])
          .executeTakeFirst();
        // Only a submission with an idempotency key can conflict.
        if (!inserted) {
          return await trx
            .selectFrom("job")
            .select([...JOB_COLUMNS, "contentHash"])
            .where("organizationId", "=", organization.id)
            .where("idempotencyKey", "=", submission.idempotencyKey ?? null)
            .executeTakeFirstOrThrow();
        }
        const inputFiles = Object.entries(submission.inputFiles);
        if (inputFiles.length > 0) {
          await trx
            .insertInto("inputFile")
            .values(
              inputFiles.map(([name, file]) => ({
                jobId: inserted.id,
                name: InputFileName(name),
                content: file.content,
              })),
            )
            .execute();
        }
        return inserted;
      }),
    );
    if (job.contentHash !== contentHash) return yield* Effect.fail(new IdempotencyKeyReused());
    return toApiJob(Struct.pick(job, JOB_COLUMNS));
  });
}

/** The job `jobId`, or a 404 error. */
export function getJob(jobId: JobId) {
  return Effect.gen(function* () {
    // An ID that is not a UUID names no job; Postgres would refuse it in a `uuid` column.
    if (!isUuid(jobId)) return yield* Effect.fail(new NotFound());
    const database = yield* Db;
    const job = yield* database.query((db) =>
      db.selectFrom("job").select(JOB_COLUMNS).where("id", "=", jobId).executeTakeFirst(),
    );
    if (!job) return yield* Effect.fail(new NotFound());
    return toApiJob(job);
  });
}
