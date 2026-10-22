import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";

import canonicalize from "canonicalize";
import {
  FINAL_JOB_STATUSES,
  IdempotencyKeyReused,
  NotFound,
  type Job,
  type JobEvent,
  type JobEventsPage,
  jobEventSchema,
  type JobSubmission,
} from "contract";
import { Array, Effect, Order, Record, String, Struct } from "effect";
import { compileSchema, isJsonError } from "json-schema-library";
import { sql } from "kysely";
import { jsonArrayFrom } from "kysely/helpers/postgres";
import { validate as isUuid, v7 as uuidv7 } from "uuid";
import * as z from "zod/mini";

import { getOrganization, keyMetadataSchema } from "./auth/auth.ts";
import { Db, DB_SCHEMA } from "./db/database.ts";
import {
  ApikeyId,
  type EventId,
  InputFileName,
  type JobControl,
  JobId,
  type JobProgressRow,
  type JobRow,
  type JobStatus,
  MemberId,
  type RunnerId,
} from "./db/types.ts";

/** The statuses a job ends in (`FINAL_JOB_STATUSES`), as the hub's database types them. */
export const FINAL_STATUSES: JobStatus[] = [...FINAL_JOB_STATUSES];

/** The statuses of a job a Runner may be running or about to run, which a control waits for. */
export const QUEUED_OR_RUNNING: JobStatus[] = ["queued", "running"];

/** About how many bytes of events, as JSON Lines, one page of a job's events holds. */
const MAX_PAGE_BYTES = 1024 * 1024;

/** What the hub reports of a job, from its own row and from its progress row. */
const JOB_COLUMNS = [
  "id",
  "status",
  "createdAt",
  "startedAt",
  "endedAt",
  "result",
  "pendingControl",
  "pendingControlRequestedAt",
] as const;
const PROGRESS_COLUMNS = ["totalTokens", "costUsd", "activity"] as const;

/**
 * A job as the API reports it: its result only once it has succeeded, its cost as the decimal text
 * Postgres gives for `numeric`, its activity only while it runs, and a control only while it waits
 * for the job's Runner. The report that starts a placement carries its first activity, so a job
 * running again never shows the last placement's.
 */
function toApiJob(
  job: Pick<JobRow, (typeof JOB_COLUMNS)[number]> &
    Pick<JobProgressRow, (typeof PROGRESS_COLUMNS)[number]>,
): Job {
  const {
    result,
    totalTokens,
    costUsd,
    activity,
    pendingControl,
    pendingControlRequestedAt,
    ...rest
  } = job;
  const summary = {
    usage: { totalTokens, cost: { totalUsd: costUsd } },
    ...(job.status === "running" && activity !== null ? { activity } : {}),
    // The migration keeps the two set together.
    ...(pendingControl !== null && pendingControlRequestedAt !== null
      ? { pendingControl: { type: pendingControl, requestedAt: pendingControlRequestedAt } }
      : {}),
  };
  return job.status === "succeeded" ? { ...rest, ...summary, result } : { ...rest, ...summary };
}

/**
 * What is wrong with a Result contract's schema, as messages: text that is not Unicode, then
 * json-schema-library's own checks of the schema, then the hub's rules for a result.
 */
export function resultContractProblems(schema: Record.ReadonlyRecord<string, unknown>): string[] {
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
            status: "queued",
            prompt: submission.prompt,
            resultContract: submission.resultContract,
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
            .innerJoin("jobProgress", "jobProgress.jobId", "job.id")
            .select([...JOB_COLUMNS, "contentHash", ...PROGRESS_COLUMNS])
            .where("organizationId", "=", organization.id)
            .where("idempotencyKey", "=", submission.idempotencyKey ?? null)
            .executeTakeFirstOrThrow();
        }
        // Reports from the job's Runner update this row, starting from nothing stored and nothing used.
        const progress = await trx
          .insertInto("jobProgress")
          .values({ jobId: inserted.id })
          .returning(PROGRESS_COLUMNS)
          .executeTakeFirstOrThrow();
        if (!Record.isEmptyRecord(submission.inputFiles)) {
          await trx
            .insertInto("inputFile")
            .values(
              Record.collect(submission.inputFiles, (name, file) => ({
                jobId: inserted.id,
                name: InputFileName(name),
                content: file.content,
              })),
            )
            .execute();
        }
        return { ...inserted, ...progress };
      }),
    );
    if (job.contentHash !== contentHash) return yield* Effect.fail(new IdempotencyKeyReused());
    return toApiJob(Struct.pick(job, [...JOB_COLUMNS, ...PROGRESS_COLUMNS]));
  });
}

/** The job `jobId`, or a 404 error. */
export function getJob(jobId: JobId) {
  return Effect.gen(function* () {
    // An ID that is not a UUID names no job; Postgres would refuse it in a `uuid` column.
    if (!isUuid(jobId)) return yield* Effect.fail(new NotFound());
    const database = yield* Db;
    const job = yield* database.query((db) =>
      db
        .selectFrom("job")
        .innerJoin("jobProgress", "jobProgress.jobId", "job.id")
        .select([...JOB_COLUMNS, ...PROGRESS_COLUMNS])
        .where("id", "=", jobId)
        .executeTakeFirst(),
    );
    if (!job) return yield* Effect.fail(new NotFound());
    return toApiJob(job);
  });
}

/**
 * Cancels the job `jobId` and returns it, or a 404 error, with the Runner to tell to sync now
 * when the cancel waits for one. A job no Runner was sent ends at once, at the hub's clock. One its
 * Runner may be running gets a pending cancel, which the Runner confirms by ending the job, unless
 * the job ends first; repeating the cancel keeps the time it was first asked for. A job that has
 * ended is left as it is. One statement decides, so a placement taking the job at the same moment
 * either finds it cancelled or carries its pending cancel to the Runner.
 */
export function cancelJob(jobId: JobId) {
  return Effect.gen(function* () {
    if (!isUuid(jobId)) return yield* Effect.fail(new NotFound());
    const database = yield* Db;
    const now = new Date();
    const cancelled = yield* database.query((db) =>
      db
        .updateTable("job")
        .from("jobProgress")
        .set((eb) => {
          const unsent = eb.and([eb("job.status", "=", "queued"), eb("job.runnerId", "is", null)]);
          return {
            status: eb
              .case()
              .when(unsent)
              .then(eb.val<JobStatus>("cancelled"))
              .else(eb.ref("job.status"))
              .end(),
            endedAt: eb.case().when(unsent).then(eb.val(now)).else(eb.ref("job.endedAt")).end(),
            pendingControl: eb
              .case()
              .when(unsent)
              .then(eb.lit(null))
              // Cast, since neither branch gives the enum's type.
              .else(eb.cast<JobControl>(eb.val("cancel"), sql.id(DB_SCHEMA, "jobControl")))
              .end(),
            pendingControlRequestedAt: eb
              .case()
              .when(unsent)
              .then(eb.lit(null))
              .when("job.pendingControl", "=", "cancel")
              .then(eb.ref("job.pendingControlRequestedAt"))
              .else(eb.val(now))
              .end(),
          };
        })
        .whereRef("jobProgress.jobId", "=", "job.id")
        .where("job.id", "=", jobId)
        .where("job.status", "in", QUEUED_OR_RUNNING)
        .returning([
          ...JOB_COLUMNS.map((column) => `job.${column}` as const),
          ...PROGRESS_COLUMNS,
          "job.runnerId",
        ])
        .executeTakeFirst(),
    );
    if (!cancelled) return { job: yield* getJob(jobId) };
    const { runnerId, ...job } = cancelled;
    return {
      job: toApiJob(job),
      ...(job.pendingControl !== null && runnerId !== null ? { hint: runnerId } : {}),
    } satisfies { job: Job; hint?: RunnerId };
  });
}

/**
 * A page of the job `jobId`'s events after the position `after`, or a 404 error. Stored rows
 * each hold one report's events, so the page takes the rows after `after` in order, at most `limit`
 * of them and only as many as start within the byte budget, splits them into events, and drops
 * those at or before `after`.
 */
export function readJobEvents(jobId: JobId, after: EventId, limit: number) {
  return Effect.gen(function* () {
    if (!isUuid(jobId)) return yield* Effect.fail(new NotFound());
    const database = yield* Db;
    // One statement, so the job's status, its position and its events are read at one moment. Read
    // apart, events stored between two reads would be missing from a page whose status is final.
    const job = yield* database.query((db) =>
      db
        .selectFrom("job as j")
        .innerJoin("jobProgress as p", "p.jobId", "j.id")
        .where("j.id", "=", jobId)
        .select((eb) => [
          "j.status",
          "p.position",
          jsonArrayFrom(
            eb
              .selectFrom((sized) =>
                sized
                  .selectFrom((next) =>
                    next
                      .selectFrom("jobEventChunk as c")
                      .select((size) => [
                        "c.lastId",
                        "c.events",
                        size.fn<number>("octet_length", ["c.events"]).as("size"),
                      ])
                      .whereRef("c.jobId", "=", "j.id")
                      .where("c.lastId", ">", after)
                      .orderBy("c.lastId")
                      .limit(limit)
                      .as("next"),
                  )
                  .select((sum) => [
                    "next.lastId",
                    "next.events",
                    sum(
                      sum.fn.sum<number>("next.size").over((over) => over.orderBy("next.lastId")),
                      "-",
                      sum.ref("next.size"),
                    ).as("before"),
                  ])
                  .as("sized"),
              )
              .select(["sized.lastId", "sized.events"])
              // The first row always, so an event larger than the budget is still read.
              .where("sized.before", "<", MAX_PAGE_BYTES)
              .orderBy("sized.lastId"),
          ).as("chunks"),
        ])
        .executeTakeFirst(),
    );
    if (!job) return yield* Effect.fail(new NotFound());

    const events: JobEvent[] = [];
    let bytes = 0;
    read: for (const chunk of Array.sortWith(job.chunks, (row) => row.lastId, Order.Number)) {
      for (const line of String.linesIterator(chunk.events)) {
        const event = z.parse(jobEventSchema, JSON.parse(line));
        if (event.id <= after) continue;
        const size = Buffer.byteLength(line) + 1;
        if (events.length === limit || (events.length > 0 && bytes + size > MAX_PAGE_BYTES)) {
          break read;
        }
        events.push(event);
        bytes += size;
      }
    }
    const hasMore = (events.at(-1)?.id ?? after) < job.position;
    // Events are stored only for a job that has not ended (`applyReports`), and its end only with
    // its last event, so nothing follows a page without more whose status is final.
    return { events, hasMore, status: job.status } satisfies JobEventsPage;
  });
}
