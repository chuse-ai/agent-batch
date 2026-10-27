import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";

import canonicalize from "canonicalize";
import {
  type BlockedBy,
  ControlRefused,
  FINAL_JOB_STATUSES,
  NotFound,
  type Job,
  type JobEvent,
  type JobEventsPage,
  type JobsPage,
  type Labels,
  jobEventSchema,
} from "contract";
import { Array, Effect, Order, Record, String, Struct } from "effect";
import { compileSchema, isJsonError } from "json-schema-library";
import {
  type AliasableExpression,
  type Expression,
  type ExpressionBuilder,
  expressionBuilder,
  type Kysely,
  type QueryCreator,
  sql,
  type Transaction,
} from "kysely";
import { jsonArrayFrom } from "kysely/helpers/postgres";
import * as z from "zod/mini";

import { keyMetadataSchema } from "./auth/auth.ts";
import { Db, DB_SCHEMA } from "./db/database.ts";
import {
  ApikeyId,
  type BatchId,
  type Database,
  type EventId,
  type JobId,
  type JobProgressRow,
  type JobFailureReason,
  type JobDeletionRow,
  type JobRow,
  type JobStatus,
  type JobUpdate,
  MemberId,
  type RunnerId,
} from "./db/types.ts";
import { deleteUnusedBlobs, lockInputFileBlobs } from "./input-files.ts";
import type { HubSettings } from "./settings.ts";

/** The statuses a job ends in (`FINAL_JOB_STATUSES`), as the hub's database types them. */
export const FINAL_STATUSES: JobStatus[] = [...FINAL_JOB_STATUSES];

/** The statuses of a job a Runner may be running or about to run, which a control waits for. */
export const QUEUED_OR_RUNNING: JobStatus[] = ["queued", "running"];

/** About how many bytes of events, as JSON Lines, one page of a job's events holds. */
const MAX_PAGE_BYTES = 1024 * 1024;

/** What a job's Usage record copies from the job's row at its end, beside the job's ID. */
export const USAGE_COPIED_COLUMNS = [
  "organizationId",
  "batchId",
  "labels",
  "status",
  "failureReason",
  "runnerId",
  "submitterKeyId",
  "submitterKeyName",
  "submitterMemberId",
  "createdAt",
  "startedAt",
  "endedAt",
] as const;

/** What the hub reports of a job, from its own row and from its progress row. */
const JOB_COLUMNS = [
  "id",
  "status",
  "createdAt",
  "startedAt",
  "endedAt",
  "runTimeoutSeconds",
  "modelBudgetUsd",
  "result",
  "failureReason",
  "failureMessage",
  "pendingControl",
  "pendingControlRequestedAt",
  "batchId",
  "customId",
  "labels",
] as const;
const PROGRESS_COLUMNS = ["totalTokens", "costUsd", "activity"] as const;

/**
 * A job as the API reports it: its result only once it has succeeded and its failure once it has
 * failed, its activity only while it runs, and a control only while it waits for the job's Runner.
 * The report of a run's start carries its first activity, so a job running again never shows the
 * last run's.
 */
function toApiJob(
  job: Pick<JobRow, (typeof JOB_COLUMNS)[number]> &
    Pick<JobProgressRow, (typeof PROGRESS_COLUMNS)[number]> & { blockedBy: BlockedBy | null },
): Job {
  const {
    status,
    result,
    failureReason,
    failureMessage,
    totalTokens,
    costUsd,
    activity,
    pendingControl,
    pendingControlRequestedAt,
    blockedBy,
    ...rest
  } = job;
  const summary = {
    usage: { totalTokens, cost: { totalUsd: costUsd } },
    ...(job.status === "running" && activity !== null ? { activity } : {}),
    // The migration keeps the two set together.
    ...(pendingControl !== null && pendingControlRequestedAt !== null
      ? { pendingControl: { type: pendingControl, requestedAt: pendingControlRequestedAt } }
      : {}),
    ...(blockedBy === null ? {} : { blockedBy }),
  };
  return withOutcome({ ...rest, ...summary }, { status, result, failureReason, failureMessage });
}

/**
 * What keeps the job from being sent to a Runner, or null: only a queued job not yet sent to
 * one is blocked, by its Batch's pause, a caller's or the breaker's, or, outside its Batch's Canary,
 * by the Canary while any of its jobs is unfinished. Derived whenever it is read, from the Batch,
 * so pausing or resuming a Batch is one write; placement sends only the jobs this finds nothing for.
 */
export function blockedByOf(job: {
  status: Expression<JobRow["status"]>;
  placedAt: Expression<JobRow["placedAt"]>;
  batchId: Expression<JobRow["batchId"]>;
  inCanary: Expression<JobRow["inCanary"]>;
}): AliasableExpression<BlockedBy | null> {
  const eb = expressionBuilder<Database>();
  return eb
    .case()
    .when(eb.or([eb(job.status, "!=", "queued"), eb(job.placedAt, "is not", null)]))
    .then(eb.lit(null))
    .else(
      eb
        .selectFrom("batch as b")
        .select((batch) =>
          batch
            .case()
            .when("b.pausedBy", "=", "caller")
            .then(batch.val<BlockedBy>("batchPausedByCaller"))
            .when("b.pausedBy", "=", "breaker")
            .then(batch.val<BlockedBy>("batchPausedByBreaker"))
            .when(
              batch.and([
                batch(job.inCanary, "=", false),
                batch.exists(
                  batch
                    .selectFrom("job as canary")
                    .select("canary.id")
                    .whereRef("canary.batchId", "=", job.batchId)
                    .where("canary.inCanary", "=", true)
                    .where("canary.status", "in", UNFINISHED),
                ),
              ]),
            )
            .then(batch.val<BlockedBy>("batchCanary"))
            .else(batch.lit(null))
            .end()
            .as("reason"),
        )
        .whereRef("b.id", "=", job.batchId),
    )
    .end();
}

/** The statuses of a job that has not ended. */
export const UNFINISHED: JobStatus[] = ["queued", "running", "paused"];

/** The jobs `ids` as the API reports them, in no particular order, each with what blocks it. */
export async function readJobs(db: Kysely<Database>, ids: readonly JobId[]): Promise<Job[]> {
  if (ids.length === 0) return [];
  const rows = await db
    .selectFrom("job as j")
    .innerJoin("jobProgress as p", "p.jobId", "j.id")
    .select((eb) => [
      ...JOB_COLUMNS.map((column) => `j.${column}` as const),
      ...PROGRESS_COLUMNS.map((column) => `p.${column}` as const),
      blockedByOf({
        status: eb.ref("j.status"),
        placedAt: eb.ref("j.placedAt"),
        batchId: eb.ref("j.batchId"),
        inCanary: eb.ref("j.inCanary"),
      }).as("blockedBy"),
    ])
    .where("j.id", "=", (eb) => eb.fn.any(eb.val([...ids])))
    .execute();
  return rows.map(toApiJob);
}

/** What every job reports whatever its status. */
type JobBase = Omit<Job, "status" | "result" | "failure">;

/**
 * `job` with its status and what the status carries: a succeeded job's result, where it has one,
 * and a failed job's failure, with its message where the cause gave one.
 */
function withOutcome(
  job: JobBase,
  outcome: {
    status: JobStatus;
    result?: JobRow["result"];
    failureReason: JobFailureReason | null;
    failureMessage: string | null;
  },
): Job {
  const { status, result, failureReason, failureMessage } = outcome;
  switch (status) {
    case "succeeded":
      return { ...job, status, ...(result === undefined ? {} : { result }) };
    case "failed":
      // The migration keeps a reason on every failed job.
      if (failureReason === null) throw new Error(`The failed job ${job.id} has no failure reason`);
      return {
        ...job,
        status,
        failure: {
          reason: failureReason,
          ...(failureMessage === null ? {} : { message: failureMessage }),
        },
      };
    default:
      return { ...job, status };
  }
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
  // `submit_result` takes one value (ADR 0009), so a list of results is one job per item.
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

/** The limits a submission or a resume gives a job, each one optional. */
export type GivenLimits = Pick<JobUpdate, "runTimeoutSeconds" | "modelBudgetUsd">;

/**
 * What `given` asks above the hub's maximums, as problems with the input fields they name,
 * which the API refuses as it refuses any invalid input.
 */
export function limitProblems(given: GivenLimits, settings: HubSettings["jobLimits"]) {
  const problems: { message: string; path: (string | number)[] }[] = [];
  if (
    given.runTimeoutSeconds !== undefined &&
    given.runTimeoutSeconds > settings.runTimeoutSecondsMax
  ) {
    problems.push({
      message: `Must be at most ${settings.runTimeoutSecondsMax}, the hub's maximum`,
      path: ["runTimeoutSeconds"],
    });
  }
  if (given.modelBudgetUsd !== undefined && given.modelBudgetUsd > settings.modelBudgetUsdMax) {
    problems.push({
      message: `Must be at most ${settings.modelBudgetUsdMax}, the hub's maximum`,
      path: ["modelBudgetUsd"],
    });
  }
  return problems;
}

/** Who submitted a job: the API key and the member who created it, copied as they were. */
export interface Submitter {
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
 * The `content` of a job or a Batch, without the custom ID that names it, as the hex SHA-256 of its
 * RFC 8785 canonical JSON: the same content always hashes the same, however its JSON was written.
 */
export function contentHashOf(content: object) {
  // Canonical JSON, like JSON, leaves out properties whose value is `undefined`, so a key set to
  // `undefined` is left out.
  const canonical = canonicalize(content);
  return createHash("sha256")
    .update(canonical ?? "")
    .digest("hex");
}

/** The job `jobId`, or a 404 error. */
export function getJob(jobId: JobId) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const [job] = yield* database.query((db) => readJobs(db, [jobId]));
    if (job) return job;
    const deleted = yield* database.query((db) => readDeletedJob(db, jobId));
    if (!deleted) return yield* Effect.fail(new NotFound());
    return deleted;
  });
}

/** A control a caller asks of a job. */
export type Control = "pause" | "resume" | "cancel";

/**
 * What a control is decided on: the job's status, where it runs, the control waiting, and whether
 * its Runner still exists, since a removed Runner's host is beyond the hub's reach.
 */
type ControlledJob = Pick<
  JobRow,
  "status" | "runnerId" | "placedAt" | "startedAt" | "pendingControl"
> &
  Pick<JobProgressRow, "costUsd"> & {
    runnerExists: boolean;
    budgetLeft: boolean;
    givenLimitsHold: boolean;
    /** Whether its Batch is cancelled, which is final. */
    batchCancelled: boolean;
  };

/**
 * What a control does to a job as it is: nothing, a refusal saying why, or a change, with the
 * Runners to tell to sync now: the job's own, or every live one for a job any Runner may take.
 */
type Decision =
  | { kind: "unchanged" }
  | { kind: "refused"; why: string }
  | { kind: "changed"; set: JobUpdate; hint?: RunnerId | "live" };

const unchanged: Decision = { kind: "unchanged" };
const refused = (why: string): Decision => ({ kind: "refused", why });

/**
 * What `control` does to `job` at `now`: a control is a no-op when what it asks
 * for already holds, and refused when it cannot hold; otherwise the hub applies it where no Runner
 * can be running the job, and it waits for the job's Runner where one may be. A resume that changes
 * the job gives it the limits in `given`, and is refused when its model budget would not be
 * above what its model calls have cost, as for a job a spent budget stopped; one that gives other
 * limits than its own to a job it does not change is refused, as that job's run keeps its own.
 */
function decide(control: Control, job: ControlledJob, now: Date, given: GivenLimits): Decision {
  const waiting = job.pendingControl;
  const ended = {
    failureReason: null,
    failureMessage: null,
    pendingControl: null,
    pendingControlRequestedAt: null,
  };
  // The job's Runner, told to sync now when a control waits for it or the job resumes there.
  const toItsRunner = job.runnerId === null ? {} : { hint: job.runnerId };
  // A resume that changes the job gives it the limits it names, and is refused where its model
  // budget, the one it names or its own, would leave nothing to spend.
  const resumeIfBudgetLeft = (set: JobUpdate, hint?: RunnerId | "live"): Decision =>
    // A Batch's cancel is final: a job resumed would run in a Batch that takes no jobs, and, while
    // the cancel waits, under a cancel that is never to stop it. A cancelled job does not record who
    // cancelled it, so the job a person cancelled cannot be told from one its Batch's cancel ended.
    job.batchCancelled
      ? refused("The job's Batch is cancelled")
      : job.budgetLeft
        ? { kind: "changed", set: { ...set, ...given }, ...(hint === undefined ? {} : { hint }) }
        : refused(
            `The job's model calls have cost $${job.costUsd}, so it resumes only with a modelBudgetUsd above that`,
          );
  // A resumed job that started carries on from its journal, which only its Runner holds, as a new
  // run there (ADR 0026), sent when that Runner next syncs; one that never started has
  // no work to carry on.
  const resumeOnItsRunner: Decision = !job.runnerExists
    ? refused("The job's Runner was removed, so the work its host holds cannot be carried on")
    : resumeIfBudgetLeft(
        {
          status: "queued",
          // A resume starts a new run. A pause is often the last thing that happens to a job, so it
          // aborts the step in flight and records what it cut off, rather than leaving it pending,
          // which would keep that step's answer and cost out of a job never resumed for good
          // (ADR 0028); a resume then answers a new input, the note, and the paused run's late
          // reports must not touch it.
          runId: crypto.randomUUID(),
          placedAt: null,
          endedAt: null,
          result: null,
          failureReason: null,
          failureMessage: null,
        },
        job.runnerId ?? undefined,
      );
  switch (job.status) {
    case "queued":
    case "running": {
      // A queued job not yet sent to a Runner, new or resumed: no Runner can be running it.
      const unsent = job.status === "queued" && job.placedAt === null;
      if (control === "resume") {
        if (waiting === "pause") {
          return refused("The job's pause has not yet been confirmed by its Runner");
        }
        if (waiting === "cancel") return refused("The job's cancel is waiting for its Runner");
        // Its run has its limits; they change only when a resume starts a new one. A resume
        // naming the ones it has changes nothing, as when it repeats one that queued the job.
        if (!job.givenLimitsHold) {
          return refused("The job is queued or running, so a resume cannot change its limits");
        }
        return unchanged;
      }
      if (control === "pause") {
        if (waiting === "cancel") return refused("A pause cannot undo the job's waiting cancel");
        if (unsent) return { kind: "changed", set: { status: "paused" } };
        if (waiting === "pause") return unchanged;
        return {
          kind: "changed",
          set: { pendingControl: "pause", pendingControlRequestedAt: now },
          ...toItsRunner,
        };
      }
      if (unsent) return { kind: "changed", set: { status: "cancelled", endedAt: now, ...ended } };
      if (waiting === "cancel") return unchanged;
      // A cancel replaces a waiting pause.
      return {
        kind: "changed",
        set: { pendingControl: "cancel", pendingControlRequestedAt: now },
        ...toItsRunner,
      };
    }
    case "paused":
      if (control === "pause") return unchanged;
      // Nothing of a paused job runs, so the hub ends it itself.
      if (control === "cancel") {
        return { kind: "changed", set: { status: "cancelled", endedAt: now, ...ended } };
      }
      // A job held before it started has no journal, so any Runner may start it.
      if (job.startedAt === null) {
        return resumeIfBudgetLeft(
          { status: "queued", failureReason: null, failureMessage: null },
          "live",
        );
      }
      return resumeOnItsRunner;
    case "failed":
    case "cancelled":
      if (control === "cancel") return unchanged;
      if (control === "pause") return refused("The job has ended");
      if (job.startedAt === null || job.runnerId === null) {
        return refused("The job ended before it started, so it has no work to carry on");
      }
      return resumeOnItsRunner;
  }
  // The job succeeded.
  if (control === "cancel") return unchanged;
  if (control === "pause") return refused("The job has ended");
  return refused("A job that succeeded has no work to carry on");
}

/**
 * Locks, in the transaction `trx`, the Batch of the job `jobId` before the job is locked: a Batch is
 * locked before its jobs wherever both are (ADR 0033). A job's Batch never changes, so it is read
 * without a lock first.
 */
async function lockBatchOfJob(trx: Transaction<Database>, jobId: JobId) {
  await trx
    .selectFrom("batch")
    .select("batch.id")
    .where("batch.id", "=", (eb) =>
      eb.selectFrom("job").select("job.batchId").where("job.id", "=", jobId),
    )
    .forUpdate()
    .execute();
}

/**
 * Applies `control` to the job `jobId`, with the limits `given` for a resume, and returns
 * the job, or fails with a 404 error or, for a control that cannot take effect, a 409 saying why;
 * with the Runners to tell to sync now. The job's row is locked while the control is decided, so
 * placement, the Runner's reports and other controls take it before or after, not between.
 */
export function controlJob(jobId: JobId, control: Control, given: GivenLimits = {}) {
  /** The budget the resume names, as `numeric`, or null. */
  const givenBudget = (eb: ExpressionBuilder<Database, "job">) =>
    eb.cast<number>(eb.val(given.modelBudgetUsd ?? null), "numeric");
  return Effect.gen(function* () {
    const database = yield* Db;
    const now = new Date();
    const controlled = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        // A resume is decided on its Batch's cancel, which waits for this resume or comes first,
        // and may withdraw the job's failure from its breaker's counts.
        if (control === "resume") await lockBatchOfJob(trx, jobId);
        const job = await trx
          .selectFrom("job")
          .innerJoin("jobProgress", "jobProgress.jobId", "job.id")
          .select((eb) => [
            ...JOB_COLUMNS.map((column) => `job.${column}` as const),
            ...PROGRESS_COLUMNS,
            blockedByOf({
              status: eb.ref("job.status"),
              placedAt: eb.ref("job.placedAt"),
              batchId: eb.ref("job.batchId"),
              inCanary: eb.ref("job.inCanary"),
            }).as("blockedBy"),
            "job.runnerId",
            "job.placedAt",
            // The Runner's row is locked, so a removal of the Runner waits for this control and then
            // ends the job a resume queued on it, or comes first, and the resume is refused.
            eb
              .exists(
                eb
                  .selectFrom("runner")
                  .select("runner.id")
                  .whereRef("runner.id", "=", "job.runnerId")
                  .forKeyShare(),
              )
              // pg reads a boolean as one.
              .$castTo<boolean>()
              .as("runnerExists"),
            // Money exactly, as `numeric`: whether the budget the resume names, or else the job's,
            // is above what its model calls have cost, and whether the limits it names are the
            // job's own.
            eb(
              eb.fn.coalesce(givenBudget(eb), "job.modelBudgetUsd"),
              ">",
              eb.ref("jobProgress.costUsd"),
            )
              .$castTo<boolean>()
              .as("budgetLeft"),
            eb
              .and([
                eb(
                  eb.fn.coalesce(
                    eb.cast<number>(eb.val(given.runTimeoutSeconds ?? null), "integer"),
                    "job.runTimeoutSeconds",
                  ),
                  "=",
                  eb.ref("job.runTimeoutSeconds"),
                ),
                eb(
                  eb.fn.coalesce(givenBudget(eb), "job.modelBudgetUsd"),
                  "=",
                  eb.ref("job.modelBudgetUsd"),
                ),
              ])
              .$castTo<boolean>()
              .as("givenLimitsHold"),
            eb
              .exists(
                eb
                  .selectFrom("batch")
                  .select("batch.id")
                  .whereRef("batch.id", "=", "job.batchId")
                  .where("batch.cancelledAt", "is not", null),
              )
              .$castTo<boolean>()
              .as("batchCancelled"),
          ])
          .where("job.id", "=", jobId)
          .forUpdate("job")
          .executeTakeFirst();
        if (!job) return undefined;
        const decision = decide(control, job, now, given);
        if (decision.kind !== "changed") {
          const shown = Struct.pick(job, [...JOB_COLUMNS, ...PROGRESS_COLUMNS, "blockedBy"]);
          return { decision, job: toApiJob(shown) };
        }
        // The job as the control left it; a control changes nothing of its progress.
        const changed = await trx
          .updateTable("job")
          .set(decision.set)
          .where("id", "=", jobId)
          .returning((eb) => [
            ...JOB_COLUMNS,
            blockedByOf({
              status: eb.ref("job.status"),
              placedAt: eb.ref("job.placedAt"),
              batchId: eb.ref("job.batchId"),
              inCanary: eb.ref("job.inCanary"),
            }).as("blockedBy"),
          ])
          .executeTakeFirstOrThrow();
        if (FINAL_STATUSES.includes(changed.status)) await recordUsageOfEnd(trx, jobId);
        if (control === "resume" && job.status === "failed") {
          await withdrawCountedFailure(trx, job.batchId, jobId);
        }
        return {
          decision,
          job: toApiJob({ ...changed, ...Struct.pick(job, PROGRESS_COLUMNS) }),
        };
      }),
    );
    if (!controlled) {
      // A deleted job still reads while its Runner owes the deletion of its files, and is over: a
      // cancel leaves it as it is, and nothing carries it on.
      const deleted = yield* database.query((db) => readDeletedJob(db, jobId));
      if (!deleted) return yield* Effect.fail(new NotFound());
      if (control === "cancel") return { job: deleted, hint: undefined };
      return yield* Effect.fail(
        new ControlRefused({
          message:
            control === "resume"
              ? "The job is deleted, and its journal is being deleted from its host"
              : "The job is deleted",
        }),
      );
    }
    const { decision, job } = controlled;
    if (decision.kind === "refused") {
      return yield* Effect.fail(new ControlRefused({ message: decision.why }));
    }
    const hint = decision.kind === "changed" ? decision.hint : undefined;
    return { job, hint } satisfies { job: Job; hint?: RunnerId | "live" };
  });
}

/**
 * Withdraws, in the transaction `trx` that resumes the failed job `jobId`, its failure from the
 * breaker's counts of its Batch `batchId`, locked first, if they counted it: if its failure's Usage
 * record, its latest, came after its Batch's current Canary began. The breaker counts a job once,
 * by its latest end, as a resumed job's next end is evidence about that job, not about the content
 * the jobs share. A failure the breaker did not count, as one before the Canary began, is left.
 * The record's ID orders it against the Canary's start, which the hub records under the Batch's
 * lock; the job's `endedAt` would not, as a Runner's clock gives it.
 */
async function withdrawCountedFailure(trx: Transaction<Database>, batchId: BatchId, jobId: JobId) {
  await trx
    .updateTable("batch as b")
    .set((eb) => ({
      countedEnds: eb("b.countedEnds", "-", eb.lit(1)),
      countedFailures: eb("b.countedFailures", "-", eb.lit(1)),
    }))
    .where("b.id", "=", batchId)
    .where("b.canary", "is not", null)
    .where("b.countedAfterUsageRecordId", "<", (eb) =>
      eb
        .selectFrom("usageRecord")
        .select((max) => max.fn.max<number>("id").as("id"))
        .where("jobId", "=", jobId),
    )
    .execute();
}

/** A job's end that gets a Usage record: the job as it ended, and its totals then. */
export type UsageOfEnd = Pick<
  JobRow,
  (typeof USAGE_COPIED_COLUMNS)[number] | "recordedTotalTokens" | "recordedCostUsd"
> & {
  jobId: JobId;
  jobTotalTokens: number;
  jobCostUsd: number;
};

/**
 * Inserts the Usage record of each job end in the `ended` table, a `WITH` query of the statement
 * `db` builds: the job as it ended, what it used since its previous end, and its totals now. What
 * it used is its totals less what its records already count (`recordedTotalTokens`,
 * `recordedCostUsd`), as the job's row held them before this end; the statement that ends the job
 * sets them to its totals. The ends the hub makes itself and those its Runners report are recorded
 * alike.
 */
export function insertUsageRecords(db: QueryCreator<Database & { ended: UsageOfEnd }>) {
  return db
    .insertInto("usageRecord")
    .columns([
      "jobId",
      ...USAGE_COPIED_COLUMNS,
      "totalTokens",
      "costUsd",
      "jobTotalTokens",
      "jobCostUsd",
    ])
    .expression((eb) =>
      eb
        .selectFrom("ended as e")
        .select((select) => [
          "e.jobId",
          ...USAGE_COPIED_COLUMNS.map((column) => `e.${column}` as const),
          select("e.jobTotalTokens", "-", select.ref("e.recordedTotalTokens")).as("totalTokens"),
          select("e.jobCostUsd", "-", select.ref("e.recordedCostUsd")).as("costUsd"),
          "e.jobTotalTokens",
          "e.jobCostUsd",
        ]),
    );
}

/**
 * Records the Usage record of the job `jobId`'s end, which the hub has just made itself, in the
 * transaction `trx` that made it, from the job's row and its totals now, which its records then
 * count.
 */
async function recordUsageOfEnd(trx: Transaction<Database>, jobId: JobId) {
  await insertUsageRecords(
    trx
      .with("ended", (qb) =>
        qb
          .selectFrom("job as j")
          .innerJoin("jobProgress as p", "p.jobId", "j.id")
          .select([
            "j.id as jobId",
            ...USAGE_COPIED_COLUMNS.map((column) => `j.${column}` as const),
            "j.recordedTotalTokens",
            "j.recordedCostUsd",
            "p.totalTokens as jobTotalTokens",
            "p.costUsd as jobCostUsd",
          ])
          .where("j.id", "=", jobId),
      )
      // Runs whether or not the insert reads it, as Postgres runs every data-modifying `WITH`.
      .with("counted", (qb) =>
        qb
          .updateTable("job as j")
          .from("jobProgress as p")
          .set((eb) => ({
            recordedTotalTokens: eb.ref("p.totalTokens"),
            recordedCostUsd: eb.ref("p.costUsd"),
          }))
          .whereRef("p.jobId", "=", "j.id")
          .where("j.id", "=", jobId)
          .returning("j.id"),
      ),
  ).execute();
}

/**
 * Deletes the finished job `jobId` and returns it as it now reads, or fails with a 404 error or,
 * for an unfinished job, a 409; with its Runner to tell to sync now. In one transaction: the job's
 * row goes, and with it its Input files, events and progress, and the blobs of its Input files that
 * no other job uses; its latest Usage record is marked deleted, as its tombstone; and the deletion
 * of its files from its host is recorded for its Runner, unless no Runner holds any of the job, as
 * when none was sent it or none started it. A repeat while that deletion waits returns the job
 * again, as Kubernetes returns a pod already being deleted; once the deletion is done, the job is
 * not found.
 */
export function deleteJob(jobId: JobId) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const now = new Date();
    const deleted = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        const job = await trx
          .selectFrom("job")
          .select((eb) => [
            "job.status",
            "job.runnerId",
            // A removed Runner's host is beyond the hub's reach, so its files are abandoned at once.
            // The Runner's row is locked while the deletion is recorded, so a removal of the
            // Runner sees it and abandons it in turn.
            eb
              .exists(
                eb
                  .selectFrom("runner")
                  .select("runner.id")
                  .whereRef("runner.id", "=", "job.runnerId")
                  .forKeyShare(),
              )
              // pg reads a boolean as one.
              .$castTo<boolean>()
              .as("runnerExists"),
          ])
          .where("job.id", "=", jobId)
          .forUpdate("job")
          .executeTakeFirst();
        if (!job) return { deleted: await readDeletedJob(trx, jobId) };
        if (!FINAL_STATUSES.includes(job.status)) {
          return {
            refused:
              job.status === "paused"
                ? "The job is paused, not finished; cancel it first"
                : "The job is not finished; cancel it first",
          };
        }
        // Locked before the job's files go, through which they are found.
        const blobs = await lockInputFileBlobs(trx, jobId);
        // One statement marks the Usage record deleted, records the deletion the Runner owes with a
        // snapshot of the job from that record, and deletes the job's row.
        const snapshot = await trx
          .with("usage", (qb) =>
            qb
              .updateTable("usageRecord")
              .set(
                job.runnerId === null
                  ? {
                      deletedAt: now,
                      hostDeletionOutcome: "notRequired",
                      hostDeletionOutcomeAt: now,
                    }
                  : job.runnerExists
                    ? { deletedAt: now }
                    : {
                        deletedAt: now,
                        hostDeletionOutcome: "abandoned",
                        hostDeletionOutcomeAt: now,
                      },
              )
              // The job's latest record: a job's records follow their IDs.
              .where("id", "=", (eb) =>
                eb
                  .selectFrom("usageRecord")
                  .select((max) => max.fn.max("id").as("id"))
                  .where("jobId", "=", jobId),
              )
              .returning([
                "jobId",
                "status",
                "failureReason",
                "createdAt",
                "startedAt",
                "endedAt",
                "jobTotalTokens as totalTokens",
                "jobCostUsd as costUsd",
                "organizationId",
                "id",
              ]),
          )
          .with("deletion", (qb) =>
            qb
              .insertInto("jobDeletion")
              .columns([
                ...DELETED_JOB_COLUMNS,
                "organizationId",
                "usageRecordId",
                "runnerId",
                "requestedAt",
              ])
              .expression((eb) =>
                eb
                  .selectFrom("usage")
                  .innerJoin("runner", (join) => join.on("runner.id", "=", job.runnerId))
                  .select([
                    ...DELETED_JOB_COLUMNS.map((column) => `usage.${column}` as const),
                    "usage.organizationId",
                    "usage.id as usageRecordId",
                    "runner.id as runnerId",
                    eb.val(now).as("requestedAt"),
                  ]),
              )
              .returning("jobId"),
          )
          .with("gone", (qb) => qb.deleteFrom("job").where("id", "=", jobId).returning("id"))
          .selectFrom("usage")
          .select(DELETED_JOB_COLUMNS)
          // Every end writes a Usage record, so a finished job has one.
          .executeTakeFirstOrThrow();
        await deleteUnusedBlobs(trx, blobs);
        return {
          deleted: toDeletedJob(snapshot, now, job.runnerExists ? now : null),
          hint: job.runnerExists ? (job.runnerId ?? undefined) : undefined,
        };
      }),
    );
    if ("refused" in deleted)
      return yield* Effect.fail(new ControlRefused({ message: deleted.refused }));
    if (!deleted.deleted) return yield* Effect.fail(new NotFound());
    return { job: deleted.deleted, hint: deleted.hint };
  });
}

/** What a deleted job reads from: the snapshot the deletion its Runner owes keeps. */
const DELETED_JOB_COLUMNS = [
  "jobId",
  "status",
  "failureReason",
  "createdAt",
  "startedAt",
  "endedAt",
  "totalTokens",
  "costUsd",
] as const;

/**
 * Deletes the Runner `runnerId`'s row and, in the same transaction, ends the work it held: each
 * of its jobs that had not ended ends at once, `cancelled` where a cancel was waiting and otherwise
 * `failed` with the reason `runnerRemoved`, with the Usage record of that end, and each deletion of a
 * deleted job's files it owed is abandoned, on the job's Usage record. A job its Runner reported
 * ending before this keeps its end. Placement, reports and controls lock the Runner's row while
 * they use it, so the delete waits for them, and what follows sees what they did; once it is gone,
 * none of them uses the Runner again. Returns the Runner's key, the jobs it ended, and every job
 * whose files the host may still hold, those whose delete abandoned them included; or nothing when
 * there is no such Runner.
 */
export function deleteRunnerWithItsWork(runnerId: RunnerId) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const now = new Date();
    return yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        const runner = await trx
          .deleteFrom("runner")
          .where("id", "=", runnerId)
          .returning("keyId")
          .executeTakeFirst();
        if (!runner) return undefined;
        // Its unfinished jobs, locked in the order of their IDs before the statement below ends
        // them, as a Batch's cancel and a report lock jobs.
        await trx
          .selectFrom("job")
          .select("id")
          .where("runnerId", "=", runnerId)
          .where("status", "not in", FINAL_STATUSES)
          .orderBy("id")
          .forUpdate()
          .execute();
        const ended = await insertUsageRecords(
          trx
            .with("endedJob", (qb) =>
              qb
                .updateTable("job")
                .from("jobProgress")
                .whereRef("jobProgress.jobId", "=", "job.id")
                .set((eb) => {
                  const cancelWaiting = eb("pendingControl", "=", "cancel");
                  // Cast, since no branch gives the enums' types.
                  const status = (value: JobStatus) =>
                    eb.cast<JobStatus>(eb.val(value), sql.id(DB_SCHEMA, "jobStatus"));
                  return {
                    status: eb
                      .case()
                      .when(cancelWaiting)
                      .then(status("cancelled"))
                      .else(status("failed"))
                      .end(),
                    failureReason: eb
                      .case()
                      .when(cancelWaiting)
                      .then(eb.lit(null))
                      .else(
                        eb.cast<JobFailureReason>(
                          eb.val("runnerRemoved"),
                          sql.id(DB_SCHEMA, "jobFailureReason"),
                        ),
                      )
                      .end(),
                    endedAt: now,
                    pendingControl: null,
                    pendingControlRequestedAt: null,
                    // The end's Usage record, below, counts the job's totals from here on.
                    recordedTotalTokens: eb.ref("jobProgress.totalTokens"),
                    recordedCostUsd: eb.ref("jobProgress.costUsd"),
                  };
                })
                .where("job.runnerId", "=", runnerId)
                .where("job.status", "not in", FINAL_STATUSES)
                .returning([
                  "job.id",
                  ...USAGE_COPIED_COLUMNS.map((column) => `job.${column}` as const),
                  "jobProgress.totalTokens as jobTotalTokens",
                  "jobProgress.costUsd as jobCostUsd",
                ]),
            )
            .with("ended", (qb) =>
              qb
                .selectFrom("endedJob as j")
                // The job's row as the statement found it, before it ended: what its records counted.
                .innerJoin("job as before", "before.id", "j.id")
                .select([
                  "j.id as jobId",
                  ...USAGE_COPIED_COLUMNS.map((column) => `j.${column}` as const),
                  "before.recordedTotalTokens",
                  "before.recordedCostUsd",
                  "j.jobTotalTokens",
                  "j.jobCostUsd",
                ]),
            ),
        )
          .returning("jobId")
          .execute();
        await trx
          .with("abandoned", (qb) =>
            qb
              .deleteFrom("jobDeletion")
              .where("runnerId", "=", runnerId)
              .returning("usageRecordId"),
          )
          .updateTable("usageRecord as u")
          .from("abandoned as a")
          .set({ hostDeletionOutcome: "abandoned", hostDeletionOutcomeAt: now })
          .whereRef("u.id", "=", "a.usageRecordId")
          .execute();
        // Its jobs not yet deleted, and its deleted jobs whose files were abandoned, here or by a
        // delete that found the Runner gone.
        const withFilesLeft = await trx
          .selectFrom("job")
          .select("id as jobId")
          .where("runnerId", "=", runnerId)
          .unionAll((eb) =>
            eb
              .selectFrom("usageRecord")
              .select("jobId")
              .where("runnerId", "=", runnerId)
              .where("hostDeletionOutcome", "=", "abandoned"),
          )
          .execute();
        return {
          keyId: runner.keyId,
          endedJobs: ended.map(({ jobId }) => jobId),
          jobsWithFilesLeft: withFilesLeft.map(({ jobId }) => jobId),
        };
      }),
    );
  });
}

/**
 * A deleted job as it reads: without its content, its final status, times and totals,
 * `snapshot`, with when it was deleted, `deletedAt`, and the deletion of its files its Runner still
 * owes, requested at `deletionRequestedAt`, while it waits.
 */
function toDeletedJob(
  snapshot: Pick<JobDeletionRow, (typeof DELETED_JOB_COLUMNS)[number]>,
  deletedAt: Date,
  deletionRequestedAt: Date | null,
): Job {
  return withOutcome(
    {
      id: snapshot.jobId,
      createdAt: snapshot.createdAt,
      startedAt: snapshot.startedAt,
      endedAt: snapshot.endedAt,
      usage: { totalTokens: snapshot.totalTokens, cost: { totalUsd: snapshot.costUsd } },
      deletedAt,
      ...(deletionRequestedAt !== null
        ? { pendingControl: { type: "delete", requestedAt: deletionRequestedAt } }
        : {}),
    },
    // A deleted job's message went with its content.
    { status: snapshot.status, failureReason: snapshot.failureReason, failureMessage: null },
  );
}

/**
 * A deleted job as it reads while its Runner still owes the deletion of its files, from the
 * snapshot that deletion keeps; or nothing, once its files are gone or when the hub has no such
 * job.
 */
async function readDeletedJob(db: Kysely<Database>, jobId: JobId): Promise<Job | undefined> {
  const row = await db
    .selectFrom("jobDeletion")
    .select([...DELETED_JOB_COLUMNS, "requestedAt"])
    .where("jobId", "=", jobId)
    .executeTakeFirst();
  // The deletion is requested as the job is deleted.
  return row && toDeletedJob(row, row.requestedAt, row.requestedAt);
}

/** What a list of jobs is filtered by, each filter optional, and where its page starts. */
export interface JobListQuery {
  statuses: readonly JobStatus[];
  labels?: Labels;
  batchId?: BatchId;
  customId?: string;
  after?: JobId;
  limit: number;
}

/**
 * A page of jobs matching `query`, newest first, after the job `query.after`: at most
 * `query.limit` of them, without their content, and whether more follow.
 */
export function listJobs(query: JobListQuery) {
  return Db.use((database) =>
    database.query(async (db) => {
      const { statuses, labels, batchId, customId, after, limit } = query;
      const rows = await db
        .selectFrom("job as j")
        .innerJoin("jobProgress as p", "p.jobId", "j.id")
        // Without the content a list leaves out.
        .select((eb) => [
          ...LISTED_JOB_COLUMNS.map((column) => `j.${column}` as const),
          ...(["totalTokens", "costUsd"] as const).map((column) => `p.${column}` as const),
          blockedByOf({
            status: eb.ref("j.status"),
            placedAt: eb.ref("j.placedAt"),
            batchId: eb.ref("j.batchId"),
            inCanary: eb.ref("j.inCanary"),
          }).as("blockedBy"),
        ])
        .$if(statuses.length > 0, (qb) =>
          qb.where("j.status", "=", (eb) => eb.fn.any(eb.val([...statuses]))),
        )
        .$if(labels !== undefined && Object.keys(labels).length > 0, (qb) =>
          qb.where((eb) =>
            eb("j.labels", "@>", eb.cast<Labels>(eb.val(JSON.stringify(labels)), "jsonb")),
          ),
        )
        .$if(batchId !== undefined, (qb) => qb.where("j.batchId", "=", batchId ?? null))
        .$if(customId !== undefined, (qb) => qb.where("j.customId", "=", customId ?? null))
        .$if(after !== undefined, (qb) => qb.where("j.id", "<", after ?? null))
        .orderBy("j.id", "desc")
        // One more than the page, to learn whether more follow.
        .limit(limit + 1)
        .execute();
      return {
        // Read without their content, which the contract's `ListedJob` leaves out of the answer.
        jobs: rows
          .slice(0, limit)
          .map((row) => toApiJob({ ...row, result: null, failureMessage: null, activity: null })),
        hasMore: rows.length > limit,
      } satisfies JobsPage;
    }),
  );
}

/** What a list reads of a job's row: its columns without its result and its failure's message. */
const LISTED_JOB_COLUMNS = [
  "id",
  "status",
  "createdAt",
  "startedAt",
  "endedAt",
  "runTimeoutSeconds",
  "modelBudgetUsd",
  "failureReason",
  "pendingControl",
  "pendingControlRequestedAt",
  "batchId",
  "customId",
  "labels",
] as const satisfies readonly (typeof JOB_COLUMNS)[number][];

/**
 * A page of the job `jobId`'s events after the position `after`, or a 404 error. Stored rows
 * each hold one report's events, so the page takes the rows after `after` in order, at most `limit`
 * of them and only as many as start within the byte budget, splits them into events, and drops
 * those at or before `after`.
 */
export function readJobEvents(jobId: JobId, after: EventId, limit: number) {
  return Effect.gen(function* () {
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
