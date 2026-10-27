import canonicalize from "canonicalize";
import {
  type Batch,
  type BatchCreation,
  type BatchJobSubmission,
  type BatchSubmissionResult,
  ControlRefused,
  FAIL_FAST_DEFAULTS,
  IdempotencyKeyReused,
  type Job,
  labelsSchema,
  NotFound,
} from "contract";
import { Effect } from "effect";
import { type Kysely, sql, type Transaction } from "kysely";
import { v7 as uuidv7 } from "uuid";
import * as z from "zod/mini";

import { getOrganization } from "./auth/auth.ts";
import { Db, DB_SCHEMA } from "./db/database.ts";
import {
  BatchId,
  type Database,
  JobId,
  type JobStatus,
  type NewJob,
  type RunnerId,
  type UsageRecordId,
} from "./db/types.ts";
import { prepareInputFiles, storeInputFiles } from "./input-files.ts";
import {
  contentHashOf,
  FINAL_STATUSES,
  insertUsageRecords,
  limitProblems,
  readJobs,
  resultContractProblems,
  type Submitter,
  UNFINISHED,
  USAGE_COPIED_COLUMNS,
} from "./jobs.ts";
import type { HubSettings } from "./settings.ts";

// Batches: the jobs submitted together, released, paused and cancelled as one. A Batch's row keeps
// its settings, its hold, its cancel, the failure breaker's counts and what deleted members left
// behind; its counts by status and its cost are read from its members.
//
// Locks: a Batch is locked before its members wherever both are: by a Batch's cancel and a
// submission into it, by a report that ends members (several Batches in the order of their IDs),
// and by a member's resume and deletion. Placement locks members and reads their Batches without a
// lock, so it never waits on a Batch.

/** The failure breaker a creation asks for, as the Batch's row stores it: both null for none. */
function failFastOf(given: BatchCreation["failFast"]) {
  if (given === false) return { canarySize: null, tolerancePercent: null };
  const tuned = given === true ? {} : given;
  return {
    canarySize: tuned.canary ?? FAIL_FAST_DEFAULTS.canary,
    tolerancePercent: tuned.tolerance ?? FAIL_FAST_DEFAULTS.tolerance,
  };
}

/** What the hub reads of a Batch's row to report it. */
const BATCH_COLUMNS = [
  "id",
  "createdAt",
  "labels",
  "canarySize",
  "tolerancePercent",
  "pausedBy",
  "cancelledAt",
  "deletedJobs",
  "deletedTotalTokens",
  "deletedCostUsd",
] as const;

/**
 * The Batch `batchId` as the API reports it, with its members' counts and usage read from them; or
 * nothing when there is no such Batch.
 */
export async function readBatch(
  db: Kysely<Database>,
  batchId: BatchId,
): Promise<Batch | undefined> {
  const row = await db
    .selectFrom("batch as b")
    .innerJoinLateral(
      (eb) =>
        eb
          .selectFrom("job as m")
          .innerJoin("jobProgress as p", "p.jobId", "m.id")
          .whereRef("m.batchId", "=", "b.id")
          .select((agg) => {
            const count = (status: JobStatus) =>
              agg.fn.countAll<number>().filterWhere("m.status", "=", status).as(status);
            return [
              count("queued"),
              count("running"),
              count("paused"),
              count("succeeded"),
              count("failed"),
              count("cancelled"),
              agg
                .cast<number>(agg.fn.coalesce(agg.fn.sum("p.totalTokens"), agg.lit(0)), "bigint")
                .as("totalTokens"),
              agg.fn.coalesce(agg.fn.sum<number>("p.costUsd"), agg.lit(0)).as("costUsd"),
            ];
          })
          .as("m"),
      (join) => join.onTrue(),
    )
    .select((eb) => [
      ...BATCH_COLUMNS.map((column) => `b.${column}` as const),
      "m.queued",
      "m.running",
      "m.paused",
      "m.succeeded",
      "m.failed",
      "m.cancelled",
      // Added up as Postgres's exact `numeric` and `bigint`, not in JavaScript.
      eb(eb.ref("m.totalTokens"), "+", eb.ref("b.deletedTotalTokens")).as("totalTokens"),
      eb(eb.ref("m.costUsd"), "+", eb.ref("b.deletedCostUsd")).as("costUsd"),
    ])
    .where("b.id", "=", batchId)
    .executeTakeFirst();
  if (!row) return undefined;
  const unfinished = row.queued + row.running + row.paused;
  const finished = row.succeeded + row.failed + row.cancelled + row.deletedJobs;
  // A Batch with no members yet waits for its jobs, as between the requests that submit them.
  const status =
    row.cancelledAt !== null && unfinished === 0
      ? "cancelled"
      : row.pausedBy !== null
        ? "paused"
        : unfinished === 0 && finished > 0
          ? "ended"
          : "inProgress";
  return {
    id: row.id,
    createdAt: row.createdAt,
    status,
    ...(row.pausedBy === null ? {} : { pausedBy: row.pausedBy }),
    // A member's resume is refused while the cancel waits, so every unfinished member of a
    // cancelled Batch is one whose Runner has yet to stop it.
    ...(row.cancelledAt !== null && unfinished > 0
      ? { pendingControl: { type: "cancel" as const, requestedAt: row.cancelledAt } }
      : {}),
    labels: row.labels,
    failFast:
      row.canarySize === null || row.tolerancePercent === null
        ? false
        : { canary: row.canarySize, tolerance: row.tolerancePercent },
    jobCounts: {
      queued: row.queued,
      running: row.running,
      paused: row.paused,
      succeeded: row.succeeded,
      failed: row.failed,
      cancelled: row.cancelled,
      deleted: row.deletedJobs,
    },
    usage: { totalTokens: row.totalTokens, cost: { totalUsd: row.costUsd } },
  };
}

/** The Batch `batchId`, or a 404 error. */
export function getBatch(batchId: BatchId) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const batch = yield* database.query((db) => readBatch(db, batchId));
    if (!batch) return yield* Effect.fail(new NotFound());
    return batch;
  });
}

/**
 * Creates the Batch `creation` asks for and submits its jobs into it. A creation with the
 * idempotency key of an earlier Batch submits its jobs into that Batch if their labels and breaker
 * are the same, and fails otherwise.
 */
export function createBatch(
  creation: BatchCreation,
  submitter: Submitter,
  settings: HubSettings["jobLimits"],
) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const organization = yield* getOrganization;
    const failFast = failFastOf(creation.failFast);
    const batch = yield* database.query(async (db) => {
      // A concurrent creation with the same key waits here for the first to commit, then inserts
      // nothing; the select after it sees the committed Batch.
      const inserted = await db
        .insertInto("batch")
        .values({
          id: BatchId(uuidv7()),
          organizationId: organization.id,
          idempotencyKey: creation.idempotencyKey ?? null,
          labels: creation.labels,
          ...failFast,
          submitterKeyId: submitter.keyId,
          submitterKeyName: submitter.keyName,
          submitterMemberId: submitter.memberId,
          createdAt: new Date(),
          pausedBy: null,
          pausedAt: null,
          cancelledAt: null,
        })
        .onConflict((conflict) =>
          conflict.constraint("batch_organizationId_idempotencyKey_key").doNothing(),
        )
        .returning(["id", "labels", "canarySize", "tolerancePercent"])
        .executeTakeFirst();
      return (
        inserted ??
        (await db
          .selectFrom("batch")
          .select(["id", "labels", "canarySize", "tolerancePercent"])
          .where("organizationId", "=", organization.id)
          .where("idempotencyKey", "=", creation.idempotencyKey ?? null)
          .executeTakeFirstOrThrow())
      );
    });
    const same =
      canonicalize(batch.labels) === canonicalize(creation.labels) &&
      batch.canarySize === failFast.canarySize &&
      batch.tolerancePercent === failFast.tolerancePercent;
    if (!same) return yield* Effect.fail(new IdempotencyKeyReused());
    return yield* submitIntoBatch(batch.id, creation.jobs, submitter, settings);
  });
}

/** What became of one job a request submitted into a Batch, before its job is read. */
type Outcome =
  | { key: string; outcome: "created" | "existing"; jobId: JobId }
  | {
      key: string;
      outcome: "rejected";
      reason: "invalid" | "keyReused" | "batchCancelled";
      problems?: { message: string; path: string[] }[];
    };

/**
 * Submits `jobs` into the Batch `batchId`, each accepted or refused on its own, and returns the
 * Batch with what became of each, in order, and whether any was created; or fails with a 404 error.
 * A job whose key the Batch already has returns that job when its content is the same, and is
 * refused as `keyReused` otherwise, even when it was given twice in one request; a job the hub's
 * checks refuse is `invalid`, with its problems; a new job in a cancelled Batch is
 * `batchCancelled`. New jobs take the Batch's labels beneath their own, and fill its Canary's
 * places while no member of the Canary has ended.
 */
export function submitIntoBatch(
  batchId: BatchId,
  jobs: readonly BatchJobSubmission[],
  submitter: Submitter,
  settings: HubSettings["jobLimits"],
) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const organization = yield* getOrganization;
    const checked = jobs.map((job) => ({
      job,
      contentHash: contentHashOf({ ...job, key: undefined }),
      problems: [
        ...resultContractProblems(job.resultContract.schema).map((message) => ({
          message,
          path: ["resultContract", "schema"],
        })),
        ...limitProblems(job, settings),
      ],
    }));
    const valid = checked.filter(({ problems }) => problems.length === 0);
    const prepared = yield* prepareInputFiles(valid.map(({ job }) => job.inputFiles));
    const preparedOf = new Map(valid.map(({ job }, index) => [job, prepared[index]]));
    const submitted = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        // Locked first, so the Batch's controls and other submissions into it come before or after
        // this one, and its Canary's places are counted once.
        const batch = await trx
          .selectFrom("batch")
          .select(["id", "labels", "canarySize", "cancelledAt"])
          .where("id", "=", batchId)
          .forUpdate()
          .executeTakeFirst();
        if (!batch) return undefined;
        const keys = jobs.map((job) => job.key);
        const existing = await trx
          .selectFrom("job")
          .select(["id", "idempotencyKey", "contentHash"])
          .where("batchId", "=", batchId)
          .where("idempotencyKey", "=", (eb) => eb.fn.any(eb.val(keys)))
          .execute();
        const canary = await trx
          .selectFrom("job")
          .select((eb) => [
            eb.fn.countAll<number>().filterWhere("inCanary", "=", true).as("members"),
            eb.fn
              .countAll<number>()
              .filterWhere((where) =>
                where.and([where("inCanary", "=", true), where("status", "in", FINAL_STATUSES)]),
              )
              .as("ended"),
          ])
          .where("batchId", "=", batchId)
          .executeTakeFirstOrThrow();
        // Each key as the Batch has it, or as this request creates it.
        const known = new Map(
          existing.map((row) => [row.idempotencyKey ?? "", { id: row.id, hash: row.contentHash }]),
        );
        let canaryPlaces =
          batch.canarySize === null || canary.ended > 0 ? 0 : batch.canarySize - canary.members;
        const now = new Date();
        const created: { row: NewJob; files: (typeof prepared)[number] | undefined }[] = [];
        const outcomes = checked.map(({ job, contentHash, problems }): Outcome => {
          const { key } = job;
          const same = known.get(key);
          if (same) {
            return same.hash === contentHash
              ? { key, outcome: "existing", jobId: same.id }
              : { key, outcome: "rejected", reason: "keyReused" };
          }
          const labels = { ...batch.labels, ...job.labels };
          // The Batch's labels and the job's own each pass, but together may be too many.
          const merged = z.safeParse(labelsSchema, labels);
          const allProblems = merged.success
            ? problems
            : [
                ...problems,
                ...merged.error.issues.map((issue) => ({
                  message: `${issue.message}, the Batch's included`,
                  path: ["labels"],
                })),
              ];
          if (allProblems.length > 0) {
            return { key, outcome: "rejected", reason: "invalid", problems: allProblems };
          }
          // Creating runnable jobs would undo the cancel unseen.
          if (batch.cancelledAt !== null) {
            return { key, outcome: "rejected", reason: "batchCancelled" };
          }
          const id = JobId(uuidv7());
          const inCanary = canaryPlaces > 0;
          if (inCanary) canaryPlaces -= 1;
          created.push({
            row: {
              id,
              organizationId: organization.id,
              batchId,
              status: "queued",
              prompt: job.prompt,
              resultContract: job.resultContract,
              labels,
              runTimeoutSeconds: job.runTimeoutSeconds ?? settings.runTimeoutSecondsDefault,
              modelBudgetUsd: job.modelBudgetUsd ?? settings.modelBudgetUsdDefault,
              idempotencyKey: key,
              contentHash,
              inCanary,
              submitterKeyId: submitter.keyId,
              submitterKeyName: submitter.keyName,
              submitterMemberId: submitter.memberId,
              createdAt: now,
            },
            files: preparedOf.get(job),
          });
          known.set(key, { id, hash: contentHash });
          return { key, outcome: "created", jobId: id };
        });
        if (created.length > 0) {
          await trx
            .insertInto("job")
            .values(created.map(({ row }) => row))
            .execute();
          // Reports from each job's Runner update its row, starting from nothing stored and nothing
          // used.
          await trx
            .insertInto("jobProgress")
            .values(created.map(({ row }) => ({ jobId: row.id })))
            .execute();
          await storeInputFiles(
            trx,
            created.flatMap(({ row, files }) =>
              files ? [{ jobId: row.id, prepared: files }] : [],
            ),
          );
        }
        const read = await readJobs(
          trx,
          outcomes.flatMap((outcome) => (outcome.outcome === "rejected" ? [] : [outcome.jobId])),
        );
        const batchRead = await readBatch(trx, batchId);
        return { outcomes, read, batch: batchRead, created: created.length > 0 };
      }),
    );
    if (!submitted?.batch) return yield* Effect.fail(new NotFound());
    const jobsById = new Map<string, Job>(submitted.read.map((job) => [job.id, job]));
    const result: BatchSubmissionResult = {
      batch: submitted.batch,
      jobs: submitted.outcomes.map((outcome) => {
        if (outcome.outcome === "rejected") return outcome;
        const job = jobsById.get(outcome.jobId);
        // Read in the transaction that created or found it, so it is there.
        if (!job) throw new Error(`The job ${outcome.jobId} was not read back`);
        return { key: outcome.key, outcome: outcome.outcome, job };
      }),
    };
    return { result, created: submitted.created };
  });
}

/** What a Batch control does: nothing, a refusal saying why, or a change. */
type BatchDecision =
  | { kind: "unchanged" }
  | { kind: "refused"; why: string }
  | { kind: "changed"; hints: (RunnerId | "live")[] };

/**
 * Pauses or resumes the Batch `batchId` and returns it, or fails with a 404 error or, for a
 * cancelled Batch, a 409. A pause holds the members not yet sent to a Runner, and those submitted
 * later; a resume releases them, and, for a pause the breaker made, starts the breaker's count
 * afresh, since whoever releases it allows a new trial. A resume of a caller's pause during which
 * the breaker's counts tripped leaves the Batch paused, by the breaker. Pausing a paused Batch, or resuming one not
 * paused, changes nothing.
 */
export function holdBatch(batchId: BatchId, control: "pause" | "resume") {
  return Effect.gen(function* () {
    const database = yield* Db;
    const controlled = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        const batch = await trx
          .selectFrom("batch")
          .select(["pausedBy", "cancelledAt", "tolerancePercent", "trialEnds", "trialFailures"])
          .where("id", "=", batchId)
          .forUpdate()
          .executeTakeFirst();
        if (!batch) return undefined;
        // A caller's pause keeps the breaker from pausing the Batch, not from counting, so ends
        // counted meanwhile that trip it leave the Batch paused by the breaker when the caller
        // resumes it, as the breaker's own rule in `recordTrialEndsAndTrip` decides.
        const tripped =
          batch.pausedBy === "caller" &&
          batch.tolerancePercent !== null &&
          batch.trialFailures >= 2 &&
          batch.trialFailures * 100 > batch.tolerancePercent * batch.trialEnds;
        const decision = ((): BatchDecision => {
          if (batch.cancelledAt !== null) return { kind: "refused", why: "The Batch is cancelled" };
          if (control === "pause") {
            return batch.pausedBy === null ? { kind: "changed", hints: [] } : { kind: "unchanged" };
          }
          if (batch.pausedBy === null) return { kind: "unchanged" };
          return { kind: "changed", hints: tripped ? [] : ["live"] };
        })();
        if (decision.kind === "changed") {
          const now = new Date();
          await trx
            .updateTable("batch")
            .set(
              control === "pause"
                ? { pausedBy: "caller", pausedAt: now }
                : tripped
                  ? { pausedBy: "failFast", pausedAt: now }
                  : {
                      pausedBy: null,
                      pausedAt: null,
                      ...(batch.pausedBy === "failFast" ? { trialEnds: 0, trialFailures: 0 } : {}),
                    },
            )
            .where("id", "=", batchId)
            .execute();
        }
        return { decision, batch: await readBatch(trx, batchId) };
      }),
    );
    return yield* settle(controlled);
  });
}

/**
 * Cancels the Batch `batchId` and returns it, with the Runners to tell to sync now, or fails with a
 * 404 error. Each unfinished member is cancelled by a job's own rules: at once where no Runner may
 * be running it, that is one not yet sent to a Runner or paused, and otherwise by a cancel that
 * waits for its Runner, replacing a waiting pause. The cancel also ends the Batch's pause, and new
 * jobs are refused from then on. Cancelling a cancelled Batch, or one whose members have all ended
 * and that is not paused, changes nothing.
 */
export function cancelBatch(batchId: BatchId) {
  const jobStatus = sql.id(DB_SCHEMA, "jobStatus");
  return Effect.gen(function* () {
    const database = yield* Db;
    const now = new Date();
    const controlled = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        const batch = await trx
          .selectFrom("batch")
          .select(["pausedBy", "cancelledAt", "deletedJobs"])
          .where("id", "=", batchId)
          .forUpdate()
          .executeTakeFirst();
        if (!batch) return undefined;
        // Its unfinished members, locked in the order of their IDs, as a report and a Runner's
        // removal lock jobs.
        const unfinished = await trx
          .selectFrom("job")
          .select("id")
          .where("batchId", "=", batchId)
          .where("status", "in", UNFINISHED)
          .orderBy("id")
          .forUpdate()
          .execute();
        // A Batch whose members have all ended has nothing to withdraw; one with no members yet is
        // cancelled, so that it takes none.
        const ended =
          unfinished.length === 0 &&
          batch.pausedBy === null &&
          (batch.deletedJobs > 0 ||
            (await trx
              .selectFrom("job")
              .select("id")
              .where("batchId", "=", batchId)
              .limit(1)
              .executeTakeFirst()) !== undefined);
        if (batch.cancelledAt !== null || ended) {
          return { decision: { kind: "unchanged" } as const, batch: await readBatch(trx, batchId) };
        }
        await trx
          .updateTable("batch")
          .set({ cancelledAt: now, pausedBy: null, pausedAt: null })
          .where("id", "=", batchId)
          .execute();
        // Members no Runner may be running end now, each with the Usage record of its end.
        await insertUsageRecords(
          trx
            .with("endedJob", (qb) =>
              qb
                .updateTable("job")
                .from("jobProgress")
                .whereRef("jobProgress.jobId", "=", "job.id")
                .set((eb) => ({
                  status: eb.cast<JobStatus>(eb.val("cancelled"), jobStatus),
                  endedAt: now,
                  failureReason: null,
                  failureMessage: null,
                  pendingControl: null,
                  pendingControlRequestedAt: null,
                  // The end's Usage record, below, counts the job's totals from here on.
                  recordedTotalTokens: eb.ref("jobProgress.totalTokens"),
                  recordedCostUsd: eb.ref("jobProgress.costUsd"),
                }))
                .where("job.batchId", "=", batchId)
                .where((eb) =>
                  eb.or([
                    eb("job.status", "=", "paused"),
                    eb.and([eb("job.status", "=", "queued"), eb("job.placedAt", "is", null)]),
                  ]),
                )
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
        ).execute();
        // Members a Runner may be running wait for it.
        const waiting = await trx
          .updateTable("job")
          .set({ pendingControl: "cancel", pendingControlRequestedAt: now })
          .where("batchId", "=", batchId)
          .where("status", "in", ["queued", "running"])
          .where("placedAt", "is not", null)
          .where((eb) => eb("pendingControl", "is distinct from", "cancel"))
          .returning("runnerId")
          .execute();
        const hints = [
          ...new Set(waiting.flatMap(({ runnerId }) => (runnerId === null ? [] : [runnerId]))),
        ];
        return {
          decision: { kind: "changed", hints } as const,
          batch: await readBatch(trx, batchId),
        };
      }),
    );
    return yield* settle(controlled);
  });
}

/** A Batch control's outcome as its caller gets it: the Batch and the Runners to hint, or an error. */
function settle(controlled: { decision: BatchDecision; batch: Batch | undefined } | undefined) {
  return Effect.gen(function* () {
    if (!controlled?.batch) return yield* Effect.fail(new NotFound());
    const { decision, batch } = controlled;
    if (decision.kind === "refused") {
      return yield* Effect.fail(new ControlRefused({ message: decision.why }));
    }
    return { batch, hints: decision.kind === "changed" ? decision.hints : [] };
  });
}

/**
 * Locks, in the transaction `trx`, the Batches with a breaker of the jobs `jobIds`, in the order of
 * their IDs, before a report that ends them is applied, as a Batch is locked before its members
 * (ADR 0033); returns their IDs.
 */
export async function lockBreakerBatches(trx: Transaction<Database>, jobIds: readonly JobId[]) {
  if (jobIds.length === 0) return [];
  const rows = await trx
    .selectFrom("batch")
    .select("id")
    .where("id", "in", (eb) =>
      eb
        .selectFrom("job")
        .select("batchId")
        .where("id", "=", eb.fn.any(eb.val([...jobIds])))
        .where("batchId", "is not", null)
        .$narrowType<{ batchId: BatchId }>(),
    )
    .where("canarySize", "is not", null)
    .orderBy("id")
    .forUpdate()
    .execute();
  return rows.map(({ id }) => id);
}

/**
 * Adds, to the failure breaker's counts, the ends recorded in the Usage records `recorded`, in the
 * transaction `trx` that recorded them, and pauses each Batch they trip. An end counts when it
 * succeeded or failed; cancelled ends say nothing about the jobs' shared content. Only a report's
 * ends are counted: those the hub makes itself are cancels and Runner removals, whose
 * `runnerRemoved` failures say nothing about the content either. A Batch trips once at least two of its counted ends since its trial began
 * failed and their share among them is above its tolerance, so a single early failure never
 * pauses it. `batches` are the Batches with a breaker that `lockBreakerBatches` locked.
 */
export async function recordTrialEndsAndTrip(
  trx: Transaction<Database>,
  recorded: readonly UsageRecordId[],
  batches: readonly BatchId[],
  now: Date,
) {
  if (recorded.length === 0 || batches.length === 0) return;
  await trx
    .with("counted", (qb) =>
      qb
        .selectFrom("usageRecord")
        .select((eb) => {
          const failed = eb("status", "=", "failed");
          return [
            "batchId",
            eb.fn
              .countAll<number>()
              .filterWhere(eb.or([eb("status", "=", "succeeded"), failed]))
              .as("ends"),
            eb.fn.countAll<number>().filterWhere(failed).as("failures"),
          ];
        })
        .where("id", "=", (eb) => eb.fn.any(eb.val([...recorded])))
        .where("batchId", "=", (eb) => eb.fn.any(eb.val([...batches])))
        .groupBy("batchId")
        .$narrowType<{ batchId: BatchId }>(),
    )
    .updateTable("batch as b")
    .from("counted as c")
    .set((eb) => {
      const ends = eb(eb.ref("b.trialEnds"), "+", eb.ref("c.ends"));
      const failures = eb(eb.ref("b.trialFailures"), "+", eb.ref("c.failures"));
      const trips = eb.and([
        eb("b.pausedBy", "is", null),
        eb("b.cancelledAt", "is", null),
        eb(failures, ">=", eb.lit(2)),
        eb(
          eb(failures, "*", eb.lit(100)),
          ">",
          eb(eb.ref("b.tolerancePercent").$notNull(), "*", ends),
        ),
      ]);
      return {
        trialEnds: ends,
        trialFailures: failures,
        pausedBy: eb
          .case()
          .when(trips)
          .then(eb.val("failFast" as const))
          .else(eb.ref("b.pausedBy"))
          .end(),
        pausedAt: eb.case().when(trips).then(eb.val(now)).else(eb.ref("b.pausedAt")).end(),
      };
    })
    .whereRef("b.id", "=", "c.batchId")
    .execute();
}
