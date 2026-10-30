import {
  type AddedJobs,
  type Agent,
  type Batch,
  type BatchCreation,
  BREAKER_DEFAULTS,
  ControlRefused,
  CustomIdReused,
  type JobSubmission,
  labelsSchema,
  NotFound,
} from "contract";
import { Array, Data, Effect, Order } from "effect";
import { type Kysely, type NotNull, sql, type Transaction } from "kysely";
import { v7 as uuidv7 } from "uuid";
import * as z from "zod/mini";

import { getOrganization } from "./auth/auth.ts";
import { Db, DB_SCHEMA, jsonRows } from "./db/database.ts";
import {
  BatchId,
  type Database,
  JobId,
  type JobStatus,
  type NewJob,
  type RunnerId,
  type UsageRecordId,
} from "./db/types.ts";
import { lockHeldBlobs, storeInputFiles, unheldFileProblems } from "./input-files.ts";
import {
  contentHashOf,
  insertUsageRecords,
  limitProblems,
  readJobs,
  outputContractProblems,
  type Submitter,
  UNFINISHED,
  USAGE_COPIED_COLUMNS,
} from "./jobs.ts";
import type { HubSettings } from "./settings.ts";

// Batches: every job belongs to one, the jobs added together, which run the Batch's agent and are
// released, paused and cancelled as one. A Batch's row keeps only what its jobs cannot tell: its
// agent, labels and breaker, its pause and its cancel, and the breaker's counts and Canary; its
// counts by status and its cost are read from its jobs.
//
// Locks: a Batch is locked before its jobs wherever both are: by a Batch's cancel, an add and a
// breaker's resume of it, by a report that ends jobs (several Batches in the order of their IDs),
// and by a job's resume. Placement locks jobs and reads their Batches without a lock, so it never
// waits on a Batch (ADR 0033).

/** Input a module refuses as the contract refuses invalid input, with each problem's path. */
export class InvalidInput extends Data.TaggedError("InvalidInput")<{
  problems: readonly { message: string; path: (string | number)[] }[];
}> {}

/** The breaker a creation asks for, as the Batch's row stores it: both null for none. */
function breakerOf(given: BatchCreation["breaker"]) {
  if (given === false) return { canary: null, maxFailurePercent: null };
  const tuned = given === true ? {} : given;
  return {
    canary: tuned.canary ?? BREAKER_DEFAULTS.canary,
    maxFailurePercent: tuned.maxFailurePercent ?? BREAKER_DEFAULTS.maxFailurePercent,
  };
}

/** The breaker a Batch's row stores, as the API reports it. */
function apiBreakerOf(row: { canary: number | null; maxFailurePercent: number | null }) {
  return row.canary === null || row.maxFailurePercent === null
    ? (false as const)
    : { canary: row.canary, maxFailurePercent: row.maxFailurePercent };
}

/**
 * The Batch `batchId` as the API reports it, with its jobs' counts and usage read from them; or
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
          .selectFrom("job as j")
          .innerJoin("jobProgress as p", "p.jobId", "j.id")
          .whereRef("j.batchId", "=", "b.id")
          .select((agg) => {
            const count = (status: JobStatus) =>
              agg.fn.countAll<number>().filterWhere("j.status", "=", status).as(status);
            return [
              count("queued"),
              count("running"),
              count("paused"),
              count("succeeded"),
              count("failed"),
              count("cancelled"),
              // Added up as Postgres's exact `numeric` and `bigint`, not in JavaScript.
              agg
                .cast<number>(agg.fn.coalesce(agg.fn.sum("p.totalTokens"), agg.lit(0)), "bigint")
                .as("totalTokens"),
              agg.fn.coalesce(agg.fn.sum<number>("p.costUsd"), agg.lit(0)).as("costUsd"),
            ];
          })
          .as("j"),
      (join) => join.onTrue(),
    )
    .select([
      "b.id",
      "b.customId",
      "b.createdAt",
      "b.labels",
      "b.canary",
      "b.maxFailurePercent",
      "b.pausedBy",
      "b.cancelledAt",
      "j.queued",
      "j.running",
      "j.paused",
      "j.succeeded",
      "j.failed",
      "j.cancelled",
      "j.totalTokens",
      "j.costUsd",
    ])
    .where("b.id", "=", batchId)
    .executeTakeFirst();
  if (!row) return undefined;
  const unfinished = row.queued + row.running + row.paused;
  // A cancel clears the pause, and a cancelled Batch takes no jobs and resumes none, so every
  // unfinished job of a cancelled Batch is one whose Runner has yet to stop it.
  const cancelWaits = row.cancelledAt !== null && unfinished > 0;
  return {
    id: row.id,
    customId: row.customId,
    createdAt: row.createdAt,
    status:
      row.cancelledAt !== null && !cancelWaits
        ? "cancelled"
        : row.pausedBy !== null
          ? "paused"
          : "active",
    pausedBy: row.pausedBy,
    ...(cancelWaits && row.cancelledAt !== null
      ? { pendingControl: { type: "cancel" as const, requestedAt: row.cancelledAt } }
      : {}),
    labels: row.labels,
    breaker: apiBreakerOf(row),
    jobCounts: {
      queued: row.queued,
      running: row.running,
      paused: row.paused,
      succeeded: row.succeeded,
      failed: row.failed,
      cancelled: row.cancelled,
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
 * What is wrong with `agent` that the contract's schemas cannot check, with paths from the
 * creation: its Output contract's schema, and limits above the hub's maximums.
 */
function agentProblems(agent: Agent, settings: HubSettings["jobLimits"]) {
  return [
    ...outputContractProblems(agent.outputContract.schema).map((message) => ({
      message,
      path: ["agent", "outputContract", "schema"],
    })),
    ...limitProblems(agent, settings).map(({ message, path }) => ({
      message,
      path: ["agent", ...path],
    })),
  ];
}

/**
 * Creates the Batch `creation` asks for, without jobs, and returns it; or fails with its problems,
 * or with a 422 error for a custom ID the Organization already used for a Batch with other content.
 * A creation with an earlier Batch's custom ID and the same content returns that Batch.
 */
export function createBatch(
  creation: BatchCreation,
  submitter: Submitter,
  settings: HubSettings["jobLimits"],
) {
  return Effect.gen(function* () {
    const problems = agentProblems(creation.agent, settings);
    if (problems.length > 0) return yield* Effect.fail(new InvalidInput({ problems }));
    const database = yield* Db;
    const organization = yield* getOrganization;
    const breaker = breakerOf(creation.breaker);
    const { customId, agent } = creation;
    // The content a creation under the same custom ID must repeat, with the breaker as it resolves,
    // so `true` and the defaults spelled out are the same.
    const contentHash =
      customId === undefined
        ? undefined
        : contentHashOf({ agent, labels: creation.labels, breaker: apiBreakerOf(breaker) });
    const created = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        const now = new Date();
        // A concurrent creation with the same custom ID waits here for the first to commit, then
        // inserts nothing; the select after it sees the committed Batch.
        const inserted = await trx
          .insertInto("batch")
          .values({
            id: BatchId(uuidv7()),
            organizationId: organization.id,
            customId,
            contentHash,
            instructions: agent.instructions,
            outputContract: agent.outputContract,
            runTimeoutSeconds: agent.runTimeoutSeconds ?? settings.runTimeoutSecondsDefault,
            modelBudgetUsd: agent.modelBudgetUsd ?? settings.modelBudgetUsdDefault,
            labels: creation.labels,
            ...breaker,
            canaryPlacesLeft: breaker.canary ?? 0,
            submitterKeyId: submitter.keyId,
            submitterKeyName: submitter.keyName,
            submitterMemberId: submitter.memberId,
            createdAt: now,
            pausedBy: null,
            pausedAt: null,
            breakerTrippedAt: null,
            cancelledAt: null,
          })
          .onConflict((conflict) =>
            conflict.constraint("batch_organizationId_customId_key").doNothing(),
          )
          .returning("id")
          .executeTakeFirst();
        if (inserted) return { id: inserted.id, same: true };
        // Only a creation with a custom ID can conflict.
        const existing = await trx
          .selectFrom("batch")
          .select(["id", "contentHash"])
          .where("organizationId", "=", organization.id)
          .where("customId", "=", customId ?? null)
          .executeTakeFirstOrThrow();
        return { id: existing.id, same: existing.contentHash === contentHash };
      }),
    );
    if (!created.same) {
      return yield* Effect.fail(
        new CustomIdReused({
          message: "The custom ID was already used for a Batch with other content",
        }),
      );
    }
    return yield* getBatch(created.id);
  });
}

/** `issues` as the API lists invalid input's problems: one per line, each with its path. */
function problemsText(issues: readonly { message: string; path: (string | number)[] }[]) {
  return z.prettifyError({
    issues: issues.map((issue) => ({ code: "custom" as const, ...issue })),
  });
}

/**
 * Input files as a content hash compares them: by path, so the same files in another order are the
 * same content, as they were when keyed by name.
 */
function filesHashed(files: JobSubmission["files"]) {
  return Array.sort(
    files,
    Order.mapInput(Order.String, (file: { path: string }) => file.path),
  );
}

/**
 * What is wrong with `jobs`, to be added to a Batch with `labels`, that the contract's schemas
 * cannot check, with paths from the request: a custom ID given twice, labels over the limit with
 * the Batch's, and a job with neither an input nor Input files.
 */
function jobProblems(jobs: readonly JobSubmission[], labels: Batch["labels"]) {
  const firstWithId = new Map<string, number>();
  return jobs.flatMap((job, index) => {
    const problems: { message: string; path: (string | number)[] }[] = [];
    const first = firstWithId.get(job.customId);
    if (first === undefined) firstWithId.set(job.customId, index);
    else {
      problems.push({
        message: `Must be unique in the request, as jobs[${first}] has it`,
        path: ["jobs", index, "customId"],
      });
    }
    // The Batch's labels and the job's own each pass, but together may be too many.
    const merged = z.safeParse(labelsSchema, { ...labels, ...job.labels });
    for (const issue of merged.success ? [] : merged.error.issues) {
      problems.push({
        message: `${issue.message}, the Batch's included`,
        path: ["jobs", index, "labels"],
      });
    }
    // A job with neither would be the same work as every other. Checked here, as Zod's `extend`
    // refuses to replace a key of a refined object, and the CLI's `jobs submit` replaces the
    // submission's `customId`.
    if (job.input === undefined && job.files.length === 0) {
      problems.push({
        message: "Must be given for a job without Input files",
        path: ["jobs", index, "input"],
      });
    }
    return problems;
  });
}

/**
 * Adds `jobs` to the Batch `batchId`, all or none, and returns the Batch with each job in the order
 * given and whether this add created it. A job whose custom ID the Batch already has with the same
 * content is the job already added. Fails with a 404 error for no such Batch; with the problems of
 * invalid jobs; then with a 409 error for a cancelled Batch; then with a 422 error for each custom
 * ID the Batch has for a job with other content. New jobs take the Batch's labels beneath their own
 * and its agent's limits, and the Canary's places left, in order.
 */
export function addJobs(batchId: BatchId, jobs: readonly JobSubmission[], submitter: Submitter) {
  return Effect.gen(function* () {
    const database = yield* Db;
    // A Batch's labels and agent never change, so they are read for the checks without a lock.
    const frozen = yield* database.query((db) =>
      db.selectFrom("batch").select("labels").where("id", "=", batchId).executeTakeFirst(),
    );
    if (!frozen) return yield* Effect.fail(new NotFound());
    const problems = jobProblems(jobs, frozen.labels);
    if (problems.length > 0) return yield* Effect.fail(new InvalidInput({ problems }));
    // Each job with its content as a later add under the same custom ID must repeat it.
    const given = jobs.map((job) => ({
      job,
      contentHash: contentHashOf({
        input: job.input,
        files: filesHashed(job.files),
        labels: job.labels,
      }),
    }));
    const added = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        // Locked first, so the Batch's controls and other adds come before or after this one, and
        // its Canary's places are taken once.
        const locked = await trx
          .selectFrom("batch")
          .select(["labels", "cancelledAt", "canaryPlacesLeft", "organizationId"])
          .where("id", "=", batchId)
          .forUpdate()
          .executeTakeFirstOrThrow();
        // Its files' blobs, locked against deletion before any job names them; content the
        // Organization does not hold is invalid input, refused before a cancel.
        const held = await lockHeldBlobs(
          trx,
          locked.organizationId,
          jobs.flatMap((job) => job.files),
        );
        const unheld = unheldFileProblems(jobs, held);
        if (unheld.length > 0) return { kind: "unheld" as const, unheld };
        // A cancelled Batch takes no jobs: a runnable one would undo the cancel unseen.
        if (locked.cancelledAt !== null) return { kind: "cancelled" as const };
        // The jobs the Batch already has under these custom IDs, locked in the order of their IDs
        // against their deletion, so the answer can read them back.
        const existing = await trx
          .selectFrom("job")
          .select(["id", "customId", "contentHash"])
          .where("batchId", "=", batchId)
          .where("customId", "=", (eb) => eb.fn.any(eb.val(jobs.map((job) => job.customId))))
          .orderBy("id")
          .forKeyShare()
          .execute();
        const byCustomId = new Map(existing.map((row) => [row.customId, row]));
        const reused = given.flatMap(({ job, contentHash }, index) => {
          const found = byCustomId.get(job.customId);
          return found && found.contentHash !== contentHash
            ? [
                {
                  message: "Already used in the Batch for a job with other content",
                  path: ["jobs", index, "customId"],
                },
              ]
            : [];
        });
        if (reused.length > 0) return { kind: "reused" as const, reused };
        // Each job given, in order: the one the Batch already has under its custom ID, or a new
        // one, whose ID, a UUID v7, is minted in the order given, which placement follows.
        const entries = given.map((entry) => {
          const found = byCustomId.get(entry.job.customId);
          return found
            ? { ...entry, id: found.id, created: false }
            : { ...entry, id: JobId(uuidv7()), created: true };
        });
        const fresh = entries.filter(({ created }) => created);
        if (fresh.length > 0) {
          await insertJobs(
            trx,
            batchId,
            fresh.map(({ id, job, contentHash }, order) => ({
              id,
              customId: job.customId,
              input: job.input,
              labels: { ...locked.labels, ...job.labels },
              contentHash,
              inCanary: order < locked.canaryPlacesLeft,
            })),
            submitter,
          );
          await storeInputFiles(
            trx,
            fresh.map(({ id, job }) => ({ jobId: id, files: job.files })),
          );
          const taken = Math.min(fresh.length, locked.canaryPlacesLeft);
          if (taken > 0) {
            await trx
              .updateTable("batch")
              .set((eb) => ({ canaryPlacesLeft: eb("canaryPlacesLeft", "-", taken) }))
              .where("id", "=", batchId)
              .execute();
          }
        }
        const read = await readJobs(
          trx,
          entries.map(({ id }) => id),
        );
        const readById = new Map(read.map((job) => [job.id, job]));
        // The Batch is never deleted, and every job is read in the transaction that found or
        // created it, under its lock.
        const batch = await readBatch(trx, batchId);
        if (!batch) throw new Error(`The Batch ${batchId} vanished under its lock`);
        return {
          kind: "added" as const,
          added: {
            batch,
            jobs: entries.map(({ id, created }) => {
              const job = readById.get(id);
              if (!job) throw new Error(`The job ${id} was not read back`);
              return { created, job };
            }),
          } satisfies AddedJobs,
        };
      }),
    );
    if (added.kind === "cancelled") {
      return yield* Effect.fail(
        new ControlRefused({ message: "The Batch is cancelled, so it takes no new jobs" }),
      );
    }
    if (added.kind === "unheld") {
      return yield* Effect.fail(new InvalidInput({ problems: added.unheld }));
    }
    if (added.kind === "reused") {
      return yield* Effect.fail(new CustomIdReused({ message: problemsText(added.reused) }));
    }
    return added.added;
  });
}

/**
 * Inserts the new `jobs` of the Batch `batchId`, given as one JSON parameter, in the transaction
 * `trx`: each queued, with the Batch's limits, copied in SQL, and with the progress row its
 * Runner's reports update, starting from nothing stored and nothing used.
 */
async function insertJobs(
  trx: Transaction<Database>,
  batchId: BatchId,
  jobs: Pick<NewJob, "id" | "customId" | "input" | "labels" | "contentHash" | "inCanary">[],
  submitter: Submitter,
) {
  const jobStatus = sql.id(DB_SCHEMA, "jobStatus");
  const now = new Date();
  await trx
    .with("inserted", (qb) =>
      qb
        .insertInto("job")
        .columns([
          "id",
          "organizationId",
          "batchId",
          "status",
          "input",
          "labels",
          "customId",
          "contentHash",
          "runTimeoutSeconds",
          "modelBudgetUsd",
          "inCanary",
          "submitterKeyId",
          "submitterKeyName",
          "submitterMemberId",
          "createdAt",
        ])
        .expression(
          qb
            .selectFrom((eb) => jsonRows(eb, jobs))
            .crossJoin("batch")
            .where("batch.id", "=", batchId)
            .select((eb) => {
              const field = (name: "id" | "customId" | "input" | "contentHash" | "inCanary") =>
                eb.ref("row.value", "->>").key(name);
              return [
                eb.cast<JobId>(field("id"), "uuid").as("id"),
                "batch.organizationId",
                "batch.id as batchId",
                eb.cast<JobStatus>(eb.val("queued"), jobStatus).as("status"),
                field("input").as("input"),
                eb.ref("row.value", "->").key("labels").as("labels"),
                field("customId").as("customId"),
                field("contentHash").as("contentHash"),
                "batch.runTimeoutSeconds",
                "batch.modelBudgetUsd",
                eb.cast<boolean>(field("inCanary"), "boolean").as("inCanary"),
                eb.val(submitter.keyId).as("submitterKeyId"),
                eb.cast<string | null>(eb.val(submitter.keyName), "text").as("submitterKeyName"),
                eb.val(submitter.memberId).as("submitterMemberId"),
                eb.cast<Date>(eb.val(now), "timestamptz").as("createdAt"),
              ];
            }),
        )
        .returning("id"),
    )
    .insertInto("jobProgress")
    .columns(["jobId"])
    .expression((eb) => eb.selectFrom("inserted").select("inserted.id"))
    .execute();
}

/** What a Batch control does: nothing, a refusal saying why, or a change. */
type BatchDecision =
  | { kind: "unchanged" }
  | { kind: "refused"; why: string }
  | { kind: "changed"; hints: (RunnerId | "live")[] };

/**
 * Starts a new Canary of the Batch `batchId`, of `canary` jobs, in the transaction `trx` that holds
 * the Batch's lock: the jobs of the last one leave it, the first `canary` jobs not yet sent to a
 * Runner join it in the order they were added, and the places left go to the jobs added next.
 * Returns how many places are left.
 */
async function startCanary(trx: Transaction<Database>, batchId: BatchId, canary: number) {
  const { joined } = await trx
    .with("candidate", (qb) =>
      qb
        .selectFrom("job")
        .select("id")
        .where("batchId", "=", batchId)
        .where("status", "=", "queued")
        .where("placedAt", "is", null)
        .orderBy("id")
        .limit(canary),
    )
    // The jobs leaving and joining, locked together in the order of their IDs, as jobs are locked
    // among themselves (ADR 0033). A candidate sent to a Runner while its lock waited no longer
    // matches once it is granted, and does not join.
    .with("locked", (qb) =>
      qb
        .selectFrom("job")
        .select((eb) => [
          "job.id",
          eb
            .and([
              eb("job.id", "in", eb.selectFrom("candidate").select("candidate.id")),
              eb("job.status", "=", "queued"),
              eb("job.placedAt", "is", null),
            ])
            .$castTo<boolean>()
            .as("joins"),
        ])
        .where("job.batchId", "=", batchId)
        .where((eb) =>
          eb.or([
            eb("job.inCanary", "=", true),
            eb.and([
              eb("job.id", "in", eb.selectFrom("candidate").select("candidate.id")),
              eb("job.status", "=", "queued"),
              eb("job.placedAt", "is", null),
            ]),
          ]),
        )
        .orderBy("job.id")
        .forUpdate(),
    )
    .with("flagged", (qb) =>
      qb
        .updateTable("job")
        .from("locked")
        .set((eb) => ({ inCanary: eb.ref("locked.joins") }))
        .whereRef("job.id", "=", "locked.id")
        .returning("locked.joins"),
    )
    .selectFrom("flagged")
    .select((eb) => eb.fn.countAll<number>().filterWhere("flagged.joins", "=", true).as("joined"))
    .executeTakeFirstOrThrow();
  return canary - joined;
}

/**
 * Pauses or resumes the Batch `batchId` and returns it, with the Runners to tell to sync now, or
 * fails with a 404 error or, for a cancelled Batch, a 409. A pause blocks the jobs not yet sent to
 * a Runner, and those added later. A resume releases them; a resume of the breaker's pause, or of a
 * caller's pause during which the breaker's rule tripped, restarts the breaker's counts and starts a
 * new Canary, since whoever releases it allows a new trial. Pausing a paused Batch, or resuming one
 * not paused, changes nothing.
 */
export function controlBatch(batchId: BatchId, control: "pause" | "resume") {
  return Effect.gen(function* () {
    const database = yield* Db;
    const controlled = yield* database.query((db) =>
      db.transaction().execute(async (trx) => {
        const batch = await trx
          .selectFrom("batch")
          .select(["pausedBy", "breakerTrippedAt", "cancelledAt", "canary"])
          .where("id", "=", batchId)
          .forUpdate()
          .executeTakeFirst();
        if (!batch) return undefined;
        const decision = ((): BatchDecision => {
          if (batch.cancelledAt !== null) return { kind: "refused", why: "The Batch is cancelled" };
          if (control === "pause") {
            return batch.pausedBy === null ? { kind: "changed", hints: [] } : { kind: "unchanged" };
          }
          return batch.pausedBy === null
            ? { kind: "unchanged" }
            : { kind: "changed", hints: ["live"] };
        })();
        if (decision.kind === "changed") {
          const now = new Date();
          if (control === "pause") {
            await trx
              .updateTable("batch")
              .set({ pausedBy: "caller", pausedAt: now })
              .where("id", "=", batchId)
              .execute();
          } else {
            const released = { pausedBy: null, pausedAt: null, breakerTrippedAt: null };
            const breakerResume = batch.pausedBy === "breaker" || batch.breakerTrippedAt !== null;
            if (breakerResume && batch.canary !== null) {
              const placesLeft = await startCanary(trx, batchId, batch.canary);
              await trx
                .updateTable("batch")
                .set((eb) => ({
                  ...released,
                  // The counts restart from the ends recorded after this, the Batch's lock keeping
                  // its jobs' counted ends, which take it first, before or after.
                  countedAfterUsageRecordId: eb.fn.coalesce(
                    eb
                      .selectFrom("usageRecord")
                      .select((max) => max.fn.max<UsageRecordId | null>("id").as("id")),
                    eb.lit(0),
                  ),
                  countedEnds: 0,
                  countedFailures: 0,
                  canaryPlacesLeft: placesLeft,
                }))
                .where("id", "=", batchId)
                .execute();
            } else {
              await trx.updateTable("batch").set(released).where("id", "=", batchId).execute();
            }
          }
        }
        return { decision, batch: await readBatch(trx, batchId) };
      }),
    );
    return yield* settle(controlled);
  });
}

/**
 * Cancels the Batch `batchId` and returns it, with the Runners to tell to sync now, or fails with a
 * 404 error. Each unfinished job is cancelled by a job's own rules: at once where no Runner may be
 * running it, that is one not yet sent to a Runner or paused, and otherwise by a cancel that waits
 * for its Runner, replacing a waiting pause. The cancel also ends the Batch's pause, and is final,
 * also for a Batch whose jobs have all ended: it takes no new jobs, and resumes none. Cancelling a
 * cancelled Batch changes nothing.
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
          .select("cancelledAt")
          .where("id", "=", batchId)
          .forUpdate()
          .executeTakeFirst();
        if (!batch) return undefined;
        if (batch.cancelledAt !== null) {
          return { decision: { kind: "unchanged" } as const, batch: await readBatch(trx, batchId) };
        }
        // Its unfinished jobs, locked in the order of their IDs, as a report and a Runner's removal
        // lock jobs.
        await trx
          .selectFrom("job")
          .select("id")
          .where("batchId", "=", batchId)
          .where("status", "in", UNFINISHED)
          .orderBy("id")
          .forUpdate()
          .execute();
        await trx
          .updateTable("batch")
          .set({ cancelledAt: now, pausedBy: null, pausedAt: null, breakerTrippedAt: null })
          .where("id", "=", batchId)
          .execute();
        // Jobs no Runner may be running end now, each with the Usage record of its end.
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
        // Jobs a Runner may be running wait for it.
        const waiting = await trx
          .updateTable("job")
          .set({ pendingControl: "cancel", pendingControlRequestedAt: now })
          .where("batchId", "=", batchId)
          .where("status", "in", ["queued", "running"])
          .where("placedAt", "is not", null)
          .where((eb) => eb("pendingControl", "is distinct from", "cancel"))
          .returning("runnerId")
          // Placed jobs have a Runner (job_placedAt_check).
          .$narrowType<{ runnerId: NotNull }>()
          .execute();
        const hints = Array.dedupe(waiting.map(({ runnerId }) => runnerId));
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
 * their IDs, before a report that ends them is applied, as a Batch is locked before its jobs
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
        .where("id", "=", eb.fn.any(eb.val([...jobIds]))),
    )
    .where("canary", "is not", null)
    .orderBy("id")
    .forUpdate()
    .execute();
  return rows.map(({ id }) => id);
}

/**
 * Adds, to the breaker's counts, the ends recorded in the Usage records `recorded`, in the
 * transaction `trx` that recorded them, and pauses each Batch they trip. An end counts when it
 * succeeded or failed; a cancelled end says nothing about the jobs' shared content. Only a report's
 * ends are counted: those the hub makes itself are cancels and Runner removals, whose
 * `runnerRemoved` failures say nothing about the content either. A job counts once, by its latest
 * end: a resume withdraws a failure counted since the Canary began (`controlJob`).
 *
 * A Batch trips when one of its Canary's jobs failed, or, once the Canary is over, when more than
 * `maxFailurePercent` of the jobs counted since it began failed, those of the Canary included. A
 * Batch a caller paused keeps counting without pausing, and records the trip for the caller's
 * resume, which then starts a new Canary as the breaker's would. `batches` are the Batches with a
 * breaker that `lockBreakerBatches` locked.
 */
export async function countEndsAndPauseTrippedBatches(
  trx: Transaction<Database>,
  recorded: readonly UsageRecordId[],
  batches: readonly BatchId[],
  now: Date,
) {
  if (recorded.length === 0 || batches.length === 0) return;
  await trx
    .with("counted", (qb) =>
      qb
        .selectFrom("usageRecord as u")
        .innerJoin("job as j", "j.id", "u.jobId")
        .select((eb) => {
          const failed = eb("u.status", "=", "failed");
          return [
            "u.batchId",
            eb.fn
              .countAll<number>()
              .filterWhere(eb.or([eb("u.status", "=", "succeeded"), failed]))
              .as("ends"),
            eb.fn.countAll<number>().filterWhere(failed).as("failures"),
            eb.fn
              .countAll<number>()
              .filterWhere(eb.and([failed, eb("j.inCanary", "=", true)]))
              .as("canaryFailures"),
          ];
        })
        .where("u.id", "=", (eb) => eb.fn.any(eb.val([...recorded])))
        .where("u.batchId", "=", (eb) => eb.fn.any(eb.val([...batches])))
        .groupBy("u.batchId"),
    )
    .updateTable("batch as b")
    .from("counted as c")
    .set((eb) => {
      // In parentheses, as Kysely writes a binary operation without them.
      const ends = eb.parens(eb.ref("b.countedEnds"), "+", eb.ref("c.ends"));
      const failures = eb.parens(eb.ref("b.countedFailures"), "+", eb.ref("c.failures"));
      // The Canary is over once its places are filled and its jobs have ended. Until then the
      // percentage waits, so that a job sent before the Canary began, still ending, does not judge
      // the Batch in the Canary's place; its end is counted, and judged with the Canary's. The
      // places left count too: a resume of the breaker's pause that finds fewer jobs to flag than
      // the Canary's places leaves the rest to jobs added later, and a Canary with no jobs yet has
      // not run.
      const canaryOver = eb.and([
        eb("b.canaryPlacesLeft", "=", eb.lit(0)),
        eb.not(
          eb.exists(
            eb
              .selectFrom("job as canary")
              .select("canary.id")
              .whereRef("canary.batchId", "=", "b.id")
              .where("canary.inCanary", "=", true)
              .where("canary.status", "in", UNFINISHED),
          ),
        ),
      ]);
      const trips = eb.and([
        eb("b.cancelledAt", "is", null),
        eb.or([
          eb("c.canaryFailures", ">", eb.lit(0)),
          eb.and([
            canaryOver,
            eb(
              eb(failures, "*", eb.lit(100)),
              ">",
              eb(eb.ref("b.maxFailurePercent").$notNull(), "*", ends),
            ),
          ]),
        ]),
      ]);
      const pauses = eb.and([trips, eb("b.pausedBy", "is", null)]);
      return {
        countedEnds: ends,
        countedFailures: failures,
        pausedBy: eb
          .case()
          .when(pauses)
          .then(eb.val("breaker" as const))
          .else(eb.ref("b.pausedBy"))
          .end(),
        pausedAt: eb.case().when(pauses).then(eb.val(now)).else(eb.ref("b.pausedAt")).end(),
        breakerTrippedAt: eb
          .case()
          .when(eb.and([trips, eb("b.pausedBy", "=", "caller")]))
          .then(eb.fn.coalesce("b.breakerTrippedAt", eb.val(now)))
          .else(eb.ref("b.breakerTrippedAt"))
          .end(),
      };
    })
    .whereRef("b.id", "=", "c.batchId")
    .execute();
}
