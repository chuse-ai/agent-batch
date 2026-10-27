import {
  createJsonRpcServer,
  hubNotifications,
  JOB_CANCELLED,
  JOB_FAILED,
  JOB_PAUSED,
  JOB_RECOVERED,
  JOB_RESUMED,
  JOB_STARTED,
  JOB_SUCCEEDED,
  intentSchema,
  isoDate,
  jobEndingSchema,
  type JobFailure,
  jobFailureSchema,
  jobOpeningSchema,
  jobReportSchema,
  jobSuccessSchema,
  notification,
  runnerRequests,
  type DesiredState,
  type JobReport,
  type JobActivity,
  type JobUsage,
  type JsonValue,
  type Load,
} from "contract";
import { Array, type Context, Effect, Result } from "effect";
import { type Expression, type Kysely, sql } from "kysely";
import { jsonArrayFrom, jsonBuildObject } from "kysely/helpers/postgres";
import * as z from "zod/mini";

import { lockBreakerBatches, countEndsAndPauseTrippedBatches } from "./batches.ts";
import { Db, DB_SCHEMA, jsonRows } from "./db/database.ts";
import {
  type Database,
  JobId,
  type JobFailureReason,
  type JobStatus,
  type RunnerId,
} from "./db/types.ts";
import {
  FINAL_STATUSES,
  insertUsageRecords,
  QUEUED_OR_RUNNING,
  USAGE_COPIED_COLUMNS,
  blockedByOf,
} from "./jobs.ts";
import { recordRunnerHello, recordRunnerSeen, SYNC_INTERVAL_MS } from "./runners.ts";

// The hub's side of the Runner protocol's socket (ADR 0026), shared by both hubs: each answers a
// Runner's message by calling `handleRunnerMessage` with the Runner's identity and a database. A sync or a
// report is answered by one statement that records what the Runner said and reads its desired
// state: every queued or running job placed on it, with the content of those it does not hold,
// queued jobs newly placed on its free slots, and the deletions of deleted jobs' files it owes.

/** The statuses that close a run: the job's end, or its pause. */
const CLOSING_STATUSES: JobStatus[] = [...FINAL_STATUSES, "paused"];

/** What a Runner's message is handled with. */
export interface RunnerMessageContext {
  runnerId: RunnerId;
  services: Context.Context<Db>;
  /**
   * Whether to record this message as when the Runner was last seen. The connection's holder says
   * so at most once per sync interval, so a busy Runner's reports do not write on every message; a
   * hello is always recorded.
   */
  recordSeen: boolean;
  /** How often Runners report their jobs' new events, as the hub's deployment sets it. */
  reportIntervalMs: number;
}

/** One model's or one tool's usage in a job's usage totals, as pi counts it. */
type Usage = JobUsage["models"][string];

/** Runs `effect` with the context's services. */
function runWithServices<A, E>(
  context: RunnerMessageContext,
  effect: Effect.Effect<A, E, Db>,
): Promise<A> {
  return Effect.runPromise(Effect.provide(effect, context.services));
}

/** A job's report as the statement below reads it, with what the hub derives from its events. */
interface ReportRow {
  jobId: JobId;
  runId: string;
  position: number;
  /** The events as JSON Lines, each line ending in `\n`, or null when there are none. */
  events: string | null;
  lastId: number | null;
  /** The usage, absent when the report carries none. */
  usage?: JobUsage;
  /** The activity, absent when the report carries none. */
  activity?: JobActivity;
  /** The job's status and times, or null when the report carries no status. */
  status: JobStatus | null;
  startedAt: string | null;
  endedAt: string | null;
  /**
   * Why the job failed, null unless it did, and what the cause said, null too when the cause gave no
   * text.
   */
  failureReason: JobFailureReason | null;
  failureMessage: string | null;
  /** The result, absent unless the report carries a success; `null` is itself a result. */
  result?: JsonValue;
  /**
   * Whether the Runner never started the job, so no Runner holds any of its work: a job held so
   * may be resumed on any Runner.
   */
  neverStarted: boolean;
}

function reportRow(report: JobReport, now: Date): ReportRow {
  const { jobId, runId, position, events, usage, activity } = report;
  const row = {
    jobId: JobId(jobId),
    runId,
    position,
    // `JSON.stringify` writes no raw newline inside a string, so each event is one line.
    events: events.length > 0 ? events.map((event) => `${JSON.stringify(event)}\n`).join("") : null,
    lastId: events.at(-1)?.id ?? null,
    usage,
    activity,
  };
  return { ...row, ...lifecycleOf(report, now) };
}

/** The kinds of the Runner's entries that end a job, with the status each ends it in. */
const ENDS: ReadonlyMap<string, JobStatus> = new Map([
  [JOB_SUCCEEDED, "succeeded"],
  [JOB_FAILED, "failed"],
  [JOB_CANCELLED, "cancelled"],
]);

/**
 * What a report says of its job's lifecycle, read from the Runner's own entries among its
 * events: a start, a recovery or a resume makes the job running, a pause makes it paused, and its
 * end, one kind per outcome, makes it succeeded, with its result, failed or cancelled. Each
 * applies with the events that carry it, so a job pauses or ends only once every entry before it is
 * stored, and a hub that loses one loses those events too and is sent both again. A job that failed
 * without its journal recording an end says so in `failure`. A job its Runner never started has no
 * journal, so its pause or cancel arrives with no events at the start of the journal; the hub holds
 * or ends the job itself, at `now`.
 */
function lifecycleOf(
  report: JobReport,
  now: Date,
): Pick<
  ReportRow,
  | "status"
  | "startedAt"
  | "endedAt"
  | "failureReason"
  | "failureMessage"
  | "result"
  | "neverStarted"
> {
  const records = report.events.flatMap((event) => (event.type === "entry" ? [event.record] : []));
  const started = records.find((record) => record.kind === JOB_STARTED);
  const startedAt = started ? isoDate(z.parse(jobOpeningSchema, started.data).timestamp) : null;
  const base = {
    startedAt,
    endedAt: null,
    failureReason: null,
    failureMessage: null,
    neverStarted: false,
  };
  const failed = ({ timestamp, reason, message }: JobFailure) => ({
    ...base,
    status: "failed" as const,
    endedAt: isoDate(timestamp),
    failureReason: reason,
    failureMessage: message ?? null,
  });
  const end = records.find((record) => ENDS.has(record.kind));
  const status = end && ENDS.get(end.kind);
  if (end && status) {
    const ended = { ...base, status };
    if (status === "succeeded") {
      const { timestamp, result } = z.parse(jobSuccessSchema, end.data);
      return { ...ended, endedAt: isoDate(timestamp), result };
    }
    if (status === "failed") return failed(z.parse(jobFailureSchema, end.data));
    return { ...ended, endedAt: isoDate(z.parse(jobEndingSchema, end.data).timestamp) };
  }
  const pause = records.find((record) => record.kind === JOB_PAUSED);
  if (pause) {
    return { ...base, status: "paused" };
  }
  if (report.failure) return failed(report.failure);
  if (
    (report.intent === "pause" || report.intent === "cancel") &&
    report.position === 0 &&
    report.events.length === 0
  ) {
    return report.intent === "pause"
      ? { ...base, status: "paused", neverStarted: true }
      : { ...base, status: "cancelled", endedAt: now.toISOString(), neverStarted: true };
  }
  const opened = records.some(
    (record) =>
      record.kind === JOB_STARTED || record.kind === JOB_RECOVERED || record.kind === JOB_RESUMED,
  );
  return { ...base, status: opened ? "running" : null };
}

/**
 * Applies the Runner's reports, each to its job while the reported run is the job's current one on
 * the Runner, in one statement (`applyReportsStatement`). A report that moves a job's status runs in
 * a transaction that first locks what the statement will change, and counts the ends it records in
 * their Batches' breakers; one that moves none runs alone, a single round trip.
 */
function applyReports(runnerId: RunnerId, reports: JobReport[]) {
  const now = new Date();
  const rows = reports.map((report) => reportRow(report, now));
  const withStatus = rows.flatMap(({ jobId, status }) =>
    status === null ? [] : [{ jobId, status }],
  );
  return Db.use((database) =>
    database.query(async (outer) => {
      // Nothing ends, so the breaker has nothing to count, and the statement's own lock on the
      // Runner's row orders it against a removal of the Runner.
      if (withStatus.length === 0) {
        await applyReportsStatement(outer, runnerId, rows, now);
        return;
      }
      // One transaction, so the ends a report records and the breaker's counts of them go together.
      await outer.transaction().execute(async (db) => {
        // The Runner's row first, as its removal deletes it before it ends the Runner's jobs, so
        // a removal waits for this report or comes first, and the report then changes nothing.
        await db
          .selectFrom("runner")
          .select("id")
          .where("id", "=", runnerId)
          .forKeyShare()
          .execute();
        // The Batches whose breaker the reports' ends may count in, locked before their jobs
        // (ADR 0033).
        const batches = await lockBreakerBatches(
          db,
          withStatus.flatMap(({ jobId, status }) =>
            FINAL_STATUSES.includes(status) ? [jobId] : [],
          ),
        );
        // The jobs whose status a report may move, locked in the order of their IDs before the
        // statement updates them in the order of the reports, as a Batch's cancel and a Runner's
        // removal lock jobs in that order.
        await db
          .selectFrom("job")
          .select("id")
          .where("id", "=", (eb) => eb.fn.any(eb.val(withStatus.map(({ jobId }) => jobId))))
          .orderBy("id")
          .forUpdate()
          .execute();
        const recorded = await applyReportsStatement(db, runnerId, rows, now);
        await countEndsAndPauseTrippedBatches(
          db,
          recorded.map(({ id }) => id),
          batches,
          now,
        );
      });
    }),
  );
}

/**
 * The statement that applies the Runner's reports, read as `rows`, each to its job while the
 * reported run is the job's current one on the Runner, and returns the Usage records of the ends it
 * recorded. A job's usage applies when neither total is smaller than the stored one, as pi's totals
 * only grow. Its events, its position, its activity and its status apply only when the report's
 * position is the hub's, so a report repeated after a lost answer changes nothing, and only while
 * the job is queued or running, the statuses a report may move it from, so a page of its events
 * that says no more will come stays true, and a paused run's later reports change nothing but its
 * usage; the answer then gives the Runner the position to read after. An end clears the activity.
 * The job's progress row is locked while the statement reads it, so two reports of one job apply
 * one after the other.
 */
function applyReportsStatement(
  db: Kysely<Database>,
  runnerId: RunnerId,
  rows: ReportRow[],
  now: Date,
) {
  const jobStatus = sql.id(DB_SCHEMA, "jobStatus");
  const jobFailureReason = sql.id(DB_SCHEMA, "jobFailureReason");
  return insertUsageRecords(
    db
      .with("report", (qb) =>
        qb
          .selectFrom((eb) => jsonRows(eb, rows))
          .select((eb) => {
            const field = (name: keyof ReportRow) => eb.ref("row.value", "->>").key(name);
            const json = <Name extends "usage" | "activity" | "result">(name: Name) =>
              eb.ref("row.value", "->").key(name);
            return [
              eb.cast<JobId>(field("jobId"), "uuid").as("jobId"),
              eb.cast<string>(field("runId"), "uuid").as("runId"),
              eb.cast<number>(field("position"), "bigint").as("position"),
              eb.cast<string | null>(field("events"), "text").as("events"),
              eb.cast<number | null>(field("lastId"), "bigint").as("lastId"),
              json("usage").as("usage"),
              json("activity").as("activity"),
              eb.cast<JobStatus | null>(field("status"), jobStatus).as("status"),
              eb.cast<Date | null>(field("startedAt"), "timestamptz").as("startedAt"),
              eb.cast<Date | null>(field("endedAt"), "timestamptz").as("endedAt"),
              eb
                .cast<JobFailureReason | null>(field("failureReason"), jobFailureReason)
                .as("failureReason"),
              eb.cast<string | null>(field("failureMessage"), "text").as("failureMessage"),
              json("result").as("result"),
              eb.cast<boolean>(field("neverStarted"), "boolean").as("neverStarted"),
            ];
          }),
      )
      // Each report of a job's current run on the Runner, whether its position is
      // the hub's, and what the job's progress row holds once the report applies.
      .with("current", (qb) =>
        qb
          .selectFrom("report as r")
          .innerJoin("job as j", (join) =>
            join
              .onRef("j.id", "=", "r.jobId")
              .onRef("j.runId", "=", "r.runId")
              .on("j.runnerId", "=", runnerId),
          )
          .innerJoin("jobProgress as p", "p.jobId", "r.jobId")
          // A removed Runner's reports change nothing: its row is gone, and its jobs end with
          // it. The row is locked, so a removal waits for a report already being applied, whose
          // events then come before the end the removal gives the job.
          .where((eb) =>
            eb.exists(
              eb.selectFrom("runner").select("id").where("id", "=", runnerId).forKeyShare(),
            ),
          )
          // The reported totals: pi's usage for each model and each tool, added up exactly, as
          // `numeric`.
          .crossJoinLateral((eb) => {
            const amountsOf = (map: "models" | "tools") =>
              eb
                .selectFrom(
                  eb
                    .fn<{ value: Usage }>("jsonb_each", [eb.ref("r.usage", "->").key(map)])
                    .as("amount"),
                )
                .select("amount.value");
            return eb
              .selectFrom(amountsOf("models").unionAll(amountsOf("tools")).as("amount"))
              .select((sum) => [
                sum
                  .cast<number>(
                    sum.fn.coalesce(
                      sum.fn.sum(
                        sum.cast(sum.ref("amount.value", "->>").key("totalTokens"), "bigint"),
                      ),
                      sum.lit(0),
                    ),
                    "bigint",
                  )
                  .as("tokens"),
                sum.fn
                  .coalesce(
                    sum.fn.sum<number>(
                      sum.cast(sum.ref("amount.value", "->>").key("cost").key("total"), "numeric"),
                    ),
                    sum.lit(0),
                  )
                  .as("cost"),
              ])
              .as("totals");
          })
          .selectAll("r")
          .select((eb) => {
            // Events apply while the job may run, the statuses a report may move it from, so a
            // paused run's later reports change nothing but its usage.
            const fresh = eb.and([
              eb("p.position", "=", eb.ref("r.position")),
              eb("j.status", "in", QUEUED_OR_RUNNING),
            ]);
            const usageApplies = eb.and([
              eb("r.usage", "is not", null),
              eb("totals.tokens", ">=", eb.ref("p.totalTokens")),
              eb("totals.cost", ">=", eb.ref("p.costUsd")),
            ]);
            const afterUsage = <T>(reported: Expression<T>, stored: Expression<T>) =>
              eb.case().when(usageApplies).then(reported).else(stored).end();
            return [
              fresh.as("fresh"),
              eb
                .case()
                .when(fresh)
                .then(eb.fn.coalesce("r.lastId", "p.position"))
                .else(eb.ref("p.position"))
                .end()
                .as("nextPosition"),
              afterUsage(eb.ref("r.usage").$notNull(), eb.ref("p.usage")).as("nextUsage"),
              afterUsage(eb.ref("totals.tokens"), eb.ref("p.totalTokens")).as("nextTotalTokens"),
              afterUsage(eb.ref("totals.cost"), eb.ref("p.costUsd")).as("nextCostUsd"),
              // A paused or ended job is doing nothing, whatever its last report said.
              eb
                .case()
                .when(eb.and([fresh, eb("r.status", "in", CLOSING_STATUSES)]))
                .then(eb.lit(null))
                .when(eb.and([fresh, eb("r.activity", "is not", null)]))
                .then(eb.ref("r.activity"))
                .else(eb.ref("p.activity"))
                .end()
                .as("nextActivity"),
            ];
          })
          .forUpdate("p"),
      )
      .with("progress", (qb) =>
        qb
          .updateTable("jobProgress as p")
          .from("current as c")
          .set((eb) => ({
            position: eb.ref("c.nextPosition"),
            usage: eb.ref("c.nextUsage"),
            totalTokens: eb.ref("c.nextTotalTokens"),
            costUsd: eb.ref("c.nextCostUsd"),
            activity: eb.ref("c.nextActivity"),
          }))
          .whereRef("p.jobId", "=", "c.jobId")
          // A report that changes nothing leaves the row as it is. The totals change with the usage.
          .where((eb) =>
            eb.or([
              eb("c.nextPosition", "is distinct from", eb.ref("p.position")),
              eb("c.nextUsage", "is distinct from", eb.ref("p.usage")),
              eb("c.nextActivity", "is distinct from", eb.ref("p.activity")),
            ]),
          ),
      )
      .with("stored", (qb) =>
        qb
          .insertInto("jobEventChunk")
          .columns(["jobId", "lastId", "events", "storedAt"])
          .expression((eb) =>
            eb
              .selectFrom("current")
              .select((selectEb) => ["jobId", "lastId", "events", selectEb.val(now).as("storedAt")])
              .where("fresh", "=", true)
              .where("lastId", "is not", null)
              .$narrowType<{ lastId: number; events: string }>(),
          ),
      )
      .with("reported", (qb) =>
        qb
          .selectFrom("current")
          .select([
            "jobId",
            "status",
            "startedAt",
            "endedAt",
            "failureReason",
            "failureMessage",
            "result",
            "neverStarted",
            "nextTotalTokens",
            "nextCostUsd",
          ])
          .where("fresh", "=", true)
          .where("status", "is not", null)
          .$narrowType<{ status: JobStatus }>(),
      )
      .with("moved", (qb) =>
        qb
          .updateTable("job as j")
          .from("reported as r")
          .set((eb) => {
            // A cancel that was waiting when its Runner confirmed a pause ends the paused job at once,
            // as a cancel of a paused job does.
            const cancelledOnPause = eb.and([
              eb("r.status", "=", "paused"),
              eb("j.pendingControl", "=", "cancel"),
            ]);
            // A control waits until the job's status confirms it or the job ends first.
            const settled = eb("r.status", "in", CLOSING_STATUSES);
            const unlessSettled = <T>(stored: Expression<T>) =>
              eb.case().when(settled).then(eb.lit(null)).else(stored).end();
            const unlessNeverStarted = <T>(stored: Expression<T>) =>
              eb.case().when("r.neverStarted", "=", true).then(eb.lit(null)).else(stored).end();
            const ends = eb.or([eb("r.status", "in", FINAL_STATUSES), cancelledOnPause]);
            return {
              status: eb
                .case()
                .when(cancelledOnPause)
                .then(eb.cast<JobStatus>(eb.val("cancelled"), jobStatus))
                .else(eb.ref("r.status"))
                .end(),
              startedAt: eb.fn.coalesce("j.startedAt", "r.startedAt"),
              endedAt: eb
                .case()
                .when(cancelledOnPause)
                .then(eb.val(now))
                .else(eb.ref("r.endedAt"))
                .end(),
              failureReason: eb.ref("r.failureReason"),
              failureMessage: eb.ref("r.failureMessage"),
              result: eb.ref("r.result"),
              pendingControl: unlessSettled(eb.ref("j.pendingControl")),
              pendingControlRequestedAt: unlessSettled(eb.ref("j.pendingControlRequestedAt")),
              // A job held before it started has no work on any Runner, so it is no longer sent to
              // one, and a resume may place it anywhere.
              runnerId: unlessNeverStarted(eb.ref("j.runnerId")),
              placedAt: unlessNeverStarted(eb.ref("j.placedAt")),
              // An end's Usage record, below, counts the job's totals from here on.
              recordedTotalTokens: eb
                .case()
                .when(ends)
                .then(eb.ref("r.nextTotalTokens"))
                .else(eb.ref("j.recordedTotalTokens"))
                .end(),
              recordedCostUsd: eb
                .case()
                .when(ends)
                .then(eb.ref("r.nextCostUsd"))
                .else(eb.ref("j.recordedCostUsd"))
                .end(),
            };
          })
          .whereRef("j.id", "=", "r.jobId")
          // Each status moves the job forward once; anything else is stale or repeated. A pause or an
          // end applies to a queued job too: its start may come in the same report, or it failed
          // before its journal opened, or its Runner was told to pause or cancel it before starting
          // it.
          .where((eb) =>
            eb.or([
              eb.and([eb("r.status", "=", "running"), eb("j.status", "=", "queued")]),
              eb.and([
                eb("r.status", "in", CLOSING_STATUSES),
                eb("j.status", "in", QUEUED_OR_RUNNING),
              ]),
            ]),
          )
          .returning(["j.id", ...USAGE_COPIED_COLUMNS.map((column) => `j.${column}` as const)]),
      )
      // Each job the statement ended gets the Usage record of its end, with its totals as the
      // report applied them.
      .with("ended", (qb) =>
        qb
          .selectFrom("moved as m")
          .innerJoin("current as c", "c.jobId", "m.id")
          // The job's row as the statement found it, before it ended: what its records counted.
          .innerJoin("job as before", "before.id", "m.id")
          .select([
            "m.id as jobId",
            ...USAGE_COPIED_COLUMNS.map((column) => `m.${column}` as const),
            "before.recordedTotalTokens",
            "before.recordedCostUsd",
            "c.nextTotalTokens as jobTotalTokens",
            "c.nextCostUsd as jobCostUsd",
          ])
          .where("m.status", "in", FINAL_STATUSES),
      ),
  )
    .returning("id")
    .execute();
}

/**
 * Records the deletions of deleted jobs' files the Runner reports done: each goes from those
 * the Runner owes, and the job's latest Usage record keeps the outcome, `confirmed`, with its time.
 * A deletion reported again after that changes nothing.
 */
function confirmDeletions(runnerId: RunnerId, deleted: string[]) {
  const now = new Date();
  return Db.use((database) =>
    database.query((db) =>
      db
        .with("confirmed", (qb) =>
          qb
            .deleteFrom("jobDeletion")
            // The job IDs as one array parameter, however many the Runner reports.
            .where((eb) => eb("jobId", "=", eb.fn.any(eb.val(deleted.map((id) => JobId(id))))))
            .where("runnerId", "=", runnerId)
            .returning("usageRecordId"),
        )
        .updateTable("usageRecord as u")
        .from("confirmed as c")
        .set({ hostDeletionOutcome: "confirmed", hostDeletionOutcomeAt: now })
        .whereRef("u.id", "=", "c.usageRecordId")
        .execute(),
    ),
  );
}

/** What a run of a job is sent from: the job, its run, its Batch, its input, its control and its limits. */
const RUN_COLUMNS = [
  "id",
  "runId",
  "batchId",
  "input",
  "pendingControl",
  "runTimeoutSeconds",
  "modelBudgetUsd",
] as const;

/**
 * Places queued jobs on the Runner's free slots and reads its desired state: every queued or running
 * job placed on it, under the intent its pending control gives, with the content of those the
 * Runner does not hold and is to run, and the deleted jobs whose files it owes the deletion of.
 * One statement, so the jobs just placed come from the
 * placement's `RETURNING`, as Postgres runs a `WITH` clause on the statement's snapshot. Placement
 * takes the oldest queued jobs not yet sent to a Runner, new ones without a Runner and resumed ones
 * waiting for this Runner, which holds their journals, locking each row it takes so concurrent syncs
 * take different jobs, as queue tables are read in Postgres (`FOR UPDATE SKIP LOCKED`). Reports
 * applied before this are seen; nothing ties the two statements together, since a Runner has one
 * request in flight and a lost answer makes it repeat reports that then change nothing.
 *
 * The Runner's free slots count only the jobs it holds, so the queued or running jobs placed on it
 * that it does not hold are subtracted first: it starts them on this answer, as a restarted Runner
 * does with every job it ran. kube-scheduler likewise counts every pod assigned to a node, running or not.
 */
function placeAndRead(runnerId: RunnerId, load: Load) {
  const now = new Date();
  return Db.use((database) =>
    database.query(async (db): Promise<DesiredState> => {
      const rows = await db
        .with("held", (qb) =>
          qb
            .selectFrom((eb) => jsonRows(eb, load.held))
            .select((eb) => [
              eb.cast<JobId>(eb.ref("row.value", "->>").key("jobId"), "uuid").as("jobId"),
              eb.cast<string>(eb.ref("row.value", "->>").key("runId"), "uuid").as("runId"),
            ]),
        )
        .with("placed", (qb) =>
          qb
            .updateTable("job")
            .set((eb) => ({
              runnerId,
              // A new job's first run; a resume has already minted its own, for the Runner that
              // holds the journal.
              runId: eb
                .case()
                .when("runnerId", "is", null)
                .then(eb.fn<string>("gen_random_uuid", []))
                .else(eb.ref("runId"))
                .end(),
              placedAt: now,
            }))
            .where("id", "in", (eb) =>
              eb
                .selectFrom("job")
                .select("id")
                .where("status", "=", "queued")
                .where("placedAt", "is", null)
                .where((runnerEb) =>
                  runnerEb.or([
                    runnerEb("runnerId", "is", null),
                    runnerEb("runnerId", "=", runnerId),
                  ]),
                )
                // A job its Batch blocks waits: for the Batch's pause, a caller's or its breaker's,
                // or for its Canary. The Batch's row is read without a lock, so placement never
                // waits on a Batch: a placement already under way when a pause commits may still
                // send the jobs it took, as one that committed a moment before would have (ADR 0033).
                .where((batchEb) =>
                  batchEb(
                    blockedByOf({
                      status: batchEb.ref("job.status"),
                      placedAt: batchEb.ref("job.placedAt"),
                      batchId: batchEb.ref("job.batchId"),
                      inCanary: batchEb.ref("job.inCanary"),
                    }),
                    "is",
                    null,
                  ),
                )
                // A removed Runner takes no job, though its socket is still open while it closes. Its
                // row is locked, so a removal waits for this placement and then ends what it placed.
                .where((runnerEb) =>
                  runnerEb.exists(
                    runnerEb
                      .selectFrom("runner")
                      .select("id")
                      .where("id", "=", runnerId)
                      .forKeyShare(),
                  ),
                )
                .orderBy("id")
                .limit((limitEb) =>
                  limitEb.fn<number>("greatest", [
                    limitEb.lit(0),
                    limitEb(
                      limitEb.val(load.free),
                      "-",
                      limitEb
                        .selectFrom("job as unheld")
                        .select((countEb) => countEb.fn.countAll<number>().as("count"))
                        .where("unheld.runnerId", "=", runnerId)
                        .where("unheld.placedAt", "is not", null)
                        .where("unheld.status", "in", QUEUED_OR_RUNNING)
                        .where((heldEb) =>
                          heldEb.not(
                            heldEb.exists(
                              heldEb
                                .selectFrom("held")
                                .select("held.jobId")
                                .whereRef("held.jobId", "=", "unheld.id")
                                .whereRef("held.runId", "=", "unheld.runId"),
                            ),
                          ),
                        )
                        .$asScalar(),
                    ),
                  ]),
                )
                .forUpdate()
                .skipLocked(),
            )
            .returning(RUN_COLUMNS),
        )
        .with("wanted", (qb) =>
          qb
            .selectFrom("job")
            .select(RUN_COLUMNS)
            .where("runnerId", "=", runnerId)
            .where("placedAt", "is not", null)
            .where("status", "in", QUEUED_OR_RUNNING)
            .unionAll(qb.selectFrom("placed").select(RUN_COLUMNS)),
        )
        .selectNoFrom((answer) => [
          jsonArrayFrom(
            answer
              .selectFrom("wanted")
              .innerJoin("jobProgress", "jobProgress.jobId", "wanted.id")
              .innerJoin("batch", "batch.id", "wanted.batchId")
              .select((eb) => [
                "wanted.id",
                "wanted.runId",
                eb.fn
                  .coalesce(eb.cast<string>("wanted.pendingControl", "text"), eb.val("run"))
                  .as("intent"),
                "jobProgress.position",
                "wanted.runTimeoutSeconds",
                "wanted.modelBudgetUsd",
                eb
                  .case()
                  .when(
                    eb.or([
                      eb("wanted.pendingControl", "is not", null),
                      eb.exists(
                        eb
                          .selectFrom("held")
                          .select("held.jobId")
                          .whereRef("held.jobId", "=", "wanted.id")
                          .whereRef("held.runId", "=", "wanted.runId"),
                      ),
                    ]),
                  )
                  .then(eb.lit(null))
                  .else(
                    jsonBuildObject({
                      instructions: eb.ref("batch.instructions"),
                      input: eb.ref("wanted.input"),
                      resultContract: eb.ref("batch.resultContract"),
                      // The files, the agent's and the job's own, by name, SHA-256, size and chunk
                      // size, without their bytes, which the Runner downloads a chunk at a time.
                      inputFiles: jsonArrayFrom(
                        eb
                          .selectFrom((files) =>
                            files
                              .selectFrom("batchInputFile")
                              .select(["batchInputFile.name", "batchInputFile.sha256"])
                              .whereRef("batchInputFile.batchId", "=", "wanted.batchId")
                              .unionAll(
                                files
                                  .selectFrom("inputFile")
                                  .select(["inputFile.name", "inputFile.sha256"])
                                  .whereRef("inputFile.jobId", "=", "wanted.id"),
                              )
                              .as("file"),
                          )
                          .innerJoin("blob", "blob.sha256", "file.sha256")
                          .innerJoin("upload", "upload.sha256", "file.sha256")
                          .select((fileEb) => [
                            "file.name",
                            fileEb
                              .fn<string>("encode", ["blob.sha256", fileEb.val("hex")])
                              .as("sha256"),
                            "blob.originalSizeBytes",
                            "upload.chunkBytes",
                          ]),
                      ),
                    }),
                  )
                  .end()
                  .as("job"),
              ])
              .orderBy("wanted.id")
              // A job sent to a Runner has its run, minted when it was placed or resumed.
              .$narrowType<{ runId: string }>(),
          ).as("runs"),
          // The deleted jobs whose files the Runner still owes the deletion of.
          jsonArrayFrom(
            answer.selectFrom("jobDeletion").select("jobId").where("runnerId", "=", runnerId),
          ).as("deletions"),
        ])
        .executeTakeFirstOrThrow();
      return {
        runs: rows.runs.map((row) => ({
          jobId: row.id,
          runId: row.runId,
          intent: z.parse(intentSchema, row.intent),
          ...(row.job === null ? {} : { job: row.job }),
          position: row.position,
          runTimeoutSeconds: row.runTimeoutSeconds,
          modelBudgetUsd: row.modelBudgetUsd,
        })),
        deletions: rows.deletions.map(({ jobId }) => jobId),
      };
    }),
  );
}

/** Answers a Runner's message: the text to send back, if any. */
export const handleRunnerMessage = createJsonRpcServer<typeof runnerRequests, RunnerMessageContext>(
  runnerRequests,
  {
    hello: async ({ name }, context) => {
      await runWithServices(context, recordRunnerHello(context.runnerId, name, new Date()));
      return { syncIntervalMs: SYNC_INTERVAL_MS, reportIntervalMs: context.reportIntervalMs };
    },
    sync: async (load, context) => {
      if (context.recordSeen)
        await runWithServices(context, recordRunnerSeen(context.runnerId, new Date()));
      return await runWithServices(context, placeAndRead(context.runnerId, load));
    },
    report: async ({ jobs, deleted, ...load }, context) => {
      if (context.recordSeen)
        await runWithServices(context, recordRunnerSeen(context.runnerId, new Date()));
      // Each job's report is read on its own, so one the hub cannot apply is refused alone, and the
      // Runner's other jobs keep reporting.
      const [reports, refused] = Array.partition(jobs, (item) => {
        const parsed = z.safeParse(jobReportSchema, item);
        return parsed.success
          ? Result.succeed(parsed.data)
          : Result.fail({
              jobId: item.jobId,
              runId: item.runId,
              reason: z.prettifyError(parsed.error),
            });
      });
      if (refused.length > 0) {
        await runWithServices(
          context,
          Effect.logWarning("A Runner's job reports were refused", refused),
        );
      }
      if (reports.length > 0)
        await runWithServices(context, applyReports(context.runnerId, reports));
      if (deleted.length > 0)
        await runWithServices(context, confirmDeletions(context.runnerId, deleted));
      return { ...(await runWithServices(context, placeAndRead(context.runnerId, load))), refused };
    },
  },
);

/** The notification telling a Runner to sync now. */
export const SYNC_NOW = notification(hubNotifications, "syncNow", {});
