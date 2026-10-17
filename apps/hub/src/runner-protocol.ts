import {
  createProtocolServer,
  hubNotifications,
  notification,
  runnerMethods,
  type DesiredState,
  type JobContent,
  type JobReport,
  type JsonValue,
  type Load,
  type Placement,
} from "contract";
import { type Context, Effect } from "effect";
import { type ExpressionBuilder, sql } from "kysely";
import { jsonBuildObject } from "kysely/helpers/postgres";

import { Db, DB_SCHEMA } from "./db/database.ts";
import { type Database, JobId, type JobState, type RunnerId } from "./db/types.ts";
import { recordRunnerHello, recordRunnerSeen, SYNC_INTERVAL_MS } from "./runners.ts";

// The hub's side of the Runner protocol (ADR 0026), shared by both hubs: each answers a Runner's
// message by calling `handleRunnerMessage` with the Runner's identity and a database. A sync or a
// report is answered by one statement that records what the Runner said and reads its desired
// state: every unfinished job placed on it, with the content of those it does not hold, and queued
// jobs newly placed on its free slots.

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
}

/** Runs `effect` with the context's services. */
function run<A, E>(context: RunnerMessageContext, effect: Effect.Effect<A, E, Db>): Promise<A> {
  return Effect.runPromise(Effect.provide(effect, context.services));
}

/** A report as the statement below reads it: the outcome's fields beside the placement's. */
interface ReportRow {
  jobId: JobId;
  generation: number;
  state: JobState;
  startedAt: string | null;
  endedAt: string | null;
  /** The result as JSON text, or null when the report carries none. */
  result: string | null;
  /** The report's place in the order sent. */
  n: number;
}

function reportRow(report: JobReport, n: number): ReportRow {
  const { jobId, generation, outcome } = report;
  const row = { jobId: JobId(jobId), generation, state: outcome.state, n };
  return outcome.state === "running"
    ? { ...row, startedAt: outcome.startedAt, endedAt: null, result: null }
    : {
        ...row,
        startedAt: null,
        endedAt: outcome.endedAt,
        result: outcome.result === undefined ? null : JSON.stringify(outcome.result),
      };
}

/**
 * A set of rows from the caller, as one JSON parameter the statement expands: each row's fields
 * are read with `->>` and cast to their types where the clause selects them.
 */
function jsonRows<Row extends object>(eb: ExpressionBuilder<Database, never>, rows: Row[]) {
  return eb
    .fn<{ value: Row }>("jsonb_array_elements", [eb.cast(eb.val(JSON.stringify(rows)), "jsonb")])
    .as("row");
}

/**
 * Records what became of the Runner's placements, as reported. Of several reports for one
 * placement, the last decides the state; the earliest start and the latest end are kept, so a
 * report repeated after a lost answer changes nothing.
 */
function applyReports(runnerId: RunnerId, reports: JobReport[]) {
  const jobState = sql.id(DB_SCHEMA, "jobState");
  const unfinished: JobState[] = ["queued", "running"];
  return Db.use((database) =>
    database.query(async (db) => {
      await db
        .with("report", (qb) =>
          qb
            .selectFrom((eb) => jsonRows(eb, reports.map(reportRow)))
            .select((eb) => {
              const field = (name: keyof ReportRow) => eb.ref("row.value", "->>").key(name);
              return [
                eb.cast<JobId>(field("jobId"), "uuid").as("jobId"),
                eb.cast<number>(field("generation"), "integer").as("generation"),
                eb.cast<JobState>(field("state"), jobState).as("state"),
                eb.cast<Date | null>(field("startedAt"), "timestamptz").as("startedAt"),
                eb.cast<Date | null>(field("endedAt"), "timestamptz").as("endedAt"),
                eb.cast<JsonValue | null>(field("result"), "jsonb").as("result"),
                eb.cast<number>(field("n"), "integer").as("n"),
              ];
            }),
        )
        // One row per placement reported: the last report's state and result, the earliest start
        // and the latest end.
        .with("latest", (qb) =>
          qb
            .selectFrom("report")
            .distinctOn(["jobId", "generation"])
            .select((eb) => [
              "jobId",
              "generation",
              "state",
              "result",
              eb.fn
                .min("startedAt")
                .over((ob) => ob.partitionBy(["jobId", "generation"]))
                .as("startedAt"),
              eb.fn
                .max("endedAt")
                .over((ob) => ob.partitionBy(["jobId", "generation"]))
                .as("endedAt"),
            ])
            .orderBy(["jobId", "generation", "n desc"]),
        )
        .updateTable("job as j")
        .from("latest as r")
        .set((eb) => ({
          state: eb.ref("r.state"),
          startedAt: eb.fn.coalesce("j.startedAt", "r.startedAt"),
          endedAt: eb.fn.coalesce("r.endedAt", "j.endedAt"),
          result: eb.fn.coalesce("r.result", "j.result"),
        }))
        .whereRef("j.id", "=", "r.jobId")
        .where("j.runnerId", "=", runnerId)
        .whereRef("j.generation", "=", "r.generation")
        // Each outcome moves the job forward once; anything else is stale or repeated.
        .where((eb) =>
          eb.or([
            eb.and([eb("r.state", "=", "running"), eb("j.state", "=", "queued")]),
            eb.and([eb("r.state", "=", "succeeded"), eb("j.state", "in", unfinished)]),
          ]),
        )
        .execute();
    }),
  );
}

/**
 * Places queued jobs on the Runner's free slots and reads its desired state: every unfinished job
 * placed on it, with the content of those the Runner does not hold. One statement, so the jobs just
 * placed come from the placement's `RETURNING`, as Postgres runs a `WITH` clause on the statement's
 * snapshot. Placement takes the oldest queued jobs without a Runner, locking each row it takes so
 * concurrent syncs take different jobs, as queue tables are read in Postgres (`FOR UPDATE SKIP
 * LOCKED`). Reports applied before this are seen; nothing ties the two statements together, since a
 * Runner has one request in flight and a lost answer makes it repeat reports that then change
 * nothing.
 */
function placeAndRead(runnerId: RunnerId, load: Load) {
  const now = new Date();
  const unfinished: JobState[] = ["queued", "running"];
  return Db.use((database) =>
    database.query(async (db): Promise<DesiredState> => {
      const rows = await db
        .with("placed", (qb) =>
          qb
            .updateTable("job")
            .set((eb) => ({
              runnerId,
              generation: eb("generation", "+", 1),
              placedAt: now,
            }))
            .where("id", "in", (eb) =>
              eb
                .selectFrom("job")
                .select("id")
                .where("state", "=", "queued")
                .where("runnerId", "is", null)
                .orderBy("id")
                .limit(load.free)
                .forUpdate()
                .skipLocked(),
            )
            .returning(["id", "generation", "prompt", "outputContract"]),
        )
        .with("held", (qb) =>
          qb
            .selectFrom((eb) => jsonRows(eb, load.held))
            .select((eb) => [
              eb.cast<JobId>(eb.ref("row.value", "->>").key("jobId"), "uuid").as("jobId"),
              eb
                .cast<number>(eb.ref("row.value", "->>").key("generation"), "integer")
                .as("generation"),
            ]),
        )
        .with("wanted", (qb) =>
          qb
            .selectFrom("job")
            .select(["id", "generation", "prompt", "outputContract"])
            .where("runnerId", "=", runnerId)
            .where("state", "in", unfinished)
            .unionAll((eb) =>
              eb.selectFrom("placed").select(["id", "generation", "prompt", "outputContract"]),
            ),
        )
        .selectFrom("wanted")
        .select((eb) => [
          "id",
          "generation",
          eb
            .case()
            .when(
              eb.exists(
                eb
                  .selectFrom("held")
                  .select("held.jobId")
                  .whereRef("held.jobId", "=", "wanted.id")
                  .whereRef("held.generation", "=", "wanted.generation"),
              ),
            )
            .then(eb.lit(null))
            .else(
              jsonBuildObject({
                prompt: eb.ref("prompt"),
                outputContract: eb.ref("outputContract"),
                inputFiles: eb
                  .selectFrom("inputFile")
                  .select((fileEb) =>
                    fileEb.fn
                      .coalesce(
                        // The files by name, as the contract carries them.
                        fileEb.fn<JobContent["inputFiles"]>("jsonb_object_agg", [
                          "name",
                          jsonBuildObject({ content: fileEb.ref("content") }),
                        ]),
                        fileEb.cast<JobContent["inputFiles"]>(fileEb.val("{}"), "jsonb"),
                      )
                      .as("files"),
                  )
                  .whereRef("inputFile.jobId", "=", "wanted.id")
                  .$asScalar(),
              }),
            )
            .end()
            .as("job"),
        ])
        .orderBy("id")
        .execute();
      return {
        placements: rows.map((row): Placement => ({
          jobId: row.id,
          generation: row.generation,
          intent: "run",
          ...(row.job === null ? {} : { job: row.job }),
        })),
      };
    }),
  );
}

/** Answers a Runner's message: the text to send back, if any. */
export const handleRunnerMessage = createProtocolServer<typeof runnerMethods, RunnerMessageContext>(
  runnerMethods,
  {
    hello: async ({ name }, context) => {
      await run(context, recordRunnerHello(context.runnerId, name, new Date()));
      return { syncIntervalMs: SYNC_INTERVAL_MS };
    },
    sync: async (load, context) => {
      if (context.recordSeen) await run(context, recordRunnerSeen(context.runnerId, new Date()));
      return await run(context, placeAndRead(context.runnerId, load));
    },
    report: async ({ jobs, ...load }, context) => {
      if (context.recordSeen) await run(context, recordRunnerSeen(context.runnerId, new Date()));
      await run(context, applyReports(context.runnerId, jobs));
      return await run(context, placeAndRead(context.runnerId, load));
    },
  },
);

/** The notification telling a Runner to sync now. */
export const SYNC_NOW = notification(hubNotifications, "syncNow", {});
