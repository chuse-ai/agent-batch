import * as z from "zod/mini";

import {
  inputFilesSchema,
  jobIdSchema,
  jsonTextSchema,
  promptSchema,
  resultContractSchema,
  textSchema,
} from "./job.ts";

// The protocol between a Runner and the hub over the Runner's WebSocket (ADR 0026): JSON-RPC 2.0,
// one object per text message, with each method's params and result as schemas here. The Runner
// sends the requests and the hub answers each with the full desired state of the Runner's jobs; the
// hub itself only sends the notification that the Runner should sync now.

/**
 * The protocol version, as the WebSocket subprotocol the Runner offers and the hub selects. Within
 * a version only fields are added, which a receiver ignores; a new method or a new value in a closed
 * set bumps it. Before 1.0 the hub supports one version at a time.
 */
export const RUNNER_PROTOCOL_VERSION = "agent-batch.v1";
/** The versions this hub supports, newest first. */
export const RUNNER_PROTOCOL_VERSIONS = [RUNNER_PROTOCOL_VERSION] as const;

/**
 * The `code` in the body of the hub's 400 when a Runner offers no version the hub supports; the body
 * also lists the versions it `supported`. Either side upgrading, or the hub rolling back, resolves
 * it, so the Runner logs what to upgrade and keeps retrying as it does other refusals.
 */
export const PROTOCOL_UNSUPPORTED = "protocolUnsupported";

/**
 * The hub's refusal of a Runner's socket before it opens, as the Runner reads it: Problem Details
 * with a `code`, and for an unsupported version the versions the hub supports.
 */
export const socketRefusalSchema = z.looseObject({
  code: z.string(),
  supported: z.optional(z.array(z.string())),
});
export type SocketRefusal = z.infer<typeof socketRefusalSchema>;

/**
 * The version the hub selects from the `Sec-WebSocket-Protocol` header a Runner offers: the newest
 * the hub supports, or `undefined` when it supports none the Runner offered.
 */
export function selectProtocolVersion(offered: string | null): string | undefined {
  const versions = new Set((offered ?? "").split(",").map((version) => version.trim()));
  return RUNNER_PROTOCOL_VERSIONS.find((version) => versions.has(version));
}

/**
 * What the hub wants done with a run, which only ever moves forward: `run`, then `pause` or
 * `cancel`. Resuming a job is a new run.
 */
export const intentSchema = z.enum(["run", "pause", "cancel"]);
export type Intent = z.infer<typeof intentSchema>;

/**
 * One run of a job on the Runner, under the ID the hub gave it when it placed the job there or
 * resumed it. Its reports apply only while it is the job's current one.
 */
const runRefSchema = z.object({ jobId: jobIdSchema, runId: z.uuid() });
export type RunRef = z.infer<typeof runRefSchema>;

/**
 * Where the hub's copy of a job's journal ends: the ID of the last record it has stored, 0 before
 * any. Record IDs count up from 1 with gaps, in the order the journal committed them.
 */
const positionSchema = z.int().check(z.nonnegative());

/** A job as the Runner needs it to run the job: the submission's content, as submitted. */
export const jobContentSchema = z.object({
  prompt: promptSchema,
  inputFiles: inputFilesSchema,
  resultContract: resultContractSchema,
});
export type JobContent = z.infer<typeof jobContentSchema>;

/**
 * What the Runner is to do with one job. The job's content comes only with a run the Runner
 * said it does not hold and is to run, so a sync repeats runs but not their jobs' content.
 */
const runSchema = z.object({
  ...runRefSchema.shape,
  intent: intentSchema,
  job: z.optional(jobContentSchema),
  /** The hub's position in the job's journal, which the Runner reads after. */
  position: positionSchema,
});
export type Run = z.infer<typeof runSchema>;

/**
 * The hub's answer to a sync or a report: the full desired state of the Runner's jobs, and the
 * deleted jobs whose files it is to delete from its host, every run's.
 */
export const desiredStateSchema = z.object({
  runs: z.array(runSchema),
  deletions: z.array(jobIdSchema),
});
export type DesiredState = z.infer<typeof desiredStateSchema>;

/** What the Runner tells the hub about its load in every sync and report. */
const loadSchema = z.object({
  /** The runs the Runner holds, running or not. */
  held: z.array(runRefSchema),
  /** How many more jobs the Runner can take. */
  free: z.int().check(z.nonnegative()),
});
export type Load = z.infer<typeof loadSchema>;

/**
 * The kinds of the entries the Runner adds to a job's journal about the job, each naming what
 * happened to it: its start, each recovery after a Runner restart, each pause and resume, and its
 * end, one kind per outcome. The hub reads the job's lifecycle from them, as they reach it
 * with the job's other events.
 */
export const JOB_STARTED = "agent-batch.job.started";
export const JOB_RECOVERED = "agent-batch.job.recovered";
export const JOB_PAUSED = "agent-batch.job.paused";
export const JOB_RESUMED = "agent-batch.job.resumed";
export const JOB_SUCCEEDED = "agent-batch.job.succeeded";
export const JOB_FAILED = "agent-batch.job.failed";
export const JOB_CANCELLED = "agent-batch.job.cancelled";

/**
 * A time the Runner records in Unix milliseconds, within what the hub's `timestamptz` holds and a
 * date can show: from 1970 to the end of the year 9999.
 */
const timestampSchema = z.number().check(z.gte(0), z.lte(253_402_300_799_999));

/**
 * What a start, a recovery or a resume records: when, by the Runner's clock in Unix milliseconds as
 * pi-ai's messages keep time, under which run, and by which Runner.
 */
export const jobOpeningSchema = z.object({
  timestamp: timestampSchema,
  runId: z.uuid(),
  runnerVersion: z.string(),
});
export type JobOpening = z.infer<typeof jobOpeningSchema>;

/** What a pause records: when. Only a caller's pause request pauses a job. */
export const jobPauseSchema = z.object({ timestamp: timestampSchema });
export type JobPause = z.infer<typeof jobPauseSchema>;

/** What a success records: when, and the job's result. */
export const jobSuccessSchema = z.object({ timestamp: timestampSchema, result: jsonTextSchema });
export type JobSuccess = z.infer<typeof jobSuccessSchema>;

/** What a failure or a cancel records: when. Why a job failed comes with structured failure and limit reasons. */
export const jobEndingSchema = z.object({ timestamp: timestampSchema });
export type JobEnding = z.infer<typeof jobEndingSchema>;

/** A time the Runner keeps in Unix milliseconds, as pi does, as an ISO date like the job's others. */
export function isoDate(at: number): string {
  return new Date(at).toISOString();
}

/** The data of each of the Runner's own entries, by kind. */
const LIFECYCLE_DATA: ReadonlyMap<string, z.ZodMiniType> = new Map<string, z.ZodMiniType>([
  [JOB_STARTED, jobOpeningSchema],
  [JOB_RECOVERED, jobOpeningSchema],
  [JOB_RESUMED, jobOpeningSchema],
  [JOB_PAUSED, jobPauseSchema],
  [JOB_SUCCEEDED, jobSuccessSchema],
  [JOB_FAILED, jobEndingSchema],
  [JOB_CANCELLED, jobEndingSchema],
]);

/**
 * One of a job's events: a record of the job's journal, one of pi-durable's entries or
 * conversation records, unchanged under `record`. The record's fields are pi's, so `record` keeps
 * those this schema does not name.
 */
export const jobEventSchema = z
  .discriminatedUnion("type", [
    z.object({
      id: z.int().check(z.positive()),
      type: z.literal("entry"),
      record: z.looseObject({ kind: z.string() }),
    }),
    z.object({
      id: z.int().check(z.positive()),
      type: z.literal("conversation"),
      record: z.looseObject({}),
    }),
  ])
  .register(z.globalRegistry, { id: "JobEvent" });
export type JobEvent = z.infer<typeof jobEventSchema>;

/** One model's or tool's usage as pi-ai counts it; the hub reads its tokens and cost and keeps the rest. */
const usageSchema = z.looseObject({
  totalTokens: z.int(),
  cost: z.looseObject({ total: z.number() }),
});

/** A job's usage totals so far, per model (`provider/modelId`) and per tool, as pi reports them. */
export const jobUsageSchema = z.looseObject({
  models: z.record(z.string(), usageSchema),
  tools: z.record(z.string(), usageSchema),
});
export type JobUsage = z.infer<typeof jobUsageSchema>;

/**
 * A wait before another attempt: when it ends, and the error of the attempt that failed, a
 * provider's text, which the Runner makes text the hub's `jsonb` stores.
 */
const retrySchema = z.object({ at: z.iso.datetime(), error: textSchema });

/**
 * What one of a job's conversations is doing, from pi's live state (`pi.live`) and under pi's
 * names: the model call in progress, the tool calls not yet done and the compactions under way,
 * without what they have produced so far. pi records no time for a part's changes, so each part
 * has `changedAt`: when it last changed to what it is now, such as a new attempt, a retry wait
 * beginning or a tool call starting to run, as the Runner first saw it, up to a report interval
 * late. Times are the Runner's clock, as ISO dates like the job's own, where pi keeps Unix
 * milliseconds.
 */
export const conversationActivitySchema = z.object({
  conversationId: z.int().check(z.positive()),
  /** The model call: its attempt, and the wait before the next attempt after one failed. */
  generation: z.optional(
    z.object({
      attempt: z.int().check(z.positive()),
      retry: z.optional(retrySchema),
      changedAt: z.iso.datetime(),
    }),
  ),
  /** The tool calls of the current round that are not done, in call order. */
  tools: z.array(
    z.object({
      callId: z.string(),
      name: z.string(),
      status: z.enum(["pending", "running"]),
      changedAt: z.iso.datetime(),
    }),
  ),
  /** The compactions under way, and whether the model call waits for each. */
  compactions: z.array(
    z.object({
      reason: z.enum(["manual", "threshold", "overflow"]),
      blocking: z.boolean(),
      attempt: z.int().check(z.positive()),
      retry: z.optional(retrySchema),
      changedAt: z.iso.datetime(),
    }),
  ),
});
export type ConversationActivity = z.infer<typeof conversationActivitySchema>;

/** What a job's conversations doing anything are doing, one entry each; empty between steps. */
export const jobActivitySchema = z.array(conversationActivitySchema);
export type JobActivity = z.infer<typeof jobActivitySchema>;

/**
 * One of a job's events as a report brings it: the Runner's own entries carry the data their kind
 * says, which the hub reads the job's lifecycle from, so a report whose lifecycle cannot be read is
 * refused rather than misread. Stored events are read back as `jobEventSchema`, so a check
 * made here never makes a stored event unreadable.
 */
const reportedEventSchema = jobEventSchema.check(
  z.superRefine((event, context) => {
    const schema = event.type === "entry" ? LIFECYCLE_DATA.get(event.record.kind) : undefined;
    if (schema === undefined) return;
    const parsed = z.safeParse(schema, event.record["data"]);
    for (const issue of parsed.success ? [] : parsed.error.issues) {
      context.addIssue({ ...issue, path: ["record", "data", ...issue.path] });
    }
  }),
);

/**
 * A value the hub stores as `jsonb`, which refuses NUL and lone surrogates in its text, checked
 * with `jsonTextSchema`, whose issues say where.
 */
const storable = z.superRefine((value: unknown, context) => {
  const parsed = z.safeParse(jsonTextSchema, value);
  for (const issue of parsed.success ? [] : parsed.error.issues) context.addIssue({ ...issue });
});

/**
 * One job's report: the run, the intent the Runner applied, the journal's records after the
 * hub's position, and the job's usage and activity. A job the Runner never started has no
 * journal, so its pause or cancel writes nothing: it reports `pause` or `cancel` with no events,
 * and the hub holds or ends the job itself. The report is read fresh from the journal after the
 * position the hub last gave, so a report repeated after a lost answer starts from the same position.
 * The job's start and end are entries among its events, so they reach the hub with them.
 *
 * The hub applies the usage when the Runner and the job's current run match and neither total
 * is smaller than the one it stored. It applies the events, its position, the activity and a
 * failure only when `position` also matches its own, so a repeated report changes nothing.
 */
export const jobReportSchema = z
  .object({
    ...runRefSchema.shape,
    intent: intentSchema,
    /** The hub's position the Runner read the journal after. */
    position: positionSchema,
    /** Every record of the journal after `position`, in ID order; none when nothing is new. */
    events: z.array(reportedEventSchema),
    /** The job's usage totals; absent when they could not be read. */
    usage: z.optional(jobUsageSchema.check(storable)),
    /** What the job's conversations doing anything are doing; absent when it could not be read. */
    activity: z.optional(jobActivitySchema.check(storable)),
    /**
     * When the job failed without its journal recording its end, as when the journal could not be
     * opened or the Runner faulted. Sent until the hub drops the run, but only with a report
     * that reaches the end of what the journal holds, or with no events when the job's journal never
     * held any, so the hub never ends a job without an entry its journal holds.
     */
    failedAt: z.optional(
      z.iso
        .datetime()
        .check(
          z.refine(
            (time) => z.safeParse(timestampSchema, Date.parse(time)).success,
            "Must be a time the hub stores",
          ),
        ),
    ),
  })
  .check(
    z.refine(
      ({ position, events }) =>
        events.every((event, index) => event.id > (events[index - 1]?.id ?? position)),
      "Each event's ID is greater than the position and than the event's before it",
    ),
  );
export type JobReport = z.infer<typeof jobReportSchema>;

/** A job's report the hub could not apply, with why, in words for people and logs. */
const refusedReportSchema = z.object({ ...runRefSchema.shape, reason: z.string() });
export type RefusedReport = z.infer<typeof refusedReportSchema>;

/** The requests a Runner makes of the hub. */
export const runnerMethods = {
  /** The first request on a new socket: who the Runner is. The hub says how often to sync. */
  hello: {
    params: z.object({ name: z.string() }),
    result: z.object({
      /** How often the Runner is to sync while nothing else prompts it; the hub's interval. */
      syncIntervalMs: z.int().check(z.nonnegative()),
      /** How often the Runner is to report its jobs' new events, a setting of the hub's deployment. */
      reportIntervalMs: z.int().check(z.positive()),
    }),
  },
  /** Asks for the desired state, offering the Runner's load; the hub places queued jobs on free slots. */
  sync: { params: loadSchema, result: desiredStateSchema },
  /**
   * Reports what became of runs, and of the deletions the hub asked for, with the load, and
   * gets the desired state back.
   */
  report: {
    params: z.object({
      ...loadSchema.shape,
      /**
       * Each job at most once. The hub reads each report on its own and refuses one it cannot apply,
       * so an item needs only to name its run here.
       */
      jobs: z
        .array(z.looseObject(runRefSchema.shape))
        .check(
          z.refine(
            (jobs) => new Set(jobs.map((job) => job.jobId)).size === jobs.length,
            "Each job appears once",
          ),
        ),
      /** The deleted jobs whose files are gone from the host, until the hub stops listing them. */
      deleted: z.array(jobIdSchema),
    }),
    result: z.object({
      ...desiredStateSchema.shape,
      /**
       * The reports the hub could not apply, each with why, in words for people and logs:
       * the rest applied. The Runner keeps such a job running and reports it again later.
       */
      refused: z.array(refusedReportSchema),
    }),
  },
};

/** The notifications the hub sends a Runner; none expects an answer. */
export const hubNotifications = {
  /** The hub has something new for the Runner, such as a queued job: sync now. */
  syncNow: z.object({}),
};
