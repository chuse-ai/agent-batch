import * as z from "zod/mini";

import { inputFilesSchema, jobIdSchema, outputContractSchema, promptSchema } from "./job.ts";

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
 * What the hub wants done with a placement, which only ever moves forward: run, then pause or cancel,
 * then purge. Resuming a paused job is a new placement. All four are declared now, though only `run`
 * is sent: a new value in a closed set would bump the protocol version.
 */
export const intentSchema = z.enum(["run", "pause", "cancel", "purge"]);
export type Intent = z.infer<typeof intentSchema>;

/** One placement of a job on the Runner: the job and the generation of its placement. */
const placementRefSchema = z.object({
  jobId: jobIdSchema,
  /** Counts the job's placements from 1; a job placed again, such as after a pause, gets the next. */
  generation: z.int(),
});
export type PlacementRef = z.infer<typeof placementRefSchema>;

/** A job as the Runner needs it to run the job: the submission's content, as submitted. */
export const jobContentSchema = z.object({
  prompt: promptSchema,
  inputFiles: inputFilesSchema,
  outputContract: outputContractSchema,
});
export type JobContent = z.infer<typeof jobContentSchema>;

/**
 * What the Runner is to do with one job. The job's content comes only with a placement the Runner
 * said it does not hold, so a sync repeats placements but not jobs.
 */
const placementSchema = z.object({
  ...placementRefSchema.shape,
  intent: intentSchema,
  job: z.optional(jobContentSchema),
});
export type Placement = z.infer<typeof placementSchema>;

/** The hub's answer to a sync or a report: the full desired state of the Runner's jobs. */
export const desiredStateSchema = z.object({
  placements: z.array(placementSchema),
});
export type DesiredState = z.infer<typeof desiredStateSchema>;

/** What the Runner tells the hub about its load in every sync and report. */
const loadSchema = z.object({
  /** The placements the Runner holds, running or not. */
  held: z.array(placementRefSchema),
  /** How many more jobs the Runner can take. */
  free: z.int().check(z.gte(0)),
});
export type Load = z.infer<typeof loadSchema>;

/** What became of a placement on the Runner, reported once each. */
const outcomeSchema = z.discriminatedUnion("state", [
  z.object({ state: z.literal("running"), startedAt: z.iso.datetime() }),
  z.object({ state: z.literal("succeeded"), result: z.unknown(), endedAt: z.iso.datetime() }),
]);
export type Outcome = z.infer<typeof outcomeSchema>;

/** One job's report: the placement, the intent the Runner applied, and what came of it. */
const jobReportSchema = z.object({
  ...placementRefSchema.shape,
  intent: intentSchema,
  outcome: outcomeSchema,
});
export type JobReport = z.infer<typeof jobReportSchema>;

/** The requests a Runner makes of the hub. */
export const runnerMethods = {
  /** The first request on a new socket: who the Runner is. The hub says how often to sync. */
  hello: {
    params: z.object({ name: z.string() }),
    result: z.object({
      /** How often the Runner is to sync while nothing else prompts it; the hub's interval. */
      syncIntervalMs: z.int().check(z.gte(0)),
    }),
  },
  /** Asks for the desired state, offering the Runner's load; the hub places queued jobs on free slots. */
  sync: { params: loadSchema, result: desiredStateSchema },
  /** Reports what became of placements, with the load, and gets the desired state back. */
  report: {
    params: z.object({ ...loadSchema.shape, jobs: z.array(jobReportSchema) }),
    result: desiredStateSchema,
  },
};

/** The notifications the hub sends a Runner; none expects an answer. */
export const hubNotifications = {
  /** The hub has something new for the Runner, such as a queued job: sync now. */
  syncNow: z.object({}),
};
