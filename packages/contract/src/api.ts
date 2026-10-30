import { error } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import * as z from "zod/mini";

import {
  blobRefSchema,
  customIdSchema,
  digestSchema,
  fileSizeBytesSchema,
  inputFilesSchema,
  inputSchema,
  instructionsSchema,
  jobIdSchema,
  jobRefSchema,
  labelKeySchema,
  labelsSchema,
  labelValueSchema,
  MAX_FILE_BYTES,
  MAX_LABELS,
  modelBudgetUsdSchema,
  outputContractSchema,
  runTimeoutSecondsSchema,
} from "./job.ts";
import { api, byKey, byKeyOrMember, byMember, forOrganization, NotFound } from "./procedure.ts";
import { failureSchema, jobActivitySchema, jobEventSchema } from "./runner-protocol.ts";

/** A Runner as the hub reports it. */
export const runnerSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    /**
     * When the hub last heard from the Runner: its hello, or the latest sync or report the hub
     * recorded. A connected Runner's is at most one sync interval old.
     */
    lastSeenAt: z.date(),
  })
  .register(z.globalRegistry, { id: "Runner" });
export type Runner = z.infer<typeof runnerSchema>;

/** What an API key may do, as `{ resource: actions }`, for example `{ apiKey: ["create"] }`. */
const permissionsSchema = z.record(z.string(), z.array(z.string()));

/** An API key as the hub lists it; the key itself is shown only once, when it is created. */
export const apiKeySchema = z
  .object({
    id: z.string(),
    name: z.nullable(z.string()),
    /** The first characters of the key, to recognize it by. */
    start: z.nullable(z.string()),
    permissions: z.nullable(permissionsSchema),
    createdAt: z.date(),
    /** When the key stops working, if it was created to expire. */
    expiresAt: z.nullable(z.date()),
  })
  .register(z.globalRegistry, { id: "ApiKey" });
export type ApiKey = z.infer<typeof apiKeySchema>;

/** A newly created key: its ID and the key itself, shown only this once. */
const createdKeySchema = z.object({ id: z.string(), key: z.string() });

/** Where a job is in its life. A job is accepted as `queued`. */
export const jobStatusSchema = z.enum([
  "queued",
  "running",
  "paused",
  "succeeded",
  "failed",
  // Two l's, as Google's APIs (`google.rpc.Code.CANCELLED`), gRPC and GitHub spell it.
  "cancelled",
]);
export type JobStatus = z.infer<typeof jobStatusSchema>;

/** The statuses a job ends in: once a job reaches one, nothing more happens to it unless it is resumed. */
export const FINAL_JOB_STATUSES: readonly JobStatus[] = ["succeeded", "failed", "cancelled"];

/** Any JSON value, such as a job's output: what its Output contract's schema accepts. */
export type { JsonValue } from "type-fest";

/** What a job reports whatever its status. */
const jobBaseSchema = z.object({
  id: jobIdSchema,
  createdAt: z.date(),
  /** When the job's agent started on its Runner, once it has. */
  startedAt: z.nullable(z.date()),
  /** When the job's agent finished, once it has. */
  endedAt: z.nullable(z.date()),
  /**
   * The job's model usage so far, as pi counts it: every model call of the job's agent and its
   * sub-agents, including those made inside tools. Zeros until the job runs.
   */
  usage: z.object({
    /** Tokens of every kind, summed; cached input counts each time a model call reads it. */
    totalTokens: z.int(),
    cost: z.object({
      /** pi's estimate at the models' prices, in US dollars. */
      totalUsd: z.number(),
    }),
  }),
  /**
   * How long the job may work each time it runs on a Runner, in seconds: counted from when its
   * agent starts there, so time queued, paused or waiting for a Runner counts nothing, and started
   * afresh by a resume and by its Runner's restart. Past it, the job fails with `runTimedOut`.
   * Absent once the job is deleted, as its content is.
   */
  runTimeoutSeconds: z.optional(z.int()),
  /**
   * The most the job's model calls may cost over its whole life, in US dollars, compared with
   * `usage.cost.totalUsd` before each model call. Once the cost reaches it, the job fails with
   * `modelBudgetExceeded`; the call in progress completes, so the cost usually ends a little past
   * it. Absent once the job is deleted, as its content is.
   */
  modelBudgetUsd: z.optional(modelBudgetUsdSchema),
  /**
   * What the job is doing, for each of its conversations doing anything, as its Runner last
   * reported: present only while the job runs, and empty between steps.
   */
  activity: z.optional(jobActivitySchema),
  /**
   * A control the job's Runner has not yet acted on, beside the last `status` it confirmed: present
   * only while it waits. A pause or cancel waits for a job its Runner may be running, until
   * the Runner stops the job, which is then `paused` or ends `cancelled`, or the job ends first,
   * with its own status and output. A cancel replaces a waiting pause. A delete waits for the job's
   * Runner to delete its files from its host.
   */
  pendingControl: z.optional(
    z.object({ type: z.enum(["pause", "cancel", "delete"]), requestedAt: z.date() }),
  ),
  /**
   * When the job was deleted, present only while its Runner still deletes its files: the job reads
   * without its content, and as not found once they are gone.
   */
  deletedAt: z.optional(z.date()),
  /** The job's Batch. Absent once the job is deleted, as its content is. */
  batchId: z.optional(z.uuid()),
  /** The caller's own ID for the job, unique in its Batch. Absent once the job is deleted. */
  customId: z.optional(z.string()),
  /** The job's labels, its Batch's included. Absent once the job is deleted, as its content is. */
  labels: z.optional(labelsSchema),
  /**
   * What keeps a queued job from being sent to a Runner, to start or to carry on after a resume,
   * present only while something does: its Batch paused by a caller (`batchPausedByCaller`) or by
   * its failure breaker (`batchPausedByBreaker`), or its Batch's Canary still running
   * (`batchCanary`). A block changes no status: the job stays `queued`, and is sent once nothing
   * blocks it and a Runner has room. New values may be added.
   */
  blockedBy: z.optional(z.enum(["batchPausedByCaller", "batchPausedByBreaker", "batchCanary"])),
});

/**
 * A job as the hub reports it: once it has succeeded, with its `output`, and once it has failed,
 * with its `failure`. Only a caller pauses or cancels a job, so `paused` and `cancelled` carry no
 * reason.
 */
export const jobSchema = z
  .discriminatedUnion("status", [
    z.extend(jobBaseSchema, { status: z.enum(["queued", "running", "paused", "cancelled"]) }),
    z.extend(jobBaseSchema, {
      status: z.literal("succeeded"),
      /** The job's output. Absent once the job is deleted, as its content is. */
      output: z.optional(z.unknown()),
    }),
    z.extend(jobBaseSchema, {
      status: z.literal("failed"),
      /** Why the job failed. Its message is absent once the job is deleted, as its content is. */
      failure: failureSchema,
    }),
  ])
  .register(z.globalRegistry, { id: "Job" });
export type Job = z.infer<typeof jobSchema>;

/** What keeps a queued job from being sent to a Runner (`Job["blockedBy"]`). */
export type BlockedBy = NonNullable<Job["blockedBy"]>;

/**
 * An agent as a Batch's creation gives it: what every job of the Batch runs, each on its own input.
 * It is set once, as the Canary tests it.
 */
export const agentSchema = z
  .object({
    instructions: z.optional(
      instructionsSchema.check(
        z.describe(
          "What the agent is told for every job, after the Runner's own instructions and before the job's `input`; none by default.",
        ),
      ),
    ),
    outputContract: outputContractSchema.check(
      z.describe("What each job's output must be: `schema`, a JSON Schema the output must match."),
    ),
    runTimeoutSeconds: z.optional(
      runTimeoutSecondsSchema.check(
        z.describe(
          "How long each run of a job on a Runner may work, in seconds; the hub's default when left out, and refused above its maximum.",
        ),
      ),
    ),
    modelBudgetUsd: z.optional(
      modelBudgetUsdSchema.check(
        z.describe(
          "The most each job's model calls may cost over its whole life, in US dollars; the hub's default when left out, and refused above its maximum.",
        ),
      ),
    ),
  })
  .register(z.globalRegistry, { id: "Agent" });
export type Agent = z.infer<typeof agentSchema>;

/** The failure breaker's defaults: a Canary of 5 jobs, then at most 10% of jobs failed. */
export const BREAKER_DEFAULTS = { canary: 5, maxFailurePercent: 10 } as const;

/** How many jobs each Canary holds; 0 for none. */
const canarySchema = z.int32().check(z.nonnegative());
/** The share of failed jobs among those ended since the Canary began, as a percentage, past which the Batch pauses. */
const maxFailurePercentSchema = z.number().check(z.minimum(0), z.maximum(100));

/** A Batch's failure breaker, as the hub reports it: off, or its settings. */
const breakerSchema = z.union([
  z.literal(false),
  z.object({ canary: canarySchema, maxFailurePercent: maxFailurePercentSchema }),
]);

/** A Batch as a caller creates it. */
export const batchCreationSchema = z
  .object({
    customId: z.optional(
      customIdSchema.check(
        z.describe(
          "Your own ID for the Batch, unique in the Organization; none by default. Creating a Batch again with the same `customId` and the same `agent`, `labels` and `breaker` returns the Batch already created, so a creation can be retried safely; with any of them different it is refused. A new Batch needs a new ID.",
        ),
      ),
    ),
    agent: agentSchema,
    labels: z._default(
      labelsSchema.check(
        z.describe(
          `Labels every job of the Batch gets, beside its own, which win where both name a key; none by default. Keys follow Kubernetes's label key syntax; values are any text of at most 256 characters, kept exactly as given. A job has at most ${MAX_LABELS} labels, the Batch's and its own together, so a job whose own labels take it past that is refused.`,
        ),
      ),
      {},
    ),
    breaker: z
      ._default(
        z.union([
          z.boolean(),
          z.object({
            canary: z.optional(canarySchema),
            maxFailurePercent: z.optional(maxFailurePercentSchema),
          }),
        ]),
        true,
      )
      .check(
        z.describe(
          "The failure breaker, which pauses the Batch when a flaw its jobs share shows, after a few jobs instead of after all of them. The Batch's first `canary` jobs (5 by default), its Canary, run before the rest are released, and any of them failing pauses the Batch. Once every Canary place is filled and its jobs have ended, it pauses when more than `maxFailurePercent` (10 by default) of the jobs ended since the Canary began have failed. A job counts once, by its latest end; a job that a limit stopped counts as failed, while a cancelled job, or one its Runner's removal ended, does not count. Resuming a pause the breaker made starts a new Canary of the next `canary` jobs, those added later included. `canary: 0` releases every job at once, and the percentage then judges from the first job to end, so one early failure still pauses the Batch; `maxFailurePercent: 100` keeps the Canary alone. `true` by default, for the defaults; `false` turns the breaker off.",
        ),
      ),
  })
  .register(z.globalRegistry, { id: "BatchCreation" });
export type BatchCreation = z.infer<typeof batchCreationSchema>;

/** A job as a caller adds it to a Batch, whose agent it runs. */
export const jobSubmissionSchema = z
  .object({
    customId: customIdSchema.check(
      z.describe(
        "Your own ID for the job, unique in its Batch. Adding a job again with the same `customId` and the same content returns the job already added, so an add can be retried safely and a whole Batch added again to add only the jobs it lacks; with other content it is refused. A new job needs a new ID.",
      ),
    ),
    input: z.optional(
      inputSchema.check(
        z.describe(
          "What the job gives the Batch's agent to work on: the agent's first input, after its instructions. Optional for a job with its own `files`, which are then what it works on; a job with neither is refused.",
        ),
      ),
    ),
    // Defaults to none, so a job without them is the same job as one with an empty list.
    files: z._default(
      inputFilesSchema.check(
        z.describe(
          "Files the job's agent finds in its Workspace's `inputs/` folder, at their paths; none by default. Each is a `path`, such as `invoice.pdf` or `claims/42/photo.jpg`, and the `digest` and `sizeBytes` of a Blob the Organization uploaded with `PUT /blobs/{digest}`, or that another of its jobs uses; any other Blob is refused.",
        ),
      ),
      [],
    ),
    labels: z._default(
      labelsSchema.check(
        z.describe(
          `Labels to find the job, group it and attribute its cost by, by key, such as \`{ "customer": "acme" }\`, beside its Batch's, which they win over where both name a key; none by default. Keys follow Kubernetes's label key syntax; values are any text of at most 256 characters, kept exactly as given. At most ${MAX_LABELS}, the Batch's and the job's own together.`,
        ),
      ),
      {},
    ),
  })
  .register(z.globalRegistry, { id: "JobSubmission" });
export type JobSubmission = z.infer<typeof jobSubmissionSchema>;

/** The most jobs one request adds to a Batch. */
export const MAX_JOBS_PER_ADD = 100;

/** Where a Batch is: releasing its jobs, paused, or cancelled. */
export const batchStatusSchema = z.enum(["active", "paused", "cancelled"]);

/** A Batch as the hub reports it. */
export const batchSchema = z
  .object({
    id: z.uuid(),
    /** The caller's own ID for the Batch; null when it was created without one. */
    customId: z.nullable(z.string()),
    createdAt: z.date(),
    /**
     * `active` while it releases its jobs, an empty Batch and one whose jobs have all ended included,
     * and while a cancel waits, as `pendingControl` shows; `paused` while it is paused, whether or not
     * any job is unfinished, as jobs added to it are held; `cancelled` once it was cancelled and every
     * job has ended. A Batch has no status of its own for having no unfinished job: `jobCounts` tells.
     */
    status: batchStatusSchema,
    /** Who paused the Batch, a caller or its failure breaker; null while it is not paused. */
    pausedBy: z.nullable(z.enum(["caller", "breaker"])),
    /** A cancel waiting for its jobs' Runners to stop them, present only while it waits. */
    pendingControl: z.optional(z.object({ type: z.literal("cancel"), requestedAt: z.date() })),
    labels: labelsSchema,
    breaker: breakerSchema,
    /** How many of its jobs have each status; a deleted job is not counted. */
    jobCounts: z.object({
      queued: z.int(),
      running: z.int(),
      paused: z.int(),
      succeeded: z.int(),
      failed: z.int(),
      cancelled: z.int(),
    }),
    /**
     * Its jobs' model usage, as each job reports its own; a deleted job's is not included, and stays
     * in its Usage records.
     */
    usage: z.object({ totalTokens: z.int(), cost: z.object({ totalUsd: z.number() }) }),
  })
  .register(z.globalRegistry, { id: "Batch" });
export type Batch = z.infer<typeof batchSchema>;

/** The jobs an add asked for, in its order, each with whether the add created it, and their Batch. */
export const addedJobsSchema = z
  .object({
    batch: batchSchema,
    jobs: z.array(
      z.object({
        /**
         * Whether this add created the job: `false` for a job the Batch already had under its
         * `customId`, as when an add whose answer was lost is retried, so work that must happen once
         * per job is not to depend on `true`.
         */
        created: z.boolean(),
        job: jobSchema,
      }),
    ),
  })
  .register(z.globalRegistry, { id: "AddedJobs" });
export type AddedJobs = z.infer<typeof addedJobsSchema>;

/** A Batch as a call names it. */
export const batchRefSchema = z.object({ id: z.uuid().check(z.describe("The Batch's ID.")) });

/**
 * A job as a list reports it: without its content, so without its output, its failure's message
 * and its activity. Read the job for those.
 */
const listedJobBaseSchema = z.omit(jobBaseSchema, { activity: true });
export const listedJobSchema = z
  .discriminatedUnion("status", [
    z.extend(listedJobBaseSchema, {
      status: z.enum(["queued", "running", "paused", "succeeded", "cancelled"]),
    }),
    z.extend(listedJobBaseSchema, {
      status: z.literal("failed"),
      failure: z.pick(failureSchema, { reason: true }),
    }),
  ])
  .register(z.globalRegistry, { id: "ListedJob" });
export type ListedJob = z.infer<typeof listedJobSchema>;

/** A page of jobs, newest first. A caller reads the next page after the last job's `id` while `hasMore`. */
export const jobsPageSchema = z
  .object({ jobs: z.array(listedJobSchema), hasMore: z.boolean() })
  .register(z.globalRegistry, { id: "JobsPage" });
export type JobsPage = z.infer<typeof jobsPageSchema>;

/**
 * A page of a job's events. A caller reads the next page after the last event's `id`: at once while
 * `hasMore`, or after a few seconds otherwise, and stops once a page without more has a final
 * `status`.
 */
export const jobEventsPageSchema = z
  .object({
    /** The job's events after the position asked for, in ID order. */
    events: z.array(jobEventSchema),
    /** Whether more of the job's events are already stored after this page. */
    hasMore: z.boolean(),
    /**
     * The job's status as of this page, read with its events: once it is final, the job's events are
     * stored up to its end. A job whose host is lost may end without some of its events, so a final
     * status says nothing follows, not that every event is here.
     */
    status: jobStatusSchema,
  })
  .register(z.globalRegistry, { id: "JobEventsPage" });
export type JobEventsPage = z.infer<typeof jobEventsPageSchema>;

/** A call the member's role does not allow, decided by the hub. */
export const Forbidden = error("FORBIDDEN");
/**
 * A control that cannot take effect on the job or Batch as it is, such as a resume of a job that
 * succeeded, or jobs added to a cancelled Batch; its message says why.
 */
export const ControlRefused = error("CONFLICT");
/**
 * A Batch created, or jobs added, under a custom ID already used for one with other content; its
 * message names each.
 */
export const CustomIdReused = error("UNPROCESSABLE_CONTENT");

// Calls a member's role does not allow fail with `FORBIDDEN`, and invalid ones with `BAD_REQUEST`.
// Where Better Auth decides, the hub passes its `FORBIDDEN` through, declared without a factory;
// removing a Runner raises `Forbidden` itself. The hub raises `BAD_REQUEST` itself only for what the
// contract's schemas cannot check, such as an Output contract's schema.

/** The most files one check of stored Blobs names. */
export const MAX_FILES_PER_CHECK = 1000;

/**
 * Whether the Organization holds a file's Blob, as a check answers it, and if not, where to
 * upload it: `transfer` names how, `put` being one request carrying the whole file, so other ways,
 * such as resumable uploads, can follow.
 */
const blobCheckSchema = z.union([
  z.extend(blobRefSchema, { stored: z.literal(true) }),
  z.extend(blobRefSchema, {
    stored: z.literal(false),
    upload: z.object({
      transfer: z.literal("put"),
      url: z
        .string()
        .check(z.describe("Where to `PUT` the file's bytes, relative to the hub's URL.")),
    }),
  }),
]);

/**
 * The hub's public HTTP API, for people and their programs. Callers sign in as a member or send an
 * API key in `x-api-key`. A Runner's own calls are `runnerHttpContract`'s.
 */
export const apiContract = {
  runners: {
    list: api
      .meta(openapi({ method: "GET", path: "/runners" }), ...byMember)
      .output(z.array(runnerSchema)),
    // Removing a Runner ends the jobs it held that have not ended.
    delete: api
      .errors({ [Forbidden.code]: Forbidden, [NotFound.code]: NotFound })
      .meta(
        openapi({
          method: "DELETE",
          path: "/runners/{id}",
          description:
            "Removes a Runner, for a host that is retired or compromised: its own key stops working and its connection is dropped at once. A compromised host also holds the shared key Runners register with, so revoke that too and create a new one for future hosts; Runners already registered keep working with their own keys. Nothing the host holds can be run, resumed or removed through the hub again, so the jobs the Runner held that had not ended end: `cancelled` where a cancel was waiting, otherwise `failed` with reason `runnerRemoved`. They may still run on the host unseen. The answer lists the jobs it ended, and every job whose files the host may still hold, which whoever looks after the host can remove.",
        }),
        ...byMember,
      )
      .input(z.object({ id: z.string() }))
      .output(
        z.object({
          endedJobs: z.array(jobIdSchema).check(z.describe("The jobs the removal ended.")),
          jobsWithFilesLeft: z
            .array(jobIdSchema)
            .check(
              z.describe(
                "The jobs whose files the Runner's host may still hold, those ended included.",
              ),
            ),
        }),
      ),
  },
  apiKeys: {
    create: forOrganization
      .errors({ BAD_REQUEST: {}, FORBIDDEN: {} })
      .meta(openapi({ method: "POST", path: "/api-keys" }), ...byMember)
      .input(
        z.object({
          name: z.string(),
          permissions: permissionsSchema,
          expiresIn: z.optional(
            z
              .number()
              .check(
                z.describe(
                  "Seconds until the key stops working, from one day to a year; without it, it never does.",
                ),
              ),
          ),
        }),
      )
      .output(createdKeySchema),
    list: forOrganization
      .errors({ FORBIDDEN: {} })
      .meta(openapi({ method: "GET", path: "/api-keys" }), ...byMember)
      .output(z.array(apiKeySchema)),
    revoke: api
      .errors({ FORBIDDEN: {}, [NotFound.code]: NotFound })
      .meta(openapi({ method: "DELETE", path: "/api-keys/{id}" }), ...byMember)
      .input(z.object({ id: z.string() })),
  },
  blobs: {
    check: forOrganization
      .errors({ BAD_REQUEST: {} })
      .meta(
        openapi({
          method: "POST",
          path: "/blobs/check",
          description: `Answers, for each file named by \`digest\` and \`sizeBytes\`, in the order given, whether the Organization holds that Blob: \`stored: true\` when it uploaded it in the last seven days or one of its jobs uses it, so jobs may name it without uploading it again; otherwise \`stored: false\` and where to upload it. At most ${MAX_FILES_PER_CHECK} files per request. A file whose size is not that of the Blob the Organization holds with its digest is refused.`,
        }),
        ...byKey({ job: ["create"] }),
      )
      .input(
        z.object({
          files: z
            .array(blobRefSchema)
            .check(
              z.minLength(1),
              z.maxLength(MAX_FILES_PER_CHECK),
              z.describe(`The files to check, 1 to ${MAX_FILES_PER_CHECK} per request.`),
            ),
        }),
      )
      .output(z.object({ files: z.array(blobCheckSchema) })),
    upload: forOrganization
      .errors({ BAD_REQUEST: {} })
      .meta(
        openapi({
          method: "PUT",
          path: "/blobs/{digest}",
          // The body is handed over as it arrives, so the hub stores a file a chunk at a time.
          inputStructure: "detailed",
          requestBodyHint: "octet-stream",
          description: `Uploads a file's bytes as the request's body, at most ${MAX_FILE_BYTES / 1024 / 1024} MiB, with \`Content-Length\`, under its \`digest\`, and answers once the hub has checked the bytes' size and SHA-256 and stored them as a Blob. The Organization then holds the Blob for seven days, renewed by each upload of it, and as long as any of its jobs uses it after that. Bytes whose size or SHA-256 differ from what the request names are refused, and nothing is stored.`,
        }),
        ...byKey({ job: ["create"] }),
      )
      .input(
        z.object({
          params: z.object({ digest: digestSchema }),
          headers: z.object({
            // Coerced from the header's text by the hub's coercion plugin.
            "content-length": fileSizeBytesSchema.check(z.describe("The file's size in bytes.")),
          }),
          // A stream of the request's bytes, which oRPC hands over without reading it first; its
          // JSON Schema is a file's.
          body: z.instanceof(ReadableStream<Uint8Array>).check(
            z.meta({
              type: "string",
              format: "binary",
              contentMediaType: "application/octet-stream",
            }),
          ),
        }),
      )
      .output(blobRefSchema),
  },
  batches: {
    create: forOrganization
      .errors({ BAD_REQUEST: {}, [CustomIdReused.code]: CustomIdReused })
      .meta(
        // One status, 200, whether the Batch is new or a retry's, as for an add: no caller acts on
        // the difference, and a status per answer would make the typed client answer this one call
        // with `{ status, body }`.
        openapi({
          method: "POST",
          path: "/batches",
          description:
            "Creates a Batch, without jobs, and answers with it: the `agent` every job added to it runs, set once, its labels and its failure breaker. Add its jobs with `POST /batches/{id}/jobs`. The hub checks the Output contract's schema, refusing one that is invalid, describes a list or refers outside itself. A `customId` makes the creation safe to retry: the same `customId` with the same `agent`, `labels` and `breaker` answers with the Batch already created, and with any of them different is refused.",
        }),
        ...byKey({ job: ["create"] }),
      )
      .input(batchCreationSchema)
      .output(batchSchema),
    addJobs: forOrganization
      .errors({
        BAD_REQUEST: {},
        [NotFound.code]: NotFound,
        [ControlRefused.code]: ControlRefused,
        [CustomIdReused.code]: CustomIdReused,
      })
      .meta(
        openapi({
          method: "POST",
          path: "/batches/{id}/jobs",
          description: `Adds jobs to a Batch, at most ${MAX_JOBS_PER_ADD} per request, each running the Batch's agent on its own \`input\` and files, and answers with the Batch and each job in the order given, with whether this add created it. An add is all or nothing: any problem refuses the whole request, naming every problem with its path, such as \`jobs[17].input\`: 400 when any job is invalid, then 409 when the Batch is cancelled, then 422 for each \`customId\` the Batch already has for a job with other content. A job whose \`customId\` the Batch already has with the same content is returned as it is, with \`created: false\`, so an add can be retried safely, and a whole Batch added again to add only the jobs it lacks; a new job needs a new \`customId\`. Jobs added to a paused Batch wait for it.`,
        }),
        ...byKey({ job: ["create"] }),
      )
      .input(
        z.extend(batchRefSchema, {
          jobs: z
            .array(jobSubmissionSchema)
            .check(
              z.minLength(1),
              z.maxLength(MAX_JOBS_PER_ADD),
              z.describe(
                `The jobs to add, 1 to ${MAX_JOBS_PER_ADD} per request; a larger Batch is added over several.`,
              ),
            ),
        }),
      )
      .output(addedJobsSchema),
    get: api
      .errors({ [NotFound.code]: NotFound })
      .meta(
        openapi({ method: "GET", path: "/batches/{id}", description: "Reads a Batch." }),
        ...byKeyOrMember({ job: ["read"] }),
      )
      .input(batchRefSchema)
      .output(batchSchema),
    pause: api
      .errors({ [NotFound.code]: NotFound, [ControlRefused.code]: ControlRefused })
      .meta(
        openapi({
          method: "POST",
          path: "/batches/{id}/pause",
          description:
            "Pauses a Batch and answers with it. Its jobs not yet sent to a Runner, and those added later, are blocked: they stay `queued`, with `blockedBy: batchPausedByCaller`. Jobs already being sent or running carry on to their own ends, so a pause stops only the jobs not already being sent. A pause changes no job's status. Pausing a paused Batch changes nothing; a cancelled one is refused.",
        }),
        ...byKeyOrMember({ job: ["pause"] }),
      )
      .input(batchRefSchema)
      .output(batchSchema),
    resume: api
      .errors({ [NotFound.code]: NotFound, [ControlRefused.code]: ControlRefused })
      .meta(
        openapi({
          method: "POST",
          path: "/batches/{id}/resume",
          description:
            "Resumes a paused Batch and answers with it: its blocked jobs are released, behind its Canary while one runs. Resuming a pause its failure breaker made, or a caller's pause during which the breaker's rule tripped, restarts the breaker's counts and starts a new Canary of the next `canary` jobs, those added later included. A job paused by its own control stays paused. Resuming a Batch that is not paused changes nothing; a cancelled one is refused.",
        }),
        ...byKeyOrMember({ job: ["resume"] }),
      )
      .input(batchRefSchema)
      .output(batchSchema),
    cancel: api
      .errors({ [NotFound.code]: NotFound })
      .meta(
        openapi({
          method: "POST",
          path: "/batches/{id}/cancel",
          description:
            "Cancels a Batch and answers with it: each unfinished job is cancelled as a job's cancel does, at once where no Runner may be running it and otherwise once its Runner stops it, shown meanwhile as the Batch's `pendingControl`. The Batch is `cancelled` once every job has ended. A cancel is final, also for a Batch whose jobs have all ended: the Batch takes no new jobs, and none of its jobs can be resumed. Cancelling a cancelled Batch changes nothing.",
        }),
        ...byKeyOrMember({ job: ["cancel"] }),
      )
      .input(batchRefSchema)
      .output(batchSchema),
  },
  jobs: {
    get: api
      .errors({ [NotFound.code]: NotFound })
      .meta(
        openapi({ method: "GET", path: "/jobs/{id}", description: "Reads a job." }),
        ...byKeyOrMember({ job: ["read"] }),
      )
      .input(jobRefSchema)
      .output(jobSchema),
    pause: api
      .errors({ [NotFound.code]: NotFound, [ControlRefused.code]: ControlRefused })
      .meta(
        openapi({
          method: "POST",
          path: "/jobs/{id}/pause",
          description:
            "Pauses a job and answers with the job. A job not yet sent to a Runner is held at once as `paused`. Otherwise the pause waits for its Runner, shown as `pendingControl`, and the job is `paused` once the Runner has stopped it, its work kept for a resume, holding no capacity. Pausing a paused job, or one whose pause waits, changes nothing; a job that has ended, or whose cancel waits, is refused.",
        }),
        ...byKeyOrMember({ job: ["pause"] }),
      )
      .input(jobRefSchema)
      .output(jobSchema),
    resume: api
      .errors({
        BAD_REQUEST: {},
        [NotFound.code]: NotFound,
        [ControlRefused.code]: ControlRefused,
      })
      .meta(
        openapi({
          method: "POST",
          path: "/jobs/{id}/resume",
          description:
            "Resumes a stopped job and answers with the job. A paused, failed or cancelled job goes back to `queued` and carries on from where it stopped, on the Runner that holds its work; one paused before it started simply starts. Resuming a job that is queued or running changes nothing, and one that gives it other limits than its own is refused: its run keeps them. A job whose pause its Runner has not yet confirmed, whose cancel waits, that succeeded, that never started before it ended, or whose Batch is cancelled, is refused. A job resumed while its Batch is paused is `queued` and waits for the Batch. A resume that changes the job may give it a new `runTimeoutSeconds` and `modelBudgetUsd`, and is refused when the job's model calls have cost its `modelBudgetUsd` or more, as for a job that failed with `modelBudgetExceeded`.",
        }),
        ...byKeyOrMember({ job: ["resume"] }),
      )
      .input(
        z.object({
          ...jobRefSchema.shape,
          runTimeoutSeconds: z.optional(
            runTimeoutSecondsSchema.check(
              z.describe(
                "A new `runTimeoutSeconds` for the job's next run, refused above the hub's maximum, and for a job that is queued or running unless it is the job's own.",
              ),
            ),
          ),
          modelBudgetUsd: z.optional(
            modelBudgetUsdSchema.check(
              z.describe(
                "A new `modelBudgetUsd` for the job, refused above the hub's maximum, at or below what its model calls have cost, and for a job that is queued or running unless it is the job's own.",
              ),
            ),
          ),
        }),
      )
      .output(jobSchema),
    cancel: api
      .errors({ [NotFound.code]: NotFound })
      .meta(
        openapi({
          method: "POST",
          path: "/jobs/{id}/cancel",
          description:
            "Cancels a job and answers with the job. A job not yet sent to a Runner, or paused, ends `cancelled` at once. Otherwise the cancel waits for its Runner, shown as `pendingControl`, replacing a waiting pause, and the job ends `cancelled` once the Runner has stopped it, keeping its events and costs. A job that has ended, or ends before its Runner stops it, keeps the status and output it ended with.",
        }),
        ...byKeyOrMember({ job: ["cancel"] }),
      )
      .input(jobRefSchema)
      .output(jobSchema),
    // Deletion removes content and records the host file purge still owed.
    delete: api
      .errors({ [NotFound.code]: NotFound, [ControlRefused.code]: ControlRefused })
      .meta(
        openapi({
          method: "DELETE",
          path: "/jobs/{id}",
          description:
            "Deletes a finished job and answers with the job as it now reads. Its content goes from the hub at once, its Input files' Blobs included unless an upload under seven days old still holds them, and from its host once its Runner has deleted its journal and Workspace; until then the job reads without its content, with `deletedAt` and a waiting `delete`, and then as not found. Its Usage record stays, and so does its Batch, with the agent's instructions and Output contract. An unfinished job is refused: cancel it first.",
        }),
        ...byKeyOrMember({ job: ["delete"] }),
      )
      .input(jobRefSchema)
      .output(jobSchema),
    events: api
      .errors({ [NotFound.code]: NotFound })
      .meta(
        openapi({
          method: "GET",
          path: "/jobs/{id}/events",
          description:
            "Reads a job's events in pages, while the job runs and after it ends. A page holds at most `limit` events and stops early after about 1 MiB of them, though it always holds at least one if there are any.",
        }),
        ...byKeyOrMember({ job: ["read"] }),
      )
      .input(
        z.object({
          ...jobRefSchema.shape,
          after: z._default(
            z
              .int()
              .check(
                z.nonnegative(),
                z.describe(
                  "A position: the ID of the last event the caller has, 0 to read from the start.",
                ),
              ),
            0,
          ),
          limit: z._default(
            z
              .int()
              .check(
                z.minimum(1),
                z.maximum(100),
                z.describe("At most this many events in the page, 100 by default."),
              ),
            100,
          ),
        }),
      )
      .output(jobEventsPageSchema),
    list: api
      .meta(
        openapi({
          method: "GET",
          path: "/jobs",
          description:
            "Lists jobs, newest first, in pages of at most `limit`. Filters combine: a job is listed when it has one of the `status` values given, every label in `labels`, and the `batchId` and `customId` given. Read the next page with the same filters and `after` set to the last job's `id`, while the page says `hasMore`. A listed job has no `output`, failure `message` or `activity`: read the job for those.",
          // Repeated `status=…` parameters, a single one included, read as a list.
          queryStyles: { status: "array" },
        }),
        ...byKeyOrMember({ job: ["read"] }),
      )
      .input(
        z.object({
          status: z.optional(
            z
              .array(jobStatusSchema)
              .check(z.describe("Statuses, one per `status` parameter; any status when none.")),
          ),
          labels: z.optional(
            z
              .record(labelKeySchema, labelValueSchema)
              .check(
                z.describe(
                  "Labels the job must all have, as `labels[customer]=acme`; repeat for more.",
                ),
              ),
          ),
          batchId: z.optional(z.uuid().check(z.describe("The Batch the job belongs to."))),
          customId: z.optional(
            customIdSchema.check(
              z.describe(
                "The job's own `customId`, which is unique in its Batch, so without `batchId` it may match a job in each of several Batches.",
              ),
            ),
          ),
          after: z.optional(
            jobIdSchema.check(
              z.describe("The last job's ID on the previous page, to read on after it."),
            ),
          ),
          limit: z._default(
            z
              .int()
              .check(
                z.minimum(1),
                z.maximum(100),
                z.describe("At most this many jobs in the page, 50 by default."),
              ),
            50,
          ),
        }),
      )
      .output(jobsPageSchema),
  },
};
