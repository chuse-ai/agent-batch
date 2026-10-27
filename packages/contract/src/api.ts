import { error } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import * as z from "zod/mini";

import {
  inputFilesSchema,
  jobIdSchema,
  jobRefSchema,
  modelBudgetUsdSchema,
  labelsSchema,
  resultContractSchema,
  promptSchema,
  textSchema,
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

/** Any JSON value, such as a job's result: what its Result contract's schema accepts. */
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
   * with its own status and result. A cancel replaces a waiting pause. A delete waits for the job's
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
  /** The job's labels, its Batch's included. Absent once the job is deleted, as its content is. */
  labels: z.optional(labelsSchema),
  /** The job's Batch, for a member of one. Absent once the job is deleted, as its content is. */
  batchId: z.optional(z.uuid()),
  /** The member's key in its Batch, for a member of one. Absent once the job is deleted. */
  key: z.optional(z.string()),
  /**
   * Why a queued job waits beside capacity, present only while it does: its Batch is paused by a
   * caller (`batchPaused`) or by its failure breaker (`failFast`), or its Batch's Canary still
   * runs (`canary`). New reasons may be added.
   */
  waitingFor: z.optional(z.enum(["batchPaused", "failFast", "canary"])),
});

/**
 * A job as the hub reports it: once it has succeeded, with its `result`, and once it has failed,
 * with its `failure`. Only a caller pauses or cancels a job, so `paused` and `cancelled` carry no
 * reason.
 */
export const jobSchema = z
  .discriminatedUnion("status", [
    z.extend(jobBaseSchema, { status: z.enum(["queued", "running", "paused", "cancelled"]) }),
    z.extend(jobBaseSchema, {
      status: z.literal("succeeded"),
      /** The job's result. Absent once the job is deleted, as its content is. */
      result: z.optional(z.unknown()),
    }),
    z.extend(jobBaseSchema, {
      status: z.literal("failed"),
      /** Why the job failed. Its message is absent once the job is deleted, as its content is. */
      failure: failureSchema,
    }),
  ])
  .register(z.globalRegistry, { id: "Job" });
export type Job = z.infer<typeof jobSchema>;

/** A job as a caller submits it. */
export const jobSubmissionSchema = z
  .object({
    prompt: promptSchema.check(z.describe("What the agent is asked to do.")),
    // Defaults to none, so a submission without them is the same job as one with an empty list.
    inputFiles: z._default(
      inputFilesSchema.check(
        z.describe(
          "Files the agent finds in its Workspace's `inputs/` folder, by name; none by default.",
        ),
      ),
      {},
    ),
    resultContract: resultContractSchema.check(
      z.describe("What the job's result must be: `schema`, a JSON Schema the result must match."),
    ),
    idempotencyKey: z.optional(
      textSchema.check(
        z.minLength(1),
        z.maxLength(255),
        z.describe(
          "Makes submitting safe to retry: submitting again with the same key and the same job returns the job already accepted, and with a different job is refused.",
        ),
      ),
    ),
    runTimeoutSeconds: z.optional(
      runTimeoutSecondsSchema.check(
        z.describe(
          "How long each run of the job on a Runner may work, in seconds; the hub's default when left out, and refused above its maximum.",
        ),
      ),
    ),
    modelBudgetUsd: z.optional(
      modelBudgetUsdSchema.check(
        z.describe(
          "The most the job's model calls may cost over its whole life, in US dollars; the hub's default when left out, and refused above its maximum.",
        ),
      ),
    ),
    labels: z.optional(
      labelsSchema.check(
        z.describe(
          'Labels to find the job, group it and attribute its cost by, by key, such as `{ "customer": "acme" }`; none by default. Keys and values follow Kubernetes\'s label syntax.',
        ),
      ),
    ),
  })
  .register(z.globalRegistry, { id: "JobSubmission" });
export type JobSubmission = z.infer<typeof jobSubmissionSchema>;

/**
 * A job of a Batch as a caller submits it: a job's submission, named by a key unique in its Batch
 * rather than by an idempotency key.
 */
export const batchJobSubmissionSchema = z
  .extend(z.omit(jobSubmissionSchema, { idempotencyKey: true }), {
    key: textSchema.check(
      z.minLength(1),
      z.maxLength(255),
      z.describe(
        "The job's name in its Batch, unique there: submitting it again with the same content returns the job already accepted, and with different content is refused, so a whole Batch can be submitted again to add only the jobs it lacks.",
      ),
    ),
  })
  .register(z.globalRegistry, { id: "BatchJobSubmission" });
export type BatchJobSubmission = z.infer<typeof batchJobSubmissionSchema>;

/** The most jobs one request submits into a Batch. */
export const MAX_BATCH_JOBS_PER_REQUEST = 100;

const batchJobsSchema = z
  .array(batchJobSubmissionSchema)
  .check(
    z.maxLength(MAX_BATCH_JOBS_PER_REQUEST),
    z.describe(
      `The jobs to submit into the Batch, at most ${MAX_BATCH_JOBS_PER_REQUEST} per request; a larger Batch is submitted over several.`,
    ),
  );

/** The failure breaker's defaults: a Canary of 5 jobs, and a tolerance of 25% failed. */
export const FAIL_FAST_DEFAULTS = { canary: 5, tolerance: 25 } as const;

/** A Batch's failure breaker, as the hub reports it: off, or its Canary and tolerance. */
const failFastSchema = z.union([
  z.literal(false),
  z.object({
    /** How many of the Batch's first jobs form its Canary, which runs before the rest are released. */
    canary: z.int32().check(z.positive()),
    /** The share of failed jobs among finished ones, as a percentage, past which the Batch pauses. */
    tolerance: z.number().check(z.nonnegative(), z.lt(100)),
  }),
]);

/** A Batch as a caller creates it. */
export const batchCreationSchema = z
  .object({
    idempotencyKey: z.optional(
      textSchema.check(
        z.minLength(1),
        z.maxLength(255),
        z.describe(
          "Makes creating safe to retry: creating again with the same key and the same `labels` and `failFast` returns the Batch already created, and submits its `jobs` into it as any submission does; with a different `labels` or `failFast` it is refused.",
        ),
      ),
    ),
    labels: z._default(
      labelsSchema.check(
        z.describe(
          "Labels every job of the Batch gets, beside its own, which win where both name a key; none by default.",
        ),
      ),
      {},
    ),
    failFast: z
      ._default(
        z.union([
          z.boolean(),
          z.object({
            canary: z.optional(z.int32().check(z.positive())),
            tolerance: z.optional(z.number().check(z.nonnegative(), z.lt(100))),
          }),
        ]),
        true,
      )
      .check(
        z.describe(
          "The failure breaker, which catches a flaw every job shares after a few jobs instead of after all of them. The Batch's first `canary` jobs (5 by default), its Canary, run before the rest are released, and the Batch pauses once at least two of its finished jobs failed and their share among them is above `tolerance` percent (25 by default). A job that a limit stopped counts as failed; a cancelled job, or one its Runner's removal ended, does not count. `true` by default; `false` turns it off.",
        ),
      ),
    jobs: z._default(batchJobsSchema, []),
  })
  .register(z.globalRegistry, { id: "BatchCreation" });
export type BatchCreation = z.infer<typeof batchCreationSchema>;

/** Where a Batch is: some members unfinished, paused, every member finished, or cancelled. */
export const batchStatusSchema = z.enum(["inProgress", "paused", "ended", "cancelled"]);

/** A Batch as the hub reports it. */
export const batchSchema = z
  .object({
    id: z.uuid(),
    createdAt: z.date(),
    /**
     * `inProgress` while any member is unfinished; `paused` while it is paused, whether or not any
     * member is unfinished, since jobs submitted into it are held; `ended` once every member has
     * ended; `cancelled` once it was cancelled and every member has ended.
     */
    status: batchStatusSchema,
    /** Who paused the Batch, while it is paused: a caller, or its failure breaker. */
    pausedBy: z.optional(z.enum(["caller", "failFast"])),
    /** A cancel waiting for members' Runners to stop them, present only while it waits. */
    pendingControl: z.optional(z.object({ type: z.literal("cancel"), requestedAt: z.date() })),
    labels: labelsSchema,
    failFast: failFastSchema,
    /** How many of its members have each status, and how many were deleted. */
    jobCounts: z.object({
      queued: z.int(),
      running: z.int(),
      paused: z.int(),
      succeeded: z.int(),
      failed: z.int(),
      cancelled: z.int(),
      deleted: z.int(),
    }),
    /** Its members' model usage, those deleted included, as each job reports its own. */
    usage: z.object({ totalTokens: z.int(), cost: z.object({ totalUsd: z.number() }) }),
  })
  .register(z.globalRegistry, { id: "Batch" });
export type Batch = z.infer<typeof batchSchema>;

/** What became of each job a request submitted into a Batch, in the order of the request. */
const batchJobOutcomeSchema = z.discriminatedUnion("outcome", [
  z.object({
    key: z.string(),
    /** `created` for a job accepted now, `existing` for one the Batch already had with this key. */
    outcome: z.enum(["created", "existing"]),
    job: jobSchema,
  }),
  z.object({
    key: z.string(),
    outcome: z.literal("rejected"),
    /**
     * Why: `invalid` for a job the hub refuses, as it refuses a job submitted alone, with the
     * `problems` found; `keyReused` for a key the Batch already has for a job with other content;
     * `batchCancelled` for a new job in a Batch that was cancelled.
     */
    reason: z.enum(["invalid", "keyReused", "batchCancelled"]),
    problems: z.optional(z.array(z.object({ message: z.string(), path: z.array(z.string()) }))),
  }),
]);

/** A Batch with what became of each job a request submitted into it. */
export const batchSubmissionResultSchema = z
  .object({ batch: batchSchema, jobs: z.array(batchJobOutcomeSchema) })
  .register(z.globalRegistry, { id: "BatchSubmissionResult" });
export type BatchSubmissionResult = z.infer<typeof batchSubmissionResultSchema>;

/** A Batch as a call names it. */
export const batchRefSchema = z.object({ id: z.uuid().check(z.describe("The Batch's ID.")) });

/**
 * A job as a list reports it: without its content, so without its result, its failure's message
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
 * A control that cannot take effect on the job as it is, such as a resume of a job that succeeded;
 * its message says why.
 */
export const ControlRefused = error("CONFLICT");
/** A job submitted with an idempotency key the Organization already used for a different job. */
export const IdempotencyKeyReused = error("UNPROCESSABLE_CONTENT", {
  message: "The idempotency key was already used for a different job",
});

// Calls a member's role does not allow fail with `FORBIDDEN`, and invalid ones with `BAD_REQUEST`.
// Where Better Auth decides, the hub passes its `FORBIDDEN` through, declared without a factory;
// removing a Runner raises `Forbidden` itself. The hub raises `BAD_REQUEST` itself only for what the
// contract's schemas cannot check, such as a Result contract's schema.

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
  batches: {
    create: forOrganization
      .meta(
        openapi({
          method: "POST",
          path: "/batches",
          successStatus: 201,
          description:
            "Creates a Batch, and submits its first `jobs` into it, as `POST /batches/{id}/jobs` does. Answers with the Batch and what became of each job.",
        }),
        ...byKey({ job: ["create"] }),
      )
      .errors({ [IdempotencyKeyReused.code]: IdempotencyKeyReused })
      .input(batchCreationSchema)
      .output(batchSubmissionResultSchema),
    submit: forOrganization
      .errors({ [NotFound.code]: NotFound })
      .meta(
        openapi({
          method: "POST",
          path: "/batches/{id}/jobs",
          description:
            "Submits jobs into a Batch and answers with the Batch and what became of each job, in the order given. Each job is accepted or refused on its own: a job the hub refuses is reported with its problems while the rest are accepted, and a job whose key the Batch already has returns that job when its content is the same and is refused otherwise, so a whole Batch can be submitted again to add only the jobs it lacks. Jobs submitted into a paused Batch are held; into a cancelled one, new jobs are refused. A request whose shape is invalid is refused whole.",
        }),
        ...byKey({ job: ["create"] }),
      )
      .input(z.object({ ...batchRefSchema.shape, jobs: batchJobsSchema }))
      .output(batchSubmissionResultSchema),
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
            "Pauses a Batch and answers with it: its members not yet sent to a Runner, and those submitted later, are held, `queued` with `waitingFor: batchPaused`, while those already sent carry on to their own ends. Pausing a paused Batch changes nothing; a cancelled one is refused.",
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
            "Resumes a paused Batch and answers with it: its held members are released, behind its Canary while that still runs. Resuming a Batch its failure breaker paused starts the breaker's count afresh; resuming one a caller paused, while the breaker's count would have paused it, leaves it paused by the breaker. A member a caller paused stays paused. Resuming a Batch that is not paused changes nothing; a cancelled one is refused.",
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
            "Cancels a Batch and answers with it: each unfinished member is cancelled as a job's cancel does, at once where no Runner may be running it and otherwise once its Runner stops it, shown meanwhile as the Batch's `pendingControl`. The Batch is `cancelled` once every member has ended, and new jobs submitted into it are refused. Cancelling a Batch whose members have all ended, and was not paused, changes nothing.",
        }),
        ...byKeyOrMember({ job: ["cancel"] }),
      )
      .input(batchRefSchema)
      .output(batchSchema),
  },
  jobs: {
    submit: forOrganization
      .errors({ BAD_REQUEST: {}, [IdempotencyKeyReused.code]: IdempotencyKeyReused })
      .meta(
        openapi({
          method: "POST",
          path: "/jobs",
          successStatus: 201,
          description:
            "Submits a job. The hub checks the Result contract's schema, refusing one that is invalid, describes a list or refers outside itself.",
        }),
        ...byKey({ job: ["create"] }),
      )
      .input(jobSubmissionSchema)
      .output(jobSchema),
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
            "Resumes a stopped job and answers with the job. A paused, failed or cancelled job goes back to `queued` and carries on from where it stopped, on the Runner that holds its work; one paused before it started simply starts. Resuming a job that is queued or running changes nothing, and one that gives it other limits than its own is refused: its run keeps them. A job whose pause its Runner has not yet confirmed, whose cancel waits, that succeeded, or that never started before it ended, is refused. A resume that changes the job may give it a new `runTimeoutSeconds` and `modelBudgetUsd`, and is refused when the job's model calls have cost its `modelBudgetUsd` or more, as for a job that failed with `modelBudgetExceeded`.",
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
            "Cancels a job and answers with the job. A job not yet sent to a Runner, or paused, ends `cancelled` at once. Otherwise the cancel waits for its Runner, shown as `pendingControl`, replacing a waiting pause, and the job ends `cancelled` once the Runner has stopped it, keeping its events and costs. A job that has ended, or ends before its Runner stops it, keeps the status and result it ended with.",
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
            "Deletes a finished job and answers with the job as it now reads. Its content goes from the hub at once, and from its host once its Runner has deleted its journal and Workspace; until then the job reads without its content, with `deletedAt` and a waiting `delete`, and then as not found. Its Usage record stays. An unfinished job is refused: cancel it first.",
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
            "Lists jobs, newest first, in pages of at most `limit`, each job without its content. Filters combine: a job is listed when it has one of the `status` values given, every label in `labels`, and the `batchId` and `key` given. Read the next page with the same filters and `after` set to the last job's `id`, while the page says `hasMore`.",
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
              .record(z.string(), z.string())
              .check(
                z.describe(
                  "Labels the job must all have, as `labels[customer]=acme`; repeat for more.",
                ),
              ),
          ),
          batchId: z.optional(z.uuid().check(z.describe("The Batch the job belongs to."))),
          key: z.optional(
            z
              .string()
              .check(
                z.describe(
                  "The key the job was submitted with: its idempotency key, or a member's key in the Batch `batchId` names.",
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
