import { defineMeta, error, oc } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import { en } from "zod/locales";
import * as z from "zod/mini";

import {
  inputFilesSchema,
  jobIdSchema,
  modelBudgetUsdSchema,
  resultContractSchema,
  promptSchema,
  textSchema,
  runTimeoutSecondsSchema,
} from "./job.ts";
import { PROBLEM_DETAILS_CONTENT_TYPE } from "./problem-details.ts";
import { failureSchema, jobActivitySchema, jobEventSchema } from "./runner-protocol.ts";

export * from "./problem-details.ts";
export * from "./json-rpc.ts";
export { jsonTextSchema, type ResultContract } from "./job.ts";
export * from "./runner-protocol.ts";

// `zod/mini` leaves messages out unless asked; without them every problem reads "Invalid input".
z.config(en());

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

/**
 * The permissions a call can require of an API key, by resource: `register` for the shared key
 * Runners start with, `connect` for a Runner's own key, and the job calls. The hub's access control
 * holds them among its statements.
 */
export const KEY_PERMISSIONS = {
  runner: ["register", "connect"],
  job: ["create", "read", "pause", "resume", "cancel", "delete"],
} as const;

/** Actions of one resource of `KEY_PERMISSIONS` a call requires, such as `{ job: ["pause"] }`. */
export type KeyPermissions = {
  [Resource in keyof typeof KEY_PERMISSIONS]: {
    [Only in Resource]: (typeof KEY_PERMISSIONS)[Resource][number][];
  };
}[keyof typeof KEY_PERMISSIONS];

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

/** A newly registered Runner: its ID and its own key, which it keeps and connects with. */
export const registeredRunnerSchema = z.object({ id: z.string(), key: z.string() });
export type RegisteredRunner = z.infer<typeof registeredRunnerSchema>;

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
const jobBaseShape = {
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
};

/**
 * A job as the hub reports it: once it has succeeded, with its `result`, and once it has failed,
 * with its `failure`. Only a caller pauses or cancels a job, so the other statuses carry neither.
 */
export const jobSchema = z
  .discriminatedUnion("status", [
    z.object({ ...jobBaseShape, status: z.enum(["queued", "running", "paused", "cancelled"]) }),
    z.object({
      ...jobBaseShape,
      status: z.literal("succeeded"),
      /** The job's result. Absent once the job is deleted, as its content is. */
      result: z.optional(z.unknown()),
    }),
    z.object({
      ...jobBaseShape,
      status: z.literal("failed"),
      /** Why the job failed. Its message is absent once the job is deleted, as its content is. */
      failure: failureSchema,
    }),
  ])
  .register(z.globalRegistry, { id: "Job" });
export type Job = z.infer<typeof jobSchema>;

/** A job as a call names it. */
export const jobRefSchema = z.object({ id: jobIdSchema.check(z.describe("The job's ID.")) });

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
  })
  .register(z.globalRegistry, { id: "JobSubmission" });
export type JobSubmission = z.infer<typeof jobSubmissionSchema>;

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

/** A call made without a signed-in member or an API key holding the permission it needs. */
export const Unauthorized = error("UNAUTHORIZED");
/** A call that acts for the Organization, made before the hub is set up. */
export const HubNotSetUp = error("SERVICE_UNAVAILABLE", { message: "The hub is not set up" });
/** A call naming a Runner, an API key or a job the hub does not have. */
export const NotFound = error("NOT_FOUND");

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
 * Every call of the API, whose bodies are all JSON. oRPC's client reads only `application/json` as
 * JSON by itself, and errors arrive as Problem Details. Every call says who may make it (`byKey`,
 * `byMember` or `byKeyOrMember`), and the hub refuses anyone else with `UNAUTHORIZED`.
 */
const api = oc.errors({ [Unauthorized.code]: Unauthorized }).meta(
  openapi({ responseBodyHint: "json" }),
  // oRPC's OpenAPI document labels every error response `application/json`, with no setting to
  // change it (middleapi/orpc#2317).
  openapi.spec((operation) => ({
    ...operation,
    responses: Object.fromEntries(
      Object.entries(operation.responses ?? {}).map(([status, response]) => {
        const body = "content" in response ? response.content?.["application/json"] : undefined;
        return /^[45]/.test(status) && body
          ? [status, { ...response, content: { [PROBLEM_DETAILS_CONTENT_TYPE]: body } }]
          : [status, response];
      }),
    ),
  })),
);
/**
 * How the OpenAPI document describes an API key, sent in `x-api-key`. A call's security lists the
 * permissions the key needs as role names.
 */
export const API_KEY_SECURITY_SCHEME = {
  type: "apiKey",
  in: "header",
  name: "x-api-key",
  description:
    "An API key, sent in `x-api-key`. A call lists the permissions the key needs as `resource:action`, all of them required. A call that also takes a member's session checks the key whenever one is sent, whatever session the request carries.",
} as const;

/**
 * Who may make a call: with an API key holding the permissions in `key`, as a signed-in member, or
 * either. The hub checks it before every call, and the OpenAPI document gives it as the call's
 * `security`.
 */
export interface Access {
  key?: KeyPermissions;
  member?: true;
}

const [accessMeta, getAccess] = defineMeta("access", (incoming: Access) => incoming);
/** The access a procedure declares; the hub refuses to run a procedure that declares none. */
export { getAccess };

/** A key's permissions as OpenAPI role names, `resource:action`, such as `job:pause`. */
function roleNamesOf(permissions: KeyPermissions) {
  return Object.entries<string[]>(permissions).flatMap(([resource, actions]) =>
    actions.map((action) => `${resource}:${action}`),
  );
}

/**
 * Declares who may make a call, as the hub's access and as the OpenAPI document's `security`, where
 * an API key (`apiKey`) and a signed-in member (`session`, the hub's session cookie, which the hub
 * names for its deployment) are alternatives.
 */
function calledBy(access: Access) {
  const security: Record<string, string[]>[] = [];
  if (access.key) security.push({ apiKey: roleNamesOf(access.key) });
  if (access.member) security.push({ session: [] });
  return [accessMeta(access), openapi.spec((operation) => ({ ...operation, security }))] as const;
}
const byKey = (permissions: KeyPermissions) => calledBy({ key: permissions });
const byMember = calledBy({ member: true });
const byKeyOrMember = (permissions: KeyPermissions) => calledBy({ key: permissions, member: true });

/** Calls that act for the Organization, which exists once the hub is set up. */
const forOrganization = api.errors({ [HubNotSetUp.code]: HubNotSetUp });

/** The hub's public HTTP API. Callers sign in as a member or send an API key in `x-api-key`. */
export const contract = {
  runners: {
    list: api
      .meta(openapi({ method: "GET", path: "/runners" }), ...byMember)
      .output(z.array(runnerSchema)),
    register: forOrganization
      .meta(
        openapi({
          method: "POST",
          path: "/runners/register",
          description:
            "Registers a new Runner, called with the shared key Runners start with. Returns the Runner's ID and its own key, which the Runner keeps and connects with.",
        }),
        ...byKey({ runner: ["register"] }),
      )
      .input(
        z.object({ name: z.string().check(z.describe("The Runner's name, as the hub lists it.")) }),
      )
      .output(registeredRunnerSchema),
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
  },
};

/**
 * Where a Runner opens its WebSocket to the hub, sending its own key in `x-api-key`; the hub takes
 * the Runner's identity from the key.
 */
export const RUNNER_CONNECT_PATH = "/runners/connect";

/**
 * The `code` in the body of the hub's 401 when it no longer knows a Runner's key: the Runner was
 * removed, or the hub was reset. A Runner stops only on this answer and retries any other refusal,
 * which may come from a proxy in front of the hub or from a hub that cannot reach its database.
 */
export const RUNNER_NOT_FOUND = "runnerNotFound";

/** How often a Runner sends a heartbeat. */
export const HEARTBEAT_INTERVAL_MS = 30_000;
/** After this long without pongs (three missed), a Runner treats the hub as gone and reconnects. */
export const HEARTBEAT_TIMEOUT_MS = 3 * HEARTBEAT_INTERVAL_MS;
/**
 * A heartbeat on the wire: the Runner sends `HEARTBEAT_PING` as a text message, and the hub answers
 * `HEARTBEAT_PONG`; on Workers the runtime answers without waking the hub. Neither is a JSON-RPC
 * message, so both are answered before the protocol sees a message.
 */
export const HEARTBEAT_PING = "ping";
export const HEARTBEAT_PONG = "pong";
