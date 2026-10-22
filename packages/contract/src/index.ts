import { error, oc } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import { en } from "zod/locales";
import * as z from "zod/mini";

import {
  inputFilesSchema,
  jobIdSchema,
  resultContractSchema,
  promptSchema,
  textSchema,
} from "./job.ts";
import { jobActivitySchema, jobEventSchema } from "./runner-protocol.ts";

export * from "./problem-details.ts";
export * from "./json-rpc.ts";
export { type ResultContract } from "./job.ts";
export * from "./runner-protocol.ts";

// `zod/mini` leaves messages out unless asked; without them every problem reads "Invalid input".
z.config(en());

/** A Runner as the hub reports it. */
export const runnerSchema = z.object({
  id: z.string(),
  name: z.string(),
  /**
   * When the hub last heard from the Runner: its hello, or the latest sync or report the hub
   * recorded. A connected Runner's is at most one sync interval old.
   */
  lastSeenAt: z.date(),
});
export type Runner = z.infer<typeof runnerSchema>;

/** What an API key may do, as `{ resource: actions }`, for example `{ apiKey: ["create"] }`. */
const permissionsSchema = z.record(z.string(), z.array(z.string()));

/** An API key as the hub lists it; the key itself is shown only once, when it is created. */
export const apiKeySchema = z.object({
  id: z.string(),
  name: z.nullable(z.string()),
  /** The first characters of the key, to recognise it by. */
  start: z.nullable(z.string()),
  permissions: z.nullable(permissionsSchema),
  createdAt: z.date(),
  /** When the key stops working, if it was created to expire. */
  expiresAt: z.nullable(z.date()),
});
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
  "cancelled",
]);
export type JobStatus = z.infer<typeof jobStatusSchema>;

/** The statuses a job ends in: once a job reaches one, nothing more happens to it unless it is resumed. */
export const FINAL_JOB_STATUSES: readonly JobStatus[] = ["succeeded", "failed", "cancelled"];

/** Any JSON value, such as a job's result: what its Result contract's schema accepts. */
export type { JsonValue } from "type-fest";

/** A job as the hub reports it. */
export const jobSchema = z.object({
  id: jobIdSchema,
  status: jobStatusSchema,
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
      /**
       * pi's estimate at the models' prices, in US dollars, as decimal text without a fixed scale,
       * such as `"0.012345"`.
       */
      totalUsd: z.string(),
    }),
  }),
  /**
   * What the job is doing, for each of its conversations doing anything, as its Runner last
   * reported: present only while the job runs, and empty between steps.
   */
  activity: z.optional(jobActivitySchema),
  /** The job's result, once it has succeeded. */
  result: z.optional(z.unknown()),
  /** Why the job stopped, while it is paused: `pausedByUser` when a caller paused it; else null. */
  reason: z.nullable(z.enum(["pausedByUser"])),
  /**
   * A control the job's Runner has not yet acted on, beside the last `status` it confirmed: present
   * only while it waits. A pause or cancel waits for a job its Runner may be running, until
   * the Runner stops the job, which is then `paused` or ends `cancelled`, or the job ends first,
   * with its own status and result. A cancel replaces a waiting pause.
   */
  pendingControl: z.optional(
    z.object({ type: z.enum(["pause", "cancel"]), requestedAt: z.date() }),
  ),
});
export type Job = z.infer<typeof jobSchema>;

/** A job as a caller submits it. */
export const jobSubmissionSchema = z.object({
  /** What the agent is asked to do. */
  prompt: promptSchema,
  /** Defaults to none, so a submission without them is the same job as one with an empty list. */
  inputFiles: z._default(inputFilesSchema, {}),
  resultContract: resultContractSchema,
  /**
   * Makes submitting safe to retry: submitting again with the same key and the same job returns the
   * job already accepted, and with a different job is refused.
   */
  idempotencyKey: z.optional(textSchema.check(z.minLength(1), z.maxLength(255))),
});
export type JobSubmission = z.infer<typeof jobSubmissionSchema>;

/**
 * A page of a job's events. A caller reads the next page after the last event's `id`: at once while
 * `hasMore`, or after a few seconds otherwise, and stops once a page without more has a final
 * `status`.
 */
export const jobEventsPageSchema = z.object({
  /** The job's events after the position asked for, in ID order. */
  events: z.array(jobEventSchema),
  /** Whether more of the job's events are already stored after this page. */
  hasMore: z.boolean(),
  /**
   * The job's status as of this page, read with its events: once it is final, the job's events are
   * stored up to its end. A job whose Host is lost may end without some of its events, so a final
   * status says nothing follows, not that every event is here.
   */
  status: jobStatusSchema,
});
export type JobEventsPage = z.infer<typeof jobEventsPageSchema>;

/** A call made without a signed-in member or an API key holding the permission it needs. */
export const Unauthorized = error("UNAUTHORIZED");
/** A call that acts for the Organization, made before the hub is set up. */
export const HubNotSetUp = error("SERVICE_UNAVAILABLE", { message: "The hub is not set up" });
/** A call naming a Runner, an API key or a job the hub does not have. */
export const NotFound = error("NOT_FOUND");
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
// Neither is declared with a factory: the hub passes `FORBIDDEN` through from Better Auth, and raises
// `BAD_REQUEST` itself in only two procedures. `FORBIDDEN` becomes a factory once the hub raises it.

/**
 * Every call of the API, whose bodies are all JSON. oRPC's client reads only `application/json` as
 * JSON by itself, and errors arrive as Problem Details.
 */
const api = oc.meta(openapi({ responseBodyHint: "json" }));
/** Calls made by a signed-in member or with an API key. */
const authenticated = api.errors({ [Unauthorized.code]: Unauthorized });
/** Calls that act for the Organization, which exists once the hub is set up. */
const forOrganization = authenticated.errors({ [HubNotSetUp.code]: HubNotSetUp });

/** The hub's public HTTP API. Callers sign in as a member or send an API key in `x-api-key`. */
export const contract = {
  runners: {
    list: authenticated
      .meta(openapi({ method: "GET", path: "/runners" }))
      .output(z.array(runnerSchema)),
    /**
     * Registers a new Runner, called with the shared key Runners start with. Returns the Runner's ID
     * and its own key, which the Runner keeps and connects with.
     */
    register: forOrganization
      .meta(openapi({ method: "POST", path: "/runners/register" }))
      .input(z.object({ name: z.string() }))
      .output(registeredRunnerSchema),
    /**
     * Removes a Runner, for a host that is retired or compromised: its own key stops working and its
     * connection is dropped at once. A compromised host also holds the shared key Runners register
     * with, so revoke that too and create a new one for future hosts; Runners already registered
     * keep working with their own keys.
     */
    delete: authenticated
      .errors({ FORBIDDEN: {}, [NotFound.code]: NotFound })
      .meta(openapi({ method: "DELETE", path: "/runners/{id}" }))
      .input(z.object({ id: z.string() })),
  },
  apiKeys: {
    create: forOrganization
      .errors({ BAD_REQUEST: {}, FORBIDDEN: {} })
      .meta(openapi({ method: "POST", path: "/api-keys" }))
      .input(
        z.object({
          name: z.string(),
          permissions: permissionsSchema,
          /** Seconds until the key stops working, from one day to a year; without it, it never does. */
          expiresIn: z.optional(z.number()),
        }),
      )
      .output(createdKeySchema),
    list: forOrganization
      .errors({ FORBIDDEN: {} })
      .meta(openapi({ method: "GET", path: "/api-keys" }))
      .output(z.array(apiKeySchema)),
    revoke: authenticated
      .errors({ FORBIDDEN: {}, [NotFound.code]: NotFound })
      .meta(openapi({ method: "DELETE", path: "/api-keys/{id}" }))
      .input(z.object({ id: z.string() })),
  },
  jobs: {
    /**
     * Submits a job, called with an API key holding `job: ["create"]`. The hub checks the Result
     * contract's schema, refusing one that is invalid, describes a list or refers outside itself.
     */
    submit: forOrganization
      .errors({ BAD_REQUEST: {}, [IdempotencyKeyReused.code]: IdempotencyKeyReused })
      .meta(openapi({ method: "POST", path: "/jobs", successStatus: 201 }))
      .input(jobSubmissionSchema)
      .output(jobSchema),
    /** Reads a job, called by a signed-in member or with an API key holding `job: ["read"]`. */
    get: authenticated
      .errors({ [NotFound.code]: NotFound })
      .meta(openapi({ method: "GET", path: "/jobs/{id}" }))
      .input(z.object({ id: z.string() }))
      .output(jobSchema),
    /**
     * Pauses a job, called by a signed-in member or with an API key holding `job: ["pause"]`, and
     * answers with the job. A job not yet sent to a Runner is held at once as `paused`. Otherwise the
     * pause waits for its Runner, shown as `pendingControl`, and the job is `paused` once the Runner
     * has stopped it, its work kept for a resume, holding no capacity. Pausing a paused job, or one
     * whose pause waits, changes nothing; a job that has ended, or whose cancel waits, is refused.
     */
    pause: authenticated
      .errors({ [NotFound.code]: NotFound, [ControlRefused.code]: ControlRefused })
      .meta(openapi({ method: "POST", path: "/jobs/{id}/pause" }))
      .input(z.object({ id: z.string() }))
      .output(jobSchema),
    /**
     * Resumes a stopped job, called by a signed-in member or with an API key holding
     * `job: ["resume"]`, and answers with the job. A paused, failed or cancelled job goes back to
     * `queued` and carries on from where it stopped, on the Runner that holds its work; one paused
     * before it started simply starts. Resuming a job that is queued or running changes nothing. A
     * job whose pause its Runner has not yet confirmed, whose cancel waits, that succeeded, or that
     * never started before it ended, is refused.
     */
    resume: authenticated
      .errors({ [NotFound.code]: NotFound, [ControlRefused.code]: ControlRefused })
      .meta(openapi({ method: "POST", path: "/jobs/{id}/resume" }))
      .input(z.object({ id: z.string() }))
      .output(jobSchema),
    /**
     * Cancels a job, called by a signed-in member or with an API key holding `job: ["cancel"]`, and
     * answers with the job. A job not yet sent to a Runner, or paused, ends `cancelled` at once.
     * Otherwise the cancel waits for its Runner, shown as `pendingControl`, replacing a waiting
     * pause, and the job ends `cancelled` once the Runner has stopped it, keeping its events and
     * costs. A job that has ended, or ends before its Runner stops it, keeps the status and result it ended with.
     */
    cancel: authenticated
      .errors({ [NotFound.code]: NotFound })
      .meta(openapi({ method: "POST", path: "/jobs/{id}/cancel" }))
      .input(z.object({ id: z.string() }))
      .output(jobSchema),
    /**
     * Reads a job's events in pages, during and after its run, called by a signed-in member or with
     * an API key holding `job: ["read"]`. A page holds at most `limit` events and stops early after
     * about 1 MiB of them, though it always holds at least one if there are any.
     */
    events: authenticated
      .errors({ [NotFound.code]: NotFound })
      .meta(openapi({ method: "GET", path: "/jobs/{id}/events" }))
      .input(
        z.object({
          id: z.string(),
          /** A position: the ID of the last event the caller has, 0 to read from the start. */
          after: z._default(z.int().check(z.nonnegative()), 0),
          limit: z._default(z.int().check(z.minimum(1), z.maximum(100)), 100),
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
