import { error, oc, type } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import isValidFilename from "valid-filename";
import { en } from "zod/locales";
import * as z from "zod/mini";

export * from "./problem-details.ts";

// `zod/mini` leaves messages out unless asked; without them every problem reads "Invalid input".
z.config(en());

/** A Runner as the hub reports it. */
export const runnerSchema = z.object({
  id: z.string(),
  name: z.string(),
  /**
   * When the hub last heard from the Runner: its latest subscription, or a heartbeat the hub has
   * recorded. A connected Runner's is at most one heartbeat timeout old.
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
export const jobStateSchema = z.enum([
  "queued",
  "running",
  "paused",
  "succeeded",
  "failed",
  "cancelled",
]);
export type JobState = z.infer<typeof jobStateSchema>;

/** A job as the hub reports it. */
export const jobSchema = z.object({
  id: z.string(),
  state: jobStateSchema,
  createdAt: z.date(),
});
export type Job = z.infer<typeof jobSchema>;

/**
 * Text as a job keeps it. JSON can carry a lone UTF-16 surrogate, which is not Unicode text and which
 * neither UTF-8 storage nor canonical JSON accepts, and NUL, which Postgres text cannot hold.
 */
const textSchema = z
  .string()
  .check(
    z.refine(
      (text) => text.isWellFormed() && !text.includes("\0"),
      "Must be Unicode text without NUL",
    ),
  );

/** File systems limit a file name to 255 bytes of UTF-8; `valid-filename` counts UTF-16 units. */
const MAX_NAME_BYTES = 255;

/** An Input file's name: one file name, without folders, valid wherever a Runner writes it. */
const fileNameSchema = textSchema.check(
  z.refine(isValidFilename),
  z.refine((name) => new TextEncoder().encode(name).length <= MAX_NAME_BYTES),
);

/** An Input file's content, given inline as UTF-8 text. */
const inputFileSchema = z.object({ content: textSchema });

/**
 * Files written into the job's Workspace before its agent starts, by file name, as GitHub's Gists
 * take them: the agent finds them all in one folder, and no two can share a name. Zod reports a bad
 * key without the key's own messages, so the record says what a name must be.
 */
const inputFilesSchema = z.record(fileNameSchema, inputFileSchema, {
  error: (issue) =>
    issue.code === "invalid_key"
      ? `Must be a file name that is valid on Linux, macOS and Windows, of at most ${MAX_NAME_BYTES} bytes`
      : undefined,
});

/**
 * The result a job must produce: a JSON Schema (2020-12 unless `$schema` says otherwise) describing
 * a single value, not a list, and referring to nothing outside itself.
 */
const outputContractSchema = z.object({ schema: z.record(z.string(), z.unknown()) });
export type OutputContract = z.infer<typeof outputContractSchema>;

/** A job as a caller submits it. */
export const jobSubmissionSchema = z.object({
  /** What the agent is asked to do. */
  prompt: textSchema.check(z.minLength(1)),
  /** Defaults to none, so a submission without them is the same job as one with an empty list. */
  inputFiles: z._default(inputFilesSchema, {}),
  outputContract: outputContractSchema,
  /**
   * Makes submitting safe to retry: submitting again with the same key and the same job returns the
   * job already accepted, and with a different job is refused.
   */
  idempotencyKey: z.optional(textSchema.check(z.minLength(1), z.maxLength(255))),
});
export type JobSubmission = z.infer<typeof jobSubmissionSchema>;

/** A call made without a signed-in member or an API key holding the permission it needs. */
export const Unauthorized = error("UNAUTHORIZED");
/** A call that acts for the Organization, made before the hub is set up. */
export const HubNotSetUp = error("SERVICE_UNAVAILABLE", { message: "The hub is not set up" });
/** A call naming a Runner, an API key or a job the hub does not have. */
export const NotFound = error("NOT_FOUND");
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
     * Submits a job, called with an API key holding `job: ["create"]`. The hub checks the Output
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
 * which may come from a proxy in front of the hub or from a hub that cannot reach its database. The
 * refusal belongs to the Runner protocol, not the API, so it is not a Problem Details document.
 */
export const RUNNER_NOT_FOUND = "runnerNotFound";

/** How often a Runner sends a heartbeat. */
export const HEARTBEAT_INTERVAL_MS = 30_000;
/** After this long without heartbeats (three missed), each side treats the other as gone. */
export const HEARTBEAT_TIMEOUT_MS = 3 * HEARTBEAT_INTERVAL_MS;
/**
 * A heartbeat on the wire: the Runner sends `HEARTBEAT_PING` as a text message, and the hub answers
 * `HEARTBEAT_PONG`; on Workers the runtime answers without waking the hub. Neither is an oRPC
 * message, so oRPC ignores both.
 */
export const HEARTBEAT_PING = "ping";
export const HEARTBEAT_PONG = "pong";

/** What the hub tells a Runner to do. None exist yet; jobs will be the first. */
export type Command = never;

/** What a Runner calls on the hub over its WebSocket. */
export const runnerProtocol = {
  /**
   * Registers the Runner and streams the hub's commands to it while the socket stays open; a Runner
   * subscribes again whenever the stream ends. The output is declared with `type` because the hub
   * answers with oRPC's hibernating iterator, which a validating `eventIterator` schema would wrap.
   */
  commands: oc.input(z.object({ name: z.string() })).output(type<AsyncIteratorObject<Command>>()),
};
