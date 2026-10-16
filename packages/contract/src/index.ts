import { error, oc, type } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import * as z from "zod/mini";

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

/** A call made without a signed-in member or an API key holding the permission it needs. */
export const Unauthorized = error("UNAUTHORIZED");
/** A call that acts for the Organization, made before the hub is set up. */
export const HubNotSetUp = error("SERVICE_UNAVAILABLE", { message: "The hub is not set up" });
/** A call naming a Runner or an API key the hub does not have. */
export const NotFound = error("NOT_FOUND");

// Calls a member's role does not allow fail with `FORBIDDEN`, and invalid ones with `BAD_REQUEST`.
// Neither is declared with a factory: the hub passes `FORBIDDEN` through from Better Auth, and raises
// `BAD_REQUEST` itself in only one procedure. `FORBIDDEN` becomes a factory once the hub raises it.

/** Calls made by a signed-in member or with an API key. */
const authenticated = oc.errors({ [Unauthorized.code]: Unauthorized });
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
export const RUNNER_NOT_FOUND = "RUNNER_NOT_FOUND";

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
