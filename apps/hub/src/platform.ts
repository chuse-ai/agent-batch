import { type Cause, Context, type Effect } from "effect";

import type { Auth } from "./auth/auth.ts";
import type { Db } from "./db/database.ts";
import type { RunnerId } from "./db/types.ts";

// The hub runs on Workers and in Docker alike (ADR 0014). What differs between the two is a service
// with one Layer per platform (ADR 0022); everything else is shared.

/**
 * Runners' WebSockets: on Workers, a Durable Object per Runner holds them; in Docker, the hub's one
 * process does.
 */
export class RunnerConnections extends Context.Service<
  RunnerConnections,
  {
    /** Closes the Runner's sockets, for a Runner the hub has removed. */
    readonly drop: (runnerId: RunnerId) => Effect.Effect<void, Cause.UnknownError>;
    /**
     * Tells the Runner to sync now, such as for a new job or a control on one of its jobs. A Runner
     * that is not connected misses nothing: its next sync finds the same.
     */
    readonly hint: (runnerId: RunnerId) => Effect.Effect<void, Cause.UnknownError>;
  }
>()("hub/RunnerConnections") {}

/**
 * The services every platform provides to the hub: its database and Better Auth on it, built once in
 * Docker and per request on Workers, and its Runners' connections.
 */
export type HubServices = Db | Auth | RunnerConnections;
