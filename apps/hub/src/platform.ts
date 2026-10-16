import { Context, type Effect } from "effect";
import type { Kysely } from "kysely";

import type { Auth } from "./auth/auth.ts";
import type { Database, RunnerId } from "./db/types.ts";

// The hub runs on Workers and in Docker alike (ADR 0014). What differs between the two is a service
// with one Layer per platform (ADR 0022); everything else is shared.

/**
 * The hub's database and Better Auth on it. On Workers each request opens its own connection, since
 * Workers forbid using one in another request; in Docker one pool serves every request.
 */
export class DatabaseConnection extends Context.Service<
  DatabaseConnection,
  {
    /** Runs `work` with Better Auth and the hub's database. */
    readonly withAuth: <T>(
      work: (auth: Auth, db: Kysely<Database>) => Promise<T>,
    ) => Effect.Effect<T, unknown>;
  }
>()("hub/DatabaseConnection") {}

/**
 * Runners' WebSockets: on Workers, a Durable Object per Runner holds them; in Docker, the hub's one
 * process does.
 */
export class RunnerConnections extends Context.Service<
  RunnerConnections,
  {
    /** Closes the Runner's sockets, for a Runner the hub has removed. */
    readonly drop: (runnerId: RunnerId) => Effect.Effect<void, unknown>;
  }
>()("hub/RunnerConnections") {}
