import { OpenAPIHandler } from "@orpc/openapi/fetch";
import { ORPCError, implement } from "@orpc/server";
import { isAPIError } from "better-auth/api";
import { contract, RUNNER_CONNECT_PATH, RUNNER_NOT_FOUND } from "contract";
import { Effect } from "effect";
import { Hono } from "hono";
import { sql, type Kysely } from "kysely";

import { getOrganization, requireKey, requireMember, type Auth } from "./auth/auth.ts";
import { API_KEY_CONFIG, GRANTABLE_KEY_PERMISSIONS } from "./auth/options.ts";
import type { ApikeyId, Database, RunnerId } from "./db/types.ts";
import { DatabaseConnection, RunnerConnections } from "./platform.ts";
import { findRunnerByKey, registerRunner, removeRunner } from "./runner-registration.ts";
import { listRunners } from "./runners.ts";

interface ApiContext {
  auth: Auth;
  db: Kysely<Database>;
  headers: Headers;
  /** Closes the Runner's WebSocket, wherever the platform holds it. */
  dropRunnerConnection: (runnerId: RunnerId) => Promise<void>;
}

const os = implement<typeof contract, ApiContext>(contract).use(async ({ next }) => {
  // oRPC answers an error it does not know with a 500, so Better Auth's errors, which carry their
  // status, become oRPC's.
  try {
    return await next();
  } catch (error) {
    if (!isAPIError(error)) throw error;
    // Better Auth names its statuses as oRPC names its codes, such as `BAD_REQUEST` and `NOT_FOUND`.
    throw new ORPCError(String(error.status), { message: error.body?.message });
  }
});

const router = os.router({
  runners: {
    list: os.runners.list.handler(async ({ context }) => {
      await requireMember(context.auth, context.headers);
      return listRunners(context.db);
    }),
    register: os.runners.register.handler(async ({ input, context }) => {
      await requireKey(context.auth, context.headers, { runner: ["register"] });
      return registerRunner(context.auth, context.db, input.name);
    }),
    delete: os.runners.delete.handler(async ({ input, context }) => {
      await requireMember(context.auth, context.headers);
      // The ID enters the hub here.
      const runnerId = input.id as RunnerId;
      await removeRunner(context.auth, context.db, context.headers, runnerId);
      await context.dropRunnerConnection(runnerId);
    }),
  },
  apiKeys: {
    create: os.apiKeys.create.handler(async ({ input, context }) => {
      const member = await requireMember(context.auth, context.headers);
      const grant = GRANTABLE_KEY_PERMISSIONS.authorize(input.permissions);
      if (!grant.success) throw new ORPCError("BAD_REQUEST", { message: grant.error });
      const organization = await getOrganization(context.db);
      // Without request headers, as a call from the hub itself: Better Auth sets a key's permissions
      // only then, and still checks that the member may create the Organization's keys.
      const key = await context.auth.api.createApiKey({
        body: {
          configId: API_KEY_CONFIG,
          organizationId: organization.id,
          userId: member.id,
          name: input.name,
          permissions: input.permissions,
          expiresIn: input.expiresIn,
        },
      });
      return { id: key.id, key: key.key };
    }),
    list: os.apiKeys.list.handler(async ({ context }) => {
      await requireMember(context.auth, context.headers);
      const organization = await getOrganization(context.db);
      const { apiKeys } = await context.auth.api.listApiKeys({
        headers: context.headers,
        query: { organizationId: organization.id, configId: API_KEY_CONFIG },
      });
      return apiKeys;
    }),
    revoke: os.apiKeys.revoke.handler(async ({ input, context }) => {
      await requireMember(context.auth, context.headers);
      await context.auth.api.deleteApiKey({
        headers: context.headers,
        body: { keyId: input.id, configId: API_KEY_CONFIG },
      });
    }),
  },
});

const api = new OpenAPIHandler(router);

/**
 * The challenge every 401 carries, as HTTP requires (RFC 9110, section 15.5.2): callers authenticate
 * with an API key in `x-api-key`, or members with Better Auth's session cookie, for which HTTP has no
 * scheme.
 */
export const AUTH_CHALLENGE = 'ApiKey realm="agent-batch"';

/** What the app needs from the platform it runs on. */
export interface HubPlatform<Bindings> {
  /** Runs `effect` with the platform's services, for a request made with `env`. */
  run<A>(
    env: Bindings,
    effect: Effect.Effect<A, unknown, DatabaseConnection | RunnerConnections>,
  ): Promise<A>;
  /**
   * Accepts the WebSocket that `request` opens for the Runner `runnerId`, already authenticated, on
   * platforms where upgrade requests reach the app.
   */
  acceptRunner(env: Bindings, request: Request, runnerId: RunnerId): Promise<Response>;
}

/** The hub's answer to a Runner whose key it does not know, on which the Runner stops. */
function runnerNotFound(): Response {
  return Response.json(
    { code: RUNNER_NOT_FOUND },
    { status: 401, headers: { "www-authenticate": AUTH_CHALLENGE } },
  );
}

/**
 * Decides a request to open a Runner's WebSocket: the Runner it comes from, or the response refusing
 * it. Both hubs accept the socket their own way: on Workers the route hands it to the Runner's
 * Durable Object, and in Docker the server's upgrade hook takes it.
 */
export function decideRunnerUpgrade(
  request: Request,
): Effect.Effect<RunnerId | Response, unknown, DatabaseConnection> {
  // Only a WebSocket opens here; HTTP's answer to anything else names the protocol to upgrade to.
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return Effect.succeed(new Response(null, { status: 426, headers: { upgrade: "websocket" } }));
  }
  return DatabaseConnection.use((database) =>
    database.withAuth((auth, db) => authenticateRunner(request.headers, auth, db)),
  );
}

/** The Runner whose own key `headers` carry, or the hub's answer refusing it. */
async function authenticateRunner(
  headers: Headers,
  auth: Auth,
  db: Kysely<Database>,
): Promise<RunnerId | Response> {
  let keyId: ApikeyId;
  try {
    keyId = (await requireKey(auth, headers, { runner: ["connect"] })).id as ApikeyId;
  } catch (error) {
    if (!(error instanceof ORPCError)) throw error;
    // Better Auth answers a key it could not check, such as while the database is unreachable, as it
    // answers an unknown key. Only a hub that reaches its database tells a Runner it is unknown;
    // otherwise the Runner retries.
    try {
      await sql`select 1`.execute(db);
    } catch {
      return new Response(null, { status: 503 });
    }
    return runnerNotFound();
  }
  return (await findRunnerByKey(db, keyId)) ?? runnerNotFound();
}

/** The hub's HTTP API, Better Auth's routes and the Runners' WebSocket endpoint, on any platform. */
export function createApp<Bindings extends object>(platform: HubPlatform<Bindings>) {
  const app = new Hono<{ Bindings: Bindings }>();

  app.use(async (c, next) => {
    await next();
    if (c.res.status === 401 && !c.res.headers.has("www-authenticate")) {
      c.res.headers.set("www-authenticate", AUTH_CHALLENGE);
    }
  });

  // Better Auth's own routes, such as signing in and out, at its default path.
  app.on(["GET", "POST"], "/api/auth/*", (c) =>
    platform.run(
      c.env,
      DatabaseConnection.use((database) => database.withAuth((auth) => auth.handler(c.req.raw))),
    ),
  );

  app.get(RUNNER_CONNECT_PATH, async (c) => {
    const runner = await platform.run(c.env, decideRunnerUpgrade(c.req.raw));
    if (runner instanceof Response) return runner;
    return await platform.acceptRunner(c.env, c.req.raw, runner);
  });

  app.all("*", async (c) => {
    const { matched, response } = await platform.run(
      c.env,
      Effect.gen(function* () {
        const database = yield* DatabaseConnection;
        const connections = yield* RunnerConnections;
        return yield* database.withAuth((auth, db) =>
          api.handle(c.req.raw, {
            context: {
              auth,
              db,
              headers: c.req.raw.headers,
              dropRunnerConnection: (runnerId) => Effect.runPromise(connections.drop(runnerId)),
            },
          }),
        );
      }),
    );
    return matched ? response : c.notFound();
  });

  return app;
}
