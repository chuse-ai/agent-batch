import { OpenAPIHandler } from "@orpc/openapi/fetch";
import { ORPCError, implement } from "@orpc/server";
import { isAPIError } from "better-auth/api";
import { contract, RUNNER_CONNECT_PATH, RUNNER_NOT_FOUND } from "contract";
import { Hono, type Context } from "hono";
import { HTTPException } from "hono/http-exception";
import { sql, type Kysely } from "kysely";

import {
  getOrganization,
  requireKey,
  requireMember,
  withAuth,
  type Auth,
  type AuthEnvironment,
} from "./auth/auth.ts";
import { API_KEY_CONFIG, GRANTABLE_KEY_PERMISSIONS } from "./auth/options.ts";
import type { ApikeyId, Database, RunnerId } from "./db/types.ts";
import { findRunnerByKey, registerRunner, removeRunner } from "./runner-registration.ts";
import { listRunners } from "./runners.ts";

export { RunnerConnection } from "./runner-connection.ts";

interface ApiContext {
  auth: Auth;
  db: Kysely<Database>;
  headers: Headers;
  /** Closes the Runner's WebSocket, wherever the platform holds it. */
  dropRunnerConnection: (runnerId: RunnerId) => Promise<void>;
}

/** Variables only tests set; the deployed config declares none. */
type TestEnv = Env & { AUTH_RATE_LIMIT?: string };

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

function authEnvironment(env: TestEnv): AuthEnvironment {
  // Without them Better Auth would guess the URL from each request, which its docs warn against, and
  // sign sessions with a development secret; a hub missing either answers no request.
  const missing = (["BETTER_AUTH_SECRET", "HUB_URL"] as const).filter((name) => !env[name]);
  if (missing.length > 0) {
    throw new HTTPException(503, {
      message: `The hub is not configured: set ${missing.join(" and ")}`,
    });
  }
  return {
    connectionString: env.HYPERDRIVE.connectionString,
    secret: env.BETTER_AUTH_SECRET,
    baseURL: env.HUB_URL,
    ipAddressHeaders: [env.CLIENT_IP_HEADER],
    rateLimit: env.AUTH_RATE_LIMIT !== "false",
  };
}

const app = new Hono<{ Bindings: Env }>();

// Better Auth's own routes, such as signing in and out, at its default path.
app.on(["GET", "POST"], "/api/auth/*", (c) =>
  withAuth(authEnvironment(c.env), (auth) => auth.handler(c.req.raw)),
);

/** The hub's answer to a Runner whose key it does not know, on which the Runner stops. */
function runnerNotFound(c: Context) {
  return c.json({ code: RUNNER_NOT_FOUND }, 401);
}

app.get(RUNNER_CONNECT_PATH, (c) =>
  withAuth(authEnvironment(c.env), async (auth, db) => {
    let keyId: ApikeyId;
    try {
      keyId = (await requireKey(auth, c.req.raw.headers, { runner: ["connect"] })).id as ApikeyId;
    } catch (error) {
      if (!(error instanceof ORPCError)) throw error;
      // Better Auth answers a key it could not check, such as while the database is unreachable, as
      // it answers an unknown key. Only a hub that reaches its database tells a Runner it is unknown;
      // otherwise the Runner retries.
      try {
        await sql`select 1`.execute(db);
      } catch {
        return c.body(null, 503);
      }
      return runnerNotFound(c);
    }
    const runnerId = await findRunnerByKey(db, keyId);
    if (!runnerId) return runnerNotFound(c);
    return await c.env.RUNNER_CONNECTION.getByName(runnerId).fetch(c.req.raw);
  }),
);

app.all("*", async (c) => {
  const { matched, response } = await withAuth(authEnvironment(c.env), (auth, db) =>
    api.handle(c.req.raw, {
      context: {
        auth,
        db,
        headers: c.req.raw.headers,
        dropRunnerConnection: (runnerId) =>
          c.env.RUNNER_CONNECTION.getByName(runnerId).dropConnections(),
      },
    }),
  );
  return matched ? response : c.notFound();
});

export default app;
