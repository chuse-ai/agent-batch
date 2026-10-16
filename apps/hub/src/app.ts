import { catchORPCErrorCode, type WithEffectContext } from "@orpc/experimental-effect";
import "@orpc/experimental-effect/extensions/effect";
import { OpenAPIHandler } from "@orpc/openapi/fetch";
import { implement, ValidationError } from "@orpc/server";
import { contract, RUNNER_CONNECT_PATH, RUNNER_NOT_FOUND } from "contract";
import { type Context, Effect } from "effect";
import { Hono } from "hono";
import {
  PROBLEM_JSON_CONTENT_TYPE,
  problemDetails,
  problemDetailsHandler,
} from "hono-problem-details";
import { sql } from "kysely";

import {
  Auth,
  getOrganization,
  type KeyMetadata,
  requireKey,
  requireKeyOrMember,
  requireMember,
} from "./auth/auth.ts";
import { API_KEY_CONFIG, GRANTABLE_KEY_PERMISSIONS } from "./auth/options.ts";
import { Db } from "./db/database.ts";
import type { ApikeyId, JobId, RunnerId } from "./db/types.ts";
import { getJob, outputContractProblems, submitJob, submitterOf } from "./jobs.ts";
import { RunnerConnections, type HubServices } from "./platform.ts";
import { toProblemDetails } from "./problem-details.ts";
import { findRunnerByKey, registerRunner, removeRunner } from "./runner-registration.ts";
import { listRunners } from "./runners.ts";

interface ApiContext extends WithEffectContext<HubServices> {
  headers: Headers;
}

const os = implement<typeof contract, ApiContext>(contract);

const router = os.router({
  runners: {
    list: os.runners.list.effect(function* ({ context }) {
      yield* requireMember(context.headers);
      return yield* listRunners;
    }),
    register: os.runners.register.effect(function* ({ input, context }) {
      yield* requireKey(context.headers, { runner: ["register"] });
      return yield* registerRunner(input.name);
    }),
    delete: os.runners.delete.effect(function* ({ input, context }) {
      yield* requireMember(context.headers);
      // The ID enters the hub here.
      const runnerId = input.id as RunnerId;
      yield* removeRunner(context.headers, runnerId);
      yield* RunnerConnections.use((connections) => connections.drop(runnerId));
    }),
  },
  apiKeys: {
    create: os.apiKeys.create.effect(function* ({ input, context, errors }) {
      const member = yield* requireMember(context.headers);
      const grant = GRANTABLE_KEY_PERMISSIONS.authorize(input.permissions);
      if (!grant.success) return yield* Effect.fail(errors.BAD_REQUEST({ message: grant.error }));
      const organization = yield* getOrganization;
      const auth = yield* Auth;
      // Without request headers, as a call from the hub itself: Better Auth sets a key's permissions
      // only then, and still checks that the member may create the Organization's keys.
      const key = yield* auth.call((betterAuth) =>
        betterAuth.api.createApiKey({
          body: {
            configId: API_KEY_CONFIG,
            organizationId: organization.id,
            userId: member.id,
            name: input.name,
            permissions: input.permissions,
            expiresIn: input.expiresIn,
            metadata: { createdBy: member.id } satisfies KeyMetadata,
          },
        }),
      );
      return { id: key.id, key: key.key };
    }),
    list: os.apiKeys.list.effect(function* ({ context }) {
      yield* requireMember(context.headers);
      const organization = yield* getOrganization;
      const auth = yield* Auth;
      const { apiKeys } = yield* auth.call((betterAuth) =>
        betterAuth.api.listApiKeys({
          headers: context.headers,
          query: { organizationId: organization.id, configId: API_KEY_CONFIG },
        }),
      );
      return apiKeys;
    }),
    revoke: os.apiKeys.revoke.effect(function* ({ input, context }) {
      yield* requireMember(context.headers);
      const auth = yield* Auth;
      yield* auth.call((betterAuth) =>
        betterAuth.api.deleteApiKey({
          headers: context.headers,
          body: { keyId: input.id, configId: API_KEY_CONFIG },
        }),
      );
    }),
  },
  jobs: {
    submit: os.jobs.submit.effect(function* ({ input, context, errors }) {
      const key = yield* requireKey(context.headers, { job: ["create"] });
      const problems = outputContractProblems(input.outputContract.schema);
      if (problems.length > 0) {
        // Raised as oRPC raises invalid input, so the answer lists the problems the same way.
        const path = ["outputContract", "schema"];
        const cause = new ValidationError({
          message: "Input validation failed",
          issues: problems.map((message) => ({ message, path })),
          invalidData: input,
        });
        return yield* Effect.fail(errors.BAD_REQUEST({ cause }));
      }
      return yield* submitJob(input, submitterOf(key));
    }),
    get: os.jobs.get.effect(function* ({ input, context }) {
      yield* requireKeyOrMember(context.headers, { job: ["read"] });
      // The ID enters the hub here.
      return yield* getJob(input.id as JobId);
    }),
  },
});

const api = new OpenAPIHandler(router, { customErrorResponseBodyEncoder: toProblemDetails });

/**
 * The challenge every 401 carries, as HTTP requires (RFC 9110, section 15.5.2): callers authenticate
 * with an API key in `x-api-key`, or members with Better Auth's session cookie, for which HTTP has no
 * scheme.
 */
export const AUTH_CHALLENGE = 'ApiKey realm="agent-batch"';

/** What the app needs from the platform it runs on. */
export interface HubPlatform<Bindings> {
  /** Runs `effect` with the platform's services, for a request made with `env`. */
  run<A, E>(env: Bindings, effect: Effect.Effect<A, E, HubServices>): Promise<A>;
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
export function decideRunnerUpgrade(request: Request) {
  // Only a WebSocket opens here; HTTP's answer to anything else names the protocol to upgrade to.
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return Effect.succeed(new Response(null, { status: 426, headers: { upgrade: "websocket" } }));
  }
  return authenticateRunner(request.headers);
}

/** The Runner whose own key `headers` carry, or the hub's answer refusing it. */
function authenticateRunner(headers: Headers) {
  return Effect.gen(function* () {
    const key = yield* requireKey(headers, { runner: ["connect"] });
    return (yield* findRunnerByKey(key.id as ApikeyId)) ?? runnerNotFound();
  }).pipe(
    // Better Auth answers a key it could not check, such as while the database is unreachable, as it
    // answers an unknown key. Only a hub that reaches its database tells a Runner it is unknown;
    // otherwise the Runner retries.
    catchORPCErrorCode("UNAUTHORIZED", () =>
      Db.use((database) => database.query((db) => sql`select 1`.execute(db))).pipe(
        Effect.as(runnerNotFound()),
        Effect.orElseSucceed(() => new Response(null, { status: 503 })),
      ),
    ),
  );
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
      Auth.use((auth) => auth.call((betterAuth) => betterAuth.handler(c.req.raw))),
    ),
  );

  // Paths outside the API, and failures outside oRPC, answer as the API does.
  app.notFound(() => problemDetails({ status: 404 }).getResponse());
  app.onError(problemDetailsHandler({ onUnhandledError: (error) => console.error(error) }));

  app.get(RUNNER_CONNECT_PATH, async (c) => {
    const runner = await platform.run(c.env, decideRunnerUpgrade(c.req.raw));
    if (runner instanceof Response) return runner;
    return await platform.acceptRunner(c.env, c.req.raw, runner);
  });

  app.all("*", async (c) => {
    const { matched, response } = await platform.run(
      c.env,
      Effect.contextWith((services: Context.Context<HubServices>) =>
        Effect.promise(() =>
          api.handle(c.req.raw, {
            context: { "effect/context": services, headers: c.req.raw.headers },
          }),
        ),
      ),
    );
    if (!matched) return c.notFound();
    // oRPC labels every JSON body `application/json`; its error bodies are Problem Details.
    if (response.status >= 400) response.headers.set("content-type", PROBLEM_JSON_CONTENT_TYPE);
    return response;
  });

  return app;
}
