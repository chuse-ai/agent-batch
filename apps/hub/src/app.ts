import { catchORPCErrorCode, type WithEffectContext } from "@orpc/experimental-effect";
import "@orpc/experimental-effect/extensions/effect";
import { SmartCoercionHandlerPlugin } from "@orpc/json-schema";
import { OpenAPIHandler } from "@orpc/openapi/fetch";
import { implement, ValidationError } from "@orpc/server";
import { ZodToJsonSchemaConverter } from "@orpc/zod";
import {
  contract,
  PROTOCOL_UNSUPPORTED,
  RUNNER_CONNECT_PATH,
  RUNNER_NOT_FOUND,
  RUNNER_PROTOCOL_VERSIONS,
  selectProtocolVersion,
} from "contract";
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
import { ApikeyId, EventId, JobId, RunnerId } from "./db/types.ts";
import {
  type Control,
  controlJob,
  deleteJob,
  getJob,
  resultContractProblems,
  readJobEvents,
  submitJob,
  submitterOf,
} from "./jobs.ts";
import { RunnerConnections, type HubServices } from "./platform.ts";
import { toProblemDetails } from "./problem-details.ts";
import { findRunnerByKey, registerRunner, removeRunner } from "./runner-registration.ts";
import { hintLiveRunners, listRunners } from "./runners.ts";

interface ApiContext extends WithEffectContext<HubServices> {
  headers: Headers;
}

const os = implement<typeof contract, ApiContext>(contract);

/**
 * Tells the Runners a change to a job concerns to sync now: one Runner, or every live one for a job
 * any Runner may take. They learn of it at their next sync anyway; the hint only makes it sooner, so
 * a failed one is only logged.
 */
function hintRunners(hint: RunnerId | "live" | undefined) {
  if (hint === undefined) return Effect.void;
  return (
    hint === "live"
      ? hintLiveRunners
      : RunnerConnections.use((connections) => connections.hint(hint))
  ).pipe(
    Effect.catchCause((cause) =>
      Effect.sync(() => console.error("Hinting Runners to sync failed", cause)),
    ),
  );
}

/** Applies `control` to the job `jobId` and tells the Runners it concerns to sync now. */
function controlJobAndHint(jobId: JobId, control: Control) {
  return Effect.gen(function* () {
    const { job, hint } = yield* controlJob(jobId, control);
    yield* hintRunners(hint);
    return job;
  });
}

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
      const runnerId = RunnerId(input.id);
      // Its steps run to the end once begun, also when the caller goes away and the Node hub
      // interrupts the request, so a removal leaves no Runner gone with its jobs still running.
      return yield* Effect.uninterruptible(
        removeRunner(context.headers, runnerId).pipe(
          Effect.tap(() => RunnerConnections.use((connections) => connections.drop(runnerId))),
        ),
      );
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
      const problems = resultContractProblems(input.resultContract.schema);
      if (problems.length > 0) {
        // Raised as oRPC raises invalid input, so the answer lists the problems the same way.
        const path = ["resultContract", "schema"];
        const cause = new ValidationError({
          message: "Input validation failed",
          issues: problems.map((message) => ({ message, path })),
          invalidData: input,
        });
        return yield* Effect.fail(errors.BAD_REQUEST({ cause }));
      }
      const job = yield* submitJob(input, submitterOf(key));
      yield* hintRunners("live");
      return job;
    }),
    get: os.jobs.get.effect(function* ({ input, context }) {
      yield* requireKeyOrMember(context.headers, { job: ["read"] });
      // The ID enters the hub here.
      return yield* getJob(JobId(input.id));
    }),
    pause: os.jobs.pause.effect(function* ({ input, context }) {
      yield* requireKeyOrMember(context.headers, { job: ["pause"] });
      // The ID enters the hub here.
      return yield* controlJobAndHint(JobId(input.id), "pause");
    }),
    resume: os.jobs.resume.effect(function* ({ input, context }) {
      yield* requireKeyOrMember(context.headers, { job: ["resume"] });
      // The ID enters the hub here.
      return yield* controlJobAndHint(JobId(input.id), "resume");
    }),
    cancel: os.jobs.cancel.effect(function* ({ input, context }) {
      yield* requireKeyOrMember(context.headers, { job: ["cancel"] });
      // The ID enters the hub here.
      return yield* controlJobAndHint(JobId(input.id), "cancel");
    }),
    delete: os.jobs.delete.effect(function* ({ input, context }) {
      yield* requireKeyOrMember(context.headers, { job: ["delete"] });
      // The ID enters the hub here.
      const { job, hint } = yield* deleteJob(JobId(input.id));
      yield* hintRunners(hint);
      return job;
    }),
    events: os.jobs.events.effect(function* ({ input, context }) {
      yield* requireKeyOrMember(context.headers, { job: ["read"] });
      // The IDs enter the hub here.
      return yield* readJobEvents(JobId(input.id), EventId(input.after), input.limit);
    }),
  },
});

const api = new OpenAPIHandler(router, {
  customErrorResponseBodyEncoder: toProblemDetails,
  // A query string's values arrive as text; this turns them into what the input's schema expects,
  // such as `after=42` into a number.
  plugins: [new SmartCoercionHandlerPlugin({ converters: [new ZodToJsonSchemaConverter()] })],
});

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

/**
 * A refusal of a Runner's connection: Problem Details (ADR 0023) with a `code` the Runner acts on,
 * and whatever else it needs, such as the versions the hub supports.
 */
function refuseRunner(
  status: number,
  detail: string,
  extensions: Record<string, unknown>,
  headers: Record<string, string> = {},
): Response {
  const response = problemDetails({ status, detail, extensions }).getResponse();
  return new Response(response.body, {
    status,
    headers: { ...headers, ...Object.fromEntries(response.headers) },
  });
}

/** The hub's answer to a Runner whose key it does not know, on which the Runner stops. */
function runnerNotFound(): Response {
  return refuseRunner(
    401,
    "The hub does not know this Runner's key",
    { code: RUNNER_NOT_FOUND },
    { "www-authenticate": AUTH_CHALLENGE },
  );
}

/** The hub's answer to a Runner offering no protocol version the hub supports (ADR 0026). */
function protocolUnsupported(): Response {
  return refuseRunner(
    400,
    `The hub supports none of the protocol versions offered; it supports ${RUNNER_PROTOCOL_VERSIONS.join(", ")}`,
    { code: PROTOCOL_UNSUPPORTED, supported: RUNNER_PROTOCOL_VERSIONS },
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
  // The version first, before any database is reached or a Durable Object wakes.
  if (selectProtocolVersion(request.headers.get("sec-websocket-protocol")) === undefined) {
    return Effect.succeed(protocolUnsupported());
  }
  return authenticateRunner(request.headers);
}

/** The Runner whose own key `headers` carry, or the hub's answer refusing it. */
function authenticateRunner(headers: Headers) {
  return Effect.gen(function* () {
    const key = yield* requireKey(headers, { runner: ["connect"] });
    return (yield* findRunnerByKey(ApikeyId(key.id))) ?? runnerNotFound();
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
