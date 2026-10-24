import { Effect, Layer } from "effect";
import ensureError from "ensure-error";
import { HTTPException } from "hono/http-exception";

import { createApp } from "./app.ts";
import { databaseAndAuthLayer } from "./auth/auth.ts";
import { RunnerConnections } from "./platform.ts";
import {
  authSettings,
  hubSettings,
  jobLimitSettingsLayer,
  readConfig,
  type HubSettings,
} from "./settings.ts";

export { RunnerConnection } from "./runner-connection.ts";

// The hub on Cloudflare Workers: each request opens its own database connection through Hyperdrive,
// and a Durable Object per Runner holds the Runner's WebSocket.

/** The Workers platform's services for one request. */
function workersLayer(env: Env) {
  let settings: HubSettings;
  try {
    settings = readConfig(hubSettings);
  } catch (error) {
    // Without them Better Auth would guess the URL from each request, which its docs warn against,
    // and sign sessions with a development secret; a hub missing either answers no request. Wrangler
    // refuses to deploy without them, so this catches a secret removed from a live hub, or a setting
    // Wrangler cannot check, such as a limit's default above its maximum: its callers learn only
    // that it is unavailable, and the operator finds which setting in the logs.
    console.error(ensureError(error).message);
    throw new HTTPException(503);
  }
  return Layer.mergeAll(
    databaseAndAuthLayer(env.HYPERDRIVE.connectionString, authSettings(settings)),
    jobLimitSettingsLayer(settings),
    Layer.succeed(RunnerConnections, {
      drop: (runnerId) =>
        Effect.tryPromise(() => env.RUNNER_CONNECTION.getByName(runnerId).dropConnections()),
      hint: (runnerId) =>
        Effect.tryPromise(() => env.RUNNER_CONNECTION.getByName(runnerId).requestSync()),
    }),
  );
}

export default createApp<Env>({
  run: (env, effect) => Effect.runPromise(Effect.provide(effect, workersLayer(env))),
  acceptRunner: (env, request, runnerId) =>
    env.RUNNER_CONNECTION.getByName(runnerId).fetch(request),
});
