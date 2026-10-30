import path from "node:path";

import * as NodeRuntime from "@effect/platform-node-shared/NodeRuntime";
import { Config, Effect, Layer, Option, Redacted, Schedule } from "effect";
import ensureError from "ensure-error";

import { createApp, decideRunnerUpgrade } from "./app.ts";
import { sweepUnusedContent } from "./blobs.ts";
import { migrateDatabase } from "./db/migrate.ts";
import { NodeRunnerSockets, nodeServices } from "./node/layers.ts";
import { startServer, stopServer } from "./node/server.ts";
import type { HubServices } from "./platform.ts";
import { hubSettings, settingsProblems } from "./settings.ts";
import { setUpOrganization } from "./setup.ts";

// The hub as a Node process, which its Docker image runs (ADR 0014). It runs as a single process:
// two would split Runners' connections between them. On start it migrates the database and sets up
// the Organization and its owner from `ADMIN_EMAIL` and `ADMIN_PASSWORD` unless it exists (ADR 0015),
// then serves until `SIGINT` or `SIGTERM`. It runs as Effect's example of an application entry point
// does, a layer started with `Layer.launch` under `NodeRuntime.runMain`, which the Runner's entry
// point uses too: a signal interrupts it, and the server stops before the services it runs on.

/** The Node hub's settings, read from the environment by Effect's default `Config` source. */
const nodeSettings = Config.all({
  hub: hubSettings,
  databaseUrl: Config.Redacted("DATABASE_URL"),
  port: Config.Port("PORT").pipe(Config.withDefault(8787)),
  adminEmail: Config.String("ADMIN_EMAIL").pipe(Config.option),
  adminPassword: Config.Redacted("ADMIN_PASSWORD").pipe(Config.option),
});

/** Tells the operator why the hub cannot start, and ends it with a failing exit code. */
function refuse(message: string) {
  return Effect.sync(() => {
    console.error(message);
    process.exitCode = 1;
  });
}

/**
 * The hub's server on `port`, running each request on the services it is given. Stopping drains
 * before it releases: the server stops before those services, so Runners' sockets close and the
 * server finishes its requests first.
 */
function hubServer(port: number) {
  return Layer.effectDiscard(
    Effect.gen(function* () {
      const run = Effect.runPromiseWith(yield* Effect.context<HubServices>());
      const app = createApp<object>({
        run: (_env, effect) => run(effect),
        acceptRunner: () => {
          // crossws takes every upgrade in the server's `upgrade` event before the app sees the
          // request, so only requests the decision refuses reach the app's Runner route here.
          throw new Error("The Node hub accepts Runners' sockets in its upgrade hook");
        },
      });
      const sockets = yield* NodeRunnerSockets;
      yield* Effect.acquireRelease(
        Effect.promise(() =>
          startServer(app.fetch, (request) => run(decideRunnerUpgrade(request)), sockets, port),
        ),
        (server) =>
          Effect.promise(() => {
            sockets.closeAll();
            return stopServer(server);
          }),
      );
    }),
  );
}

/**
 * The sweep of content nothing uses, at start and every hour after, as the Workers hub's Cron
 * Trigger runs it there; a sweep that fails is logged, and the next one tries again.
 */
const sweeper = Layer.effectDiscard(
  Effect.forkScoped(
    sweepUnusedContent.pipe(
      Effect.catchCause(Effect.logError),
      Effect.repeat(Schedule.spaced("1 hour")),
    ),
  ),
);

Effect.gen(function* () {
  const settings = yield* nodeSettings;
  const databaseUrl = Redacted.value(settings.databaseUrl);
  yield* Effect.tryPromise({
    try: async () => {
      // Migrations sit beside `src` and `dist` alike, so the path holds before and after building.
      await migrateDatabase(databaseUrl, path.join(import.meta.dirname, "../migrations"));
      await setUpOrganization(databaseUrl, {
        email: Option.getOrUndefined(settings.adminEmail),
        password: Option.getOrUndefined(Option.map(settings.adminPassword, Redacted.value)),
      });
    },
    catch: ensureError,
  });
  return yield* Layer.launch(
    Layer.mergeAll(hubServer(settings.port), sweeper).pipe(
      Layer.provide(nodeServices(settings.hub, databaseUrl)),
    ),
  );
}).pipe(
  Effect.catchTag("ConfigError", (error) =>
    refuse(`The hub is not configured: ${settingsProblems(error)}`),
  ),
  Effect.catch((error) => refuse(error.message)),
  NodeRuntime.runMain,
);
