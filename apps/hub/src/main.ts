import path from "node:path";

import { Config, Effect, ManagedRuntime, Option, Redacted } from "effect";
import ensureError from "ensure-error";

import { createApp, decideRunnerUpgrade } from "./app.ts";
import { migrateDatabase } from "./db/migrate.ts";
import { NodeRunnerSockets, nodeServices } from "./node/layers.ts";
import { startServer, stopServer } from "./node/server.ts";
import { hubSettings, readConfig } from "./settings.ts";
import { setUpOrganization } from "./setup.ts";

// The hub as a Node process, which its Docker image runs (ADR 0014). It runs as a single process:
// two would split Runners' connections between them. On start it migrates the database and sets up
// the Organization and its owner from `ADMIN_EMAIL` and `ADMIN_PASSWORD` unless it exists (ADR 0015),
// then serves until `SIGINT` or `SIGTERM`, as Effect's example of `ManagedRuntime` with Hono does.

/** The Node hub's settings: the hub's own, its database and port, and the owner to set up. */
const nodeSettings = Config.all({
  hub: hubSettings,
  databaseUrl: Config.Redacted("DATABASE_URL"),
  port: Config.Port("PORT").pipe(Config.withDefault(8787)),
  adminEmail: Config.String("ADMIN_EMAIL").pipe(Config.option),
  adminPassword: Config.Redacted("ADMIN_PASSWORD").pipe(Config.option),
});

let settings: Config.Success<typeof nodeSettings>;
try {
  settings = readConfig(nodeSettings, process.env);
} catch (error) {
  console.error(ensureError(error).message);
  process.exit(1);
}
const databaseUrl = Redacted.value(settings.databaseUrl);

try {
  // Migrations sit beside `src` and `dist` alike, so the path holds before and after building.
  await migrateDatabase(databaseUrl, path.join(import.meta.dirname, "../migrations"));
  await setUpOrganization(databaseUrl, {
    email: Option.getOrUndefined(settings.adminEmail),
    password: Option.getOrUndefined(Option.map(settings.adminPassword, Redacted.value)),
  });
} catch (error) {
  console.error(ensureError(error).message);
  process.exit(1);
}

const runtime = ManagedRuntime.make(nodeServices(settings.hub, databaseUrl));
const app = createApp<object>({
  run: (_env, effect) => runtime.runPromise(effect),
  acceptRunner: () => {
    // crossws takes every upgrade in the server's `upgrade` event before the app sees the request,
    // so only requests the decision refuses reach the app's Runner route here.
    throw new Error("The Node hub accepts Runners' sockets in its upgrade hook");
  },
});
const sockets = await runtime.runPromise(NodeRunnerSockets.use(Effect.succeed));
const server = await startServer(
  app.fetch,
  (request) => runtime.runPromise(decideRunnerUpgrade(request)),
  sockets,
  settings.port,
);

// Stopping drains before it releases: disposing the runtime interrupts any request still running on
// it, so Runners' sockets close and the server finishes its requests first.
const shutdown = async () => {
  sockets.closeAll();
  await stopServer(server);
  await runtime.dispose();
};
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
