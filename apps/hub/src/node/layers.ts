import { Clock, Context, Effect, FiberMap, Layer } from "effect";

import { createAuth } from "../auth/auth.ts";
import { DB_SCHEMA, openDatabase } from "../db/database.ts";
import type { RunnerId } from "../db/types.ts";
import { DatabaseConnection, RunnerConnections } from "../platform.ts";
import { authSettings, type HubSettings } from "../settings.ts";
import { RunnerSockets } from "./runner-sockets.ts";

// The hub in Docker: one process, with one database pool for every request and every Runner's
// WebSocket. Each is acquired in a Layer and released when the runtime is disposed.

/** The process's Runner sockets, which only the Node hub's server reaches. */
export class NodeRunnerSockets extends Context.Service<NodeRunnerSockets, RunnerSockets>()(
  "hub/NodeRunnerSockets",
) {}

/** The Node platform's services: one database pool, and the process's Runner sockets. */
export function nodeServices(settings: HubSettings, connectionString: string) {
  const database = Layer.effect(
    DatabaseConnection,
    Effect.gen(function* () {
      const db = yield* Effect.acquireRelease(
        Effect.sync(() => openDatabase(connectionString)),
        (db) => Effect.promise(() => db.destroy()),
      );
      // One Better Auth instance serves every request: unlike on Workers, connections and promises
      // may outlive the request that made them.
      const auth = createAuth(db, authSettings(settings));
      return {
        withAuth: (work) =>
          Effect.tryPromise({ try: () => work(auth, db.withSchema(DB_SCHEMA)), catch: (e) => e }),
      };
    }),
  );
  const sockets = Layer.effect(
    NodeRunnerSockets,
    Effect.gen(function* () {
      const database = yield* DatabaseConnection;
      const clock = yield* Clock.Clock;
      // Each Runner's watching fiber, interrupted when the runtime is disposed.
      const watches = yield* FiberMap.make<RunnerId>();
      const watch = yield* FiberMap.runtime(watches)();
      return yield* Effect.acquireRelease(
        Effect.sync(() => new RunnerSockets(watch, database, clock, settings.heartbeatTimeoutMs)),
        (sockets) => Effect.sync(() => sockets.closeAll()),
      );
    }),
  ).pipe(Layer.provide(database));
  const connections = Layer.effect(
    RunnerConnections,
    NodeRunnerSockets.use((sockets) =>
      Effect.succeed({ drop: (runnerId) => Effect.sync(() => sockets.drop(runnerId)) }),
    ),
  ).pipe(Layer.provide(sockets));
  return Layer.mergeAll(database, sockets, connections);
}
