import { Context, Effect, Layer } from "effect";

import { databaseAndAuthLayer } from "../auth/auth.ts";
import type { Db } from "../db/database.ts";
import { RunnerConnections } from "../platform.ts";
import { hintLiveRunners } from "../runners.ts";
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
  // One Better Auth instance serves every request: unlike on Workers, connections and promises may
  // outlive the request that made them.
  const databaseAndAuth = databaseAndAuthLayer(connectionString, authSettings(settings));
  const socketsLayer = Layer.effect(
    NodeRunnerSockets,
    Effect.gen(function* () {
      const services = yield* Effect.context<Db>();
      return yield* Effect.acquireRelease(
        Effect.sync(() => new RunnerSockets(services, settings.reportIntervalMs)),
        (opened) => Effect.sync(() => opened.closeAll()),
      );
    }),
  ).pipe(Layer.provide(databaseAndAuth));
  const connections = Layer.effect(
    RunnerConnections,
    NodeRunnerSockets.use((sockets) =>
      Effect.succeed({
        drop: (runnerId) => Effect.sync(() => sockets.drop(runnerId)),
        hintLive: hintLiveRunners(async (runnerId) => sockets.requestSync(runnerId)),
      }),
    ),
  ).pipe(Layer.provide(socketsLayer));
  return Layer.mergeAll(databaseAndAuth, socketsLayer, connections);
}
