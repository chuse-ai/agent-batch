import type { Server } from "node:http";

import { serve } from "@hono/node-server";
import crossws from "crossws/adapters/node";

import type { RunnerId } from "../db/types.ts";
import type { RunnerSockets } from "./runner-sockets.ts";

/**
 * Serves `fetch`, the app's handler, on `port`, and opens Runners' WebSockets as `decideUpgrade`
 * decides.
 *
 * WebSockets are opened with crossws, whose upgrade hook either accepts a socket or answers with the
 * hub's refusal, body included: a Runner stops only on the body of the hub's refusal
 * (`RUNNER_NOT_FOUND`), which Hono's own Node helper leaves out.
 */
export async function startServer(
  fetch: (request: Request) => Response | Promise<Response>,
  decideUpgrade: (request: Request) => Promise<RunnerId | Response>,
  sockets: RunnerSockets,
  port: number,
): Promise<Server> {
  const websockets = crossws({
    // The hub watches Runners by their own heartbeats (RunnerSockets), not crossws's protocol pings,
    // which `ws` would answer for a Runner that has stopped sending them.
    idleTimeout: 0,
    hooks: {
      upgrade: async (request) => {
        const runner = await decideUpgrade(request);
        return runner instanceof Response ? runner : { context: { runnerId: runner } };
      },
      open: (peer) => sockets.open(peer),
      message: (peer, message) => sockets.message(peer, message),
      close: (peer) => sockets.close(peer),
    },
  });
  let server!: Server;
  const listening = new Promise<void>((resolve) => {
    server = serve({ fetch, port }, () => resolve()) as Server;
  });
  server.on("upgrade", (request, socket, head) => {
    void websockets.handleUpgrade(request, socket, head);
  });
  await listening;
  console.info(`The hub is listening on port ${port}`);
  return server;
}

/** Stops `server`, once every connection it holds has closed. */
export function stopServer(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
    server.closeIdleConnections();
  });
}
