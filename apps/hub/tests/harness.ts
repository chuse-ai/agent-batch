import type { Runner } from "contract";
import { expect, inject } from "vite-plus/test";
import { createTestHarness } from "wrangler";

import { clearDatabase } from "../src/db/migrate.ts";

/** Starts the hub and one runner in workerd, against the run's Postgres. */
export function startSystem(
  options: { hubVars?: Record<string, string>; runnerVars?: Record<string, string> } = {},
) {
  const server = createTestHarness({
    workers: [
      { configPath: "./wrangler.jsonc", vars: options.hubVars },
      {
        configPath: "../runner/wrangler.jsonc",
        vars: { RUNNER_ID: "runner-1", RUNNER_NAME: "test runner", ...options.runnerVars },
      },
    ],
  });
  const hub = server.getWorker<Env>("hub");
  const runner = server.getWorker("runner");

  async function listRunners(): Promise<Runner[]> {
    const response = await hub.fetch("/runners");
    expect(response.status).toBe(200);
    return (await response.json()) as Runner[];
  }

  async function findRunner(id: string): Promise<Runner | undefined> {
    return (await listRunners()).find((runner) => runner.id === id);
  }

  return {
    hub,
    runner,
    async start() {
      const databaseUrl = inject("databaseUrl");
      await clearDatabase(databaseUrl);
      process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = databaseUrl;
      await server.listen();
    },
    stop: () => server.close(),
    listRunners,
    /** The hub's listing of one Runner, or `undefined` if it lists none by that ID. */
    findRunner,
    /** When the hub last saw the Runner, in milliseconds since the epoch. */
    lastSeen: async (id: string) => {
      const runner = await findRunner(id);
      if (!runner) throw new Error(`The hub lists no runner ${id}`);
      return Date.parse(runner.lastSeenAt);
    },
  };
}
