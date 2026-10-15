import type { Runner } from "contract";
import { sql } from "kysely";
import { expect, inject } from "vite-plus/test";
import { startRunner, type Runner as RunnerProcess } from "runner";
import { createTestHarness } from "wrangler";

import { DB_SCHEMA, withDatabase } from "../src/db/database.ts";

/** Deletes all rows from the hub's tables, keeping the schema and the record of applied migrations. */
async function clearDatabase(connectionString: string) {
  await withDatabase(connectionString, async (db) => {
    const tables = (await db.introspection.getTables()).filter(
      (table) => table.schema === DB_SCHEMA,
    );
    if (tables.length === 0) return;
    await sql`truncate ${sql.join(tables.map((table) => sql.table(`${table.schema}.${table.name}`)))}`.execute(
      db,
    );
  });
}

/**
 * Starts the hub in workerd, against the run's Postgres, and lets tests start the Runner `runner-1`
 * beside it as a Node Runner on a host would run: dialling the hub's URL over a real WebSocket.
 */
export function startSystem(
  options: { hubVars?: Record<string, string>; heartbeatIntervalMs?: number } = {},
) {
  const server = createTestHarness({
    workers: [{ configPath: "./wrangler.jsonc", vars: options.hubVars }],
  });
  const hub = server.getWorker<Env>("hub");
  let runner: RunnerProcess | undefined;

  async function stopRunner() {
    await runner?.stop();
    runner = undefined;
  }

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
    async start() {
      const databaseUrl = inject("databaseUrl");
      await clearDatabase(databaseUrl);
      process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = databaseUrl;
      await server.listen();
    },
    async stop() {
      await stopRunner();
      await server.close();
    },
    /** Starts the Runner `runner-1`, connecting to the hub's URL. */
    async startRunner() {
      const { url } = await server.listen();
      runner = startRunner({
        hubUrl: url,
        runnerId: "runner-1",
        heartbeatIntervalMs: options.heartbeatIntervalMs,
      });
    },
    /** Stops the Runner `runner-1`, closing its connection. */
    stopRunner,
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
