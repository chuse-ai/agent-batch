import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { createORPCClient } from "@orpc/client";
import type { ContractRouterClient } from "@orpc/contract";
import type { JsonifiedClient } from "@orpc/openapi";
import { OpenAPILink } from "@orpc/openapi/fetch";
import { contract, RUNNER_CONNECT_PATH, type RegisteredRunner } from "contract";
import { sql } from "kysely";
import { startRunner, type Runner as RunnerProcess } from "runner";
import { expect, inject } from "vite-plus/test";
import { createTestHarness } from "wrangler";

import { DB_SCHEMA, withDatabase } from "../src/db/database.ts";
import { setUpOrganization } from "../src/setup.ts";

/** The hub's API as a caller sees it over JSON, with dates as ISO strings. */
export type HubClient = JsonifiedClient<ContractRouterClient<typeof contract>>;

/**
 * The origin the hub is told it serves. Better Auth signs members in only from its own origin once a
 * request carries browser fetch metadata, which Node's `fetch` sends; sign-in sends it as `Origin`,
 * as the dashboard on the hub's origin will.
 */
const HUB_ORIGIN = "http://hub.test";

/** The owner the hub is set up with. */
export const OWNER = { email: "owner@example.com", password: "correct horse battery staple" };

/** Deletes all rows from the hub's tables, keeping the schema and the record of applied migrations. */
export async function clearDatabase(connectionString: string) {
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
 * Starts the hub in workerd, against the run's Postgres and set up with `OWNER`, and lets tests start
 * a Runner beside it as a Node Runner on a host would run: registering with a shared key, then
 * dialling the hub's URL over a real WebSocket with its own key.
 */
export function startSystem(
  options: { hubVars?: Record<string, string>; heartbeatIntervalMs?: number } = {},
) {
  const server = createTestHarness({
    workers: [
      {
        configPath: "./wrangler.jsonc",
        vars: {
          BETTER_AUTH_SECRET: "a-secret-only-for-tests-0123456789abcdef",
          HUB_URL: HUB_ORIGIN,
          // Every test request comes from the same address, so Better Auth's limits would trip.
          AUTH_RATE_LIMIT: "false",
          ...options.hubVars,
        },
      },
    ],
  });
  const hub = server.getWorker<Env>("hub");
  let url: URL;
  let owner: HubClient;
  let ownerCookie: string;
  let runner: RunnerProcess | undefined;
  let runnerDataDir: string | undefined;

  /** A client of the hub's API sending `headers`, such as a member's session or an API key. */
  function client(headers: Record<string, string> = {}): HubClient {
    return createORPCClient(new OpenAPILink(contract, { origin: url.origin, headers }));
  }

  /** Signs in with `credentials`, returning the session cookie a member's requests carry. */
  async function signIn(credentials: { email: string; password: string }): Promise<string> {
    const response = await hub.fetch("/api/auth/sign-in/email", {
      method: "POST",
      headers: { "content-type": "application/json", origin: HUB_ORIGIN },
      body: JSON.stringify(credentials),
    });
    expect(response.status).toBe(200);
    const cookie = response.headers.get("set-cookie");
    if (!cookie) throw new Error("Signing in set no session cookie");
    return cookie.split(";")[0] ?? "";
  }

  /** Creates the key Runners share to register, as the owner. */
  async function createRegistrationKey(): Promise<string> {
    const { key } = await owner.apiKeys.create({
      name: "Runners",
      permissions: { runner: ["register"] },
    });
    return key;
  }

  /** Asks to open a Runner's WebSocket, as a Runner does, sending `key` if given. */
  function connect(key?: string) {
    return hub.fetch(RUNNER_CONNECT_PATH, {
      headers: { Upgrade: "websocket", ...(key && { "x-api-key": key }) },
    });
  }

  async function stopRunner() {
    await runner?.stop();
    runner = undefined;
  }

  async function findRunner(id: string) {
    return (await owner.runners.list()).find((listed) => listed.id === id);
  }

  return {
    hub,
    /** The origin the hub serves, which Better Auth requires on sign-in from Node. */
    origin: HUB_ORIGIN,
    client,
    signIn,
    /** A client signed in as `OWNER`. */
    owner: () => owner,
    /** The session cookie of `OWNER`. */
    ownerCookie: () => ownerCookie,
    async start() {
      const databaseUrl = inject("databaseUrl");
      await clearDatabase(databaseUrl);
      await setUpOrganization(databaseUrl, OWNER, () => {});
      process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = databaseUrl;
      ({ url } = await server.listen());
      ownerCookie = await signIn(OWNER);
      owner = client({ cookie: ownerCookie });
    },
    async stop() {
      await stopRunner();
      await server.close();
    },
    createRegistrationKey,
    connect,
    /**
     * Starts the Runner; it registers on its first start and keeps its identity across restarts,
     * since its data directory stays the same. Returns the Runner's ID.
     */
    async startRunner(
      runnerOptions: { register?: boolean; onRemoved?: () => void } = {},
    ): Promise<string> {
      runnerDataDir ??= await mkdtemp(path.join(tmpdir(), "runner-"));
      runner = await startRunner({
        hubUrl: url,
        registrationKey: await createRegistrationKey(),
        dataDir: runnerDataDir,
        heartbeatIntervalMs: options.heartbeatIntervalMs,
        ...runnerOptions,
      });
      const registration = JSON.parse(
        await readFile(path.join(runnerDataDir, "runner.json"), "utf8"),
      ) as RegisteredRunner;
      return registration.id;
    },
    /** Stops the Runner, closing its connection. */
    stopRunner,
    /**
     * Registers a Runner and opens its socket to the hub directly with its own key, without a Runner's
     * reconnects and heartbeats.
     */
    async connectRunner() {
      const registration = client({ "x-api-key": await createRegistrationKey() });
      const { id: runnerId, key } = await registration.runners.register({ name: "test runner" });
      const response = await connect(key);
      const socket = response.webSocket;
      if (!socket) throw new Error(`Hub refused the WebSocket: ${response.status}`);
      socket.accept();
      return { runnerId, key, socket };
    },
    /** The hub's listing of one Runner, or `undefined` if it lists none by that ID. */
    findRunner,
    /** When the hub last saw the Runner, in milliseconds since the epoch. */
    lastSeen: async (id: string) => {
      const listed = await findRunner(id);
      if (!listed) throw new Error(`The hub lists no runner ${id}`);
      return Date.parse(listed.lastSeenAt);
    },
  };
}
