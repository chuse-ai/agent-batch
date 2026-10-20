import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { text } from "node:stream/consumers";

import { createORPCClient } from "@orpc/client";
import type { ContractRouterClient } from "@orpc/contract";
import {
  contract,
  createProblemDetailsLink,
  RUNNER_CONNECT_PATH,
  RUNNER_PROTOCOL_VERSION,
  registeredRunnerSchema,
} from "contract";
import getPort from "get-port";
import { sql } from "kysely";
import { startRunner, type Runner as RunnerProcess } from "runner";
import { expect, inject, vi } from "vite-plus/test";
import { createTestHarness } from "wrangler";
import WebSocket from "ws";
import * as z from "zod/mini";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { setUpOrganization } from "../src/setup.ts";
import { scriptedModel } from "./scripted-model.ts";

/** The hub's API as a caller sees it through the contract's link. */
export type HubClient = ContractRouterClient<typeof contract>;

/** Which hub the suite runs against: the Worker in workerd, or the Node process its image runs. */
export type HubTarget = "workers" | "node";

declare module "vite-plus/test" {
  interface ProvidedContext {
    hub: HubTarget;
  }
}

/**
 * The origin the hub is told it serves. Better Auth signs members in only from its own origin once a
 * request carries browser fetch metadata, which Node's `fetch` sends; sign-in sends it as `Origin`,
 * as the dashboard on the hub's origin will.
 */
const HUB_ORIGIN = "http://hub.test";

/** The owner the hub is set up with. */
export const OWNER = { email: "owner@example.com", password: "correct horse battery staple" };

/** Settings every test hub gets, beside each test file's own. */
const HUB_SETTINGS = {
  BETTER_AUTH_SECRET: "a-secret-only-for-tests-0123456789abcdef",
  HUB_URL: HUB_ORIGIN,
  // Every test request comes from the same address, so Better Auth's limits would trip.
  AUTH_RATE_LIMIT: "false",
};

/** Deletes all rows from the hub's tables, keeping the schema and the record of applied migrations. */
export async function clearDatabase(connectionString: string) {
  await using db = openDatabase(connectionString);
  const tables = (await db.introspection.getTables()).filter((table) => table.schema === DB_SCHEMA);
  if (tables.length === 0) return;
  await sql`truncate ${sql.join(tables.map((table) => sql.table(`${table.schema}.${table.name}`)))}`.execute(
    db,
  );
}

/** A running hub, as the suite reaches it: over HTTP at `url`. */
interface RunningHub {
  url: URL;
  stop(): Promise<void>;
}

/** Starts the hub Worker from its Wrangler config in workerd, with Wrangler's test harness. */
async function startWorkersHub(databaseUrl: string, settings: Record<string, string>) {
  const server = createTestHarness({
    workers: [{ configPath: "./wrangler.jsonc", vars: settings }],
  });
  process.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE = databaseUrl;
  const { url } = await server.listen();
  return { url: new URL(url), stop: () => server.close() } satisfies RunningHub;
}

/**
 * Starts the hub as a Node process, as its Docker image runs it, on a free port. It migrates the
 * database and sets up the Organization and its owner as it starts, then answers requests.
 */
async function startNodeHub(databaseUrl: string, settings: Record<string, string>) {
  const port = await getPort();
  const hub: ChildProcess = spawn(process.execPath, ["src/main.ts"], {
    env: {
      ...process.env,
      ...settings,
      DATABASE_URL: databaseUrl,
      PORT: String(port),
      ADMIN_EMAIL: OWNER.email,
      ADMIN_PASSWORD: OWNER.password,
    },
    stdio: "inherit",
  });
  const exited = once(hub, "exit");
  const url = new URL(`http://localhost:${port}`);
  // The hub answers once it has migrated and set up; until then, connecting fails.
  await vi.waitFor(
    async () => {
      if (hub.exitCode !== null) throw new Error(`The Node hub exited with ${hub.exitCode}`);
      await fetch(url);
    },
    { timeout: 30_000, interval: 100 },
  );
  return {
    url,
    async stop() {
      if (hub.exitCode !== null) return;
      hub.kill("SIGTERM");
      await exited;
    },
  } satisfies RunningHub;
}

/**
 * Starts the hub this test project runs against, against the run's Postgres and set up with `OWNER`,
 * and lets tests start a Runner beside it as a Node Runner on a host would run: registering with a
 * shared key, then dialling the hub's URL over a real WebSocket with its own key.
 */
export function startSystem(
  options: { hubVars?: Record<string, string>; heartbeatIntervalMs?: number } = {},
) {
  let running: RunningHub;
  let url: URL;
  let owner: HubClient;
  let ownerCookie: string;
  let runner: RunnerProcess | undefined;
  let runnerDataDir: string | undefined;
  const scripted = scriptedModel();

  /** Sends a request to the hub over HTTP, for a path on it. */
  function hubFetch(pathname: string, init?: RequestInit): Promise<Response> {
    return fetch(new URL(pathname, url), init);
  }

  /** A client of the hub's API sending `headers`, such as a member's session or an API key. */
  function client(headers: Record<string, string> = {}): HubClient {
    return createORPCClient(createProblemDetailsLink(contract, { origin: url.origin, headers }));
  }

  /** Signs in with `credentials`, returning the session cookie a member's requests carry. */
  async function signIn(credentials: { email: string; password: string }): Promise<string> {
    const response = await hubFetch("/api/auth/sign-in/email", {
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

  /**
   * Opens a Runner's WebSocket as a Runner does, sending `key` if given and offering `versions` of
   * the protocol: the open socket, or the hub's response refusing it.
   */
  function openSocket(
    key?: string,
    versions: string[] = [RUNNER_PROTOCOL_VERSION],
  ): Promise<WebSocket | Response> {
    const socket = new WebSocket(new URL(RUNNER_CONNECT_PATH, url).href, versions, {
      headers: key ? { "x-api-key": key } : {},
    });
    return new Promise((resolve, reject) => {
      socket.once("open", () => resolve(socket));
      socket.once("unexpected-response", (_request, response) => {
        void text(response).then((body) => {
          socket.terminate();
          const headers = new Headers();
          for (const [name, value] of Object.entries(response.headers)) {
            if (typeof value === "string") headers.set(name, value);
          }
          resolve(new Response(body || null, { status: response.statusCode, headers }));
        }, reject);
      });
      socket.once("error", reject);
    });
  }

  /**
   * Asks to open a Runner's WebSocket, sending `key` if given and offering `versions`, and returns
   * the hub's answer.
   */
  async function connect(key?: string, versions?: string[]): Promise<Response> {
    const answer = await openSocket(key, versions);
    if (answer instanceof Response) return answer;
    answer.close(1000);
    return new Response(null, { status: 101 });
  }

  async function stopRunner() {
    await runner?.stop();
    runner = undefined;
  }

  async function findRunner(id: string) {
    return (await owner.runners.list()).find((listed) => listed.id === id);
  }

  return {
    hub: { fetch: hubFetch },
    /** The hub's URL, which a Runner dials. */
    url: () => url,
    /** The origin the hub serves, which Better Auth requires on sign-in from Node. */
    origin: HUB_ORIGIN,
    client,
    signIn,
    /** A client signed in as `OWNER`. */
    owner: () => owner,
    /** Creates an API key allowed to submit and read jobs, and returns a client sending it. */
    async clientWithNewJobsKey() {
      const { key } = await owner.apiKeys.create({
        name: "jobs",
        permissions: { job: ["create", "read"] },
      });
      return client({ "x-api-key": key });
    },
    /** The session cookie of `OWNER`. */
    ownerCookie: () => ownerCookie,
    async start() {
      const databaseUrl = inject("databaseUrl");
      await clearDatabase(databaseUrl);
      const settings = { ...HUB_SETTINGS, ...options.hubVars };
      if (inject("hub") === "node") {
        running = await startNodeHub(databaseUrl, settings);
      } else {
        await setUpOrganization(databaseUrl, OWNER, () => {});
        running = await startWorkersHub(databaseUrl, settings);
      }
      url = running.url;
      ownerCookie = await signIn(OWNER);
      owner = client({ cookie: ownerCookie });
    },
    async stop() {
      // Both at once: a Runner that does not stop then fails the hook without keeping the hub's
      // process, and with it the test run, alive.
      await Promise.all([stopRunner(), running.stop()]);
    },
    createRegistrationKey,
    connect,
    /**
     * Starts the Runner; it registers on its first start and keeps its identity across restarts,
     * since its data directory stays the same. Returns the Runner's ID.
     */
    async startRunner(
      runnerOptions: { register?: boolean; onRemoved?: () => void; maxJobs?: number } = {},
    ): Promise<string> {
      runnerDataDir ??= await mkdtemp(path.join(tmpdir(), "runner-"));
      runner = await startRunner({
        hubUrl: url,
        registrationKey: await createRegistrationKey(),
        dataDir: runnerDataDir,
        models: scripted.models,
        model: scripted.model,
        heartbeatIntervalMs: options.heartbeatIntervalMs,
        ...runnerOptions,
      });
      const registration = z.parse(
        registeredRunnerSchema,
        JSON.parse(await readFile(path.join(runnerDataDir, "runner.json"), "utf8")),
      );
      return registration.id;
    },
    /** Stops the Runner, closing its connection. */
    stopRunner,
    /** The scripted model the Runner's jobs run on. */
    model: scripted.faux,
    /**
     * Registers a Runner and opens its socket to the hub directly with its own key, without a Runner's
     * reconnects and heartbeats.
     */
    async connectRunner() {
      const registration = client({ "x-api-key": await createRegistrationKey() });
      const { id: runnerId, key } = await registration.runners.register({ name: "test runner" });
      const socket = await openSocket(key);
      if (socket instanceof Response)
        throw new Error(`Hub refused the WebSocket: ${socket.status}`);
      return { runnerId, key, socket };
    },
    /** The hub's listing of one Runner, or `undefined` if it lists none by that ID. */
    findRunner,
    /** When the hub last saw the Runner, in milliseconds since the epoch. */
    lastSeen: async (id: string) => {
      const listed = await findRunner(id);
      if (!listed) throw new Error(`The hub lists no runner ${id}`);
      return listed.lastSeenAt.getTime();
    },
  };
}
