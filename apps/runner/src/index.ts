import { mkdir, readFile, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import path from "node:path";
import { text } from "node:stream/consumers";

import type { Models } from "@earendil-works/pi-ai";
import { createORPCClient } from "@orpc/client";
import type { ContractRouterClient } from "@orpc/contract";
import {
  HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_PING,
  HEARTBEAT_PONG,
  HEARTBEAT_TIMEOUT_MS,
  contract,
  createProblemDetailsLink,
  createProtocolClient,
  hubNotifications,
  PROTOCOL_UNSUPPORTED,
  RUNNER_CONNECT_PATH,
  RUNNER_NOT_FOUND,
  RUNNER_PROTOCOL_VERSIONS,
  runnerMethods,
  type RegisteredRunner,
  registeredRunnerSchema,
  socketRefusalSchema,
  type SocketRefusal,
} from "contract";
import { Cause, Data, Effect, Exit, Fiber, Latch, Random, Schedule, Scope } from "effect";
import * as Socket from "effect/socket/Socket";
import ensureError from "ensure-error";
import WebSocket from "ws";
import * as z from "zod/mini";

import type { AgentModel } from "./agent.ts";
import { JobRunner } from "./jobs.ts";
import {
  type JobEnvironments,
  prepareJobEnvironments,
  type SandboxSettings,
  SandboxUnavailable,
} from "./sandbox.ts";

export { SandboxUnavailable };

// TODO: remove once `@types/ws` declares `closeTimeout`, added in `ws` 8.19.
declare module "ws" {
  interface ClientOptions {
    closeTimeout?: number;
  }
}

// Connection timings (ADR 0008). Opening a socket may take 4 s. The first attempt goes at once and
// each retry waits about 3 s, growing 1.3 times per attempt up to about 10 s; each wait varies by up
// to a fifth either way, as gRPC's connection backoff does, so Runners dropped together do not redial
// together. Only a connection that stays open for 5 s starts the delays over, so a hub that accepts
// and then drops Runners is not redialled at a fixed pace.
const CONNECT_TIMEOUT_MS = 4_000;
/** How long a closing socket waits for the hub's answer before dropping the connection itself. */
const CLOSE_TIMEOUT_MS = 4_000;
const RECONNECT_DELAY_MS = 3_000;
const RECONNECT_DELAY_GROWTH = 1.3;
const MAX_RECONNECT_DELAY_MS = 10_000;
const MIN_UPTIME_MS = 5_000;
/**
 * How long the hub may take to answer a request before the Runner treats the connection as dead. The
 * JSON-RPC library keeps this deadline rather than Effect: it cannot cancel a request, so only its
 * own timeout also forgets the request.
 */
const REQUEST_TIMEOUT_MS = 30_000;
/** How far either side of the sync interval a sync may fall, so Runners' syncs spread out. */
const SYNC_JITTER = 0.1;

export interface RunnerOptions {
  /** The hub's URL; the Runner opens its WebSocket there, over `wss:` for `https:`. */
  hubUrl: string | URL;
  /** The key Runners share to register; a Runner uses it once, to receive a key of its own. */
  registrationKey: string;
  /** Where the Runner keeps what must survive restarts, such as its own key and its jobs' files. */
  dataDir: string;
  /** The models the Runner's agents can run on, with their providers' keys. */
  models: Models;
  /** The model every job runs on. */
  model: AgentModel;
  /** How many jobs the Runner runs at once. */
  maxJobs?: number;
  /** How jobs' sandboxes are made; `DEFAULT_SANDBOX_SETTINGS` unless given. */
  sandbox?: SandboxSettings;
  /** How often to send a heartbeat; tests set it shorter. */
  heartbeatIntervalMs?: number;
  /** Discards the Runner's own key and registers again, as the `register` command does. */
  register?: boolean;
}

export interface StartRunnerOptions extends RunnerOptions {
  /**
   * Called when the hub answers that it does not know the Runner's own key, because the Runner was
   * removed or the hub was reset; the Runner has stopped by then, since retrying cannot succeed.
   */
  onRemoved?: () => void;
}

export interface Runner {
  /** Stops the Runner: closes its connection, so the hub drops it at once, and stops its jobs. */
  stop(): Promise<void>;
}

/**
 * How many jobs a Runner runs at once unless `RUNNER_MAX_JOBS` says otherwise. A low placeholder:
 * an agent loop mostly waits on a model, so a machine holds many, and the Runner is to derive its
 * allowance from the host's memory and a measured loop's footprint, admitting less under memory
 * pressure.
 */
const DEFAULT_MAX_JOBS = 8;

/**
 * Jobs' sandboxes unless the Runner is told otherwise: no folders readable beyond the system's, and
 * a configurable 1 GiB memory cap per job where the host supports it (ADR 0027).
 */
export const DEFAULT_SANDBOX_SETTINGS: SandboxSettings = {
  extraReadPaths: [],
  jobMemoryLimit: "1G",
  runWithoutSandbox: false,
};

/**
 * The Runner as a program: registers it on its first start, then runs it until it is interrupted, or
 * until it fails with `Removed` once the hub no longer knows it. Its jobs stop before it ends.
 */
export function runRunner(
  options: RunnerOptions,
): Effect.Effect<never, Removed | SandboxUnavailable> {
  const hubUrl = new URL(options.hubUrl);
  return Effect.gen(function* () {
    const environments = yield* prepareEnvironments(options);
    const registration = yield* Effect.promise(() => loadOrRegister(hubUrl, options));
    return yield* runRegistered(hubUrl, registration, options, environments);
  });
}

/** Starts a Runner that stays connected to the hub until stopped, registering it on its first start. */
export async function startRunner(options: StartRunnerOptions): Promise<Runner> {
  const hubUrl = new URL(options.hubUrl);
  const environments = await Effect.runPromise(prepareEnvironments(options));
  const registration = await loadOrRegister(hubUrl, options);
  const loop = Effect.runFork(
    runRegistered(hubUrl, registration, options, environments).pipe(
      Effect.catchTag("Removed", () => Effect.sync(() => options.onRemoved?.())),
    ),
  );
  return {
    async stop() {
      await Effect.runPromise(Fiber.interrupt(loop));
    },
  };
}

/**
 * Runs a registered Runner: its jobs, and the connection that keeps them in sync with the hub. The
 * jobs live in this scope rather than a connection's, so they outlive reconnects but not the Runner:
 * however it ends, removed, interrupted or by a defect, its jobs stop first.
 */
function runRegistered(
  hubUrl: URL,
  registration: RegisteredRunner,
  options: RunnerOptions,
  environments: JobEnvironments,
): Effect.Effect<never, Removed> {
  return Effect.scoped(
    Effect.gen(function* () {
      const syncNow = yield* Latch.make(false);
      const agent = {
        jobsDir: path.join(options.dataDir, "jobs"),
        environments,
        models: options.models,
        model: options.model,
      };
      const jobs = new JobRunner(
        options.maxJobs ?? DEFAULT_MAX_JOBS,
        agent,
        yield* Effect.scope,
        () => syncNow.openUnsafe(),
      );
      return yield* keepConnected({
        url: socketUrl(hubUrl),
        key: registration.key,
        heartbeatIntervalMs: options.heartbeatIntervalMs ?? HEARTBEAT_INTERVAL_MS,
        jobs,
        syncNow,
      });
    }),
  );
}

/**
 * Finds what the host offers jobs, and tells the operator what they run without: a sandbox, where
 * the operator allowed that, or a memory cap. Fails with `SandboxUnavailable` when jobs cannot run
 * in a sandbox and the operator has not allowed running without one.
 */
function prepareEnvironments(
  options: RunnerOptions,
): Effect.Effect<JobEnvironments, SandboxUnavailable> {
  return prepareJobEnvironments(options.sandbox ?? DEFAULT_SANDBOX_SETTINGS).pipe(
    Effect.tap((environments) =>
      Effect.sync(() => {
        if (environments.unsandboxedBecause) {
          console.error(
            `Jobs run without a sandbox, as allowed: ${environments.unsandboxedBecause}`,
          );
        }
        if (environments.uncappedBecause) {
          console.error(`Jobs run without a memory limit: ${environments.uncappedBecause}`);
        }
      }),
    ),
  );
}

/** The hub's WebSocket URL for Runners: the hub's URL, over `wss:` for `https:`. */
function socketUrl(hubUrl: URL): URL {
  const url = new URL(RUNNER_CONNECT_PATH, hubUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url;
}

/**
 * The Runner's own key, kept in its data directory, or, on its first start, a new one from
 * registering with the shared key, as GitHub Actions and Buildkite runners do.
 */
async function loadOrRegister(hubUrl: URL, options: RunnerOptions): Promise<RegisteredRunner> {
  const file = path.join(options.dataDir, "runner.json");
  if (!options.register) {
    try {
      return z.parse(registeredRunnerSchema, JSON.parse(await readFile(file, "utf8")));
    } catch (error) {
      const absent = error instanceof Error && "code" in error && error.code === "ENOENT";
      if (!absent) throw error;
    }
  }
  const hub: ContractRouterClient<typeof contract> = createORPCClient(
    createProblemDetailsLink(contract, {
      origin: hubUrl.origin,
      headers: { "x-api-key": options.registrationKey },
    }),
  );
  const registration = await hub.runners.register({ name: hostname() });
  await mkdir(options.dataDir, { recursive: true });
  // Only the operating-system user running the Runner may read its key.
  await writeFile(file, JSON.stringify(registration), { mode: 0o600 });
  return registration;
}

/** What a connection needs: where and as whom to connect, and the jobs it keeps in sync. */
interface ConnectionContext {
  url: URL;
  /** The Runner's own key, from which the hub takes its identity. */
  key: string;
  heartbeatIntervalMs: number;
  jobs: JobRunner;
  /** Opened when the hub asks for a sync or a job ends, so the next sync goes at once. */
  syncNow: Latch.Latch;
}

/** The hub no longer knows this Runner: it was removed, or the hub was reset. Nothing retries it. */
export class Removed extends Data.TaggedError("Removed") {}

/** A connection ended: the hub refused it, closed it, or stopped answering. */
class Dropped extends Data.TaggedError("Dropped")<{ reason: string }> {}

/** The hub's refusal of the socket, read before the socket is dropped. */
interface Refusal {
  status: number;
  body: SocketRefusal | undefined;
}

/**
 * Keeps the Runner connected to the hub and in sync with it (ADR 0026): on each new socket it says
 * hello, then syncs, and from then on syncs at the interval the hub gave it, when the hub hints, and
 * when a job ends, reporting what became of its jobs. Every answer is the desired state of its jobs,
 * which the job runner is brought to. A connection that drops is reopened with growing delays; one
 * that stayed open long enough starts the delays over (ADR 0022).
 */
function keepConnected(context: ConnectionContext): Effect.Effect<never, Removed> {
  const delays = Schedule.min([
    Schedule.exponential(RECONNECT_DELAY_MS, RECONNECT_DELAY_GROWTH),
    Schedule.spaced(MAX_RECONNECT_DELAY_MS),
  ]).pipe(Schedule.jittered);
  const attempt = Effect.gen(function* () {
    const opened = { at: Number.POSITIVE_INFINITY };
    const ended = yield* connectAndSync(context, opened).pipe(Effect.scoped, Effect.flip);
    if (ended instanceof Dropped) console.error(ended.reason);
    // A connection that lasted succeeds, ending the retries, so the delays start over.
    if (ended instanceof Removed || Date.now() - opened.at < MIN_UPTIME_MS) yield* ended;
  });
  const untilOneLasts = Effect.retry(attempt, {
    schedule: delays,
    while: (error) => error instanceof Dropped,
  });
  // After a connection that lasted, the next waits the first delay.
  const afterOneLasted = Schedule.spaced(RECONNECT_DELAY_MS).pipe(Schedule.jittered);
  return Effect.repeat(untilOneLasts, afterOneLasted).pipe(
    // Neither schedule ends, so a drop cannot get past them; one that did would be a bug.
    Effect.catchIf((error) => error instanceof Dropped, Effect.die),
    Effect.andThen(Effect.die(new Error("The Runner's reconnect schedule ended"))),
  );
}

/**
 * One connection: opens a socket to the hub, notes in `opened` when it opened, and runs on it the
 * heartbeat, the reading of what the hub sends, and the syncs, until one of them fails. Never
 * succeeds.
 */
function connectAndSync(
  context: ConnectionContext,
  opened: { at: number },
): Effect.Effect<never, Removed | Dropped, Scope.Scope> {
  const refusal: Refusal = { status: 0, body: undefined };
  return Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const socket = yield* Socket.fromWebSocket(openSocket(context, refusal), {
      openTimeout: CONNECT_TIMEOUT_MS,
    });
    const writer = yield* socket.writer;
    // The reader is ready once the socket is open.
    const reader = yield* socket.reader;
    opened.at = Date.now();
    const pong = { at: Date.now() };
    const hub = createProtocolClient(
      runnerMethods,
      hubNotifications,
      { syncNow: () => context.syncNow.openUnsafe() },
      // In the connection's scope: a write after the socket closed would otherwise wait forever.
      (message) => Effect.runSync(Effect.forkIn(writer.write(message), scope)),
    );
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => hub.rejectAllPendingRequests("The connection closed")),
    );

    /** A request of the hub, dropped when not answered in time or at all. */
    const request = <Name extends keyof typeof runnerMethods>(
      name: Name,
      params: Parameters<typeof hub.request<Name>>[1],
    ) =>
      Effect.tryPromise({
        try: () => hub.request(name, params, REQUEST_TIMEOUT_MS),
        catch: (error) =>
          new Dropped({ reason: `Syncing with the hub failed: ${ensureError(error).message}` }),
      });

    /** Passes what the hub sends to the client, noting its pongs. */
    const reading = Effect.forever(
      Effect.gen(function* () {
        for (const message of yield* reader.pull) {
          if (message === HEARTBEAT_PONG) pong.at = Date.now();
          else if (typeof message === "string") hub.receive(message);
        }
      }),
    );

    /**
     * Sends a heartbeat, or drops the connection when the hub stopped answering them. The heartbeat
     * is the Runner's own because nothing below provides one: on Workers the hub's runtime answers a
     * text message without waking the hub, and the protocol has no request the hub makes of an idle
     * Runner.
     */
    const heartbeat = Effect.suspend((): Effect.Effect<void, Dropped | Socket.SocketError> =>
      Date.now() - pong.at > HEARTBEAT_TIMEOUT_MS
        ? Effect.fail(new Dropped({ reason: "The hub stopped answering heartbeats; reconnecting" }))
        : writer.write(HEARTBEAT_PING),
    ).pipe(Effect.repeat(Schedule.spaced(context.heartbeatIntervalMs)));

    /**
     * Says hello, then syncs: at the interval the hub gave, with jitter, or as soon as the hub hints
     * or a job ends. A sync reports what became of jobs, if anything did, and brings the job runner
     * to the desired state the hub answers with.
     */
    const syncing = Effect.gen(function* () {
      const { syncIntervalMs } = yield* request("hello", { name: hostname() });
      for (;;) {
        yield* context.syncNow.close;
        const reports = context.jobs.takeReports();
        const load = context.jobs.load();
        const desired = yield* (
          reports.length > 0 ? request("report", { ...load, jobs: reports }) : request("sync", load)
        ).pipe(
          // The reports go again with the next sync on the next socket, also when the connection
          // dropping interrupts the request.
          Effect.onError(() => Effect.sync(() => context.jobs.returnReports(reports))),
        );
        context.jobs.reconcile(desired);
        const jitter = yield* Random.nextBetween(1 - SYNC_JITTER, 1 + SYNC_JITTER);
        yield* Effect.race(Effect.sleep(syncIntervalMs * jitter), context.syncNow.await);
      }
    });

    yield* Effect.all([reading, heartbeat, syncing], { concurrency: "unbounded", discard: true });
    return yield* Effect.never;
  }).pipe(
    Effect.mapError((error) =>
      Socket.isSocketError(error) ? refusedOrDropped(refusal, error) : error,
    ),
  );
}

/**
 * Why a socket did not open, as the Runner reads it. The hub answers 401 with `RUNNER_NOT_FOUND` to
 * a key it no longer knows: the Runner was removed, or the hub was reset. Retrying cannot succeed,
 * so the Runner stops, as a GitHub Actions runner does only on its server's `invalid_client`. A hub
 * supporting none of the versions offered says which it supports; either side upgrading resolves
 * that, so the Runner says so and retries. Other refusals, such as a proxy's or a hub's that cannot
 * reach its database, end the attempt, and the Runner tries again.
 */
function refusedOrDropped(refusal: Refusal, error: Socket.SocketError): Removed | Dropped {
  if (refusal.status === 401 && refusal.body?.code === RUNNER_NOT_FOUND) return new Removed();
  if (refusal.body?.code === PROTOCOL_UNSUPPORTED) {
    const supported = refusal.body.supported?.join(", ") ?? "none it names";
    return new Dropped({
      reason: `The hub supports none of this Runner's protocol versions (${RUNNER_PROTOCOL_VERSIONS.join(", ")}); it supports ${supported}. Upgrade whichever is older.`,
    });
  }
  return new Dropped({ reason: `The connection to the hub failed: ${error.message}` });
}

/**
 * Opens the socket, offering the protocol versions this Runner speaks, and reads the hub's refusal
 * into `refusal` if there is one, before the socket is dropped. A stopping Runner's socket closes,
 * waiting for the hub's answer for at most `CLOSE_TIMEOUT_MS`; a failed connection's is dropped.
 */
function openSocket(
  context: ConnectionContext,
  refusal: Refusal,
): Effect.Effect<WebSocket, never, Scope.Scope> {
  return Effect.acquireRelease(
    Effect.sync(() => {
      const socket = new WebSocket(context.url, [...RUNNER_PROTOCOL_VERSIONS], {
        headers: { "x-api-key": context.key },
        handshakeTimeout: CONNECT_TIMEOUT_MS,
        closeTimeout: CLOSE_TIMEOUT_MS,
      });
      // Failures reach the Runner through Effect's socket, whose own listener hears only the first
      // and is gone once the socket is released (Effect-TS/effect#9463); without this, a later one
      // would end the process.
      socket.on("error", () => {});
      socket.on("unexpected-response", (_request, response) => {
        // The body is read before the socket is dropped, which would discard it.
        void text(response)
          .catch(() => "")
          .then((body) => {
            refusal.status = response.statusCode ?? 0;
            refusal.body = parseRefusal(body);
            socket.terminate();
          });
      });
      return socket;
    }),
    (socket, exit) =>
      Effect.callback<void>((resume) => {
        if (socket.readyState === WebSocket.CLOSED) {
          resume(Effect.void);
          return;
        }
        socket.once("close", () => resume(Effect.void));
        // A stopping Runner closes politely, so the hub drops it at once. A connection that failed
        // is dropped at once rather than closed politely: the hub may not be answering.
        const stopping = Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause);
        if (stopping && socket.readyState === WebSocket.OPEN) socket.close(1000, "Runner stopped");
        else socket.terminate();
      }),
  );
}

/** The hub's refusal in `body`, if it is one the Runner knows how to read. */
function parseRefusal(body: string): SocketRefusal | undefined {
  try {
    const parsed = z.safeParse(socketRefusalSchema, JSON.parse(body));
    return parsed.success ? parsed.data : undefined;
  } catch {
    return undefined;
  }
}
