import { randomBytes } from "node:crypto";
import path from "node:path";

import * as NodeChildProcessSpawner from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import * as NodePath from "@effect/platform-node-shared/NodePath";
import { createORPCClient } from "@orpc/client";
import type { ContractRouterClient } from "@orpc/contract";
import { apiContract, createProblemDetailsLink } from "contract";
import {
  Config,
  Effect,
  FileSystem,
  Layer,
  Path,
  PlatformError,
  Predicate,
  Schedule,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import ensureError from "ensure-error";
import envPaths from "env-paths";
import lockfile from "proper-lockfile";
import { os } from "trpc-cli";
import writeFileAtomic from "write-file-atomic";
import * as z from "zod/mini";

// `agent-batch dev`: a hub, a Runner and the hub's Postgres on this machine, from the images released
// with this version of the command, through Docker Compose (ADR 0034). It runs in the foreground, as
// `wrangler dev` and `temporal server start-dev` do, and stopping it removes the containers. What
// they hold stays in the project's volumes, and the secrets and keys it made stay in a file of the
// user's, so the next start finds the same jobs, keys and Runner, with any sign-in the Runner holds.

/**
 * The Compose project, apart from the quickstart's `agent-batch`, so neither touches the other.
 * `AGENT_BATCH_DEV_PROJECT` names another, so the repository's tests never touch a contributor's own.
 */
const PROJECT = process.env["AGENT_BATCH_DEV_PROJECT"] ?? "agent-batch-dev";

/** The owner the hub creates on its first start; nobody signs in as it but this command. */
const OWNER_EMAIL = "owner@example.com";

/** Where the state lives: the user's data folder, never the working directory, where it could be committed. */
const STATE_DIR = path.join(envPaths("agent-batch").data, "dev");
const STATE_FILE = path.join(STATE_DIR, "state.json");
const COMPOSE_FILE = path.join(STATE_DIR, "compose.json");

/** The model the Runner runs every job on, as the Runner's settings name it. */
const modelSchema = z.object({
  provider: z.string(),
  id: z.string(),
  /** The variable in the user's environment holding the provider's key, passed to the Runner. */
  keyEnv: z.string(),
});
type Model = z.infer<typeof modelSchema>;

/** What a start keeps for the next: the secrets the database and hub were created with, and the keys made on them. */
const stateSchema = z.object({
  postgresPassword: z.string(),
  betterAuthSecret: z.string(),
  ownerPassword: z.string(),
  port: z.int(),
  model: modelSchema,
  /** The key Runners register with, which the Runner uses on its first start only. */
  registrationKey: z.optional(z.string()),
  /** The key for submitting and controlling jobs, which other commands use when no other is set. */
  apiKey: z.optional(z.string()),
});
type State = z.infer<typeof stateSchema>;

/** The quickstart's model: free, so a first job costs nothing but an OpenRouter account. */
const DEFAULT_MODEL = { provider: "openrouter", id: "openrouter/free" };

/**
 * The variable Pi reads a provider's key from, for most providers: its name in capitals, `-` as
 * `_`, then `_API_KEY`, as `OPENAI_API_KEY` and `OPENROUTER_API_KEY`. Pi exports its full list
 * (`findEnvKeys`) only from `@earendil-works/pi-ai/compat`, which it marks as temporary, and with it
 * pi-ai would enter every user's bundle, so a provider whose variable differs, such as `google`'s
 * `GEMINI_API_KEY`, is named with `--model-key-env`.
 */
function keyEnvOf(provider: string): string {
  return `${provider.toUpperCase().replaceAll("-", "_")}_API_KEY`;
}

/** The error for a state file that is not as this command wrote it. */
function corruptState() {
  return new Error(
    `${STATE_FILE} is not as agent-batch dev wrote it; agent-batch dev reset deletes it with the data it belongs to`,
  );
}

/** The state of an earlier start, if any. */
function readState(): Effect.Effect<State | undefined, Error> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs
      .readFileString(STATE_FILE)
      .pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(undefined)));
    if (text === undefined) return undefined;
    const json: unknown = yield* Effect.try({ try: () => JSON.parse(text), catch: corruptState });
    const parsed = z.safeParse(stateSchema, json);
    if (!parsed.success) return yield* Effect.fail(corruptState());
    return parsed.data;
  }).pipe(Effect.mapError(ensureError), Effect.provide(NodeFileSystem.layer));
}

/** Makes the state's folder, which only the user may enter. */
const makeStateDir = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(STATE_DIR, { recursive: true, mode: 0o700 });
});

/**
 * Writes `contents` to `file` in the state's folder, readable only by the user, whole or not at
 * all: the state holds the keys and the database's password, which a torn write would lose.
 */
function writeStateFile(file: string, contents: string) {
  return makeStateDir.pipe(
    Effect.andThen(
      Effect.tryPromise({
        try: () => writeFileAtomic(file, contents, { mode: 0o600 }),
        catch: ensureError,
      }),
    ),
    Effect.mapError(ensureError),
    Effect.provide(NodeFileSystem.layer),
  );
}

/** Saves `state`. */
function writeState(state: State) {
  return writeStateFile(STATE_FILE, `${JSON.stringify(state, null, 2)}\n`);
}

/** A random secret, as `openssl rand -hex 24` makes one for the quickstart. */
function secret() {
  return randomBytes(24).toString("hex");
}

/** The hub `agent-batch dev` runs and the key it made, for commands run without their own settings. */
export function savedHub(): Effect.Effect<{ url: URL; apiKey: string } | undefined, Error> {
  return Effect.map(readState(), (state) =>
    state?.apiKey === undefined
      ? undefined
      : { url: new URL(hubUrl(state.port)), apiKey: state.apiKey },
  );
}

function hubUrl(port: number) {
  return `http://localhost:${port}`;
}

/** `value` as Compose reads it literally: it interpolates `$` in a Compose file's values. */
function literal(value: string) {
  return value.replaceAll("$", "$$$$");
}

/**
 * The Compose file for `state`, with the images of `version`. It follows the repository's
 * `compose.yaml`, which the quickstart runs, with this command's settings in place of `.env`, the
 * hub published on this machine only, and no restart policy: containers of a run stopped
 * outright stay stopped, for the next run or `agent-batch dev stop` to remove, rather than
 * starting again with Docker.
 */
function composeFile(state: State, version: string) {
  const database = {
    user: "agent_batch",
    password: literal(state.postgresPassword),
    name: "agent_batch",
  };
  return {
    name: PROJECT,
    services: {
      database: {
        image: "postgres:18-alpine",
        environment: {
          POSTGRES_USER: database.user,
          POSTGRES_PASSWORD: database.password,
          POSTGRES_DB: database.name,
        },
        volumes: ["database:/var/lib/postgresql"],
        shm_size: "128mb",
        healthcheck: {
          test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -U $${POSTGRES_USER} -d $${POSTGRES_DB}"],
          start_period: "30s",
          start_interval: "2s",
        },
        restart: "no",
      },
      hub: {
        image: `ghcr.io/chuse-ai/agent-batch-hub:${version}`,
        depends_on: { database: { condition: "service_healthy" } },
        environment: {
          DATABASE_URL: `postgres://${database.user}:${database.password}@database:5432/${database.name}`,
          HUB_URL: hubUrl(state.port),
          BETTER_AUTH_SECRET: literal(state.betterAuthSecret),
          ADMIN_EMAIL: OWNER_EMAIL,
          ADMIN_PASSWORD: literal(state.ownerPassword),
        },
        ports: [`127.0.0.1:${state.port}:8787`],
        restart: "no",
      },
      runner: {
        image: `ghcr.io/chuse-ai/agent-batch-runner:${version}`,
        depends_on: { hub: { condition: "service_healthy" } },
        security_opt: ["seccomp=unconfined", "systempaths=unconfined", "apparmor=unconfined"],
        mem_limit: "8g",
        environment: {
          HUB_URL: "http://hub:8787",
          RUNNER_REGISTRATION_KEY: literal(state.registrationKey ?? ""),
          RUNNER_MODEL_PROVIDER: literal(state.model.provider),
          RUNNER_MODEL_ID: literal(state.model.id),
          // Without a value, Compose passes the variable from this command's environment, if set.
          [state.model.keyEnv]: null,
        },
        volumes: ["runner-data:/data"],
        restart: "no",
      },
    },
    volumes: { database: {}, "runner-data": {} },
  };
}

/** Writes the Compose file for `state`; `docker compose` reads it from there, `dev login` included. */
function writeComposeFile(state: State, version: string) {
  // JSON is YAML, which Compose reads.
  return writeStateFile(COMPOSE_FILE, JSON.stringify(composeFile(state, version), null, 2));
}

const NodeServices = NodeChildProcessSpawner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
);

/**
 * Runs `docker compose` with `args` on the project, printing what it prints, and fails unless it
 * succeeds. It reads the Compose file written for the project once there is one: without it,
 * Compose finds the project's containers by its name, but not the one-off containers of
 * `dev login`. Stopping this command interrupts it with `SIGINT`, as Ctrl-C would, so Compose stops
 * what it started before it exits. In a process group of its own, as the spawner starts it, it gets
 * no Ctrl-C from the terminal: this command decides what a Ctrl-C stops. `interactive` keeps it in
 * this command's group with the terminal's input, for a command that reads it. `errorsOnly` leaves
 * out Compose's warnings, such as that there was nothing to remove.
 */
function compose(
  args: readonly string[],
  options: { interactive?: boolean; errorsOnly?: boolean } = {},
) {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const fs = yield* FileSystem.FileSystem;
    const file = (yield* fs.exists(COMPOSE_FILE)) ? ["--file", COMPOSE_FILE] : [];
    const logLevel = options.errorsOnly ? ["--log-level", "error"] : [];
    const handle = yield* spawner.spawn(
      ChildProcess.make(
        "docker",
        [...logLevel, "compose", "--project-name", PROJECT, ...file, ...args],
        {
          stdin: options.interactive ? "inherit" : "ignore",
          stdout: "inherit",
          stderr: "inherit",
          detached: !options.interactive,
          killSignal: "SIGINT",
          forceKillAfter: "30 seconds",
        },
      ),
    );
    const exitCode = yield* handle.exitCode;
    if (exitCode !== 0) {
      yield* Effect.fail(new Error(`docker compose ${args[0]} exited with ${exitCode}`));
    }
  }).pipe(
    Effect.scoped,
    Effect.mapError((error) =>
      PlatformError.isPlatformError(error) && Predicate.isTagged(error.reason, "NotFound")
        ? new Error("agent-batch dev runs the hub and a Runner with Docker: install Docker")
        : ensureError(error),
    ),
  );
}

/** Removes the project's containers and network, leaving its volumes, as after a run stopped outright. */
const removeContainers = compose(["down", "--remove-orphans"], { errorsOnly: true });

/**
 * Holds the lock on the state while the scope is open, so a second `agent-batch dev` cannot remove
 * the first one's containers. A run killed outright leaves the lock behind; proper-lockfile takes
 * it over once its holder stops refreshing it, by default after 10 seconds.
 */
const lockState = Effect.acquireRelease(
  makeStateDir.pipe(
    Effect.provide(NodeFileSystem.layer),
    Effect.andThen(
      Effect.tryPromise({
        try: () =>
          lockfile.lock(STATE_DIR, {
            lockfilePath: path.join(STATE_DIR, "lock"),
            // Without it, proper-lockfile throws from a timer, outside any fiber, and the process
            // ends without removing its containers.
            onCompromised: (error) =>
              process.stderr.write(
                `agent-batch dev lost its lock (${error.message}), so another could start beside it\n`,
              ),
          }),
        catch: (error) =>
          error instanceof Error && "code" in error && error.code === "ELOCKED"
            ? new Error("agent-batch dev is running; Ctrl-C in its terminal stops it")
            : ensureError(error),
      }),
    ),
    Effect.mapError(ensureError),
  ),
  // Releasing a lock that was lost fails, and changes nothing.
  (release) => Effect.tryPromise(() => release()).pipe(Effect.ignore),
);

type MemberClient = ContractRouterClient<typeof apiContract>;

/**
 * Signs in as the owner and returns a client calling the hub as them, for what only a member may
 * do: making keys and listing Runners. Better Auth answers a sign-in with its session cookie, which
 * the client sends back.
 */
function signIn(state: State) {
  return Effect.tryPromise({
    try: async (): Promise<MemberClient> => {
      const url = hubUrl(state.port);
      const response = await fetch(`${url}/api/auth/sign-in/email`, {
        method: "POST",
        // Better Auth refuses a sign-in from fetch without an origin it trusts, as it guards against
        // cross-site requests; the hub trusts its own URL.
        headers: { "content-type": "application/json", origin: url },
        body: JSON.stringify({ email: OWNER_EMAIL, password: state.ownerPassword }),
      });
      if (!response.ok) {
        throw new Error(`The hub refused the owner's sign-in: ${await response.text()}`);
      }
      // A `Set-Cookie` header is the cookie's `name=value`, then its attributes after `;`.
      const cookie = response.headers
        .getSetCookie()
        .map((header) => header.split(";", 1)[0])
        .join("; ");
      return createORPCClient(
        createProblemDetailsLink(apiContract, { origin: url, url: "/", headers: { cookie } }),
      );
    },
    catch: ensureError,
  });
}

/** The keys a fresh hub needs, made as the quickstart makes them: one for Runners, one for jobs. */
function createKeys(client: MemberClient) {
  return Effect.tryPromise({
    try: async () => {
      const runners = await client.apiKeys.create({
        name: "agent-batch dev runners",
        permissions: { runner: ["register"] },
      });
      const jobs = await client.apiKeys.create({
        name: "agent-batch dev jobs",
        permissions: { job: ["create", "read", "pause", "resume", "cancel", "delete"] },
      });
      return { registrationKey: runners.key, apiKey: jobs.key };
    },
    catch: ensureError,
  });
}

/** When the hub last heard from each Runner it knows, by its own clock. */
function listRunnersLastSeen(client: MemberClient) {
  return Effect.tryPromise({ try: () => client.runners.list(), catch: ensureError }).pipe(
    Effect.map((runners) => new Map(runners.map((runner) => [runner.id, runner.lastSeenAt]))),
  );
}

/**
 * Waits until the hub hears from a Runner after `before`, what it had last heard from each:
 * compared on the hub's clock alone, which the host's may differ from, as in a virtual machine.
 */
function waitForRunner(client: MemberClient, before: Map<string, Date>) {
  return listRunnersLastSeen(client).pipe(
    // A read the hub does not answer only waits: the hub stopping ends the run through Compose.
    Effect.retry({ schedule: Schedule.spaced("1 second") }),
    Effect.repeat({
      schedule: Schedule.spaced("1 second"),
      until: (now) =>
        [...now].some(([id, lastSeenAt]) => {
          const earlier = before.get(id);
          return earlier === undefined || lastSeenAt > earlier;
        }),
    }),
  );
}

/** The model for this run: the flags given, else the last run's, else the quickstart's. */
function chooseModel(
  flags: { modelProvider?: string; modelId?: string; modelKeyEnv?: string },
  saved: Model | undefined,
): Model {
  const provider = flags.modelProvider ?? saved?.provider ?? DEFAULT_MODEL.provider;
  const sameProvider = provider === saved?.provider;
  const id =
    flags.modelId ??
    (sameProvider ? saved?.id : undefined) ??
    (provider === DEFAULT_MODEL.provider ? DEFAULT_MODEL.id : undefined);
  if (id === undefined) throw new Error(`Give the model to run on ${provider} with --model-id`);
  const keyEnv =
    flags.modelKeyEnv ?? (sameProvider ? saved?.keyEnv : undefined) ?? keyEnvOf(provider);
  return { provider, id, keyEnv };
}

/** The state for a run with `flags`: an earlier run's, or new secrets, with the flags applied. */
function saveStateForRun(flags: { port?: number } & Parameters<typeof chooseModel>[0]) {
  return Effect.gen(function* () {
    const saved = yield* readState();
    const model = yield* Effect.try({
      try: () => chooseModel(flags, saved?.model),
      catch: ensureError,
    });
    const state: State = saved ?? {
      postgresPassword: secret(),
      betterAuthSecret: secret(),
      ownerPassword: secret(),
      port: 8787,
      model,
    };
    const updated = { ...state, port: flags.port ?? state.port, model };
    yield* writeState(updated);
    return updated;
  });
}

/** The version of the images to run: this command's, unless `AGENT_BATCH_VERSION` names another. */
function imageVersion(version: string) {
  return Config.String("AGENT_BATCH_VERSION").pipe(Config.withDefault(version));
}

function say(message: string) {
  return Effect.sync(() => process.stderr.write(`${message}\n`));
}

const modelFlags = {
  modelProvider: z.optional(
    z
      .string()
      .check(
        z.describe(
          "The provider of the model the Runner runs every job on, as Pi names it, such as anthropic; openrouter at first, then the last one given.",
        ),
      ),
  ),
  modelId: z.optional(
    z
      .string()
      .check(
        z.describe(
          "The model, as Pi names it, such as claude-sonnet-5-5; openrouter/free at first, then the last one given.",
        ),
      ),
  ),
  modelKeyEnv: z.optional(
    z
      .string()
      .check(
        z.describe(
          "The variable holding the provider's key, passed to the Runner; <PROVIDER>_API_KEY by default, as Pi names most.",
        ),
      ),
  ),
};

/** `agent-batch dev`, until it is stopped. */
function start(version: string, flags: { port?: number } & Parameters<typeof chooseModel>[0]) {
  return Effect.gen(function* () {
    yield* lockState;
    let state = yield* saveStateForRun(flags);
    const images = yield* imageVersion(version);
    yield* writeComposeFile(state, images);
    // Containers a run stopped outright left behind; the volumes stay.
    yield* removeContainers;
    yield* Effect.addFinalizer(() =>
      say("Removing the containers; the data stays for the next start").pipe(
        Effect.andThen(removeContainers),
        Effect.catch((error) =>
          say(`${error.message}. agent-batch dev stop removes what is left.`),
        ),
      ),
    );
    yield* compose(["up", "--detach", "--wait", "hub"]);
    const client = yield* signIn(state);
    if (state.apiKey === undefined || state.registrationKey === undefined) {
      state = { ...state, ...(yield* createKeys(client)) };
      yield* writeState(state);
      yield* writeComposeFile(state, images);
    }
    const before = yield* listRunnersLastSeen(client);
    yield* Effect.forkScoped(
      waitForRunner(client, before).pipe(
        Effect.andThen(
          say(
            [
              "",
              `The hub is at ${hubUrl(state.port)}, with a Runner running ${state.model.id} from ${state.model.provider}.`,
              "In another terminal, agent-batch commands use it while AGENT_BATCH_URL and AGENT_BATCH_API_KEY are unset;",
              "agent-batch dev env prints them for other clients. Press Ctrl-C to stop.",
              "",
            ].join("\n"),
          ),
        ),
      ),
    );
    // Compose's own foreground mode: it starts the Runner, prints every container's log, and stops
    // them all when any of them stops, as a Runner that refuses to start does. It ends on its own
    // only then, or when it cannot start the Runner.
    const stopped = yield* compose(["up", "--abort-on-container-exit"]).pipe(
      Effect.match({ onFailure: (error) => `${error.message}. `, onSuccess: () => "" }),
    );
    return yield* Effect.fail(
      new Error(
        `${stopped}The hub, the Runner or the database stopped, or Compose could not start them, so agent-batch dev stopped them all; Compose's output above says why. If the Runner says jobs cannot run in a sandbox, on Ubuntu 24.04 run: sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0 (see https://github.com/chuse-ai/agent-batch/blob/main/apps/runner/README.md#what-a-host-needs).`,
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices));
}

/**
 * Runs `work` until it ends or the user stops it with Ctrl-C, `SIGTERM` or a closed terminal
 * (`SIGHUP`), which interrupts it so its finalizers run. A signal while it stops is ignored, so the
 * containers are still removed; a listener on the signal keeps Node from exiting at it. Effect's
 * `NodeRuntime.runMain` does the same for `SIGINT` and `SIGTERM` only, and is built to end the
 * process, where this returns to trpc-cli.
 */
async function runUntilStopped(work: Effect.Effect<void, unknown>) {
  const controller = new AbortController();
  const onSignal = () => {
    if (controller.signal.aborted) {
      process.stderr.write("Still stopping: the containers are being removed\n");
      return;
    }
    controller.abort();
  };
  const signals = ["SIGINT", "SIGTERM", "SIGHUP"] as const;
  for (const signal of signals) process.on(signal, onSignal);
  try {
    await Effect.runPromise(work, { signal: controller.signal });
  } catch (error) {
    // Stopping is how the command ends; only a failure before it is one.
    if (!controller.signal.aborted) throw error;
  } finally {
    for (const signal of signals) process.off(signal, onSignal);
  }
}

/** Runs a command's `work` to its end, its failure an `Error` for trpc-cli to print. */
function runToEnd(
  work: Effect.Effect<
    void,
    unknown,
    ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
  >,
) {
  return Effect.runPromise(work.pipe(Effect.mapError(ensureError), Effect.provide(NodeServices)));
}

/** The `dev` commands, running the images of `version`. */
export function devCommands(version: string) {
  return os.router({
    start: os
      .meta({
        default: true,
        jsonInput: "never",
        description:
          "Runs a hub, a Runner and the hub's Postgres on this machine with Docker, until stopped with Ctrl-C, which removes the containers and keeps their data for the next start. The first start creates the hub's owner and an API key, which agent-batch commands in other terminals use while AGENT_BATCH_URL and AGENT_BATCH_API_KEY are unset. The Runner takes its provider's key from this command's environment, or the sign-in agent-batch dev login saves.",
      })
      .input(
        z.object({
          port: z.optional(
            z
              .int()
              .check(
                z.describe(
                  "The port on this machine the hub listens on; 8787 at first, then the last one given.",
                ),
              ),
          ),
          ...modelFlags,
        }),
      )
      .handler(({ input }) => runUntilStopped(start(version, input))),
    stop: os
      .meta({
        jsonInput: "never",
        description:
          "Removes the containers agent-batch dev left behind when it was stopped outright, keeping their data.",
      })
      // Without an input, trpc-cli offers a `--json` it cannot describe.
      .input(z.object({}))
      .handler(() => runToEnd(lockState.pipe(Effect.andThen(removeContainers), Effect.scoped))),
    reset: os
      .meta({
        jsonInput: "never",
        description:
          "Deletes everything agent-batch dev keeps: the hub's jobs, the Runner's data and any sign-in it holds, and the keys and secrets made for them.",
      })
      // Without an input, trpc-cli offers a `--json` it cannot describe.
      .input(z.object({}))
      .handler(() =>
        runToEnd(
          Effect.gen(function* () {
            yield* lockState;
            yield* compose(["down", "--remove-orphans", "--volumes"], { errorsOnly: true });
            // Compose succeeds when a volume is still in use, and keeps it; its password is in the
            // state, so the state goes only with the volumes.
            const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
            const left = yield* spawner.string(
              ChildProcess.make("docker", [
                "volume",
                "ls",
                "--quiet",
                "--filter",
                `label=com.docker.compose.project=${PROJECT}`,
              ]),
            );
            if (left.trim() !== "") {
              return yield* Effect.fail(
                new Error(
                  `Docker kept volumes still in use, so the keys and secrets for them stay: ${left.trim().split("\n").join(", ")}`,
                ),
              );
            }
            const fs = yield* FileSystem.FileSystem;
            return yield* fs.remove(STATE_DIR, { recursive: true, force: true });
          }).pipe(Effect.scoped),
        ),
      ),
    login: os
      .meta({
        jsonInput: "never",
        description:
          "Signs the Runner in to its model provider's subscription, where Pi offers one, instead of a key; the Runner keeps the sign-in with its data. See the Runner's README for what a sign-in allows.",
      })
      .input(z.object(modelFlags))
      .handler(({ input }) =>
        runToEnd(
          Effect.gen(function* () {
            yield* lockState;
            const state = yield* saveStateForRun(input);
            yield* writeComposeFile(state, yield* imageVersion(version));
            yield* compose(
              ["run", "--rm", "--no-deps", "runner", "node", "dist/main.mjs", "login"],
              { interactive: true },
            );
          }).pipe(Effect.scoped),
        ),
      ),
    env: os
      .meta({
        jsonInput: "never",
        description:
          "Prints the hub's address and the API key agent-batch dev made as shell exports, for clients other than agent-batch.",
      })
      // Without an input, trpc-cli offers a `--json` it cannot describe.
      .input(z.object({}))
      .handler(() =>
        Effect.runPromise(
          Effect.gen(function* () {
            const hub = yield* savedHub();
            if (hub === undefined) {
              return yield* Effect.fail(new Error("agent-batch dev has not started a hub yet"));
            }
            return yield* Effect.sync(() => {
              process.stdout.write(
                `export AGENT_BATCH_URL=${hub.url.origin}\nexport AGENT_BATCH_API_KEY=${hub.apiKey}\n`,
              );
            });
          }),
        ),
      ),
  });
}
