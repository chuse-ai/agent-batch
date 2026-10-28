import { randomBytes } from "node:crypto";
import path from "node:path";

// The shared package avoids @effect/platform-node's required redis peer, which the CLI does not use.
import * as NodeChildProcessSpawner from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import * as NodePath from "@effect/platform-node-shared/NodePath";
import * as NodeRuntime from "@effect/platform-node-shared/NodeRuntime";
import { createORPCClient } from "@orpc/client";
import type { ContractRouterClient } from "@orpc/contract";
import { writeFile } from "atomically";
import { apiContract, createProblemDetailsLink } from "contract";
import { Cause, Config, Effect, Exit, FileSystem, Layer, Path, Runtime, Stream } from "effect";
import { Cookies } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import ensureError from "ensure-error";
import envPaths from "env-paths";
import lockfile from "proper-lockfile";
import { os } from "trpc-cli";
import * as z from "zod/mini";

// `agent-batch stack`: a hub, a Runner and the hub's Postgres on this machine, from the images
// released with this version of the command, through Docker Compose (ADR 0034). The containers
// run in the background until stopped. Their volumes and the user's saved secrets and keys keep
// the same jobs, keys and Runner, with any sign-in the Runner holds, across starts.

/**
 * The Compose project, apart from the Compose deployment's `agent-batch`, so neither touches the other.
 * `AGENT_BATCH_STACK_PROJECT` names another, so the repository's tests never touch a contributor's own.
 */
const PROJECT = process.env["AGENT_BATCH_STACK_PROJECT"] ?? "agent-batch-stack";

/** The owner the hub creates on its first start; nobody signs in as it but this command. */
const OWNER_EMAIL = "owner@example.com";

/** Where the state lives: the user's data folder, never the working directory, where it could be committed. */
const STATE_DIR = path.join(envPaths("agent-batch").data, "stack");
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

/** The first model: free, so a first job costs nothing but an OpenRouter account. */
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
    `${STATE_FILE} is not as agent-batch stack wrote it; agent-batch stack delete deletes it with the data it belongs to`,
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
        try: () => writeFile(file, contents, { mode: 0o600 }),
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

/** A random secret, as `openssl rand -hex 24` makes one. */
function secret() {
  return randomBytes(24).toString("hex");
}

/** The hub `agent-batch stack` runs and the key it made, for commands run without their own settings. */
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
 * `compose.yaml`, the Compose deployment's, with this command's settings in place of `.env`, the
 * hub published on this machine only. Containers restart with Docker unless explicitly stopped,
 * so jobs can keep running after the user closes their terminal.
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
        restart: "unless-stopped",
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
        restart: "unless-stopped",
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
        restart: "unless-stopped",
      },
    },
    volumes: { database: {}, "runner-data": {} },
  };
}

/** Writes the Compose file for `state`; `docker compose` reads it from there, `stack login` included. */
function writeComposeFile(state: State, version: string) {
  // JSON is YAML, which Compose reads.
  return writeStateFile(COMPOSE_FILE, JSON.stringify(composeFile(state, version), null, 2));
}

const NodeServices = NodeChildProcessSpawner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
);

/**
 * Runs `docker compose` with `args` on the project, printing what it prints, and fails unless it
 * succeeds. It reads only the stack's Compose file, never a deployment in the working directory.
 * `interactive` inherits the terminal's input, for a command that reads it. `errorsOnly` leaves
 * out Compose's warnings, such as that there was nothing to remove.
 */
function compose(
  args: readonly string[],
  options: { interactive?: boolean; errorsOnly?: boolean } = {},
) {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const fs = yield* FileSystem.FileSystem;
    if (!(yield* fs.exists(COMPOSE_FILE))) {
      yield* say("agent-batch stack has not started a hub yet");
      return;
    }
    const logLevel = options.errorsOnly ? ["--log-level", "error"] : [];
    const exitCode = yield* spawner.exitCode(
      ChildProcess.make(
        "docker",
        [...logLevel, "compose", "--project-name", PROJECT, "--file", COMPOSE_FILE, ...args],
        {
          stdin: options.interactive ? "inherit" : "ignore",
          stdout: "inherit",
          stderr: "inherit",
          // Ctrl-C must also reach Compose, including while following logs.
          detached: false,
        },
      ),
    );
    if (exitCode !== 0) {
      yield* Effect.fail(new Error(`docker compose ${args[0]} exited with ${exitCode}`));
    }
  }).pipe(
    Effect.catchReason("PlatformError", "NotFound", () =>
      Effect.fail(
        new Error("agent-batch stack runs the hub and a Runner with Docker: install Docker"),
      ),
    ),
    Effect.mapError(ensureError),
  );
}

/** What `docker` with `args` prints, failing unless it succeeds. */
function dockerOutput(args: readonly string[]) {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const handle = yield* spawner.spawn(
      ChildProcess.make("docker", args, {
        stderr: "inherit",
      }),
    );
    const [stdout, exitCode] = yield* Effect.all(
      [Stream.mkString(Stream.decodeText(handle.stdout)), handle.exitCode],
      { concurrency: "unbounded" },
    );
    if (exitCode !== 0) {
      return yield* Effect.fail(new Error(`docker ${args[0]} exited with ${exitCode}`));
    }
    return stdout;
  }).pipe(Effect.scoped);
}

/**
 * Holds the lock while a mutating command runs, so starts, stops, deletes and sign-ins cannot
 * race over the state or containers. After a command is killed, proper-lockfile takes the lock
 * over once its holder stops refreshing it, by default after 10 seconds.
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
            // ends outside the command's error handling.
            onCompromised: (error) =>
              process.stderr.write(
                `agent-batch stack lost its lock (${error.message}), so another command could change the stack\n`,
              ),
          }),
        catch: (caught) => {
          const error = ensureError(caught);
          return "code" in error && error.code === "ELOCKED"
            ? new Error(
                "Another agent-batch stack command is changing the stack; if it was killed, its lock expires after 10 seconds",
              )
            : error;
        },
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
 * do: making keys. Better Auth answers a sign-in with its session cookie, which the client sends back.
 */
function signIn(state: State) {
  return Effect.tryPromise({
    try: async (signal): Promise<MemberClient> => {
      const url = hubUrl(state.port);
      const response = await fetch(`${url}/api/auth/sign-in/email`, {
        signal,
        method: "POST",
        // Better Auth refuses a sign-in from fetch without an origin it trusts, as it guards against
        // cross-site requests; the hub trusts its own URL.
        headers: { "content-type": "application/json", origin: url },
        body: JSON.stringify({ email: OWNER_EMAIL, password: state.ownerPassword }),
      });
      if (!response.ok) {
        throw new Error(`The hub refused the owner's sign-in: ${await response.text()}`);
      }
      const cookie = Cookies.toCookieHeader(Cookies.fromSetCookie(response.headers.getSetCookie()));
      return createORPCClient(
        createProblemDetailsLink(apiContract, {
          origin: url,
          url: "/",
          headers: { cookie },
        }),
      );
    },
    catch: ensureError,
  });
}

/** The keys a fresh hub needs, made as the Compose deployment's users make them: one for Runners, one for jobs. */
function createKeys(client: MemberClient) {
  return Effect.tryPromise({
    try: async (signal) => {
      const runners = await client.apiKeys.create(
        {
          name: "agent-batch stack runners",
          permissions: { runner: ["register"] },
        },
        { signal },
      );
      const jobs = await client.apiKeys.create(
        {
          name: "agent-batch stack jobs",
          permissions: { job: ["create", "read", "pause", "resume", "cancel", "delete"] },
        },
        { signal },
      );
      return { registrationKey: runners.key, apiKey: jobs.key };
    },
    catch: ensureError,
  });
}

/** The model for this start: the flags given, else the last start's, else the first model. */
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

/** Saves secrets and model settings, returning the proposed state with `flags` applied. */
function saveStateForStart(flags: { port?: number } & Parameters<typeof chooseModel>[0]) {
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
    // Persist secrets before creating volumes, but keep callers on the previous endpoint until
    // Compose confirms the replacement hub is healthy: a failed pull can leave the old hub running.
    yield* writeState({ ...updated, port: saved?.port ?? updated.port });
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

/** Starts the stack in the background, returning once Compose reports it running and the hub healthy. */
function start(version: string, flags: { port?: number } & Parameters<typeof chooseModel>[0]) {
  return Effect.gen(function* () {
    yield* lockState;
    let state = yield* saveStateForStart(flags);
    const images = yield* imageVersion(version);
    yield* writeComposeFile(state, images);
    // The first Runner needs a registration key from the healthy hub before it can start.
    yield* compose(["up", "-d", "--wait", "hub"]);
    yield* writeState(state);
    if (state.apiKey === undefined || state.registrationKey === undefined) {
      const client = yield* signIn(state);
      state = { ...state, ...(yield* createKeys(client)) };
      yield* writeState(state);
      yield* writeComposeFile(state, images);
    }
    yield* compose(["up", "-d", "--wait"]);
    yield* say(
      [
        `The hub is at ${hubUrl(state.port)}. The stack runs in the background.`,
        "agent-batch commands use it while AGENT_BATCH_URL and AGENT_BATCH_API_KEY are unset;",
        "agent-batch stack env prints them for other clients.",
        "Use agent-batch stack status or agent-batch stack logs to check it, and agent-batch stack stop to stop it.",
      ].join("\n"),
    );
  }).pipe(Effect.scoped);
}

/** Runs a command's `work` to its end, its failure an `Error` for trpc-cli to print. */
function runToEnd(
  work: Effect.Effect<
    void,
    unknown,
    ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
  >,
) {
  // runMain interrupts on signals and waits for scoped Compose children before releasing the
  // mutation lock. Its teardown leaves ordinary failures to trpc-cli's error formatter.
  return new Promise<void>((resolve, reject) => {
    NodeRuntime.runMain(work.pipe(Effect.provide(NodeServices)), {
      disableErrorReporting: true,
      teardown: (exit, onExit) => {
        if (Exit.isSuccess(exit)) resolve();
        else if (Cause.hasInterruptsOnly(exit.cause)) Runtime.defaultTeardown(exit, onExit);
        else reject(ensureError(Cause.squash(exit.cause)));
      },
    });
  });
}

/** The `stack` commands, running the images of `version`. */
export function stackCommands(version: string) {
  return os.router({
    start: os
      .meta({
        default: true,
        description:
          "Run and manage a hub, a Runner and Postgres on this machine. Starts the stack in the background with Docker and returns once the hub is healthy. The first start creates the hub's owner and an API key, which agent-batch commands use while AGENT_BATCH_URL and AGENT_BATCH_API_KEY are unset. The Runner takes its provider's key from this command's environment, or the sign-in agent-batch stack login saves.",
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
      .handler(({ input }) => runToEnd(start(version, input))),
    stop: os
      .meta({
        description:
          "Stops the stack, keeping the database, Runner data and saved credentials for the next start.",
      })
      // Without an input, trpc-cli offers a `--json` it cannot describe.
      .input(z.object({}))
      .handler(() => runToEnd(lockState.pipe(Effect.andThen(compose(["stop"])), Effect.scoped))),
    delete: os
      .meta({
        description:
          "Stops the stack and deletes its database and Runner volumes, including jobs and provider sign-ins, and its saved credentials file.",
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
            const left = yield* dockerOutput([
              "volume",
              "ls",
              "--quiet",
              "--filter",
              `label=com.docker.compose.project=${PROJECT}`,
            ]);
            if (left.trim() !== "") {
              yield* Effect.fail(
                new Error(
                  `Docker kept volumes still in use, so the keys and secrets for them stay: ${left.trim().split("\n").join(", ")}`,
                ),
              );
            }
            const fs = yield* FileSystem.FileSystem;
            // Keep the directory and its lock until this command releases it.
            yield* fs.remove(STATE_FILE, { force: true });
            yield* fs.remove(COMPOSE_FILE, { force: true });
          }).pipe(Effect.scoped),
        ),
      ),
    status: os
      .meta({ description: "Shows the stack's containers, health, image versions and hub URL." })
      .input(z.object({}))
      .handler(() =>
        runToEnd(
          Effect.gen(function* () {
            const state = yield* readState();
            if (state !== undefined) yield* say(`Hub URL: ${hubUrl(state.port)}`);
            yield* compose(["ps", "--all"]);
          }),
        ),
      ),
    logs: os
      .meta({ description: "Prints the stack's container logs." })
      .input(
        z.object({
          follow: z.optional(z.boolean().check(z.describe("Keep following new log output."))),
        }),
      )
      .handler(({ input }) => runToEnd(compose(["logs", ...(input.follow ? ["--follow"] : [])]))),
    login: os
      .meta({
        description:
          "Signs the Runner in to its model provider's subscription, where Pi offers one, instead of a key; the Runner keeps the sign-in with its data. See the Runner's README for what a sign-in allows.",
      })
      .input(
        z.object({
          modelProvider: z.optional(
            z
              .string()
              .check(
                z.describe(
                  "The provider to sign in to, as Pi names it, such as openai-codex; the one agent-batch stack last ran on by default. Start agent-batch stack with it to run jobs on the sign-in.",
                ),
              ),
          ),
        }),
      )
      .handler(({ input }) =>
        runToEnd(
          Effect.gen(function* () {
            yield* lockState;
            // The Runner's `login` reads only the provider; the model a start runs on stays as saved.
            const state = (yield* readState()) ?? (yield* saveStateForStart({}));
            const provider = input.modelProvider ?? state.model.provider;
            yield* writeComposeFile(
              { ...state, model: { ...state.model, provider } },
              yield* imageVersion(version),
            );
            yield* compose(
              ["run", "--rm", "--no-deps", "runner", "node", "dist/main.mjs", "login"],
              { interactive: true },
            );
          }).pipe(Effect.scoped),
        ),
      ),
    env: os
      .meta({
        description:
          "Prints the hub's address and the API key agent-batch stack made as shell exports, for clients other than agent-batch.",
      })
      // Without an input, trpc-cli offers a `--json` it cannot describe.
      .input(z.object({}))
      .handler(() =>
        Effect.runPromise(
          Effect.gen(function* () {
            const hub = yield* savedHub();
            if (hub === undefined) {
              return yield* Effect.fail(new Error("agent-batch stack has not started a hub yet"));
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
