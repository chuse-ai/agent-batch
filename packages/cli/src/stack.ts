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
import {
  Cause,
  Config,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Path,
  Result,
  Runtime,
  Stream,
} from "effect";
import { Cookies } from "effect/http";
import { ChildProcess, ChildProcessSpawner, CommandLine } from "effect/process";
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
const RUNNER_CONFIG_LABEL = "ai.chuse.agent-batch.runner-config";

/** The model the Runner runs every job on, as the Runner's settings name it. */
const modelSchema = z.object({
  provider: z.string(),
  id: z.string(),
});
type Model = z.infer<typeof modelSchema>;

/** What a start keeps for the next: the secrets the database and hub were created with, and the keys made on them. */
const stateSchema = z.object({
  postgresPassword: z.string(),
  betterAuthSecret: z.string(),
  ownerPassword: z.string(),
  port: z.int(),
  model: modelSchema,
  /** Names only: their external values are required again when creating a Runner. */
  env: z._default(z.array(z.string()), []),
  /** The key Runners register with, which the Runner uses on its first start only. */
  registrationKey: z.optional(z.string()),
  /** The key for submitting and controlling jobs, which other commands use when no other is set. */
  apiKey: z.optional(z.string()),
});
type State = z.infer<typeof stateSchema>;

/** The first model: free, so a first job costs nothing but an OpenRouter account. */
const DEFAULT_MODEL = { provider: "openrouter", id: "openrouter/free" };

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

/** The Runner declaration contains forwarding names, never their external values. */
function runnerService(state: State, version: string) {
  return {
    image: `ghcr.io/chuse-ai/agent-batch-runner:${version}`,
    // The image's healthcheck reports the Runner healthy once it has finished starting.
    depends_on: { hub: { condition: "service_healthy" } },
    security_opt: ["seccomp=unconfined", "systempaths=unconfined", "apparmor=unconfined"],
    mem_limit: "8g",
    environment: {
      ...Object.fromEntries(state.env.map((name) => [name, null])),
      HUB_URL: "http://hub:8787",
      RUNNER_REGISTRATION_KEY: literal(state.registrationKey ?? ""),
      RUNNER_MODEL_PROVIDER: literal(state.model.provider),
      RUNNER_MODEL_ID: literal(state.model.id),
      RUNNER_DATA_DIR: "/data",
    },
    volumes: ["runner-data:/data"],
    restart: "unless-stopped",
  };
}

function runnerConfiguration(state: State, version: string) {
  const runner = runnerService(state, version);
  // Compose's hash includes resolved external values. This label compares the declaration
  // without reading retained secrets. The label travels with the container, so it describes the
  // Runner that exists; the file on disk is rewritten before each start and can describe one that
  // never launched. Registration is used only to initialize the Runner's
  // identity in its volume, so making the initial key does not change this declaration.
  return JSON.stringify({
    ...runner,
    environment: { ...runner.environment, RUNNER_REGISTRATION_KEY: undefined },
  });
}

/** The repository's Compose deployment with the stack's settings and a localhost-only hub. */
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
        ...runnerService(state, version),
        labels: { [RUNNER_CONFIG_LABEL]: literal(runnerConfiguration(state, version)) },
      },
    },
    volumes: { database: {}, "runner-data": {} },
  };
}

/** Writes the stack's deployment declaration, with forwarding names but never their values. */
function writeComposeFile(state: State, version: string) {
  // JSON is YAML, which Compose reads.
  return writeStateFile(COMPOSE_FILE, JSON.stringify(composeFile(state, version), null, 2));
}

/** A scoped declaration keeps credential commands from changing the selected deployment. */
function writeTemporaryComposeFile(declaration: object) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* makeStateDir;
    const file = yield* fs.makeTempFileScoped({ directory: STATE_DIR, prefix: "compose-" });
    yield* writeStateFile(file, JSON.stringify(declaration, null, 2));
    return file;
  });
}

/** Runs only the Runner command, sharing its data volume without starting the hub. */
function runRunnerCommand(
  state: State,
  version: string,
  args: readonly string[],
  interactive = false,
  terminal = false,
) {
  return Effect.gen(function* () {
    // Compose prepares every declared volume even with --no-deps. Authentication needs only
    // Runner data; it must not initialize the database as a side effect of logging in.
    // Authentication starts no sandbox and never reaches the hub, so the container gets neither the
    // sandbox's security options nor the hub's address and registration key. Its command opens no
    // listener, so the image's healthcheck is off rather than reporting it unhealthy.
    const { image, environment, volumes } = runnerService(state, version);
    const { HUB_URL: _hub, RUNNER_REGISTRATION_KEY: _key, ...authEnvironment } = environment;
    const file = yield* writeTemporaryComposeFile({
      name: PROJECT,
      services: {
        runner: { image, environment: authEnvironment, volumes, healthcheck: { disable: true } },
      },
      volumes: { "runner-data": {} },
    });
    yield* compose(
      [
        "run",
        "--rm",
        "--no-deps",
        ...(terminal ? [] : ["-T"]),
        "runner",
        "node",
        "dist/main.mjs",
        ...args,
      ],
      { file, interactive },
    );
  });
}

const NodeServices = NodeChildProcessSpawner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
);

/**
 * Runs `docker compose` with `args` on the project, printing what it prints, and fails unless it
 * succeeds. It reads only the stack's Compose file, never a deployment in the working directory.
 * `interactive` inherits the terminal's input, for a command that reads it. `errorsOnly` leaves
 * out Compose's warnings, such as that there was nothing to remove. Another `file` declares only
 * part of the project, so Compose would call the stack's other services orphans and suggest
 * --remove-orphans, which would remove them.
 */
function compose(
  args: readonly string[],
  options: {
    interactive?: boolean;
    errorsOnly?: boolean;
    file?: string;
  } = {},
) {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const fs = yield* FileSystem.FileSystem;
    const file = options.file ?? COMPOSE_FILE;
    if (!(yield* fs.exists(file))) {
      yield* say("agent-batch stack has not started a hub yet");
      return;
    }
    const logLevel = options.errorsOnly ? ["--log-level", "error"] : [];
    const exitCode = yield* spawner.exitCode(
      ChildProcess.make(
        "docker",
        [...logLevel, "compose", "--project-name", PROJECT, "--file", file, ...args],
        {
          stdin: options.interactive ? "inherit" : "ignore",
          stdout: "inherit",
          stderr: "inherit",
          // Ctrl-C must also reach Compose, including while following logs.
          detached: false,
          ...(options.file ? { env: { COMPOSE_IGNORE_ORPHANS: "true" }, extendEnv: true } : {}),
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
  }).pipe(
    Effect.scoped,
    Effect.catchReason("PlatformError", "NotFound", () =>
      Effect.fail(
        new Error("agent-batch stack runs the hub and a Runner with Docker: install Docker"),
      ),
    ),
    Effect.mapError(ensureError),
  );
}

/** Reads only lifecycle metadata and our nonsecret declaration, never retained environment values. */
function existingRunner() {
  return Effect.gen(function* () {
    const output = yield* dockerOutput([
      "ps",
      "--all",
      "--filter",
      `label=com.docker.compose.project=${PROJECT}`,
      "--filter",
      "label=com.docker.compose.service=runner",
      "--filter",
      "label=com.docker.compose.oneoff=False",
      "--format",
      `{{json .State}}\t{{json (.Label "${RUNNER_CONFIG_LABEL}")}}`,
    ]);
    if (output.trim() === "") return undefined;
    const containers = output.trim().split("\n");
    if (containers.length !== 1) {
      return yield* Effect.fail(
        new Error(
          "The stack has more than one Runner container; inspect it with docker compose before starting",
        ),
      );
    }
    return yield* Effect.try({
      try: () => {
        const [running, configuration] = containers[0].split("\t");
        return {
          running: z.parse(z.string(), JSON.parse(running)) === "running",
          configuration: z.parse(z.string(), JSON.parse(configuration)),
        };
      },
      catch: ensureError,
    });
  });
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
  flags: { provider?: string; model?: string },
  saved: Model | undefined,
): Model {
  const provider = flags.provider ?? saved?.provider ?? DEFAULT_MODEL.provider;
  const sameProvider = provider === saved?.provider;
  const id =
    flags.model ??
    (sameProvider ? saved?.id : undefined) ??
    (provider === DEFAULT_MODEL.provider ? DEFAULT_MODEL.id : undefined);
  if (id === undefined) throw new Error(`Give the model to run on ${provider} with --model`);
  return { provider, id };
}

type StartFlags = { port?: number; env?: string[]; clearEnv?: boolean } & Parameters<
  typeof chooseModel
>[0];

/** Proposes settings without committing a failed start's selection or endpoint. */
function stateForStart(flags: StartFlags, saved: State | undefined) {
  return Effect.gen(function* () {
    if (flags.env !== undefined && flags.clearEnv) {
      return yield* Effect.fail(new Error("Use either --env or --clear-env, not both"));
    }
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
      env: [],
    };
    const env = flags.clearEnv ? [] : (flags.env ?? state.env);
    return { ...state, port: flags.port ?? state.port, model, env: [...new Set(env)].toSorted() };
  });
}

/** Explicit forwarding must not replace the stack's endpoint, model selection or volume location. */
const environmentName = z.string().check(
  z.regex(/^[A-Za-z_][A-Za-z0-9_]*$/, "Give an environment variable name, without a value"),
  z.refine(
    (name) =>
      ![
        "HUB_URL",
        "RUNNER_REGISTRATION_KEY",
        "RUNNER_MODEL_PROVIDER",
        "RUNNER_MODEL_ID",
        "RUNNER_DATA_DIR",
      ].includes(name),
    "This variable is configured by the stack",
  ),
);

function requireForwardedEnvironment(names: readonly string[]) {
  return Effect.gen(function* () {
    const missing = names.filter((name) => process.env[name] === undefined);
    if (missing.length > 0) {
      yield* Effect.fail(
        new Error(
          `Creating the Runner requires these forwarded variables: ${missing.join(", ")}. Set them again, or use --clear-env and agent-batch stack login for persistent authentication`,
        ),
      );
    }
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
  provider: z.optional(
    z
      .string()
      .check(
        z.describe(
          "The provider of the model the Runner runs every job on, as Pi names it, such as anthropic; openrouter at first, then the last one given.",
        ),
      ),
  ),
  model: z.optional(
    z
      .string()
      .check(
        z.describe(
          "The model, as Pi names it, such as claude-sonnet-5-5; openrouter/free at first, then the last one given.",
        ),
      ),
  ),
};

/** Starts the stack in the background, waiting for the hub and the Runner's completed startup. */
function start(version: string, flags: StartFlags) {
  return Effect.gen(function* () {
    yield* lockState;
    const saved = yield* readState();
    let state = yield* stateForStart(flags, saved);
    const images = yield* imageVersion(version);
    // Save initial secrets even if Docker is unavailable, and before preflight can create a
    // volume. An existing deployment's settings stay authoritative until its hub is healthy.
    if (saved === undefined) yield* writeState(state);
    const runner = yield* existingRunner();
    const replaceRunner =
      runner === undefined ||
      flags.env !== undefined ||
      runner.configuration !== runnerConfiguration(state, images);
    const interactive = process.stdin.isTTY && process.stdout.isTTY;
    if (replaceRunner) yield* requireForwardedEnvironment(state.env);
    // Temporary preflight is valid only when it has the same authentication inputs as the Runner
    // being started. A replacement uses its proposed environment. For reuse, matching image/model
    // and the shared credential volume establish equivalence when no variables are forwarded.
    // Docker-retained inputs must be checked inside the existing Runner.
    if (replaceRunner || (!runner?.running && state.env.length === 0)) {
      yield* runRunnerCommand(
        state,
        images,
        ["auth-check", ...(interactive ? ["--login"] : [])],
        interactive,
        interactive,
      );
    } else if (runner?.running) {
      // Validate against the retained environment, not the caller's current shell. A stopped
      // existing container starts with its own environment without requiring that shell again.
      yield* compose(
        [
          "exec",
          ...(interactive ? [] : ["-T"]),
          "runner",
          "node",
          "dist/main.mjs",
          "auth-check",
          ...(interactive ? ["--login"] : []),
        ],
        { interactive },
      ).pipe(
        Effect.mapError(
          (error) =>
            new Error(
              `${error.message}. The existing Runner was not stopped. Check agent-batch stack logs; to repair provider authentication, run ${shellWords("agent-batch", "stack", "login", "--provider", state.model.provider)}, then agent-batch stack start.`,
            ),
        ),
      );
    }
    yield* writeComposeFile(state, images);
    // The first Runner needs a registration key from the healthy hub before it can start.
    yield* compose(["up", "-d", "--wait", "--wait-timeout", "60", "hub"]);
    yield* writeState(state);
    if (state.apiKey === undefined || state.registrationKey === undefined) {
      const client = yield* signIn(state);
      state = { ...state, ...(yield* createKeys(client)) };
      yield* writeState(state);
      yield* writeComposeFile(state, images);
    }
    yield* compose([
      "up",
      "-d",
      "--wait",
      "--wait-timeout",
      "60",
      "--no-deps",
      replaceRunner ? "--force-recreate" : "--no-recreate",
      "runner",
    ]).pipe(
      Effect.onError(() =>
        Effect.gen(function* () {
          const launched = replaceRunner || !runner?.running;
          // Cleanup must also finish on interruption, before the mutation lock is released.
          // Preserve the container and environment for repair, and leave existing work alone.
          const disposition = launched
            ? yield* compose(["stop", "--timeout", "10", "runner"]).pipe(
                Effect.match({
                  onSuccess: () => "The Runner was stopped",
                  onFailure: (error) =>
                    `Stopping the Runner failed: ${error.message}. It may still be running or restarting`,
                }),
              )
            : "The existing Runner was not stopped";
          yield* say(
            `Runner startup did not complete. ${disposition}; the hub and saved data remain. Check agent-batch stack status and agent-batch stack logs. If provider authentication is missing or needs repair, run ${shellWords("agent-batch", "stack", "login", "--provider", state.model.provider)}, then agent-batch stack start.`,
          );
        }),
      ),
    );
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

function runCredentialCommand(
  version: string,
  command: "login" | "logout",
  input: { provider?: string; withApiKey?: boolean },
) {
  return Effect.gen(function* () {
    // The container reads the key through `run -T`, where stdin is never a terminal, so only this
    // process can tell that a key would be typed, echoed, into the terminal.
    if (input.withApiKey && process.stdin.isTTY) {
      yield* Effect.fail(
        new Error(
          "Pipe the API key to agent-batch stack login --with-api-key, or use agent-batch stack login for hidden entry.",
        ),
      );
    }
    yield* lockState;
    const saved = yield* readState();
    const state = saved ?? (yield* stateForStart({}, undefined));
    if (saved === undefined) yield* writeState(state);
    // Authentication never imports the deployment's forwarded environment or changes its
    // selected provider. The isolated container shares only the Runner's existing data volume.
    const authentication = {
      ...state,
      env: [],
      model: { ...state.model, provider: input.provider ?? state.model.provider },
    };
    yield* runRunnerCommand(
      authentication,
      yield* imageVersion(version),
      [command, ...(input.withApiKey ? ["--with-api-key"] : [])],
      command === "login",
      command === "login" && !input.withApiKey && process.stdin.isTTY && process.stdout.isTTY,
    );
  }).pipe(Effect.scoped);
}

/**
 * `words`, each quoted where a POSIX shell needs it, so a command printed for a person to run is
 * one they can paste, whatever the names and IDs in it hold. `join` refuses only NUL, which no word
 * here can hold: each comes from the command line, at once or as saved, or from the hub, which
 * refuses it.
 */
export function shellWords(...words: readonly string[]): string {
  return Result.getOrThrow(CommandLine.join(words));
}

/**
 * Runs a command's `work` to its end, its failure an `Error` for trpc-cli to print, with the exit
 * code Effect's teardown gives it: 130 when a signal interrupted it, and an error's own
 * `Runtime.errorExitCode`, else 1.
 */
export function runToEnd(
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
        else {
          // Set before trpc-cli exits, as `bin.ts`'s exit keeps a code already set: 124 for a
          // timed-out wait, say, rather than trpc-cli's 1.
          process.exitCode = Runtime.getErrorExitCode(Cause.squash(exit.cause));
          reject(ensureError(Cause.squash(exit.cause)));
        }
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
          "Run and manage a hub, a Runner and Postgres on this machine. Starts the stack in the background with Docker and returns once the hub is healthy and the Runner has initialized and synced with it. The first start creates the hub's owner and an API key, which agent-batch commands use while AGENT_BATCH_URL and AGENT_BATCH_API_KEY are unset. Use stack login for persistent provider authentication, or --env to forward explicitly named variables to the Runner. An interactive first start offers login if authentication is missing.",
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
          env: z.optional(
            z
              .array(environmentName)
              .check(
                z.minLength(1),
                z.describe(
                  "Forward named host variables to the Runner under the same names; repeatable. Saves names only. Omit to retain the list; supply a list to replace it and refresh values, requiring Runner recreation.",
                ),
              ),
          ),
          clearEnv: z.optional(
            z
              .boolean()
              .check(
                z.describe(
                  "Clear all forwarded variable names; cannot be combined with --env. The Runner must have another authentication source before recreation.",
                ),
              ),
          ),
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
            const state = yield* readState();
            // Login can create the data volume before the first start writes a deployment.
            const file =
              state === undefined
                ? undefined
                : yield* writeTemporaryComposeFile(
                    composeFile({ ...state, env: [] }, yield* imageVersion(version)),
                  );
            yield* compose(["down", "--remove-orphans", "--volumes"], { errorsOnly: true, file });
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
      .meta({
        description:
          "Shows the stack's containers, health, image versions, hub URL and provider authentication source, without secrets or refreshing authentication.",
      })
      .input(z.object({}))
      .handler(() =>
        runToEnd(
          Effect.gen(function* () {
            const state = yield* readState();
            if (state !== undefined) yield* say(`Hub URL: ${hubUrl(state.port)}`);
            yield* compose(["ps", "--all"]);
            if (state === undefined) return;
            const runner = yield* existingRunner();
            if (runner?.running) {
              yield* compose(["exec", "-T", "runner", "node", "dist/main.mjs", "auth-status"]);
            } else {
              yield* say(
                "Provider authentication: runtime unavailable while the Runner is stopped or absent; credentials have not been validated.",
              );
              if (state.env.length > 0) {
                yield* say(
                  `Configured forwarding names: ${state.env.join(", ")}. Existing containers retain their creation-time environment; recreation requires the external values again.`,
                );
              }
            }
          }).pipe(Effect.scoped),
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
          "Saves a provider API key or subscription sign-in in the Runner's data volume without starting the stack or changing its selected model. Interactive API key entry is hidden. See the Runner's README for what a sign-in allows.",
      })
      .input(
        z.object({
          provider: z.optional(
            z
              .string()
              .check(
                z.describe(
                  "The provider to authenticate to, as Pi names it; defaults to the stack's selected provider. Does not change the selected provider or model.",
                ),
              ),
          ),
          withApiKey: z.optional(
            z
              .boolean()
              .check(
                z.describe(
                  "Read one provider API key from stdin and save it in the Runner's data volume, without interactive prompts.",
                ),
              ),
          ),
        }),
      )
      .handler(({ input }) => runToEnd(runCredentialCommand(version, "login", input))),
    logout: os
      .meta({
        description:
          "Removes the selected provider's stored credential from the Runner's data volume, without changing the deployment or forwarded environment.",
      })
      .input(
        z.object({
          provider: z.optional(
            z
              .string()
              .check(
                z.describe(
                  "The provider whose stored credential to remove; defaults to the stack's selected provider.",
                ),
              ),
          ),
        }),
      )
      .handler(({ input }) => runToEnd(runCredentialCommand(version, "logout", input))),
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
