import { randomUUID } from "node:crypto";
import path from "node:path";
import { parseArgs } from "node:util";

import type { AuthInteraction } from "@earendil-works/pi-ai";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { MODEL_THINKING_LEVELS } from "@earendil-works/pi-ai/providers/model-schema";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import * as NodePath from "@effect/platform-node-shared/NodePath";
// The shared package rather than `@effect/platform-node`, whose required `redis` peer the Runner
// would install without using.
import * as NodeRuntime from "@effect/platform-node-shared/NodeRuntime";
import * as NodeSocketServer from "@effect/platform-node-shared/NodeSocketServer";
import * as NodeTerminal from "@effect/platform-node-shared/NodeTerminal";
import { getDotPath } from "@standard-schema/utils";
import {
  Config,
  Data,
  Effect,
  FileSystem,
  Layer,
  Latch,
  Option,
  Redacted,
  Runtime,
  Schema,
  SchemaIssue,
  Terminal,
} from "effect";
import * as Prompt from "effect/cli/Prompt";

import { createRunnerAuth, readApiKey } from "./auth.ts";
import { fileCredentialStore } from "./credentials.ts";
import { DEFAULT_SANDBOX_SETTINGS, runRunner } from "./index.ts";

// Runs a Runner from the environment; `register` discards its own key and registers it again, and
// `login` signs in to the model's provider. A stop signal interrupts
// a Runner: it closes its connection, so the hub drops it at once, and stops its jobs. Effect's Node
// runtime logs a failure's cause and sets the exit code. The model is named as Pi names it; Pi reads
// the provider's key from the environment, or uses the sign-in `login` saved.

/**
 * What `error`, a missing or malformed setting, finds wrong with the settings, on one line, as the
 * hub and the CLI report their own: each setting it names, then the problem. Each of the three
 * keeps a copy, as the contract they share has no Effect (ADR 0022).
 */
function settingsProblems(error: Config.ConfigError): string {
  if (!Schema.isSchemaError(error.cause)) return error.message;
  const { issues } = SchemaIssue.makeFormatterStandardSchemaV1()(error.cause.issue);
  return issues
    .map((issue) => {
      const setting = getDotPath(issue);
      return setting === null ? issue.message : `${setting}: ${issue.message}`;
    })
    .join("; ");
}

/** How hard the model thinks unless `RUNNER_THINKING_LEVEL` says otherwise: Pi's coding agent's default. */
const DEFAULT_THINKING_LEVEL = "medium";

/** The file in the Runner's data directory holding its provider credentials. */
const CREDENTIALS_FILE = "auth.json";

/** The file in the Runner's data directory holding the ID a provider knows this installation by. */
const INSTALLATION_FILE = "installation_id";

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    "with-api-key": { type: "boolean" },
    login: { type: "boolean" },
    healthcheck: { type: "boolean" },
  },
});
const [command] = positionals;
if (
  positionals.length > 1 ||
  (command !== undefined &&
    !["register", "login", "logout", "auth-check", "auth-status"].includes(command)) ||
  (values["with-api-key"] && command !== "login") ||
  (values.login && command !== "auth-check") ||
  (values.healthcheck && command !== undefined && command !== "register")
) {
  console.error(
    "Usage: node dist/main.mjs [register] [--healthcheck] | login [--with-api-key] | logout | auth-check [--login] | auth-status",
  );
  process.exit(1);
}

/** Where the Runner keeps what must survive restarts. */
const dataDirSetting = Config.String("RUNNER_DATA_DIR").pipe(Config.withDefault("data"));

/** Where the Runner calls its provider's models instead of the provider's own URL, if anywhere. */
const modelBaseUrlSetting = Config.schema(
  Schema.URL.check(
    Schema.makeFilter((url) => url.protocol === "http:" || url.protocol === "https:", {
      message: "Not an http: or https: URL",
    }),
  ),
  "RUNNER_MODEL_BASE_URL",
).pipe(Config.option);

/** The Runner's settings, read from the environment by Effect's default `Config` source. */
const runnerSettings = Config.all({
  hubUrl: Config.URL("HUB_URL"),
  registrationKey: Config.Redacted("RUNNER_REGISTRATION_KEY"),
  dataDir: dataDirSetting,
  maxJobs: Config.Int("RUNNER_MAX_JOBS").pipe(Config.option),
  provider: Config.NonEmptyString("RUNNER_MODEL_PROVIDER"),
  modelId: Config.NonEmptyString("RUNNER_MODEL_ID"),
  modelBaseUrl: modelBaseUrlSetting,
  thinkingLevel: Config.Literals(MODEL_THINKING_LEVELS, "RUNNER_THINKING_LEVEL").pipe(
    Config.withDefault(DEFAULT_THINKING_LEVEL),
  ),
  // Separated as `PATH` is, by `:` (`;` on Windows).
  extraReadPaths: Config.String("RUNNER_JOB_READ_PATHS").pipe(
    Config.map((paths) => paths.split(path.delimiter).filter(Boolean)),
    Config.withDefault(DEFAULT_SANDBOX_SETTINGS.extraReadPaths),
  ),
  jobMemoryLimit: Config.NonEmptyString("RUNNER_JOB_MEMORY_LIMIT").pipe(
    Config.withDefault(DEFAULT_SANDBOX_SETTINGS.jobMemoryLimit),
  ),
  runWithoutSandbox: Config.Boolean("RUNNER_RUN_WITHOUT_SANDBOX").pipe(
    Config.withDefault(DEFAULT_SANDBOX_SETTINGS.runWithoutSandbox),
  ),
});

/**
 * The Runner refused to start, having told the operator why. It fails the program, so Effect's
 * `runMain` ends the process with a failing exit code, even with sandbox-runtime's proxies running.
 */
class Refused extends Data.TaggedError("Refused") {
  // Told already, in the operator's words, so `runMain` does not log it again.
  readonly [Runtime.errorReported] = false;
}

/** Tells the operator why the Runner cannot start, and ends it with a failing exit code. */
function refuse(message: string) {
  return Effect.sync(() => console.error(message)).pipe(Effect.andThen(Effect.fail(new Refused())));
}

/** The same Pi models and credential store serve startup and credential commands. */
function runnerAuth(dataDir: string) {
  const credentials = fileCredentialStore(path.join(dataDir, CREDENTIALS_FILE));
  const models = builtinModels({ credentials });
  return { models, auth: createRunnerAuth(models, credentials) };
}

/**
 * The ID a provider knows this installation by, such as OpenAI's agent host ID: made on the first
 * sign-in and kept, so signing in again, or registering again, stays the same installation.
 */
const loadOrCreateInstallationId = (dataDir: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const file = path.join(dataDir, INSTALLATION_FILE);
    const load = fs
      .readFileString(file)
      .pipe(Effect.flatMap(Schema.decodeUnknownEffect(installationIdSchema)));
    return yield* load.pipe(
      Effect.catchReason("PlatformError", "NotFound", () => {
        const id = randomUUID();
        return fs.writeFileString(file, id, { flag: "wx" }).pipe(
          Effect.as(id),
          // Another sign-in made it first: this one uses its ID.
          Effect.catchReason("PlatformError", "AlreadyExists", () => load),
        );
      }),
    );
  });

/** `installation_id`, the ID alone, as Codex keeps its own: Pi passes it on as a UUID. */
const installationIdSchema = Schema.Trim.check(Schema.isUUID());

/**
 * Asks the operator in the terminal, for a sign-in: the sign-in's page, opened in a browser on any
 * device, ends on a redirect to this host that a remote host never receives, so the operator pastes
 * its address here.
 */
const terminalInteraction = (): AuthInteraction => ({
  prompt: (prompt) => {
    const ask =
      prompt.type === "select"
        ? Prompt.Select({
            message: prompt.message,
            choices: prompt.options.map((option) => ({ title: option.label, value: option.id })),
          })
        : prompt.type === "secret"
          ? Prompt.Hidden({
              message: prompt.message,
              // Pi persists the returned value; an accidental empty submission must not replace
              // an existing credential. Provider-specific optional fields use text prompts.
              validate: (value) =>
                value.trim().length === 0
                  ? Effect.fail("Enter a nonempty credential.")
                  : Effect.succeed(value.trim()),
            }).pipe(Prompt.map(Redacted.value))
          : Prompt.String({ message: prompt.message });
    return Effect.runPromise(
      Prompt.run(ask).pipe(
        Effect.provide(Layer.mergeAll(NodeTerminal.layer, NodeFileSystem.layer, NodePath.layer)),
      ),
      { signal: prompt.signal },
    );
  },
  notify: (event) => {
    switch (event.type) {
      case "auth_url":
        console.log(`Open this page in a browser, on any device:\n${event.url}`);
        if (event.instructions) console.log(event.instructions);
        break;
      case "device_code":
        console.log(`Open ${event.verificationUri} and enter the code ${event.userCode}`);
        break;
      case "info":
      case "progress":
        console.log(event.message);
        break;
    }
  },
});

const terminalLayer = Layer.mergeAll(NodeTerminal.layer, NodeFileSystem.layer, NodePath.layer);
const hasTerminal = () => process.stdin.isTTY && process.stdout.isTTY;

const authSettings = Config.all({
  dataDir: dataDirSetting,
  provider: Config.NonEmptyString("RUNNER_MODEL_PROVIDER"),
});

/** The same login operation serves explicit login and confirmed first-start setup. */
function loginProvider(
  auth: ReturnType<typeof createRunnerAuth>,
  settings: { dataDir: string; provider: string; baseUrl?: URL },
  withApiKey = false,
) {
  return Effect.gen(function* () {
    if (withApiKey && process.stdin.isTTY) {
      yield* refuse("Pipe the API key to login --with-api-key, or use login for masked entry.");
    }
    if (!withApiKey && !hasTerminal()) {
      yield* refuse(
        "Interactive login needs a terminal. To save a provider API key, pipe it to agent-batch stack login --with-api-key.",
      );
    }
    const apiKey = withApiKey
      ? yield* readApiKey(process.stdin).pipe(Effect.catch((error) => refuse(error.message)))
      : undefined;
    const fs = yield* FileSystem.FileSystem;
    yield* fs.makeDirectory(settings.dataDir, { recursive: true });
    const id = yield* loadOrCreateInstallationId(settings.dataDir);
    yield* auth
      .login(settings.provider, terminalInteraction(), {
        apiKey,
        baseUrl: settings.baseUrl,
        // The provider sees this app's name. A returning sign-in reuses this installation's ID;
        // Pi otherwise creates a new client each time (earendil-works/pi#10670).
        agentName: "agent-batch",
        getDeviceId: () => id,
      })
      .pipe(
        Effect.catch((error) =>
          Terminal.isQuitError(error)
            ? refuse("Login cancelled.")
            : refuse(`Could not log in: ${error.message}`),
        ),
      );
    console.log("Authentication saved. The Runner uses it from its next model call.");
  }).pipe(
    Effect.catchTag("PlatformError", () =>
      refuse("Could not prepare the Runner's credential directory."),
    ),
    Effect.catchTag("SchemaError", () => refuse("The Runner's installation ID is invalid.")),
    Effect.provide(NodeFileSystem.layer),
  );
}

const login = Effect.gen(function* () {
  const settings = yield* authSettings;
  const baseUrl = Option.getOrUndefined(yield* modelBaseUrlSetting);
  yield* loginProvider(
    runnerAuth(settings.dataDir).auth,
    { ...settings, baseUrl },
    values["with-api-key"],
  );
});

const logout = Effect.gen(function* () {
  const { dataDir, provider } = yield* authSettings;
  yield* runnerAuth(dataDir)
    .auth.logout(provider)
    .pipe(Effect.catch(() => refuse("Could not remove saved provider authentication.")));
  console.log(
    `Saved authentication removed for "${provider}". Environment authentication, if configured, still applies.`,
  );
});

const authStatus = Effect.gen(function* () {
  const { dataDir, provider } = yield* authSettings;
  const status = yield* runnerAuth(dataDir)
    .auth.status(provider)
    .pipe(
      Effect.catch(() => refuse(`Could not inspect authentication for provider "${provider}".`)),
    );
  console.log(
    status.configured
      ? `Provider ${status.provider}: configured (${status.type}; source: ${status.source ?? "unspecified"}). Not validated.`
      : `Provider ${status.provider}: no authentication configured.`,
  );
});

const authCheck = Effect.gen(function* () {
  const settings = yield* authSettings;
  const modelId = yield* Config.NonEmptyString("RUNNER_MODEL_ID");
  const baseUrl = Option.getOrUndefined(yield* modelBaseUrlSetting);
  const { auth } = runnerAuth(settings.dataDir);
  const selection = { provider: settings.provider, modelId, baseUrl };
  let problem = yield* auth.prepareModel(selection);
  if (problem?.kind === "missing" && values.login && hasTerminal()) {
    const confirmed = yield* Prompt.run(
      Prompt.Confirm({ message: `No authentication for ${settings.provider}. Log in now?` }),
    ).pipe(
      Effect.provide(terminalLayer),
      Effect.catch(() => refuse("Login cancelled.")),
    );
    if (confirmed) {
      yield* loginProvider(auth, { ...settings, baseUrl });
      problem = yield* auth.prepareModel(selection);
    }
  }
  if (problem) yield* refuse(problem.message);
});

const run = Effect.gen(function* () {
  const settings = yield* runnerSettings;
  const { models, auth } = runnerAuth(settings.dataDir);
  const problem = yield* auth.prepareModel({
    provider: settings.provider,
    modelId: settings.modelId,
    baseUrl: Option.getOrUndefined(settings.modelBaseUrl),
  });
  if (problem) return yield* refuse(problem.message);
  const ready = yield* Latch.make(false);
  const running = runRunner({
    hubUrl: settings.hubUrl,
    registrationKey: Redacted.value(settings.registrationKey),
    dataDir: settings.dataDir,
    models,
    model: {
      provider: settings.provider,
      modelId: settings.modelId,
      thinkingLevel: settings.thinkingLevel,
    },
    maxJobs: Option.getOrUndefined(settings.maxJobs),
    sandbox: {
      extraReadPaths: settings.extraReadPaths,
      jobMemoryLimit: settings.jobMemoryLimit,
      runWithoutSandbox: settings.runWithoutSandbox,
    },
    register: command === "register",
    onReady: ready.open,
  }).pipe(
    Effect.catchTag("SandboxUnavailable", (error) => refuse(error.message)),
    Effect.catchTag("Removed", () =>
      // FIXME: re-registering belongs to the CLI. Once it exists, this message names its command
      // (such as `agent-batch runner register`) and `register` moves out of this entry point.
      refuse(
        "This Runner was removed from the hub, or the hub no longer knows it (for example after a reset). To add it again, start it once with `register`.",
      ),
    ),
  );
  if (!values.healthcheck) return yield* running;
  // Only this process owns the listener: a restart cannot inherit a stale ready signal.
  // It records startup, not ongoing provider validity or hub connectivity.
  const healthcheck = Effect.gen(function* () {
    yield* ready.await;
    const server = yield* NodeSocketServer.make({ host: "127.0.0.1", port: 8790 });
    return yield* server.run((socket) => socket.reader.pipe(Effect.asVoid, Effect.scoped));
  }).pipe(Effect.scoped);
  return yield* Effect.all([running, healthcheck], { concurrency: "unbounded", discard: true });
});

const program =
  command === "login"
    ? login
    : command === "logout"
      ? logout
      : command === "auth-check"
        ? authCheck
        : command === "auth-status"
          ? authStatus
          : run;

NodeRuntime.runMain(
  program.pipe(
    Effect.catchTag("ConfigError", (error) =>
      refuse(`The Runner is not configured: ${settingsProblems(error)}`),
    ),
  ),
);
