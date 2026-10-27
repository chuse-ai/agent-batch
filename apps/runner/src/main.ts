import { randomUUID } from "node:crypto";
import path from "node:path";
import { parseArgs } from "node:util";

import { type AuthInteraction, type Models, ModelsError } from "@earendil-works/pi-ai";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { MODEL_THINKING_LEVELS } from "@earendil-works/pi-ai/providers/model-schema";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import * as NodePath from "@effect/platform-node-shared/NodePath";
// The shared package rather than `@effect/platform-node`, whose required `redis` peer the Runner
// would install without using.
import * as NodeRuntime from "@effect/platform-node-shared/NodeRuntime";
import * as NodeTerminal from "@effect/platform-node-shared/NodeTerminal";
import { getDotPath } from "@standard-schema/utils";
import {
  Config,
  Data,
  Effect,
  FileSystem,
  Layer,
  Option,
  Redacted,
  Runtime,
  Schema,
  SchemaIssue,
  Terminal,
} from "effect";
import * as Prompt from "effect/cli/Prompt";
import ensureError from "ensure-error";

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

/** The file in the Runner's data directory holding its sign-ins' tokens. */
const CREDENTIALS_FILE = "auth.json";

/**
 * Why a sign-in is not used with `RUNNER_MODEL_BASE_URL`: its tokens would go to whatever serves
 * that URL, and Pi shapes a sign-in's requests for the provider's own endpoint, such as ChatGPT's,
 * which it recognizes by its URL.
 */
const SIGN_IN_WITH_BASE_URL =
  "RUNNER_MODEL_BASE_URL works with the provider's key only, not with a sign-in or a subscription's token, which would go to that URL. Unset it, or delete the sign-in (auth.json in the data directory) or unset ANTHROPIC_OAUTH_TOKEN, and give the provider's key in the environment.";

/** The file in the Runner's data directory holding the ID a provider knows this installation by. */
const INSTALLATION_FILE = "installation_id";

/**
 * Why the Runner cannot run jobs on the model, if it cannot: Pi does not know it, has no key or
 * sign-in for it, or cannot renew the sign-in. Renews an expired sign-in and saves the new tokens:
 * `getAuth` rather than `checkAuth`, which finds a sign-in stored without renewing it, so a sign-in
 * that can no longer be renewed stops the Runner here rather than failing its jobs.
 */
async function renewModelAuthProblem(models: Models, provider: string, modelId: string) {
  if (!models.getModel(provider, modelId)) {
    return withAvailable(
      models,
      `Pi does not know the model "${modelId}" of provider "${provider}"`,
    );
  }
  const auth = await models.getAuth(provider).catch(ensureError);
  // A renewal also fails while the provider cannot be reached, so the operator is told why it
  // failed, not that the sign-in is gone.
  if (auth instanceof ModelsError && auth.code === "oauth") {
    return `Pi cannot renew the sign-in to provider "${provider}": ${auth.message}. If the provider no longer accepts it, sign in again with \`login\`.`;
  }
  if (auth instanceof Error)
    return `Pi cannot authenticate to provider "${provider}": ${auth.message}`;
  if (!auth) {
    return withAvailable(
      models,
      `Pi finds no key for provider "${provider}" in the environment, and the Runner has not signed in to it with \`login\``,
    );
  }
  return undefined;
}

/** `problem`, followed by the models the Runner could run on instead. */
async function withAvailable(models: Models, problem: string) {
  const available = (await models.getAvailable()).map((model) => `${model.provider} ${model.id}`);
  return available.length > 0
    ? `${problem}. Models with a key or sign-in: ${available.join(", ")}`
    : `${problem}. No provider has a key in the environment or a sign-in.`;
}

const { positionals } = parseArgs({ allowPositionals: true });
const [command] = positionals;
if (command !== undefined && command !== "register" && command !== "login") {
  console.error(`Unknown command "${command}"; the commands are "register" and "login"`);
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

/**
 * The models Pi knows, with the keys in the environment and the sign-ins in `dataDir`. With
 * `baseUrl`, the provider `provider`'s models are called there, as Pi's coding agent calls them for
 * a provider's `baseUrl` in its `models.json`, and with its key alone: the provider then takes no
 * sign-in, not even one saved while the Runner runs, so its calls fail rather than take a sign-in's
 * tokens to `baseUrl`.
 */
function runnerModels(dataDir: string, baseUrl?: { provider: string; url: URL }) {
  const models = builtinModels({
    credentials: fileCredentialStore(path.join(dataDir, CREDENTIALS_FILE)),
  });
  const provider = baseUrl && models.getProvider(baseUrl.provider);
  const apiKey = provider?.auth.apiKey;
  if (baseUrl && provider && apiKey) {
    const url = baseUrl.url.href;
    const atUrl = <M extends { baseUrl: string }>(model: M): M => ({ ...model, baseUrl: url });
    models.setProvider({
      ...provider,
      baseUrl: url,
      auth: { apiKey },
      getModels: () => provider.getModels().map(atUrl),
      getAllModels: provider.getAllModels && (() => provider.getAllModels?.().map(atUrl) ?? []),
    });
  }
  return models;
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
const terminalInteraction: AuthInteraction = {
  prompt: (prompt) => {
    const ask =
      prompt.type === "select"
        ? Prompt.Select({
            message: prompt.message,
            choices: prompt.options.map((option) => ({ title: option.label, value: option.id })),
          })
        : prompt.type === "secret"
          ? Prompt.Hidden({ message: prompt.message }).pipe(Prompt.map(Redacted.value))
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
};

/** Signs in to the model's provider, as `login` does, and saves the sign-in in the data directory. */
const login = Effect.gen(function* () {
  const { dataDir, provider, modelBaseUrl } = yield* Config.all({
    dataDir: dataDirSetting,
    provider: Config.NonEmptyString("RUNNER_MODEL_PROVIDER"),
    modelBaseUrl: modelBaseUrlSetting,
  });
  // A refusal fails the program, so nothing after it runs.
  if (Option.isSome(modelBaseUrl)) yield* refuse(SIGN_IN_WITH_BASE_URL);
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(dataDir, { recursive: true }).pipe(Effect.orDie);
  const id = yield* loadOrCreateInstallationId(dataDir).pipe(Effect.orDie);
  // OpenAI's Sign in with ChatGPT Terms ask for express consent before background use.
  console.log(
    `The Runner will make its jobs' model calls on this sign-in in the background, without asking each time. Run only your own jobs on it.`,
  );
  yield* Effect.tryPromise((signal) =>
    runnerModels(dataDir).login(
      provider,
      "oauth",
      { ...terminalInteraction, signal },
      // OpenAI's sign-in guide asks a returning sign-in to reuse the client it issued; Pi registers a
      // new one each time (earendil-works/pi#10670).
      {
        // The guide also asks an app to sign in under its own name, not Pi's.
        agentName: "agent-batch",
        getDeviceId: () => id,
      },
    ),
  ).pipe(
    Effect.catch((error) =>
      // Esc or Ctrl-C at a prompt, which the terminal reads in raw mode rather than as a signal.
      Terminal.isQuitError(error.cause)
        ? refuse("Sign-in cancelled.")
        : refuse(`Could not sign in: ${ensureError(error.cause).message}`),
    ),
  );
  console.log("Signed in. The Runner uses the sign-in from its next model call, running or not.");
}).pipe(
  Effect.catchTag("ConfigError", (error) =>
    refuse(`The sign-in is not configured: ${settingsProblems(error)}`),
  ),
  Effect.provide(NodeFileSystem.layer),
);

const run = Effect.gen(function* () {
  const settings = yield* runnerSettings;
  const baseUrl = Option.getOrUndefined(settings.modelBaseUrl);
  // Pi calls Azure OpenAI at `AZURE_OPENAI_BASE_URL`, or the URL of `AZURE_OPENAI_RESOURCE_NAME`,
  // ahead of a model's own URL, so the setting would be passed over whenever either is set.
  if (baseUrl && settings.provider === "azure") {
    return yield* refuse(
      "RUNNER_MODEL_BASE_URL is not used with Azure OpenAI: set AZURE_OPENAI_BASE_URL to call its models at another URL.",
    );
  }
  // A sign-in is looked for among all of the provider's auth, as the models with `baseUrl` take
  // none and would only report the provider unconfigured.
  if (
    baseUrl &&
    (yield* Effect.promise(() => runnerModels(settings.dataDir).checkAuth(settings.provider)))
      ?.type === "oauth"
  ) {
    return yield* refuse(SIGN_IN_WITH_BASE_URL);
  }
  const models = runnerModels(
    settings.dataDir,
    baseUrl && { provider: settings.provider, url: baseUrl },
  );
  const problem = yield* Effect.promise(() =>
    renewModelAuthProblem(models, settings.provider, settings.modelId),
  );
  if (problem) return yield* refuse(problem);
  // Pi takes a Claude subscription's token from this variable as it takes a key, ahead of
  // `ANTHROPIC_API_KEY`, and names it as the source.
  if (
    baseUrl &&
    (yield* Effect.promise(() => models.getAuth(settings.provider)))?.source ===
      "ANTHROPIC_OAUTH_TOKEN"
  ) {
    return yield* refuse(SIGN_IN_WITH_BASE_URL);
  }
  return yield* runRunner({
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
}).pipe(
  Effect.catchTag("ConfigError", (error) =>
    refuse(`The Runner is not configured: ${settingsProblems(error)}`),
  ),
);

if (command === "login") NodeRuntime.runMain(login);
else NodeRuntime.runMain(run);
