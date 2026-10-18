import { parseArgs } from "node:util";

import type { Models } from "@earendil-works/pi-ai";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { MODEL_THINKING_LEVELS } from "@earendil-works/pi-ai/providers/model-schema";
// The shared package rather than `@effect/platform-node`, whose required `redis` peer the Runner
// would install without using.
import * as NodeRuntime from "@effect/platform-node-shared/NodeRuntime";
import { Config, Effect, Option, Redacted } from "effect";

import { runRunner } from "./index.ts";

// Runs a Runner from the environment; `register` discards its own key and registers it again. A stop
// signal interrupts it: it closes its connection, so the hub drops it at once, and stops its jobs.
// Effect's Node runtime logs a failure's cause and sets the exit code. The model is named as Pi names
// it, and Pi reads the provider's key from the environment.

/** How hard the model thinks unless `RUNNER_THINKING_LEVEL` says otherwise: Pi's coding agent's default. */
const DEFAULT_THINKING_LEVEL = "medium";

/** Why the Runner cannot run jobs on the model, if it cannot: Pi does not know it, or has no key for it. */
async function modelProblem(models: Models, provider: string, modelId: string) {
  const problem = !models.getModel(provider, modelId)
    ? `Pi does not know the model "${modelId}" of provider "${provider}"`
    : !(await models.checkAuth(provider))
      ? `Pi finds no key for provider "${provider}" in the environment`
      : undefined;
  if (!problem) return undefined;
  const available = (await models.getAvailable()).map((model) => `${model.provider} ${model.id}`);
  return available.length > 0
    ? `${problem}. Models with a key: ${available.join(", ")}`
    : `${problem}. No provider has a key in the environment.`;
}

const { positionals } = parseArgs({ allowPositionals: true });
const [command] = positionals;
if (command !== undefined && command !== "register") {
  console.error(`Unknown command "${command}"; the only command is "register"`);
  process.exit(1);
}

/** The Runner's settings, read from the environment by Effect's default `Config` source. */
const runnerSettings = Config.all({
  hubUrl: Config.URL("HUB_URL"),
  registrationKey: Config.Redacted("RUNNER_REGISTRATION_KEY"),
  dataDir: Config.String("RUNNER_DATA_DIR").pipe(Config.withDefault("data")),
  maxJobs: Config.Int("RUNNER_MAX_JOBS").pipe(Config.option),
  provider: Config.NonEmptyString("RUNNER_MODEL_PROVIDER"),
  modelId: Config.NonEmptyString("RUNNER_MODEL_ID"),
  thinkingLevel: Config.Literals(MODEL_THINKING_LEVELS, "RUNNER_THINKING_LEVEL").pipe(
    Config.withDefault(DEFAULT_THINKING_LEVEL),
  ),
});

/** Tells the operator why the Runner cannot start, and ends it with a failing exit code. */
function refuse(message: string) {
  return Effect.sync(() => {
    console.error(message);
    process.exitCode = 1;
  });
}

Effect.gen(function* () {
  const settings = yield* runnerSettings;
  const models = builtinModels();
  const problem = yield* Effect.promise(() =>
    modelProblem(models, settings.provider, settings.modelId),
  );
  if (problem) return yield* refuse(problem);
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
    register: command === "register",
  }).pipe(
    Effect.catchTag("Removed", () =>
      // FIXME: re-registering belongs to the CLI. Once it exists, this message names its command
      // (such as `agent-batch runner register`) and `register` moves out of this entry point.
      refuse(
        "This Runner was removed from the hub, or the hub no longer knows it (for example after a reset). To add it again, start it once with `register`.",
      ),
    ),
  );
}).pipe(
  // A missing or malformed setting names itself, on one line as the hub reports its own.
  Effect.catchTag("ConfigError", (error) =>
    refuse(`The Runner is not configured: ${error.cause.message.replace(/\s+/g, " ")}`),
  ),
  NodeRuntime.runMain,
);
