import { parseArgs } from "node:util";

// The shared package rather than `@effect/platform-node`, whose required `redis` peer the Runner
// would install without using.
import * as NodeRuntime from "@effect/platform-node-shared/NodeRuntime";
import { Effect } from "effect";

import { runRunner } from "./index.ts";

// Runs a Runner from the environment; `register` discards its own key and registers it again. A stop
// signal interrupts it: it closes its connection, so the hub drops it at once, and stops its jobs.
// Effect's Node runtime logs a failure's cause and sets the exit code.

const { positionals } = parseArgs({ allowPositionals: true });
const [command] = positionals;
if (command !== undefined && command !== "register") {
  console.error(`Unknown command "${command}"; the only command is "register"`);
  process.exit(1);
}

const hubUrl = process.env.HUB_URL;
const registrationKey = process.env.RUNNER_REGISTRATION_KEY;
if (!hubUrl || !registrationKey) {
  console.error("Set HUB_URL and RUNNER_REGISTRATION_KEY to start a Runner");
  process.exit(1);
}

runRunner({
  hubUrl,
  registrationKey,
  dataDir: process.env.RUNNER_DATA_DIR ?? "data",
  maxJobs:
    process.env.RUNNER_MAX_JOBS === undefined ? undefined : Number(process.env.RUNNER_MAX_JOBS),
  register: command === "register",
}).pipe(
  Effect.catchTag("Removed", () =>
    Effect.sync(() => {
      // FIXME: re-registering belongs to the CLI. Once it exists, this message names its command
      // (such as `agent-batch runner register`) and `register` moves out of this entry point.
      console.error(
        "This Runner was removed from the hub, or the hub no longer knows it (for example after a reset). To add it again, start it once with `register`.",
      );
      process.exitCode = 1;
    }),
  ),
  NodeRuntime.runMain,
);
