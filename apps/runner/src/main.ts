import { parseArgs } from "node:util";

import { startRunner } from "./index.ts";

// Starts a Runner from the environment; `register` discards its own key and registers it again. A
// stop signal closes its connection, so the hub drops it at once, and the process exits when nothing
// is left to run; a second signal exits immediately.

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

const runner = await startRunner({
  hubUrl,
  registrationKey,
  dataDir: process.env.RUNNER_DATA_DIR ?? "data",
  register: command === "register",
  onRemoved: () => {
    // FIXME: re-registering belongs to the CLI. Once it exists, this message names its command
    // (such as `agent-batch runner register`) and `register` moves out of this entry point.
    console.error(
      "This Runner was removed from the hub, or the hub no longer knows it (for example after a reset). To add it again, start it once with `register`.",
    );
    process.exitCode = 1;
  },
});

let stopping = false;
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    if (stopping) process.exit(1);
    stopping = true;
    void runner.stop();
  });
}
