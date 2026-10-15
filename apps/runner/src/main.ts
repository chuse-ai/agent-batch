import { startRunner } from "./index.ts";

// Starts a Runner from the environment. A stop signal closes its connection, so the hub drops it at
// once, and the process exits when nothing is left to run; a second signal exits immediately.

const hubUrl = process.env.HUB_URL;
const runnerId = process.env.RUNNER_ID;
if (!hubUrl || !runnerId) {
  console.error("Set HUB_URL and RUNNER_ID to start a Runner");
  process.exit(1);
}

const runner = startRunner({ hubUrl, runnerId });

let stopping = false;
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    if (stopping) process.exit(1);
    stopping = true;
    void runner.stop();
  });
}
