#!/usr/bin/env node
import { FailedToExitError } from "trpc-cli";

import packageJson from "../package.json" with { type: "json" };
import { formatError, readInput, runAgentBatchCli } from "./index.ts";

// A reader that stops reading (`EPIPE`) has taken what it wanted, so the command ends quietly. Any
// other failed write, as to a full disk, fails the command; without a listener, the stream's
// `error` event would end the process with it.
for (const stream of [process.stdout, process.stderr]) {
  stream.on("error", (error: NodeJS.ErrnoException) => {
    if (error.code === "EPIPE") return;
    process.exitCode = 1;
    if (stream === process.stdout)
      console.error(`The output could not be written: ${error.message}`);
  });
}

try {
  const argv = await readInput(process.argv.slice(2));
  await runAgentBatchCli(packageJson.version, argv);
} catch (error) {
  if (!(error instanceof FailedToExitError)) {
    console.error(formatError(error));
    process.exitCode = 1;
  }
}
