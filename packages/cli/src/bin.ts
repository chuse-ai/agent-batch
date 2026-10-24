#!/usr/bin/env node
import { FailedToExitError } from "trpc-cli";

import packageJson from "../package.json" with { type: "json" };
import { agentBatchCli, formatError, readInput } from "./index.ts";

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
  await agentBatchCli(packageJson.version).run({
    argv,
    formatError,
    prompts: false,
    // trpc-cli exits as soon as a command ends, cutting off what a pipe has not yet taken of its
    // output (mmkal/trpc-cli#228). Setting the exit code instead, through trpc-cli's documented
    // override, lets Node exit once stdout and stderr have drained, as its docs for
    // `process.exit()` advise; trpc-cli then throws `FailedToExitError`. A write that has already
    // failed keeps its exit code.
    process: {
      exit: (code) => {
        process.exitCode ??= code;
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- trpc-cli types `exit` as never returning, though the override it documents returns.
        return undefined as never;
      },
    },
  });
} catch (error) {
  if (!(error instanceof FailedToExitError)) {
    console.error(formatError(error));
    process.exitCode = 1;
  }
}
