import { once } from "node:events";
import { createServer, type RequestListener } from "node:http";
import { setTimeout as sleep } from "node:timers/promises";

import { FailedToExitError } from "trpc-cli";
import { afterAll, vi } from "vite-plus/test";
import * as z from "zod/mini";

import { agentBatchCli, formatError } from "../src/index.ts";

// Effect reads the environment once, at the first setting a command reads, and keeps it for the
// process, so every run in a test file calls one hub, at one address, answering as its run asks.
let answer: RequestListener = (_request, response) => response.writeHead(404).end();
const hub = createServer((request, response) => answer(request, response));
hub.on("error", () => {});
afterAll(() => {
  hub.closeAllConnections();
  hub.close();
});
hub.listen(0, "127.0.0.1");
await once(hub, "listening");
const { port } = z.parse(z.object({ port: z.int() }), hub.address());

/** Moves the faked clock on by 100 ms, then lets 5 ms of real time pass. */
async function tick() {
  await vi.advanceTimersByTimeAsync(100);
  await sleep(5);
  return false;
}

/**
 * Runs `agent-batch` with `argv` in this process, as `src/bin.ts` runs it, in the folder `cwd`,
 * calling a hub that answers with `hub`: its exit code and what it printed. Starting the command as
 * a process takes about half a second; `packed.test.ts` starts the packed command, so what only a
 * process shows, such as its exit code reaching the shell, is covered there.
 *
 * The clock is faked, so the command's waits, such as a retry's backoff or `--wait-timeout`, pass
 * at once: from when `clockFrom` resolves, if given, until the command ends, it runs 20 times as
 * fast as real time. Not faster, as fetch's own timeouts run on the faked clock too, and must not
 * pass while the hub answers: its 10 s to connect is then half a second. Effect sleeps with
 * `setTimeout` and runs its fibers with `setImmediate`, so only `setTimeout` is faked, with
 * `clearTimeout`; `node:timers/promises` is not.
 */
export async function runCli(
  argv: readonly string[],
  options: { hub: RequestListener; cwd?: string; clockFrom?: Promise<unknown> },
) {
  answer = options.hub;
  vi.stubEnv("AGENT_BATCH_URL", `http://127.0.0.1:${port}`);
  vi.stubEnv("AGENT_BATCH_API_KEY", "key");
  const cwd = process.cwd();
  if (options.cwd !== undefined) process.chdir(options.cwd);
  const stdout: string[] = [];
  const stderr: string[] = [];
  const writes = [
    vi.spyOn(process.stdout, "write").mockImplementation((chunk) => {
      stdout.push(String(chunk));
      return true;
    }),
    vi.spyOn(process.stderr, "write").mockImplementation((chunk) => {
      stderr.push(String(chunk));
      return true;
    }),
  ];
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    const run = agentBatchCli("0.0.0")
      .run({
        argv: [...argv],
        formatError,
        prompts: false,
        // trpc-cli's own logger holds the `console` it had when loaded, out of reach of a spy.
        logger: {
          info: (...args) => stdout.push(`${args.join(" ")}\n`),
          error: (...args) => stderr.push(`${args.join(" ")}\n`),
        },
        // As `bin.ts` exits: a code the command set already, such as 124, is kept.
        process: {
          exit: (code) => {
            process.exitCode ??= code;
            // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- trpc-cli types `exit` as never returning, though the override it documents returns.
            return undefined as never;
          },
        },
      })
      .catch((error: unknown) => {
        if (!(error instanceof FailedToExitError)) throw error;
      });
    const ended = run.then(() => true);
    await options.clockFrom;
    let done = false;
    while (!done) done = await Promise.race([ended, tick()]);
    return { exitCode: process.exitCode, stdout: stdout.join(""), stderr: stderr.join("") };
  } finally {
    process.exitCode = undefined;
    vi.useRealTimers();
    for (const write of writes) write.mockRestore();
    process.chdir(cwd);
    vi.unstubAllEnvs();
    // A request the hub never answers ends with the run.
    hub.closeAllConnections();
  }
}
