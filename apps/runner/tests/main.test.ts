import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { ConfigProvider, Effect, Exit, Layer, Option, Stdio } from "effect";
import { CliError } from "effect/cli";
import { describe, expect, it, onTestFinished, vi } from "vite-plus/test";

import { runnerCli } from "../src/main.ts";

// `agent-batch stack` and the Runner image call the Runner with these exact arguments. With no
// settings, a list the command line accepts stops at reading them, and one it refuses never gets
// there.

/** Runs the Runner's command line in-process on `args` with no settings, and what it printed on stderr. */
async function runner(args: string[]) {
  const stderr = vi.spyOn(console, "error").mockImplementation(() => {});
  // Help and parse errors go to stdout.
  vi.spyOn(console, "log").mockImplementation(() => {});
  onTestFinished(() => {
    vi.restoreAllMocks();
  });
  const exit = await Effect.runPromiseExit(
    runnerCli.pipe(
      Effect.provide(
        Layer.mergeAll(
          Stdio.layerTest({ args: Effect.succeed(args) }),
          ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
        ),
      ),
    ),
  );
  return { exit, stderr: stderr.mock.calls.flat().join("\n") };
}

describe("the Runner's command line", () => {
  it.for([
    ["--healthcheck"],
    ["register"],
    ["register", "--healthcheck"],
    ["login"],
    ["login", "--with-api-key"],
    ["logout"],
    ["auth-check"],
    ["auth-check", "--login"],
    ["auth-status"],
  ])("accepts %j", async (args) => {
    const { exit, stderr } = await runner(args);

    expect(Exit.isFailure(exit)).toBe(true);
    expect(stderr).toContain("The Runner is not configured");
  });

  it.for([
    ["login", "--healthcheck"],
    ["logout", "--with-api-key"],
    ["auth-check", "--with-api-key"],
    ["unknown"],
  ])("refuses %j", async (args) => {
    const { exit, stderr } = await runner(args);

    expect(CliError.isCliError(Option.getOrUndefined(Exit.findErrorOption(exit)))).toBe(true);
    expect(stderr).not.toContain("The Runner is not configured");
  });

  // The one start of the entry point itself, with no arguments, as the image starts it: its
  // refusal reaches the shell as a failing exit code.
  it("accepts [] from a started process, and exits 1", async () => {
    const main = fileURLToPath(new URL("../src/main.ts", import.meta.url));
    await expect(
      promisify(execFile)(process.execPath, [main], { env: { PATH: process.env.PATH } }),
    ).rejects.toMatchObject({
      code: 1,
      stderr: expect.stringContaining("The Runner is not configured"),
    });
  });
});
