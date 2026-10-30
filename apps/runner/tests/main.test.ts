import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { it } from "@effect/vitest";
import { ConfigProvider, Effect, Exit, Layer, Option, Stdio } from "effect";
import { CliError } from "effect/cli";
import { describe, expect, onTestFinished, vi } from "vite-plus/test";

import { runnerCli } from "../src/main.ts";

// `agent-batch stack` and the Runner image call the Runner with these exact arguments. With no
// settings, a list the command line accepts stops at reading them, and one it refuses never gets
// there.

/** Runs the Runner's command line in-process on `args` with no settings, and what it printed on stderr. */
function runner(args: string[]) {
  return Effect.gen(function* () {
    const stderr = vi.spyOn(console, "error").mockImplementation(() => {});
    // Help goes to stdout; parse errors go to stderr.
    vi.spyOn(console, "log").mockImplementation(() => {});
    onTestFinished(() => {
      vi.restoreAllMocks();
    });
    const exit = yield* runnerCli.pipe(
      Effect.provide(
        Layer.mergeAll(
          Stdio.layerTest({ args: Effect.succeed(args) }),
          ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
        ),
      ),
      Effect.exit,
    );
    return { exit, stderr: stderr.mock.calls.flat().join("\n") };
  });
}

describe("the Runner's command line", () => {
  it.live.each([
    { args: ["--healthcheck"] },
    { args: ["register"] },
    { args: ["register", "--healthcheck"] },
    { args: ["login"] },
    { args: ["login", "--with-api-key"] },
    { args: ["logout"] },
    { args: ["auth-check"] },
    { args: ["auth-check", "--login"] },
    { args: ["auth-status"] },
  ])("accepts $args", ({ args }) =>
    Effect.gen(function* () {
      const { exit, stderr } = yield* runner(args);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(stderr).toContain("The Runner is not configured");
    }),
  );

  it.live.each([
    { args: ["login", "--healthcheck"] },
    { args: ["logout", "--with-api-key"] },
    { args: ["auth-check", "--with-api-key"] },
    { args: ["unknown"] },
  ])("refuses $args", ({ args }) =>
    Effect.gen(function* () {
      const { exit, stderr } = yield* runner(args);

      expect(CliError.isCliError(Option.getOrUndefined(Exit.findErrorOption(exit)))).toBe(true);
      expect(stderr).not.toContain("The Runner is not configured");
    }),
  );

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
