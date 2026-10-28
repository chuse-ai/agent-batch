import { execFile } from "node:child_process";

import { describe, expect, it } from "vite-plus/test";

const main = new URL("../src/main.ts", import.meta.url).pathname;

/** Runs the Runner's entry point with no settings, so a command that parses stops at its settings. */
function runner(args: string[]) {
  return new Promise<{ exitCode: number | string | null | undefined; stderr: string }>((resolve) =>
    execFile(
      process.execPath,
      [main, ...args],
      { env: { PATH: process.env.PATH } },
      (error, _, stderr) => resolve({ exitCode: error ? error.code : 0, stderr }),
    ),
  );
}

// `agent-batch stack` and the Runner image call the Runner with these exact arguments.
describe.concurrent("the Runner's command line", () => {
  it.for([
    [],
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
    expect(await runner(args)).toEqual({
      exitCode: 1,
      stderr: expect.stringContaining("The Runner is not configured"),
    });
  });

  it.for([
    ["login", "--healthcheck"],
    ["logout", "--with-api-key"],
    ["auth-check", "--with-api-key"],
    ["unknown"],
  ])("refuses %j", async (args) => {
    const { exitCode, stderr } = await runner(args);
    expect(exitCode).toBe(1);
    expect(stderr).not.toContain("The Runner is not configured");
  });
});
