import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { describe, expect, it } from "vite-plus/test";

const main = fileURLToPath(new URL("../src/main.ts", import.meta.url));

/** Runs the Runner's entry point with no settings, so a command that parses stops at its settings. */
const runner = (args: string[]) =>
  promisify(execFile)(process.execPath, [main, ...args], { env: { PATH: process.env.PATH } });

// `agent-batch stack` and the Runner image call the Runner with these exact arguments. Each case
// starts a Node process, all at once, while CI's two vCPUs also run the CLI's tests: on CI a case
// takes 2 to 5 s, so the default 5 s limit is too tight.
describe.concurrent("the Runner's command line", { timeout: 20_000 }, () => {
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
    await expect(runner(args)).rejects.toMatchObject({
      code: 1,
      stderr: expect.stringContaining("The Runner is not configured"),
    });
  });

  it.for([
    ["login", "--healthcheck"],
    ["logout", "--with-api-key"],
    ["auth-check", "--with-api-key"],
    ["unknown"],
  ])("refuses %j", async (args) => {
    await expect(runner(args)).rejects.toMatchObject({
      code: 1,
      stderr: expect.not.stringContaining("The Runner is not configured"),
    });
  });
});
