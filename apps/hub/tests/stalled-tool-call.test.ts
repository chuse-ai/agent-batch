import { execFile } from "node:child_process";

import type { JobSubmission } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { type HubClient, startSystem, untilStatus } from "./harness.ts";
import { callTool } from "./scripted-model.ts";

// A job whose tool call never ends, against a real hub and Runner: its daemon never answers the
// call while it keeps answering pings, as when the host refuses the daemon a thread, here because
// the agent submits its result as a FIFO nothing writes. The run timeout, a cancel and the Runner's stop must end
// the job all the same, and stop everything it left running.

const system = startSystem();

beforeAll(async () => {
  await system.start();
  await system.startRunner();
});
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Submit what the pipe holds.",
  inputFiles: {},
  resultContract: { schema: { type: "string" } },
} satisfies JobSubmission;

/**
 * Submits a job that leaves a `sleep` running, for a duration no other process uses, and then
 * submits its result from a FIFO, which never ends; waits until that call runs. Returns the job's ID and whether
 * the `sleep` still runs, as seen from outside its sandbox.
 */
async function submitStalledJob(client: HubClient, limits: { runTimeoutSeconds?: number } = {}) {
  const seconds = String(1000 + Math.floor(Math.random() * 9000));
  system.model.setResponses([
    callTool("bash", { command: `mkfifo pipe && (nohup sleep ${seconds} >/dev/null 2>&1 &)` }),
    callTool("submit_result", { path: "pipe" }),
  ]);
  const { id } = await client.jobs.submit({ ...SUBMISSION, ...limits });
  await expect
    .poll(async () => (await client.jobs.get({ id })).activity?.[0]?.tools, { timeout: 10_000 })
    .toMatchObject([{ name: "submit_result", status: "running" }]);
  // `pgrep` exits with 1 when nothing matches; any other failure tells nothing.
  const sleeping = () =>
    new Promise<boolean>((resolve, reject) =>
      execFile("pgrep", ["-f", `^sleep ${seconds}$`], (error) => {
        if (error === null) resolve(true);
        else if (error.code === 1) resolve(false);
        else reject(error);
      }),
    );
  expect(await sleeping()).toBe(true);
  return { id, sleeping };
}

test("a job whose tool call never ends fails at its run timeout, and its processes are stopped", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id, sleeping } = await submitStalledJob(client, { runTimeoutSeconds: 3 });

  await untilStatus(client, id, "failed");
  expect(await client.jobs.get({ id })).toMatchObject({ failure: { reason: "runTimedOut" } });
  await expect.poll(sleeping).toBe(false);
}, 30_000);

test("cancelling a job whose tool call never ends ends it at once, and its processes are stopped", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id, sleeping } = await submitStalledJob(client);

  await client.jobs.cancel({ id });

  await untilStatus(client, id, "cancelled", 5_000);
  await expect.poll(sleeping).toBe(false);
}, 30_000);

// Last, since it stops the Runner.
test("a Runner stopped during a tool call that never ends stops at once, and the job's processes are stopped", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id, sleeping } = await submitStalledJob(client);

  const before = Date.now();
  await system.stopRunner();

  expect(Date.now() - before).toBeLessThan(2_000);
  expect((await client.jobs.get({ id })).status).toBe("running");
  await expect.poll(sleeping).toBe(false);
}, 30_000);
