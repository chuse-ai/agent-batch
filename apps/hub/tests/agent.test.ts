import { writeFile } from "node:fs/promises";
import path from "node:path";

import type { JobSubmission } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { startSystem } from "./harness.ts";
import { callTool, lastToolResult, reply, untilAborted } from "./scripted-model.ts";

// What a job's agent works with and how it ends the job, driven by a scripted model: the Input files
// in its Workspace, and `submit_result`, which takes only a result matching the Result contract.

const system = startSystem();

beforeAll(async () => {
  await system.start();
  await system.startRunner();
});
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Summarise the quarter.",
  inputFiles: { "q3.md": { content: "Revenue grew 4%." } },
  resultContract: {
    schema: { type: "object", properties: { summary: { type: "string" } }, required: ["summary"] },
  },
} satisfies JobSubmission;

/** Submits `submission` with a new key and waits for the job to end; returns the job. */
async function runJob(submission: JobSubmission) {
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(submission);
  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toMatch(/^(succeeded|failed)$/);
  return client.jobs.get({ id });
}

test("the agent finds the Input files in inputs/ and submits a result read from them", async () => {
  system.model.setResponses([
    callTool("read", { path: "inputs/q3.md" }),
    (context) =>
      callTool("submit_result", { value: { summary: lastToolResult(context.messages).text } }),
  ]);

  expect(await runJob(SUBMISSION)).toMatchObject({
    status: "succeeded",
    result: { summary: expect.stringContaining("Revenue grew 4%.") },
  });
});

test("a result that does not match the Result contract goes back to the agent, which corrects it", async () => {
  let rejection: ReturnType<typeof lastToolResult> | undefined;
  system.model.setResponses([
    callTool("submit_result", { value: { summary: 4 } }),
    (context) => {
      rejection = lastToolResult(context.messages);
      return callTool("submit_result", { value: { summary: "Revenue grew 4%." } });
    },
  ]);

  expect(await runJob(SUBMISSION)).toMatchObject({
    status: "succeeded",
    result: { summary: "Revenue grew 4%." },
  });
  expect(rejection).toMatchObject({
    isError: true,
    text: expect.stringContaining("Result contract"),
  });
});

test("the agent can submit the result as a JSON file it wrote in its Workspace", async () => {
  system.model.setResponses([
    callTool("write", { path: "out/result.json", content: '{"summary":"Revenue grew 4%."}' }),
    callTool("submit_result", { path: "out/result.json" }),
  ]);

  expect(await runJob(SUBMISSION)).toMatchObject({
    status: "succeeded",
    result: { summary: "Revenue grew 4%." },
  });
});

test("an agent that stops without an accepted result fails the job", async () => {
  system.model.setResponses([reply("The quarter went well.")]);

  expect((await runJob(SUBMISSION)).status).toBe("failed");
});

test("reading a running job shows its model call, then its running tool call, and an ended job shows no activity", async () => {
  const { promise: answered, resolve: answer } = Promise.withResolvers<void>();
  system.model.setResponses([
    async () => {
      await answered;
      // Runs until the test writes `release` into the Workspace.
      return callTool("bash", { command: "until [ -e release ]; do sleep 0.1; done" });
    },
    callTool("submit_result", { value: { summary: "A good quarter." } }),
  ]);
  const client = await system.clientWithNewJobsKey();
  const before = Date.now();
  const { id } = await client.jobs.submit(SUBMISSION);

  await expect
    .poll(async () => (await client.jobs.get({ id })).activity, { timeout: 10_000 })
    .toEqual([
      {
        conversationId: expect.any(Number),
        generation: { attempt: 1, changedAt: expect.any(String) },
        tools: [],
        compactions: [],
      },
    ]);
  const { activity } = await client.jobs.get({ id });
  const changedAt = Date.parse(activity?.[0]?.generation?.changedAt ?? "");
  expect(changedAt).toBeGreaterThanOrEqual(before);
  expect(changedAt).toBeLessThanOrEqual(Date.now());

  answer();
  await expect
    .poll(async () => (await client.jobs.get({ id })).activity, { timeout: 10_000 })
    .toEqual([
      {
        conversationId: expect.any(Number),
        tools: [
          {
            callId: expect.any(String),
            name: "bash",
            status: "running",
            changedAt: expect.any(String),
          },
        ],
        compactions: [],
      },
    ]);

  await writeFile(path.join(system.workspace(id), "release"), "");
  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toBe("succeeded");
  expect(await client.jobs.get({ id })).not.toHaveProperty("activity");
}, 30_000);

// Last, since it stops the Runner.
test("a runner stopped during a job's model call stops at once, leaving the job running", async () => {
  let calling = false;
  system.model.setResponses([
    (...args) => {
      calling = true;
      return untilAborted(...args);
    },
  ]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await expect.poll(() => calling, { timeout: 10_000 }).toBe(true);

  const before = Date.now();
  await system.stopRunner();

  expect(Date.now() - before).toBeLessThan(2_000);
  expect((await client.jobs.get({ id })).status).toBe("running");
});
