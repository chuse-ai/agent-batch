import { writeFile } from "node:fs/promises";
import path from "node:path";

import { getCurrentSystemPrompt, getCurrentTools, type Message } from "@earendil-works/pi-ai";
import type { FauxResponseFactory } from "@earendil-works/pi-ai/providers/faux";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { type OneJob, startSystem, submitJob, untilStatus } from "./harness.ts";
import { callTool, lastToolResult, providerError, reply, untilAborted } from "./scripted-model.ts";

// What a job's agent works with and how it ends the job, driven by a scripted model: the Input files
// in its Workspace, and `submit_output`, which takes only an output matching the Output contract.

const system = startSystem();

beforeAll(async () => {
  await system.start();
  await system.startRunner();
});
afterAll(() => system.stop());

const SUBMISSION = {
  input: "Summarise the quarter.",
  files: [{ path: "q3.md", content: new File(["Revenue grew 4%."], "q3.md") }],
  outputContract: {
    schema: { type: "object", properties: { summary: { type: "string" } }, required: ["summary"] },
  },
} satisfies OneJob;

/** Submits `submission` with a new key and waits for the job to end; returns the job. */
async function runJob(submission: OneJob) {
  const client = await system.clientWithNewJobsKey();
  const { id } = await submitJob(client, submission);
  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toMatch(/^(succeeded|failed)$/);
  return client.jobs.get({ id });
}

test("the agent finds the Input files in inputs/ and submits an output read from them", async () => {
  system.model.setResponses([
    callTool("read", { path: "inputs/q3.md" }),
    (context) =>
      callTool("submit_output", { value: { summary: lastToolResult(context.messages).text } }),
  ]);

  expect(await runJob(SUBMISSION)).toMatchObject({
    status: "succeeded",
    output: { summary: expect.stringContaining("Revenue grew 4%.") },
  });
});

test("every job of a Batch is told the same instructions, the Runner's then the Batch's, before its own input", async () => {
  // Each job's messages at its first model call.
  const told: Message[][] = [];
  const submit: FauxResponseFactory = (context) => {
    told.push(context.messages);
    return callTool("submit_output", { value: { summary: "Le chiffre a crû de 4 %." } });
  };
  system.model.setResponses([submit, submit]);
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({
    agent: { instructions: "Answer in French.", outputContract: SUBMISSION.outputContract },
    breaker: false,
  });
  const inputs = ["Summarise the quarter.", "Summarise the year."];
  const { jobs } = await client.batches.addJobs({
    id: batch.id,
    jobs: inputs.map((input, index) => ({ customId: `job-${index}`, input })),
  });
  for (const { job } of jobs) await untilStatus(client, job.id, "succeeded");

  // The whole system prompt and the tools before it are the same for both jobs, so a model
  // provider can cache them across the Batch.
  const [first, second] = told.map((messages) => ({
    systemPrompt: getCurrentSystemPrompt(messages),
    tools: getCurrentTools(messages),
  }));
  expect(second).toEqual(first);
  const runners = first?.systemPrompt.indexOf("You are running a job unattended") ?? -1;
  expect(runners).toBeGreaterThan(-1);
  expect(first?.systemPrompt.indexOf("Answer in French.")).toBeGreaterThan(runners);
  expect(
    told.map((messages) => JSON.stringify(messages.find((message) => message.role === "user"))),
  ).toEqual(expect.arrayContaining(inputs.map((input) => expect.stringContaining(input))));
});

test("an output that does not match the Output contract goes back to the agent, which corrects it", async () => {
  let rejection: ReturnType<typeof lastToolResult> | undefined;
  system.model.setResponses([
    callTool("submit_output", { value: { summary: 4 } }),
    (context) => {
      rejection = lastToolResult(context.messages);
      return callTool("submit_output", { value: { summary: "Revenue grew 4%." } });
    },
  ]);

  expect(await runJob(SUBMISSION)).toMatchObject({
    status: "succeeded",
    output: { summary: "Revenue grew 4%." },
  });
  expect(rejection).toMatchObject({
    isError: true,
    text: expect.stringContaining("Output contract"),
  });
});

test("the agent can submit the output as a JSON file it wrote in its Workspace", async () => {
  system.model.setResponses([
    callTool("write", { path: "out/output.json", content: '{"summary":"Revenue grew 4%."}' }),
    callTool("submit_output", { path: "out/output.json" }),
  ]);

  expect(await runJob(SUBMISSION)).toMatchObject({
    status: "succeeded",
    output: { summary: "Revenue grew 4%." },
  });
});

test("an output nested too deeply goes back to the agent, which submits one at the limit", async () => {
  const output = JSON.parse("[".repeat(64) + "0" + "]".repeat(64));
  let rejection: ReturnType<typeof lastToolResult> | undefined;
  system.model.setResponses([
    callTool("submit_output", { value: [output] }),
    (context) => {
      rejection = lastToolResult(context.messages);
      return callTool("submit_output", { value: output });
    },
  ]);

  expect(await runJob({ ...SUBMISSION, outputContract: { schema: {} } })).toMatchObject({
    status: "succeeded",
    output,
  });
  expect(rejection).toMatchObject({
    isError: true,
    text: expect.stringContaining("JSON must not nest more than 64 containers"),
  });
});

test("an agent that stops without an accepted output fails the job", async () => {
  system.model.setResponses([reply("The quarter went well.")]);

  expect(await runJob(SUBMISSION)).toEqual(
    expect.objectContaining({ status: "failed", failure: { reason: "noOutput" } }),
  );
});

test("a model call its provider refuses fails the job with the provider's error", async () => {
  // Text that matches none of pi-ai's retry patterns, as a gateway's refusal did.
  const error = "gateway_concurrency_limit: Concurrency limit reached for this key";
  system.model.setResponses([providerError(error)]);

  expect(await runJob(SUBMISSION)).toMatchObject({
    status: "failed",
    failure: { reason: "modelFailed", message: error },
  });
});

test("reading a running job shows its model call, then its running tool call, and an ended job shows no activity", async () => {
  const { promise: answered, resolve: answer } = Promise.withResolvers<void>();
  system.model.setResponses([
    async () => {
      await answered;
      // Runs until the test writes `release` into the Workspace.
      return callTool("bash", { command: "until [ -e release ]; do sleep 0.1; done" });
    },
    callTool("submit_output", { value: { summary: "A good quarter." } }),
  ]);
  const client = await system.clientWithNewJobsKey();
  const before = Date.now();
  const { id } = await submitJob(client, SUBMISSION);

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
  const { id } = await submitJob(client, SUBMISSION);
  await expect.poll(() => calling, { timeout: 10_000 }).toBe(true);
  // The model call can start before the hub has stored the Runner's report that the job started.
  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toBe("running");

  const before = Date.now();
  await system.stopRunner();

  expect(Date.now() - before).toBeLessThan(2_000);
  expect((await client.jobs.get({ id })).status).toBe("running");
});
