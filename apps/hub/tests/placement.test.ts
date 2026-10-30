import { setTimeout } from "node:timers/promises";

import { afterAll, assert, beforeAll, expect, inject, test } from "vite-plus/test";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { JobId } from "../src/db/types.ts";
import { type OneJob, startSystem, submitJob } from "./harness.ts";
import { callTool, untilAborted } from "./scripted-model.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  input: "Say hello.",
  outputContract: {
    schema: {
      type: "object",
      properties: { greeting: { type: "string" } },
      required: ["greeting"],
    },
  },
} satisfies Omit<OneJob, "inputFiles">;

/** The output the scripted agent submits, which matches the Output contract. */
const OUTPUT = { greeting: "Hello." };

test("a job submitted while a runner is connected succeeds, and its output is read from the hub", async () => {
  const runnerId = await system.startRunner();
  await expect.poll(() => system.findRunner(runnerId)).toBeDefined();
  const client = await system.clientWithNewJobsKey();
  const before = Date.now();
  system.model.setResponses([callTool("submit_output", { value: OUTPUT })]);

  const { id } = await submitJob(client, SUBMISSION);

  await expect
    .poll(() => client.jobs.get({ id }), { timeout: 10_000 })
    .toMatchObject({ status: "succeeded", output: OUTPUT });
  const job = await client.jobs.get({ id });
  assert(job.startedAt !== null && job.endedAt !== null, "a succeeded job has its loop's times");
  expect(job.startedAt.getTime()).toBeGreaterThanOrEqual(before - 1_000);
  expect(job.endedAt.getTime()).toBeGreaterThanOrEqual(job.startedAt.getTime());
  // The scripted model counts tokens but prices none, so only the tokens show its call.
  expect(job.usage.totalTokens).toBeGreaterThan(0);

  // No API shows which Runner ran a job yet, so the test reads it where the hub keeps it.
  await using db = openDatabase(inject("databaseUrl"));
  expect(
    await db
      .withSchema(DB_SCHEMA)
      .selectFrom("job")
      .select(["runnerId", "runId"])
      .where("id", "=", JobId(id))
      .executeTakeFirst(),
  ).toEqual({ runnerId, runId: expect.any(String) });
});

test("a job submitted while no runner is connected stays queued, and runs once one connects", async () => {
  await system.stopRunner();
  const client = await system.clientWithNewJobsKey();
  system.model.setResponses([callTool("submit_output", { value: OUTPUT })]);

  const { id } = await submitJob(client, SUBMISSION);
  await setTimeout(1_000);
  expect((await client.jobs.get({ id })).status).toBe("queued");

  await system.startRunner();
  await expect
    .poll(() => client.jobs.get({ id }), { timeout: 10_000 })
    .toMatchObject({ status: "succeeded", output: OUTPUT });
});

test("a Runner started again with fewer slots than its jobs recovers only as many, and is given no new job meanwhile", async () => {
  await system.stopRunner();
  const client = await system.clientWithNewJobsKey();
  // The scripted model counts every call it gets, including those beyond its scripted responses.
  const calls = () => system.model.state.callCount;
  const before = calls();
  system.model.setResponses([untilAborted, untilAborted]);
  await system.startRunner({ maxJobs: 2 });
  const first = await submitJob(client, SUBMISSION);
  const second = await submitJob(client, SUBMISSION);
  await expect.poll(() => calls() - before, { timeout: 10_000 }).toBe(2);
  const third = await submitJob(client, SUBMISSION);
  await system.stopRunner();

  // The job recovered first submits its output only once the test, having checked the rest,
  // releases it.
  const { promise: released, resolve: release } = Promise.withResolvers<void>();
  const restarted = calls();
  system.model.setResponses([
    async () => {
      await released;
      return callTool("submit_output", { value: OUTPUT });
    },
    callTool("submit_output", { value: OUTPUT }),
    callTool("submit_output", { value: OUTPUT }),
  ]);
  await system.startRunner({ maxJobs: 1 });
  await expect.poll(() => calls() - restarted, { timeout: 10_000 }).toBe(1);
  await setTimeout(1_000);
  expect(calls() - restarted).toBe(1);
  await using db = openDatabase(inject("databaseUrl"));
  expect(
    await db
      .withSchema(DB_SCHEMA)
      .selectFrom("job")
      .select("runnerId")
      .where("id", "=", JobId(third.id))
      .executeTakeFirst(),
  ).toEqual({ runnerId: null });

  release();
  for (const { id } of [first, second, third]) {
    await expect
      .poll(() => client.jobs.get({ id }), { timeout: 10_000 })
      .toMatchObject({ status: "succeeded", output: OUTPUT });
  }
}, 30_000);
