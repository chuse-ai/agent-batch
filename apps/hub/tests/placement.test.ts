import { setTimeout } from "node:timers/promises";

import type { JobSubmission } from "contract";
import { afterAll, assert, beforeAll, expect, inject, test } from "vite-plus/test";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { JobId } from "../src/db/types.ts";
import { callTool, startSystem } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Say hello.",
  outputContract: {
    schema: {
      type: "object",
      properties: { greeting: { type: "string" } },
      required: ["greeting"],
    },
  },
} satisfies Omit<JobSubmission, "inputFiles">;

/** The result the scripted agent submits, which matches the Output contract. */
const RESULT = { greeting: "Hello." };

test("a job submitted while a runner is connected succeeds, and its result is read from the hub", async () => {
  const runnerId = await system.startRunner();
  await expect.poll(() => system.findRunner(runnerId)).toBeDefined();
  const client = await system.clientWithNewJobsKey();
  const before = Date.now();
  system.model.setResponses([callTool("submit_result", { value: RESULT })]);

  const { id } = await client.jobs.submit(SUBMISSION);

  await expect
    .poll(() => client.jobs.get({ id }), { timeout: 10_000 })
    .toMatchObject({ state: "succeeded", result: RESULT });
  const job = await client.jobs.get({ id });
  assert(job.startedAt !== null && job.endedAt !== null, "a succeeded job has its loop's times");
  expect(job.startedAt.getTime()).toBeGreaterThanOrEqual(before - 1_000);
  expect(job.endedAt.getTime()).toBeGreaterThanOrEqual(job.startedAt.getTime());

  // No API shows which Runner ran a job yet, so the test reads it where the hub keeps it.
  await using db = openDatabase(inject("databaseUrl"));
  expect(
    await db
      .withSchema(DB_SCHEMA)
      .selectFrom("job")
      .select(["runnerId", "generation"])
      .where("id", "=", JobId(id))
      .executeTakeFirst(),
  ).toEqual({ runnerId, generation: 1 });
});

test("a job submitted while no runner is connected stays queued, and runs once one connects", async () => {
  await system.stopRunner();
  const client = await system.clientWithNewJobsKey();
  system.model.setResponses([callTool("submit_result", { value: RESULT })]);

  const { id } = await client.jobs.submit(SUBMISSION);
  await setTimeout(1_000);
  expect((await client.jobs.get({ id })).state).toBe("queued");

  await system.startRunner();
  await expect
    .poll(() => client.jobs.get({ id }), { timeout: 10_000 })
    .toMatchObject({ state: "succeeded", result: RESULT });
});
