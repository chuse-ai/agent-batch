import type { JobSubmission } from "contract";
import { afterAll, assert, beforeAll, expect, inject, test } from "vite-plus/test";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { JobId } from "../src/db/types.ts";
import { startSystem } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

/** A job whose Output contract the stub agent's result, the prompt echoed, satisfies. */
const SUBMISSION = {
  prompt: "Say hello.",
  outputContract: {
    schema: { type: "object", properties: { echo: { type: "string" } }, required: ["echo"] },
  },
} satisfies Omit<JobSubmission, "inputFiles">;

/** A client sending a new API key allowed to submit and read jobs. */
async function jobsClient() {
  const { key } = await system.owner().apiKeys.create({
    name: "jobs",
    permissions: { job: ["create", "read"] },
  });
  return system.client({ "x-api-key": key });
}

test("a job submitted while a runner is connected succeeds, and its result is read from the hub", async () => {
  const runnerId = await system.startRunner();
  await expect.poll(() => system.findRunner(runnerId)).toBeDefined();
  const client = await jobsClient();
  const before = Date.now();

  const { id } = await client.jobs.submit(SUBMISSION);

  await expect
    .poll(() => client.jobs.get({ id }), { timeout: 10_000 })
    .toMatchObject({
      state: "succeeded",
      result: { echo: SUBMISSION.prompt },
    });
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
  const client = await jobsClient();

  const { id } = await client.jobs.submit(SUBMISSION);
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  expect((await client.jobs.get({ id })).state).toBe("queued");

  await system.startRunner();
  await expect
    .poll(() => client.jobs.get({ id }), { timeout: 10_000 })
    .toMatchObject({ state: "succeeded", result: { echo: SUBMISSION.prompt } });
});
