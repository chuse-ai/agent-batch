import { ConfigProvider } from "effect";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { hubSettings, readConfig } from "../src/settings.ts";
import { type OneJob, startSystem, submitJob, untilStatus } from "./harness.ts";
import { callTool, untilAborted } from "./scripted-model.ts";

// A job's limits, against a real hub and Runner: a run timeout and a model budget, the hub's
// defaults unless the job gives its own and refused above the hub's maximums: limits that fail a
// job that passes them, and that a resume can renew to carry the job on.

const system = startSystem({
  hubVars: {
    JOB_RUN_TIMEOUT_SECONDS_MAX: "3600",
    JOB_MODEL_BUDGET_USD_MAX: "5",
  },
});

beforeAll(async () => {
  await system.start();
  await system.startRunner();
});
afterAll(() => system.stop());

const SUBMISSION = {
  input: "Count to a million.",
  files: [],
  outputContract: { schema: { type: "integer" } },
} satisfies OneJob;

test("a job without limits gets the hub's defaults, and one above its maximums is refused", async () => {
  const client = await system.clientWithNewJobsKey();
  system.model.setResponses([callTool("submit_output", { value: 1_000_000 })]);
  const job = await submitJob(client, SUBMISSION);
  expect(job).toMatchObject({ runTimeoutSeconds: 30 * 60, modelBudgetUsd: 1 });
  // Its model call is made before the next test gives the model other responses.
  await untilStatus(client, job.id, "succeeded");
  await expect(
    submitJob(client, { ...SUBMISSION, runTimeoutSeconds: 2 * 60 * 60 }),
  ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  await expect(submitJob(client, { ...SUBMISSION, modelBudgetUsd: 5.01 })).rejects.toMatchObject({
    code: "BAD_REQUEST",
  });
});

test("a job whose run passes its timeout fails, and a resume with a longer one carries it on", async () => {
  const client = await system.clientWithNewJobsKey();
  system.model.setResponses([untilAborted]);
  const { id } = await submitJob(client, { ...SUBMISSION, runTimeoutSeconds: 1 });
  await untilStatus(client, id, "failed");
  expect(await client.jobs.get({ id })).toMatchObject({
    failure: { reason: "runTimedOut" },
    runTimeoutSeconds: 1,
  });

  system.model.setResponses([callTool("submit_output", { value: 1_000_000 })]);
  expect(await client.jobs.resume({ id, runTimeoutSeconds: 10 * 60 })).toMatchObject({
    status: "queued",
    runTimeoutSeconds: 10 * 60,
  });
  await untilStatus(client, id, "succeeded");
  const succeeded = await client.jobs.get({ id });
  expect(succeeded).toMatchObject({ output: 1_000_000 });
  expect(succeeded).not.toHaveProperty("failure");
});

test("a resume repeating a running job's limits returns it, and one changing them is refused", async () => {
  const client = await system.clientWithNewJobsKey();
  system.model.setResponses([untilAborted]);
  const { id } = await submitJob(client, { ...SUBMISSION, runTimeoutSeconds: 600 });
  expect(await client.jobs.resume({ id, runTimeoutSeconds: 600, modelBudgetUsd: 1 })).toMatchObject(
    { runTimeoutSeconds: 600, modelBudgetUsd: 1 },
  );
  await expect(client.jobs.resume({ id, runTimeoutSeconds: 601 })).rejects.toMatchObject({
    code: "CONFLICT",
  });
  await client.jobs.cancel({ id });
  await untilStatus(client, id, "cancelled");
});

test("a job whose model cost reaches its budget fails, and only a higher budget carries it on", async () => {
  const client = await system.clientWithNewJobsKey();
  // The first call costs more than the budget, so the agent's next request is never made.
  system.model.setResponses([callTool("bash", { command: "true" })]);
  const { id } = await submitJob(client, { ...SUBMISSION, modelBudgetUsd: 0.000001 });
  await untilStatus(client, id, "failed");
  const failed = await client.jobs.get({ id });
  expect(failed).toMatchObject({ failure: { reason: "modelBudgetExceeded" } });
  expect(failed.usage.cost.totalUsd).toBeGreaterThan(0.000001);

  await expect(client.jobs.resume({ id })).rejects.toMatchObject({ code: "CONFLICT" });
  await expect(
    client.jobs.resume({ id, modelBudgetUsd: failed.usage.cost.totalUsd }),
  ).rejects.toMatchObject({ code: "CONFLICT" });

  system.model.setResponses([callTool("submit_output", { value: 1_000_000 })]);
  expect(await client.jobs.resume({ id, modelBudgetUsd: 1 })).toMatchObject({
    status: "queued",
    modelBudgetUsd: 1,
  });
  await untilStatus(client, id, "succeeded");
});

test("the hub refuses settings whose default limit is above its maximum", () => {
  const env = { BETTER_AUTH_SECRET: "secret", HUB_URL: "http://localhost" };
  expect(() =>
    readConfig(
      hubSettings,
      ConfigProvider.fromEnv({
        env: { ...env, JOB_RUN_TIMEOUT_SECONDS_DEFAULT: String(25 * 60 * 60) },
      }),
    ),
  ).toThrow(
    "JOB_RUN_TIMEOUT_SECONDS_DEFAULT: Expected a value less than or equal to JOB_RUN_TIMEOUT_SECONDS_MAX (86400), got 90000",
  );
  expect(() =>
    readConfig(
      hubSettings,
      ConfigProvider.fromEnv({ env: { ...env, JOB_MODEL_BUDGET_USD_MAX: "0.5" } }),
    ),
  ).toThrow(
    "JOB_MODEL_BUDGET_USD_DEFAULT: Expected a value less than or equal to JOB_MODEL_BUDGET_USD_MAX (0.5), got 1",
  );
});
