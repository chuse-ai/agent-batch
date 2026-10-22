import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import type { JobEvent, JobSubmission } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { startSystem } from "./harness.ts";
import { callTool, untilAborted } from "./scripted-model.ts";

// The controls a caller has over a job, against a real hub and Runner.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Count to a million.",
  inputFiles: {},
  resultContract: { schema: { type: "integer" } },
} satisfies JobSubmission;

/** The kinds of the Runner's own entries among `events`, in order. */
function lifecycleOf(events: JobEvent[]) {
  return events.flatMap((event) =>
    event.type === "entry" && event.record.kind.startsWith("agent-batch.")
      ? [event.record.kind]
      : [],
  );
}

// First in the file: no Runner has started, so no job is sent to one.
test("cancelling a job no Runner was sent ends it at once", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);

  const job = await client.jobs.cancel({ id });

  expect(job).toMatchObject({ status: "cancelled", endedAt: expect.any(Date) });
  expect(job).not.toHaveProperty("pendingControl");
  expect(await client.jobs.events({ id })).toEqual({
    events: [],
    hasMore: false,
    status: "cancelled",
  });
});

test("cancelling a running job stops its work at once and ends it cancelled, with the work it cut off in its events", async () => {
  await system.startRunner();
  system.model.setResponses([callTool("bash", { command: "sleep 600" })]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await expect
    .poll(async () => (await client.jobs.get({ id })).activity?.[0]?.tools, { timeout: 10_000 })
    .toMatchObject([{ name: "bash", status: "running" }]);

  expect(await client.jobs.cancel({ id })).toMatchObject({
    status: "running",
    pendingControl: { type: "cancel", requestedAt: expect.any(Date) },
  });

  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toBe("cancelled");
  expect(await client.jobs.get({ id })).not.toHaveProperty("pendingControl");
  const page = await client.jobs.events({ id });
  expect(page.status).toBe("cancelled");
  expect(lifecycleOf(page.events)).toEqual([
    "agent-batch.job.started",
    "agent-batch.job.cancelled",
  ]);
  // The command the cancel cut off has its result recorded before the job's end.
  const kinds = page.events.flatMap((event) => (event.type === "entry" ? [event.record.kind] : []));
  expect(kinds.slice(-2)).toEqual(["pi.tool-result", "agent-batch.job.cancelled"]);
});

test("a cancel that reaches the Runner after the job's result was accepted leaves it succeeded", async () => {
  // The result is accepted while a command of the same round still runs, so the run has not ended.
  system.model.setResponses([
    fauxAssistantMessage(
      [
        fauxToolCall("submit_result", { value: 1_000_000 }),
        fauxToolCall("bash", { command: "sleep 600" }),
      ],
      { stopReason: "toolUse" },
    ),
  ]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await expect
    .poll(
      async () =>
        (await client.jobs.events({ id })).events.some(
          (event) =>
            event.type === "entry" && JSON.stringify(event.record).includes("Result accepted."),
        ),
      { timeout: 10_000 },
    )
    .toBe(true);

  await client.jobs.cancel({ id });

  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toBe("succeeded");
  expect(await client.jobs.get({ id })).toMatchObject({ result: 1_000_000 });
});

test("cancelling a job that has ended leaves it as it ended", async () => {
  system.model.setResponses([callTool("submit_result", { value: 1_000_000 })]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toBe("succeeded");

  const job = await client.jobs.cancel({ id });

  expect(job).toMatchObject({ status: "succeeded", result: 1_000_000 });
  expect(job).not.toHaveProperty("pendingControl");
});

test("a job cancelled while its Runner is stopped ends cancelled once the Runner is back, without running again", async () => {
  system.model.setResponses([untilAborted]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toBe("running");
  await system.stopRunner();

  expect(await client.jobs.cancel({ id })).toMatchObject({
    status: "running",
    pendingControl: { type: "cancel" },
  });
  const calls = system.model.state.callCount;
  await system.startRunner();

  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 20_000 })
    .toBe("cancelled");
  expect(system.model.state.callCount).toBe(calls);
  const page = await client.jobs.events({ id });
  expect(lifecycleOf(page.events)).toEqual([
    "agent-batch.job.started",
    "agent-batch.job.cancelled",
  ]);
}, 40_000);
