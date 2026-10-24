import { access } from "node:fs/promises";
import path from "node:path";

import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import type { JobSubmission } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { entryKinds, lifecycleOf } from "./entries.ts";
import { type HubClient, startSystem, untilStatus } from "./harness.ts";
import { callTool, reply, untilAborted } from "./scripted-model.ts";

// The controls a caller has over a job, against a real hub and Runner. The first tests run
// before any Runner has started, so no job is sent to one; the second starts the Runner the rest
// share.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Count to a million.",
  inputFiles: {},
  resultContract: { schema: { type: "integer" } },
} satisfies JobSubmission;

/** Submits a job whose agent's model call runs until the Runner cuts it off, and waits until it runs. */
async function submitRunningJob(client: HubClient) {
  system.model.setResponses([untilAborted]);
  const { id } = await client.jobs.submit(SUBMISSION);
  await untilStatus(client, id, "running");
  return id;
}

test("cancelling a job no Runner was sent ends it at once, and deleting it then removes it at once", async () => {
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

  const deleted = await client.jobs.delete({ id });
  expect(deleted).toMatchObject({ status: "cancelled", deletedAt: expect.any(Date) });
  expect(deleted).not.toHaveProperty("pendingControl");
  await expect(client.jobs.get({ id })).rejects.toMatchObject({ code: "NOT_FOUND" });
});

test("pausing a job no Runner was sent holds it, and resuming it lets it start", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);

  expect(await client.jobs.pause({ id })).toMatchObject({ status: "paused" });
  expect(await client.jobs.resume({ id })).toMatchObject({ status: "queued" });
  system.model.setResponses([callTool("submit_result", { value: 1_000_000 })]);
  await system.startRunner();

  await untilStatus(client, id, "succeeded");
});

test("cancelling a running job stops its work at once and ends it cancelled, with the work it cut off in its events", async () => {
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

  await untilStatus(client, id, "cancelled");
  expect(await client.jobs.get({ id })).not.toHaveProperty("pendingControl");
  const page = await client.jobs.events({ id });
  expect(page.status).toBe("cancelled");
  expect(lifecycleOf(page.events)).toEqual([
    "agent-batch.job.started",
    "agent-batch.job.cancelled",
  ]);
  // The command the cancel cut off has its result recorded before the job's end.
  expect(entryKinds(page.events).slice(-2)).toEqual([
    "pi.tool-result",
    "agent-batch.job.cancelled",
  ]);
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

  await untilStatus(client, id, "succeeded");
  expect(await client.jobs.get({ id })).toMatchObject({ result: 1_000_000 });
});

test("a job that succeeded is left as it is by a cancel, and refuses a pause and a resume", async () => {
  system.model.setResponses([callTool("submit_result", { value: 1_000_000 })]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await untilStatus(client, id, "succeeded");

  const job = await client.jobs.cancel({ id });
  expect(job).toMatchObject({ status: "succeeded", result: 1_000_000 });
  expect(job).not.toHaveProperty("pendingControl");
  await expect(client.jobs.pause({ id })).rejects.toMatchObject({ code: "CONFLICT" });
  await expect(client.jobs.resume({ id })).rejects.toMatchObject({ code: "CONFLICT" });
});

test("pausing a running job stops it with its work kept, and resuming it carries the work on to its end", async () => {
  const client = await system.clientWithNewJobsKey();
  const id = await submitRunningJob(client);

  expect(await client.jobs.pause({ id })).toMatchObject({
    status: "running",
    pendingControl: { type: "pause" },
  });
  await untilStatus(client, id, "paused");
  const paused = await client.jobs.get({ id });
  expect(paused).not.toHaveProperty("pendingControl");
  expect(paused.endedAt).toBeNull();

  let told: string[] = [];
  system.model.setResponses([
    (context) => {
      told = context.messages.flatMap((message) =>
        message.role === "user" && typeof message.content === "string" ? [message.content] : [],
      );
      return callTool("submit_result", { value: 1_000_000 });
    },
  ]);
  expect(await client.jobs.resume({ id })).toMatchObject({ status: "queued" });
  await untilStatus(client, id, "succeeded");
  const { events } = await client.jobs.events({ id });
  expect(lifecycleOf(events)).toEqual([
    "agent-batch.job.started",
    "agent-batch.job.paused",
    "agent-batch.job.resumed",
    "agent-batch.job.succeeded",
  ]);
  // The agent carries on its conversation: the pause settled the model call it cut off, so the
  // agent is asked to carry on after the job's Prompt.
  expect(told).toEqual([SUBMISSION.prompt, expect.stringContaining("Carry on")]);
});

test("a cancelled job resumes from where it stopped", async () => {
  const client = await system.clientWithNewJobsKey();
  const id = await submitRunningJob(client);
  await client.jobs.cancel({ id });
  await untilStatus(client, id, "cancelled");

  system.model.setResponses([callTool("submit_result", { value: 1_000_000 })]);
  await client.jobs.resume({ id });

  await untilStatus(client, id, "succeeded");
  const { events } = await client.jobs.events({ id });
  expect(lifecycleOf(events)).toEqual([
    "agent-batch.job.started",
    "agent-batch.job.cancelled",
    "agent-batch.job.resumed",
    "agent-batch.job.succeeded",
  ]);
});

test("a failed job resumes with a note asking its agent to carry on", async () => {
  system.model.setResponses([reply("Counting is tedious.")]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await untilStatus(client, id, "failed");

  let told = "";
  system.model.setResponses([
    (context) => {
      const last = context.messages.at(-1);
      told = last?.role === "user" && typeof last.content === "string" ? last.content : "";
      return callTool("submit_result", { value: 1_000_000 });
    },
  ]);
  await client.jobs.resume({ id });

  await untilStatus(client, id, "succeeded");
  expect(told).toContain("Carry on");
});

test("a pause waits for the Runner to confirm it before a resume, and a cancel replaces it", async () => {
  const client = await system.clientWithNewJobsKey();
  const id = await submitRunningJob(client);
  await system.stopRunner();

  await client.jobs.pause({ id });
  await expect(client.jobs.resume({ id })).rejects.toMatchObject({ code: "CONFLICT" });
  expect(await client.jobs.cancel({ id })).toMatchObject({
    status: "running",
    pendingControl: { type: "cancel" },
  });
  await expect(client.jobs.pause({ id })).rejects.toMatchObject({ code: "CONFLICT" });
  const calls = system.model.state.callCount;
  await system.startRunner();

  await untilStatus(client, id, "cancelled", 20_000);
  expect(system.model.state.callCount).toBe(calls);
  const { events } = await client.jobs.events({ id });
  expect(lifecycleOf(events)).toEqual(["agent-batch.job.started", "agent-batch.job.cancelled"]);
}, 40_000);

test("cancelling a paused job ends it at once, though its Runner is away", async () => {
  const client = await system.clientWithNewJobsKey();
  const id = await submitRunningJob(client);
  await client.jobs.pause({ id });
  await untilStatus(client, id, "paused");
  await system.stopRunner();

  expect(await client.jobs.cancel({ id })).toMatchObject({
    status: "cancelled",
    endedAt: expect.any(Date),
  });
  await system.startRunner();
});

test("a job resumed while its Runner is away is not yet sent to it, so a cancel ends it again at once", async () => {
  const client = await system.clientWithNewJobsKey();
  const id = await submitRunningJob(client);
  await client.jobs.cancel({ id });
  await untilStatus(client, id, "cancelled");
  await system.stopRunner();
  expect(await client.jobs.resume({ id })).toMatchObject({ status: "queued" });

  expect(await client.jobs.cancel({ id })).toMatchObject({
    status: "cancelled",
    endedAt: expect.any(Date),
  });
  await system.startRunner();
});

test("pausing a running job frees its slot for the next queued job", async () => {
  await system.stopRunner();
  await system.startRunner({ maxJobs: 1 });
  const client = await system.clientWithNewJobsKey();
  const id = await submitRunningJob(client);
  system.model.setResponses([callTool("submit_result", { value: 1_000_000 })]);
  const next = await client.jobs.submit(SUBMISSION);

  await client.jobs.pause({ id });

  await untilStatus(client, id, "paused");
  await untilStatus(client, next.id, "succeeded");
  await client.jobs.cancel({ id });
  await system.stopRunner();
  await system.startRunner();
});

test("deleting an unfinished job is refused", async () => {
  const client = await system.clientWithNewJobsKey();
  const id = await submitRunningJob(client);

  await expect(client.jobs.delete({ id })).rejects.toMatchObject({
    code: "CONFLICT",
    message: expect.stringContaining("cancel it first"),
  });
  await client.jobs.cancel({ id });
  await untilStatus(client, id, "cancelled");
});

test("deleting a finished job removes its content at once and its files from its host once its Runner confirms", async () => {
  system.model.setResponses([callTool("submit_result", { value: 1_000_000 })]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await untilStatus(client, id, "succeeded");
  const jobDir = path.dirname(system.workspace(id));
  await access(jobDir);

  const deleted = await client.jobs.delete({ id });

  expect(deleted).toMatchObject({
    status: "succeeded",
    deletedAt: expect.any(Date),
    pendingControl: { type: "delete", requestedAt: expect.any(Date) },
  });
  expect(deleted).not.toHaveProperty("result");
  await expect(client.jobs.events({ id })).rejects.toMatchObject({ code: "NOT_FOUND" });
  await expect
    .poll(() => client.jobs.get({ id }).catch((error: unknown) => error), { timeout: 10_000 })
    .toMatchObject({ code: "NOT_FOUND" });
  await expect(access(jobDir)).rejects.toMatchObject({ code: "ENOENT" });
  await expect(client.jobs.delete({ id })).rejects.toMatchObject({ code: "NOT_FOUND" });
});

test("a job deleted while its Runner is away reads as deleted, and a repeated delete returns it, until the Runner is back", async () => {
  system.model.setResponses([callTool("submit_result", { value: 1_000_000 })]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await untilStatus(client, id, "succeeded");
  await system.stopRunner();

  const deleted = await client.jobs.delete({ id });
  expect(await client.jobs.get({ id })).toEqual(deleted);
  expect(await client.jobs.delete({ id })).toEqual(deleted);
  expect(await client.jobs.cancel({ id })).toEqual(deleted);
  await expect(client.jobs.resume({ id })).rejects.toMatchObject({ code: "CONFLICT" });
  await system.startRunner();

  await expect
    .poll(() => client.jobs.get({ id }).catch((error: unknown) => error), { timeout: 20_000 })
    .toMatchObject({ code: "NOT_FOUND" });
}, 40_000);

// Last in the file: it removes the Runner the others share.
test("removing a Runner ends the jobs it held, and leaves its finished jobs no work to carry on or files to remove", async () => {
  const tedious = reply("Counting is tedious.");
  system.model.setResponses([tedious, tedious, tedious]);
  const client = await system.clientWithNewJobsKey();
  const { id: finished } = await client.jobs.submit(SUBMISSION);
  const { id: resumed } = await client.jobs.submit(SUBMISSION);
  const { id: deleting } = await client.jobs.submit(SUBMISSION);
  for (const id of [finished, resumed, deleting]) await untilStatus(client, id, "failed");
  system.model.setResponses([untilAborted, untilAborted]);
  const { id: running } = await client.jobs.submit(SUBMISSION);
  const { id: cancelling } = await client.jobs.submit(SUBMISSION);
  await untilStatus(client, running, "running");
  await untilStatus(client, cancelling, "running");
  await system.stopRunner();
  await client.jobs.cancel({ id: cancelling });
  // Queued for the Runner, which holds its journal, and the removal of the other's files waits for it.
  await client.jobs.resume({ id: resumed });
  await client.jobs.delete({ id: deleting });
  const runnerId = (await system.owner().runners.list())[0]?.id ?? "";

  const removed = await system.owner().runners.delete({ id: runnerId });

  expect(removed.endedJobs.toSorted()).toEqual([running, cancelling, resumed].toSorted());
  expect(removed.jobsWithFilesLeft).toEqual(
    expect.arrayContaining([finished, running, cancelling, resumed, deleting]),
  );
  for (const id of [running, resumed]) {
    expect(await client.jobs.get({ id })).toMatchObject({
      status: "failed",
      reason: "runnerRemoved",
    });
  }
  expect(await client.jobs.get({ id: cancelling })).toMatchObject({ status: "cancelled" });
  await expect(client.jobs.get({ id: deleting })).rejects.toMatchObject({ code: "NOT_FOUND" });
  await expect(client.jobs.resume({ id: finished })).rejects.toMatchObject({
    code: "CONFLICT",
    message: expect.stringContaining("Runner was removed"),
  });
  const deleted = await client.jobs.delete({ id: finished });
  expect(deleted).not.toHaveProperty("pendingControl");
  await expect(client.jobs.get({ id: finished })).rejects.toMatchObject({ code: "NOT_FOUND" });
});
