import type { BatchJobSubmission } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";
import * as z from "zod/mini";

import { type ApiClient, startSystem, untilStatus } from "./harness.ts";
import { callTool, providerError, untilAborted } from "./scripted-model.ts";

// Batches, labels and the job list, against a real hub. The first tests run before any Runner has
// started, so no job is sent to one, and each cancels what it leaves unfinished; the rest share the
// Runner the first of them starts.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

/** A Batch's job named `key`. */
function job(key: string, extra: Partial<BatchJobSubmission> = {}): BatchJobSubmission {
  return {
    key,
    prompt: `Answer for ${key}.`,
    inputFiles: {},
    resultContract: { schema: { type: "integer" } },
    ...extra,
  };
}

/** Waits until the Batch `id` has `status`. */
async function untilBatchStatus(client: ApiClient, id: string, status: string) {
  await expect
    .poll(async () => (await client.batches.get({ id })).status, { timeout: 15_000 })
    .toBe(status);
}

test("a Batch's jobs are accepted or refused one by one, and submitting it again adds only the jobs it lacks", async () => {
  const client = await system.clientWithNewJobsKey();
  const invalid = job("d", { resultContract: { schema: { type: "array" } } });

  const first = await client.batches.create({
    idempotencyKey: "invoices-2026-10",
    failFast: false,
    jobs: [job("a"), job("b"), invalid, job("c")],
  });

  expect(first.jobs.map(({ key, outcome }) => [key, outcome])).toEqual([
    ["a", "created"],
    ["b", "created"],
    ["d", "rejected"],
    ["c", "created"],
  ]);
  expect(first.jobs[2]).toMatchObject({
    reason: "invalid",
    problems: [{ path: ["resultContract", "schema"] }],
  });
  expect(first.batch.jobCounts).toMatchObject({ queued: 3 });

  // The script runs again, from the start: the same Batch, the jobs it has, and the one it lacks.
  const again = await client.batches.create({
    idempotencyKey: "invoices-2026-10",
    failFast: false,
    jobs: [job("a"), job("b", { prompt: "Something else." }), job("c"), job("e")],
  });

  expect(again.batch.id).toBe(first.batch.id);
  expect(again.jobs.map(({ key, outcome }) => [key, outcome])).toEqual([
    ["a", "existing"],
    ["b", "rejected"],
    ["c", "existing"],
    ["e", "created"],
  ]);
  expect(again.jobs[1]).toMatchObject({ reason: "keyReused" });
  const [a] = first.jobs;
  const [againA] = again.jobs;
  expect(againA && "job" in againA && againA.job.id).toBe(a && "job" in a && a.job.id);
  expect(again.batch.jobCounts).toMatchObject({ queued: 4 });

  await expect(
    client.batches.create({ idempotencyKey: "invoices-2026-10", failFast: true }),
  ).rejects.toMatchObject({ code: "UNPROCESSABLE_CONTENT" });
  await client.batches.cancel({ id: first.batch.id });
});

test("a Batch created without jobs is in progress until its jobs come and end", async () => {
  const client = await system.clientWithNewJobsKey();
  const { batch } = await client.batches.create({ failFast: false });
  expect(batch).toMatchObject({ status: "inProgress", jobCounts: { queued: 0 } });
  expect(await client.batches.cancel({ id: batch.id })).toMatchObject({ status: "cancelled" });
});

test("jobs are listed by label, status and Batch, newest first, with a Batch's labels beneath their own", async () => {
  const client = await system.clientWithNewJobsKey();
  const { batch } = await client.batches.create({
    labels: { customer: "acme", team: "billing" },
    failFast: false,
    jobs: [job("x", { labels: { team: "support" } }), job("y")],
  });
  const alone = await client.jobs.submit({
    prompt: "Alone.",
    inputFiles: {},
    resultContract: { schema: { type: "integer" } },
    labels: { customer: "acme" },
  });
  await client.jobs.cancel({ id: alone.id });

  const members = await client.jobs.list({ batchId: batch.id });
  expect(members.jobs.map((listed) => [listed.key, listed.labels])).toEqual([
    ["y", { customer: "acme", team: "billing" }],
    ["x", { customer: "acme", team: "support" }],
  ]);

  const acme = await client.jobs.list({ labels: { customer: "acme" }, limit: 2 });
  expect(acme.jobs.map((listed) => listed.id)).toEqual([alone.id, members.jobs[0]?.id]);
  expect(acme.hasMore).toBe(true);
  const next = await client.jobs.list({ labels: { customer: "acme" }, after: alone.id, limit: 2 });
  expect(next.jobs.map((listed) => listed.key)).toEqual(["y", "x"]);

  const support = await client.jobs.list({ labels: { customer: "acme", team: "support" } });
  expect(support.jobs.map((listed) => listed.key)).toEqual(["x"]);

  const cancelled = await client.jobs.list({ labels: { customer: "acme" }, status: ["cancelled"] });
  expect(cancelled.jobs.map((listed) => listed.id)).toEqual([alone.id]);
  expect(cancelled.jobs[0]).not.toHaveProperty("key");

  // The query string as a caller writes it by hand: one status, and a label in brackets.
  const response = await system.hub.fetch(
    `/jobs?status=queued&labels%5Bteam%5D=billing&batchId=${batch.id}`,
    { headers: { "x-api-key": await system.newJobsKey() } },
  );
  const page = z.parse(
    z.object({ jobs: z.array(z.object({ key: z.string() })) }),
    await response.json(),
  );
  expect(page.jobs.map((listed) => listed.key)).toEqual(["y"]);
  await client.batches.cancel({ id: batch.id });
});

test("a paused Batch holds its jobs and those submitted into it, and a cancelled one refuses new jobs", async () => {
  const client = await system.clientWithNewJobsKey();
  const { batch } = await client.batches.create({ failFast: false, jobs: [job("a")] });

  expect(await client.batches.pause({ id: batch.id })).toMatchObject({
    status: "paused",
    pausedBy: "caller",
  });
  const held = await client.batches.submit({ id: batch.id, jobs: [job("b")] });
  expect(held.jobs[0]).toMatchObject({
    outcome: "created",
    job: { status: "queued", waitingFor: "batchPaused" },
  });

  expect(await client.batches.resume({ id: batch.id })).toMatchObject({ status: "inProgress" });
  const [listed] = (await client.jobs.list({ batchId: batch.id, key: "b" })).jobs;
  expect(listed).toMatchObject({ status: "queued" });
  expect(listed).not.toHaveProperty("waitingFor");

  await client.batches.pause({ id: batch.id });
  const cancelled = await client.batches.cancel({ id: batch.id });
  // No Runner was sent any of them, so every member ends at once.
  expect(cancelled).toMatchObject({ status: "cancelled", jobCounts: { cancelled: 2 } });
  expect(cancelled).not.toHaveProperty("pendingControl");

  const late = await client.batches.submit({ id: batch.id, jobs: [job("a"), job("c")] });
  expect(late.jobs.map(({ outcome }) => outcome)).toEqual(["existing", "rejected"]);
  expect(late.jobs[1]).toMatchObject({ reason: "batchCancelled" });
  await expect(client.batches.resume({ id: batch.id })).rejects.toMatchObject({ code: "CONFLICT" });
});

test("a Batch's first jobs, its Canary, run before the rest are released", async () => {
  const client = await system.clientWithNewJobsKey();
  const keys = ["1", "2", "3", "4", "5", "6", "7"];
  const { batch, jobs } = await client.batches.create({ jobs: keys.map((key) => job(key)) });

  expect(batch.failFast).toEqual({ canary: 5, tolerance: 25 });
  expect(jobs.map((outcome) => "job" in outcome && (outcome.job.waitingFor ?? "none"))).toEqual([
    "none",
    "none",
    "none",
    "none",
    "none",
    "canary",
    "canary",
  ]);
  await client.batches.cancel({ id: batch.id });
});

test("two failures among the Canary pause the Batch and hold the rest, and cancelling it cancels them", async () => {
  // Every job's model call fails, as when a budget every job shares is set too low.
  system.model.setResponses(Array.from({ length: 20 }, () => providerError("Budget too low")));
  await system.startRunner();
  const client = await system.clientWithNewJobsKey();
  const { batch } = await client.batches.create({
    jobs: ["1", "2", "3", "4", "5", "6", "7", "8"].map((key) => job(key)),
  });

  await untilBatchStatus(client, batch.id, "paused");
  await expect
    .poll(async () => (await client.batches.get({ id: batch.id })).jobCounts.running, {
      timeout: 15_000,
    })
    .toBe(0);
  const paused = await client.batches.get({ id: batch.id });
  expect(paused).toMatchObject({ pausedBy: "failFast", jobCounts: { queued: 3 } });
  const held = await client.jobs.list({ batchId: batch.id, status: ["queued"] });
  expect(held.jobs.map((listed) => listed.waitingFor)).toEqual([
    "failFast",
    "failFast",
    "failFast",
  ]);

  const cancelled = await client.batches.cancel({ id: batch.id });
  expect(cancelled).toMatchObject({ status: "cancelled", jobCounts: { queued: 0, cancelled: 3 } });
});

test("a Batch without its breaker releases every job at once and never pauses itself", async () => {
  system.model.setResponses(Array.from({ length: 4 }, () => providerError("Budget too low")));
  const client = await system.clientWithNewJobsKey();
  const { batch, jobs } = await client.batches.create({
    failFast: false,
    jobs: ["1", "2", "3", "4"].map((key) => job(key)),
  });
  expect(jobs.every((outcome) => "job" in outcome && !outcome.job.waitingFor)).toBe(true);

  await untilBatchStatus(client, batch.id, "ended");
  expect((await client.batches.get({ id: batch.id })).jobCounts).toMatchObject({ failed: 4 });
});

test("a Batch's summary counts its members by status and adds up their cost, those deleted included", async () => {
  system.model.setResponses([
    callTool("submit_result", { value: 1 }),
    callTool("submit_result", { value: 2 }),
  ]);
  const client = await system.clientWithNewJobsKey();
  const { batch, jobs } = await client.batches.create({
    failFast: false,
    jobs: [job("1"), job("2")],
  });
  await untilBatchStatus(client, batch.id, "ended");
  const members = await client.jobs.list({ batchId: batch.id });
  const costs = members.jobs.map((listed) => listed.usage.cost.totalUsd);
  const ended = await client.batches.get({ id: batch.id });
  expect(ended.jobCounts).toMatchObject({ succeeded: 2, deleted: 0 });
  expect(ended.usage.cost.totalUsd).toBeCloseTo(
    costs.reduce((sum, cost) => sum + cost, 0),
    10,
  );
  expect(ended.usage.cost.totalUsd).toBeGreaterThan(0);

  const [first] = jobs;
  if (!first || !("job" in first)) throw new Error("The first job was not created");
  await client.jobs.delete({ id: first.job.id });
  const afterDelete = await client.batches.get({ id: batch.id });
  expect(afterDelete.jobCounts).toMatchObject({ succeeded: 1, deleted: 1 });
  expect(afterDelete.usage).toEqual(ended.usage);
});

test("cancelling a Batch waits for the Runners of its running members, shown as its pendingControl", async () => {
  system.model.setResponses([untilAborted]);
  const client = await system.clientWithNewJobsKey();
  const { batch, jobs } = await client.batches.create({ failFast: false, jobs: [job("1")] });
  const [member] = jobs;
  if (!member || !("job" in member)) throw new Error("The job was not created");
  await untilStatus(client, member.job.id, "running");

  expect(await client.batches.cancel({ id: batch.id })).toMatchObject({
    status: "inProgress",
    pendingControl: { type: "cancel", requestedAt: expect.any(Date) },
  });
  await untilBatchStatus(client, batch.id, "cancelled");
  expect(await client.batches.get({ id: batch.id })).not.toHaveProperty("pendingControl");
});
