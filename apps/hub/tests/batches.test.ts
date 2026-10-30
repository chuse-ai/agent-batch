import { ORPCError } from "@orpc/client";
import { problemOf } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";
import * as z from "zod/mini";

import { type ApiClient, type JobInput, startSystem, submitJob, untilStatus } from "./harness.ts";
import { callTool, reply, untilAborted } from "./scripted-model.ts";

// Batches, labels and the job list, against a real hub. The first tests run before any Runner has
// started, so no job is sent to one, and each cancels what it leaves unfinished; the rest share the
// Runner the first of them starts.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const AGENT = { outputContract: { schema: { type: "integer" } } };

/** An Input file at `path`. */
function fileAt(path: string) {
  return { path, content: new File(["2.2"], "rates.csv") };
}

/** A job with the custom ID `customId`. */
function jobNamed(customId: string, extra: Partial<JobInput> = {}): JobInput {
  return { customId, input: `Answer for ${customId}.`, ...extra };
}

/** What blocks each job an add answered with, or "none". */
function blockedBy(added: Awaited<ReturnType<ApiClient["batches"]["addJobs"]>>) {
  return added.jobs.map(({ job }) => job.blockedBy ?? "none");
}

/** Waits until the Batch `id` has `status`. */
async function untilBatchStatus(client: ApiClient, id: string, status: string) {
  await expect
    .poll(async () => (await client.batches.get({ id })).status, { timeout: 15_000 })
    .toBe(status);
}

/** Posts `body` as raw JSON to `path` with a new key, as any HTTP client would. */
async function post(path: string, body: unknown) {
  return system.hub.fetch(path, {
    method: "POST",
    headers: { "x-api-key": await system.newJobsKey(), "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

test("a custom ID used again with the same content returns what it named, and with other content is refused", async () => {
  const client = await system.clientWithNewJobsKey();
  const creation = { customId: "invoices-2026-10", agent: AGENT, breaker: false } as const;
  const batch = await client.batches.create(creation);
  expect(batch).toMatchObject({ customId: "invoices-2026-10", status: "active", breaker: false });
  // The script runs again, from the start: the same Batch.
  expect((await client.batches.create(creation)).id).toBe(batch.id);
  await expect(client.batches.create({ ...creation, breaker: true })).rejects.toMatchObject({
    code: "UNPROCESSABLE_CONTENT",
  });

  const first = await client.batches.addJobs({
    id: batch.id,
    jobs: [jobNamed("a"), jobNamed("b")],
  });
  expect(first.jobs.map(({ created, job }) => [job.customId, created])).toEqual([
    ["a", true],
    ["b", true],
  ]);
  // Again, with a job the Batch lacks: the jobs it has are returned, and only the one it lacks added.
  const again = await client.batches.addJobs({
    id: batch.id,
    jobs: [jobNamed("a"), jobNamed("c"), jobNamed("b")],
  });
  expect(again.jobs.map(({ created, job }) => [job.customId, created])).toEqual([
    ["a", false],
    ["c", true],
    ["b", false],
  ]);
  expect(again.jobs[0]?.job.id).toBe(first.jobs[0]?.job.id);
  expect(again.batch.jobCounts).toMatchObject({ queued: 3 });

  // A custom ID used for a job with other content refuses the whole add, naming each such job.
  const response = await post(`/batches/${batch.id}/jobs`, {
    jobs: [jobNamed("d"), jobNamed("b", { input: "Something else." })],
  });
  expect(response.status).toBe(422);
  expect(await response.json()).toMatchObject({
    detail: expect.stringContaining("jobs[1].customId"),
  });
  expect((await client.batches.get({ id: batch.id })).jobCounts).toMatchObject({ queued: 3 });
  await client.batches.cancel({ id: batch.id });
});

test("an add with an invalid job adds none, and its answer names every problem with its path", async () => {
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({
    agent: { ...AGENT, files: [{ path: "rates.csv", content: new File(["1.1"], "rates.csv") }] },
    labels: { team: "billing" },
    breaker: false,
  });
  const many = Object.fromEntries(Array.from({ length: 64 }, (_, index) => [`l${index}`, "x"]));

  // Problems only the hub sees, with the Batch: a custom ID given twice, a file at the path of one
  // of the agent's or inside it, as if it were a folder, and labels over the limit with the
  // Batch's. Sent with files, so as `multipart/form-data`.
  const refused = await client.batches
    .addJobs({
      id: batch.id,
      jobs: [
        jobNamed("a"),
        jobNamed("a"),
        jobNamed("b", { files: [fileAt("rates.csv")] }),
        jobNamed("c", { labels: many }),
        jobNamed("d", { files: [fileAt("notes.txt"), fileAt("rates.csv/2026.csv")] }),
      ],
    })
    .catch((error: unknown) => error);
  const problem = refused instanceof ORPCError ? problemOf(refused) : undefined;
  expect(problem?.status).toBe(400);
  for (const path of [
    "jobs[1].customId",
    "jobs[2].files[0].path",
    "jobs[3].labels",
    "jobs[4].files[1].path",
  ]) {
    expect(problem?.detail).toContain(path);
  }

  // A label value holds any text of at most 256 characters, but no NUL.
  const invalid = await post(`/batches/${batch.id}/jobs`, {
    jobs: [
      jobNamed("d", { labels: { note: "é".repeat(256) } }),
      jobNamed("e", { labels: { note: "\0" } }),
    ],
  });
  expect(invalid.status).toBe(400);
  expect(await invalid.json()).toMatchObject({ detail: expect.stringContaining("jobs[1].labels") });
  expect((await client.batches.get({ id: batch.id })).jobCounts).toMatchObject({ queued: 0 });
  await client.batches.cancel({ id: batch.id });
});

test("jobs are listed by label, status, Batch and custom ID, newest first, with a Batch's labels beneath their own", async () => {
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({
    agent: AGENT,
    labels: { customer: "acme", team: "billing" },
    breaker: false,
  });
  await client.batches.addJobs({
    id: batch.id,
    jobs: [jobNamed("x", { labels: { team: "support" } }), jobNamed("y")],
  });
  const alone = await submitJob(client, {
    ...AGENT,
    customId: "alone",
    input: "Alone.",
    labels: { customer: "acme" },
  });
  await client.jobs.cancel({ id: alone.id });

  const jobs = await client.jobs.list({ batchId: batch.id });
  expect(jobs.jobs.map((listed) => [listed.customId, listed.labels])).toEqual([
    ["y", { customer: "acme", team: "billing" }],
    ["x", { customer: "acme", team: "support" }],
  ]);

  const acme = await client.jobs.list({ labels: { customer: "acme" }, limit: 2 });
  expect(acme.jobs.map((listed) => listed.customId)).toEqual(["alone", "y"]);
  expect(acme.hasMore).toBe(true);
  const next = await client.jobs.list({ labels: { customer: "acme" }, after: alone.id, limit: 2 });
  expect(next.jobs.map((listed) => listed.customId)).toEqual(["y", "x"]);
  expect(next.hasMore).toBe(false);

  const support = await client.jobs.list({ labels: { customer: "acme", team: "support" } });
  expect(support.jobs.map((listed) => listed.customId)).toEqual(["x"]);
  const cancelled = await client.jobs.list({ labels: { customer: "acme" }, status: ["cancelled"] });
  expect(cancelled.jobs.map((listed) => listed.id)).toEqual([alone.id]);
  const named = await client.jobs.list({ customId: "x", labels: { customer: "acme" } });
  expect(named.jobs.map((listed) => listed.batchId)).toEqual([batch.id]);

  // The query string as a caller writes it by hand: one status, and a label in brackets.
  const response = await system.hub.fetch(
    `/jobs?status=queued&labels%5Bteam%5D=billing&batchId=${batch.id}`,
    { headers: { "x-api-key": await system.newJobsKey() } },
  );
  const page = z.parse(
    z.object({ jobs: z.array(z.object({ customId: z.string() })) }),
    await response.json(),
  );
  expect(page.jobs.map((listed) => listed.customId)).toEqual(["y"]);
  await client.batches.cancel({ id: batch.id });
});

test("a paused Batch blocks the jobs not yet sent and those added later, without changing their status", async () => {
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({ agent: AGENT, breaker: false });
  const [first] = (await client.batches.addJobs({ id: batch.id, jobs: [jobNamed("a")] })).jobs;
  if (!first) throw new Error("The add answered without the job");

  expect(await client.batches.pause({ id: batch.id })).toMatchObject({
    status: "paused",
    pausedBy: "caller",
  });
  const added = await client.batches.addJobs({ id: batch.id, jobs: [jobNamed("b")] });
  expect(added.jobs[0]?.job).toMatchObject({ status: "queued", blockedBy: "batchPausedByCaller" });
  expect(await client.jobs.get({ id: first.job.id })).toMatchObject({
    status: "queued",
    blockedBy: "batchPausedByCaller",
  });

  expect(await client.batches.resume({ id: batch.id })).toMatchObject({ status: "active" });
  const [listed] = (await client.jobs.list({ batchId: batch.id, customId: "b" })).jobs;
  expect(listed).toMatchObject({ status: "queued" });
  expect(listed).not.toHaveProperty("blockedBy");
  await client.batches.cancel({ id: batch.id });
});

test("a cancel is final: the Batch takes no new jobs and resumes none, even with every job ended", async () => {
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({ agent: AGENT, breaker: false });
  await client.batches.addJobs({ id: batch.id, jobs: [jobNamed("a")] });
  await client.batches.pause({ id: batch.id });

  const cancelled = await client.batches.cancel({ id: batch.id });
  // No Runner was sent the job, so it ends at once.
  expect(cancelled).toMatchObject({
    status: "cancelled",
    pausedBy: null,
    jobCounts: { cancelled: 1 },
  });
  expect(cancelled).not.toHaveProperty("pendingControl");
  // Also an add of the job it has.
  await expect(
    client.batches.addJobs({ id: batch.id, jobs: [jobNamed("a"), jobNamed("b")] }),
  ).rejects.toMatchObject({ code: "CONFLICT" });
  await expect(client.batches.resume({ id: batch.id })).rejects.toMatchObject({ code: "CONFLICT" });

  const empty = await client.batches.create({ agent: AGENT });
  expect(empty).toMatchObject({ status: "active", jobCounts: { queued: 0 } });
  expect(await client.batches.cancel({ id: empty.id })).toMatchObject({ status: "cancelled" });
  await expect(
    client.batches.addJobs({ id: empty.id, jobs: [jobNamed("a")] }),
  ).rejects.toMatchObject({
    code: "CONFLICT",
  });
});

test("a Batch's Canary, its first jobs, those added later included, runs before the rest are released", async () => {
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({ agent: AGENT });
  expect(batch.breaker).toEqual({ canary: 5, maxFailurePercent: 10 });

  const first = await client.batches.addJobs({
    id: batch.id,
    jobs: [jobNamed("1"), jobNamed("2")],
  });
  expect(blockedBy(first)).toEqual(["none", "none"]);
  const rest = await client.batches.addJobs({
    id: batch.id,
    jobs: ["3", "4", "5", "6", "7"].map((customId) => jobNamed(customId)),
  });
  expect(blockedBy(rest)).toEqual(["none", "none", "none", "batchCanary", "batchCanary"]);
  await client.batches.cancel({ id: batch.id });

  // A Canary of none releases every job at once.
  const uncautious = await client.batches.create({
    agent: AGENT,
    breaker: { canary: 0, maxFailurePercent: 10 },
  });
  const all = await client.batches.addJobs({
    id: uncautious.id,
    jobs: [jobNamed("1"), jobNamed("2")],
  });
  expect(blockedBy(all)).toEqual(["none", "none"]);
  await client.batches.cancel({ id: uncautious.id });
});

test("a failure in the Canary pauses the Batch, and resuming it runs a new Canary of the next jobs first, those added later included", async () => {
  await system.startRunner();
  // Every job's model call fails, as when a budget every job shares is set too low.
  system.model.setResponses([]);
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({
    agent: AGENT,
    breaker: { canary: 1, maxFailurePercent: 100 },
  });
  const { jobs } = await client.batches.addJobs({
    id: batch.id,
    jobs: [jobNamed("1"), jobNamed("2"), jobNamed("3")],
  });
  const [, second, third] = jobs.map(({ job }) => job.id);
  if (!second || !third) throw new Error("The add answered without the jobs");

  await untilBatchStatus(client, batch.id, "paused");
  expect(await client.batches.get({ id: batch.id })).toMatchObject({
    pausedBy: "breaker",
    jobCounts: { failed: 1, queued: 2 },
  });
  const held = await client.jobs.list({ batchId: batch.id, status: ["queued"] });
  expect(held.jobs.map((listed) => listed.blockedBy)).toEqual([
    "batchPausedByBreaker",
    "batchPausedByBreaker",
  ]);

  system.model.setResponses([untilAborted]);
  expect(await client.batches.resume({ id: batch.id })).toMatchObject({ status: "active" });
  await untilStatus(client, second, "running");
  expect(await client.jobs.get({ id: third })).toMatchObject({ blockedBy: "batchCanary" });
  const late = await client.batches.addJobs({ id: batch.id, jobs: [jobNamed("4")] });
  expect(late.jobs[0]?.job.blockedBy).toBe("batchCanary");

  await client.batches.cancel({ id: batch.id });
  await untilBatchStatus(client, batch.id, "cancelled");
});

// With four jobs, the new Canary flags the third and fourth; with two, it finds none to flag, and its
// places wait for jobs added later.
test.each([4, 2])(
  "a job sent before a new Canary began, ending during it, does not judge the Batch in the Canary's place (%i jobs)",
  async (total) => {
    const client = await system.clientWithNewJobsKey();
    const batch = await client.batches.create({
      agent: AGENT,
      breaker: { canary: 2, maxFailurePercent: 10 },
    });
    const { promise: failing, resolve: fail } = Promise.withResolvers<void>();
    // One of the first Canary's jobs fails at once; the other, later.
    system.model.setResponses([
      reply("I cannot."),
      async () => {
        await failing;
        return reply("I cannot either.");
      },
    ]);
    const { jobs } = await client.batches.addJobs({
      id: batch.id,
      jobs: Array.from({ length: total }, (_, i) => jobNamed(String(i + 1))),
    });
    const nextCanary = jobs.slice(2).map(({ job }) => job.id);
    await untilBatchStatus(client, batch.id, "paused");

    system.model.setResponses([untilAborted, untilAborted]);
    await client.batches.resume({ id: batch.id });
    for (const id of nextCanary) await untilStatus(client, id, "running");
    fail();
    await expect
      .poll(async () => (await client.batches.get({ id: batch.id })).jobCounts.failed, {
        timeout: 15_000,
      })
      .toBe(2);
    expect(await client.batches.get({ id: batch.id })).toMatchObject({ status: "active" });
    await client.batches.cancel({ id: batch.id });
    await untilBatchStatus(client, batch.id, "cancelled");
  },
);

test("outside a Canary, the breaker pauses the Batch when more than maxFailurePercent of its jobs ended since have failed, each job counted once", async () => {
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({
    agent: AGENT,
    breaker: { canary: 0, maxFailurePercent: 50 },
  });
  /** Adds the job `customId`, whose agent makes `responses`, and waits for it to end `status`. */
  async function run(
    customId: string,
    responses: Parameters<typeof system.model.setResponses>[0],
    status: string,
  ) {
    system.model.setResponses(responses);
    const [added] = (await client.batches.addJobs({ id: batch.id, jobs: [jobNamed(customId)] }))
      .jobs;
    if (!added) throw new Error("The add answered without the job");
    await untilStatus(client, added.job.id, status);
    return added.job.id;
  }

  await run("1", [callTool("submit_output", { value: 1 })], "succeeded");
  // One of two failed: not more than half.
  const failed = await run("2", [reply("I cannot.")], "failed");
  // Its resume withdraws its failure, so failing again leaves one of two.
  system.model.setResponses([reply("I still cannot.")]);
  await client.jobs.resume({ id: failed });
  await untilStatus(client, failed, "failed");
  expect(await client.batches.get({ id: batch.id })).toMatchObject({ status: "active" });

  // Two of three.
  await run("3", [reply("I cannot either.")], "failed");
  await untilBatchStatus(client, batch.id, "paused");
  expect(await client.batches.get({ id: batch.id })).toMatchObject({ pausedBy: "breaker" });
  await client.batches.cancel({ id: batch.id });
}, 20_000);

test("a caller's pause keeps the breaker counting, and the caller's resume after it tripped runs a new Canary first", async () => {
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({
    agent: AGENT,
    breaker: { canary: 1, maxFailurePercent: 100 },
  });
  const { promise: failing, resolve: fail } = Promise.withResolvers<void>();
  system.model.setResponses([
    async () => {
      await failing;
      return reply("I cannot.");
    },
  ]);
  const [first] = (await client.batches.addJobs({ id: batch.id, jobs: [jobNamed("1")] })).jobs;
  if (!first) throw new Error("The add answered without the job");
  await untilStatus(client, first.job.id, "running");

  await client.batches.pause({ id: batch.id });
  fail();
  await untilStatus(client, first.job.id, "failed");
  expect(await client.batches.get({ id: batch.id })).toMatchObject({
    status: "paused",
    pausedBy: "caller",
  });
  const { jobs } = await client.batches.addJobs({
    id: batch.id,
    jobs: [jobNamed("2"), jobNamed("3")],
  });
  const [second, third] = jobs.map(({ job }) => job.id);
  if (!second || !third) throw new Error("The add answered without the jobs");

  system.model.setResponses([untilAborted]);
  await client.batches.resume({ id: batch.id });
  await untilStatus(client, second, "running");
  expect(await client.jobs.get({ id: third })).toMatchObject({ blockedBy: "batchCanary" });

  await client.batches.cancel({ id: batch.id });
  await untilBatchStatus(client, batch.id, "cancelled");
});

test("a Batch without a breaker releases every job at once and never pauses itself", async () => {
  // Every job's model call fails.
  system.model.setResponses([]);
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({ agent: AGENT, breaker: false });
  const added = await client.batches.addJobs({
    id: batch.id,
    jobs: ["1", "2", "3"].map((customId) => jobNamed(customId)),
  });
  expect(blockedBy(added)).toEqual(["none", "none", "none"]);

  await expect
    .poll(async () => (await client.batches.get({ id: batch.id })).jobCounts.failed, {
      timeout: 15_000,
    })
    .toBe(3);
  expect(await client.batches.get({ id: batch.id })).toMatchObject({ status: "active" });
});

test("a Batch's summary counts its jobs by status and adds up their cost, those deleted left out", async () => {
  system.model.setResponses([
    callTool("submit_output", { value: 1 }),
    callTool("submit_output", { value: 2 }),
  ]);
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({ agent: AGENT, breaker: false });
  const { jobs } = await client.batches.addJobs({
    id: batch.id,
    jobs: [jobNamed("1"), jobNamed("2")],
  });
  await expect
    .poll(async () => (await client.batches.get({ id: batch.id })).jobCounts.succeeded, {
      timeout: 15_000,
    })
    .toBe(2);
  const listed = await client.jobs.list({ batchId: batch.id });
  const costs = new Map(listed.jobs.map((each) => [each.id, each.usage.cost.totalUsd]));
  const ended = await client.batches.get({ id: batch.id });
  expect(ended.usage.cost.totalUsd).toBeCloseTo(
    [...costs.values()].reduce((sum, cost) => sum + cost, 0),
    10,
  );
  expect(ended.usage.cost.totalUsd).toBeGreaterThan(0);

  const [deleted, kept] = jobs.map(({ job }) => job.id);
  if (!deleted || !kept) throw new Error("The add answered without the jobs");
  await client.jobs.delete({ id: deleted });
  const afterDelete = await client.batches.get({ id: batch.id });
  expect(afterDelete.jobCounts).toMatchObject({ succeeded: 1 });
  expect(afterDelete.usage.cost.totalUsd).toBeCloseTo(costs.get(kept) ?? Number.NaN, 10);
});

test("cancelling a Batch waits for the Runners of its running jobs, shown as its pendingControl, and then resumes none of them", async () => {
  system.model.setResponses([untilAborted]);
  const client = await system.clientWithNewJobsKey();
  const batch = await client.batches.create({ agent: AGENT, breaker: false });
  const [added] = (await client.batches.addJobs({ id: batch.id, jobs: [jobNamed("1")] })).jobs;
  if (!added) throw new Error("The add answered without the job");
  await untilStatus(client, added.job.id, "running");

  expect(await client.batches.cancel({ id: batch.id })).toMatchObject({
    status: "active",
    pendingControl: { type: "cancel", requestedAt: expect.any(Date) },
  });
  await untilBatchStatus(client, batch.id, "cancelled");
  expect(await client.batches.get({ id: batch.id })).not.toHaveProperty("pendingControl");
  await expect(client.jobs.resume({ id: added.job.id })).rejects.toMatchObject({
    code: "CONFLICT",
  });
});
