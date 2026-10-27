import { randomUUID } from "node:crypto";

import type { JobEvent, JobSubmission } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { entryKinds } from "./entries.ts";
import { type ApiClient, startSystem } from "./harness.ts";
import { callTool, untilAborted } from "./scripted-model.ts";

// How callers read a job's events: in pages after the last event they have, during and after the
// run, until a page says no more will come.

const system = startSystem();

beforeAll(async () => {
  await system.start();
  await system.startRunner();
});
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Greet the reader.",
  inputFiles: {},
  resultContract: {
    schema: {
      type: "object",
      properties: { greeting: { type: "string" } },
      required: ["greeting"],
    },
  },
} satisfies JobSubmission;

/** An ended job's events from the start, read `limit` at a time until a page has no more. */
async function readAll(client: ApiClient, id: string, limit: number) {
  const events: JobEvent[] = [];
  for (;;) {
    const page = await client.jobs.events({ id, after: events.at(-1)?.id ?? 0, limit });
    events.push(...page.events);
    if (!page.hasMore && page.status === "succeeded") return events;
    // The job has ended, so a page that is not the last says more is stored.
    expect(page.hasMore).toBe(true);
  }
}

test("a finished job's events, read in pages, run from its start to its end without gaps or repeats", async () => {
  system.model.setResponses([
    callTool("write", { path: "out/greeting.txt", content: "Hello" }),
    callTool("submit_result", { value: { greeting: "Hello" } }),
  ]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await expect
    .poll(async () => (await client.jobs.get({ id })).status, { timeout: 10_000 })
    .toBe("succeeded");

  const events = await readAll(client, id, 100);
  expect(await readAll(client, id, 2)).toEqual(events);
  expect(events[0]).toMatchObject({ type: "conversation" });
  expect(entryKinds(events)).toContain("pi.tool-result");
  expect(entryKinds(events)[0]).toBe("agent-batch.job.started");
  expect(events.at(-1)).toMatchObject({
    type: "entry",
    record: { kind: "agent-batch.job.succeeded", data: { result: { greeting: "Hello" } } },
  });
});

test("reading the events of a job the hub does not have fails with 404", async () => {
  const client = await system.clientWithNewJobsKey();
  await expect(client.jobs.events({ id: randomUUID() })).rejects.toMatchObject({
    code: "NOT_FOUND",
  });
});

// Last in the file: the job runs until the system stops.
test("a running job's pages say it is running", async () => {
  system.model.setResponses([untilAborted]);
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit(SUBMISSION);
  await expect
    .poll(async () => entryKinds((await client.jobs.events({ id })).events), { timeout: 10_000 })
    .toContain("agent-batch.job.started");

  const { events } = await client.jobs.events({ id });
  const page = await client.jobs.events({ id, after: events.at(-1)?.id ?? 0 });
  expect(page).toMatchObject({ hasMore: false, status: "running" });
});
