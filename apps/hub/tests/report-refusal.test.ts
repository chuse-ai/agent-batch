import { createJsonRpcClient, hubNotifications, JOB_SUCCEEDED, runnerRequests } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";
import type WebSocket from "ws";

import { type OneJob, startSystem, submitJob } from "./harness.ts";

// A Runner's report carries every job it holds, so the hub reads each job's on its own: one it
// cannot apply is refused alone, and the Runner's other jobs keep reporting. A failure the job's
// journal could not record travels beside its events. No Runner of the system's runs, so the jobs
// are placed on a socket that speaks the protocol directly.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  input: "Count to three.",
  inputFiles: {},
  outputContract: { schema: { type: "integer" } },
} satisfies OneJob;

/** A Runner's own entry, first in its job's journal. */
function entry(kind: string, data: unknown) {
  return { id: 1, type: "entry", record: { kind, data } };
}

// Node 22's JSON.stringify overflows on the 5,000-level fixture before it reaches the hub.
// Let the protocol client serialize this marker, then replace its quoted JSON string with
// the nested JSON text before sending, so the test exercises the hub's depth check.
const NESTED_OUTPUT = "__nested_output__";

/** A Runner's side of the protocol over `socket`. */
function protocolClient(socket: WebSocket, depth: number | undefined) {
  const client = createJsonRpcClient(runnerRequests, hubNotifications, { syncNow() {} }, (text) =>
    socket.send(
      depth === undefined
        ? text
        : text.replace(
            JSON.stringify(NESTED_OUTPUT),
            '{"child":'.repeat(depth) + "0" + "}".repeat(depth),
          ),
    ),
  );
  socket.addEventListener("message", (event) => {
    // `ws` types a message as text or binary; the protocol is text (ADR 0026).
    if (typeof event.data === "string") client.receive(event.data);
  });
  return client;
}

test.each([
  { problem: "missing output", depth: undefined },
  { problem: "output nested 65 containers deep", depth: 65 },
  { problem: "output nested 5,000 containers deep", depth: 5_000 },
])("a $problem refuses only that job's report", async ({ depth }) => {
  const client = await system.clientWithNewJobsKey();
  const output = JSON.parse('{"child":'.repeat(64) + "0" + "}".repeat(64));
  const good = await submitJob(client, { ...SUBMISSION, outputContract: { schema: {} } });
  const bad = await submitJob(client, SUBMISSION);
  const { socket } = await system.connectRunner();
  const runner = protocolClient(socket, depth);

  const { runs } = await runner.request("sync", { held: [], free: 2 }, 10_000);
  const held = runs.map(({ jobId, runId }) => ({ jobId, runId }));
  const runOf = (jobId: string) => {
    const run = held.find((ref) => ref.jobId === jobId);
    if (!run) throw new Error(`the job ${jobId} was not placed`);
    return run;
  };

  const answer = await runner.request(
    "report",
    {
      held,
      free: 0,
      deleted: [],
      jobs: [
        {
          ...runOf(good.id),
          intent: "run",
          position: 0,
          events: [
            entry(JOB_SUCCEEDED, {
              timestamp: Date.now(),
              output,
            }),
          ],
        },
        {
          ...runOf(bad.id),
          intent: "run",
          position: 0,
          events: [
            entry(JOB_SUCCEEDED, {
              timestamp: Date.now(),
              ...(depth === undefined ? {} : { output: NESTED_OUTPUT }),
            }),
          ],
        },
      ],
    },
    10_000,
  );

  expect(answer.refused).toEqual([
    {
      ...runOf(bad.id),
      reason:
        depth === undefined
          ? expect.any(String)
          : expect.stringContaining("JSON must not nest more than 64 containers"),
    },
  ]);
  expect(await client.jobs.get({ id: good.id })).toMatchObject({ status: "succeeded", output });
  expect((await client.jobs.get({ id: bad.id })).status).toBe("queued");
  socket.close(1000);
});

test("a failure the job's journal did not record fails the job with its reason and message", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id } = await submitJob(client, SUBMISSION);
  const { socket } = await system.connectRunner();
  const runner = protocolClient(socket, undefined);
  const { runs } = await runner.request("sync", { held: [], free: 1 }, 10_000);
  const held = runs.map(({ jobId, runId }) => ({ jobId, runId }));
  const failure = {
    timestamp: Date.now(),
    reason: "runFaulted" as const,
    message: "The job's journal could not be opened",
  };

  await runner.request(
    "report",
    {
      held,
      free: 0,
      deleted: [],
      jobs: held.map((run) => ({ ...run, intent: "run", position: 0, events: [], failure })),
    },
    10_000,
  );

  expect(await client.jobs.get({ id })).toMatchObject({
    status: "failed",
    endedAt: new Date(failure.timestamp),
    failure: { reason: "runFaulted", message: failure.message },
  });
  socket.close(1000);
});
