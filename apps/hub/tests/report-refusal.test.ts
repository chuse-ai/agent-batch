import {
  createProtocolClient,
  hubNotifications,
  JOB_SUCCEEDED,
  type JobSubmission,
  runnerMethods,
} from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";
import type WebSocket from "ws";

import { startSystem } from "./harness.ts";

// A Runner's report carries every job it holds, so the hub reads each job's on its own: one it
// cannot apply is refused alone, and the Runner's other jobs keep reporting. No Runner of
// the system's runs, so the jobs are placed on a socket that speaks the protocol directly.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Count to three.",
  inputFiles: {},
  resultContract: { schema: { type: "integer" } },
} satisfies JobSubmission;

/** A Runner's own entry, first in its job's journal. */
function entry(kind: string, data: unknown) {
  return { id: 1, type: "entry", record: { kind, data } };
}

/** A Runner's side of the protocol over `socket`, as the Runner speaks it. */
function protocolClient(socket: WebSocket) {
  const client = createProtocolClient(runnerMethods, hubNotifications, { syncNow() {} }, (text) =>
    socket.send(text),
  );
  socket.addEventListener("message", (event) => {
    // `ws` types a message as text or binary; the protocol is text (ADR 0026).
    if (typeof event.data === "string") client.receive(event.data);
  });
  return client;
}

test.each([
  { problem: "missing result", depth: undefined },
  { problem: "result nested 65 containers deep", depth: 65 },
  { problem: "result nested 5,000 containers deep", depth: 5_000 },
])("a $problem refuses only that job's report", async ({ depth }) => {
  const client = await system.clientWithNewJobsKey();
  const result = JSON.parse('{"child":'.repeat(64) + "0" + "}".repeat(64));
  const good = await client.jobs.submit({ ...SUBMISSION, resultContract: { schema: {} } });
  const bad = await client.jobs.submit(SUBMISSION);
  const { socket } = await system.connectRunner();
  const runner = protocolClient(socket);

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
              result,
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
              ...(depth === undefined
                ? {}
                : { result: JSON.parse('{"child":'.repeat(depth) + "0" + "}".repeat(depth)) }),
            }),
          ],
        },
      ],
    },
    10_000,
  );

  expect(answer.refused).toEqual([{ ...runOf(bad.id), reason: expect.any(String) }]);
  expect(await client.jobs.get({ id: good.id })).toMatchObject({ status: "succeeded", result });
  expect((await client.jobs.get({ id: bad.id })).status).toBe("queued");
  socket.close(1000);
});
