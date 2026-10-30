import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import {
  type DesiredState,
  HEARTBEAT_PING,
  HEARTBEAT_PONG,
  type JobReport,
  jobReportSchema,
  registeredRunnerSchema,
  runnerRequests,
} from "contract";
import { startRunner } from "runner";
import { assert, expect, test } from "vite-plus/test";
import { WebSocketServer } from "ws";
import * as z from "zod/mini";

import { entryKinds } from "./entries.ts";
import { callTool, scriptedModel } from "./scripted-model.ts";

/** Whether `report` carries the job's success, the end the Runner writes for its accepted output. */
function carriesSuccess(report: JobReport) {
  return entryKinds(report.events).includes("agent-batch.job.succeeded");
}

// What a Runner reports must reach the hub even when the hub loses it: the connection drops while
// the Runner reports, or the hub's database goes back, as after a failover. The real hub cannot be
// made to do either, so these run a Runner against a stand-in that keeps the job's position and
// applies a report as the real one does, only from that position.

const RUN = {
  jobId: randomUUID(),
  runId: randomUUID(),
  intent: "run" as const,
  job: { instructions: null, input: "hello", files: [], outputContract: { schema: {} } },
  runTimeoutSeconds: 1800,
  modelBudgetUsd: 1,
};

/**
 * Runs a Runner whose job submits an output at once against a stand-in hub that lists the job at
 * `position` on every sync and answers each report as `answer` says: the desired state, or nothing
 * to drop the connection unanswered. Resolves once `done` holds after an answer, with how many
 * calls the job's model received.
 */
async function runAgainstStandIn(
  answer: (jobs: JobReport[], connection: number) => DesiredState | undefined,
  position: () => number,
  done: () => boolean,
) {
  const hub = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  await once(hub, "listening");
  let connections = 0;
  const finished = new Promise<void>((resolve) => {
    hub.on("connection", (socket) => {
      const connection = ++connections;
      socket.addEventListener("message", ({ data: text }) => {
        assert(typeof text === "string", "the Runner sent a binary message");
        if (text === HEARTBEAT_PING) return socket.send(HEARTBEAT_PONG);
        const request = z.parse(
          z.object({ id: z.number(), method: z.string(), params: z.unknown() }),
          JSON.parse(text),
        );
        const reply = (result: unknown) =>
          socket.send(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }));
        if (request.method === "hello") {
          return reply({ syncIntervalMs: 200, reportIntervalMs: 50 });
        }
        if (request.method === "sync") {
          return reply({ runs: [{ ...RUN, position: position() }], deletions: [] });
        }
        if (request.method !== "report") return;
        const jobs = z
          .parse(runnerRequests.report.params, request.params)
          .jobs.map((job) => z.parse(jobReportSchema, job));
        const desired = answer(jobs, connection);
        if (!desired) return socket.terminate();
        reply({ ...desired, refused: [] });
        if (done()) resolve();
      });
    });
  });
  const dataDir = await mkdtemp(path.join(tmpdir(), "runner-"));
  const registration = z.parse(registeredRunnerSchema, { id: "runner", key: "key" });
  await writeFile(path.join(dataDir, "runner.json"), JSON.stringify(registration));
  const address = hub.address();
  if (address === null || typeof address === "string") throw new Error("no port");
  const { faux, models, model } = scriptedModel();
  faux.setResponses([callTool("submit_output", { value: {} })]);
  const runner = await startRunner({
    hubUrl: `http://127.0.0.1:${address.port}`,
    registrationKey: "unused",
    dataDir,
    models,
    model,
  });
  try {
    await finished;
  } finally {
    await runner.stop();
    hub.close();
  }
  return { modelCalls: faux.state.callCount };
}

test("a report the hub never answered is read again on the next connection", async () => {
  let position = 0;
  const applied: JobReport[] = [];
  await runAgainstStandIn(
    (jobs, connection) => {
      // The first connection drops without answering the job's first report.
      if (connection === 1) return undefined;
      for (const report of jobs.filter((job) => job.position === position)) {
        applied.push(report);
        position = report.events.at(-1)?.id ?? position;
      }
      return {
        runs: applied.some(carriesSuccess) ? [] : [{ ...RUN, position }],
        deletions: [],
      };
    },
    () => position,
    () => applied.some(carriesSuccess),
  );
  const kinds = applied.flatMap((report) => entryKinds(report.events));
  // The job's start went with the report that was never answered.
  expect(kinds).toContain("agent-batch.job.started");
  expect(kinds).toContain("agent-batch.job.succeeded");
}, 15_000);

test("a job whose end the hub lost after dropping it ends again with the same events, without running again", async () => {
  let position = 0;
  const ends: JobReport[] = [];
  const { modelCalls } = await runAgainstStandIn(
    (jobs) => {
      for (const report of jobs.filter((job) => job.position === position)) {
        position = report.events.at(-1)?.id ?? position;
        if (!carriesSuccess(report)) continue;
        ends.push(report);
        // The hub stores the first end and drops the run. Its database then goes back to
        // before that end, as a failover loses the last transaction, so it lists the job again.
        if (ends.length === 1) position = report.position;
        return { runs: [], deletions: [] };
      }
      return { runs: [{ ...RUN, position }], deletions: [] };
    },
    () => position,
    () => ends.length === 2,
  );
  expect(ends[1]?.events).toEqual(ends[0]?.events);
  expect(modelCalls).toBe(1);
}, 15_000);
