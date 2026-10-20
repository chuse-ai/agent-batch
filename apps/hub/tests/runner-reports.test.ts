import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import {
  HEARTBEAT_PING,
  HEARTBEAT_PONG,
  type JobReport,
  registeredRunnerSchema,
  runnerMethods,
} from "contract";
import { startRunner } from "runner";
import { assert, expect, test } from "vite-plus/test";
import { WebSocketServer } from "ws";
import * as z from "zod/mini";

import { callTool, scriptedModel } from "./scripted-model.ts";

// A job's outcome must reach the hub even when the connection drops while the Runner reports it, or
// the job looks stuck. The hub here is a stand-in, since the real one cannot be made to drop a
// connection between receiving a report and answering it.
test("a report the hub never answered goes again on the next connection", async () => {
  const placement = {
    jobId: randomUUID(),
    generation: 1,
    intent: "run" as const,
    job: { prompt: "hello", inputFiles: {}, outputContract: { schema: {} } },
  };
  const hub = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  await once(hub, "listening");
  let connections = 0;
  const reportedAgain = new Promise<JobReport[]>((resolve) => {
    hub.on("connection", (socket) => {
      const connection = ++connections;
      socket.addEventListener("message", ({ data: text }) => {
        assert(typeof text === "string", "the Runner sent a binary message");
        if (text === HEARTBEAT_PING) return socket.send(HEARTBEAT_PONG);
        const request = z.parse(
          z.object({ id: z.number(), method: z.string(), params: z.unknown() }),
          JSON.parse(text),
        );
        const answer = (result: unknown) =>
          socket.send(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }));
        if (request.method === "hello") return answer({ syncIntervalMs: 60_000 });
        if (request.method === "sync") return answer({ placements: [placement] });
        if (request.method !== "report") return;
        // The first connection drops without answering the job's report.
        if (connection === 1) return socket.terminate();
        resolve(z.parse(runnerMethods.report.params, request.params).jobs);
        answer({ placements: [] });
      });
    });
  });
  const dataDir = await mkdtemp(path.join(tmpdir(), "runner-"));
  const registration = z.parse(registeredRunnerSchema, { id: "runner", key: "key" });
  await writeFile(path.join(dataDir, "runner.json"), JSON.stringify(registration));
  const address = hub.address();
  if (address === null || typeof address === "string") throw new Error("no port");
  const { faux, models, model } = scriptedModel();
  faux.setResponses([callTool("submit_result", { value: {} })]);
  const runner = await startRunner({
    hubUrl: `http://127.0.0.1:${address.port}`,
    registrationKey: "unused",
    dataDir,
    models,
    model,
  });
  try {
    const reports = await reportedAgain;
    expect(reports.map((report) => [report.jobId, report.outcome.state])).toContainEqual([
      placement.jobId,
      "succeeded",
    ]);
  } finally {
    await runner.stop();
    hub.close();
  }
}, 15_000);
