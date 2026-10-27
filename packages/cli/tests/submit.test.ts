import { once } from "node:events";
import { createServer } from "node:http";
import path from "node:path";
import { text } from "node:stream/consumers";

import { expect, onTestFinished, test } from "vite-plus/test";
import * as z from "zod/mini";

import { runCommand } from "./run-command.ts";

// `jobs submit` creates a Batch of one and adds its job under one custom ID, and retries a call that
// got no answer with that ID, so the hub accepts the Batch and the job at most once; the hub's own
// tests check that one custom ID makes one Batch and one job.

const BIN = path.join(import.meta.dirname, "../src/bin.ts");

test("jobs submit retries an add whose connection dropped with the custom ID it printed and created the Batch with", async () => {
  const batchId = crypto.randomUUID();
  const created: unknown[] = [];
  const added: unknown[] = [];
  // Answers the Batch's creation, then reads each add and drops its connection without answering.
  const hub = createServer(async (request, response) => {
    const body: unknown = JSON.parse(await text(request));
    if (request.url === "/batches") {
      const { customId } = z.parse(z.object({ customId: z.string() }), body);
      created.push(customId);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          id: batchId,
          customId,
          createdAt: new Date().toISOString(),
          status: "active",
          pausedBy: null,
          labels: {},
          breaker: false,
          jobCounts: { queued: 0, running: 0, paused: 0, succeeded: 0, failed: 0, cancelled: 0 },
          usage: { totalTokens: 0, cost: { totalUsd: 0 } },
        }),
      );
      return;
    }
    expect(request.url).toBe(`/batches/${batchId}/jobs`);
    const { jobs } = z.parse(z.object({ jobs: z.array(z.object({ customId: z.string() })) }), body);
    added.push(...jobs.map((job) => job.customId));
    request.socket.destroy();
  });
  hub.on("error", () => {});
  onTestFinished(() => {
    hub.close();
  });
  hub.listen(0, "127.0.0.1");
  await once(hub, "listening");
  const { port } = z.parse(z.object({ port: z.int() }), hub.address());
  const { exitCode, stderr } = await runCommand(
    process.execPath,
    [
      BIN,
      "jobs",
      "submit",
      "--json",
      JSON.stringify({
        agent: { resultContract: { schema: { type: "integer" } } },
        input: "Count to ten.",
      }),
    ],
    {
      env: { AGENT_BATCH_URL: `http://127.0.0.1:${port}`, AGENT_BATCH_API_KEY: "key" },
      extendEnv: true,
    },
  );

  expect(created).toHaveLength(1);
  expect(added).toHaveLength(3);
  expect(new Set([...created, ...added]).size).toBe(1);
  expect(exitCode).toBe(1);
  expect(stderr).toContain(`Submitting with custom ID ${String(created[0])}`);
});
