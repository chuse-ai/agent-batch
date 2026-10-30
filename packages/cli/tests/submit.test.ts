import type { RequestListener } from "node:http";
import { text } from "node:stream/consumers";

import { expect, test } from "vite-plus/test";
import * as z from "zod/mini";

import { runCli } from "./run-cli.ts";

// `jobs submit` creates a Batch of one and adds its job under one custom ID, and retries a call that
// got no answer with that ID, so the hub accepts the Batch and the job at most once; the hub's own
// tests check that one custom ID makes one Batch and one job.

test("jobs submit retries an add whose connection dropped with the custom ID it printed and created the Batch with", async () => {
  const batchId = crypto.randomUUID();
  const created: unknown[] = [];
  const added: unknown[] = [];
  // Answers the Batch's creation, then reads each add and drops its connection without answering.
  const hub: RequestListener = async (request, response) => {
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
  };
  const { exitCode, stderr } = await runCli(
    [
      "jobs",
      "submit",
      "--json",
      JSON.stringify({
        agent: { outputContract: { schema: { type: "integer" } } },
        input: "Count to ten.",
      }),
    ],
    { hub },
  );

  expect(created).toHaveLength(1);
  expect(added).toHaveLength(3);
  expect(new Set([...created, ...added]).size).toBe(1);
  expect(exitCode).toBe(1);
  expect(stderr).toContain(`Submitting with custom ID ${String(created[0])}`);
});
