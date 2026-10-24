import { once } from "node:events";
import { createServer } from "node:http";
import path from "node:path";
import { text } from "node:stream/consumers";

import { expect, onTestFinished, test } from "vite-plus/test";
import * as z from "zod/mini";

import { runCommand } from "./run-command.ts";

// `jobs submit` retries a submission that got no answer with the key it was first sent with, so the
// hub accepts the job at most once; the hub's own tests check that one key makes one job.

const BIN = path.join(import.meta.dirname, "../src/bin.ts");

test("jobs submit retries a submission whose connection dropped with the key it printed", async () => {
  const keys: unknown[] = [];
  // Reads each submission, then drops its connection without answering.
  const hub = createServer(async (request) => {
    keys.push(
      z.parse(z.object({ idempotencyKey: z.string() }), JSON.parse(await text(request)))
        .idempotencyKey,
    );
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
      "--prompt",
      "Count to ten.",
      "--result-contract",
      '{"schema":{"type":"integer"}}',
    ],
    {
      env: { AGENT_BATCH_URL: `http://127.0.0.1:${port}`, AGENT_BATCH_API_KEY: "key" },
      extendEnv: true,
    },
  );

  expect(keys).toHaveLength(3);
  expect(new Set(keys).size).toBe(1);
  expect(exitCode).toBe(1);
  expect(stderr).toContain(`Submitting with idempotency key ${String(keys[0])}`);
});
