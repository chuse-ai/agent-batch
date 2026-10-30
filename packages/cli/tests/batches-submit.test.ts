import { once } from "node:events";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { buffer } from "node:stream/consumers";

import { BracketNotationSerializer } from "@orpc/openapi";
import { expect, onTestFinished, test } from "vite-plus/test";
import * as z from "zod/mini";

import { runCommand } from "./run-command.ts";

const BIN = path.join(import.meta.dirname, "../src/bin.ts");

const addSchema = z.object({
  jobs: z.array(
    z.object({
      customId: z.string(),
      files: z.array(z.object({ path: z.string(), content: z.instanceof(File) })),
    }),
  ),
});
type AddedJob = z.infer<typeof addSchema>["jobs"][number];

function answer(response: ServerResponse, body: unknown) {
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

/** A Batch as the hub answers it, with `jobCounts`. */
function batchOf(id: string, jobCounts: Partial<Record<string, number>> = {}) {
  return {
    id,
    customId: "batch",
    createdAt: new Date().toISOString(),
    status: "active",
    pausedBy: null,
    labels: {},
    breaker: { canary: 5, maxFailurePercent: 10 },
    jobCounts: {
      queued: 0,
      running: 0,
      paused: 0,
      succeeded: 0,
      failed: 0,
      cancelled: 0,
      ...jobCounts,
    },
    usage: { totalTokens: 0, cost: { totalUsd: 0 } },
  };
}

/** A job as the hub answers it, with `status` and whatever goes with it. */
function jobOf(id: string, customId: string, status: Record<string, unknown>) {
  return {
    id,
    customId,
    createdAt: new Date().toISOString(),
    startedAt: null,
    endedAt: null,
    usage: { totalTokens: 0, cost: { totalUsd: 0 } },
    ...status,
  };
}

/**
 * A hub answering `batches submit`: it creates one Batch, keeps the jobs each add sends, and answers
 * each job's read with `ended(customId)`, every job having ended by the first read of the Batch,
 * which it never answers when `stalled`.
 */
async function fakeHub(
  ended: (customId: string) => Record<string, unknown>,
  { stalled = false } = {},
) {
  const batchId = crypto.randomUUID();
  const adds: AddedJob[][] = [];
  const ids = new Map<string, string>();
  const hub = createServer(async (request: IncomingMessage, response) => {
    const body = await buffer(request);
    if (request.method === "POST" && request.url === "/batches") {
      return answer(response, batchOf(batchId));
    }
    if (request.method === "POST" && request.url === `/batches/${batchId}/jobs`) {
      // An add with files is multipart, in bracket notation, as the hub reads it.
      const form = await new Response(body, {
        headers: { "content-type": request.headers["content-type"] ?? "" },
      }).formData();
      const { jobs } = z.parse(
        addSchema,
        new BracketNotationSerializer().deserialize([...form.entries()]),
      );
      adds.push(jobs);
      return answer(response, {
        batch: batchOf(batchId, { queued: ids.size + jobs.length }),
        jobs: jobs.map((job) => {
          const id = crypto.randomUUID();
          ids.set(id, job.customId);
          return { created: true, job: jobOf(id, job.customId, { status: "queued" }) };
        }),
      });
    }
    if (request.method === "GET" && request.url === `/batches/${batchId}`) {
      if (stalled) return;
      return answer(response, batchOf(batchId, { succeeded: ids.size }));
    }
    const jobId = request.url?.match(/^\/jobs\/([^/]+)$/)?.[1];
    const customId = jobId === undefined ? undefined : ids.get(jobId);
    if (request.method === "GET" && jobId !== undefined && customId !== undefined) {
      return answer(response, jobOf(jobId, customId, ended(customId)));
    }
    response.writeHead(404).end();
  });
  hub.on("error", () => {});
  onTestFinished(() => {
    hub.closeAllConnections();
    hub.close();
  });
  hub.listen(0, "127.0.0.1");
  await once(hub, "listening");
  const { port } = z.parse(z.object({ port: z.int() }), hub.address());
  return { url: `http://127.0.0.1:${port}`, adds };
}

/** A new folder holding `files`, by path, and an Output schema. */
async function folderWith(files: Record<string, string | Uint8Array>) {
  const root = await mkdtemp(path.join(tmpdir(), "batches-submit-"));
  for (const [name, content] of Object.entries({ ...files, "schema.json": "{}" })) {
    await mkdir(path.dirname(path.join(root, name)), { recursive: true });
    await writeFile(path.join(root, name), content);
  }
  return root;
}

/** A job's files as an add sent them: each path with its bytes. */
async function contents(job: AddedJob | undefined) {
  return Promise.all(
    (job?.files ?? []).map(async ({ path: filePath, content }) => ({
      path: filePath,
      bytes: new Uint8Array(await content.arrayBuffer()),
    })),
  );
}

function submit(url: string, cwd: string, args: readonly string[]) {
  return runCommand(
    process.execPath,
    [BIN, "batches", "submit", ...args, "--output-schema", "schema.json"],
    { cwd, env: { AGENT_BATCH_URL: url, AGENT_BATCH_API_KEY: "key" }, extendEnv: true },
  );
}

test("batches submit adds a job for each file and folder, named by it, in the order given, at most 100 or 8 MiB at a time", async () => {
  const names = Array.from({ length: 101 }, (_, index) => `invoice-${index}.pdf`);
  const photo = new Uint8Array([0xff, 0xd8, 0x00, 0x80]);
  const big = new Uint8Array(5 * 1024 * 1024);
  const cwd = await folderWith({
    ...Object.fromEntries(names.map((name) => [name, `text of ${name}`])),
    "claim/form.txt": "form",
    "claim/photos/front.jpg": photo,
    "claim/.DS_Store": "hidden",
    "claim/.git/HEAD": "hidden",
    "big-a.bin": big,
    "big-b.bin": big,
  });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }));

  const { exitCode } = await submit(hub.url, cwd, [...names, "claim/", "big-a.bin", "big-b.bin"]);

  expect(exitCode).toBe(0);
  expect(hub.adds.map((jobs) => jobs.length)).toEqual([100, 3, 1]);
  const jobs = hub.adds.flat();
  expect(jobs.map((job) => job.customId)).toEqual([...names, "claim", "big-a.bin", "big-b.bin"]);
  expect(await contents(jobs[0])).toEqual([
    { path: "invoice-0.pdf", bytes: new TextEncoder().encode("text of invoice-0.pdf") },
  ]);
  expect(await contents(jobs[101])).toEqual([
    { path: "form.txt", bytes: new TextEncoder().encode("form") },
    { path: "photos/front.jpg", bytes: photo },
  ]);
});

test("batches submit refuses two paths that name the same job before sending anything", async () => {
  const cwd = await folderWith({ "march/a.pdf": "1", "april/a.pdf": "2" });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }));

  const { exitCode, stderr } = await submit(hub.url, cwd, ["march/a.pdf", "april/a.pdf"]);

  expect(exitCode).toBe(1);
  expect(stderr).toContain("march/a.pdf and april/a.pdf would both be the job a.pdf");
  expect(hub.adds).toEqual([]);
});

test("batches submit --wait prints each job's output or failure in order and exits 1 when one failed", async () => {
  const cwd = await folderWith({ "a.pdf": "a", "b.pdf": "b" });
  const failure = { reason: "noOutput", message: "The agent stopped without an output" };
  const hub = await fakeHub((customId) =>
    customId === "a.pdf"
      ? { status: "succeeded", output: { total: 12 } }
      : { status: "failed", failure },
  );

  const { exitCode, stdout } = await submit(hub.url, cwd, ["a.pdf", "b.pdf", "--wait"]);

  expect(exitCode).toBe(1);
  expect(
    stdout
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line)),
  ).toEqual([
    { customId: "a.pdf", status: "succeeded", output: { total: 12 } },
    { customId: "b.pdf", status: "failed", failure },
  ]);
});

test("batches submit --wait-timeout ends the command when it passes, with 124, even while a read hangs", async () => {
  const cwd = await folderWith({ "a.pdf": "a" });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }), { stalled: true });

  const { exitCode, stderr } = await submit(hub.url, cwd, [
    "a.pdf",
    "--wait",
    "--wait-timeout",
    "1",
  ]);

  expect(exitCode).toBe(124);
  expect(stderr).toContain("Stopped waiting after 1 seconds; the Batch carries on.");
  expect(stderr).not.toContain("Stopped waiting; the Batch carries on.");
});
