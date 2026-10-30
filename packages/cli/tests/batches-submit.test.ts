import { createHash } from "node:crypto";
import { once } from "node:events";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { buffer } from "node:stream/consumers";

import { expect, onTestFinished, test } from "vite-plus/test";
import * as z from "zod/mini";

import { runCommand } from "./run-command.ts";

const BIN = path.join(import.meta.dirname, "../src/bin.ts");

const addSchema = z.object({
  jobs: z.array(
    z.object({
      customId: z.string(),
      input: z.optional(z.string()),
      files: z.optional(
        z.array(z.object({ path: z.string(), digest: z.string(), sizeBytes: z.int() })),
      ),
    }),
  ),
});
type AddedJob = z.infer<typeof addSchema>["jobs"][number];

const checkSchema = z.object({
  files: z.array(z.object({ digest: z.string(), sizeBytes: z.int() })),
});

function digestOf(content: string | Uint8Array) {
  return `sha256:${createHash("sha256").update(content).digest("hex")}`;
}

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
 * A hub answering `batches submit`: it holds the Blobs with the digests in `stored` and those
 * uploaded to it, keeps each upload's bytes and each add's jobs, creates one Batch, and answers
 * each job's read with `ended(customId)`, every job having ended by the first read of the Batch,
 * which it never answers when `stalled`.
 */
async function fakeHub(
  ended: (customId: string) => Record<string, unknown>,
  { stalled = false, stored = [] as string[] } = {},
) {
  const batchId = crypto.randomUUID();
  const held = new Set(stored);
  const uploads: { digest: string; bytes: Uint8Array }[] = [];
  const adds: AddedJob[][] = [];
  const ids = new Map<string, string>();
  const hub = createServer(async (request: IncomingMessage, response) => {
    const body = await buffer(request);
    const json = (): unknown => JSON.parse(body.toString());
    if (request.method === "POST" && request.url === "/blobs/check") {
      const { files } = z.parse(checkSchema, json());
      return answer(response, {
        files: files.map((file) =>
          held.has(file.digest)
            ? { ...file, stored: true }
            : { ...file, stored: false, upload: { transfer: "put", url: `/blobs/${file.digest}` } },
        ),
      });
    }
    // The client escapes the digest's `:` in the path.
    const uploaded = decodeURIComponent(request.url ?? "").match(
      /^\/blobs\/(sha256:[0-9a-f]+)$/,
    )?.[1];
    if (request.method === "PUT" && uploaded !== undefined) {
      uploads.push({ digest: uploaded, bytes: new Uint8Array(body) });
      held.add(uploaded);
      return answer(response, { digest: uploaded, sizeBytes: body.length });
    }
    if (request.method === "POST" && request.url === "/batches") {
      return answer(response, batchOf(batchId));
    }
    if (request.method === "POST" && request.url === `/batches/${batchId}/jobs`) {
      const { jobs } = z.parse(addSchema, json());
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
      const counts: Record<string, number> = {};
      for (const each of ids.values()) {
        const status = String(ended(each)["status"]);
        counts[status] = (counts[status] ?? 0) + 1;
      }
      return answer(response, batchOf(batchId, counts));
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
  return { url: `http://127.0.0.1:${port}`, uploads, adds };
}

/** A new folder holding `files`, by path, and an Output schema. */
async function folderWith(files: Record<string, string | Uint8Array>) {
  const root = await mkdtemp(path.join(tmpdir(), "batches-submit-"));
  onTestFinished(() => rm(root, { recursive: true }));
  for (const [name, content] of Object.entries({ ...files, "schema.json": "{}" })) {
    await mkdir(path.dirname(path.join(root, name)), { recursive: true });
    await writeFile(path.join(root, name), content);
  }
  return root;
}

function submit(url: string, cwd: string, args: readonly string[]) {
  return runCommand(
    process.execPath,
    [BIN, "batches", "submit", ...args, "--output-schema", "schema.json"],
    { cwd, env: { AGENT_BATCH_URL: url, AGENT_BATCH_API_KEY: "key" }, extendEnv: true },
  );
}

test("batches submit --each makes a job of each file and folder, named by its path, and uploads only the Blobs the hub lacks, each once", async () => {
  const photo = new Uint8Array([0xff, 0xd8, 0x00, 0x80]);
  const cwd = await folderWith({
    "invoices/a.pdf": "same",
    "invoices/b.pdf": "same",
    "invoices/c.pdf": "held",
    "claim/form.txt": "form",
    "claim/photos/front.jpg": photo,
    "claim/.DS_Store": "hidden",
    "claim/.git/HEAD": "hidden",
    ".env": "hidden",
  });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }), {
    stored: [digestOf("held")],
  });

  const { exitCode } = await submit(hub.url, cwd, [
    "--each",
    "./invoices/a.pdf",
    "invoices/b.pdf",
    "invoices//c.pdf",
    "claim/",
    ".env",
  ]);

  expect(exitCode).toBe(0);
  expect(hub.adds.flat()).toEqual([
    {
      customId: "invoices/a.pdf",
      files: [{ path: "a.pdf", digest: digestOf("same"), sizeBytes: 4 }],
    },
    {
      customId: "invoices/b.pdf",
      files: [{ path: "b.pdf", digest: digestOf("same"), sizeBytes: 4 }],
    },
    {
      customId: "invoices/c.pdf",
      files: [{ path: "c.pdf", digest: digestOf("held"), sizeBytes: 4 }],
    },
    {
      customId: "claim",
      files: [
        { path: "form.txt", digest: digestOf("form"), sizeBytes: 4 },
        { path: "photos/front.jpg", digest: digestOf(photo), sizeBytes: 4 },
      ],
    },
  ]);
  expect(hub.uploads.toSorted((a, b) => a.digest.localeCompare(b.digest))).toEqual(
    [
      { digest: digestOf("same"), bytes: new TextEncoder().encode("same") },
      { digest: digestOf("form"), bytes: new TextEncoder().encode("form") },
      { digest: digestOf(photo), bytes: photo },
    ].toSorted((a, b) => a.digest.localeCompare(b.digest)),
  );
});

test("batches submit adds the jobs of a JSONL file in its order, at most 100 at a time", async () => {
  const lines = Array.from({ length: 101 }, (_, index) =>
    JSON.stringify({ customId: `posting-${index}`, input: `Posting ${index}.` }),
  );
  const cwd = await folderWith({ "jobs.jsonl": `${lines.join("\n")}\n` });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }));

  const { exitCode } = await submit(hub.url, cwd, ["jobs.jsonl"]);

  expect(exitCode).toBe(0);
  expect(hub.adds.map((jobs) => jobs.length)).toEqual([100, 1]);
  expect(hub.adds.flat().map((job) => job.customId)).toEqual(
    lines.map((_, index) => `posting-${index}`),
  );
});

// Root reads any folder, so the refusal shows only for another user.
test.skipIf(process.getuid?.() === 0)(
  "batches submit --each refuses a folder whose tree it cannot read before sending anything",
  async () => {
    const cwd = await folderWith({ "claim/form.txt": "form", "claim/2024/scan.txt": "scan" });
    const locked = path.join(cwd, "claim/2024");
    await chmod(locked, 0o000);
    const hub = await fakeHub(() => ({ status: "succeeded", output: {} }));

    const refused = await submit(hub.url, cwd, ["--each", "claim"]).finally(() =>
      chmod(locked, 0o755),
    );

    expect(refused).toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining("claim/2024 cannot be read"),
    });
    expect(hub.adds).toEqual([]);
  },
);

test("batches submit run again with its --custom-id sends the same jobs, so the hub adds only those it lacks", async () => {
  const cwd = await folderWith({ "a.pdf": "a", "claim/form.txt": "form" });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }));
  const args = ["--each", "a.pdf", "claim", "--custom-id", "march"];

  expect((await submit(hub.url, cwd, args)).exitCode).toBe(0);
  expect((await submit(hub.url, cwd, args)).exitCode).toBe(0);

  expect(hub.adds).toHaveLength(2);
  expect(hub.adds[1]).toEqual(hub.adds[0]);
});

test("batches submit --wait --output-dir saves each job's line in order, those that succeeded apart from the rest, prints the Batch and exits 1 when one failed", async () => {
  const cwd = await folderWith({ "a.pdf": "a", "b.pdf": "b", "c.pdf": "c" });
  const failure = { reason: "noOutput", message: "The agent stopped without an output" };
  const hub = await fakeHub((customId) =>
    customId === "b.pdf"
      ? { status: "failed", failure }
      : { status: "succeeded", output: { total: customId } },
  );
  const args = ["--each", "a.pdf", "b.pdf", "c.pdf", "--wait", "--output-dir", "outputs"];

  const { exitCode, stdout } = await submit(hub.url, cwd, args);

  expect(exitCode).toBe(1);
  expect(JSON.parse(stdout)).toMatchObject({ jobCounts: { succeeded: 2, failed: 1 } });
  const lines = async (name: string) =>
    (await readFile(path.join(cwd, "outputs", name), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
  expect(await lines("output.jsonl")).toEqual([
    { customId: "a.pdf", status: "succeeded", output: { total: "a.pdf" } },
    { customId: "c.pdf", status: "succeeded", output: { total: "c.pdf" } },
  ]);
  expect(await lines("error.jsonl")).toEqual([{ customId: "b.pdf", status: "failed", failure }]);
});

test("batches submit --output-dir refuses a folder already holding a result file before sending anything", async () => {
  const cwd = await folderWith({ "a.pdf": "a", "outputs/error.jsonl": "" });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }));

  const refused = await submit(hub.url, cwd, [
    "--each",
    "a.pdf",
    "--wait",
    "--output-dir",
    "outputs",
  ]);

  expect(refused).toMatchObject({
    exitCode: 1,
    stderr: expect.stringContaining(`${path.join("outputs", "error.jsonl")} already exists`),
  });
  expect(hub.uploads).toEqual([]);
  expect(hub.adds).toEqual([]);
});

test("batches submit --wait-timeout ends the command when it passes, with 124, even while a read hangs", async () => {
  const cwd = await folderWith({ "a.pdf": "a" });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }), { stalled: true });

  const { exitCode, stderr } = await submit(hub.url, cwd, [
    "--each",
    "a.pdf",
    "--wait",
    "--wait-timeout",
    "1s",
  ]);

  expect(exitCode).toBe(124);
  expect(stderr).toContain("Stopped waiting after 1s; the Batch carries on.");
  expect(stderr).not.toContain("Stopped waiting; the Batch carries on.");
});

// A duration with no unit is refused, so seconds given as a number never wait a thousandth as long.
test("batches submit refuses a --wait-timeout without a unit before sending anything", async () => {
  const cwd = await folderWith({ "a.pdf": "a" });
  const hub = await fakeHub(() => ({ status: "succeeded", output: {} }));

  const refused = await submit(hub.url, cwd, [
    "--each",
    "a.pdf",
    "--wait",
    "--wait-timeout",
    "1800",
  ]);

  expect(refused.exitCode).not.toBe(0);
  expect(refused.stderr).toContain("A duration needs a unit, such as 30m or 1h30m");
  expect(hub.adds).toEqual([]);
});
