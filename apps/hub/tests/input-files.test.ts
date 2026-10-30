import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { zstdDecompressSync } from "node:zlib";

import { createJsonRpcClient, hubNotifications, runnerRequests } from "contract";
import { sql } from "kysely";
import { afterAll, assert, beforeAll, expect, inject, test } from "vite-plus/test";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { type OneJob, startSystem, submitJob } from "./harness.ts";
import { callTool } from "./scripted-model.ts";

// A job's Input files reach its Runner apart from the hub's answers, which name them only: the
// Runner the job is placed on downloads each a chunk at a time and puts it in the Workspace's
// `inputs/` folder before the agent starts.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  input: "Read the report.",
  outputContract: { schema: {} },
} satisfies OneJob;

/** An Input file at `path` holding `content`. */
function fileAt(filePath: string, content: string | Uint8Array<ArrayBuffer>) {
  return { path: filePath, content: new File([content], path.basename(filePath)) };
}

/** Text of more than two chunks, with characters of several bytes across the chunks' boundaries. */
const LARGE = "Grüße, 世界! ".repeat(150_000);

function sha256(content: string | Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}

/** A Runner's side of the protocol on a socket the test opens itself. */
async function connectProtocolRunner() {
  const { key, socket } = await system.connectRunner();
  const runner = createJsonRpcClient(runnerRequests, hubNotifications, { syncNow() {} }, (text) =>
    socket.send(text),
  );
  socket.addEventListener("message", (event) => {
    if (typeof event.data === "string") runner.receive(event.data);
  });
  return { runner, api: system.runnerClient({ "x-api-key": key }), close: () => socket.close() };
}

test("a sync names a job's Input files, and only the Runner the job is placed on reads them, a chunk at a time", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id } = await submitJob(client, {
    ...SUBMISSION,
    files: [fileAt("report.txt", LARGE), fileAt("empty.txt", "")],
  });
  const placed = await connectProtocolRunner();
  const other = await connectProtocolRunner();
  try {
    const { runs } = await placed.runner.request("sync", { held: [], free: 1 }, 10_000);

    const listed = runs.find((run) => run.jobId === id)?.job?.files;
    expect(listed).toEqual(
      expect.arrayContaining([
        {
          path: "report.txt",
          sha256: sha256(LARGE),
          originalSizeBytes: Buffer.byteLength(LARGE),
          chunkBytes: expect.any(Number),
        },
        {
          path: "empty.txt",
          sha256: sha256(""),
          originalSizeBytes: 0,
          chunkBytes: expect.any(Number),
        },
      ]),
    );
    const chunkBytes = listed?.find((file) => file.path === "report.txt")?.chunkBytes;
    assert(chunkBytes !== undefined);
    const count = Math.ceil(Buffer.byteLength(LARGE) / chunkBytes);
    const chunks: Buffer[] = [];
    for (let index = 0; index < count; index++) {
      const chunk = await placed.api.getInputFileChunk({ id, path: "report.txt", index });
      assert(chunk instanceof Blob, "a chunk arrives as a file");
      chunks.push(zstdDecompressSync(await chunk.arrayBuffer()));
    }
    expect(chunks.slice(0, -1).every((chunk) => chunk.length === chunkBytes)).toBe(true);
    expect(Buffer.concat(chunks).toString("utf8")).toBe(LARGE);
    await expect(
      placed.api.getInputFileChunk({ id, path: "report.txt", index: count }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
    await expect(
      other.api.getInputFileChunk({ id, path: "report.txt", index: 0 }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  } finally {
    placed.close();
    other.close();
  }
});

test("one add stores and serves every Input file across many jobs", async () => {
  const client = await system.clientWithNewJobsKey();
  const shared = "Shared across the add.";
  // An Output schema of one type, which a multipart request carries as it is.
  const batch = await client.batches.create({
    agent: {
      outputContract: { schema: { type: "string" } },
      files: [fileAt("shared.txt", shared)],
    },
    breaker: false,
  });
  const contents = (index: number) => ({
    "own.txt": index === 0 ? `${LARGE}First job.` : `File for job ${index}.`,
    "copy.txt": shared,
    "empty.txt": "",
  });
  const jobs = Array.from({ length: 100 }, (_, index) => ({
    customId: `files-${index}`,
    input: "Read the files.",
    files: Object.entries(contents(index)).map(([filePath, content]) => fileAt(filePath, content)),
  }));
  const added = await client.batches.addJobs({ id: batch.id, jobs });
  expect(added.jobs).toHaveLength(jobs.length);
  expect(added.jobs.every(({ created }) => created)).toBe(true);
  const placed = await connectProtocolRunner();
  try {
    const { runs } = await placed.runner.request("sync", { held: [], free: jobs.length }, 10_000);
    for (const index of jobs.keys()) {
      const addedJob = added.jobs[index];
      assert(addedJob);
      const id = addedJob.job.id;
      const listed = runs.find((run) => run.jobId === id)?.job?.files;
      assert(listed);
      const expected = { ...contents(index), "shared.txt": shared };
      expect(listed.map((file) => file.path).toSorted()).toEqual(Object.keys(expected).toSorted());
      for (const [filePath, content] of Object.entries(expected)) {
        const file = listed.find((entry) => entry.path === filePath);
        assert(file);
        expect(file).toMatchObject({
          sha256: sha256(content),
          originalSizeBytes: Buffer.byteLength(content),
        });
        const chunks: Buffer[] = [];
        const count = Math.ceil(file.originalSizeBytes / file.chunkBytes);
        for (let chunkIndex = 0; chunkIndex < count; chunkIndex++) {
          const chunk = await placed.api.getInputFileChunk({
            id,
            path: filePath,
            index: chunkIndex,
          });
          assert(chunk instanceof Blob);
          chunks.push(zstdDecompressSync(await chunk.arrayBuffer()));
        }
        expect(Buffer.concat(chunks).toString("utf8")).toBe(content);
      }
    }
  } finally {
    await client.batches.cancel({ id: batch.id });
    placed.close();
  }
}, 30_000);

test("deleting a job deletes the content of its Input files that no other job uses", async () => {
  const client = await system.clientWithNewJobsKey();
  const shared = "Shared between both jobs.";
  const first = await submitJob(client, {
    ...SUBMISSION,
    files: [fileAt("shared.txt", shared), fileAt("own.txt", "Only the first job's.")],
  });
  const second = await submitJob(client, { ...SUBMISSION, files: [fileAt("copy.txt", shared)] });
  // No API shows blobs, which the hub keeps once however many jobs use them.
  const all = [sha256(shared), sha256("Only the first job's.")].toSorted();
  await using db = openDatabase(inject("databaseUrl"));
  const stored = async () =>
    (
      await sql<{ sha256: string }>`
        select encode(sha256, 'hex') as sha256 from ${sql.table(`${DB_SCHEMA}.blob`)}
      `.execute(db)
    ).rows
      .map((row) => row.sha256)
      .filter((address) => all.includes(address))
      .toSorted();
  expect(await stored()).toEqual(all);

  // Jobs never sent to a Runner end at once when cancelled.
  await client.jobs.cancel({ id: first.id });
  await client.jobs.delete({ id: first.id });
  expect(await stored()).toEqual([sha256(shared)]);

  await client.jobs.cancel({ id: second.id });
  await client.jobs.delete({ id: second.id });
  expect(await stored()).toEqual([]);
});

test("the Runner puts a job's Input files in its Workspace as they were submitted, at their paths", async () => {
  system.model.setResponses([callTool("submit_output", { value: "done" })]);
  const client = await system.clientWithNewJobsKey();
  // Bytes that are not UTF-8, at a path of folders and characters of several bytes.
  const binary = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x00, 0xff, 0xfe, 0x80]);
  await system.startRunner();
  try {
    const { id } = await submitJob(client, {
      ...SUBMISSION,
      files: [
        fileAt("report.txt", LARGE),
        fileAt("empty.txt", ""),
        fileAt("claims/42/façade.pdf", binary),
      ],
    });
    await expect
      .poll(async () => (await client.jobs.get({ id })).status, { timeout: 15_000 })
      .toBe("succeeded");

    const inputs = path.join(system.workspace(id), "inputs");
    expect(await readFile(path.join(inputs, "report.txt"), "utf8")).toBe(LARGE);
    expect(await readFile(path.join(inputs, "empty.txt"), "utf8")).toBe("");
    expect(new Uint8Array(await readFile(path.join(inputs, "claims/42/façade.pdf")))).toEqual(
      binary,
    );
  } finally {
    await system.stopRunner();
  }
}, 20_000);

test("a job whose Input file does not arrive as it was listed fails, naming the file", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id } = await submitJob(client, {
    ...SUBMISSION,
    files: [fileAt("broken.txt", "Will not survive.")],
  });
  // Damaged where the hub keeps it, before any Runner downloads it.
  await using db = openDatabase(inject("databaseUrl"));
  await sql`
    update ${sql.table(`${DB_SCHEMA}.uploadChunk`)} set bytes = '\\x00'::bytea
    where "uploadId" = (
      select upload.id from ${sql.table(`${DB_SCHEMA}.upload`)} as upload
      join ${sql.table(`${DB_SCHEMA}.inputFile`)} as file on file.sha256 = upload.sha256
      where file."jobId" = ${id}
    )
  `.execute(db);
  await system.startRunner();
  try {
    await expect
      .poll(async () => (await client.jobs.get({ id })).status, { timeout: 15_000 })
      .toBe("failed");
    expect(await client.jobs.get({ id })).toMatchObject({
      failure: {
        reason: "runFaulted",
        message: expect.stringContaining("Downloading the Input file broken.txt failed"),
      },
    });
  } finally {
    await system.stopRunner();
  }
}, 20_000);
