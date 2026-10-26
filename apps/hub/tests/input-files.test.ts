import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { zstdDecompressSync } from "node:zlib";

import {
  createProtocolClient,
  hubNotifications,
  type JobSubmission,
  runnerMethods,
} from "contract";
import { sql } from "kysely";
import { afterAll, assert, beforeAll, expect, inject, test } from "vite-plus/test";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { startSystem } from "./harness.ts";
import { callTool } from "./scripted-model.ts";

// A job's Input files reach its Runner apart from the hub's answers, which name them only: the
// Runner the job is placed on downloads each a chunk at a time and puts it in the Workspace's
// `inputs/` folder before the agent starts.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Read the report.",
  inputFiles: {},
  resultContract: { schema: {} },
} satisfies JobSubmission;

/** Text of more than two chunks, with characters of several bytes across the chunks' boundaries. */
const LARGE = "Grüße, 世界! ".repeat(150_000);

function sha256(text: string): string {
  return createHash("sha256").update(text, "utf8").digest("hex");
}

/** A Runner's side of the protocol on a socket the test opens itself. */
async function connectProtocolRunner() {
  const { key, socket } = await system.connectRunner();
  const runner = createProtocolClient(runnerMethods, hubNotifications, { syncNow() {} }, (text) =>
    socket.send(text),
  );
  socket.addEventListener("message", (event) => {
    if (typeof event.data === "string") runner.receive(event.data);
  });
  return { runner, api: system.client({ "x-api-key": key }), close: () => socket.close() };
}

test("a sync names a job's Input files, and only the Runner the job is placed on reads them, a chunk at a time", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit({
    ...SUBMISSION,
    inputFiles: { "report.txt": { content: LARGE }, "empty.txt": { content: "" } },
  });
  const placed = await connectProtocolRunner();
  const other = await connectProtocolRunner();
  try {
    const { runs } = await placed.runner.request("sync", { held: [], free: 1 }, 10_000);

    const listed = runs.find((run) => run.jobId === id)?.job?.inputFiles;
    expect(listed).toEqual(
      expect.arrayContaining([
        {
          name: "report.txt",
          sha256: sha256(LARGE),
          originalSizeBytes: Buffer.byteLength(LARGE),
          chunkBytes: expect.any(Number),
        },
        {
          name: "empty.txt",
          sha256: sha256(""),
          originalSizeBytes: 0,
          chunkBytes: expect.any(Number),
        },
      ]),
    );
    const chunkBytes = listed?.find((file) => file.name === "report.txt")?.chunkBytes;
    assert(chunkBytes !== undefined);
    const count = Math.ceil(Buffer.byteLength(LARGE) / chunkBytes);
    const chunks: Buffer[] = [];
    for (let index = 0; index < count; index++) {
      const chunk = await placed.api.jobs.inputFileChunk({ id, name: "report.txt", index });
      assert(chunk instanceof Blob, "a chunk arrives as a file");
      chunks.push(zstdDecompressSync(await chunk.arrayBuffer()));
    }
    expect(chunks.slice(0, -1).every((chunk) => chunk.length === chunkBytes)).toBe(true);
    expect(Buffer.concat(chunks).toString("utf8")).toBe(LARGE);
    await expect(
      placed.api.jobs.inputFileChunk({ id, name: "report.txt", index: count }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
    await expect(
      other.api.jobs.inputFileChunk({ id, name: "report.txt", index: 0 }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  } finally {
    placed.close();
    other.close();
  }
});

test("deleting a job deletes the content of its Input files that no other job uses", async () => {
  const client = await system.clientWithNewJobsKey();
  const shared = { content: "Shared between both jobs." };
  const first = await client.jobs.submit({
    ...SUBMISSION,
    inputFiles: { "shared.txt": shared, "own.txt": { content: "Only the first job's." } },
  });
  const second = await client.jobs.submit({ ...SUBMISSION, inputFiles: { "copy.txt": shared } });
  // No API shows stored content, which the hub keeps once however many jobs use it.
  const all = [sha256(shared.content), sha256("Only the first job's.")].toSorted();
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
  expect(await stored()).toEqual([sha256(shared.content)]);

  await client.jobs.cancel({ id: second.id });
  await client.jobs.delete({ id: second.id });
  expect(await stored()).toEqual([]);
});

test("the Runner puts a job's Input files in its Workspace as they were submitted", async () => {
  system.model.setResponses([callTool("submit_result", { value: "done" })]);
  const client = await system.clientWithNewJobsKey();
  await system.startRunner();
  try {
    const { id } = await client.jobs.submit({
      ...SUBMISSION,
      inputFiles: { "report.txt": { content: LARGE }, "empty.txt": { content: "" } },
    });
    await expect
      .poll(async () => (await client.jobs.get({ id })).status, { timeout: 15_000 })
      .toBe("succeeded");

    const inputs = path.join(system.workspace(id), "inputs");
    expect(await readFile(path.join(inputs, "report.txt"), "utf8")).toBe(LARGE);
    expect(await readFile(path.join(inputs, "empty.txt"), "utf8")).toBe("");
  } finally {
    await system.stopRunner();
  }
}, 20_000);

test("a job whose Input file does not arrive as it was listed fails, naming the file", async () => {
  const client = await system.clientWithNewJobsKey();
  const { id } = await client.jobs.submit({
    ...SUBMISSION,
    inputFiles: { "broken.txt": { content: "Will not survive." } },
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
