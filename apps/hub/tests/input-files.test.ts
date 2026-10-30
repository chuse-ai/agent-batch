import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { zstdDecompressSync } from "node:zlib";

import { ORPCError } from "@orpc/client";
import { createJsonRpcClient, hubNotifications, problemOf, runnerRequests } from "contract";
import { Effect } from "effect";
import { sql } from "kysely";
import { afterAll, assert, beforeAll, expect, inject, test } from "vite-plus/test";

import { sweepUnusedContent } from "../src/blobs.ts";
import { Db, DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { type OneJob, startSystem, submitJob, uploadContent } from "./harness.ts";
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
function fileAt(filePath: string, content: string | Uint8Array) {
  return { path: filePath, content };
}

/** Text of more than two chunks, with characters of several bytes across the chunks' boundaries. */
const LARGE = "Grüße, 世界! ".repeat(150_000);

function digestOf(content: string | Uint8Array): string {
  return `sha256:${createHash("sha256").update(content).digest("hex")}`;
}

/** Runs the hub's hourly sweep now, on the test run's database. */
async function sweep() {
  await using db = openDatabase(inject("databaseUrl"));
  await Effect.runPromise(
    Effect.gen(function* () {
      return yield* Effect.provideService(sweepUnusedContent, Db, yield* Db.make(db));
    }),
  );
}

/** Ends every upload hold now, as seven days passing would. */
async function expireUploads() {
  await using db = openDatabase(inject("databaseUrl"));
  await sql`update ${sql.table(`${DB_SCHEMA}.organizationBlob`)} set "expiresAt" = now()`.execute(
    db,
  );
}

/** Which of `contents` the hub stores, by digest; no API shows blobs. */
async function storedOf(contents: readonly string[]) {
  const digests = contents.map(digestOf);
  await using db = openDatabase(inject("databaseUrl"));
  const { rows } = await sql<{ digest: string }>`
    select 'sha256:' || encode(sha256, 'hex') as digest from ${sql.table(`${DB_SCHEMA}.blob`)}
  `.execute(db);
  return contents.filter((_, index) => rows.some((row) => row.digest === digests[index]));
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
          digest: digestOf(LARGE),
          originalSizeBytes: Buffer.byteLength(LARGE),
          chunkBytes: expect.any(Number),
        },
        {
          path: "empty.txt",
          digest: digestOf(""),
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

test("content uploaded once is served to every job of an add that names it", async () => {
  const client = await system.clientWithNewJobsKey();
  const shared = "Shared across the add.";
  const batch = await client.batches.create({
    agent: { outputContract: { schema: { type: "string" } } },
    breaker: false,
  });
  const contents = (index: number) => ({
    "own.txt": index === 0 ? `${LARGE}First job.` : `File for job ${index}.`,
    "copy.txt": shared,
    "notes/empty.txt": "",
  });
  // Each content uploaded once, however many jobs name it.
  const refs = new Map<string, { digest: string; sizeBytes: number }>();
  for (const content of new Set(
    Array.from({ length: 100 }, (_, i) => Object.values(contents(i))).flat(),
  )) {
    refs.set(content, await uploadContent(client, content));
  }
  const jobs = Array.from({ length: 100 }, (_, index) => ({
    customId: `files-${index}`,
    input: "Read the files.",
    files: Object.entries(contents(index)).map(([filePath, content]) => {
      const ref = refs.get(content);
      assert(ref);
      return { path: filePath, ...ref };
    }),
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
      const expected = contents(index);
      expect(listed.map((file) => file.path).toSorted()).toEqual(Object.keys(expected).toSorted());
      for (const [filePath, content] of Object.entries(expected)) {
        const file = listed.find((entry) => entry.path === filePath);
        assert(file);
        expect(file).toMatchObject({
          digest: digestOf(content),
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

test("a check answers which content the Organization holds, and where to upload the rest", async () => {
  const client = await system.clientWithNewJobsKey();
  const kept = "Kept by a job.";
  const lapsed = "Held by its upload alone.";
  const ref = (content: string) => ({ digest: digestOf(content), sizeBytes: content.length });
  const { files } = await client.blobs.check({ files: [ref(kept), ref(lapsed)] });
  expect(files).toEqual([
    { ...ref(kept), stored: false, upload: { transfer: "put", url: `/blobs/${digestOf(kept)}` } },
    {
      ...ref(lapsed),
      stored: false,
      upload: { transfer: "put", url: `/blobs/${digestOf(lapsed)}` },
    },
  ]);

  const job = await submitJob(client, { ...SUBMISSION, files: [fileAt("kept.txt", kept)] });
  await uploadContent(client, lapsed);
  expect((await client.blobs.check({ files: [ref(kept), ref(lapsed)] })).files).toMatchObject([
    { stored: true },
    { stored: true },
  ]);
  await expect(
    client.blobs.check({ files: [{ ...ref(kept), sizeBytes: kept.length + 1 }] }),
  ).rejects.toMatchObject({ code: "BAD_REQUEST" });

  // Seven days on, a job still holds its files' content, and an upload alone no longer does.
  await expireUploads();
  expect((await client.blobs.check({ files: [ref(kept), ref(lapsed)] })).files).toMatchObject([
    { stored: true },
    { stored: false },
  ]);
  await client.jobs.cancel({ id: job.id });
});

test("an upload whose bytes are not those its digest names stores nothing", async () => {
  const client = await system.clientWithNewJobsKey();
  const claimed = "What the caller meant to send.";
  const refused = await client.blobs
    .upload({
      params: { digest: digestOf(claimed) },
      headers: { "content-length": "4" },
      body: new Blob(["else"]).stream(),
    })
    .catch((error: unknown) => error);
  const problem = refused instanceof ORPCError ? problemOf(refused) : undefined;
  expect(problem).toMatchObject({ status: 400, detail: expect.stringContaining(digestOf("else")) });
  expect(await storedOf([claimed, "else"])).toEqual([]);
  await expect(
    client.batches.addJobs({
      id: (await client.batches.create({ agent: SUBMISSION })).id,
      jobs: [
        { customId: "a", files: [{ path: "a.txt", digest: digestOf(claimed), sizeBytes: 30 }] },
      ],
    }),
  ).rejects.toMatchObject({ code: "BAD_REQUEST" });
});

test("deleting a job keeps its files' content while an upload holds it, and the sweep deletes what nothing holds", async () => {
  const client = await system.clientWithNewJobsKey();
  const shared = "Shared between both jobs.";
  const own = "Only the first job's.";
  const first = await submitJob(client, {
    ...SUBMISSION,
    files: [fileAt("shared.txt", shared), fileAt("own.txt", own)],
  });
  const second = await submitJob(client, { ...SUBMISSION, files: [fileAt("copy.txt", shared)] });

  // Jobs never sent to a Runner end at once when cancelled.
  await client.jobs.cancel({ id: first.id });
  await client.jobs.delete({ id: first.id });
  expect(await storedOf([shared, own])).toEqual([shared, own]);

  // Once the uploads no longer hold it, a deletion takes the content no other job uses with it,
  // and the sweep takes the rest.
  await expireUploads();
  await client.jobs.cancel({ id: second.id });
  await client.jobs.delete({ id: second.id });
  expect(await storedOf([shared, own])).toEqual([own]);
  await sweep();
  expect(await storedOf([shared, own])).toEqual([]);
});

test("the sweep deletes uploads that were never published once they expire", async () => {
  await using db = openDatabase(inject("databaseUrl"));
  const upload = sql.table(`${DB_SCHEMA}.upload`);
  // As a request cut off after its first chunk leaves it.
  await sql`
    insert into ${upload} (id, sha256, "chunkBytes", "expiresAt")
    values (gen_random_uuid(), null, 1048576, now() - interval '1 second'),
      (gen_random_uuid(), null, 1048576, now() + interval '1 hour')
  `.execute(db);
  await sweep();
  const { rows } = await sql<{ count: number }>`
    select count(*)::int as count from ${upload} where sha256 is null
  `.execute(db);
  expect(rows).toEqual([{ count: 1 }]);
  await sql`delete from ${upload} where sha256 is null`.execute(db);
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
