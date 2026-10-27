import { afterAll, beforeAll, expect, inject, test } from "vite-plus/test";
import * as z from "zod/mini";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { JobId } from "../src/db/types.ts";
import { OWNER, startSystem, submitJob } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const AGENT = {
  resultContract: {
    schema: { type: "object", properties: { summary: { type: "string" } }, required: ["summary"] },
  },
};
const JOB = {
  customId: "report-q3",
  input: "Summarise the report in one sentence.",
  inputFiles: { "q3.md": { content: "Revenue grew 4%." } },
};

/** A client sending a new API key with `permissions`, and the key's ID. */
async function withKey(permissions: Record<string, string[]>, name = "jobs") {
  const { id, key } = await system.owner().apiKeys.create({ name, permissions });
  return { keyId: id, key, client: system.client({ "x-api-key": key }) };
}

/** Posts `body` as raw JSON to `path` with `key`, as any HTTP client would. */
function post(key: string, path: string, body: unknown) {
  return system.hub.fetch(path, {
    method: "POST",
    headers: { "x-api-key": key, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

test("a Batch is created and a job added to it, each with 200, and the job reads back as queued, with a key or a member's session", async () => {
  const { key, client } = await withKey({ job: ["create", "read"] });

  const created = await post(key, "/batches", { agent: AGENT });
  expect(created.status).toBe(200);
  const batch = z.parse(z.looseObject({ id: z.string() }), await created.json());
  const response = await post(key, `/batches/${batch.id}/jobs`, { jobs: [JOB] });
  expect(response.status).toBe(200);
  const added = z.parse(
    z.object({
      batch: z.looseObject({ id: z.string() }),
      jobs: z.array(
        z.object({
          created: z.boolean(),
          job: z.looseObject({ id: z.string(), createdAt: z.string() }),
        }),
      ),
    }),
    await response.json(),
  );
  expect(added.batch).toMatchObject({ id: batch.id, jobCounts: { queued: 1 } });
  const [first] = added.jobs;
  if (!first) throw new Error("The add answered without the job");
  const unused = { totalTokens: 0, cost: { totalUsd: 0 } };
  // The hub's default limits.
  const limits = { runTimeoutSeconds: 30 * 60, modelBudgetUsd: 1 };
  const job = {
    id: first.job.id,
    status: "queued",
    startedAt: null,
    endedAt: null,
    usage: unused,
    ...limits,
    batchId: batch.id,
    customId: "report-q3",
    labels: {},
  };
  expect(first).toEqual({ created: true, job: { ...job, createdAt: expect.any(String) } });

  const read = { ...job, createdAt: new Date(first.job.createdAt) };
  expect(await client.jobs.get({ id: job.id })).toEqual(read);
  expect(await system.owner().jobs.get({ id: job.id })).toEqual(read);
});

test("a job keeps the key that submitted it and the member who created the key, after revocation", async () => {
  const { keyId, client } = await withKey({ job: ["create"] }, "nightly batch");
  const { id } = await submitJob(client, { ...AGENT, ...JOB });
  await system.owner().apiKeys.revoke({ id: keyId });

  // No API shows a job's submitter yet, so the test reads it where the hub keeps it; it moves to the
  // API once one does.
  await using db = openDatabase(inject("databaseUrl"));
  const owner = await db
    .withSchema(DB_SCHEMA)
    .selectFrom("user")
    .select("id")
    .where("email", "=", OWNER.email)
    .executeTakeFirstOrThrow();
  expect(
    await db
      .withSchema(DB_SCHEMA)
      .selectFrom("job")
      .select(["submitterKeyId", "submitterKeyName", "submitterMemberId"])
      .where("id", "=", JobId(id))
      .executeTakeFirst(),
  ).toEqual({
    submitterKeyId: keyId,
    submitterKeyName: "nightly batch",
    submitterMemberId: owner.id,
  });
});

test("a key adds and reads jobs only with the permission for each", async () => {
  const { client: addOnly } = await withKey({ job: ["create"] });
  const { id, batchId } = await submitJob(addOnly, { ...AGENT, ...JOB });
  if (batchId === undefined) throw new Error("The job has no Batch");

  await expect(addOnly.jobs.get({ id })).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  await expect(addOnly.jobs.events({ id })).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  await expect(addOnly.batches.get({ id: batchId })).rejects.toMatchObject({
    code: "UNAUTHORIZED",
  });
  const { client: readOnly } = await withKey({ job: ["read"] });
  await expect(readOnly.batches.create({ agent: AGENT })).rejects.toMatchObject({
    code: "UNAUTHORIZED",
  });
  await expect(readOnly.batches.addJobs({ id: batchId, jobs: [JOB] })).rejects.toMatchObject({
    code: "UNAUTHORIZED",
  });
});

test("an invalid Result contract or Input file name is refused with 400", async () => {
  const { key, client } = await withKey({ job: ["create"] });
  const schemas = [
    { type: 5 },
    // A list of results is submitted as jobs.
    { type: "array", items: { type: "string" } },
    { type: "object", properties: { a: { $ref: "https://example.com/schema" } } },
  ];
  for (const schema of schemas) {
    await expect(
      client.batches.create({ agent: { resultContract: { schema } } }),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  }
  const batch = await client.batches.create({ agent: AGENT });
  for (const name of ["reports/q3.md", "../outside", "..", "a\\b"]) {
    const inputFiles = { [name]: { content: "" } };
    await expect(client.batches.create({ agent: { ...AGENT, inputFiles } })).rejects.toMatchObject({
      code: "BAD_REQUEST",
    });
    await expect(
      client.batches.addJobs({ id: batch.id, jobs: [{ ...JOB, inputFiles }] }),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  }

  // The answer names the problem, so a caller can correct the request.
  const response = await post(key, "/batches", {
    agent: { resultContract: { schema: schemas[2] } },
  });
  expect(await response.json()).toMatchObject({
    status: 400,
    detail: expect.stringContaining("agent.resultContract.schema"),
  });
});
