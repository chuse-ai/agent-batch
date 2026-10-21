import type { JobSubmission } from "contract";
import { afterAll, beforeAll, expect, inject, test } from "vite-plus/test";
import * as z from "zod/mini";

import { DB_SCHEMA, openDatabase } from "../src/db/database.ts";
import { JobId } from "../src/db/types.ts";
import { OWNER, startSystem } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const SUBMISSION = {
  prompt: "Summarise the report in one sentence.",
  inputFiles: { "q3.md": { content: "Revenue grew 4%." } },
  resultContract: {
    schema: { type: "object", properties: { summary: { type: "string" } }, required: ["summary"] },
  },
} satisfies JobSubmission;

/** A client sending a new API key with `permissions`, and the key's ID. */
async function withKey(permissions: Record<string, string[]>, name = "jobs") {
  const { id, key } = await system.owner().apiKeys.create({ name, permissions });
  return { keyId: id, key, client: system.client({ "x-api-key": key }) };
}

/** Submits `body` as raw JSON with `key`, as any HTTP client would. */
function submit(key: string, body: string) {
  return system.hub.fetch("/jobs", {
    method: "POST",
    headers: { "x-api-key": key, "content-type": "application/json" },
    body,
  });
}

test("a submitted job answers 201 and reads back as queued, with a key or a member's session", async () => {
  const { key, client } = await withKey({ job: ["create", "read"] });

  const response = await submit(key, JSON.stringify(SUBMISSION));
  expect(response.status).toBe(201);
  const job = z.parse(
    z.looseObject({ id: z.string(), createdAt: z.string() }),
    await response.json(),
  );
  const unused = { totalTokens: 0, cost: { totalUsd: "0" } };
  expect(job).toEqual({
    id: expect.any(String),
    status: "queued",
    createdAt: expect.any(String),
    startedAt: null,
    endedAt: null,
    usage: unused,
  });

  const read = {
    id: job.id,
    status: "queued",
    createdAt: new Date(job.createdAt),
    startedAt: null,
    endedAt: null,
    usage: unused,
  };
  expect(await client.jobs.get({ id: job.id })).toEqual(read);
  expect(await system.owner().jobs.get({ id: job.id })).toEqual(read);
});

test("resubmitting with an idempotency key returns the same job, or refuses different content", async () => {
  const { client } = await withKey({ job: ["create"] });
  const submission = { ...SUBMISSION, idempotencyKey: "report-q3" };

  const first = await client.jobs.submit(submission);
  // The same content written differently is the same job.
  const { prompt, inputFiles, resultContract, idempotencyKey } = submission;
  expect(await client.jobs.submit({ resultContract, inputFiles, idempotencyKey, prompt })).toEqual(
    first,
  );

  await expect(
    client.jobs.submit({ ...submission, prompt: "Something else." }),
  ).rejects.toMatchObject({ code: "UNPROCESSABLE_CONTENT", defined: true });
});

test("a job keeps the key that submitted it and the member who created the key, after revocation", async () => {
  const { keyId, client } = await withKey({ job: ["create"] }, "nightly batch");
  const { id } = await client.jobs.submit(SUBMISSION);
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

test("a key submits and reads jobs only with the permission for each", async () => {
  const { id } = await (await withKey({ job: ["create"] })).client.jobs.submit(SUBMISSION);

  const { client: submitOnly } = await withKey({ job: ["create"] });
  await expect(submitOnly.jobs.get({ id })).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  await expect(submitOnly.jobs.events({ id })).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  const { client: readOnly } = await withKey({ job: ["read"] });
  await expect(readOnly.jobs.submit(SUBMISSION)).rejects.toMatchObject({ code: "UNAUTHORIZED" });
});

test("an invalid Result contract or Input file name is refused with 400", async () => {
  const { key, client } = await withKey({ job: ["create"] });
  const schemas = [
    { type: 5 },
    // A list of results is submitted as items.
    { type: "array", items: { type: "string" } },
    { type: "object", properties: { a: { $ref: "https://example.com/schema" } } },
  ];
  for (const schema of schemas) {
    await expect(
      client.jobs.submit({ ...SUBMISSION, resultContract: { schema } }),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  }
  for (const name of ["reports/q3.md", "../outside", "..", "a\\b"]) {
    const inputFiles = { [name]: { content: "" } };
    await expect(client.jobs.submit({ ...SUBMISSION, inputFiles })).rejects.toMatchObject({
      code: "BAD_REQUEST",
    });
  }

  // The answer names the problem, so a caller can correct the request.
  const response = await submit(
    key,
    JSON.stringify({ ...SUBMISSION, resultContract: { schema: schemas[2] } }),
  );
  expect(await response.json()).toMatchObject({
    status: 400,
    detail: expect.stringContaining("resultContract.schema"),
  });
});
