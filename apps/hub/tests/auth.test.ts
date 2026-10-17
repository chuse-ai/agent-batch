import { RUNNER_NOT_FOUND } from "contract";
import { afterAll, beforeAll, expect, inject, test } from "vite-plus/test";

import { resetPassword, setUpOrganization } from "../src/setup.ts";
import { clearDatabase, OWNER, startSystem } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

test("calls without a member's session or a key are refused", async () => {
  for (const path of ["/runners", "/api-keys"]) {
    expect((await system.hub.fetch(path)).status).toBe(401);
  }
  // `defined`: the contract declares the error, so the TypeScript client's errors stay typed.
  await expect(system.client().runners.register({ name: "host" })).rejects.toMatchObject({
    code: "UNAUTHORIZED",
    defined: true,
  });
  expect((await system.connect()).status).toBe(401);
});

test("API errors are RFC 9457 Problem Details", async () => {
  const refused = await system.hub.fetch("/runners");
  expect(refused.headers.get("content-type")).toMatch(/^application\/problem\+json/);
  expect(await refused.json()).toEqual({ type: "about:blank", title: "Unauthorized", status: 401 });

  const missing = await system.hub.fetch("/no-such-path");
  expect(missing.headers.get("content-type")).toMatch(/^application\/problem\+json/);
  expect(await missing.json()).toEqual({ type: "about:blank", title: "Not Found", status: 404 });

  // Invalid input names its problems, so a caller can correct the request.
  const { key } = await system.owner().apiKeys.create({
    name: "invalid input",
    permissions: { runner: ["register"] },
  });
  const invalid = await system.hub.fetch("/runners/register", {
    method: "POST",
    headers: { "x-api-key": key, "content-type": "application/json" },
    body: "{}",
  });
  expect(await invalid.json()).toEqual({
    type: "about:blank",
    title: "Bad Request",
    status: 400,
    detail: expect.stringContaining("name"),
  });
});

test("a member creates and lists keys, and a revoked key is refused at once", async () => {
  const owner = system.owner();
  const { id, key } = await owner.apiKeys.create({
    name: "CI",
    permissions: { runner: ["register"] },
  });

  const listed = await owner.apiKeys.list();
  expect(listed).toContainEqual(
    expect.objectContaining({ id, name: "CI", permissions: { runner: ["register"] } }),
  );
  // Only the key's first characters are listed; its value is shown once, when it is created.
  expect(JSON.stringify(listed)).not.toContain(key);

  const withKey = system.client({ "x-api-key": key });
  await expect(withKey.runners.register({ name: "host" })).resolves.toMatchObject({
    id: expect.any(String),
  });
  await owner.apiKeys.revoke({ id });
  await expect(withKey.runners.register({ name: "host" })).rejects.toMatchObject({
    code: "UNAUTHORIZED",
  });
});

test("a key cannot reach member operations or permissions it was not granted", async () => {
  const registrationKey = await system.createRegistrationKey();
  const withRegistrationKey = system.client({ "x-api-key": registrationKey });
  // Keys are managed and runners listed only by signed-in members.
  await expect(withRegistrationKey.apiKeys.list()).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  await expect(
    withRegistrationKey.apiKeys.create({ name: "escalated", permissions: { apiKey: ["create"] } }),
  ).rejects.toMatchObject({ code: "UNAUTHORIZED" });
  await expect(withRegistrationKey.runners.list()).rejects.toMatchObject({ code: "UNAUTHORIZED" });

  // A shared key may register Runners but not connect as one; a Runner's own key, the reverse. Better
  // Auth refuses a key without the permission as it refuses an unknown key.
  const { key: runnerKey } = await withRegistrationKey.runners.register({ name: "host" });
  expect((await system.connect(registrationKey)).status).toBe(401);
  await expect(
    system.client({ "x-api-key": runnerKey }).runners.register({ name: "host" }),
  ).rejects.toMatchObject({ code: "UNAUTHORIZED" });
});

test("removing a Runner refuses its key and takes it off the list", async () => {
  // The hub also closes the Runner's socket at once (code 1008). Local workerd completes that close
  // only about 10 s later, for every client tried, so the test checks what takes effect at once.
  const { runnerId, key, socket } = await system.connectRunner();

  await system.owner().runners.delete({ id: runnerId });

  expect(await system.findRunner(runnerId)).toBeUndefined();
  const reconnect = await system.connect(key);
  expect(reconnect.status).toBe(401);
  expect(await reconnect.json()).toMatchObject({ status: 401, code: RUNNER_NOT_FOUND });
  socket.close(1000);
});

test("a key can be created to expire, within Better Auth's limits of a day to a year", async () => {
  const owner = system.owner();
  const day = 24 * 60 * 60;
  const { id } = await owner.apiKeys.create({
    name: "one batch of hosts",
    permissions: { runner: ["register"] },
    expiresIn: day,
  });

  const listed = (await owner.apiKeys.list()).find((key) => key.id === id);
  expect(listed?.expiresAt?.getTime()).toBeGreaterThan(Date.now() + day * 1000 - 60_000);
  await expect(
    owner.apiKeys.create({
      name: "too short",
      permissions: { runner: ["register"] },
      expiresIn: 60,
    }),
  ).rejects.toMatchObject({ code: "BAD_REQUEST" });
});

test("members cannot grant a key member permissions or a Runner's own permission", async () => {
  const tooMuch: Record<string, string[]>[] = [{ apiKey: ["create"] }, { runner: ["connect"] }];
  for (const permissions of tooMuch) {
    await expect(
      system.owner().apiKeys.create({ name: "too much", permissions }),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  }
});

test("setting up again leaves the existing Organization and owner as they are", async () => {
  const messages: string[] = [];
  await setUpOrganization(
    inject("databaseUrl"),
    { email: "someone-else@example.com", password: "another password" },
    (message) => messages.push(message),
  );

  expect(messages).toEqual([
    "The Organization exists, so ADMIN_EMAIL and its password are ignored",
  ]);
  await system.signIn(OWNER);
});

test("a forgotten password is reset with the recovery command, signing out old sessions", async () => {
  const oldSession = await system.signIn(OWNER);
  const password = "a brand new password";
  await resetPassword(inject("databaseUrl"), { email: OWNER.email, password });

  expect((await system.hub.fetch("/runners", { headers: { cookie: oldSession } })).status).toBe(
    401,
  );

  await system.signIn({ email: OWNER.email, password });
  const oldPassword = await system.hub.fetch("/api/auth/sign-in/email", {
    method: "POST",
    headers: { "content-type": "application/json", origin: system.origin },
    body: JSON.stringify(OWNER),
  });
  expect(oldPassword.status).toBe(401);
});

test("without an Organization or owner settings, setting up refuses", async () => {
  await clearDatabase(inject("databaseUrl"));

  await expect(setUpOrganization(inject("databaseUrl"), {})).rejects.toThrow(
    "Set ADMIN_EMAIL and ADMIN_PASSWORD",
  );
});
