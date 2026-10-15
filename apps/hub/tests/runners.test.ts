import { hostname } from "node:os";

import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { startSystem } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

test("the hub lists no runners before any connect", async () => {
  expect(await system.owner().runners.list()).toEqual([]);
});

test("a runner that connects is listed under its host's name, seen when it subscribed", async () => {
  const before = Date.now();
  const runnerId = await system.startRunner();

  await expect
    .poll(() => system.findRunner(runnerId))
    .toEqual({ id: runnerId, name: hostname(), lastSeenAt: expect.any(String) });
  expect(await system.lastSeen(runnerId)).toBeGreaterThanOrEqual(before);
});

test("a runner that restarts keeps its identity, listed once and seen again", async () => {
  const [{ id: runnerId }] = await system.owner().runners.list();
  const firstSeen = await system.lastSeen(runnerId);

  await system.stopRunner();
  expect(await system.startRunner()).toBe(runnerId);

  await expect.poll(() => system.lastSeen(runnerId)).toBeGreaterThan(firstSeen);
  expect(await system.owner().runners.list()).toEqual([
    expect.objectContaining({ id: runnerId, name: hostname() }),
  ]);
});

test("the hub serves many requests at once", async () => {
  const responses = await Promise.all(
    Array.from({ length: 20 }, () =>
      system.hub.fetch("/runners", { headers: { cookie: system.ownerCookie() } }),
    ),
  );

  expect(responses.map((response) => response.status)).toEqual(Array(20).fill(200));
});

test("a removed runner stops when started again, and joins anew with register", async () => {
  const runnerId = await system.startRunner();
  await system.stopRunner();
  await system.owner().runners.delete({ id: runnerId });

  const removed = new Promise<void>((resolve) => system.startRunner({ onRemoved: resolve }));
  await removed;

  const newRunnerId = await system.startRunner({ register: true });
  expect(newRunnerId).not.toBe(runnerId);
  await expect.poll(() => system.findRunner(newRunnerId)).toBeDefined();
});
