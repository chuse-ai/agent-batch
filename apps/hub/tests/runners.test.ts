import { hostname } from "node:os";

import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { startSystem } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

test("the hub lists no runners before any connect", async () => {
  expect(await system.listRunners()).toEqual([]);
});

test("a runner that connects is listed under its host's name, seen when it subscribed", async () => {
  const before = Date.now();
  await system.startRunner();

  await expect
    .poll(() => system.findRunner("runner-1"))
    .toEqual({ id: "runner-1", name: hostname(), lastSeenAt: expect.any(String) });
  expect(await system.lastSeen("runner-1")).toBeGreaterThanOrEqual(before);
});

test("a runner that reconnects is listed once, seen again", async () => {
  const firstSeen = await system.lastSeen("runner-1");

  await system.stopRunner();
  await system.startRunner();

  await expect.poll(() => system.lastSeen("runner-1")).toBeGreaterThan(firstSeen);
  expect(await system.listRunners()).toEqual([
    expect.objectContaining({ id: "runner-1", name: hostname() }),
  ]);
});

test("the hub serves many requests at once", async () => {
  const responses = await Promise.all(
    Array.from({ length: 20 }, () => system.hub.fetch("/runners")),
  );

  expect(responses.map((response) => response.status)).toEqual(Array(20).fill(200));
});
