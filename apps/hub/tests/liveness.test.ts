import { setTimeout } from "node:timers/promises";

import { HEARTBEAT_PING, HEARTBEAT_PONG } from "contract";
import { afterAll, assert, beforeAll, expect, test } from "vite-plus/test";

import { startSystem } from "./harness.ts";

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

test("a runner's sync records it as seen", async () => {
  const { runnerId, socket } = await system.connectRunner();
  const before = await system.lastSeen(runnerId);
  const replies: string[] = [];
  socket.addEventListener("message", (event) => {
    // `ws` types a message as text or binary; the protocol is text (ADR 0026).
    assert(typeof event.data === "string", "the hub sent a binary message");
    replies.push(event.data);
  });

  // The hub stamps a sighting to the millisecond and only moves it forward, so its clock must move.
  await setTimeout(20);
  socket.send(
    JSON.stringify({ jsonrpc: "2.0", id: 1, method: "sync", params: { held: [], free: 0 } }),
  );

  await expect.poll(() => replies.length).toBe(1);
  expect(JSON.parse(replies[0] ?? "")).toMatchObject({
    id: 1,
    result: { runs: [], deletions: [] },
  });
  expect(await system.lastSeen(runnerId)).toBeGreaterThan(before);
  socket.close(1000);
});

test("the hub answers a runner's heartbeat", async () => {
  const { socket } = await system.connectRunner();
  const replies: unknown[] = [];
  socket.addEventListener("message", (event) => replies.push(event.data));

  socket.send(HEARTBEAT_PING);

  await expect.poll(() => replies).toEqual([HEARTBEAT_PONG]);
  socket.close(1000);
});
