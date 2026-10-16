import { createORPCClient } from "@orpc/client";
import { RPCLink } from "@orpc/client/websocket";
import type { ContractRouterClient } from "@orpc/contract";
import { HEARTBEAT_PING, HEARTBEAT_PONG, type runnerProtocol } from "contract";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { startSystem } from "./harness.ts";

// The Runner's first heartbeat is 30 s away, so a 2 s liveness timeout lets the hub's alarm find it
// silent within seconds.
const system = startSystem({ hubVars: { HEARTBEAT_TIMEOUT_MS: "2000" } });

beforeAll(() => system.start());
afterAll(() => system.stop());

// Longer than Vitest's default 5 s timeout: the alarm fires after 2 s, the Runner waits 3 s before
// subscribing again, and the poll allows for both.
test("a runner that falls silent is dropped, and seen again once it reconnects", async () => {
  const runnerId = await system.startRunner();
  await expect.poll(() => system.findRunner(runnerId)).toBeDefined();
  const subscribedAt = await system.lastSeen(runnerId);

  // Only a new subscription can move the time this far: the Runner sent no heartbeat to record.
  await expect
    .poll(() => system.lastSeen(runnerId), { timeout: 10_000 })
    .toBeGreaterThanOrEqual(subscribedAt + 2_000);
}, 20_000);

// Longer than Vitest's default 5 s timeout: the Runner sends heartbeats for 4 s, then the alarm needs
// up to 2 s to find it silent.
test("a runner that sent heartbeats and then stopped is dropped, last seen at its last heartbeat", async () => {
  const { runnerId, socket } = await system.connectRunner();
  const closeCode = new Promise<number>((resolve) =>
    socket.addEventListener("close", (event) => resolve(event.code)),
  );
  const hub: ContractRouterClient<typeof runnerProtocol> = createORPCClient(
    // The harness returns `ws`'s WebSocket: the same API as the standard one, typed separately.
    new RPCLink({ connect: () => socket as unknown as WebSocket }),
  );
  await hub.commands({ name: "silent runner" });

  // Twice the liveness timeout, so the alarm has found heartbeats and moved on at least once.
  const heartbeats = setInterval(() => socket.send(HEARTBEAT_PING), 500);
  await new Promise((resolve) => setTimeout(resolve, 4_000));
  clearInterval(heartbeats);
  const stoppedAt = Date.now();

  expect(await closeCode).toBe(1001);
  await expect.poll(() => system.lastSeen(runnerId)).toBeGreaterThan(stoppedAt - 1_000);
  expect(await system.lastSeen(runnerId)).toBeLessThan(stoppedAt + 1_000);
}, 15_000);

test("the hub answers a runner's heartbeat", async () => {
  const { socket } = await system.connectRunner();
  const replies: unknown[] = [];
  socket.addEventListener("message", (event) => replies.push(event.data));

  socket.send(HEARTBEAT_PING);

  await expect.poll(() => replies).toEqual([HEARTBEAT_PONG]);
  socket.close(1000);
});
