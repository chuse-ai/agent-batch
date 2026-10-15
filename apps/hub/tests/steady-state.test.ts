import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { startSystem } from "./harness.ts";

// Heartbeats every 0.5 s against a 1.5 s liveness timeout, so a few seconds cover several cycles.
const system = startSystem({ hubVars: { HEARTBEAT_TIMEOUT_MS: "1500" }, heartbeatIntervalMs: 500 });

beforeAll(() => system.start());
afterAll(() => system.stop());

// Longer than Vitest's default 5 s timeout: the test watches the Runner for 6 s.
test("a runner that keeps sending heartbeats stays connected, seen within each timeout", async () => {
  const runnerId = await system.startRunner();
  await expect.poll(() => system.findRunner(runnerId)).toBeDefined();

  // Were the hub to drop the socket, the Runner would wait 3 s to subscribe again, and its last-seen
  // time would fall further behind than this.
  const watchUntil = Date.now() + 6_000;
  while (Date.now() < watchUntil) {
    expect(Date.now() - (await system.lastSeen(runnerId))).toBeLessThan(2_500);
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}, 15_000);
