import { once } from "node:events";
import { mkdtemp, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { setImmediate } from "node:timers/promises";

import { registeredRunnerSchema } from "contract";
import { startRunner } from "runner";
import { expect, test } from "vite-plus/test";
import * as z from "zod/mini";

import { scriptedModel } from "./harness.ts";

// A Runner is stopped while its socket to the hub is still connecting, as when an operator stops it
// during a hub outage: the stop must return, and the process must survive.
test("a runner stopped while the hub never completes the handshake stops cleanly", async () => {
  // Accepts the TCP connection and never answers the WebSocket upgrade.
  const hub = createServer(() => {}).listen(0, "127.0.0.1");
  const dialled = once(hub, "connection");
  await once(hub, "listening");
  const address = hub.address();
  if (address === null || typeof address === "string") throw new Error("no port");
  const dataDir = await mkdtemp(path.join(tmpdir(), "runner-"));
  const registration = z.parse(registeredRunnerSchema, { id: "runner", key: "key" });
  await writeFile(path.join(dataDir, "runner.json"), JSON.stringify(registration));
  const uncaught: unknown[] = [];
  const record = (error: unknown) => uncaught.push(error);
  process.on("uncaughtException", record);
  try {
    const { models, model } = scriptedModel();
    const runner = await startRunner({
      hubUrl: `http://127.0.0.1:${address.port}`,
      registrationKey: "unused",
      dataDir,
      models,
      model,
    });
    await dialled;
    await runner.stop();
    // `ws` emits a handshake it aborted as an `error` on the next tick.
    await setImmediate();
    expect(uncaught).toEqual([]);
  } finally {
    process.off("uncaughtException", record);
    hub.close();
  }
});
