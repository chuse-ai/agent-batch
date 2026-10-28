import { execFile } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";

import { afterAll, beforeAll, expect, onTestFinished, test } from "vite-plus/test";
import * as z from "zod/mini";

import { runCommand } from "./run-command.ts";

// The package as npm installs it from the registry: built, packed and installed outside the
// workspace, so it runs with only its published files, on the Node the tests run on, which in CI's
// node-22 job is the lowest the package allows (ADR 0024). The other tests run the command from its
// source; these run the bundle, which is what users run and can differ from it.

const run = promisify(execFile);
const PACKAGE_DIR = path.join(import.meta.dirname, "..");
const VP = path.join(PACKAGE_DIR, "../../node_modules/.bin/vp");

let dir: string | undefined;
let agentBatch: string;
beforeAll(async () => {
  dir = await mkdtemp(path.join(tmpdir(), "agent-batch-packed-"));
  await run(VP, ["pack"], { cwd: PACKAGE_DIR });
  await run(VP, ["pm", "pack", "--out", path.join(dir, "agent-batch.tgz")], { cwd: PACKAGE_DIR });
  await writeFile(path.join(dir, "package.json"), "{}");
  // The package bundles everything it runs and has no dependencies, so npm installs it alone.
  await run("npm", ["install", "--no-audit", "--no-fund", "./agent-batch.tgz"], { cwd: dir });
  agentBatch = path.join(dir, "node_modules/.bin/agent-batch");
}, 300_000);
// Apart from the setup, so the directory goes even when a step of the setup fails.
afterAll(async () => {
  if (dir !== undefined) await rm(dir, { recursive: true, force: true });
});

test("the packed package installs and runs outside the workspace", async () => {
  const { stdout: help } = await run(agentBatch, ["jobs", "--help"]);
  expect(help).toContain("Available subcommands: submit, get, pause, resume, cancel");
});

// A call to a hub runs most of what the bundle carries: Effect, the configuration, `fetch`, oRPC's
// link and the contract's reading of a problem. A refusal is not retried, so it answers at once.
test("a call the hub refuses prints its Problem Details and exits non-zero", async () => {
  const problem = { type: "about:blank", title: "Not Found", status: 404 };
  const hub = createServer((_request, response) => {
    response.writeHead(problem.status, { "content-type": "application/problem+json" });
    response.end(JSON.stringify(problem));
  });
  hub.on("error", () => {});
  onTestFinished(() => {
    hub.close();
  });
  hub.listen(0, "127.0.0.1");
  await once(hub, "listening");
  const { port } = z.parse(z.object({ port: z.int() }), hub.address());
  const { exitCode, stderr } = await runCommand(
    agentBatch,
    ["jobs", "get", "--id", crypto.randomUUID()],
    {
      env: { AGENT_BATCH_URL: `http://127.0.0.1:${port}`, AGENT_BATCH_API_KEY: "key" },
      extendEnv: true,
    },
  );

  expect(exitCode).toBe(1);
  expect(JSON.parse(stderr)).toEqual(problem);
});

// `agent-batch stack` runs its own bundled dependencies before Docker: the lock, the state it saves and
// the Compose file it writes. Without Docker on the `PATH`, it stops there and says so. The data
// folder follows `XDG_DATA_HOME` on Linux only.
test.skipIf(process.platform !== "linux")(
  "agent-batch stack saves its state, then asks for Docker where there is none",
  async () => {
    const data = await mkdtemp(path.join(tmpdir(), "agent-batch-stack-"));
    onTestFinished(() => rm(data, { recursive: true, force: true }));
    const { exitCode, stderr } = await runCommand(agentBatch, ["stack", "start"], {
      env: { PATH: path.dirname(process.execPath), XDG_DATA_HOME: data },
    });

    expect({ exitCode, stderr: stderr.trim() }).toEqual({
      exitCode: 1,
      stderr: "agent-batch stack runs the hub and a Runner with Docker: install Docker",
    });
    await expect(
      readFile(path.join(data, "agent-batch-nodejs/stack/state.json"), "utf8").then(JSON.parse),
    ).resolves.toMatchObject({ port: 8787, model: { provider: "openrouter" } });
  },
);
