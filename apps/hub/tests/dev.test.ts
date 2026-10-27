/**
 * @module-tag container
 */
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";

import getPort from "get-port";
import { afterAll, describe, expect, inject, onTestFinished, test } from "vite-plus/test";

import { buildRepositoryImage } from "./docker-images.ts";

// `agent-batch dev` as a user runs it: the hub, a Runner and Postgres from the images, here built
// from the repository and tagged as a release's, through Docker Compose, in a Compose project and a
// data folder of its own, apart from a contributor's own `agent-batch dev`. It needs Docker and image
// builds, so it is tagged and the default run skips it (ADR 0003).

const run = promisify(execFile);
const CLI = path.join(import.meta.dirname, "../node_modules/.bin/agent-batch");
const PROJECT = "agent-batch-dev-test";
const PROJECT_LABEL = `label=com.docker.compose.project=${PROJECT}`;
const VERSION = "test";

const stateHome = await mkdtemp(path.join(tmpdir(), "agent-batch-dev-"));
/** The environment of every command here: the state in a folder of its own, the images built here. */
const { AGENT_BATCH_URL: _url, AGENT_BATCH_API_KEY: _key, ...inherited } = process.env;
const env = {
  ...inherited,
  XDG_DATA_HOME: stateHome,
  AGENT_BATCH_DEV_PROJECT: PROJECT,
  AGENT_BATCH_VERSION: VERSION,
  // The Runner starts only with a key for its provider; no job here reaches the model.
  OPENROUTER_API_KEY: "not-a-key",
};

/** Starts `agent-batch dev` and resolves once it says the hub and a Runner are ready. */
async function startDev(port: number) {
  const dev = spawn(CLI, ["dev", "--port", String(port)], {
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  // Stopped before `afterAll` resets, also when an assertion fails while it runs.
  onTestFinished(async () => {
    if (dev.exitCode === null) await stopDev(dev);
  });
  let output = "";
  dev.stdout.on("data", (chunk: Buffer) => (output += chunk.toString()));
  await new Promise<void>((resolve, reject) => {
    dev.stderr.on("data", (chunk: Buffer) => {
      output += chunk.toString();
      if (output.includes("The hub is at")) resolve();
    });
    dev.once("error", reject);
    dev.once("exit", (code) =>
      reject(new Error(`agent-batch dev exited with ${code}:\n${output}`)),
    );
  });
  return dev;
}

/** Stops `dev` as Ctrl-C does and resolves with its exit code. */
async function stopDev(dev: ChildProcess) {
  const exited = once(dev, "exit");
  dev.kill("SIGINT");
  const [code] = await exited;
  return code;
}

async function dockerIds(kind: "ps" | "volume", ...args: string[]) {
  const command = kind === "ps" ? ["ps", "--all"] : ["volume", "ls"];
  const { stdout } = await run("docker", [
    ...command,
    "--quiet",
    "--filter",
    PROJECT_LABEL,
    ...args,
  ]);
  return stdout.split("\n").filter((id) => id !== "");
}

/** Calls a job command as a user would with no settings of their own. */
async function getJob(id: string) {
  return run(CLI, ["jobs", "get", "--id", id], { env }).then(
    () => ({ exitCode: 0, stderr: "" }),
    (error: { code: number; stderr: string }) => ({ exitCode: error.code, stderr: error.stderr }),
  );
}

// The data folder follows `XDG_DATA_HOME` on Linux only; elsewhere the test would use the user's own.
describe.skipIf(inject("hub") !== "node" || process.platform !== "linux")("agent-batch dev", () => {
  afterAll(async () => {
    await run(CLI, ["dev", "reset"], { env }).catch(() => {});
    await rm(stateHome, { recursive: true, force: true });
  });

  test("runs a hub and a Runner that job commands reach, and Ctrl-C removes the containers but keeps the data and keys", async () => {
    await Promise.all(
      (["hub", "runner"] as const).map(async (target) => {
        const built = await buildRepositoryImage(target);
        await run("docker", ["tag", built, `ghcr.io/chuse-ai/agent-batch-${target}:${VERSION}`]);
      }),
    );
    const port = await getPort();

    const dev = await startDev(port);
    // A job the hub does not have: the key the command found was accepted, so the hub says so.
    const missing = await getJob("00000000-0000-7000-8000-000000000000");
    expect(missing.exitCode).toBe(1);
    expect(missing.stderr).toContain(`agent-batch dev runs at http://localhost:${port}`);
    expect(missing.stderr).toContain('"status":404');
    const { stdout: exports } = await run(CLI, ["dev", "env"], { env });

    expect(await stopDev(dev)).toBe(0);
    expect(await dockerIds("ps")).toEqual([]);
    expect(await dockerIds("volume")).toHaveLength(2);

    // The next start finds the same database, so the key it made still works.
    const again = await startDev(port);
    expect((await run(CLI, ["dev", "env"], { env })).stdout).toBe(exports);
    expect((await getJob("00000000-0000-7000-8000-000000000000")).stderr).toContain('"status":404');
    expect(await stopDev(again)).toBe(0);
  });
});
