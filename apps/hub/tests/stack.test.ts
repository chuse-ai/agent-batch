/**
 * @module-tag container
 */
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";

import getPort from "get-port";
import { afterAll, describe, expect, inject, test } from "vite-plus/test";
import * as z from "zod/mini";

import { buildRepositoryImage } from "./docker-images.ts";

// `agent-batch stack` as a user runs it: the hub, a Runner and Postgres from the images, here built
// from the repository and tagged as a release's, through Docker Compose, in a Compose project and a
// data folder of its own, apart from a contributor's own stack. It needs Docker and image builds,
// so it is tagged and the default run skips it (ADR 0003).

const run = promisify(execFile);
const CLI = path.join(import.meta.dirname, "../node_modules/.bin/agent-batch");
const PROJECT = "agent-batch-stack-test";
const PROJECT_LABEL = `label=com.docker.compose.project=${PROJECT}`;
const VERSION = "test";

const stateHome = await mkdtemp(path.join(tmpdir(), "agent-batch-stack-"));
const stateFile = path.join(stateHome, "agent-batch-nodejs/stack/state.json");
const composeFile = path.join(stateHome, "agent-batch-nodejs/stack/compose.json");
/** The environment of every command here: the state in a folder of its own, the images built here. */
const { AGENT_BATCH_URL: _url, AGENT_BATCH_API_KEY: _key, ...inherited } = process.env;
const env: NodeJS.ProcessEnv = {
  ...inherited,
  XDG_DATA_HOME: stateHome,
  AGENT_BATCH_STACK_PROJECT: PROJECT,
  AGENT_BATCH_VERSION: VERSION,
  // The Runner starts only with a key for its provider; the job's paused Batch keeps it from running.
  OPENROUTER_API_KEY: "not-a-key",
};

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

/** Calls the CLI with no selected hub; stack commands use this machine and job commands find its saved hub. */
function agentBatch(...args: string[]) {
  return run(CLI, args, { env, timeout: 120_000 });
}

// The data folder follows `XDG_DATA_HOME` on Linux only; elsewhere the test would use the user's own.
describe.skipIf(inject("hub") !== "node" || process.platform !== "linux")(
  "agent-batch stack",
  () => {
    afterAll(async () => {
      await agentBatch("stack", "delete");
      await rm(stateHome, { recursive: true, force: true });
    });

    test("keeps a usable stack running after start exits, preserves jobs on stop, and removes data on delete", async () => {
      await Promise.all(
        (["hub", "runner"] as const).map(async (target) => {
          const built = await buildRepositoryImage(target);
          await run("docker", ["tag", built, `ghcr.io/chuse-ai/agent-batch-${target}:${VERSION}`]);
        }),
      );
      const port = await getPort();
      const url = `http://localhost:${port}`;

      // An ambient provider key is not permission to forward it, even on the first start.
      await expect(agentBatch("stack", "start", "--port", String(port))).rejects.toMatchObject({
        code: 1,
        stderr: expect.stringContaining("login"),
      });
      expect(await dockerIds("ps")).toEqual([]);

      // Login works before a hub exists and leaves only the shared data volume behind.
      const login = agentBatch("stack", "login", "--provider", "openrouter", "--with-api-key");
      login.child.stdin?.end("stored-provider-key\n");
      await login;
      expect(await dockerIds("ps")).toEqual([]);
      expect(await dockerIds("volume")).toHaveLength(1);

      await agentBatch("stack", "delete");
      expect(await dockerIds("volume")).toEqual([]);
      await expect(stat(stateFile)).rejects.toMatchObject({ code: "ENOENT" });
      const loginAgain = agentBatch("stack", "login", "--provider", "openrouter", "--with-api-key");
      loginAgain.child.stdin?.end("stored-provider-key\n");
      await loginAgain;

      const started = await agentBatch("stack", "start", "--port", String(port));
      expect(started.stderr).toContain(`The hub is at ${url}`);
      expect((await fetch(`${url}/healthz`)).ok).toBe(true);
      expect(await dockerIds("ps", "--filter", "status=running")).toHaveLength(3);

      await agentBatch("stack", "logout", "--provider", "openrouter");
      await agentBatch("stack", "start", "--env", "OPENROUTER_API_KEY");
      const containers = await dockerIds("ps", "--filter", "status=running");
      // Forwarding saves only names; stopping and starting will use Docker's retained values.
      delete env.OPENROUTER_API_KEY;
      for (const file of [stateFile, composeFile]) {
        const content = await readFile(file, "utf8");
        expect(content).not.toContain("not-a-key");
        expect(content).not.toContain("stored-provider-key");
      }
      await expect(
        agentBatch("stack", "start", "--env", "OPENROUTER_API_KEY"),
      ).rejects.toMatchObject({
        code: 1,
        stderr: expect.stringContaining("OPENROUTER_API_KEY"),
      });
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual(containers);

      const { stdout: exports } = await agentBatch("stack", "env");
      expect(exports).toContain(`export AGENT_BATCH_URL=${url}`);
      await agentBatch("stack", "start");
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual(containers);
      expect((await agentBatch("stack", "env")).stdout).toBe(exports);

      // A failed replacement must leave commands pointed at the hub that is still serving them.
      const replacementPort = await getPort();
      await expect(
        run(CLI, ["stack", "start", "--port", String(replacementPort)], {
          env: { ...env, AGENT_BATCH_VERSION: "test-missing-version" },
          timeout: 120_000,
        }),
      ).rejects.toMatchObject({ code: 1 });
      expect((await fetch(`${url}/healthz`)).ok).toBe(true);
      expect((await agentBatch("stack", "env")).stdout).toBe(exports);

      // Status stays on this machine and reports the running images, regardless of the selected hub or version.
      const status = await run(CLI, ["stack", "status"], {
        env: {
          ...env,
          AGENT_BATCH_VERSION: "not-running",
          AGENT_BATCH_URL: "http://127.0.0.1:1",
          AGENT_BATCH_API_KEY: "another-hub-key",
        },
      });
      expect(status.stderr).toContain(url);
      expect(status.stdout).toContain(`ghcr.io/chuse-ai/agent-batch-hub:${VERSION}`);
      expect(status.stdout).toContain(`ghcr.io/chuse-ai/agent-batch-runner:${VERSION}`);
      expect(status.stdout).toContain("healthy");
      expect(status.stdout).toContain("source: OPENROUTER_API_KEY");
      expect(status.stdout).not.toContain("not-a-key");
      const logs = await agentBatch("stack", "logs");
      expect(logs.stdout).toContain("hub-1");
      expect(logs.stdout).toContain("database-1");

      // A paused Batch makes persistence independent of model access and job execution.
      const batch = await agentBatch(
        "batches",
        "create",
        "--json",
        JSON.stringify({ agent: { resultContract: { schema: { type: "integer" } } } }),
      );
      const { id: batchId } = z.parse(z.object({ id: z.string() }), JSON.parse(batch.stdout));
      await agentBatch("batches", "pause", "--id", batchId);
      const added = await agentBatch(
        "batches",
        "add-jobs",
        "--json",
        JSON.stringify({
          id: batchId,
          jobs: [{ customId: "survives-stop", input: "Count to ten." }],
        }),
      );
      const { jobs } = z.parse(
        z.object({ jobs: z.tuple([z.object({ job: z.object({ id: z.string() }) })]) }),
        JSON.parse(added.stdout),
      );
      const id = jobs[0].job.id;

      await agentBatch("stack", "stop");
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual([]);
      expect(await dockerIds("volume")).toHaveLength(2);
      expect((await agentBatch("stack", "env")).stdout).toBe(exports);
      expect((await agentBatch("stack", "status")).stdout).toContain("Exited");

      await agentBatch("stack", "start");
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual(containers);
      expect((await agentBatch("stack", "env")).stdout).toBe(exports);
      const job = await agentBatch("jobs", "get", "--id", id);
      expect(job.stderr).toContain(`agent-batch stack runs at ${url}`);
      expect(JSON.parse(job.stdout)).toMatchObject({
        id,
        customId: "survives-stop",
        status: "queued",
      });

      const [runnerBeforePortChange] = await dockerIds(
        "ps",
        "--filter",
        "label=com.docker.compose.service=runner",
      );
      await agentBatch("stack", "start", "--port", String(replacementPort));
      const replacementUrl = `http://localhost:${replacementPort}`;
      expect((await fetch(`${replacementUrl}/healthz`)).ok).toBe(true);
      expect((await agentBatch("stack", "env")).stdout).toBe(exports.replace(url, replacementUrl));
      expect(JSON.parse((await agentBatch("jobs", "get", "--id", id)).stdout)).toMatchObject({
        id,
        status: "queued",
      });

      expect(await dockerIds("ps", "--filter", "label=com.docker.compose.service=runner")).toEqual([
        runnerBeforePortChange,
      ]);

      // A deliberate login survives replacement, letting the caller stop forwarding a key.
      const replacementLogin = agentBatch(
        "stack",
        "login",
        "--provider",
        "openrouter",
        "--with-api-key",
      );
      replacementLogin.child.stdin?.end("replacement-provider-key\n");
      await replacementLogin;
      await agentBatch("stack", "start", "--clear-env");
      expect(
        await dockerIds("ps", "--filter", "label=com.docker.compose.service=runner"),
      ).not.toEqual([runnerBeforePortChange]);
      expect(JSON.parse(await readFile(stateFile, "utf8"))).toMatchObject({ env: [] });
      expect((await fetch(`${replacementUrl}/healthz`)).ok).toBe(true);
      expect((await agentBatch("stack", "status")).stdout).toContain("source: stored credential");

      const storedLoginContainers = await dockerIds("ps");
      const [storedLoginRunner] = await dockerIds(
        "ps",
        "--filter",
        "label=com.docker.compose.service=runner",
      );
      const { stdout: runningSince } = await run("docker", [
        "inspect",
        "--format",
        "{{.State.StartedAt}}",
        storedLoginRunner,
      ]);
      await agentBatch("stack", "logout");
      // Checking a running Runner must not stop its jobs just because the saved login is gone.
      await expect(agentBatch("stack", "start")).rejects.toMatchObject({
        code: 1,
        stderr: expect.stringContaining("agent-batch stack login --provider openrouter"),
      });
      expect(
        (await run("docker", ["inspect", "--format", "{{.State.StartedAt}}", storedLoginRunner]))
          .stdout,
      ).toBe(runningSince);
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual(storedLoginContainers);

      await agentBatch("stack", "stop");
      // Without forwarded inputs, preflight can find missing authentication before starting
      // any service, and must leave the deployment's endpoint and containers unchanged.
      await expect(agentBatch("stack", "start")).rejects.toMatchObject({
        code: 1,
        stderr: expect.stringContaining("agent-batch stack login --provider openrouter"),
      });
      expect(await dockerIds("ps")).toEqual(storedLoginContainers);
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual([]);
      expect((await agentBatch("stack", "env")).stdout).toBe(exports.replace(url, replacementUrl));

      const repair = agentBatch("stack", "login", "--with-api-key");
      repair.child.stdin?.end("repaired-provider-key\n");
      await repair;
      await agentBatch("stack", "start");
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual(storedLoginContainers);
      expect((await agentBatch("stack", "status")).stdout).toContain("source: stored credential");

      // An unrelated forwarded variable cannot supply authentication. Its retained value
      // forces startup through the real container, rather than the temporary preflight path.
      env.AGENT_BATCH_TEST_FORWARDING = "retained-noncredential";
      await agentBatch("stack", "start", "--env", "AGENT_BATCH_TEST_FORWARDING");
      delete env.AGENT_BATCH_TEST_FORWARDING;
      const forwardedContainers = await dockerIds("ps");
      const [forwardedRunner] = await dockerIds(
        "ps",
        "--filter",
        "label=com.docker.compose.service=runner",
      );
      await agentBatch("stack", "stop");
      await agentBatch("stack", "logout");
      // Readiness must fail before the subprocess watchdog, without leaving a restart loop.
      const failedRestart = agentBatch("stack", "start");
      await expect(failedRestart).rejects.toMatchObject({
        code: 1,
        stderr: expect.stringContaining("The Runner was stopped"),
      });
      await expect(failedRestart).rejects.toMatchObject({
        stderr: expect.stringContaining("agent-batch stack login --provider openrouter"),
      });
      expect(await dockerIds("ps")).toEqual(forwardedContainers);
      expect(await dockerIds("ps", "--filter", "status=running")).toHaveLength(2);
      expect(
        (
          await run("docker", ["inspect", "--format", "{{.State.Status}}", forwardedRunner])
        ).stdout.trim(),
      ).toBe("exited");
      expect(JSON.parse((await agentBatch("jobs", "get", "--id", id)).stdout)).toMatchObject({
        id,
        status: "queued",
      });
      expect((await agentBatch("stack", "env")).stdout).toBe(exports.replace(url, replacementUrl));

      const forwardedRepair = agentBatch("stack", "login", "--with-api-key");
      forwardedRepair.child.stdin?.end("repaired-provider-key\n");
      await forwardedRepair;
      await agentBatch("stack", "start");
      expect(await dockerIds("ps", "--filter", "status=running")).toEqual(forwardedContainers);
      expect((await agentBatch("stack", "status")).stdout).toContain("source: stored credential");

      await agentBatch("stack", "delete");
      expect(await dockerIds("ps")).toEqual([]);
      expect(await dockerIds("volume")).toEqual([]);
      await expect(stat(stateFile)).rejects.toMatchObject({ code: "ENOENT" });
      await expect(agentBatch("stack", "env")).rejects.toMatchObject({
        code: 1,
        stderr: expect.stringContaining("has not started a hub yet"),
      });
    });
  },
);
