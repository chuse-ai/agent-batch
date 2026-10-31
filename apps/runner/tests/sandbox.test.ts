import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { homedir, tmpdir } from "node:os";
import path from "node:path";

import { BACKGROUND_CONTEXT, withCancel } from "@earendil-works/chord/context";
import { getOrThrow } from "@earendil-works/pi-durable/env";
import { registerEnvConformance } from "@earendil-works/pi-durable/testing";
import { Effect } from "effect";
import { beforeAll, describe, expect, it, onTestFinished, vi } from "vite-plus/test";

import { DEFAULT_SANDBOX_SETTINGS } from "../src/index.ts";
import {
  type JobEnvironment,
  type JobEnvironments,
  prepareJobEnvironments,
} from "../src/sandbox.ts";

// pi-durable's tools reach a job's files and commands only through this environment: pi-env's
// daemon in the job's sandbox (ADR 0027). So it is held to the cases pi-durable defines for every
// environment, and to what the sandbox promises a job: its own folders and the system's programs,
// no network, none of the Runner's variables, no other processes, a memory limit where systemd can
// set one, and on Linux processes the kernel kills before the Runner when the host runs out of
// memory.

let environments: JobEnvironments;
beforeAll(async () => {
  environments = await Effect.runPromise(
    prepareJobEnvironments({ ...DEFAULT_SANDBOX_SETTINGS, jobMemoryLimit: "256M" }),
  );
});

/** Runs `use` with a job's environment in a new job directory, closed and removed afterwards. */
async function withJob(
  use: (
    job: JobEnvironment,
    dirs: { dir: string; workspace: string; temp: string },
  ) => Promise<void>,
) {
  const dir = await mkdtemp(path.join(tmpdir(), "job-"));
  const dirs = { dir, workspace: path.join(dir, "workspace"), temp: path.join(dir, "tmp") };
  await mkdir(dirs.workspace);
  const job = environments.open(path.basename(dir), dirs);
  try {
    await use(job, dirs);
  } finally {
    await job.close();
    await rm(dir, { recursive: true, force: true });
  }
}

/** Runs `command` in the job's shell and returns its exit code and its output, both streams together. */
async function run(job: JobEnvironment, command: string) {
  let output = "";
  const result = await job.env.exec(
    command,
    { onOutput: (text) => (output += text) },
    BACKGROUND_CONTEXT,
  );
  return { exitCode: getOrThrow(result).exitCode, output };
}

/**
 * Leaves a `sleep` running in the job's background, for a duration no other process uses, and
 * returns whether it is still running, as seen from outside the sandbox.
 */
async function leaveSleepRunning(job: JobEnvironment) {
  const seconds = String(1000 + Math.floor(Math.random() * 9000));
  await run(job, `nohup sleep ${seconds} >/dev/null 2>&1 &`);
  const sleeping = () =>
    new Promise<boolean>((resolve) =>
      execFile("pgrep", ["-f", `sleep ${seconds}`], (error) => resolve(error === null)),
    );
  // Polled: while the background shell execs `nohup` and then `sleep`, its command line is briefly empty.
  await expect.poll(sleeping).toBe(true);
  return { seconds, sleeping };
}

registerEnvConformance({ describe, it, expect }, "pi-durable's environment cases", (use) =>
  withJob((job) => use(job.env)),
);

// pi-durable's cases leave out the byte-order mark, which files saved by Windows tools, such as
// Excel's CSVs, begin with: an agent's edit must keep it.
it("a file's text keeps its byte-order mark, which pi-durable's edit tool writes back", () =>
  withJob(async (job, { workspace }) => {
    await writeFile(path.join(workspace, "q3.csv"), "﻿quarter,revenue\r\n");
    const text = await job.env.readTextFile("q3.csv", BACKGROUND_CONTEXT);
    expect(text).toEqual({
      ok: true,
      value: await readFile(path.join(workspace, "q3.csv"), "utf8"),
    });
  }));

it("a job reads and writes its own folders, and nothing of another job, its journal or the operator's home", () =>
  withJob(async (job, { dir, workspace }) => {
    const other = await mkdtemp(path.join(tmpdir(), "other-job-"));
    try {
      const journal = path.join(dir, "journal.sqlite");
      await writeFile(path.join(other, "file.txt"), "another job's file");
      await writeFile(journal, "the job's journal");
      await symlink(path.join(other, "file.txt"), path.join(workspace, "link-out"));

      expect((await run(job, "echo made > made.txt && cat made.txt")).output).toBe("made\n");
      for (const outside of [
        path.join(other, "file.txt"),
        path.join(workspace, "link-out"),
        journal,
        path.join(homedir(), ".bashrc"),
      ]) {
        expect((await job.env.readTextFile(outside, BACKGROUND_CONTEXT)).ok).toBe(false);
        expect((await run(job, `cat '${outside}'`)).exitCode).not.toBe(0);
      }
      // Outside its own folders a job sees only empty folders private to its sandbox, so what it
      // writes there never reaches the host.
      await job.env.writeFile(path.join(other, "x"), "x", BACKGROUND_CONTEXT);
      await run(job, `echo x > '${path.join(other, "y")}'`);
      expect(await readdir(other)).toEqual(["file.txt"]);
      await job.env.writeFile(journal, "x", BACKGROUND_CONTEXT);
      await run(job, `echo x > '${journal}'`);
      expect(await readFile(journal, "utf8")).toBe("the job's journal");
    } finally {
      await rm(other, { recursive: true, force: true });
    }
  }));

// sandbox-runtime gives every sandbox an HTTP proxy, named in `HTTPS_PROXY`, and a SOCKS proxy,
// named in `FTP_PROXY` (its `ALL_PROXY` names the HTTP one), which allow the domains its
// configuration allows: for a job, none.
it("a job's commands have no network, directly or through the sandbox's proxies", async () => {
  const server = createServer((_request, response) => response.end("reached"));
  server.on("error", () => {});
  server.on("connection", (socket) => socket.on("error", () => {}));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("no port");
  const url = `http://127.0.0.1:${address.port}/`;
  try {
    await withJob(async (job) => {
      const direct = await run(job, `bash -c 'exec 3<>/dev/tcp/127.0.0.1/${address.port}'`);
      expect(direct.exitCode).not.toBe(0);

      const proxies = (await run(job, 'echo "$HTTPS_PROXY"; echo "$FTP_PROXY"')).output
        .split("\n")
        .filter(Boolean);
      expect(proxies).toHaveLength(2);
      for (const proxy of proxies) {
        // Without `--proxytunnel` curl asks the HTTP proxy for the URL; with it, for a tunnel.
        for (const tunnel of proxy.startsWith("http:") ? ["", "--proxytunnel"] : [""]) {
          const reached = await run(
            job,
            `curl -sS --max-time 5 --noproxy '' ${tunnel} --proxy '${proxy}' ${url}`,
          );
          expect(reached.output).not.toContain("reached");
        }
      }
    });
  } finally {
    server.close();
  }
});

it("a job's processes see none of the Runner's variables, nor its processes or another job's", async () => {
  vi.stubEnv("AGENT_BATCH_TEST_RUNNER_VARIABLE", "runner-only");
  onTestFinished(() => {
    vi.unstubAllEnvs();
  });
  await withJob(async (job) => {
    expect((await run(job, "env")).output).not.toContain("runner-only");
    expect((await run(job, `kill -0 ${process.pid}`)).exitCode).not.toBe(0);
    expect((await run(job, "ps -e -o comm=")).output).not.toMatch(/\bnode\b/);
    await withJob(async (other) => {
      const { seconds } = await leaveSleepRunning(other);
      expect((await run(job, "ps -e -o args=")).output).not.toContain(`sleep ${seconds}`);
    });
  });
});

it("a job's temporary files go to its temp directory", () =>
  withJob(async (job, { temp }) => {
    expect((await run(job, 'echo "$TMPDIR"')).output).toBe(`${temp}\n`);
    expect((await run(job, "mktemp")).output.startsWith(`${temp}/`)).toBe(true);
  }));

it("aborting or timing out a command stops it without touching another job's, and closing a job stops what it left running", () =>
  withJob((other, { workspace: othersWorkspace }) =>
    withJob(async (job) => {
      // Runs until the test, having stopped this job's commands, lets it end.
      const othersCommand = run(
        other,
        "until [ -e done ]; do sleep 0.05; done; echo still running",
      );

      const { context, cancel } = withCancel(BACKGROUND_CONTEXT);
      let started = Date.now();
      const aborted = job.env.exec("sleep 30", undefined, context);
      setTimeout(() => cancel(), 200);
      const abortedResult = await aborted;
      expect(abortedResult.ok ? undefined : abortedResult.error.code).toBe("aborted");
      expect(Date.now() - started).toBeLessThan(5_000);

      started = Date.now();
      const timedOut = await job.env.exec("sleep 30", { timeout: 0.2 }, BACKGROUND_CONTEXT);
      expect(timedOut.ok ? undefined : timedOut.error.code).toBe("timeout");
      expect(Date.now() - started).toBeLessThan(5_000);

      await writeFile(path.join(othersWorkspace, "done"), "");
      expect((await othersCommand).output).toBe("still running\n");

      const { sleeping } = await leaveSleepRunning(job);
      await job.close();
      await expect.poll(sleeping).toBe(false);
    }),
  ));

it("a job's processes are the kernel's choice over the Runner when the host runs out of memory", async (test) => {
  if (process.platform !== "linux") test.skip(`${process.platform} has no OOM scores`);
  await withJob(async (job) => {
    const { output } = await run(job, "cat /proc/self/oom_score_adj");
    const runners = Number(await readFile("/proc/self/oom_score_adj", "utf8"));
    expect(Number(output)).toBeGreaterThan(runners);
  });
});

it("a command that goes over the job's memory limit is stopped and the agent told, and the job carries on", async (test) => {
  if (environments.uncappedBecause) test.skip(environments.uncappedBecause);
  await withJob(async (job) => {
    const over = await run(job, "python3 -c 'b = bytearray(1 << 30); print(len(b))'");
    expect(over.exitCode).not.toBe(0);
    expect(over.output).toContain("went over their 256M of memory");
    // The kill count can still rise after the command over the limit has returned, as it has on
    // CI, and every command that ends while it rises is told, so this one may be too.
    const next = await run(job, "echo still here");
    expect(next.exitCode).toBe(0);
    expect(next.output).toMatch(/^still here\n/);
  });
});
