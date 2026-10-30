import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import * as NodeChildProcessSpawner from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import * as NodePath from "@effect/platform-node-shared/NodePath";
import { Effect, Layer, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";

import { lifecycleOf } from "./entries.ts";
import { startSystem, submitJob } from "./harness.ts";

// A Runner can die without running any of its own cleanup, as when the kernel's out-of-memory
// killer or `kill -9` stops it. Started again on the same data directory, it recovers its jobs from
// their journals. The Runner runs in a process of its own, `runner-restart.runner.ts`, so the test
// can kill it so, together with the processes it started.

const system = startSystem();

beforeAll(() => system.start());
afterAll(() => system.stop());

const NodeServices = NodeChildProcessSpawner.layer.pipe(
  Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
);

/**
 * Starts the Runner's process on `dataDir` in `phase`, killed when the scope closes; it registers
 * on its first start. Its scripted model writes to file descriptor 3 once the job is waiting. It is
 * killed outright: it runs without the Runner's entry point, whose `runMain` exits on `SIGTERM`,
 * and sandbox-runtime's own `SIGTERM` listener keeps Node from exiting on it.
 */
function startRunnerProcess(dataDir: string, registrationKey: string, phase: "before" | "after") {
  return ChildProcessSpawner.ChildProcessSpawner.use((spawner) =>
    spawner.spawn(
      ChildProcess.make(
        process.execPath,
        [
          path.join(import.meta.dirname, "runner-restart.runner.ts"),
          system.url().href,
          registrationKey,
          dataDir,
          phase,
        ],
        {
          stdout: "inherit",
          stderr: "inherit",
          additionalFds: { fd3: { type: "output" } },
          killSignal: "SIGKILL",
        },
      ),
    ),
  );
}

test(
  "a job whose Runner is killed mid-run is recovered when the Runner starts again, with each completed tool call run once",
  () =>
    Effect.gen(function* () {
      const dataDir = yield* Effect.promise(() => mkdtemp(path.join(tmpdir(), "runner-")));
      const registrationKey = yield* Effect.promise(() => system.createRegistrationKey());
      const client = yield* Effect.promise(() => system.clientWithNewJobsKey());
      const before = yield* startRunnerProcess(dataDir, registrationKey, "before");

      const { id } = yield* Effect.promise(() =>
        submitJob(client, {
          input: "Mark the file once.",
          outputContract: {
            schema: {
              type: "object",
              properties: { lines: { type: "integer" } },
              required: ["lines"],
            },
          },
        }),
      );
      yield* Stream.runHead(before.getOutputFd(3));
      yield* before.kill({ killSignal: "SIGKILL" });
      yield* startRunnerProcess(dataDir, registrationKey, "after");

      yield* Effect.promise(() =>
        expect
          .poll(() => client.jobs.get({ id }), { timeout: 20_000 })
          .toMatchObject({ status: "succeeded", output: { lines: 1 } }),
      );
      // The job's events record its recovery by the restarted Runner, between its start and end.
      const { events } = yield* Effect.promise(() => client.jobs.events({ id }));
      expect(lifecycleOf(events)).toEqual([
        "agent-batch.job.started",
        "agent-batch.job.recovered",
        "agent-batch.job.succeeded",
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices), Effect.runPromise),
  60_000,
);
