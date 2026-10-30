import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import type { FauxResponseFactory } from "@earendil-works/pi-ai/providers/faux";
import * as NodeChildProcessSpawner from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import * as NodePath from "@effect/platform-node-shared/NodePath";
import { Effect, Layer, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { afterAll, beforeAll, expect, test } from "vite-plus/test";
import * as z from "zod/mini";

import { startSystem } from "./harness.ts";
import { callTool, lastToolResult, reply } from "./scripted-model.ts";

// The `agent-batch` command against a real hub and Runner, run as a caller runs it: in a
// process of its own, through the link pnpm makes to its bin. What it prints, the error it prints
// and how it exits.

const system = startSystem();

beforeAll(async () => {
  await system.start();
  await system.startRunner();
  process.env["AGENT_BATCH_URL"] = system.url().origin;
  process.env["AGENT_BATCH_API_KEY"] = await system.newJobsKey();
});
afterAll(() => system.stop());

const SUBMISSION = {
  agent: { outputContract: { schema: { type: "integer" } } },
  input: "Count to a million.",
};

const CLI = path.join(import.meta.dirname, "../node_modules/.bin/agent-batch");
const NodeServices = NodeChildProcessSpawner.layer.pipe(
  Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
);

/**
 * Runs `agent-batch` with `argv` until it ends: the one line of JSON it printed, its stderr and its
 * exit code, read as the Runner's `runCommand` reads a command's. A command that cannot start fails.
 */
function agentBatch(...argv: string[]) {
  return agentBatchIn(undefined, argv).pipe(
    Effect.map(({ lines, ...rest }) => ({ printed: lines[0], ...rest })),
    Effect.runPromise,
  );
}

/** Runs `agent-batch` with `argv` in the folder `cwd`: each line of JSON it printed, and the rest. */
function agentBatchIn(cwd: string | undefined, argv: readonly string[]) {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const handle = yield* spawner.spawn(ChildProcess.make(CLI, argv, { cwd }));
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        Stream.mkString(Stream.decodeText(handle.stdout)),
        Stream.mkString(Stream.decodeText(handle.stderr)),
        handle.exitCode,
      ],
      { concurrency: "unbounded" },
    );
    return {
      lines: stdout
        .split("\n")
        .filter((line) => line !== "")
        .map((line): unknown => JSON.parse(line)),
      stderr,
      exitCode,
    };
  }).pipe(Effect.scoped, Effect.provide(NodeServices));
}

test("jobs submit prints the job it accepted from a file, and jobs wait prints it once it has ended", async () => {
  system.model.setResponses([callTool("submit_output", { value: 1_000_000 })]);
  const dir = await mkdtemp(path.join(tmpdir(), "agent-batch-cli-"));
  const file = path.join(dir, "job.json");
  await writeFile(file, JSON.stringify(SUBMISSION));
  const submitted = await agentBatch("jobs", "submit", "--input", file);
  await rm(dir, { recursive: true });
  expect(submitted).toMatchObject({ printed: { status: "queued" }, exitCode: 0 });

  const { id } = z.parse(z.object({ id: z.string() }), submitted.printed);
  expect(await agentBatch("jobs", "wait", "--id", id)).toMatchObject({
    printed: { id, status: "succeeded", output: 1_000_000 },
    exitCode: 0,
  });
});

test("jobs wait exits non-zero for a job that ended without succeeding", async () => {
  system.model.setResponses([reply("I cannot count that far.")]);
  const submitted = await agentBatch("jobs", "submit", "--json", JSON.stringify(SUBMISSION));
  const { id } = z.parse(z.object({ id: z.string() }), submitted.printed);
  expect(await agentBatch("jobs", "wait", "--id", id)).toMatchObject({
    printed: { id, status: "failed" },
    exitCode: 1,
  });
});

test("a call the hub refuses prints its Problem Details and exits non-zero", async () => {
  const { stderr, exitCode } = await agentBatch("jobs", "get", "--id", crypto.randomUUID());
  expect(exitCode).toBe(1);
  expect(JSON.parse(stderr)).toMatchObject({ status: 404, title: "Not Found" });
});

// Each job lists the files in its `inputs/` folder, with their text, and submits the list.
const listInputs: FauxResponseFactory = ({ messages }) =>
  messages.some((message) => message.role === "toolResult")
    ? callTool("submit_output", { value: lastToolResult(messages).text })
    : callTool("bash", {
        command: `cd inputs && find . -type f | sort | while read -r f; do printf '%s=%s;' "$f" "$(cat "$f")"; done`,
      });

// `--wait` reads every 10 seconds, so the jobs, still running at the first read, end by the second.
test("batches submit --each uploads each file and folder as a job's Input files, and --wait prints their outputs in order", async () => {
  system.model.setResponses([listInputs, listInputs, listInputs, listInputs]);
  const dir = await mkdtemp(path.join(tmpdir(), "agent-batch-cli-"));
  await mkdir(path.join(dir, "claim/photos"), { recursive: true });
  await writeFile(path.join(dir, "notes.txt"), "one");
  await writeFile(path.join(dir, "claim/form.txt"), "form");
  await writeFile(path.join(dir, "claim/photos/front.txt"), "front");
  await writeFile(path.join(dir, "claim/.hidden"), "hidden");
  await writeFile(path.join(dir, "schema.json"), JSON.stringify({ type: "string" }));

  const { lines, exitCode } = await Effect.runPromise(
    agentBatchIn(dir, [
      "batches",
      "submit",
      "--each",
      "./notes.txt",
      "claim/",
      "--output-schema",
      "schema.json",
      "--wait",
    ]),
  );
  await rm(dir, { recursive: true });

  expect(exitCode).toBe(0);
  expect(lines).toEqual([
    {
      customId: "notes.txt",
      status: "succeeded",
      output: expect.stringMatching(/^\.\/notes\.txt=one;\s*$/),
    },
    {
      customId: "claim",
      status: "succeeded",
      output: expect.stringMatching(/^\.\/form\.txt=form;\.\/photos\/front\.txt=front;\s*$/),
    },
  ]);
}, 30_000);
