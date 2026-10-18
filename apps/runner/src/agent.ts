import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import { isJsonValue, type JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { type ModelThinkingLevel, type Models, Type } from "@earendil-works/pi-ai";
import {
  type ConversationHandle,
  createRegistry,
  defineDoc,
  defineExtension,
  defineTool,
  GenerationTask,
  Harness,
  hook,
} from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import type { JobContent } from "contract";
import { Effect } from "effect";
import ensureError from "ensure-error";
import { compileSchema } from "json-schema-library";

import { WorkspaceEnv } from "./workspace.ts";

// Runs a job's agent with pi-durable: one Harness and SQLite journal per job, so purging a job
// deletes its file (pi-durable cannot delete a conversation) and the journal holds exactly the
// job's entries in order. The agent has pi-durable's coding tools on the job's Workspace and
// `submit_result`, which checks the result against the Output contract, as the hub checks the
// contract itself (ADR 0025), and ends the run once a result is accepted (ADR 0009).

/** The model every job runs on until jobs choose their own, as Pi names it (ADR 0013). */
export interface AgentModel {
  provider: string;
  modelId: string;
  thinkingLevel: ModelThinkingLevel;
}

/** How the Runner runs every job's agent: where jobs keep their files, and on which model. */
export interface AgentSettings {
  /** The directory holding one directory per job: its journal and its Workspace. */
  jobsDir: string;
  models: Models;
  model: AgentModel;
}

/** What became of a job's agent. */
export type AgentOutcome = { state: "succeeded"; result: unknown } | { state: "failed" };

/**
 * The result `submit_result` accepted first, committed with the call so a restart keeps it; absent
 * until one is accepted, since `null` is itself a result.
 */
const AcceptedResult = defineDoc<{ result?: JsonValue }>({
  kind: "agent-batch.result",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  initial: () => ({}),
});

/** Where a job's files are on the Runner: its journal, and its Workspace with the Input files. */
function jobPaths(jobsDir: string, jobId: string) {
  const dir = path.join(jobsDir, jobId);
  return { journal: path.join(dir, "journal.sqlite"), workspace: path.join(dir, "workspace") };
}

/**
 * Writes the Input files into the Workspace's `inputs/` folder, once, as the job's conversation is
 * created: a resumed job keeps the agent's edits to them. Their names are flat file names valid on
 * every platform: the contract's schema checked them again when the placement reached the
 * Runner, so none leaves the folder.
 */
async function writeInputFiles(workspace: string, files: JobContent["inputFiles"]) {
  const inputs = path.join(workspace, "inputs");
  await mkdir(inputs, { recursive: true });
  for (const [name, file] of Object.entries(files)) {
    await writeFile(path.join(inputs, name), file.content);
  }
}

/** What the agent is told about the job, beside its Prompt. */
const INSTRUCTIONS = `You are running a job unattended; nobody will answer questions. Your Workspace, where your files are and your commands run, is /; the job's Input files, if any, are in /inputs.
When the work is complete, call submit_result once with the complete result: as \`value\`, or, for a large result or one a script produced, as the \`path\` of a JSON file in the Workspace. The result is checked against the job's Output contract; if it is rejected, correct it and submit again. A reply in prose does not complete the job.`;

function submitResultTool(schema: Record<string, unknown>) {
  const contract = compileSchema(schema);
  return defineTool({
    name: "submit_result",
    description: `Submit the job's result, which ends the job. Give exactly one of \`value\`, the result itself, or \`path\`, a JSON file in the Workspace holding it. The result must match this JSON Schema:\n${JSON.stringify(schema)}`,
    parameters: Type.Object({
      value: Type.Optional(Type.Unknown({ description: "The result" })),
      path: Type.Optional(
        Type.String({ description: "A JSON file in the Workspace holding the result" }),
      ),
    }),
    execute: async (args, api, context) => {
      // Presence, not truthiness: `false`, `0` and `null` are results.
      const given = ["value" in args, "path" in args].filter(Boolean).length;
      if (given !== 1) throw new Error("Give exactly one of `value` and `path`");
      let value: JsonValue;
      if (args.path === undefined) {
        if (!isJsonValue(args.value)) throw new Error("`value` is not JSON");
        value = args.value;
      } else {
        if (!api.env) throw new Error("The Workspace is not available");
        const text = await api.env.readTextFile(args.path, context);
        if (!text.ok) throw new Error(`Cannot read ${args.path}: ${text.error.message}`);
        try {
          value = JSON.parse(text.value);
        } catch (error) {
          throw new Error(`${args.path} is not JSON: ${ensureError(error).message}`, {
            cause: error,
          });
        }
      }
      const { valid, errors } = contract.validate(value);
      if (!valid) {
        const problems = errors.map((error) => `- ${error.message}`).join("\n");
        throw new Error(`The result does not match the Output contract:\n${problems}`);
      }
      // The first accepted result is the job's, as when the model submits twice in one round.
      const first = await api.commit(async (tx) => {
        const accepted = await tx.doc(AcceptedResult, api.conversationId);
        if ("result" in accepted) return false;
        accepted.result = value;
        return true;
      }, context);
      const text = first
        ? "Result accepted."
        : "A result was already accepted; this one is ignored.";
      return { output: [{ type: "text", text }], control: { terminate: true } };
    },
  });
}

/**
 * Runs `job`'s agent in `<jobsDir>/<jobId>` until it submits an accepted result or ends without
 * one. `dropped` completes when the hub drops the placement: the run is then aborted, which ends the
 * job's work; interruption, as when the Runner stops, only closes the journal, so the job can resume.
 */
export function runAgent(
  jobId: string,
  job: JobContent,
  options: AgentSettings & { dropped: Effect.Effect<void> },
): Effect.Effect<AgentOutcome> {
  const context = BACKGROUND_CONTEXT;
  const paths = jobPaths(options.jobsDir, jobId);
  return Effect.gen(function* () {
    // The conversation is known only once the Harness is open; the hook below needs it.
    let conversation: ConversationHandle | undefined;
    const registry = createRegistry();
    registry.install(CodingTools);
    registry.install(
      defineExtension({
        name: "agent-batch",
        tools: [submitResultTool(job.outputContract.schema)],
        hooks: [
          // An accepted result ends the run only when every call of its round asks to; the job
          // ends once the round's other calls are done, without another model turn. The result is
          // read from the journal, so a round a restarted Runner finishes ends the run too.
          hook(GenerationTask, {
            afterTools: async (_assistant, _results, api) => {
              const accepted = await api.snapshot(AcceptedResult, api.conversationId, context);
              if (!accepted || !("result" in accepted)) return;
              // Not awaited: the abort waits for this run, which is waiting for this hook. It
              // fails only when the journal closes first, as when the Runner stops, or when the
              // abort cannot be committed; the job's outcome is read from the journal either way.
              conversation?.abort(context).catch(() => {});
            },
          }),
        ],
      }),
    );
    // The Workspace exists before its Input files: they are written only when the job's
    // conversation is created.
    yield* Effect.promise(() => mkdir(paths.workspace, { recursive: true }));
    const env = new WorkspaceEnv(`job:${jobId}`, paths.workspace);
    const harness = yield* Effect.acquireRelease(
      Effect.promise(async () =>
        Harness.open(
          await openNodeSqliteStorage(paths.journal),
          { models: options.models, registry, env: () => env },
          context,
        ),
      ),
      (opened) => Effect.promise(() => opened.close(context)),
    );
    const root = yield* Effect.promise(() =>
      harness.root(context, {
        agent: {
          model: { provider: options.model.provider, modelId: options.model.modelId },
          thinkingLevel: options.model.thinkingLevel,
          instructions: INSTRUCTIONS,
          cwd: "/",
        },
        init: () => writeInputFiles(paths.workspace, job.inputFiles),
      }),
    );
    conversation = root;
    // The request ID makes the Prompt's submission happen once, also when a restarted Runner opens
    // the same journal again.
    const answered = Effect.promise(async () => {
      const submission = await root.submit(
        { type: "input", content: job.prompt, requestId: `job:${jobId}` },
        context,
      );
      await submission.wait(context);
    });
    const abortOnDrop = options.dropped.pipe(
      Effect.andThen(Effect.promise(() => root.abort(context))),
    );
    yield* Effect.raceFirst(answered, abortOnDrop);
    const accepted = yield* Effect.promise(() =>
      harness.snapshot(AcceptedResult, root.id, context),
    );
    // An agent that stopped without an accepted result fails the job at once; reminding it first
    // is planned with failure reporting and result reminders.
    return accepted && "result" in accepted
      ? ({ state: "succeeded", result: accepted.result } as const)
      : ({ state: "failed" } as const);
  }).pipe(Effect.scoped);
}
