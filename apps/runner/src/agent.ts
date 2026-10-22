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
  ROOT_CONVERSATION_ID,
} from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import type { JobContent } from "contract";
import { Effect, Exit, type FileSystem, type PlatformError, Scope } from "effect";
import ensureError from "ensure-error";
import { compileSchema } from "json-schema-library";

import {
  appendEndOnce,
  endRequest,
  JobCancelled,
  JobFailed,
  JobRecovered,
  JobStarted,
  JobSucceeded,
  RUNNER_VERSION,
} from "./journal.ts";
import type { JobEnvironments } from "./sandbox.ts";

// Runs a job's agent with pi-durable: one Harness and SQLite journal per job, so purging a job
// deletes its file (pi-durable cannot delete a conversation) and the journal holds exactly the
// job's entries in order. The agent has pi-durable's coding tools on the job's Workspace, in
// the job's sandbox (ADR 0027), and `submit_result`, which checks the result against the Result
// contract, as the hub checks the contract itself (ADR 0025), and ends the run once a result is
// accepted (ADR 0009).

/** The model every job runs on until jobs choose their own, as Pi names it (ADR 0013). */
export interface AgentModel {
  provider: string;
  modelId: string;
  thinkingLevel: ModelThinkingLevel;
}

/** How the Runner runs every job's agent: where jobs keep their files, in which sandbox, and on which model. */
export interface AgentSettings {
  /** The directory holding one directory per job: its journal, its Workspace and its temp directory. */
  jobsDir: string;
  /** The Runner's file system, where it looks for jobs' journals. */
  fileSystem: FileSystem.FileSystem;
  environments: JobEnvironments;
  models: Models;
  model: AgentModel;
}

/**
 * The result `submit_result` accepted first, committed in its own transaction before the call's
 * result, so a restart keeps it even when the call itself was cut off; absent until one is accepted,
 * since `null` is itself a result.
 */
const AcceptedResult = defineDoc<{ result?: JsonValue }>({
  kind: "agent-batch.result",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  initial: () => ({}),
});

/**
 * Where a job's files are on the Runner: its journal, its Workspace with the Input files, and its
 * temp directory. Absolute, since the agent sees them at these paths.
 */
function jobPaths(jobsDir: string, jobId: string) {
  const dir = path.resolve(jobsDir, jobId);
  return {
    journal: path.join(dir, "journal.sqlite"),
    workspace: path.join(dir, "workspace"),
    temp: path.join(dir, "tmp"),
  };
}

/**
 * Whether the job has a journal on this Runner, as a job that started here has. Fails when that
 * cannot be told, as on a permission error.
 */
export function journalExists(
  settings: AgentSettings,
  jobId: string,
): Effect.Effect<boolean, PlatformError.PlatformError> {
  return settings.fileSystem.exists(jobPaths(settings.jobsDir, jobId).journal);
}

/**
 * Writes the Input files into the Workspace's `inputs/` folder, once, as the job's conversation is
 * created: a job recovered after its Runner restarts keeps the agent's edits to them. Their
 * names are flat file names valid on every platform: the contract's schema checked them again
 * when the placement reached the Runner, so none leaves the folder.
 */
async function writeInputFiles(workspace: string, files: JobContent["inputFiles"]) {
  const inputs = path.join(workspace, "inputs");
  await mkdir(inputs, { recursive: true });
  for (const [name, file] of Object.entries(files)) {
    await writeFile(path.join(inputs, name), file.content);
  }
}

/**
 * What the agent is told about the job, beside its Prompt. The
 * sandbox's `/tmp` is the job's own but held in memory, which counts against the job's memory limit.
 */
function instructions(paths: { workspace: string }) {
  return `You are running a job unattended; nobody will answer questions. Your Workspace, where your files are and your commands run, is ${paths.workspace}; the job's Input files, if any, are in ${path.join(paths.workspace, "inputs")}. For temporary files, use the directory in $TMPDIR, not /tmp.
When the work is complete, call submit_result once with the complete result: as \`value\`, or, for a large result or one a script produced, as the \`path\` of a JSON file in the Workspace. The result is checked against the job's Result contract; if it is rejected, correct it and submit again. A reply in prose does not complete the job.`;
}

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
        throw new Error(`The result does not match the Result contract:\n${problems}`);
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
 * Opens `job`'s journal and sandbox in `<jobsDir>/<jobId>`, and gives the journal to
 * `journalOpened` before anything is written to it. The journal stays open until the caller's scope
 * closes, and the sandbox until the job's run ends. Records in the journal that the job started,
 * the first time, or that it was recovered: a journal that already holds the job, unless
 * `openedBefore` says this process opened it, is one the Runner left before it restarted, since the
 * hub places a started job on no other Runner, and not here again while it runs. A job whose
 * placement has ended is not recovered.
 *
 * Gives back the job's run, which runs its agent until it submits an accepted result or ends without
 * one, and records the job's end in the journal. Interrupting the run, as a Runner that stops, a
 * placement the hub drops and a cancel do, aborts none of the job's work: the caller's scope
 * then closes the journal and then the sandbox, which cuts the work off and leaves it pending in the
 * journal, so the job carries on from it when it is opened again, unless recording a cancel settles
 * it (`recordCancel`).
 */
export function openJob(
  jobId: string,
  job: JobContent,
  options: AgentSettings & {
    generation: number;
    openedBefore: boolean;
    journalOpened: (journal: Harness) => void;
  },
): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> {
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
        tools: [submitResultTool(job.resultContract.schema)],
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
    // The sandbox has a scope of its own, which the run closes once it has ended, while the journal
    // stays open for reporting. A Runner that stops closes the journal first and the sandbox after
    // it, so no tool call is left on a stopped daemon.
    const sandbox = yield* Scope.fork(yield* Effect.scope);
    const { env } = yield* Effect.acquireRelease(
      Effect.sync(() => options.environments.open(paths)),
      (opened) => Effect.promise(() => opened.close()),
    ).pipe(Scope.provide(sandbox));
    const harness = yield* Effect.acquireRelease(
      Effect.promise(async () =>
        Harness.open(
          await openNodeSqliteStorage(paths.journal),
          {
            models: options.models,
            registry,
            env: () => env,
            // Partial answers and command output are saved once a second rather than every 100 ms
            // (ADR 0016): the saves run on the thread all jobs share, and only a restart reads them,
            // losing at most a second of the output of a command it cut off.
            settings: { progress: { partialIntervalMs: 1_000, outputIntervalMs: 1_000 } },
          },
          context,
        ),
      ),
      (opened) => Effect.promise(() => opened.close(context)),
    );
    options.journalOpened(harness);
    const opening = () => ({
      timestamp: Date.now(),
      generation: options.generation,
      runnerVersion: RUNNER_VERSION,
    });
    let created = false;
    const root = yield* Effect.promise(() =>
      harness.root(context, {
        agent: {
          model: { provider: options.model.provider, modelId: options.model.modelId },
          thinkingLevel: options.model.thinkingLevel,
          instructions: instructions(paths),
          cwd: paths.workspace,
        },
        // The commit that creates the job's conversation, so the start is recorded exactly once.
        init: async (tx, conversationId) => {
          created = true;
          await tx.appendEntry(JobStarted, conversationId, { data: opening() });
          await writeInputFiles(paths.workspace, job.inputFiles);
        },
      }),
    );
    conversation = root;
    // A placement that has ended is not recovered, nor run again: its cancel leaves its work
    // pending, and only a new placement carries that on. Whether it has ended.
    const ended =
      !created &&
      (yield* Effect.promise(() =>
        root.commit(async (tx) => {
          if (await tx.submissionByRequest(root.id, endRequest(options.generation))) return true;
          if (!options.openedBefore) {
            await tx.appendEntry(JobRecovered, root.id, { data: opening() });
          }
          return false;
        }, context),
      ));
    // pi's plain abort leaves background work, such as a compaction, running, so this abort includes
    // it and waits until all of it has stopped: nothing then adds to the journal after the job's end
    // or runs on in its sandbox.
    const stopWork = Effect.promise(() => root.abort(context, { background: true }));
    const run = Effect.gen(function* () {
      if (ended) return;
      // The request ID makes the Prompt's submission happen once, also when a restarted Runner
      // opens the same journal again.
      const answered = Effect.promise(async () => {
        const submission = await root.submit(
          { type: "input", content: job.prompt, requestId: `job:${jobId}` },
          context,
        );
        await submission.wait(context);
      });
      // Once the job has finished, or pi has failed, all of its work stops. Interrupting the run
      // leaves the work pending instead.
      yield* answered.pipe(
        Effect.onExitIf(
          (exit) => !Exit.hasInterrupts(exit),
          () => stopWork,
        ),
      );
      const accepted = yield* Effect.promise(() =>
        harness.snapshot(AcceptedResult, root.id, context),
      );
      // An agent that stopped without an accepted result fails the job at once; reminding it
      // first is planned with failure reporting and result reminders.
      // JSON holds no `undefined`, so a result that is absent was never accepted.
      const result = accepted?.result;
      const timestamp = Date.now();
      yield* Effect.promise(() =>
        root.commit(
          (tx) =>
            result === undefined
              ? appendEndOnce(tx, root.id, options.generation, JobFailed, { data: { timestamp } })
              : appendEndOnce(tx, root.id, options.generation, JobSucceeded, {
                  data: { timestamp, result },
                }),
          context,
        ),
      );
    }).pipe(
      // However the run ends, its sandbox goes, and with it every process the job left running. A
      // Runner that stops interrupts the run instead, and closes the sandbox after the journal.
      Effect.onExitIf(
        (exit) => !Exit.hasInterrupts(exit),
        (exit) => Scope.close(sandbox, exit),
      ),
    );
    return run;
  });
}

/**
 * Records the cancel of the job `jobId`'s placement `generation`, once its run has been
 * interrupted. A cancel is a decision about the job, so it settles the work the interrupt left
 * pending, as pi's Esc does (ADR 0028): opens the job's journal without letting any of that work
 * run, since pi runs nothing until asked to (`Harness.resume`), aborts it, which writes the cut-off
 * answer and tool calls into the conversation with their usage, and appends the job's end, so that
 * the end is the journal's last entry. A job whose accepted result the interrupted run had not yet
 * recorded as its end succeeded, since completion ends a run (ADR 0009); one whose placement has
 * already ended keeps that end. Gives back the journal, open until the caller's scope closes, or
 * nothing when the job has no journal on this Runner: a job its Runner never started has nothing to
 * record.
 */
export function recordCancel(
  jobId: string,
  generation: number,
  settings: AgentSettings,
): Effect.Effect<Harness | undefined, PlatformError.PlatformError, Scope.Scope> {
  const context = BACKGROUND_CONTEXT;
  const paths = jobPaths(settings.jobsDir, jobId);
  return Effect.gen(function* () {
    // Opening a journal that does not exist would create one.
    if (!(yield* journalExists(settings, jobId))) return undefined;
    const journal = yield* Effect.acquireRelease(
      Effect.promise(async () =>
        Harness.open(
          await openNodeSqliteStorage(paths.journal),
          // Nothing runs, and every registry holds pi's own tasks, whose abort settles the work: no
          // tool needs registering and no environment opening.
          { models: settings.models, registry: createRegistry() },
          context,
        ),
      ),
      (opened) => Effect.promise(() => opened.close(context)),
    );
    const root = yield* Effect.promise(() => journal.conversation(ROOT_CONVERSATION_ID, context));
    // A root conversation that was never created, as when the Runner stopped while opening the
    // journal, means the job never started.
    if (!root) return journal;
    // Background work, such as a compaction, is aborted too, as at the job's own end.
    yield* Effect.promise(() => root.abort(context, { background: true }));
    // Read before the commit, where reading the document would create it; nothing runs to change it.
    const result = (yield* Effect.promise(() => journal.snapshot(AcceptedResult, root.id, context)))
      ?.result;
    const timestamp = Date.now();
    yield* Effect.promise(() =>
      root.commit(
        (tx) =>
          result === undefined
            ? appendEndOnce(tx, root.id, generation, JobCancelled, { data: { timestamp } })
            : appendEndOnce(tx, root.id, generation, JobSucceeded, {
                data: { timestamp, result },
              }),
        context,
      ),
    );
    return journal;
  });
}
