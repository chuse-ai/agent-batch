import { mkdir } from "node:fs/promises";
import path from "node:path";

import { isJsonValue, type JsonValue } from "@earendil-works/chord";
import { awaitWithContext, BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { type ModelThinkingLevel, type Models, Type } from "@earendil-works/pi-ai";
import {
  type ConversationHandle,
  createRegistry,
  defineDoc,
  defineExtension,
  defineTool,
  CompactionTask,
  GenerationTask,
  Harness,
  hook,
  ROOT_CONVERSATION_ID,
  type SettledSubmissionRecord,
  type Submission,
  type UsageState,
} from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import type { ContractRouterClient } from "@orpc/contract";
import {
  type Failure,
  type Intent,
  type JobContent,
  jsonTextSchema,
  type Run,
  type runnerHttpContract,
} from "contract";
import {
  Duration,
  Effect,
  Exit,
  type FileSystem,
  Latch,
  Number,
  type PlatformError,
  Record,
  Scope,
} from "effect";
import ensureError from "ensure-error";
import { compileSchema } from "json-schema-library";
import * as z from "zod/mini";

import {
  appendUnderRequest,
  closingRequest,
  failureMessage,
  JobCancelled,
  JobFailed,
  JobPaused,
  JobRecovered,
  JobResumed,
  JobStarted,
  JobSucceeded,
  openingRequest,
  RUNNER_VERSION,
  submitClosing,
} from "./journal.ts";
import type { JobEnvironments } from "./sandbox.ts";

// Runs a job's agent with pi-durable: one Harness and SQLite journal per job, so purging a job
// deletes its file (pi-durable cannot delete a conversation) and the journal holds exactly the
// job's entries in order. The agent has pi-durable's coding tools on the job's Workspace, in
// the job's sandbox (ADR 0027), and `submit_output`, which checks the output against the Output
// contract, as the hub checks the contract itself (ADR 0025), and ends the run once an output is
// accepted (ADR 0009).

/** The model every job on this Runner runs on, as Pi names it (ADR 0013). */
export interface AgentModel {
  provider: string;
  modelId: string;
  thinkingLevel: ModelThinkingLevel;
}

/** The Runner's HTTP calls to the hub; each client's link sends the key its calls need. */
export type HubHttpClient = ContractRouterClient<typeof runnerHttpContract>;

/** How the Runner runs every job's agent: where jobs keep their files, in which sandbox, and on which model. */
export interface AgentSettings {
  /** The directory holding one directory per job: its journal, its Workspace and its temp directory. */
  jobsDir: string;
  /** The Runner's file system, where it looks for jobs' journals. */
  fileSystem: FileSystem.FileSystem;
  /** The Runner's HTTP calls, made with its own key, which jobs' Input files are downloaded with. */
  hubHttp: HubHttpClient;
  environments: JobEnvironments;
  models: Models;
  model: AgentModel;
}

/**
 * What a resumed job whose work pi had settled is told, as the user's message, so that it carries on;
 * the transcript before it is unchanged.
 */
const CONTINUATION_NOTE = "Carry on with the job from where you stopped.";

/**
 * What a job that gives no input, only its own files, is told as its first message in place
 * of an input: the model's API needs one. A fixed text, so that it is the same each time the run
 * opens and nothing about the job moves into the instructions every job of the Batch shares. It
 * names no files: the Runner receives the Batch's and the job's in one list, so a list could not
 * say which are the job's, and the agent finds them all in `inputs`. "Using" rather than "on" each
 * file, as some may be the Batch's, for reference.
 */
const FILES_ONLY_NOTE = "Follow your instructions, using your files in the `inputs` folder.";

/**
 * The output `submit_output` accepted first, committed in its own transaction before the call's
 * result, so a restart keeps it even when the call itself was cut off; absent until one is accepted,
 * since `null` is itself an output.
 */
const AcceptedOutput = defineDoc<{ output?: JsonValue }>({
  kind: "agent-batch.output",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  initial: () => ({}),
  // Written once, so there is no replay of changes to bound.
  checkpointWhen: () => false,
});

/**
 * Where a job's files are on the Runner: its journal, its Workspace with the Input files, its temp
 * directory, and the Input files while they download. Absolute, since the agent sees them at these
 * paths.
 */
export function jobPaths(jobsDir: string, jobId: string) {
  const dir = path.resolve(jobsDir, jobId);
  return {
    dir,
    journal: path.join(dir, "journal.sqlite"),
    workspace: path.join(dir, "workspace"),
    temp: path.join(dir, "tmp"),
    downloads: path.join(dir, "downloads"),
  };
}

/**
 * Deletes everything of the job `jobId` on this Runner: its journal, its Workspace, its temp
 * directory and any download. Nothing there and nothing to delete are the same.
 */
export function deleteJobFiles(
  settings: AgentSettings,
  jobId: string,
): Effect.Effect<void, PlatformError.PlatformError> {
  return settings.fileSystem.remove(jobPaths(settings.jobsDir, jobId).dir, {
    recursive: true,
    force: true,
  });
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
 * What the agent is told about the job, beside its input: the Runner's own text, the same for every
 * job, then the instructions of the job's Batch's agent, if any, which every job of the Batch shares.
 * Nothing that names the job, such as its Workspace's path, comes before its input, so every job of a
 * Batch is told the same instructions, which a model provider can cache with the tools before them. The
 * sandbox's `/tmp` is the job's own but held in memory, which counts against the job's memory limit.
 */
function instructions(job: JobContent) {
  return job.instructions === null
    ? RUNNER_INSTRUCTIONS
    : `${RUNNER_INSTRUCTIONS}\n\n${job.instructions}`;
}

/**
 * What the Runner tells every job's agent about running unattended. The Workspace is the current
 * directory, where commands run and relative paths resolve, rather than its path, which names the job.
 */
const RUNNER_INSTRUCTIONS = `You are running a job unattended; nobody will answer questions. Your Workspace, where your files are and your commands run, is the current directory; the job's Input files, if any, are in its \`inputs\` folder. For temporary files, use the directory in $TMPDIR, not /tmp.
When the work is complete, call submit_output once with the complete output: as \`value\`, or, for a large output or one a script produced, as the \`path\` of a JSON file in the Workspace. The output is checked against the job's Output contract; if it is rejected, correct it and submit again. A reply in prose does not complete the job.`;

function submitOutputTool(schema: Record<string, unknown>) {
  const contract = compileSchema(schema);
  return defineTool({
    name: "submit_output",
    description: `Submit the job's output, which ends the job. Give exactly one of \`value\`, the output itself, or \`path\`, a JSON file in the Workspace holding it. The output must match this JSON Schema:\n${JSON.stringify(schema)}`,
    parameters: Type.Object({
      value: Type.Optional(Type.Unknown({ description: "The output" })),
      path: Type.Optional(
        Type.String({ description: "A JSON file in the Workspace holding the output" }),
      ),
    }),
    execute: async (args, api, context) => {
      // Presence, not truthiness: `false`, `0` and `null` are outputs.
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
      const storable = z.safeParse(jsonTextSchema, value);
      if (!storable.success) {
        throw new Error(`The output cannot be stored:\n${z.prettifyError(storable.error)}`);
      }
      const { valid, errors } = contract.validate(value);
      if (!valid) {
        const problems = errors.map((error) => `- ${error.message}`).join("\n");
        throw new Error(`The output does not match the Output contract:\n${problems}`);
      }
      // The first accepted output is the job's, as when the model submits twice in one round.
      const first = await api.commit(async (tx) => {
        const accepted = await tx.doc(AcceptedOutput, api.conversationId);
        if ("output" in accepted) return false;
        accepted.output = value;
        return true;
      }, context);
      const text = first
        ? "Output accepted."
        : "An output was already accepted; this one is ignored.";
      return { output: [{ type: "text", text }], control: { terminate: true } };
    },
  });
}

/**
 * Opens `job`'s journal and sandbox in `<jobsDir>/<jobId>`, and gives the journal to
 * `journalOpened` before anything is written to it. The journal stays open until the caller's scope
 * closes, and the sandbox until its agent stops. Records in the journal that the job started,
 * the first time, or that it was recovered: a journal that already holds the job, unless
 * `openedBefore` says this process opened it, is one the Runner left before it restarted, since the
 * hub places a started job on no other Runner, and not here again while it runs. A job whose
 * run has ended is not recovered.
 *
 * Gives back an Effect that runs the job's agent until it submits an accepted output or ends
 * without one, and records the job's end in the journal. Interrupting it, as a Runner that stops, a
 * run the hub drops, a pause and a cancel do, aborts none of the job's work: the caller's
 * scope then closes the journal and then the sandbox, which cuts the work off and leaves it pending
 * in the journal, so the job carries on from it when it is opened again, unless recording a pause or
 * cancel settles it (`recordStop`).
 */
export function openJob(
  jobId: string,
  job: JobContent,
  options: AgentSettings &
    Pick<Run, "runTimeoutSeconds" | "modelBudgetUsd"> & {
      runId: string;
      openedBefore: boolean;
      journalOpened: (journal: Harness) => void;
    },
): Effect.Effect<Effect.Effect<void>, never, Scope.Scope> {
  const context = BACKGROUND_CONTEXT;
  const paths = jobPaths(options.jobsDir, jobId);
  return Effect.gen(function* () {
    // The conversation and the journal are known only once the Harness is open; the hooks below
    // need them.
    let conversation: ConversationHandle | undefined;
    let journal: Harness | undefined;
    // Opened once the job's model cost has reached its budget, which `runAgent` stops it for.
    // The cost is pi's floating-point sum, so a cost exactly at the budget can fall either way.
    const overBudget = Latch.makeUnsafe(false);
    // Each of the job's model calls is admitted only while its cost is below the budget, once the
    // last call's usage is recorded: a call admitted below it runs to its end and can take the cost
    // past it, as providers' budget limits allow, and none follows.
    const budgetSpent = async () =>
      journal !== undefined && spent(await journal.usage(context)) >= options.modelBudgetUsd;
    const registry = createRegistry();
    registry.install(CodingTools);
    registry.install(
      defineExtension({
        name: "agent-batch",
        tools: [submitOutputTool(job.outputContract.schema)],
        hooks: [
          // An accepted output ends the run only when every call of its round asks to; the job
          // ends once the round's other calls are done, without another model turn. The output is
          // read from the journal, so a round a restarted Runner finishes ends the run too.
          hook(GenerationTask, {
            afterTools: async (_assistant, _results, api) => {
              const accepted = await api.snapshot(AcceptedOutput, api.conversationId, context);
              if (!accepted || !("output" in accepted)) return;
              // Not awaited: the abort waits for this task, which is waiting for this hook. It
              // fails only when the journal closes first, as when the Runner stops, or when the
              // abort cannot be committed; the job's outcome is read from the journal either way.
              conversation?.abort(context).catch(() => {});
            },
            // A request past the budget waits, so it is never made, until the job's work is aborted.
            beforeRequest: async (_request, _api, requestContext) => {
              if (!(await budgetSpent())) return;
              overBudget.openUnsafe();
              await awaitWithContext(new Promise<never>(() => {}), requestContext);
            },
          }),
          // Compaction summarizes with model calls of its own, outside generation's requests, so one
          // past the budget is declined. A blocking summary already admitted runs to its end, as a
          // request does; a background one is cut off when the job stops, and its usage is lost, as
          // pi records a summary's usage only once it completes, where a cut-off request keeps its
          // last saved partial's.
          hook(CompactionTask, {
            beforeCompact: async () => {
              if (!(await budgetSpent())) return undefined;
              overBudget.openUnsafe();
              return { decline: true };
            },
          }),
        ],
      }),
    );
    // A job's first run downloaded its Input files into the Workspace before this; a later one
    // keeps the agent's edits to them.
    yield* Effect.promise(() => mkdir(paths.workspace, { recursive: true }));
    // The sandbox has a scope of its own, which `runAgent` closes once the agent has stopped, while
    // the journal stays open for reporting. A Runner that stops closes the journal first and the
    // sandbox after it, so no tool call is left on a stopped daemon.
    const sandbox = yield* Scope.fork(yield* Effect.scope);
    const { env } = yield* Effect.acquireRelease(
      Effect.sync(() => options.environments.open(jobId, paths)),
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
            // Partial answers and command output are saved once a second rather than every 100 ms: the
            // saves run on the thread all jobs share (with 100 jobs streaming answers for about 10.6 s,
            // they took 3.3 s of CPU at 100 ms, 1.5 s at one second), and only a restart reads them,
            // losing at most a second of the output of a command it cut off.
            settings: { progress: { partialIntervalMs: 1_000, outputIntervalMs: 1_000 } },
          },
          context,
        ),
      ),
      (opened) => Effect.promise(() => opened.close(context)),
    );
    journal = harness;
    options.journalOpened(harness);
    const opening = () => ({
      timestamp: Date.now(),
      runId: options.runId,
      runnerVersion: RUNNER_VERSION,
    });
    let created = false;
    const root = yield* Effect.promise(() =>
      harness.root(context, {
        agent: {
          model: { provider: options.model.provider, modelId: options.model.modelId },
          thinkingLevel: options.model.thinkingLevel,
          instructions: instructions(job),
          cwd: paths.workspace,
        },
        // The commit that creates the job's conversation, so the start is recorded exactly once.
        init: async (tx, conversationId) => {
          created = true;
          await appendUnderRequest(tx, conversationId, openingRequest(options.runId), () =>
            tx.appendEntry(JobStarted, conversationId, { data: opening() }),
          );
        },
      }),
    );
    conversation = root;
    // A journal that already holds the job is opened by the same run, which the Runner's restart
    // left, or by a new run that resumes the job. A run that has closed is not recovered,
    // nor run again, also when its pause or cancel was recorded before it opened: the stop settled
    // its work, and only a new run carries the job on. Whether it is a resume, and whether it has
    // closed.
    const closed =
      !created &&
      (yield* Effect.promise(() =>
        root.commit(async (tx) => {
          // Reads first: pi forbids reading the tables after a commit's first write.
          const opened = await tx.submissionByRequest(root.id, openingRequest(options.runId));
          if (await tx.submissionByRequest(root.id, closingRequest(options.runId))) {
            return true;
          }
          if (!opened) {
            await appendUnderRequest(tx, root.id, openingRequest(options.runId), () =>
              tx.appendEntry(JobResumed, root.id, { data: opening() }),
            );
          } else if (!options.openedBefore) {
            await tx.appendEntry(JobRecovered, root.id, { data: opening() });
          }
          return false;
        }, context),
      ));
    // pi's plain abort leaves background work, such as a compaction, running, so this abort includes
    // it and waits until all of it has stopped: nothing then adds to the journal after the job's end
    // or runs on in its sandbox.
    const stopWork = Effect.promise(() => root.abort(context, { background: true }));
    /**
     * The input this run answers, the same each time the run opens, as when a restarted Runner
     * opens the journal again: the continuation note it submitted, if any; else an input still
     * waiting for its answer, which it carries on, as one a failed run left; else the job's own
     * input, or `FILES_ONLY_NOTE` for a job that gives none, which it submits if the journal has
     * none and which is its own when it entered the conversation after the run's opening; else, on
     * a resume, a short note asking the agent to carry on, as a pi user types the next message
     * after Esc, since the job's pause or cancel or pi's failure settled its work. Each is
     * submitted once under its request ID. Work a Runner's restart left pending carries on from the
     * input it was answering: the cut-off model call is made again, and a cut-off tool call gets an
     * `interrupted` result.
     */
    const submitInput = async (): Promise<Submission> => {
      const jobInputRequest = `job:${jobId}`;
      const noteRequest = `job:${jobId}:resume:${options.runId}`;
      const submit = (content: string, requestId: string) =>
        root.submit({ type: "input", content, requestId }, context);
      const { note, jobInput, opened } = await root.commit(
        async (tx) => ({
          note: await tx.submissionByRequest(root.id, noteRequest),
          jobInput: await tx.submissionByRequest(root.id, jobInputRequest),
          opened: await tx.submissionByRequest(root.id, openingRequest(options.runId)),
        }),
        context,
      );
      if (note) return submit(CONTINUATION_NOTE, noteRequest);
      const { submissions } = await harness.inspect(context);
      const waiting = submissions.find(
        (submission) => submission.type === "input" && submission.conversationId === root.id,
      );
      const carried = waiting && (await harness.submission(waiting.id, context));
      if (carried) return carried;
      const jobInputIsOwn =
        !jobInput ||
        (jobInput.entry !== undefined &&
          opened?.entry !== undefined &&
          jobInput.entry > opened.entry);
      return jobInputIsOwn
        ? submit(job.input ?? FILES_ONLY_NOTE, jobInputRequest)
        : submit(CONTINUATION_NOTE, noteRequest);
    };
    const runAgent = Effect.gen(function* () {
      if (closed) return;
      const answered = Effect.promise(async () => {
        const submission = await submitInput();
        return submission.wait(context);
      });
      // Once the job has finished, pi has failed or a limit is reached, all of its work stops. An
      // interrupt leaves the work pending instead. A limit stops the job as its own end does,
      // not as a pause does: the work it cut off is aborted and settled, and the job fails naming
      // the limit, unless the agent's output was accepted meanwhile. The timeout counts from here,
      // so time spent waiting for a slot or while the Runner was down counts nothing. Each gives
      // why the job failed, should its agent have no accepted output.
      const failure: Failure = yield* Effect.raceAllFirst([
        answered.pipe(Effect.map(failureOf)),
        Effect.sleep(Duration.seconds(options.runTimeoutSeconds)).pipe(
          Effect.as({ reason: "runTimedOut" } as const),
        ),
        overBudget.await.pipe(Effect.as({ reason: "modelBudgetExceeded" } as const)),
      ]).pipe(
        Effect.onExitIf(
          (exit) => !Exit.hasInterrupts(exit),
          () => stopWork,
        ),
      );
      const accepted = yield* Effect.promise(() =>
        harness.snapshot(AcceptedOutput, root.id, context),
      );
      // An agent that stopped without an accepted output fails the job at once; reminding it
      // first is planned.
      // JSON holds no `undefined`, so an output that is absent was never accepted.
      const output = accepted?.output;
      const timestamp = Date.now();
      yield* Effect.promise(() =>
        output === undefined
          ? submitClosing(root, options.runId, JobFailed, { timestamp, ...failure })
          : submitClosing(root, options.runId, JobSucceeded, { timestamp, output }),
      );
    }).pipe(
      // Unless interrupted, the sandbox goes once the agent stops, and with it every process the
      // job left running. An interrupt leaves it to the caller's scope, which closes it after the
      // journal.
      Effect.onExitIf(
        (exit) => !Exit.hasInterrupts(exit),
        (exit) => Scope.close(sandbox, exit),
      ),
    );
    return runAgent;
  });
}

/**
 * Why a job whose agent stopped without an accepted output failed, from how pi settled the input
 * its run answered: `noOutput` when the agent answered it; `modelFailed` for pi's `model_error`,
 * which a compaction the model call needed also gives, with the provider's text; and `runFaulted`
 * for any other reason, with pi's reason and text.
 */
function failureOf(settled: SettledSubmissionRecord): Failure {
  if (settled.status === "done") return { reason: "noOutput" };
  const { reason, detail } = settled;
  const text =
    detail === undefined ? undefined : typeof detail === "string" ? detail : JSON.stringify(detail);
  if (reason === "model_error") {
    return {
      reason: "modelFailed",
      ...(text === undefined ? {} : { message: failureMessage(text) }),
    };
  }
  return {
    reason: "runFaulted",
    message: failureMessage(text === undefined ? reason : `${reason}: ${text}`),
  };
}

/** A job's cost so far, in US dollars: every model's and every tool's, as the hub adds them up. */
function spent(usage: UsageState): number {
  return Number.sumAll(
    [...Record.values(usage.models), ...Record.values(usage.tools)].map((each) => each.cost.total),
  );
}

/** A stop the hub asks a Runner to record: a pause or a cancel. */
export type Stop = Extract<Intent, "pause" | "cancel">;

/**
 * Records the pause or cancel `intent` of the job `jobId`'s run `runId`, once its
 * agent has been interrupted. A pause or cancel is a decision about the job, so it settles the work
 * the interrupt left pending, as pi's Esc does (ADR 0028): opens the job's journal, where pi runs
 * nothing until asked to, aborts that work, which pi settles without running it, writing the
 * cut-off answer and tool calls into the conversation with their usage, and writes the run's
 * closing, the job's pause or its cancel, so that it is the journal's last entry. A job whose
 * accepted output the interrupted agent had not yet recorded as its end succeeded, since completion
 * ends a run (ADR 0009); a run that has already closed keeps its closing. Gives back the
 * journal, open until the caller's scope closes, or nothing when the job has no journal on this
 * Runner: a job its Runner never started has nothing to record.
 */
export function recordStop(
  jobId: string,
  runId: string,
  intent: Stop,
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
    // Read once the work is settled, so nothing runs to change it.
    const output = (yield* Effect.promise(() => journal.snapshot(AcceptedOutput, root.id, context)))
      ?.output;
    const timestamp = Date.now();
    yield* Effect.promise(() => {
      if (output !== undefined) {
        return submitClosing(root, runId, JobSucceeded, { timestamp, output });
      }
      return intent === "cancel"
        ? submitClosing(root, runId, JobCancelled, { timestamp })
        : submitClosing(root, runId, JobPaused, { timestamp });
    });
    return journal;
  });
}
