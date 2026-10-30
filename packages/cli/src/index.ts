import { openAsBlob } from "node:fs";
import { readFile } from "node:fs/promises";
import { validateHeaderValue } from "node:http";
import { text } from "node:stream/consumers";
import { parseArgs } from "node:util";

import { createORPCClient, ORPCError } from "@orpc/client";
import {
  type AnyProcedureContract,
  type ContractRouterClient,
  ProcedureContract,
} from "@orpc/contract";
import { getOpenAPIMeta } from "@orpc/openapi";
import { getDotPath } from "@standard-schema/utils";
import {
  agentSchema,
  apiContract,
  type Batch,
  batchCreationSchema,
  createProblemDetailsLink,
  FINAL_JOB_STATUSES,
  getAccess,
  inputFileSchema,
  type Job,
  jobRefSchema,
  type JobSubmission,
  jobSubmissionSchema,
  MAX_JOBS_PER_ADD,
  problemOf,
} from "contract";
import {
  Array,
  Config,
  Duration,
  Effect,
  FileSystem,
  Option,
  Path,
  Redacted,
  Runtime,
  Schedule,
  Schema,
  SchemaIssue,
} from "effect";
import ensureError from "ensure-error";
import isNetworkError from "is-network-error";
import { createCli, os } from "trpc-cli";
import * as z from "zod/mini";

import { runToEnd, stackCommands, savedHub } from "./stack.ts";

// The `agent-batch` command: one command per call of the hub's public API that an API key can
// make, derived from the contract, so the commands cannot drift from the API, plus `jobs submit`,
// `jobs wait` and `batches submit`. Each prints the API's JSON on stdout; an error from the hub prints its Problem
// Details on stderr and exits non-zero.

type ApiClient = ContractRouterClient<typeof apiContract>;

/**
 * Whether a value can go in an HTTP header as fetch sends it: fetch normalizes it through its own
 * `Headers`, which trims tabs, spaces and line breaks from its ends, and undici then refuses the
 * bytes Node's `validateHeaderValue` refuses. Either one quotes the whole value in its error, so a
 * key that fails gets this message instead.
 */
const isHeaderValue = Schema.makeFilter<string>((value) => {
  try {
    validateHeaderValue("x-api-key", new Headers({ "x-api-key": value }).get("x-api-key") ?? "");
    return true;
  } catch {
    return "Not a value an HTTP header can carry";
  }
});

/** Where the hub is and the API key to call it with, from the environment. */
const settingsFromEnv = Config.all({
  url: Config.URL("AGENT_BATCH_URL"),
  apiKey: Config.schema(Schema.Redacted(Schema.String.check(isHeaderValue)), "AGENT_BATCH_API_KEY"),
});

/**
 * Where the hub is and the API key to call it with, read when a command runs, not for `--help`:
 * from the environment, or, when neither is set there, the hub `agent-batch stack` runs and the key it
 * made, together, so a key never goes to a hub it was not made for.
 */
const settings = Effect.gen(function* () {
  const url = yield* Config.option(Config.String("AGENT_BATCH_URL"));
  const apiKey = yield* Config.option(Config.String("AGENT_BATCH_API_KEY"));
  if (Option.isNone(url) && Option.isNone(apiKey)) {
    const saved = yield* savedHub();
    if (saved !== undefined) {
      yield* Effect.sync(() =>
        process.stderr.write(
          `Calling the hub agent-batch stack runs at ${saved.url.origin}, as AGENT_BATCH_URL and AGENT_BATCH_API_KEY are unset\n`,
        ),
      );
      return { url: saved.url, apiKey: Redacted.make(saved.apiKey) };
    }
  }
  return yield* settingsFromEnv;
});

/** The link to the hub, which calls any of its procedures by path. */
const hubLink = Effect.map(settings, ({ url, apiKey }) =>
  createProblemDetailsLink(apiContract, {
    origin: url.origin,
    // The hub's path, for a hub served under one. An HTTP URL's path always starts with `/`, which
    // oRPC's type spells out.
    url: `/${url.pathname.slice(1)}`,
    headers: { "x-api-key": Redacted.value(apiKey) },
  }),
);

const apiClient = Effect.map(hubLink, (link): ApiClient => createORPCClient(link));

/**
 * Whether `error` may leave the hub able to answer differently next time: no answer at all, which
 * fetch reports as a network error (not any `TypeError`, which an invalid header also is), or a
 * server error, read from the Problem Details the contract's link keeps on every error the hub's
 * side answers, a proxy's page included. Any 5xx counts, the hub not being set up included: it
 * shares its 503 with a proxy's (ADR 0023), and a retry only delays its message. An error oRPC
 * raises itself has no Problem Details and is an answer.
 */
function isTransient(error: unknown): boolean {
  if (!(error instanceof ORPCError)) return isNetworkError(error);
  return (problemOf(error)?.status ?? 0) >= 500;
}

/** Retries a call that failed transiently, at most twice, waiting longer each time, as Stripe's Go client does. */
const retryTransient = {
  schedule: Schedule.exponential("500 millis").pipe(Schedule.jittered),
  times: 2,
  while: isTransient,
};

/**
 * The caller's custom ID, or one made for this run of the command and printed on stderr, through
 * `notice`, before anything is sent, so that a command stopped or killed meanwhile has told it.
 */
function customIdOf(given: string | undefined, notice: (id: string) => string) {
  if (given !== undefined) return Effect.succeed(given);
  const id = crypto.randomUUID();
  return Effect.as(
    Effect.sync(() => process.stderr.write(`${notice(id)}\n`)),
    id,
  );
}

/**
 * A call of the hub's API, its failure kept as it is for `isTransient`: ensure-error would replace
 * the Problem Details an error from the hub keeps as its cause with an `Error`.
 */
function hubCall<A>(call: (signal: AbortSignal) => Promise<A>) {
  return Effect.tryPromise({ try: call, catch: (error) => error });
}

/**
 * What `error` finds wrong with the settings, on one line, as the hub and the Runner report their
 * own: each setting it names, then the problem. Each of the three keeps a copy, as the contract
 * they share has no Effect (ADR 0022).
 */
function settingsProblems(error: Config.ConfigError): string {
  if (!Schema.isSchemaError(error.cause)) return error.message;
  const { issues } = SchemaIssue.makeFormatterStandardSchemaV1()(error.cause.issue);
  return issues
    .map((issue) => {
      const setting = getDotPath(issue);
      return setting === null ? issue.message : `${setting}: ${issue.message}`;
    })
    .join("; ");
}

/**
 * Why fetch could not reach the hub. fetch reports every such failure as "fetch failed", with the
 * reason as its cause, such as `connect ECONNREFUSED 127.0.0.1:8787`; when it tried both IPv4 and
 * IPv6, the cause is an `AggregateError` with one reason for each and no message of its own.
 */
function connectionFailure(cause: unknown): string {
  const reasons: unknown[] = cause instanceof AggregateError ? cause.errors : [cause];
  return reasons.map((reason) => ensureError(reason).message).join("; ");
}

/**
 * A command's failure as an `Error` whose message the command prints: trpc-cli prints only an
 * `Error`'s, and reports some by their cause. An error the hub answered prints its Problem Details,
 * which the contract's link keeps as its cause; missing settings say which to set.
 */
function commandError(error: unknown): Error {
  if (error instanceof ORPCError) {
    // An error oRPC raises itself has no Problem Details, so it prints its message.
    const problem = problemOf(error);
    return new Error(problem ? JSON.stringify(problem) : error.message, { cause: error });
  }
  if (error instanceof Config.ConfigError) {
    return new Error(`agent-batch is not configured: ${settingsProblems(error)}`, {
      cause: error,
    });
  }
  if (isNetworkError(error) && error.cause !== undefined) {
    return new Error(`${error.message}: ${connectionFailure(error.cause)}`, { cause: error });
  }
  return ensureError(error);
}

/** Runs a command's work, its failure printed as `commandError` gives it. */
function runCommand(work: Effect.Effect<void, unknown>) {
  return Effect.runPromise(work.pipe(Effect.mapError(commandError)));
}

/** Prints `value` on stdout as one line of JSON, as a step of a command. */
function printStep(value: unknown) {
  return Effect.sync(() => process.stdout.write(`${JSON.stringify(value)}\n`));
}

/**
 * `argv` with `--input <file>` read in as `--json`: the command's whole input from a file, or from
 * stdin for `-`, as `gh api --input` reads it. Linux holds at most 128 KiB in one argument, less
 * than a job's Input files often are, so a whole input cannot go through the shell, and trpc-cli
 * reads `--json` only inline (mmkal/trpc-cli#229). Node's parser finds the option, as
 * `--input=<file>` too, and not after `--`.
 */
export async function readInput(argv: readonly string[]): Promise<string[]> {
  const { tokens } = parseArgs({
    args: [...argv],
    options: { input: { type: "string" } },
    strict: false,
    allowPositionals: true,
    tokens: true,
  });
  const option = tokens.find((token) => token.kind === "option" && token.name === "input");
  if (option?.kind !== "option") return [...argv];
  if (option.value === undefined) throw new Error("--input needs a file, or - for stdin");
  const json =
    option.value === "-" ? await text(process.stdin) : await readFile(option.value, "utf8");
  // Commander quotes an option's whole value in its errors, so input that is not JSON is refused
  // here, by where it came from, rather than printed back.
  try {
    JSON.parse(json);
  } catch (error) {
    throw new Error(
      `${option.value === "-" ? "stdin" : option.value}: ${ensureError(error).message}`,
      {
        cause: error,
      },
    );
  }
  return argv.toSpliced(option.index, option.inlineValue ? 1 : 2, "--json", json);
}

/**
 * The command for the procedure at `path`, which calls it with its input and prints the answer. Its
 * help is the procedure's description in the contract, as the OpenAPI document gives it.
 */
function command(procedure: AnyProcedureContract, path: readonly string[]) {
  // Only this module reads oRPC's internals: a procedure's input schema.
  const [input = z.object({})] = procedure["~orpc"].inputSchemas ?? [];
  return os
    .meta({ description: getOpenAPIMeta(procedure)?.description })
    .input(input)
    .handler(({ input: given }) =>
      runCommand(
        Effect.gen(function* () {
          const link = yield* hubLink;
          yield* printStep(
            yield* hubCall((signal) => link.call([...path], given, { context: {}, signal })),
          );
        }),
      ),
    );
}

/** Commands by name, nested as the contract nests its procedures. */
interface Commands {
  [name: string]: ReturnType<typeof command> | Commands;
}

/** The commands for the procedures in `router` that an API key can call. */
function commandsOf(router: object, path: readonly string[] = []): Commands {
  const commands: Commands = {};
  for (const [name, value] of Object.entries(router)) {
    if (typeof value !== "object" || value === null) continue;
    const at = [...path, name];
    if (value instanceof ProcedureContract) {
      // The command line authenticates with an API key only, so a call only a member makes has no
      // command.
      if (getAccess(value)?.key) commands[name] = command(value, at);
    } else {
      const nested = commandsOf(value, at);
      if (Object.keys(nested).length > 0) commands[name] = nested;
    }
  }
  return commands;
}

/**
 * An Input file as the commands take it in JSON, which holds no bytes: its path, and its text as
 * `content`, which the command sends as a UTF-8 file. `batches submit` sends files from disk.
 */
const textFileSchema = z.object({
  path: inputFileSchema.shape.path,
  content: z.string().check(z.describe("The file's text, sent as UTF-8")),
});

/** Input files as the commands take them in JSON, none by default, as the API's. */
function textFilesSchema(description: string) {
  return z._default(z.array(textFileSchema).check(z.describe(description)), []);
}

/** An agent as the commands take it in JSON: its files as text. */
const textAgentSchema = z.extend(agentSchema, {
  files: textFilesSchema(
    "Files every job's agent finds in its Workspace's `inputs/` folder, beside the job's own: each a `path`, such as `notes/q3.md`, and its text as `content`; none by default.",
  ),
});

/** A job as the commands take it in JSON: its files as text. */
const textJobSchema = z.extend(jobSubmissionSchema, {
  files: textFilesSchema(
    "Files the job's agent finds in its Workspace's `inputs/` folder: each a `path`, such as `notes/q3.md`, and its text as `content`; none by default. A path that collides with one of the agent's files is refused.",
  ),
});

/** `value` with its `files` as the API takes them: each text a UTF-8 file. */
function withFiles<T extends { files: readonly z.infer<typeof textFileSchema>[] }>(value: T) {
  return {
    ...value,
    files: value.files.map(({ path, content }) => ({
      path,
      content: new File([content], path.split("/").at(-1) ?? path, {
        type: "text/plain; charset=utf-8",
      }),
    })),
  };
}

/**
 * `jobs submit`: one job, as a Batch of one without a failure breaker, which means nothing for one
 * job and would leave the Batch paused by its failure. The command creates the Batch and adds the
 * job, each call retried on a transient failure under one custom ID, so a retry never creates a
 * second Batch or job: the caller's, or one made once for this run of the command, which is printed
 * on stderr before anything is sent. The custom ID names both the Batch and its job.
 */
function submitCommand() {
  // extend may replace customId because the submission has no object-level checks.
  return os
    .meta({
      description:
        "Submits one job and prints it: creates a Batch of one for it, running `agent`, without a failure breaker, and adds the job. Retrying with the same `customId` returns the Batch and job already created, so a command that cannot tell whether the job was accepted can be run again to find out.",
      // The command takes its input as JSON only: a flag per field would include the job's `--input`,
      // which `readInput` reads as the file holding the whole input before trpc-cli sees it.
      jsonInput: "always",
      // `--input` is read before trpc-cli parses the command line, so its help shows it only here.
      examples: [
        "agent-batch jobs submit --input job.json",
        "agent-batch jobs submit --input - < job.json",
      ],
    })
    .input(
      z.extend(textJobSchema, {
        customId: z.optional(
          jobSubmissionSchema.shape.customId.check(
            z.describe(
              "Your own ID for the job and its Batch of one; one is made for you when left out. Submitting again with the same `customId` and the same job returns the job already accepted, and with a different job is refused.",
            ),
          ),
        ),
        agent: textAgentSchema,
      }),
    )
    .handler(({ input }) =>
      runCommand(
        Effect.gen(function* () {
          const client = yield* apiClient;
          const { agent, ...given } = input;
          const id = yield* customIdOf(
            input.customId,
            (made) =>
              `Submitting with custom ID ${made}: if this command cannot tell whether the job was accepted, submit it again with this ID, as customId in its JSON, to find out.`,
          );
          const batch = yield* hubCall((signal) =>
            client.batches.create(
              { customId: id, agent: withFiles(agent), breaker: false },
              { signal },
            ),
          ).pipe(Effect.retry(retryTransient));
          const added = yield* hubCall((signal) =>
            client.batches.addJobs(
              { id: batch.id, jobs: [{ ...withFiles(given), customId: id }] },
              { signal },
            ),
          ).pipe(Effect.retry(retryTransient));
          yield* printStep(added.jobs[0]?.job);
        }),
      ),
    );
}

/**
 * `batches create`: the contract's call, retried on a transient failure under one custom ID, so a
 * retry never creates a second Batch: the caller's, or one made once for this run of the command,
 * which is printed on stderr before anything is sent.
 */
function createBatchCommand() {
  return (
    os
      .meta({
        description: getOpenAPIMeta(apiContract.batches.create)?.description,
        examples: ["agent-batch batches create --input batch.json"],
      })
      // A copy of the creation's fields, each with a flag: trpc-cli reads a schema registered with an
      // id, which Zod's JSON Schema gives as a reference, as one `--json` input (mmkal/trpc-cli#233).
      .input(z.object({ ...batchCreationSchema.shape, agent: textAgentSchema }))
      .handler(({ input }) =>
        runCommand(
          Effect.gen(function* () {
            const client = yield* apiClient;
            const customId = yield* customIdOf(
              input.customId,
              (made) =>
                `Creating with custom ID ${made}: if this command cannot tell whether the Batch was created, create it again with this ID, as customId in its JSON, to find out.`,
            );
            const created = yield* hubCall((signal) =>
              client.batches.create(
                { ...input, agent: withFiles(input.agent), customId },
                { signal },
              ),
            ).pipe(Effect.retry(retryTransient));
            yield* printStep(created);
          }),
        ),
      )
  );
}

/**
 * `batches add-jobs`: the contract's call, with each job's files given as text, as `jobs submit`
 * takes them.
 */
function addJobsCommand() {
  return os
    .meta({
      description: getOpenAPIMeta(apiContract.batches.addJobs)?.description,
      examples: ["agent-batch batches add-jobs --input jobs.json"],
    })
    .input(
      z.object({
        id: z.uuid().check(z.describe("The Batch's ID.")),
        jobs: z
          .array(textJobSchema)
          .check(
            z.minLength(1),
            z.maxLength(MAX_JOBS_PER_ADD),
            z.describe(
              `The jobs to add, 1 to ${MAX_JOBS_PER_ADD} per request; a larger Batch is added over several.`,
            ),
          ),
      }),
    )
    .handler(({ input }) =>
      runCommand(
        Effect.gen(function* () {
          const client = yield* apiClient;
          yield* printStep(
            yield* hubCall((signal) =>
              client.batches.addJobs({ id: input.id, jobs: input.jobs.map(withFiles) }, { signal }),
            ),
          );
        }),
      ),
    );
}

/**
 * How long a wait waits between reads: longer while it waits, from a second up to half a minute.
 * Jittered after the cap, so the reads of many callers stay spread: 20 to 30 s at most.
 */
const pollSchedule = Schedule.min([
  Schedule.exponential("1 second"),
  Schedule.spaced("25 seconds"),
]).pipe(Schedule.jittered);

/**
 * `jobs wait`: reads the job until its status is final, waiting longer while nothing changes, from
 * a second up to half a minute; prints it, and exits non-zero unless it succeeded, as `gh run watch
 * --exit-status` does. Stopping the command leaves the job running.
 */
function waitCommand() {
  return os
    .meta({
      description:
        "Waits for a job to end and prints it, exiting non-zero unless it succeeded. A paused job keeps it waiting, as its work is still owed, until the job is resumed and ends or is cancelled. Stopping the command leaves the job running.",
    })
    .input(jobRefSchema)
    .handler(({ input }) =>
      runCommand(
        Effect.gen(function* () {
          const client = yield* apiClient;
          const job = yield* hubCall((signal) => client.jobs.get(input, { signal })).pipe(
            // A read is retried as any call is, and an error that remains ends the wait, as AWS's
            // waiters end on one: an error that waiting does not fix, such as a hub not set up,
            // would otherwise keep the wait running without a word.
            Effect.retry(retryTransient),
            Effect.repeat({
              schedule: pollSchedule,
              until: (read) => FINAL_JOB_STATUSES.includes(read.status),
            }),
          );
          yield* printStep(job);
          if (job.status !== "succeeded")
            yield* Effect.fail(new Error(`The job ended ${job.status}`));
        }),
      ),
    );
}

/** A job `batches submit` makes from one of its paths, with the path, to name it by. */
interface PathJob {
  path: string;
  job: JobSubmission;
  /** The size of its files together, in bytes. */
  bytes: number;
}

/**
 * The file at `file`, as a `File` named `name` whose bytes are read from disk only as it is sent,
 * through Node's `openAsBlob`: a Batch's files are never all in memory at once. The API's client
 * takes a `Blob`, and Effect's file system has no disk-backed one.
 */
function lazyFile(file: string, name: string) {
  return Effect.tryPromise(async () => new File([await openAsBlob(file)], name));
}

/**
 * The jobs `paths` make, in their order: a file is one job with the file as its one Input file; a
 * folder is one job with every file in its tree, at its path in the folder, hidden files and
 * folders left out. Each job's custom ID is its file's or folder's name, and each Input file keeps
 * its name. Every problem is found before anything is sent.
 */
function jobsFromPaths(paths: readonly string[]) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const jobs = yield* Effect.forEach(
      paths,
      Effect.fnUntraced(function* (given): Effect.fn.Return<PathJob, unknown> {
        // The resolved path's name, so that `invoices/` and `.` name their folder.
        const name = path.basename(path.resolve(given));
        const info = yield* fs.stat(given);
        if (info.type === "File") {
          const content = yield* lazyFile(given, name);
          return {
            path: given,
            job: { customId: name, files: [{ path: name, content }], labels: {} },
            bytes: content.size,
          };
        }
        if (info.type !== "Directory") {
          return yield* Effect.fail(new Error(`${given} is neither a file nor a folder`));
        }
        const files: JobSubmission["files"] = [];
        // A glob's `*` leaves out hidden files and folders, and never enters a hidden folder. Sorted,
        // so a job's files are in the same order on every run and every system.
        const entries = (yield* fs.glob("**", { root: given })).toSorted();
        for (const entry of entries) {
          const names = entry.split(path.sep);
          const file = path.join(given, entry);
          const { type } = yield* fs.stat(file);
          if (type === "Directory") continue;
          if (type !== "File") {
            return yield* Effect.fail(new Error(`${file} is neither a file nor a folder`));
          }
          files.push({ path: names.join("/"), content: yield* lazyFile(file, names.at(-1) ?? "") });
        }
        if (files.length === 0) {
          return yield* Effect.fail(new Error(`${given} has no files to make a job of`));
        }
        return {
          path: given,
          job: { customId: name, files, labels: {} },
          bytes: files.reduce((sum, { content }) => sum + content.size, 0),
        };
      }),
    );
    const firstWithId = new Map<string, string>();
    for (const { path: given, job } of jobs) {
      const first = firstWithId.get(job.customId);
      if (first !== undefined) {
        return yield* Effect.fail(
          new Error(
            `${first} and ${given} would both be the job ${job.customId}: a job is named by its file's or folder's name, which must be unique`,
          ),
        );
      }
      firstWithId.set(job.customId, given);
    }
    // Checked as the hub checks them, so a bad name refuses the command before any job is added
    // rather than part of the way through.
    for (const { path: given, job } of jobs) {
      const parsed = z.safeParse(jobSubmissionSchema, job);
      if (!parsed.success) {
        return yield* Effect.fail(new Error(`${given}: ${z.prettifyError(parsed.error)}`));
      }
    }
    return jobs;
  });
}

/**
 * The most bytes of files one add sends, beside its 100 jobs: half the hub's provisional limit on a
 * request's body, leaving room for the rest of a multipart body. A job above it is sent alone.
 */
const MAX_FILE_BYTES_PER_ADD = 8 * 1024 * 1024;

/** `jobs` in adds, in their order, each of at most 100 jobs and, but for a job alone, 8 MiB of files. */
function addsOf(jobs: readonly PathJob[]) {
  const adds: PathJob[][] = [];
  let bytes = 0;
  for (const job of jobs) {
    const last = adds.at(-1);
    if (
      last === undefined ||
      last.length === MAX_JOBS_PER_ADD ||
      bytes + job.bytes > MAX_FILE_BYTES_PER_ADD
    ) {
      adds.push([job]);
      bytes = job.bytes;
    } else {
      last.push(job);
      bytes += job.bytes;
    }
  }
  return adds;
}

/** The line `batches submit --wait` prints for a job: its output, or why it failed. */
function outputLine(customId: string, job: Job) {
  if (job.status === "succeeded") return { customId, status: job.status, output: job.output };
  if (job.status === "failed") return { customId, status: job.status, failure: job.failure };
  return { customId, status: job.status };
}

/** A wait that `--wait-timeout` ended, which exits 124, as `timeout` and `openai batches wait` do. */
class WaitTimedOut extends Error {
  readonly [Runtime.errorExitCode] = 124;
}

/** How a Batch's jobs stand, on one line. */
function progressOf(batch: Batch, inCanary: boolean) {
  const { queued, running, paused, succeeded, failed, cancelled } = batch.jobCounts;
  const counts = Object.entries({ succeeded, failed, cancelled, running, paused, queued })
    .filter(([status, count]) => count > 0 || status === "succeeded" || status === "failed")
    .map(([status, count]) => `${count} ${status}`)
    .join(", ");
  const canary =
    inCanary && batch.breaker !== false
      ? `; the rest wait for its Canary of ${batch.breaker.canary} jobs`
      : "";
  const pause = batch.pausedBy === "caller" ? "; paused" : "";
  return `Batch ${batch.id}: ${counts}${canary}${pause}`;
}

/** Whether waiting for `batch` is over: no job is unfinished, or its failure breaker paused it. */
function settled(batch: Batch) {
  const { queued, running, paused } = batch.jobCounts;
  return batch.pausedBy === "breaker" || queued + running + paused === 0;
}

/** Shows how `batch` stands on stderr's line, over what it showed before. */
const showProgress = (client: ApiClient, batch: Batch) =>
  Effect.gen(function* () {
    // The Batch read has no Canary of its own, so its newest queued job tells whether the
    // Canary still holds the rest.
    const newestQueued =
      batch.breaker !== false && batch.jobCounts.queued > 0
        ? yield* hubCall((signal) =>
            client.jobs.list({ batchId: batch.id, status: ["queued"], limit: 1 }, { signal }),
          ).pipe(Effect.retry(retryTransient))
        : undefined;
    const inCanary = newestQueued?.jobs[0]?.blockedBy === "batchCanary";
    yield* Effect.sync(() => {
      process.stderr.cursorTo(0);
      process.stderr.clearLine(1);
      process.stderr.write(progressOf(batch, inCanary));
    });
  });

/**
 * `batches submit --wait`: reads the Batch `batchId` until waiting for it is over, showing how it
 * stands on stderr when that is a terminal, then prints a line for each of `jobs`, in order, and
 * fails unless every one succeeded. A caller's pause keeps it waiting, as `jobs wait` does; its
 * failure breaker's pause ends the wait, as the breaker paused it for a flaw to fix first. A timeout
 * or Ctrl-C stops the wait and leaves the Batch running.
 */
function waitForOutputs(
  client: ApiClient,
  batchId: string,
  jobs: readonly (readonly [PathJob, Job])[],
  options: { again: string; timeoutSeconds: number | undefined },
) {
  return Effect.gen(function* () {
    const onTerminal = process.stderr.isTTY;
    const again = `${options.again} --wait`;
    const waitAndRead = Effect.gen(function* () {
      const batch = yield* hubCall((signal) =>
        client.batches.get({ id: batchId }, { signal }),
      ).pipe(
        Effect.retry(retryTransient),
        Effect.tap((read) => (onTerminal ? showProgress(client, read) : Effect.void)),
        Effect.repeat({ schedule: pollSchedule, until: settled }),
        Effect.ensuring(onTerminal ? Effect.sync(() => process.stderr.write("\n")) : Effect.void),
      );
      // One read per job, a few at a time, until the API reads a Batch's outputs a page at a time.
      const read = yield* Effect.forEach(
        jobs,
        ([{ job }, added]) =>
          hubCall((signal) => client.jobs.get({ id: added.id }, { signal })).pipe(
            Effect.retry(retryTransient),
            Effect.map((readJob) => outputLine(job.customId, readJob)),
          ),
        { concurrency: 8 },
      );
      return { batch, read };
    });
    const timeoutSeconds = options.timeoutSeconds;
    // The timeout covers reading the outputs too, and interrupting aborts the hub calls in flight,
    // so the command ends when it says. The interrupt message is outside it, for Ctrl-C alone.
    const { batch, read } = yield* (
      timeoutSeconds === undefined
        ? waitAndRead
        : Effect.timeoutOrElse(waitAndRead, {
            duration: Duration.seconds(timeoutSeconds),
            orElse: () =>
              Effect.fail(
                new WaitTimedOut(
                  `Stopped waiting after ${timeoutSeconds} seconds; the Batch carries on. To wait again, ${again}.`,
                ),
              ),
          })
    ).pipe(
      Effect.onInterrupt(() =>
        Effect.sync(() =>
          process.stderr.write(`Stopped waiting; the Batch carries on. To wait again, ${again}.\n`),
        ),
      ),
    );
    yield* Effect.forEach(read, printStep);
    const unsucceeded = read.filter((line) => line.status !== "succeeded").length;
    const failure =
      batch.pausedBy === "breaker"
        ? `The Batch's failure breaker paused it, as its jobs failed. Once their failures are fixed, resume it with agent-batch batches resume --id ${batch.id}, and ${again}.`
        : batch.status === "cancelled"
          ? "The Batch was cancelled"
          : unsucceeded > 0
            ? `${unsucceeded} of ${read.length} jobs did not succeed`
            : undefined;
    if (failure !== undefined) yield* Effect.fail(new Error(failure));
  });
}

/**
 * `batches submit`: one Batch running the agent its flags give, a job for each path, added in the
 * paths' order, at most 100 at a time, so the first are its Canary. Each call is retried on a transient failure,
 * under one custom ID: the caller's, or one made for this run and printed on stderr before anything
 * is sent, so running the same command again with it returns the same Batch and adds only the jobs
 * it lacks. With `--wait`, it then follows the Batch until no job is queued, running or paused and
 * prints a line for each job.
 */
function submitBatchCommand() {
  return os
    .meta({
      description:
        "Creates a Batch running an agent given by its flags, with a job for each path: a file becomes a job with that file as its Input file, and a folder a job with every file in its tree, at its path in the folder, hidden ones left out; each job is named by its file's or folder's name. Prints the Batch, or, with --wait, waits for its jobs to end and prints one JSON line for each, with its output or its failure. Running the command again with the Batch's --custom-id adds only the jobs the Batch lacks.",
      examples: [
        'agent-batch batches submit invoices/*.pdf --instructions "Extract the supplier, number, currency and total." --output-schema invoice.schema.json --wait > outputs.jsonl',
      ],
    })
    .input(
      z.tuple([
        z
          .array(z.string())
          .check(
            z.minLength(1),
            z.meta({ title: "paths", description: "Files and folders, each one job" }),
          ),
        z.object({
          instructions: z.optional(
            agentSchema.shape.instructions.check(
              z.describe("What the agent is told for every job"),
            ),
          ),
          outputSchema: z
            .string()
            .check(z.describe("A file holding the JSON Schema each job's output must match")),
          customId: batchCreationSchema.shape.customId.check(
            z.describe(
              "Your own ID for the Batch; one is made and printed when left out. Running the command again with it returns the same Batch and adds only the jobs it lacks.",
            ),
          ),
          wait: z.optional(
            z
              .boolean()
              .check(
                z.describe(
                  "Wait until no job is queued, running or paused, then print a JSON line for each job. Exits 0 when every job succeeded, 1 otherwise or when the failure breaker paused the Batch, 124 when --wait-timeout passes. Ctrl-C stops waiting and leaves the Batch running.",
                ),
              ),
          ),
          waitTimeout: z.optional(
            z.int().check(z.positive(), z.describe("How many seconds --wait waits at most")),
          ),
        }),
      ]),
    )
    .handler(({ input: [paths, flags] }) =>
      runToEnd(
        Effect.gen(function* () {
          if (flags.waitTimeout !== undefined && !flags.wait) {
            return yield* Effect.fail(new Error("--wait-timeout needs --wait"));
          }
          const fs = yield* FileSystem.FileSystem;
          const schemaText = yield* fs.readFileString(flags.outputSchema);
          const schema = yield* Effect.try({
            try: (): unknown => JSON.parse(schemaText),
            catch: (error) =>
              new Error(`${flags.outputSchema}: ${ensureError(error).message}`, { cause: error }),
          });
          const agent = { instructions: flags.instructions, outputContract: { schema } };
          const parsedAgent = z.safeParse(agentSchema, agent);
          if (!parsedAgent.success) {
            return yield* Effect.fail(new Error(z.prettifyError(parsedAgent.error)));
          }
          const jobs = yield* jobsFromPaths(paths);
          const client = yield* apiClient;
          const customId = yield* customIdOf(
            flags.customId,
            (made) =>
              `Submitting with custom ID ${made}: if this command stops before every job is added, run this command again with --custom-id ${made} to add the rest.`,
          );
          const again = `run this command again with --custom-id ${customId}`;
          const created = yield* hubCall((signal) =>
            client.batches.create({ customId, agent: parsedAgent.data }, { signal }),
          ).pipe(Effect.retry(retryTransient));
          const adds = yield* Effect.forEach(addsOf(jobs), (chunk) =>
            hubCall((signal) =>
              client.batches.addJobs(
                { id: created.id, jobs: chunk.map(({ job }) => job) },
                { signal },
              ),
            ).pipe(Effect.retry(retryTransient)),
          );
          const added = adds.flatMap((add) => add.jobs.map(({ job }) => job));
          const submitted = Effect.gen(function* () {
            yield* printStep(adds.at(-1)?.batch ?? created);
            yield* Effect.sync(() =>
              process.stderr.write(
                `To wait for its outputs, ${again} --wait; to see how it stands, run agent-batch batches get --id ${created.id}.\n`,
              ),
            );
          });
          return yield* flags.wait
            ? waitForOutputs(client, created.id, Array.zip(jobs, added), {
                again,
                timeoutSeconds: flags.waitTimeout,
              })
            : submitted;
        }).pipe(Effect.mapError(commandError)),
      ),
    );
}

/** The `agent-batch` command line. */
export function agentBatchCli(version: string) {
  const commands = commandsOf(apiContract);
  return createCli({
    name: "agent-batch",
    version,
    // A command takes its whole input as JSON with `--json`, as well as one flag per field.
    jsonInput: "auto",
    // The commands are trpc-cli's own procedures, each calling the hub through `@orpc/client`:
    // trpc-cli runs only oRPC 1's server routers, which carry their handlers, and this package has
    // only the hub's contract, which is oRPC 2's.
    router: os.router({
      ...commands,
      batches: {
        ...commands["batches"],
        create: createBatchCommand(),
        addJobs: addJobsCommand(),
        submit: submitBatchCommand(),
      },
      jobs: { submit: submitCommand(), ...commands["jobs"], wait: waitCommand() },
      stack: stackCommands(version),
    }),
  });
}

/** What the command prints for `error`: its message, which is the hub's Problem Details for its errors. */
export function formatError(error: unknown): string {
  return ensureError(error).message;
}
