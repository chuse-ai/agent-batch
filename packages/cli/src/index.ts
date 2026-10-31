import { createHash } from "node:crypto";
import { openAsBlob } from "node:fs";
import { readFile } from "node:fs/promises";
import { validateHeaderValue } from "node:http";
import { text } from "node:stream/consumers";
import { parseArgs } from "node:util";

import { createORPCClient, ORPCError } from "@orpc/client";
import {
  type AnyProcedureContract,
  type ContractRouterClient,
  type RouterContract,
} from "@orpc/contract";
import { getOpenAPIMeta } from "@orpc/openapi";
import { walkProcedureContractsSync } from "@orpc/server";
import { set } from "@orpc/shared";
import { getDotPath } from "@standard-schema/utils";
import {
  agentSchema,
  apiContract,
  type Batch,
  batchCreationSchema,
  batchIdSchema,
  createProblemDetailsLink,
  FINAL_JOB_STATUSES,
  getAccess,
  type Job,
  jobRefSchema,
  type JobSubmission,
  jobSubmissionSchema,
  MAX_BODY_BYTES,
  MAX_FILE_BYTES,
  MAX_FILES_PER_CHECK,
  MAX_JOBS_PER_ADD,
  problemOf,
} from "contract";
import {
  Array,
  ByteSize,
  Cause,
  Config,
  Duration,
  Effect,
  Exit,
  FileSystem,
  Option,
  Path,
  Redacted,
  Result,
  Runtime,
  Schedule,
  Schema,
  SchemaIssue,
  Stream,
  Struct,
} from "effect";
import { Ndjson } from "effect/encoding";
import { sumAll } from "effect/Number";
import { CommandLine } from "effect/process";
import ensureError from "ensure-error";
import isNetworkError from "is-network-error";
import { logUpdateStderr } from "log-update";
import ms from "ms";
import { createCli, type Logger, os } from "trpc-cli";
import * as z from "zod/mini";

import { runToEnd, stackCommands, savedHub } from "./stack.ts";

// The `agent-batch` command: one command per call of the hub's public API that an API key can
// make, derived from the contract, so the commands cannot drift from the API, plus `jobs submit`,
// `jobs wait` and `batches submit`. Each prints the API's JSON on stdout; an error from the hub
// prints its Problem Details on stderr and exits non-zero.

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

/**
 * Retries a call that failed transiently at most twice, after about 500 ms and then 1 s, each wait
 * jittered by up to 20% either way. Two retries growing from 500 ms are the default of OpenAI's
 * SDKs (`DEFAULT_MAX_RETRIES` and `INITIAL_RETRY_DELAY` in openai-python's `_constants.py`).
 */
const retryTransient = {
  schedule: Schedule.exponential("500 millis").pipe(Schedule.jittered),
  times: 2,
  while: isTransient,
};

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

/**
 * The caller's ID, or a UUIDv4 made for this run of the command before anything is sent, so that
 * each retry of a call within the run reuses it, with whether it was made.
 */
function givenOrNewId(given: string | undefined) {
  return given === undefined ? { id: crypto.randomUUID(), made: true } : { id: given, made: false };
}

/**
 * `work`, which on an interrupt, such as Ctrl-C, or a failure that leaves unknown what the hub did,
 * prints `hint` on stderr when the ID the command runs under was made, not given, so the person can
 * run it again under that ID. The hub's answer refusing a call before what the ID names `exists` is
 * known, and running the command again as it is would be refused again, so it gets no hint; once it
 * exists, a refusal gets the hint too, since the person can fix what was refused and carry on under
 * the same ID rather than make a second. The made ID is shown only then, not up front, to keep what a
 * run that ends well prints short; a run killed outright never shows it, so running it again makes a
 * new one.
 */
function hintMadeIdOnStop<A, E, R>(
  work: Effect.Effect<A, E, R>,
  { made }: { made: boolean },
  hint: string,
  { exists = false } = {},
) {
  const refused = (cause: Cause.Cause<E>) =>
    !Cause.hasInterrupts(cause) &&
    Option.exists(
      Cause.findErrorOption(cause),
      (error) => error instanceof ORPCError && !isTransient(error),
    );
  return made
    ? work.pipe(
        Effect.onErrorIf(
          (cause) => exists || !refused(cause),
          () => Effect.sync(() => process.stderr.write(`${hint}\n`)),
        ),
      )
    : work;
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
function commandsOf(router: RouterContract): Commands {
  const commands: Commands = {};
  walkProcedureContractsSync(router, (procedure, path) => {
    // The commands mirror the API where that makes sense; the `blobs` calls are low-level content
    // transfer, which `batches submit` does for the caller, hashing local files and uploading the
    // missing ones, so they have no commands. The command line authenticates with an API key only,
    // so a call only a member makes has no command either.
    if (!Array.isArrayNonEmpty(path) || path[0] === "blobs" || !getAccess(procedure)?.key) return;
    set(commands, path, command(procedure, path));
  });
  return commands;
}

/**
 * `jobs submit`: one job, as a Batch of one without a failure breaker, which means nothing for one
 * job and would leave the Batch paused by its failure. The command creates the Batch and adds the
 * job, each call retried on a transient failure under one custom ID, so a retry never creates a
 * second Batch or job: the caller's, or one made once for this run of the command, which is printed
 * on stderr only if the command stops early. The custom ID is both the job's and its Batch's ID, so
 * it takes the Batch ID's syntax: a run again under it finds the same Batch, which an ID made for
 * a custom ID outside that syntax would not.
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
      z.extend(jobSubmissionSchema, {
        customId: z.optional(
          batchIdSchema.check(
            z.describe(
              "Your own ID for the job, and the ID of its Batch of one, so 1 to 100 letters, digits, `_` and `-`, not beginning with `-`; one is made for you when left out. Submitting again with the same `customId` and the same job returns the job already accepted, and with a different job is refused.",
            ),
          ),
        ),
        agent: agentSchema,
      }),
    )
    .handler(({ input }) => {
      const { agent, ...given } = input;
      const runId = givenOrNewId(input.customId);
      const { id } = runId;
      return runToEnd(
        Effect.gen(function* () {
          const client = yield* apiClient;
          const hint = `The job's custom ID is ${id}: to find out whether the job was accepted, submit it again with this ID, as customId in its JSON.`;
          const batch = yield* hintMadeIdOnStop(
            hubCall((signal) =>
              client.batches.create({ id, agent, breaker: false }, { signal }),
            ).pipe(Effect.retry(retryTransient)),
            runId,
            hint,
          );
          const added = yield* hintMadeIdOnStop(
            hubCall((signal) =>
              client.batches.addJobs(
                { id: batch.id, jobs: [{ ...given, customId: id }] },
                { signal },
              ),
            ).pipe(Effect.retry(retryTransient)),
            runId,
            hint,
            { exists: true },
          );
          yield* printStep(added.jobs[0]?.job);
        }).pipe(Effect.mapError(commandError)),
      );
    });
}

/**
 * `batches create`: the contract's call, retried on a transient failure under one ID, so a retry
 * never creates a second Batch: the caller's, or one made once for this run of the command, which
 * is printed on stderr only if the command stops early.
 */
function createBatchCommand() {
  return (
    os
      .meta({
        description: getOpenAPIMeta(apiContract.batches.create)?.description,
        examples: ["agent-batch batches create --input batch.json"],
      })
      // A copy of the creation, strict as it is, without its registered id, so each field gets a
      // flag: trpc-cli reads a schema registered with an id, which Zod's JSON Schema gives as a
      // reference, as one `--json` input (mmkal/trpc-cli#233).
      .input(z.extend(batchCreationSchema, {}))
      .handler(({ input }) => {
        const runId = givenOrNewId(input.id);
        const { id } = runId;
        return runToEnd(
          Effect.gen(function* () {
            const client = yield* apiClient;
            const created = yield* hintMadeIdOnStop(
              hubCall((signal) => client.batches.create({ ...input, id }, { signal })).pipe(
                Effect.retry(retryTransient),
              ),
              runId,
              `The Batch's ID is ${id}: to find out whether it was created, run this command again with ${CommandLine.display(["--id", id])}.`,
            );
            yield* printStep(created);
          }).pipe(Effect.mapError(commandError)),
        );
      })
  );
}

/**
 * How long a wait waits between reads: the first read is at once, then one every 10 seconds, as
 * OpenAI's CLI waits for a batch.
 */
const pollSchedule = Schedule.spaced("10 seconds");

/**
 * `jobs wait`: reads the job until its status is final, every 10 seconds; prints it, and exits non-zero unless it succeeded, as `gh run watch
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

/** A file at `diskPath` that a job of `batches submit --each` carries, at `path` in `inputs`. */
interface LocalFile {
  path: string;
  diskPath: string;
  sizeBytes: number;
}

/** The job `batches submit --each` makes of a path it was given, before its files are hashed. */
interface PathJob {
  given: string;
  customId: string;
  files: LocalFile[];
}

/**
 * The custom ID of the job `--each` makes of `given`: the path as given, normalized, with `/`
 * between its names on every system and no `/` at its end, so `./invoices/a.pdf` is the job
 * `invoices/a.pdf`, and the same command run again from the same folder names the same jobs.
 */
function customIdOfPath(path: Path.Path, given: string) {
  const normalized = path.normalize(given).split(path.sep).join("/");
  return normalized.length > 1 && normalized.endsWith("/") ? normalized.slice(0, -1) : normalized;
}

/**
 * The jobs `paths` make, in their order: a file is one job with the file as its one Input file, by
 * its name; a folder is one job with every file in its tree, at its path in the folder. Hidden
 * files and folders are left out, given or inside a folder, as a shell's `*` leaves them out; a
 * link to a folder inside a folder is refused, as the glob does not follow it. Every problem with
 * the paths is found before anything is sent.
 */
function jobsFromPaths(paths: readonly string[]) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const fileAt = Effect.fnUntraced(function* (file: string, at: string) {
      const info = yield* fs.stat(file);
      if (info.type !== "File") {
        return yield* Effect.fail(new Error(`${file} is neither a file nor a folder`));
      }
      const sizeBytes = Number(info.size);
      if (sizeBytes > MAX_FILE_BYTES) {
        return yield* Effect.fail(
          new Error(
            `${file} has more than the ${MAX_FILE_BYTES / 1024 / 1024} MiB a file may have`,
          ),
        );
      }
      return { path: at, diskPath: file, sizeBytes };
    });
    const isHidden = (given: string) => {
      const name = path.basename(given);
      return name.startsWith(".") && name !== "." && name !== "..";
    };
    const hidden = paths.filter(isHidden);
    if (hidden.length > 0) {
      yield* Effect.sync(() =>
        process.stderr.write(`Leaving out hidden files and folders: ${hidden.join(", ")}\n`),
      );
    }
    const jobs = yield* Effect.forEach(
      paths.filter((given) => !isHidden(given)),
      Effect.fnUntraced(function* (given): Effect.fn.Return<PathJob, unknown> {
        const customId = customIdOfPath(path, given);
        const info = yield* fs.stat(given);
        if (info.type !== "Directory") {
          return { given, customId, files: [yield* fileAt(given, path.basename(given))] };
        }
        // A glob's `*` leaves out hidden files and folders, and never enters a hidden folder. Sorted,
        // so a job's files are in the same order on every run and every system.
        const entries = (yield* fs.glob("**", { root: given })).toSorted();
        const files: LocalFile[] = [];
        for (const entry of entries) {
          const file = path.join(given, entry);
          if ((yield* fs.stat(file)).type === "Directory") {
            // The glob passes over a folder it cannot read, which would leave the job without its
            // files.
            yield* fs
              .access(file, { readable: true })
              .pipe(Effect.mapError(() => new Error(`${file} cannot be read`)));
            const link = yield* Effect.option(fs.readLink(file));
            if (Option.isSome(link)) {
              return yield* Effect.fail(
                new Error(`${file} is a link to a folder, which --each does not follow`),
              );
            }
            continue;
          }
          files.push(yield* fileAt(file, entry.split(path.sep).join("/")));
        }
        if (files.length === 0) {
          return yield* Effect.fail(new Error(`${given} has no files to make a job of`));
        }
        return { given, customId, files };
      }),
    );
    if (jobs.length === 0) return yield* Effect.fail(new Error("--each leaves no jobs to submit"));
    const firstWithId = new Map<string, string>();
    for (const { given, customId } of jobs) {
      const first = firstWithId.get(customId);
      if (first !== undefined) {
        return yield* Effect.fail(
          new Error(`${first} and ${given} are both the job ${customId}: give each path once`),
        );
      }
      firstWithId.set(customId, given);
    }
    return jobs;
  });
}

/** The digest of the file at `file`, as the hub names content: `sha256:` and its SHA-256 in hex. */
function digestOf(file: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const hash = createHash("sha256");
    yield* Stream.runForEach(fs.stream(file), (chunk) => Effect.sync(() => hash.update(chunk)));
    return `sha256:${hash.digest("hex")}`;
  });
}

/**
 * How many files `batches submit` hashes or uploads, and how many jobs it reads, at once. Five is a
 * conventional value, as Docker sends five layers at once, not a measured one, so it is to be tuned
 * from measurement. Each upload holds one of the hub's database connections while it writes, of
 * the ten a Node hub's pool has, so five leave the hub room for other callers.
 */
const AT_ONCE = 5;

/** A file of a job with the digest of its content. */
type HashedFile = LocalFile & { digest: string };

/**
 * The jobs `pathJobs` make, as the hub takes them, their files named by digest, each checked as the
 * hub checks it, so that a bad path refuses the command before any job is added, rather than part
 * of the way through. Answers the files too, to upload.
 */
function hashJobs(pathJobs: readonly PathJob[]) {
  return Effect.gen(function* () {
    // Every file once, across jobs, as with --each over files each job has one.
    const diskPaths = [...new Set(pathJobs.flatMap(({ files }) => files.map((f) => f.diskPath)))];
    const digests = new Map(
      yield* Effect.forEach(
        diskPaths,
        (diskPath) => Effect.map(digestOf(diskPath), (digest) => [diskPath, digest] as const),
        { concurrency: AT_ONCE },
      ),
    );
    const hashed = pathJobs.map(({ files }) =>
      files.map((file): HashedFile => ({ ...file, digest: digests.get(file.diskPath) ?? "" })),
    );
    const jobs = yield* Effect.forEach(
      Array.zip(pathJobs, hashed),
      ([{ given, customId }, files]) => {
        const parsed = z.safeParse(jobSubmissionSchema, {
          customId,
          files: files.map(Struct.pick(["path", "digest", "sizeBytes"])),
        });
        return parsed.success
          ? Effect.succeed(parsed.data)
          : Effect.fail(new Error(`${given}: ${z.prettifyError(parsed.error)}`));
      },
    );
    return { jobs, files: hashed.flat() };
  });
}

/**
 * Uploads each of `files` the hub does not hold for the Organization yet, each content once, a few
 * at a time, each upload retried as any call is. Shows how far the uploads are on stderr when a
 * person watches.
 */
function uploadMissing(client: ApiClient, files: readonly HashedFile[]) {
  return Effect.gen(function* () {
    const unique = Array.dedupeWith(files, (a, b) => a.digest === b.digest);
    const checked = yield* Effect.forEach(Array.chunksOf(unique, MAX_FILES_PER_CHECK), (chunk) =>
      hubCall((signal) =>
        client.blobs.check({ files: chunk.map(Struct.pick(["digest", "sizeBytes"])) }, { signal }),
      ).pipe(
        Effect.retry(retryTransient),
        // The check answers its files in the order given.
        Effect.map(({ files: answers }) => Array.zip(chunk, answers)),
      ),
    );
    const missing = checked.flat().flatMap(([file, answer]) => (answer.stored ? [] : [file]));
    if (missing.length === 0) return;
    const onTerminal = personWatches();
    const total = sumAll(missing.map(({ sizeBytes }) => sizeBytes));
    let done = 0;
    let doneBytes = 0;
    // Sizes in MiB to a tenth, such as `5.0 MiB`.
    const inMib: ByteSize.FormatOptions = { unit: "MiB", precision: 1, trailingZeros: true };
    const show = onTerminal
      ? Effect.sync(() =>
          logUpdateStderr(
            `Uploaded ${done} of ${missing.length} files, ${ByteSize.format(ByteSize.bytes(doneBytes), inMib)} of ${ByteSize.format(ByteSize.bytes(total), inMib)}`,
          ),
        )
      : Effect.void;
    yield* show;
    yield* Effect.forEach(
      missing,
      ({ diskPath, digest, sizeBytes }) =>
        // Every upload the hub asks for today is a `put` to this route, which the client calls by
        // the contract; the body is read from disk anew for each try, and only as it is sent.
        hubCall(async (signal) =>
          client.blobs.upload(
            {
              params: { digest },
              headers: { "content-length": sizeBytes },
              body: (await openAsBlob(diskPath)).stream(),
            },
            { signal },
          ),
        ).pipe(
          Effect.retry(retryTransient),
          Effect.andThen(
            Effect.suspend(() => {
              done += 1;
              doneBytes += sizeBytes;
              return show;
            }),
          ),
        ),
      { concurrency: AT_ONCE, discard: true },
    ).pipe(Effect.ensuring(endProgress));
  });
}

/**
 * Whether a person watches the command's progress, notices and receipts: stderr a terminal, as git
 * shows its progress (`git clone --progress`), so stdout can go to a pipe or a file while the person
 * watches. openai-cli also requires stdout a terminal (`batchHumanTerminal`); git does not.
 */
function personWatches(): boolean {
  return process.stderr.isTTY;
}

/** Token counts as a person reads them: 6.8M. */
const tokenCount = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

/** US dollars as a person reads them: $1,234.50. */
const usd = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

/** Leaves the progress `logUpdateStderr` shows on stderr in place, so what follows goes below it. */
const endProgress = Effect.sync(() => {
  if (personWatches()) logUpdateStderr.done();
});

/**
 * The jobs in the JSONL file `file`, one job's JSON per line, as `batches add-jobs` takes each,
 * blank lines left out, all read before any is sent; each is checked as the hub checks it. A line
 * may hold at most what one request to the hub carries. A problem names its job by its place among
 * the jobs, as Effect's `Ndjson` counts no lines.
 */
function jobsFromJsonl(file: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    if ((yield* fs.stat(file)).type !== "File") {
      return yield* Effect.fail(new Error(`${file} is not a file`));
    }
    const jobs: JobSubmission[] = [];
    yield* fs.stream(file).pipe(
      // Ndjson skips a leading BOM and accepts CRLF and CR line ends, as files saved on Windows
      // have them. A line holding only whitespace is not skipped but fails as invalid JSON, as
      // `ignoreEmptyLines` skips only lines with nothing in them (Effect-TS/effect#10201).
      Stream.pipeThroughChannel(
        Ndjson.decode({ ignoreEmptyLines: true, maxLineBytes: MAX_BODY_BYTES }),
      ),
      Stream.runForEach((value) => {
        const parsed = z.safeParse(jobSubmissionSchema, value);
        if (!parsed.success) {
          return Effect.fail(
            new Error(`${file}: job ${jobs.length + 1}: ${z.prettifyError(parsed.error)}`),
          );
        }
        jobs.push(parsed.data);
        return Effect.void;
      }),
      Effect.catchTag("NdjsonError", (error) => {
        const at = `${file}: job ${jobs.length + 1}`;
        return Effect.fail(
          new Error(
            error.reason === "LineTooLarge"
              ? `${at} is longer than ${MAX_BODY_BYTES / 1024 / 1024} MiB, the most one request to the hub carries`
              : error.reason === "InvalidUtf8"
                ? `${at} is not valid UTF-8`
                : `${at}: ${ensureError(error.cause).message}`,
          ),
        );
      }),
    );
    if (jobs.length === 0) return yield* Effect.fail(new Error(`${file} has no jobs`));
    return jobs;
  });
}

/** The line `batches submit --wait --output-dir` saves for a job: its output, or why it failed. */
function jobLine(customId: string, job: Job) {
  if (job.status === "succeeded") return { customId, status: job.status, output: job.output };
  if (job.status === "failed") return { customId, status: job.status, failure: job.failure };
  return { customId, status: job.status };
}
type JobLine = ReturnType<typeof jobLine>;

/** A wait that `--wait-timeout` ended, which exits 124, as `timeout` and `openai batches wait` do. */
class WaitTimedOut extends Error {
  readonly [Runtime.errorExitCode] = 124;
}

/** How a Batch's jobs stand, on one line. */
function progressOf(batch: Batch, inCanary: boolean) {
  const { queued, running, paused, succeeded, failed, cancelled } = batch.jobCounts;
  // Once no job is unfinished, the line openai-cli ends its wait with; a cancelled job is neither a
  // success nor a failure, so it is named apart, only when there is one.
  if (queued + running + paused === 0) {
    const others = cancelled > 0 ? `, ${cancelled} cancelled` : "";
    return `Completed: ${succeeded} succeeded, ${failed} failed${others}.`;
  }
  const counts = Object.entries({ succeeded, failed, cancelled, running, paused, queued })
    .filter(([status, count]) => count > 0 || status === "succeeded" || status === "failed")
    // Each running job is one agent loop in a Runner, which is what the count shows.
    .map(([status, count]) => `${count} ${status === "running" ? "agents running" : status}`)
    .join(", ");
  // Short enough that the line fits in 80 columns while a Batch of hundreds waits for its Canary.
  const canary =
    inCanary && batch.breaker !== false ? `; Canary of ${batch.breaker.canary} first` : "";
  const pause = batch.pausedBy === "caller" ? "; paused" : "";
  return `${counts}${canary}${pause}`;
}

/** Whether waiting for `batch` is over: no job is unfinished, or its failure breaker paused it. */
function settled(batch: Batch) {
  const { queued, running, paused } = batch.jobCounts;
  return batch.pausedBy === "breaker" || queued + running + paused === 0;
}

/** Shows how `batch` stands on stderr, over what it showed before. */
function showProgress(client: ApiClient, batch: Batch) {
  return Effect.gen(function* () {
    // The Batch read has no Canary of its own, so its newest queued job tells whether the
    // Canary still holds the rest.
    const newestQueued =
      batch.breaker !== false && batch.jobCounts.queued > 0
        ? yield* hubCall((signal) =>
            client.jobs.list({ batchId: batch.id, status: ["queued"], limit: 1 }, { signal }),
          ).pipe(Effect.retry(retryTransient))
        : undefined;
    const inCanary = newestQueued?.jobs[0]?.blockedBy === "batchCanary";
    yield* Effect.sync(() => logUpdateStderr(progressOf(batch, inCanary)));
  });
}

/**
 * The file `name` in the folder `dir`, the folder as the person gave it, as openai-cli names the
 * files it saves, so `./results` gives `./results/output.jsonl`.
 */
function fileIn(path: Path.Path, dir: string, name: string) {
  return `${dir.endsWith(path.sep) ? dir : `${dir}${path.sep}`}${name}`;
}

/**
 * Prepares the folder `--output-dir` names before anything is sent, as openai-cli does: makes it
 * if absent, for its owner alone, and refuses it if it holds either file already, so a finished
 * wait never overwrites one.
 */
const prepareOutputDir = Effect.fnUntraced(function* (dir: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(dir, { mode: 0o700 }).pipe(
    Effect.catchReason("PlatformError", "AlreadyExists", () => Effect.void),
    Effect.mapError(() => new Error(`Could not create ${dir}. Check its parent and permissions.`)),
  );
  if ((yield* fs.stat(dir)).type !== "Directory") {
    yield* Effect.fail(new Error(`${dir} is not a folder`));
  }
  for (const name of ["output.jsonl", "error.jsonl"]) {
    const file = fileIn(path, dir, name);
    // `lstat`, so a link counts as the file being there, even a link to nothing.
    const there = yield* fs.lstat(file).pipe(
      Effect.as(true),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(false)),
    );
    if (there) yield* Effect.fail(new Error(`${file} already exists; choose a new --output-dir`));
  }
});

/**
 * Reads the line of each of `jobs` and saves them, in order, in `dir`: those that succeeded in
 * `output.jsonl`, the rest in `error.jsonl`, leaving out a file that would hold none, as openai-cli
 * does. Where a person watches, stderr says which it saved.
 */
const saveJobLines = Effect.fnUntraced(function* (
  client: ApiClient,
  dir: string,
  jobs: readonly (readonly [string, Job])[],
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // One read per job, a few at a time, until the API reads a Batch's outputs a page at a time.
  const lines = yield* Effect.forEach(
    jobs,
    ([customId, added]) =>
      hubCall((signal) => client.jobs.get({ id: added.id }, { signal })).pipe(
        Effect.retry(retryTransient),
        Effect.map((read) => jobLine(customId, read)),
      ),
    { concurrency: AT_ONCE },
  );
  const [succeeded, rest] = Array.partition(lines, (line) =>
    line.status === "succeeded" ? Result.succeed(line) : Result.fail(line),
  );
  for (const [name, kept] of [
    ["output", succeeded],
    ["error", rest],
  ] as const) {
    const file = fileIn(path, dir, `${name}.jsonl`);
    if (kept.length > 0) {
      // `wx`, so a file made since the folder was prepared is not overwritten, and for its owner
      // alone, as openai-cli saves its files.
      yield* Stream.fromIterable<JobLine>(kept).pipe(
        Stream.pipeThroughChannel(Ndjson.encode()),
        Stream.run(fs.sink(file, { flag: "wx", mode: 0o600 })),
      );
    }
    // Only the files saved are named. openai-cli also says when a Batch returned no file, which its
    // counts do not tell; here a file is absent when none of this run's jobs ended that way, which
    // the counts above show too unless the Batch holds jobs added outside this run.
    if (kept.length > 0 && personWatches()) {
      yield* Effect.sync(() => process.stderr.write(`Saved "${file}"\n`));
    }
  }
});

/**
 * `batches submit --wait`: reads the Batch `batchId` until waiting for it is over, showing how it
 * stands on stderr when a person watches, then, with `outputDir`, saves each of `jobs`' lines
 * there, prints the Batch (its usage, on a terminal), and fails unless every job succeeded, as
 * openai-cli does. A caller's pause keeps it waiting, as `jobs wait` does; its failure breaker's
 * pause ends the wait, as the breaker paused it for a flaw to fix first. A timeout or Ctrl-C stops
 * the wait, prints no Batch and leaves the Batch running; during saving, it leaves the files
 * saving had begun.
 */
function waitAndSave(
  client: ApiClient,
  batchId: string,
  jobs: readonly (readonly [string, Job])[],
  options: {
    again: string;
    idGiven: boolean;
    timeout: Duration.Duration | undefined;
    outputDir: string | undefined;
  },
) {
  return Effect.gen(function* () {
    const onTerminal = personWatches();
    const { again, idGiven, timeout, outputDir } = options;
    // Where a person watches, in openai-cli's words; the Batch's ID only when the person gave it, as
    // an ID the command made is shown only if it stops early.
    if (onTerminal) {
      yield* Effect.sync(() =>
        process.stderr.write(
          idGiven
            ? `Batch created: "${batchId}". Waiting for completion.\n`
            : "Waiting for completion.\n",
        ),
      );
    }
    const waitThenSave = Effect.gen(function* () {
      const batch = yield* hubCall((signal) =>
        client.batches.get({ id: batchId }, { signal }),
      ).pipe(
        Effect.retry(retryTransient),
        Effect.tap((read) => (onTerminal ? showProgress(client, read) : Effect.void)),
        Effect.repeat({ schedule: pollSchedule, until: settled }),
        Effect.ensuring(endProgress),
        // A read that fails ends the wait, which says how to wait again, the ID with it; not on an
        // interrupt, whose own message says so.
        Effect.tapError(() =>
          Effect.sync(() =>
            process.stderr.write(
              `Stopped waiting; the Batch carries on. To wait again, ${again}.\n`,
            ),
          ),
        ),
      );
      // Saving's failure is kept, so the Batch is printed, its ID with it, whatever saving did,
      // as openai-cli prints it.
      const saved =
        outputDir === undefined
          ? Exit.void
          : yield* Effect.exit(saveJobLines(client, outputDir, jobs));
      return { batch, saved };
    });
    // The timeout covers saving too, as openai-cli's covers its downloads, and interrupting aborts
    // the hub calls in flight, so the command ends when it says. The interrupt message is outside
    // it, for Ctrl-C alone.
    const { batch, saved } = yield* (
      timeout === undefined
        ? waitThenSave
        : Effect.timeoutOrElse(waitThenSave, {
            duration: timeout,
            orElse: () =>
              Effect.fail(
                new WaitTimedOut(
                  `Stopped waiting after ${Duration.format(timeout)}; the Batch carries on. To wait again, ${again}.`,
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
    // On a terminal, the Batch is printed for a person, as `gh pr view` prints a curated view there
    // and every field raw in a pipe: what the lines before did not say, its usage, and its ID when
    // it was made. A pipe or a file gets the Batch's JSON.
    if (process.stdout.isTTY) {
      const { totalTokens, cost } = batch.usage;
      const used = `${tokenCount.format(totalTokens)} tokens, ${usd.format(cost.totalUsd)}`;
      yield* Effect.sync(() =>
        process.stdout.write(
          idGiven ? `Used ${used}.\n` : `Batch ${JSON.stringify(batch.id)} used ${used}.\n`,
        ),
      );
    } else {
      yield* printStep(batch);
    }
    if (Exit.isFailure(saved)) yield* Effect.failCause(saved.cause);
    const { succeeded, ...others } = batch.jobCounts;
    const unsucceeded = sumAll(Object.values(others));
    const failure =
      batch.pausedBy === "breaker"
        ? `The Batch's failure breaker paused it, as its jobs failed. Once their failures are fixed, resume it with ${CommandLine.display(["agent-batch", "batches", "resume", "--id", batch.id])}, and ${again}${outputDir === undefined ? "" : " and a new --output-dir"}.`
        : batch.status === "cancelled"
          ? "The Batch was cancelled"
          : unsucceeded > 0
            ? `${unsucceeded} of ${succeeded + unsucceeded} jobs did not succeed`
            : undefined;
    if (failure !== undefined) yield* Effect.fail(new Error(failure));
  });
}

/**
 * `batches submit`: one Batch running the agent its flags give, with the jobs of a JSONL file, as
 * `openai batches submit` takes one, or a job for each path `--each` gives, whose files it uploads
 * first, only those the hub lacks. Jobs are added in their order, at most 100 at a time, so the
 * first are its Canary. Each call is retried on a transient failure, under one Batch ID: the
 * caller's, or one made for this run and printed on stderr if the command stops early, or without
 * `--wait` in the hint for following the Batch, which needs it, so running
 * the same command again with it returns the same Batch and adds only the jobs it lacks. With `--wait`,
 * it then follows the Batch until no job is queued, running or paused, saves each job's line with
 * `--output-dir`, and prints the Batch.
 */
function submitBatchCommand() {
  return os
    .meta({
      description:
        "Creates a Batch running an agent given by its flags, with the jobs of a JSONL file, one job's JSON per line as batches add-jobs takes it, or with --each, a job for each file or folder: a file becomes a job with that file as its Input file, and a folder a job with every file in its tree, at its path in the folder, hidden ones left out; each job's custom ID is its path as given, normalized. --each uploads the files the hub does not hold yet. Prints the Batch, with --wait once its jobs have ended, or then on a terminal the tokens and cost it used, and with --output-dir saves each job's output or failure in that folder. Running the command again with the Batch's --id adds only the jobs the Batch lacks.",
      examples: [
        'agent-batch batches submit --id invoices --each invoices/*.pdf --instructions "Extract the supplier, number, currency and total." --output-schema invoice.schema.json --wait --output-dir ./results',
        "agent-batch batches submit jobs.jsonl --output-schema answer.schema.json",
      ],
    })
    .input(
      z.tuple([
        // Several files, so that `submit invoices/*.pdf` is answered with --each rather than
        // commander's "too many arguments"; a default, as trpc-cli reads a tuple's item without one
        // as required, `z.optional` included.
        z._default(
          z.array(z.string()).check(
            z.meta({
              title: "file",
              description: "A JSONL file of jobs, one job's JSON per line",
            }),
          ),
          [],
        ),
        z.object({
          each: z.optional(
            z
              .array(z.string())
              .check(
                z.minLength(1),
                z.describe("Files and folders, each made one job, instead of a JSONL file"),
              ),
          ),
          instructions: agentSchema.shape.instructions.check(
            z.describe("What the agent is told for every job"),
          ),
          outputSchema: z
            .string()
            .check(z.describe("A file holding the JSON Schema each job's output must match")),
          id: batchCreationSchema.shape.id.check(
            z.describe(
              "The Batch's ID, 1 to 100 letters, digits, _ and -, not beginning with -; one is made when left out, and printed if the command stops early or, without --wait, in the hint for following the Batch. Running the command again with it returns the same Batch and adds only the jobs it lacks.",
            ),
          ),
          wait: z.optional(
            z
              .boolean()
              .check(
                z.describe(
                  "Wait until no job is queued, running or paused, then print the Batch. Exits 0 when every job succeeded, 1 otherwise or when the failure breaker paused the Batch, 124 when --wait-timeout passes. Ctrl-C stops waiting and leaves the Batch running.",
                ),
              ),
          ),
          outputDir: z.optional(
            z
              .string()
              .check(
                z.describe(
                  "With --wait, save each job's line in this folder, made if absent: output.jsonl for those that succeeded, error.jsonl for the rest; neither may exist yet",
                ),
              ),
          ),
          // `ms` reads a duration in one unit, such as `30m`, `1.5h` or `1 hour`, and refuses what it
          // cannot read whole, Go's `1h30m` that openai-cli takes included; Effect's `Duration` takes
          // only forms like `30 minutes`.
          waitTimeout: z.optional(
            z.pipe(
              z
                .string()
                .check(
                  z.describe("How long --wait waits at most, such as 30m or 1.5h; 0 for no limit"),
                ),
              z.transform((given, context) => {
                const refuse = (message: string) => {
                  context.issues.push({ code: "custom", message, input: given });
                  return z.NEVER;
                };
                // `ms` reads a number without a unit as milliseconds, so `1800` meant as seconds
                // would silently wait 1.8 s.
                if (given.trim() !== "0" && /[\d.]$/.test(given.trim())) {
                  return refuse("A duration needs a unit, such as 30m or 1.5h");
                }
                // `ms` throws on an empty string, and answers `undefined` for text it cannot read,
                // though its types promise a number for the durations they spell out.
                const millis: number | undefined =
                  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- `ms` takes any string; its types narrow it to the forms it reads.
                  given === "" ? undefined : ms(given as ms.StringValue);
                if (millis === undefined) {
                  return refuse("Expected a duration, such as 30m or 1.5h");
                }
                if (millis < 0) return refuse("A duration must not be negative");
                return Duration.millis(millis);
              }),
            ),
          ),
        }),
      ]),
    )
    .handler(({ input: [files, flags] }) =>
      runToEnd(
        Effect.gen(function* () {
          if (!flags.wait && (flags.outputDir !== undefined || flags.waitTimeout !== undefined)) {
            return yield* Effect.fail(
              new Error("Add --wait to use --output-dir or --wait-timeout. Nothing was sent."),
            );
          }
          if (flags.each !== undefined && files.length > 0) {
            return yield* Effect.fail(
              new Error(
                "Give either a JSONL file of jobs or --each with files and folders, not both",
              ),
            );
          }
          const [file, ...more] = files;
          if (more.length > 0) {
            return yield* Effect.fail(
              new Error(
                `batches submit takes one JSONL file of jobs; to make a job of each file or folder, use ${CommandLine.display(["--each", ...files])}`,
              ),
            );
          }
          if (file === undefined && flags.each === undefined) {
            return yield* Effect.fail(
              new Error("Give a JSONL file of jobs, or files and folders with --each"),
            );
          }
          const fs = yield* FileSystem.FileSystem;
          const schemaText = yield* fs.readFileString(flags.outputSchema);
          const schema = yield* Effect.try({
            try: (): unknown => JSON.parse(schemaText),
            catch: (error) =>
              new Error(`${flags.outputSchema}: ${ensureError(error).message}`, { cause: error }),
          });
          const parsedAgent = z.safeParse(agentSchema, {
            instructions: flags.instructions,
            outputContract: { schema },
          });
          if (!parsedAgent.success) {
            return yield* Effect.fail(new Error(z.prettifyError(parsedAgent.error)));
          }
          const local =
            file === undefined
              ? yield* Effect.flatMap(jobsFromPaths(flags.each ?? []), hashJobs)
              : { jobs: yield* jobsFromJsonl(file), files: [] };
          if (flags.outputDir !== undefined) yield* prepareOutputDir(flags.outputDir);
          const client = yield* apiClient;
          const runId = givenOrNewId(flags.id);
          const { id } = runId;
          const again = `run this command again with ${CommandLine.display(["--id", id])}`;
          // Once every job is added, the hints that follow say how to run the command again, the
          // ID with them.
          const hint = `The Batch's ID is ${id}: to add the jobs it lacks, ${again}.`;
          const created = yield* hintMadeIdOnStop(
            Effect.andThen(
              uploadMissing(client, local.files),
              hubCall((signal) =>
                client.batches.create({ id, agent: parsedAgent.data }, { signal }),
              ).pipe(Effect.retry(retryTransient)),
            ),
            runId,
            hint,
          );
          const adds = yield* hintMadeIdOnStop(
            Effect.forEach(Array.chunksOf(local.jobs, MAX_JOBS_PER_ADD), (jobs) =>
              hubCall((signal) =>
                client.batches.addJobs({ id: created.id, jobs }, { signal }),
              ).pipe(Effect.retry(retryTransient)),
            ),
            runId,
            hint,
            { exists: true },
          );
          const added = adds.flatMap((add) => add.jobs.map(({ job }) => job));
          const submitted = Effect.gen(function* () {
            yield* printStep(adds.at(-1)?.batch ?? created);
            if (personWatches()) {
              yield* Effect.sync(() =>
                process.stderr.write(
                  `To wait for it and save each job's line, ${again} ${CommandLine.display(["--wait", "--output-dir", "NEW_DIR"])}; to see how it stands, run ${CommandLine.display(["agent-batch", "batches", "get", "--id", created.id])}.\n`,
                ),
              );
            }
          });
          return yield* flags.wait
            ? waitAndSave(
                client,
                created.id,
                Array.zip(
                  local.jobs.map((job) => job.customId),
                  added,
                ),
                {
                  again,
                  idGiven: !runId.made,
                  // `0` waits without limit; it is a duration until here, so that without --wait it
                  // is refused as any --wait-timeout is.
                  timeout:
                    flags.waitTimeout === undefined || Duration.isZero(flags.waitTimeout)
                      ? undefined
                      : flags.waitTimeout,
                  outputDir: flags.outputDir,
                },
              )
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
        submit: submitBatchCommand(),
      },
      jobs: { submit: submitCommand(), ...commands["jobs"], wait: waitCommand() },
      stack: stackCommands(version),
    }),
  });
}

/** Runs the command line, keeping an exit code its work or output has already set. */
export function runAgentBatchCli(version: string, argv: readonly string[], logger?: Logger) {
  return agentBatchCli(version).run({
    argv: [...argv],
    logger,
    formatError,
    prompts: false,
    // trpc-cli exits as soon as a command ends, cutting off what a pipe has not yet taken of its
    // output (mmkal/trpc-cli#228). Setting the exit code instead, through trpc-cli's documented
    // override, lets Node exit once stdout and stderr have drained, as its docs for
    // `process.exit()` advise; trpc-cli then throws `FailedToExitError`. A write that has already
    // failed keeps its exit code.
    process: {
      exit: (code) => {
        process.exitCode ??= code;
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- trpc-cli types `exit` as never returning, though the override it documents returns.
        return undefined as never;
      },
    },
  });
}

/** What the command prints for `error`: its message, which is the hub's Problem Details for its errors. */
export function formatError(error: unknown): string {
  return ensureError(error).message;
}
