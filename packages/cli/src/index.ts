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
  Stream,
  Struct,
} from "effect";
import { Ndjson } from "effect/encoding";
import ensureError from "ensure-error";
import isNetworkError from "is-network-error";
import { logUpdateStderr } from "log-update";
import { createCli, os } from "trpc-cli";
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

/** Retries a call that failed transiently, at most twice, waiting longer each time, as Stripe's Go client does. */
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
 * The caller's custom ID, or one made for this run of the command and printed on stderr, through
 * `notice`, before anything is sent, so that a command stopped or killed meanwhile has told it.
 */
function customIdOrPrintNew(given: string | undefined, notice: (id: string) => string) {
  if (given !== undefined) return Effect.succeed(given);
  const id = crypto.randomUUID();
  return Effect.as(
    Effect.sync(() => process.stderr.write(`${notice(id)}\n`)),
    id,
  );
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
      z.extend(jobSubmissionSchema, {
        customId: z.optional(
          jobSubmissionSchema.shape.customId.check(
            z.describe(
              "Your own ID for the job and its Batch of one; one is made for you when left out. Submitting again with the same `customId` and the same job returns the job already accepted, and with a different job is refused.",
            ),
          ),
        ),
        agent: agentSchema,
      }),
    )
    .handler(({ input }) =>
      runCommand(
        Effect.gen(function* () {
          const client = yield* apiClient;
          const { agent, ...given } = input;
          const id = yield* customIdOrPrintNew(
            input.customId,
            (made) =>
              `Submitting with custom ID ${made}: if this command cannot tell whether the job was accepted, submit it again with this ID, as customId in its JSON, to find out.`,
          );
          const batch = yield* hubCall((signal) =>
            client.batches.create({ customId: id, agent, breaker: false }, { signal }),
          ).pipe(Effect.retry(retryTransient));
          const added = yield* hubCall((signal) =>
            client.batches.addJobs(
              { id: batch.id, jobs: [{ ...given, customId: id }] },
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
      .input(z.object(batchCreationSchema.shape))
      .handler(({ input }) =>
        runCommand(
          Effect.gen(function* () {
            const client = yield* apiClient;
            const customId = yield* customIdOrPrintNew(
              input.customId,
              (made) =>
                `Creating with custom ID ${made}: if this command cannot tell whether the Batch was created, create it again with this ID, as customId in its JSON, to find out.`,
            );
            const created = yield* hubCall((signal) =>
              client.batches.create({ ...input, customId }, { signal }),
            ).pipe(Effect.retry(retryTransient));
            yield* printStep(created);
          }),
        ),
      )
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
 * at a time, each upload retried as any call is. Shows how far the uploads are on stderr when it
 * is a terminal.
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
    const onTerminal = process.stderr.isTTY;
    const total = missing.reduce((sum, { sizeBytes }) => sum + sizeBytes, 0);
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

/** Leaves the progress `logUpdateStderr` shows on stderr in place, so what follows goes below it. */
const endProgress = Effect.sync(() => {
  if (process.stderr.isTTY) logUpdateStderr.done();
});

/**
 * The jobs in the JSONL file `file`, one job's JSON per line, as `batches add-jobs` takes each,
 * blank lines left out, all read before any is sent; each is checked as the hub checks it. A line
 * may hold at most what one request to the hub carries. A problem names its job by its place among
 * the jobs, as Effect's `Ndjson` counts no lines. A file whose first job fails is told what it holds
 * instead, from how it starts: a JSON array, with how to make it JSONL; JSON over several lines,
 * with how to put it on one; anything else, such as a PDF given as the positional by mistake, with a
 * pointer to `--each`.
 */
function jobsFromJsonl(file: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const notJsonl = `${file} is not a JSONL file of jobs, one job's JSON per line; to make a job of each file or folder, use --each ${file}`;
    const arrayHint = `${file} holds a JSON array, not one job's JSON per line; jq -c '.[]' ${file} makes it JSONL`;
    if ((yield* fs.stat(file)).type !== "File") return yield* Effect.fail(new Error(notJsonl));
    // What the file holds, from its first characters, a byte order mark left out.
    const whatItHolds = Effect.map(
      Stream.runCollect(fs.stream(file, { bytesToRead: 64 })),
      (chunks) => {
        const start = new TextDecoder().decode(Buffer.concat(chunks)).trimStart();
        return start.startsWith("[")
          ? arrayHint
          : start.startsWith("{")
            ? `${file} holds JSON over several lines, not one job's JSON per line; jq -c . ${file} puts each on one line`
            : notJsonl;
      },
    );
    const jobs: JobSubmission[] = [];
    yield* fs.stream(file).pipe(
      // Ndjson skips a leading BOM and accepts CRLF and CR line ends, as files saved on Windows
      // have them.
      Stream.pipeThroughChannel(
        Ndjson.decode({ ignoreEmptyLines: true, maxLineBytes: MAX_BODY_BYTES }),
      ),
      Stream.runForEach((value) => {
        if (Array.isArray(value)) return Effect.fail(new Error(arrayHint));
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
        if (error.reason === "LineTooLarge") {
          return Effect.fail(
            new Error(
              `${at} is longer than ${MAX_BODY_BYTES / 1024 / 1024} MiB, the most one request to the hub carries`,
            ),
          );
        }
        if (jobs.length === 0)
          return Effect.flatMap(whatItHolds, (hint) => Effect.fail(new Error(hint)));
        return Effect.fail(
          new Error(
            error.reason === "InvalidUtf8"
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
 * `batches submit --wait`: reads the Batch `batchId` until waiting for it is over, showing how it
 * stands on stderr when that is a terminal, then prints a line for each of `jobs`, in order, and
 * fails unless every one succeeded. A caller's pause keeps it waiting, as `jobs wait` does; its
 * failure breaker's pause ends the wait, as the breaker paused it for a flaw to fix first. A timeout
 * or Ctrl-C stops the wait and leaves the Batch running.
 */
function waitForOutputs(
  client: ApiClient,
  batchId: string,
  jobs: readonly (readonly [string, Job])[],
  options: { again: string; timeoutSeconds: number | undefined },
) {
  return Effect.gen(function* () {
    const onTerminal = process.stderr.isTTY;
    const again = `${options.again} --wait`;
    // Where a person watches, the Batch's ID, in OpenAI's CLI's words, as nothing else prints it
    // while the wait goes well; a script reads it from the jobs instead.
    if (onTerminal) {
      yield* Effect.sync(() =>
        process.stderr.write(`Batch created: "${batchId}". Waiting for completion.\n`),
      );
    }
    const waitAndRead = Effect.gen(function* () {
      const batch = yield* hubCall((signal) =>
        client.batches.get({ id: batchId }, { signal }),
      ).pipe(
        Effect.retry(retryTransient),
        Effect.tap((read) => (onTerminal ? showProgress(client, read) : Effect.void)),
        Effect.repeat({ schedule: pollSchedule, until: settled }),
        Effect.ensuring(endProgress),
      );
      // One read per job, a few at a time, until the API reads a Batch's outputs a page at a time.
      const read = yield* Effect.forEach(
        jobs,
        ([customId, added]) =>
          hubCall((signal) => client.jobs.get({ id: added.id }, { signal })).pipe(
            Effect.retry(retryTransient),
            Effect.map((readJob) => outputLine(customId, readJob)),
          ),
        { concurrency: AT_ONCE },
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
 * `batches submit`: one Batch running the agent its flags give, with the jobs of a JSONL file, as
 * `openai batches submit` takes one, or a job for each path `--each` gives, whose files it uploads
 * first, only those the hub lacks. Jobs are added in their order, at most 100 at a time, so the
 * first are its Canary. Each call is retried on a transient failure, under one custom ID: the
 * caller's, or one made for this run and printed on stderr before anything is sent, so running the
 * same command again with it returns the same Batch and adds only the jobs it lacks. With `--wait`,
 * it then follows the Batch until no job is queued, running or paused and prints a line for each
 * job.
 */
function submitBatchCommand() {
  return os
    .meta({
      description:
        "Creates a Batch running an agent given by its flags, with the jobs of a JSONL file, one job's JSON per line as batches add-jobs takes it, or with --each, a job for each file or folder: a file becomes a job with that file as its Input file, and a folder a job with every file in its tree, at its path in the folder, hidden ones left out; each job's custom ID is its path as given, normalized. --each uploads the files the hub does not hold yet. Prints the Batch, or, with --wait, waits for its jobs to end and prints one JSON line for each, with its output or its failure. Running the command again with the Batch's --custom-id adds only the jobs the Batch lacks.",
      examples: [
        'agent-batch batches submit --each invoices/*.pdf --instructions "Extract the supplier, number, currency and total." --output-schema invoice.schema.json --wait > outputs.jsonl',
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
    .handler(({ input: [files, flags] }) =>
      runToEnd(
        Effect.gen(function* () {
          if (flags.waitTimeout !== undefined && !flags.wait) {
            return yield* Effect.fail(new Error("--wait-timeout needs --wait"));
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
                `batches submit takes one JSONL file of jobs; to make a job of each file or folder, use --each ${files.join(" ")}`,
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
          const client = yield* apiClient;
          const customId = yield* customIdOrPrintNew(
            flags.customId,
            (made) =>
              `Submitting with custom ID ${made}: if this command stops before every job is added, run this command again with --custom-id ${made} to add the rest.`,
          );
          const again = `run this command again with --custom-id ${customId}`;
          yield* uploadMissing(client, local.files);
          const created = yield* hubCall((signal) =>
            client.batches.create({ customId, agent: parsedAgent.data }, { signal }),
          ).pipe(Effect.retry(retryTransient));
          const adds = yield* Effect.forEach(Array.chunksOf(local.jobs, MAX_JOBS_PER_ADD), (jobs) =>
            hubCall((signal) => client.batches.addJobs({ id: created.id, jobs }, { signal })).pipe(
              Effect.retry(retryTransient),
            ),
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
            ? waitForOutputs(
                client,
                created.id,
                Array.zip(
                  local.jobs.map((job) => job.customId),
                  added,
                ),
                { again, timeoutSeconds: flags.waitTimeout },
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

/** What the command prints for `error`: its message, which is the hub's Problem Details for its errors. */
export function formatError(error: unknown): string {
  return ensureError(error).message;
}
