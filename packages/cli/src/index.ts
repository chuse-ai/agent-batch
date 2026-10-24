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
  contract,
  createProblemDetailsLink,
  FINAL_JOB_STATUSES,
  getAccess,
  jobRefSchema,
  jobSubmissionSchema,
  problemOf,
} from "contract";
import { Config, Effect, Redacted, Schedule, Schema, SchemaIssue } from "effect";
import ensureError from "ensure-error";
import isNetworkError from "is-network-error";
import { createCli, os } from "trpc-cli";
import * as z from "zod/mini";

// The `agent-batch` command: one command per call of the hub's API that an API key can make,
// derived from the contract, so the commands cannot drift from the API, plus `jobs wait`. Each
// prints the API's JSON on stdout; an error from the hub prints its Problem Details on stderr and
// exits non-zero.

type HubClient = ContractRouterClient<typeof contract>;

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

/** Where the hub is and the API key to call it with, read when a command runs, not for `--help`. */
const settings = Config.all({
  url: Config.URL("AGENT_BATCH_URL"),
  apiKey: Config.schema(Schema.Redacted(Schema.String.check(isHeaderValue)), "AGENT_BATCH_API_KEY"),
});

/** The link to the hub, which calls any of its procedures by path. */
const hubLink = Effect.map(settings, ({ url, apiKey }) =>
  createProblemDetailsLink(contract, {
    origin: url.origin,
    // The hub's path, for a hub served under one. An HTTP URL's path always starts with `/`, which
    // oRPC's type spells out.
    url: `/${url.pathname.slice(1)}`,
    headers: { "x-api-key": Redacted.value(apiKey) },
  }),
);

const hubClient = Effect.map(hubLink, (link): HubClient => createORPCClient(link));

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
function hubCall<A>(call: () => Promise<A>) {
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
 * Runs a command's work, its failure an `Error` whose message the command prints: trpc-cli prints
 * only an `Error`'s, and reports some by their cause. An error the hub answered prints its Problem
 * Details, which the contract's link keeps as its cause; missing settings say which to set.
 */
function runCommand(work: Effect.Effect<void, unknown>) {
  return Effect.runPromise(
    work.pipe(
      Effect.mapError((error) => {
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
      }),
    ),
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
          yield* printStep(yield* hubCall(() => link.call([...path], given, { context: {} })));
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
      if (getAccess(value)?.key) commands[name] = command(value, at);
    } else {
      const nested = commandsOf(value, at);
      if (Object.keys(nested).length > 0) commands[name] = nested;
    }
  }
  return commands;
}

/**
 * `jobs submit`: retried on a transient failure with one idempotency key, so a retry never creates
 * a second job: the caller's, or one made once for this run of the command, which is printed on
 * stderr before anything is sent.
 */
function submitCommand() {
  // trpc-cli reads a schema registered with an id, which Zod's JSON Schema gives as a reference, as
  // one `--json` input (mmkal/trpc-cli#233); a copy of its fields gets a flag for each. Zod's
  // `safeExtend` would keep checks of the object itself, but its type here loses the fields'; the
  // submission has none.
  return os
    .meta({
      description: getOpenAPIMeta(contract.jobs.submit)?.description,
      // `--input` is read before trpc-cli parses the command line, so its help shows it only here.
      examples: [
        "agent-batch jobs submit --input job.json",
        "agent-batch jobs submit --input - < job.json",
      ],
    })
    .input(z.object(jobSubmissionSchema.shape))
    .handler(({ input }) =>
      runCommand(
        Effect.gen(function* () {
          const client = yield* hubClient;
          const idempotencyKey = input.idempotencyKey ?? crypto.randomUUID();
          if (input.idempotencyKey === undefined) {
            // Before anything is sent, so that a command stopped or killed meanwhile has told it.
            yield* Effect.sync(() =>
              process.stderr.write(
                `Submitting with idempotency key ${idempotencyKey}: if this command cannot tell whether the job was accepted, submit it again with this key, as idempotencyKey in its JSON, or as --idempotency-key when giving its fields as flags, to find out.\n`,
              ),
            );
          }
          const submitted = yield* hubCall(() =>
            client.jobs.submit({ ...input, idempotencyKey }),
          ).pipe(Effect.retry(retryTransient));
          yield* printStep(submitted);
        }),
      ),
    );
}

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
          const client = yield* hubClient;
          // Jittered after the cap, so the reads of many callers stay spread: 20 to 30 s at most.
          const between = Schedule.min([
            Schedule.exponential("1 second"),
            Schedule.spaced("25 seconds"),
          ]).pipe(Schedule.jittered);
          const job = yield* hubCall(() => client.jobs.get(input)).pipe(
            // A read is retried as any call is, and an error that remains ends the wait, as AWS's
            // waiters end on one: an error that waiting does not fix, such as a hub not set up,
            // would otherwise keep the wait running without a word.
            Effect.retry(retryTransient),
            Effect.repeat({
              schedule: between,
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

/** The `agent-batch` command line. */
export function agentBatchCli(version: string) {
  const commands = commandsOf(contract);
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
      jobs: { ...commands["jobs"], submit: submitCommand(), wait: waitCommand() },
    }),
  });
}

/** What the command prints for `error`: its message, which is the hub's Problem Details for its errors. */
export function formatError(error: unknown): string {
  return ensureError(error).message;
}
