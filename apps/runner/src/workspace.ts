import { randomUUID } from "node:crypto";
import { posix } from "node:path";

import type { Context } from "@earendil-works/chord";
import { boundOutput, type OutputLimits } from "@earendil-works/chord/output";
import {
  err,
  ExecutionError,
  type BinaryReader,
  type DirReader,
  type ExecutionEnv,
  type FileErrorCode,
  FileError,
  type FileInfo,
  LineScanner,
  ok,
  type Result,
  type FileWatcher,
  type ShellExecOptions,
  type ShellExecResult,
  type TextLine,
} from "@earendil-works/pi-durable/env";
import { Cause, Duration, Effect, Exit } from "effect";
import ensureError from "ensure-error";
import {
  Bash,
  type BashExecResult,
  type CommandName,
  getCommandNames,
  ReadWriteFs,
} from "just-bash";

// A job's Execution environment (ADR 0019): just-bash on the job's Workspace directory, which the
// agent sees as `/`. pi-durable's file operations go to just-bash's `ReadWriteFs`, which keeps every
// path inside the directory and refuses symbolic links, and its commands to a `Bash` on the same
// `ReadWriteFs`, with no network and no Python, JavaScript or SQLite. Files are read whole, which
// `ReadWriteFs` bounds at 10 MB.

/**
 * just-bash's commands without `sqlite3`, which it includes by default. Like `python3` and `js-exec`,
 * which it leaves off unless asked, `sqlite3` runs in a worker thread, which escapes the permission
 * model a contained shell runs under (ADR 0019).
 */
const COMMANDS = getCommandNames().filter(
  // `getCommandNames` lists the `CommandName`s, typed as strings.
  (name): name is CommandName => name !== "sqlite3",
);

const decoder = new TextDecoder();
/**
 * A file's text as Node's `readFile(path, "utf8")` gives it, byte-order mark included: pi-durable's
 * edit tool strips the mark and writes it back.
 */
const textDecoder = new TextDecoder("utf-8", { ignoreBOM: true });

/**
 * `ReadWriteFs` errors carry Node's code only at the start of their message, as in `ENOENT: ...`;
 * they map to pi-durable's codes as its Node environment maps them.
 */
const FILE_ERROR_CODES: Record<string, FileErrorCode> = {
  ENOENT: "not_found",
  ENOTDIR: "not_directory",
  EISDIR: "is_directory",
  EACCES: "permission_denied",
  EPERM: "permission_denied",
  EINVAL: "invalid",
};

function fileError(error: unknown, path: string): FileError {
  if (error instanceof FileError) return error;
  const cause = ensureError(error);
  const code = /^([A-Z]+):/.exec(cause.message)?.[1];
  return new FileError(FILE_ERROR_CODES[code ?? ""] ?? "unknown", cause.message, path, cause);
}

function aborted(context: Context, path: string): Result<never, FileError> | undefined {
  return context.abortSignal?.aborted
    ? err(new FileError("aborted", "Operation aborted", path))
    : undefined;
}

async function attempt<T>(
  path: string,
  context: Context,
  operation: () => Promise<T>,
): Promise<Result<T, FileError>> {
  const stop = aborted(context, path);
  if (stop) return stop;
  try {
    return ok(await operation());
  } catch (error) {
    return err(fileError(error, path));
  }
}

function isMissing(error: unknown, path: string): boolean {
  const { code } = fileError(error, path);
  return code === "not_found" || code === "not_directory";
}

/** A spill that could not be written: the complete output would be lost. */
function unsavedOutput(error: FileError): ExecutionError {
  return new ExecutionError(
    "unknown",
    `Failed to preserve complete shell output: ${error.message}`,
    error,
  );
}

/**
 * A file's text as lines, split at each `\n` and keeping any `\r`, as pi-durable's Node environment
 * reads them, and whether each ended with one.
 */
function splitLines(text: string): TextLine[] {
  if (text === "") return [];
  const parts = text.split("\n");
  const lines = parts.map((line, index) => ({ text: line, terminated: index < parts.length - 1 }));
  if (lines.at(-1)?.text === "") lines.pop();
  return lines;
}

function isByteCount(n: number): boolean {
  return Number.isInteger(n) && n >= 0;
}

export class WorkspaceEnv implements ExecutionEnv {
  readonly id: string;
  cwd = "/";
  readonly #fs: ReadWriteFs;
  readonly #bash: Bash;

  /** The environment of the Workspace at `root` on the host; `id` names it to pi-durable. */
  constructor(id: string, root: string) {
    this.id = id;
    this.#fs = new ReadWriteFs({ root });
    this.#bash = new Bash({ fs: this.#fs, cwd: "/", commands: COMMANDS });
  }

  #resolve(path: string): string {
    return posix.resolve(this.cwd, path);
  }

  /** The file at `path`, read whole, as lines. */
  async #readLines(path: string): Promise<TextLine[]> {
    return splitLines(decoder.decode(await this.#fs.readFileBuffer(path)));
  }

  async #info(path: string): Promise<FileInfo> {
    const stat = await this.#fs.lstat(path);
    const kind = stat.isSymbolicLink ? "symlink" : stat.isDirectory ? "directory" : "file";
    return {
      name: posix.basename(path) || "/",
      path,
      kind,
      size: stat.size,
      mtimeMs: stat.mtime.getTime(),
    };
  }

  async absolutePath(path: string, context: Context) {
    return attempt(path, context, async () => this.#resolve(path));
  }

  async joinPath(parts: string[], context: Context) {
    return attempt(parts.join("/"), context, async () => posix.join(...parts));
  }

  async canonicalPath(path: string, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, () => this.#fs.realpath(absolute));
  }

  async readTextFile(path: string, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, async () =>
      textDecoder.decode(await this.#fs.readFileBuffer(absolute)),
    );
  }

  async readBinaryFile(path: string, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, () => this.#fs.readFileBuffer(absolute));
  }

  async readTextLines(path: string, options: { maxLines?: number } | undefined, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, async () =>
      (await this.#readLines(absolute))
        .slice(0, Math.max(options?.maxLines ?? Infinity, 0))
        .map((line) => line.text),
    );
  }

  async openTextLineReader(path: string, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, async () => {
      const lines = await this.#readLines(absolute);
      let index = 0;
      return {
        readLine: async (readContext: Context) =>
          aborted(readContext, absolute) ?? ok<TextLine | undefined, FileError>(lines[index++]),
        close: async () => {},
      };
    });
  }

  async openBinaryReader(
    path: string,
    options: { noFollow?: boolean } | undefined,
    context: Context,
  ): Promise<Result<BinaryReader, FileError>> {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, async () => {
      const info = await this.#info(absolute);
      if (info.kind === "symlink" && options?.noFollow) {
        throw new FileError("invalid", "Path is a symbolic link", absolute);
      }
      if (info.kind === "directory") {
        throw new FileError("is_directory", "Path is a directory", absolute);
      }
      const bytes = await this.#fs.readFileBuffer(absolute);
      let closed = false;
      const guard = (readContext: Context) =>
        aborted(readContext, absolute) ??
        (closed ? err(new FileError("invalid", "Binary reader is closed", absolute)) : undefined);
      return {
        info: async (readContext: Context) =>
          guard(readContext) ?? ok<FileInfo, FileError>({ ...info, size: bytes.length }),
        read: async (offset: number, length: number, readContext: Context) => {
          const stop = guard(readContext);
          if (stop) return stop;
          if (!isByteCount(offset) || !isByteCount(length)) {
            return err<Uint8Array, FileError>(
              new FileError("invalid", "Invalid byte range", absolute),
            );
          }
          return ok<Uint8Array, FileError>(bytes.slice(offset, offset + length));
        },
        scanLines: async (range: { startLine: number; endLine?: number }, readContext: Context) => {
          const stop = guard(readContext);
          if (stop) return stop;
          let scanner: LineScanner;
          try {
            scanner = new LineScanner(range.startLine, range.endLine);
          } catch {
            return err(new FileError("invalid", "Invalid line range", absolute));
          }
          scanner.push(bytes);
          return ok(scanner.finish());
        },
        close: async () => {
          closed = true;
        },
      };
    });
  }

  async writeFile(path: string, content: string | Uint8Array, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, () => this.#fs.writeFile(absolute, content));
  }

  async appendFile(path: string, content: string | Uint8Array, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, () => this.#fs.appendFile(absolute, content));
  }

  // pi-durable truncates and flushes files only to keep a journal in them (its JSONL storage), and a
  // job's journal never lives in its Workspace, where the agent could edit it. If a pi-durable tool
  // starts calling these, implement them: truncation in place and atomic, and flushing without
  // leaving `ReadWriteFs`'s containment.
  async truncateFile(path: string): Promise<Result<void, FileError>> {
    return err(new FileError("not_supported", "A Workspace file cannot be truncated", path));
  }

  async flushFile(path: string): Promise<Result<void, FileError>> {
    return err(new FileError("not_supported", "A Workspace file cannot be flushed", path));
  }

  async renameFile(sourcePath: string, destinationPath: string, context: Context) {
    const source = this.#resolve(sourcePath);
    return attempt(source, context, () => this.#fs.mv(source, this.#resolve(destinationPath)));
  }

  async fileInfo(path: string, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, () => this.#info(absolute));
  }

  async listDir(path: string, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, async () => {
      const names = await this.#fs.readdir(absolute);
      return Promise.all(names.map((name) => this.#info(posix.join(absolute, name))));
    });
  }

  async openDirReader(path: string, context: Context): Promise<Result<DirReader, FileError>> {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, async () => {
      const names = await this.#fs.readdir(absolute);
      let index = 0;
      let closed = false;
      return {
        next: async (maxEntries: number, readContext: Context) => {
          const stop = aborted(readContext, absolute);
          if (stop) return stop;
          if (closed) return err(new FileError("invalid", "Directory reader is closed", absolute));
          if (!Number.isInteger(maxEntries) || maxEntries < 1) {
            return err(new FileError("invalid", "maxEntries must be positive", absolute));
          }
          const entries: FileInfo[] = [];
          while (index < names.length && entries.length < maxEntries) {
            const entry = posix.join(absolute, names[index++] ?? "");
            try {
              entries.push(await this.#info(entry));
            } catch (error) {
              // An entry removed since the reader opened is skipped.
              if (!isMissing(error, entry)) return err(fileError(error, entry));
            }
          }
          return ok({ entries, done: index >= names.length });
        },
        close: async () => {
          closed = true;
        },
      };
    });
  }

  async watch(): Promise<Result<FileWatcher, FileError>> {
    return err(new FileError("not_supported", "A Workspace cannot be watched"));
  }

  async exists(path: string, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, async () => {
      try {
        await this.#fs.lstat(absolute);
        return true;
      } catch (error) {
        if (isMissing(error, absolute)) return false;
        throw error;
      }
    });
  }

  async createDir(path: string, options: { recursive?: boolean } | undefined, context: Context) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, () =>
      // Recursive unless told otherwise, as in pi-durable's Node environment.
      this.#fs.mkdir(absolute, { recursive: options?.recursive ?? true }),
    );
  }

  async remove(
    path: string,
    options: { recursive?: boolean; force?: boolean } | undefined,
    context: Context,
  ) {
    const absolute = this.#resolve(path);
    return attempt(absolute, context, () =>
      this.#fs.rm(absolute, {
        recursive: options?.recursive ?? false,
        force: options?.force ?? false,
      }),
    );
  }

  async createTempDir(prefix: string | undefined, context: Context) {
    const absolute = `/tmp/${prefix ?? "tmp-"}${randomUUID()}`;
    return attempt(absolute, context, async () => {
      await this.#fs.mkdir(absolute, { recursive: true });
      return absolute;
    });
  }

  async createTempFile(
    options: { prefix?: string; suffix?: string } | undefined,
    context: Context,
  ) {
    const absolute = `/tmp/${options?.prefix ?? "tmp-"}${randomUUID()}${options?.suffix ?? ""}`;
    return attempt(absolute, context, async () => {
      await this.#fs.mkdir("/tmp", { recursive: true });
      await this.#fs.writeFile(absolute, new Uint8Array());
      return absolute;
    });
  }

  async exec(
    command: string | readonly string[],
    options: ShellExecOptions | undefined,
    context: Context,
  ): Promise<Result<ShellExecResult, ExecutionError>> {
    if (context.abortSignal?.aborted)
      return err(new ExecutionError("aborted", "Execution aborted"));
    const argv = typeof command !== "string";
    if (argv && command.length === 0) return err(new ExecutionError("spawn_error", "Empty argv"));
    // just-bash runs in a working directory that does not exist; pi-durable's Node environment does not.
    const cwd = options?.cwd ? this.#resolve(options.cwd) : this.cwd;
    const directory = await this.fileInfo(cwd, context);
    if (!directory.ok || directory.value.kind !== "directory") {
      return err(new ExecutionError("spawn_error", `Working directory does not exist: ${cwd}`));
    }
    // An argv array runs its program with exactly these arguments, which `args` passes to `command`
    // without shell parsing. The shell's environment is only `env` on just-bash's own, so
    // `inheritEnv` has nothing to inherit: the Runner's variables never reach it.
    const source = argv ? "command" : command;
    const run = Effect.callback<BashExecResult, Error>((resume, signal) => {
      const running = this.#bash.exec(source, {
        cwd,
        env: options?.env,
        signal,
        ...(argv ? { args: ["--", ...command] } : {}),
      });
      running.then(
        (result) => resume(Effect.succeed(result)),
        (error) => resume(Effect.fail(ensureError(error))),
      );
      // Interrupted, by the timeout or the caller's abort, the command stops through `signal`;
      // `exec` returns only once it has.
      return Effect.tryPromise(() => running).pipe(Effect.ignore);
    });
    const exit = await Effect.runPromiseExit(
      options?.timeout ? run.pipe(Effect.timeout(Duration.seconds(options.timeout))) : run,
      { signal: context.abortSignal },
    );
    if (Exit.isFailure(exit)) {
      if (Cause.hasInterrupts(exit.cause)) {
        return err(new ExecutionError("aborted", "Execution aborted"));
      }
      const error = Cause.squash(exit.cause);
      if (Cause.isTimeoutError(error)) {
        return err(new ExecutionError("timeout", "Execution timed out"));
      }
      const cause = ensureError(error);
      return err(new ExecutionError("unknown", cause.message, cause));
    }
    const result = exit.value;
    // Without a shell, a program that cannot be found is a spawn failure, not an exit code. Only
    // just-bash's message for the program itself counts, not for a command the program runs.
    if (
      argv &&
      result.exitCode === 127 &&
      result.stderr === `bash: ${command[0]}: command not found\n`
    ) {
      return err(new ExecutionError("spawn_error", result.stderr.trim()));
    }
    // just-bash returns a command's output only once it ends, so it reaches the caller then: stdout
    // before stderr rather than as written, nothing while it runs, and nothing when it is stopped.
    try {
      if (result.stdout) options?.onOutput?.(result.stdout, context, { stream: "stdout" });
      if (result.stderr) options?.onOutput?.(result.stderr, context, { stream: "stderr" });
    } catch (error) {
      const cause = ensureError(error);
      return err(new ExecutionError("callback_error", cause.message, cause));
    }
    const output = result.stdout + result.stderr;
    const spill = options?.spill;
    // Spilled when the limits would drop any of it, measured as chord bounds a tool's output.
    const limits: OutputLimits | undefined = spill && {
      maxBytes: spill.afterBytes,
      maxLines: spill.afterLines,
      retain: "tail",
    };
    if (!limits || boundOutput(output, limits).droppedBytes === 0) {
      return ok({ exitCode: result.exitCode });
    }
    // Output past the caller's limits is saved whole in the Workspace, where the agent can read it.
    const file = await this.createTempFile({ prefix: "pi-output-", suffix: ".log" }, context);
    if (!file.ok) return err(unsavedOutput(file.error));
    const written = await this.writeFile(file.value, output, context);
    if (!written.ok) return err(unsavedOutput(written.error));
    return ok({ exitCode: result.exitCode, spillPath: file.value });
  }

  async cleanup() {}
}
