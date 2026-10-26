import { randomUUID } from "node:crypto";
import { mkdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { type SandboxRuntimeConfig, SandboxManager } from "@anthropic-ai/sandbox-runtime";
import type { Context } from "@earendil-works/chord";
import { awaitWithContext, BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import {
  type BinaryReader,
  type DirReader,
  err,
  type ExecutionEnv,
  ExecutionError,
  FileError,
  type FileWatcher,
  ok,
  type Result,
  type ShellExecOptions,
  type ShellExecResult,
  type TextLineReader,
} from "@earendil-works/pi-durable/env";
import { Connection, localDaemon, RemoteExecutionEnv } from "@earendil-works/pi-env";
import * as NodeChildProcessSpawner from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as NodeFileSystem from "@effect/platform-node-shared/NodeFileSystem";
import * as NodePath from "@effect/platform-node-shared/NodePath";
import { Data, Effect, FileSystem, Layer, Record, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import ensureError from "ensure-error";

// A job's Execution environment (ADR 0027): pi-env's daemon, started inside an OS sandbox of the
// job's own by Anthropic's sandbox-runtime, carries out the job's file operations and runs its
// commands, and the Runner reaches it through pi-env's `RemoteExecutionEnv`. So every file access
// and every command of the job meets the same rules, enforced by the kernel: the job reads the
// system's programs and libraries, any folders the operator adds, and its own Workspace and temp
// directory, writes only the last two, and has no network. Where the host has a user systemd
// manager, the sandbox runs in a systemd scope that caps the job's memory.

/**
 * What a job may read of the host besides its own folders: Codex CLI's "platform defaults", the
 * system paths it keeps readable when reads are restricted, "intentionally system-level paths only
 * … so `include_platform_defaults` does not silently widen access to user data"
 * (`codex-rs/linux-sandbox/src/bwrap.rs`, and the paths of
 * `codex-rs/sandboxing/src/seatbelt_read_only_platform_defaults.sbpl` for macOS). sandbox-runtime
 * skips those that do not exist on the host, as Codex does.
 */
const SYSTEM_READ_PATHS: Partial<Record.ReadonlyRecord<NodeJS.Platform, readonly string[]>> = {
  linux: [
    "/bin",
    "/sbin",
    "/usr",
    "/etc",
    "/lib",
    "/lib64",
    "/nix/store",
    "/run/current-system/sw",
  ],
  darwin: [
    // Codex's profile grants some paths less than a read: only loading code, or only reading
    // metadata. sandbox-runtime has only read grants, so this list departs from Codex in three
    // ways. /System/Library/Extensions (Codex: loading code only) and /System/Library/CoreServices
    // (Codex: the folder and two files in it) are readable in full; both hold Apple's system files,
    // not user data, and Gemini CLI's strict profile reads all of /System. Codex's metadata-only
    // rules are left out, as sandbox-runtime allows reading every folder's metadata when reads
    // are denied.
    "/bin",
    "/sbin",
    "/usr/bin",
    "/usr/sbin",
    "/usr/lib",
    "/usr/libexec",
    "/usr/share",
    "/usr/local/lib",
    "/etc",
    "/private/etc",
    "/opt/homebrew/lib",
    "/Library/Apple",
    "/Library/Filesystems/NetFSPlugins",
    "/Library/Preferences",
    "/private/var/db/timezone",
    "/private/var/db/DarwinDirectory/local/recordStore.data",
    "/var/db",
    "/private/var/db",
    "/System/Library/Extensions",
    "/System/Library/Frameworks",
    "/System/Library/PrivateFrameworks",
    "/System/Library/SubFrameworks",
    "/System/Library/CoreServices",
    "/System/iOSSupport/System/Library/Frameworks",
    "/System/iOSSupport/System/Library/PrivateFrameworks",
    "/System/iOSSupport/System/Library/SubFrameworks",
  ],
};

/**
 * sandbox-runtime's seccomp helpers, which run inside the sandbox it starts: under
 * `denyRead: ["/"]` a job must be granted them. The package does not export their path, so it is
 * found next to its main module.
 */
const SECCOMP_HELPERS = fileURLToPath(
  new URL("../vendor/seccomp", import.meta.resolve("@anthropic-ai/sandbox-runtime")),
);

/**
 * The variables of the Runner's environment a job's daemon keeps, with every `LC_*`; everything
 * else, keys included, stays out. They describe the user and the locale, as POSIX defines them (XBD
 * chapter 8), and sudo's `env_reset` and systemd's services keep them too. `HOME` and `TMPDIR` are
 * the job's own, set inside the sandbox; terminal variables such as `TERM` are left out, since
 * commands run without a terminal.
 */
const KEPT_VARIABLES = ["PATH", "SHELL", "LOGNAME", "USER", "LANG", "LANGUAGE", "TZ"];

/** How the Runner runs every job's sandbox. */
export interface SandboxSettings {
  /** Folders a job may read besides the system's, such as tools installed in a home folder. */
  extraReadPaths: readonly string[];
  /** Each job's memory limit, in systemd's notation such as `1G`. */
  jobMemoryLimit: string;
  /** Runs jobs without a sandbox where none can start, instead of refusing to run them. */
  runWithoutSandbox: boolean;
}

/** A job's own folders: its Workspace, and its temp directory, which may not exist yet. */
export interface JobDirs {
  workspace: string;
  temp: string;
}

/** A job's environment, and how to stop everything it started. */
export interface JobEnvironment {
  /** Each of its operations ends as soon as the context it is given is aborted (`endingOnAbort`). */
  env: ExecutionEnv;
  /** Stops the job's daemon, which stops every process it started, and releases its sandbox. */
  close(): Promise<void>;
}

/** What the host offers jobs, found once when the Runner starts. */
export interface JobEnvironments {
  /** Why jobs run without a sandbox, if they do; only with `runWithoutSandbox`. */
  unsandboxedBecause: string | undefined;
  /** Why jobs run without a memory cap, if they do. */
  uncappedBecause: string | undefined;
  /**
   * Opens the environment of the job whose files are in `workspace` and `temp`: nothing starts
   * until its first operation, and a daemon lost, as when it ran out of memory, starts again.
   */
  open(dirs: JobDirs): JobEnvironment;
}

/** Jobs cannot run in a sandbox on this host, and the operator has not allowed running without one. */
export class SandboxUnavailable extends Data.TaggedError("SandboxUnavailable")<{
  message: string;
}> {}

/**
 * Finds what the host offers jobs: starts a sandboxed command to prove the sandbox works, and a
 * systemd scope with the job's memory limit to prove memory can be capped. Without a sandbox it
 * fails with `SandboxUnavailable` naming what is missing, unless `settings.runWithoutSandbox` is
 * set.
 */
export function prepareJobEnvironments(
  settings: SandboxSettings,
): Effect.Effect<JobEnvironments, SandboxUnavailable> {
  return Effect.gen(function* () {
    const daemon = localDaemon();
    const readable = [
      ...(SYSTEM_READ_PATHS[process.platform] ?? []),
      ...settings.extraReadPaths,
      path.dirname(daemon),
      SECCOMP_HELPERS,
    ];
    const unsandboxedBecause = yield* setUpSandboxRuntime(daemon, readable);
    if (unsandboxedBecause && !settings.runWithoutSandbox) {
      return yield* new SandboxUnavailable({
        message: `Jobs cannot run in a sandbox on this host: ${unsandboxedBecause}. Install what is missing (see the runner's README), or set RUNNER_RUN_WITHOUT_SANDBOX=true to run them without one.`,
      });
    }
    const uncappedBecause = yield* tryMemoryCap(settings.jobMemoryLimit);
    return {
      unsandboxedBecause,
      uncappedBecause,
      open: (dirs: JobDirs) =>
        openJobEnvironment({
          daemon,
          dirs,
          readable,
          sandboxed: unsandboxedBecause === undefined,
          memoryLimit: uncappedBecause === undefined ? settings.jobMemoryLimit : undefined,
        }),
    };
  }).pipe(Effect.provide(NodeServices));
}

/** Node's implementations of the process and file-system services the Runner's checks use. */
const NodeServices = NodeChildProcessSpawner.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
);

/**
 * No network for jobs until Secrets and egress are built. It is also sandbox-runtime's
 * process-wide network configuration: its proxies allow a host on that list for every sandbox,
 * before they consult the sandbox's own, so it must stay empty there too.
 */
const NO_NETWORK = { allowedDomains: [], deniedDomains: [] };

/**
 * Configures sandbox-runtime once per process: it keeps its configuration and network proxies in
 * module state. Every job's sandbox gets its own filesystem policy, so the one given here applies
 * to none of them; its network policy applies to all of them (`NO_NETWORK`).
 *
 * Initializing also adds sandbox-runtime's one-time `SIGINT` and `SIGTERM` listeners, which reset
 * the sandbox but do not exit, and a listener on a signal turns off Node's exit on it. So the
 * process hosting the Runner exits on those signals itself, as `NodeRuntime.runMain` makes
 * `main.ts` do; otherwise it outlives the first one with its sandbox reset. This is not reported to
 * sandbox-runtime yet (`registerCleanup` in 0.0.80).
 */
let initialized: Promise<void> | undefined;
function initializeSandboxRuntime(): Promise<void> {
  initialized ??= SandboxManager.initialize({
    network: NO_NETWORK,
    filesystem: { denyRead: ["/"], allowRead: [], allowWrite: [], denyWrite: [] },
  });
  return initialized;
}

/**
 * Sets up sandbox-runtime and runs a sandboxed command to prove it works. Returns why jobs cannot
 * run in a sandbox here, or `undefined` when the command ran.
 */
function setUpSandboxRuntime(
  daemon: string,
  readable: readonly string[],
): Effect.Effect<
  string | undefined,
  never,
  FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner
> {
  // sandbox-runtime's Windows backend grants folders for a whole session, not per job.
  if (process.platform === "win32")
    return Effect.succeed("native Windows is not supported yet; run the Runner in WSL");
  if (!SandboxManager.isSupportedPlatform())
    return Effect.succeed(`${process.platform} has no supported sandbox`);
  return Effect.gen(function* () {
    // Initializing checks for the programs the sandbox needs, but not for AppArmor denying
    // bubblewrap its user namespaces, as on Ubuntu 24.04 and later, so a command proves the sandbox
    // starts.
    yield* Effect.tryPromise({ try: () => initializeSandboxRuntime(), catch: ensureError });
    const scratch = yield* FileSystem.FileSystem.use((fs) =>
      fs.makeTempDirectoryScoped({ prefix: "agent-batch-sandbox-check-" }),
    );
    const wrapped = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          SandboxManager.wrapWithSandbox(
            `test -x "$AGENT_BATCH_DAEMON"`,
            undefined,
            jobPolicy({ workspace: scratch, temp: scratch }, readable),
          ),
        catch: ensureError,
      }),
      () => Effect.sync(() => SandboxManager.cleanupAfterCommand()),
    );
    const { failure } = yield* runCommand(
      ChildProcess.make("/bin/sh", ["-c", wrapped], {
        env: { ...keptEnvironment(), AGENT_BATCH_DAEMON: daemon },
      }),
    );
    return failure === undefined ? undefined : `a sandboxed command failed: ${failure}`;
  }).pipe(
    Effect.scoped,
    Effect.catch((error) => Effect.succeed(`the sandbox could not be set up: ${error.message}`)),
  );
}

/**
 * Starts a systemd scope with the job's memory limit, as every job's daemon starts, to prove it
 * can. Returns why jobs' memory cannot be capped here, such as no user systemd manager or a limit
 * systemd does not read, or `undefined` when the scope started.
 */
function tryMemoryCap(
  memoryLimit: string,
): Effect.Effect<string | undefined, never, ChildProcessSpawner.ChildProcessSpawner> {
  if (process.platform !== "linux") return Effect.succeed(`${process.platform} has no systemd`);
  const [command, ...args] = cappedScope(`agent-batch-check-${randomUUID()}.scope`, memoryLimit, [
    "true",
  ]);
  return runCommand(ChildProcess.make(command, args)).pipe(
    Effect.map(({ failure }) =>
      failure === undefined
        ? undefined
        : `no user systemd manager could start a scope with the job's memory limit of ${memoryLimit}: ${failure}`,
    ),
  );
}

/**
 * Runs `command` until it ends and returns what it printed, or why it failed: here a command
 * failing is an answer, such as no user systemd manager, not an error. A command given an `env`
 * gets only that environment, not the Runner's.
 */
function runCommand(
  command: ChildProcess.Command,
): Effect.Effect<
  { stdout: string; failure: string | undefined },
  never,
  ChildProcessSpawner.ChildProcessSpawner
> {
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const handle = yield* spawner.spawn(command);
    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        Stream.mkString(Stream.decodeText(handle.stdout)),
        Stream.mkString(Stream.decodeText(handle.stderr)),
        handle.exitCode,
      ],
      { concurrency: "unbounded" },
    );
    // A command that ran says why in its stderr; one that could not start, only in its error.
    const failure = exitCode === 0 ? undefined : stderr.trim() || `exit code ${exitCode}`;
    return { stdout, failure };
  }).pipe(
    Effect.scoped,
    Effect.catch((error) => Effect.succeed({ stdout: "", failure: error.message })),
  );
}

/** Runs `command` in a new systemd scope named `scope` whose processes may use `memoryLimit` in all. */
function cappedScope(scope: string, memoryLimit: string, command: readonly string[]): string[] {
  return [
    "systemd-run",
    "--user",
    "--scope",
    "--quiet",
    "--collect",
    // The job's command is shell text full of `$`, which systemd would otherwise expand.
    "--expand-environment=no",
    `--unit=${scope}`,
    "-p",
    `MemoryMax=${memoryLimit}`,
    "-p",
    "MemorySwapMax=0",
    // Only the process that went over is killed; by default systemd stops the whole scope.
    "-p",
    "OOMPolicy=continue",
    "--",
    ...command,
  ];
}

/** The Runner's variables a job's daemon keeps: `KEPT_VARIABLES` and every `LC_*`. */
function keptEnvironment(): Record.ReadonlyRecord<string, string> {
  return Record.filter(
    process.env,
    (value, name): value is string =>
      value !== undefined && (KEPT_VARIABLES.includes(name) || name.startsWith("LC_")),
  );
}

/**
 * A job's sandbox: reading the system's paths, `readable` and its own folders, writing only its own
 * folders, no network. sandbox-runtime also denies writes to shell and tool settings, such as
 * `.bashrc` and `.git/hooks`, but finds them only under the Runner's working directory, so in its
 * own folders a job may write those too; the README warns against running git there unsandboxed.
 */
function jobPolicy(dirs: JobDirs, readable: readonly string[]): Partial<SandboxRuntimeConfig> {
  return {
    network: NO_NETWORK,
    filesystem: {
      denyRead: ["/"],
      allowRead: [...readable, dirs.workspace, dirs.temp].map(asPath),
      allowWrite: [dirs.workspace, dirs.temp].map(asPath),
      denyWrite: [],
    },
  };
}

/**
 * A grant of `dir` itself: sandbox-runtime reads a plain string that holds `*`, `?`, `[` or `]` as
 * a glob too, so a folder such as `runner[1]` would also grant `runner1`.
 */
function asPath(dir: string) {
  return { path: dir, literal: true as const };
}

/**
 * The job's processes can read the daemon's environment, so it holds only the kept variables.
 * pi-env's `Connection` takes a command but no environment, so the command clears it: `env -i` sits
 * inside `systemd-run`, which needs the Runner's own variables to reach the user's systemd manager.
 * The temp directory is set inside the sandbox, since sandbox-runtime sets `TMPDIR` there itself;
 * it is also `HOME`, so tools that keep caches and settings in a home folder keep them with the
 * job. pi-env's connection adds `serve --token <hex>` to the command, which the outer shell passes
 * in as `$3`.
 */
const START_DAEMON = `export PI_ENV_TOKEN="$3"; exec /bin/sh -c "$0"`;
const DAEMON_IN_SANDBOX = `token="$PI_ENV_TOKEN"; daemon="$AGENT_BATCH_DAEMON"; export TMPDIR="$AGENT_BATCH_TEMP" HOME="$AGENT_BATCH_TEMP"; unset PI_ENV_TOKEN AGENT_BATCH_DAEMON AGENT_BATCH_TEMP; exec "$daemon" serve --token "$token"`;

function openJobEnvironment(options: {
  daemon: string;
  dirs: JobDirs;
  readable: readonly string[];
  sandboxed: boolean;
  memoryLimit: string | undefined;
}): JobEnvironment {
  const { daemon, dirs, memoryLimit } = options;
  /** Sandboxes wrapped for this job, each to be released once: one per start of its daemon. */
  let wraps = 0;
  /**
   * Aborted when the environment closes, which ends a start still wrapping its sandbox: pi-env's
   * `close()` reaches a start only once its command is built.
   */
  const closing = new AbortController();
  /** The systemd scope of the daemon's latest start, if memory is capped. */
  let scope: string | undefined;
  const connection = new Connection({
    command: async () => {
      await mkdir(dirs.temp, { recursive: true });
      let inner = DAEMON_IN_SANDBOX;
      if (options.sandboxed) {
        inner = await SandboxManager.wrapWithSandbox(
          DAEMON_IN_SANDBOX,
          undefined,
          jobPolicy(dirs, options.readable),
          closing.signal,
        );
        // A wrap that ends after the environment closed is released here, as `close()` has
        // released the others already.
        if (closing.signal.aborted) {
          SandboxManager.cleanupAfterCommand();
          closing.signal.throwIfAborted();
        }
        wraps += 1;
      }
      const daemonEnv = Record.collect(
        { ...keptEnvironment(), AGENT_BATCH_DAEMON: daemon, AGENT_BATCH_TEMP: dirs.temp },
        (name, value) => `${name}=${value}`,
      );
      const start = ["env", "-i", ...daemonEnv, "/bin/sh", "-c", START_DAEMON, inner];
      if (memoryLimit === undefined) return start;
      scope = `agent-batch-job-${randomUUID()}.scope`;
      return cappedScope(scope, memoryLimit, start);
    },
  });
  const env = new MemoryReportingEnv({
    connection,
    id: `job:${dirs.workspace}`,
    cwd: dirs.workspace,
    memoryLimit,
    scope: () => scope,
  });
  return {
    env: endingOnAbort<ExecutionEnv>(env),
    async close() {
      closing.abort();
      connection.close();
      for (; wraps > 0; wraps -= 1) SandboxManager.cleanupAfterCommand();
    },
  };
}

/** The operations of an environment and of the handles it opens. */
type Operation =
  | keyof ExecutionEnv
  | keyof TextLineReader
  | keyof BinaryReader
  | keyof DirReader
  | keyof FileWatcher;

/** The operations of an environment that open a handle with operations of its own. */
const OPENERS: ReadonlySet<PropertyKey> = new Set([
  "openTextLineReader",
  "openBinaryReader",
  "openDirReader",
  "watch",
] satisfies Operation[]);

/**
 * The operations that release what an environment or a handle holds. They are started even under an
 * aborted context, as a tool closes its reader once it is cut off, but not waited for then, since
 * the daemon may never answer; they give back nothing.
 */
const RELEASES: ReadonlySet<PropertyKey> = new Set(["close", "cleanup"] satisfies Operation[]);

/**
 * What an operation named `name` gives back once aborted, as pi's environments do: a command's
 * failure, nothing from a release, and a file operation's failure from the rest.
 */
function abortedResult(name: PropertyKey) {
  if (name === "exec") return err(new ExecutionError("aborted", "aborted"));
  if (RELEASES.has(name)) return undefined;
  return err(new FileError("aborted", "aborted"));
}

/** Whether an opener's `result` is a handle it opened, rather than its failure or an aborted result. */
function isOpenedHandle(
  result: unknown,
): result is { ok: true; value: { close(context: Context): Promise<void> } } {
  return typeof result === "object" && result !== null && "ok" in result && result.ok === true;
}

/**
 * `target`, an environment or a handle it opened, whose every operation ends as soon as the context
 * it is given is aborted, rather than when the daemon answers. pi aborts a tool call's context when
 * the call is cut off, by an abort of its conversation or by the journal closing, and waits for the
 * call to end before either is done. But pi-env waits for the daemon's answer to a cancel, passes
 * most file operations no signal, and waits for a start whose sandbox is still being wrapped; and a
 * daemon can stop answering a request while it still answers pings, as when the host refuses it a
 * thread for a command or a file operation opens a FIFO. So a job's timeout, pause, cancel and the
 * Runner's stop wait on nothing the daemon owes. Once a call is cut off, pi takes nothing more from
 * it: an abort records the call as aborted, and a close leaves it pending, for recovery to give it
 * an interrupted result. An operation left running carries on until the sandbox closes, which stops
 * the daemon. pi-env has no option for this, so it is done here, around
 * its public `ExecutionEnv`; every operation takes its context last.
 */
function endingOnAbort<T extends object>(target: T): T {
  return new Proxy(target, {
    get(object, key) {
      const member: unknown = Reflect.get(object, key);
      if (typeof member !== "function") return member;
      return async (...args: unknown[]) => {
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- every operation's last argument is its context
        const context = args.at(-1) as Context;
        if (context.abortSignal?.aborted && !RELEASES.has(key)) return abortedResult(key);
        const operation = Promise.resolve(Reflect.apply(member, object, args));
        // An operation the abort left behind may still fail, with nobody waiting for it.
        operation.catch(() => {});
        const result: unknown = await awaitWithContext(operation, context).catch(
          (error: unknown) => {
            if (!context.abortSignal?.aborted) throw error;
            // A handle opened once its caller was cut off reaches no caller to close it, as pi-env
            // closes one opened after an abort.
            if (OPENERS.has(key)) {
              operation
                .then((opened) =>
                  isOpenedHandle(opened) ? opened.value.close(BACKGROUND_CONTEXT) : undefined,
                )
                .catch(() => {});
            }
            return abortedResult(key);
          },
        );
        return OPENERS.has(key) && isOpenedHandle(result)
          ? ok(endingOnAbort(result.value))
          : result;
      };
    },
  });
}

/**
 * The job's environment, which tells the agent when a command was stopped for using more memory
 * than the job may. A command the kernel stops for that ends with signal 9 like any other kill, so
 * the job's scope's count of such stops (`oom_kill` in its cgroup's `memory.events`) tells them
 * apart.
 */
class MemoryReportingEnv extends RemoteExecutionEnv {
  readonly #memoryLimit: string | undefined;
  readonly #scope: () => string | undefined;

  constructor(options: {
    connection: Connection;
    id: string;
    cwd: string;
    memoryLimit: string | undefined;
    scope: () => string | undefined;
  }) {
    super({ connection: options.connection, id: options.id, cwd: options.cwd });
    this.#memoryLimit = options.memoryLimit;
    this.#scope = options.scope;
  }

  override async exec(
    command: string | readonly string[],
    options: ShellExecOptions | undefined,
    context: Context,
  ): Promise<Result<ShellExecResult, ExecutionError>> {
    if (this.#memoryLimit === undefined) return super.exec(command, options, context);
    // Starts the daemon, and with it the scope, if this is the job's first request.
    await this.connection.session().catch(() => {});
    const before = await this.#oomKills();
    const result = await super.exec(command, options, context);
    const after = await this.#oomKills();
    const advice = `Files written to /tmp count against it; write large files to $TMPDIR, which is on disk.`;
    // The count is the whole job's, and the commands of one round run at once, so the message
    // names the job, not the command: every command that ended while it rose is told.
    if (before !== undefined && after !== undefined && after > before) {
      options?.onOutput?.(
        `\nWhile this command ran, a process of this job was stopped because the job's processes went over their ${this.#memoryLimit} of memory; if this command failed, that is likely why. ${advice}\n`,
        context,
        { stream: "stderr" },
      );
    } else if (!result.ok && before !== undefined && after === undefined) {
      // The scope ends with the daemon, so a daemon lost to the limit leaves no count to read.
      options?.onOutput?.(
        `\nThe job's sandbox stopped, possibly because its processes used more than the job's ${this.#memoryLimit} of memory, and starts again with the next command; processes left running are gone. ${advice}\n`,
        context,
        { stream: "stderr" },
      );
    }
    return result;
  }

  /**
   * How many processes the kernel has stopped in the job's scope for its memory limit, read from
   * the cgroup: systemd's own `OOMKills` property needs systemd 259.
   */
  async #oomKills(): Promise<number | undefined> {
    const scope = this.#scope();
    if (scope === undefined) return undefined;
    const cgroup = await this.#cgroupOf(scope);
    if (cgroup === undefined) return undefined;
    try {
      const events = await readFile(path.join("/sys/fs/cgroup", cgroup, "memory.events"), "utf8");
      return Number(/^oom_kill (\d+)$/m.exec(events)?.[1] ?? 0);
    } catch {
      // The scope has ended, and with it its cgroup.
      return undefined;
    }
  }

  /** The cgroup of the daemon's latest scope, asked of systemd once per scope. */
  #cgroup: { scope: string; path: string } | undefined;
  async #cgroupOf(scope: string): Promise<string | undefined> {
    if (this.#cgroup?.scope === scope) return this.#cgroup.path;
    const shown = await Effect.runPromise(
      runCommand(
        ChildProcess.make("systemctl", [
          "--user",
          "show",
          "--property=ControlGroup",
          "--value",
          scope,
        ]),
      ).pipe(Effect.provide(NodeServices)),
    );
    const cgroup = shown.stdout.trim();
    if (shown.failure !== undefined || !cgroup) return undefined;
    this.#cgroup = { scope, path: cgroup };
    return cgroup;
  }
}
