import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Harness } from "@earendil-works/pi-durable";
import type {
  Run,
  RunRef,
  DesiredState,
  Intent,
  JobActivity,
  JobContent,
  JobFailure,
  JobReport,
  JobUsage,
  RefusedReport,
} from "contract";
import { Array, Effect, Equal, Fiber, Latch, Option, Order, Result, type Scope } from "effect";
import ensureError from "ensure-error";

import {
  type AgentSettings,
  journalExists,
  openJob,
  recordStop,
  deleteJobFiles,
  type Stop,
} from "./agent.ts";
import { downloadInputFiles } from "./input-files.ts";
import { failureMessage, readActivity, readEvents } from "./journal.ts";

// The Runner's jobs: each run the hub gives it runs as one supervised fiber (ADR 0022), and
// its journal's new events, its usage and what it is doing are reported to the hub. The hub's
// answer to every sync and report is the desired state, which this brings the jobs to: runs
// it does not hold start, held runs the hub pauses or cancels are stopped and their pause or
// cancel recorded, held runs the hub no longer lists are stopped and forgotten, and
// the files of jobs the hub has deleted are deleted (ADR 0026).

/**
 * One run the Runner holds: its job's journal and the hub's position in it, a failure its
 * journal could not record, the intent it has applied, and the latches the hub's pausing or
 * cancelling and its dropping it open. Either interrupts the job's agent, which leaves its work
 * pending in the journal; a pause or cancel then settles that work and is recorded as the
 * run's closing.
 */
interface HeldJob {
  run: RunRef;
  /** What the Runner has done with the run: run it, or stopped it and recorded why. */
  intent: Intent;
  /** The stop the hub asks for, the latest it gave, once it asks for one. */
  stop?: Stop;
  /** The hub's position in the job's journal, as its last answer gave it. */
  position: number;
  /** The job's journal, from when it opens until the hub drops the run. */
  journal?: Harness;
  /**
   * Whether the job's journal was known not to exist when this run was about to open it. One that
   * may have existed may hold entries the hub lacks, whether or not it opens. Only a run that knows
   * it absent downloads the job's Input files, since an existing journal means the agent may have
   * changed them.
   */
  journalKnownAbsent: boolean;
  /**
   * The job's failure its journal did not record: its journal did not open, or the Runner faulted
   * while it ran. The journal records every other start and end itself.
   */
  failure?: JobFailure;
  /** When the Runner first saw each part of the job's activity as it is now (`readActivity`). */
  seen: ReadonlyMap<string, number>;
  /** The activity and usage the Runner last reported, so that a change reports again. */
  activityReported?: JobActivity;
  usageReported?: JobUsage;
  ended: boolean;
  /** Why the hub refused the job's last report, until it applies one. */
  refused?: string;
  /** The number of the read that last read the job's report, so refused jobs take turns. */
  lastRead: number;
  stopping: Latch.Latch;
  dropped: Latch.Latch;
}

/** A deleted job's files the hub asks the Runner to delete, and whether they are gone. */
interface Deletion {
  done: boolean;
}

/**
 * Runs the Runner's jobs, up to `maxJobs` at once, in `scope`, and reads their reports from their
 * journals. Closing the scope stops every job.
 */
export class JobRunner {
  readonly #maxJobs: number;
  readonly #agent: AgentSettings;
  /** Called when a job starts or ends, so the Runner reports it and takes more work. */
  readonly #onChange: () => void;
  readonly #held = new Map<string, HeldJob>();
  /** The fiber of each job's latest run, by job ID, until it ends and has closed the job's journal. */
  readonly #fibers = new Map<string, Fiber.Fiber<void>>();
  /**
   * The jobs whose journals this process has opened, by job ID: one opened again is not recovered
   * from a restart. A job whose run stopped before its journal opened, as while its Input files
   * downloaded, has none, so its next run downloads them.
   */
  readonly #opened = new Set<string>();
  /** The deletions the hub lists, by job ID, until it stops listing them. */
  readonly #deletions = new Map<string, Deletion>();
  readonly #scope: Scope.Scope;
  /** How many jobs' reports have been read, which numbers each read. */
  #reads = 0;

  constructor(maxJobs: number, agent: AgentSettings, scope: Scope.Scope, onChange: () => void) {
    this.#maxJobs = maxJobs;
    this.#agent = agent;
    this.#scope = scope;
    this.#onChange = onChange;
  }

  /**
   * The Runner's load, as every sync tells the hub: what it holds and how many more it can take. A
   * run is held until the hub's answer drops it, but an ended job occupies no slot, so the
   * report of its end already offers its slot.
   */
  load(): { held: RunRef[]; free: number } {
    return {
      held: [...this.#held.values()].map((job) => job.run),
      free: Math.max(0, this.#maxJobs - this.#running()),
    };
  }

  /**
   * Reads a report for each held job with something new, or for every held job when `all`, and
   * records what it read: its journal's events after the hub's position, which include the job's
   * start and end, its usage and activity, and a failure its journal could not record. Each is read
   * fresh, so a report whose answer was lost is read again from the same position. Nothing records
   * what the hub applied: the hub's position covers only its events, and a hub whose database went
   * back, as after a failover, can lose an activity, usage or failure without its position showing
   * it. So a report carries them whenever they can be read, a failed job reports until the hub
   * drops it, and `all` sends every job's again. The events of all jobs come to about `maxBytes` of
   * JSON Lines; `more` says some were left for the next report.
   *
   * A failure goes only with a report that reaches the end of the journal, or alone for a job whose
   * journal was known not to exist before it failed to open, so the hub never ends a job without an
   * entry its journal holds. A job whose events cannot be read, as when its storage fails, reports
   * nothing: the hub keeps what it last had. pi fails a journal for good on its first storage error,
   * so only the Runner's restart, which opens it again, tries again. An activity or usage that cannot
   * be read is left out.
   *
   * It records when it first saw each part of a job's activity, and the activity and usage it
   * reports, so that only a change reports again. They are recorded before the hub answers, which
   * loses nothing: a request that fails drops the connection, and a new connection's first reports
   * carry every held job.
   */
  readAndRecordReports(
    maxBytes: number,
    all: boolean,
  ): Effect.Effect<{ jobs: JobReport[]; deleted: string[]; more: boolean }> {
    // A job whose report the hub refused is read only in the pass that reads every job, after the
    // rest and with the budget they leave, so its backlog cannot hold up theirs. The refused
    // ones go least recently read first, so one whose next event alone fills the budget cannot keep
    // the others from ever being tried again.
    const [ready, refused] = Array.partition(this.#held.values(), (job) =>
      job.refused === undefined ? Result.succeed(job) : Result.fail(job),
    );
    const held = all
      ? [...ready, ...Array.sortWith(refused, (job) => job.lastRead, Order.Number)]
      : ready;
    // A deletion is reported done until the hub stops listing it, so the hub that missed one hears of
    // it again.
    const deleted = [...this.#deletions].flatMap(([jobId, deletion]) =>
      deletion.done ? [jobId] : [],
    );
    return Effect.gen({ self: this }, function* () {
      const jobs: JobReport[] = [];
      let budget = maxBytes;
      let more = false;
      for (const job of held) {
        if (budget <= 0) {
          // A refused job left unread asks for no further pass, which would send refused jobs'
          // backlogs back to back. So a healthy job's event larger than the budget, read at the end
          // of the pass over every job, leaves the refused ones to the next such pass.
          more ||= job.refused === undefined;
          break;
        }
        // Numbered only once read: a refused job the budget did not reach goes first next time.
        job.lastRead = ++this.#reads;
        const report = { ...job.run, intent: job.intent, position: job.position };
        // Taken before the read: a failure is set after the job's last entry, so the read holds it.
        const { failure, journal } = job;
        if (!journal) {
          // A journal that did not exist before holds nothing of the job until its start is
          // written, which happens only once it has opened. A pause or cancel of a job without a
          // journal records nothing, so the report alone says the job never started.
          if (failure && job.journalKnownAbsent) jobs.push({ ...report, events: [], failure });
          else if (job.intent !== "run" && job.journalKnownAbsent) {
            jobs.push({ ...report, events: [] });
          }
          continue;
        }
        const read = yield* readOrNone("events", () => readEvents(journal, job.position, budget));
        if (Option.isNone(read)) continue;
        const { events, bytes, conversations } = read.value;
        budget -= bytes;
        more ||= read.value.more && job.refused === undefined;
        const observed = Option.getOrUndefined(
          yield* readOrNone("activity", () => readActivity(journal, conversations, job.seen)),
        );
        if (observed) job.seen = observed.seen;
        const activity = observed?.activity;
        // Read outside the journal's commit, where pi's other reads would wait on it forever. A
        // tool's own model calls add to it with no entry, so it can change on its own.
        const usage = Option.getOrUndefined(
          yield* readOrNone("usage", () => journal.usage(BACKGROUND_CONTEXT)),
        );
        // A pause or cancel is reported until the hub drops the run, as a failure is, so a
        // journal that holds no closing, as one that never started its job, still says it stopped.
        const due =
          all ||
          events.length > 0 ||
          failure !== undefined ||
          job.intent !== "run" ||
          (activity !== undefined && !Equal.equals(activity, job.activityReported)) ||
          (usage !== undefined && !Equal.equals(usage, job.usageReported));
        if (!due) continue;
        jobs.push({
          ...report,
          events,
          usage,
          activity,
          failure: read.value.more ? undefined : failure,
        });
        job.activityReported = activity ?? job.activityReported;
        job.usageReported = usage ?? job.usageReported;
      }
      return { jobs, deleted, more };
    });
  }

  /**
   * Records which of the `reported` jobs' reports the hub `refused`. A refused job stays held
   * and running, since the hub could not read its report, not that the job went wrong, and is read
   * again only in the pass that reads every job; one reported again without a refusal is clear.
   * Logs a refusal only when its reason is new, rather than in every pass that reads the job.
   */
  recordRefusals(
    reported: readonly RunRef[],
    refused: readonly RefusedReport[],
  ): Effect.Effect<void> {
    const refusals = new Map(refused.map((refusal) => [refusal.runId, refusal]));
    return Effect.forEach(
      reported,
      (ref) => {
        const job = this.#held.get(ref.runId);
        if (!job) return Effect.void;
        // The reported jobs are whole reports, so the log takes the hub's refusal instead.
        const refusal = refusals.get(ref.runId);
        const isNew = refusal !== undefined && refusal.reason !== job.refused;
        job.refused = refusal?.reason;
        return isNew ? Effect.logWarning("The hub refused a job's report", refusal) : Effect.void;
      },
      { discard: true },
    );
  }

  /**
   * Brings the jobs to `desired`, the hub's answer to a sync or a report: takes the hub's
   * positions, stops held runs no longer listed, pauses or cancels those the hub pauses or
   * cancels, deletes the files of jobs the hub has deleted, and starts those not held while slots
   * are free. A pause or cancel of a run the Runner does not hold, as after it restarted, is
   * recorded without running the job, and takes no slot.
   */
  reconcile(desired: DesiredState): void {
    for (const run of desired.runs) {
      const job = this.#held.get(run.runId);
      if (!job) continue;
      job.position = run.position;
      // A job that has ended keeps its end, which reaches the hub with its events. A cancel that
      // follows a pause the Runner has not yet recorded replaces it.
      if (isStop(run.intent) && !job.ended) {
        job.stop = run.intent;
        job.stopping.openUnsafe();
      }
    }
    // A run no longer listed stops, closing its journal; a deleted job's does so before its
    // files go.
    const listed = new Set(desired.runs.map((run) => run.runId));
    for (const [key, job] of this.#held) {
      if (listed.has(key)) continue;
      this.#held.delete(key);
      job.dropped.openUnsafe();
    }
    for (const run of desired.runs) {
      if (isStop(run.intent) && !this.#held.has(run.runId)) {
        this.#start(run, undefined);
      }
    }
    for (const jobId of desired.deletions) {
      if (!this.#deletions.has(jobId)) this.#deleteFiles(jobId);
    }
    const deleting = new Set(desired.deletions);
    for (const jobId of this.#deletions.keys()) {
      if (!deleting.has(jobId)) this.#deletions.delete(jobId);
    }
    // A Runner that starts with fewer slots than the jobs placed on it, as when its allowance was
    // lowered, recovers only as many as it has slots for, those the hub has events of first, since
    // they have work to carry on. The rest stay unheld: the hub sends them again with every answer,
    // and the answer to the report of a job's end starts the next.
    const waiting = Array.sortWith(
      desired.runs.filter((run) => !this.#held.has(run.runId) && run.intent === "run"),
      (run) => run.position === 0,
      Order.Boolean,
    );
    for (const run of waiting) {
      if (this.#running() >= this.#maxJobs) break;
      if (!run.job) continue;
      this.#start(run, run.job);
    }
  }

  /**
   * Deletes the deleted job's files once its last run has closed them, and reports the deletion done.
   * A deletion that fails is logged and forgotten, so the hub's next answer, which lists the deletion
   * again, tries again.
   */
  #deleteFiles(jobId: string): void {
    const deletion: Deletion = { done: false };
    this.#deletions.set(jobId, deletion);
    this.#opened.delete(jobId);
    const deletions = this.#deletions;
    const onChange = this.#onChange;
    const settings = this.#agent;
    const previous = this.#fibers.get(jobId);
    const deleting = Effect.gen(function* () {
      if (previous) yield* Fiber.await(previous);
      yield* deleteJobFiles(settings, jobId);
      deletion.done = true;
      if (deletions.get(jobId) === deletion) onChange();
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("A deleted job's files could not be deleted", cause).pipe(
          Effect.andThen(
            Effect.sync(() => {
              if (deletions.get(jobId) === deletion) deletions.delete(jobId);
            }),
          ),
        ),
      ),
    );
    Effect.runSync(Effect.forkIn(deleting, this.#scope));
  }

  /** How many held jobs are running: an ended job occupies no slot. */
  #running(): number {
    return Array.countBy(this.#held.values(), (job) => !job.ended);
  }

  /**
   * Holds `run` and runs its job, `job`, until the hub pauses, cancels or drops the run.
   * A run the hub has already paused or cancelled, given without its job, only has that
   * recorded.
   */
  #start(run: Run, job: JobContent | undefined): void {
    const ref = { jobId: run.jobId, runId: run.runId };
    const key = ref.runId;
    const held: HeldJob = {
      run: ref,
      intent: "run",
      position: run.position,
      journalKnownAbsent: false,
      seen: new Map(),
      lastRead: 0,
      // A run given without its job is only to have its pause or cancel recorded: it takes no
      // slot.
      ended: job === undefined,
      ...(isStop(run.intent) ? { stop: run.intent } : {}),
      stopping: Latch.makeUnsafe(job === undefined),
      dropped: Latch.makeUnsafe(false),
    };
    // Held before the fiber starts: its first report may come before the fork returns.
    this.#held.set(key, held);
    const changed = () => {
      // A run no longer held reports nothing: the hub has dropped it, or lists it again to be
      // held anew.
      if (this.#held.get(key) === held) this.#onChange();
    };
    const forget = () => {
      if (this.#held.get(key) === held) this.#held.delete(key);
    };
    const opened = this.#opened;
    const settings = this.#agent;
    const fibers = this.#fibers;
    const previous = fibers.get(ref.jobId);

    /** Fails the job for a fault in the Runner itself, with the fault's `message`. */
    const runFaulted = (message: string) =>
      Effect.sync(() => {
        held.failure = {
          timestamp: Date.now(),
          reason: "runFaulted",
          message: failureMessage(message),
        };
      });

    /** Runs the job, keeping its journal open after it ends, until interrupted. */
    const working = (content: JobContent) =>
      Effect.gen(function* () {
        // Looked up once the earlier run is done, since it may have opened the journal. A journal
        // that cannot be looked up may be there.
        const openedBefore = opened.has(ref.jobId);
        held.journalKnownAbsent =
          !openedBefore &&
          !(yield* journalExists(settings, ref.jobId).pipe(
            Effect.tapError((error) =>
              Effect.logError("A job's journal could not be looked up", error),
            ),
            Effect.orElseSucceed(() => true),
          ));
        // A job's Input files are downloaded on its first run, before its journal exists: a journal
        // that exists means they were, and the agent may have changed them since. The job holds its
        // slot meanwhile. A download that fails fails the job, as a journal that does not open does.
        if (held.journalKnownAbsent) {
          yield* downloadInputFiles(settings, ref.jobId, content.files).pipe(
            // The failure is reported until the hub drops the run.
            Effect.catchTag("InputFileFailed", (error) =>
              Effect.logError("A job's Input files could not be downloaded", error.message).pipe(
                Effect.andThen(runFaulted(error.message)),
                Effect.andThen(
                  Effect.sync(() => {
                    held.ended = true;
                    changed();
                  }),
                ),
                Effect.andThen(Effect.never),
              ),
            ),
          );
        }
        const agent = {
          ...settings,
          runId: ref.runId,
          runTimeoutSeconds: run.runTimeoutSeconds,
          modelBudgetUsd: run.modelBudgetUsd,
          openedBefore,
          // Held as soon as it opens, so whatever fails after that is read from the journal.
          journalOpened: (journal: Harness) => {
            opened.add(ref.jobId);
            held.journal = journal;
          },
        };
        yield* Effect.scoped(
          Effect.gen(function* () {
            const runAgent = yield* openJob(ref.jobId, content, agent);
            changed();
            yield* runAgent;
          }).pipe(
            // A fault in the Runner itself, such as its storage failing, fails the job rather than
            // leaving it running for as long as the Runner stays connected, though its journal
            // records no end. A stopping Runner interrupts the job instead, which reports nothing.
            Effect.catchDefect((defect) => {
              const error = ensureError(defect);
              return Effect.logError("A job's agent failed", error).pipe(
                Effect.andThen(runFaulted(error.message)),
              );
            }),
            Effect.tap(() =>
              Effect.sync(() => {
                held.ended = true;
                changed();
              }),
            ),
            // The journal stays open until the hub drops the run, which it does once it has
            // stored the job's end with every entry before it.
            Effect.andThen(Effect.never),
          ),
        );
      });

    /**
     * Records the job's pause or cancel, `stop`, once its agent has stopped, and keeps the journal
     * open for its reports until the hub drops the run, which it does once it has stored it.
     */
    const recording = (stop: Stop) =>
      Effect.scoped(
        Effect.gen(function* () {
          const journal = yield* recordStop(ref.jobId, ref.runId, stop, settings);
          // A job without a journal never started, and the hub forgets which Runner held it once
          // told so; but its Input files' download may have written files already, so they go
          // first. A deletion that fails is tried again with the rest.
          if (journal === undefined) yield* deleteJobFiles(settings, ref.jobId);
          held.journal = journal;
          held.journalKnownAbsent = journal === undefined;
          held.intent = stop;
          held.ended = true;
          changed();
          yield* held.dropped.await;
        }),
      ).pipe(
        // A pause or cancel that cannot be recorded, as when the journal cannot be looked up or
        // opened, is tried again: the run is forgotten, so the hub's next answer, which lists
        // it again, records it anew. Its agent has stopped, so it takes no slot meanwhile.
        Effect.catchCause((cause) =>
          Effect.logError("A job's pause or cancel could not be recorded", cause).pipe(
            Effect.andThen(
              Effect.sync(() => {
                held.ended = true;
                forget();
              }),
            ),
          ),
        ),
      );

    const runUntilStopped = Effect.gen(function* () {
      // The hub may place a job here again while its earlier run is still closing the journal.
      if (previous) yield* Fiber.await(previous);
      // A pause, a cancel or a drop interrupts the job's agent at once, which leaves its work pending
      // in the journal; recording a pause or cancel then settles it.
      const stopped = yield* Effect.raceFirst(
        job ? working(job) : Effect.never,
        Effect.raceFirst(
          held.dropped.await.pipe(Effect.as("dropped" as const)),
          held.stopping.await.pipe(Effect.as("stopping" as const)),
        ).pipe(
          // Let go of the journal before the agent's scope closes it, so no report reads it closed.
          Effect.tap(() =>
            Effect.sync(() => {
              held.journal = undefined;
            }),
          ),
        ),
      );
      // The latest stop the hub asked for, read once the agent has stopped.
      if (stopped === "stopping" && held.stop) yield* recording(held.stop);
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (fibers.get(ref.jobId) === fiber) fibers.delete(ref.jobId);
        }),
      ),
    );
    const fiber = Effect.runSync(Effect.forkIn(runUntilStopped, this.#scope));
    fibers.set(ref.jobId, fiber);
  }
}

function isStop(intent: Intent): intent is Stop {
  return intent === "pause" || intent === "cancel";
}

/**
 * Reads `part` of a report of a job from its journal, or nothing when it cannot be read, as when
 * the journal's storage fails; the error is logged, and the Runner's other jobs report on.
 */
function readOrNone<A>(part: string, read: () => Promise<A>): Effect.Effect<Option.Option<A>> {
  return Effect.tryPromise(read).pipe(
    Effect.tapError((error) =>
      Effect.logError(`A job's ${part} could not be read`, ensureError(error.cause)),
    ),
    Effect.option,
  );
}
