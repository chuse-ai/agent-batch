import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { Harness } from "@earendil-works/pi-durable";
import type {
  DesiredState,
  Intent,
  JobActivity,
  JobContent,
  JobReport,
  JobUsage,
  Placement,
  PlacementRef,
} from "contract";
import { Array, Effect, Equal, Fiber, Latch, Option, Order, type Scope } from "effect";
import ensureError from "ensure-error";

import { type AgentSettings, journalExists, openJob, recordCancel } from "./agent.ts";
import { readActivity, readEvents } from "./journal.ts";

// The Runner's jobs: each placement the hub gives it runs as one supervised fiber (ADR 0022), and
// its journal's new events, its usage and what it is doing are reported to the hub. The hub's
// answer to every sync and report is the desired state, which this brings the jobs to: placements
// it does not hold start, held placements the hub cancels are stopped and their cancel recorded,
// and held placements the hub no longer lists are stopped and forgotten (ADR 0026).

/**
 * One placement the Runner holds: its job's journal and the hub's position in it, a failure its
 * journal could not record, the intent it has applied, and the latches the hub's cancelling and
 * dropping it open. Either interrupts the job's run, which leaves its work pending in the journal; a
 * cancel then settles that work and records the job's end.
 */
interface HeldJob {
  placement: PlacementRef;
  /** What the Runner has done with the placement: run it, or stopped it and recorded its cancel. */
  intent: Intent;
  /** The hub's position in the job's journal, as its last answer gave it. */
  position: number;
  /** The job's journal, from when it opens until the hub drops the placement. */
  journal?: Harness;
  /**
   * Whether the job's journal was known not to exist when this placement's run was about to open
   * it. One that may have existed may hold entries the hub lacks, whether or not it opens.
   */
  journalKnownAbsent: boolean;
  /**
   * When the job failed without its journal recording its end: its journal did not open, or the
   * Runner faulted while it ran. The journal records every other start and end itself.
   */
  failedAt?: string;
  /** When the Runner first saw each part of the job's activity as it is now (`readActivity`). */
  seen: ReadonlyMap<string, number>;
  /** The activity and usage the Runner last reported, so that a change reports again. */
  activityReported?: JobActivity;
  usageReported?: JobUsage;
  ended: boolean;
  cancelled: Latch.Latch;
  dropped: Latch.Latch;
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
  /** Each job's latest run, by job ID, until it ends and has closed the job's journal. */
  readonly #runs = new Map<string, Fiber.Fiber<void>>();
  /** The jobs this process has started, whose journals it has opened or is opening, by job ID. */
  readonly #opened = new Set<string>();
  readonly #scope: Scope.Scope;

  constructor(maxJobs: number, agent: AgentSettings, scope: Scope.Scope, onChange: () => void) {
    this.#maxJobs = maxJobs;
    this.#agent = agent;
    this.#scope = scope;
    this.#onChange = onChange;
  }

  /**
   * The Runner's load, as every sync tells the hub: what it holds and how many more it can take. A
   * placement is held until the hub's answer drops it, but an ended job occupies no slot, so the
   * report of its end already offers its slot.
   */
  load(): { held: PlacementRef[]; free: number } {
    return {
      held: [...this.#held.values()].map((job) => job.placement),
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
  ): Effect.Effect<{ jobs: JobReport[]; more: boolean }> {
    const held = [...this.#held.values()];
    return Effect.gen(function* () {
      const jobs: JobReport[] = [];
      let budget = maxBytes;
      let more = false;
      for (const job of held) {
        if (budget <= 0) {
          more = true;
          break;
        }
        const report = { ...job.placement, intent: job.intent, position: job.position };
        // Taken before the read: a failure is set after the job's last entry, so the read holds it.
        const { failedAt, journal } = job;
        if (!journal) {
          // A journal that did not exist before holds nothing of the job until its start is
          // written, which happens only once it has opened. A cancel of a job without a journal
          // records nothing, so the report alone says the job never started.
          if (failedAt && job.journalKnownAbsent) jobs.push({ ...report, events: [], failedAt });
          else if (job.intent === "cancel" && job.journalKnownAbsent) {
            jobs.push({ ...report, events: [] });
          }
          continue;
        }
        const read = yield* readOrNone("events", () => readEvents(journal, job.position, budget));
        if (Option.isNone(read)) continue;
        const { events, bytes, conversations } = read.value;
        budget -= bytes;
        more ||= read.value.more;
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
        // A cancel is reported until the hub drops the placement, as a failure is, so a journal
        // that holds no end, as one that never started its job, still says the job stopped.
        const due =
          all ||
          events.length > 0 ||
          failedAt !== undefined ||
          job.intent === "cancel" ||
          (activity !== undefined && !Equal.equals(activity, job.activityReported)) ||
          (usage !== undefined && !Equal.equals(usage, job.usageReported));
        if (!due) continue;
        jobs.push({
          ...report,
          events,
          usage,
          activity,
          failedAt: read.value.more ? undefined : failedAt,
        });
        job.activityReported = activity ?? job.activityReported;
        job.usageReported = usage ?? job.usageReported;
      }
      return { jobs, more };
    });
  }

  /**
   * Brings the jobs to `desired`, the hub's answer to a sync or a report: takes the hub's
   * positions, stops held placements no longer listed, cancels those the hub cancels, and starts
   * those not held while slots are free. A cancel of a placement the Runner does not hold, as after
   * it restarted, is recorded without running the job, and takes no slot.
   */
  reconcile(desired: DesiredState): void {
    for (const placement of desired.placements) {
      const job = this.#held.get(keyOf(placement));
      if (!job) continue;
      job.position = placement.position;
      // A job that has ended keeps its end, which reaches the hub with its events.
      if (placement.intent === "cancel" && !job.ended) job.cancelled.openUnsafe();
    }
    const listed = new Set(desired.placements.map(keyOf));
    for (const [key, job] of this.#held) {
      if (listed.has(key)) continue;
      this.#held.delete(key);
      job.dropped.openUnsafe();
    }
    for (const placement of desired.placements) {
      if (placement.intent === "cancel" && !this.#held.has(keyOf(placement))) {
        this.#start(placement, undefined);
      }
    }
    // A Runner that starts with fewer slots than the jobs placed on it, as when its allowance was
    // lowered, recovers only as many as it has slots for, those the hub has events of first, since
    // they have work to carry on. The rest stay unheld: the hub sends them again with every answer,
    // and the answer to the report of a job's end starts the next.
    const waiting = Array.sortWith(
      desired.placements.filter(
        (placement) => !this.#held.has(keyOf(placement)) && placement.intent === "run",
      ),
      (placement) => placement.position === 0,
      Order.Boolean,
    );
    for (const placement of waiting) {
      if (this.#running() >= this.#maxJobs) break;
      if (!placement.job) continue;
      this.#start(placement, placement.job);
    }
  }

  /** How many held jobs are running: an ended job occupies no slot. */
  #running(): number {
    return Array.countBy(this.#held.values(), (job) => !job.ended);
  }

  /**
   * Holds `placement` and runs its job, `job`, until the hub cancels or drops the placement. A
   * placement the hub has already cancelled, given without its job, only has its cancel recorded.
   */
  #start(placement: Placement, job: JobContent | undefined): void {
    const ref = { jobId: placement.jobId, generation: placement.generation };
    const key = keyOf(ref);
    const held: HeldJob = {
      placement: ref,
      intent: "run",
      position: placement.position,
      journalKnownAbsent: false,
      seen: new Map(),
      // A placement given without its job is only to have its cancel recorded: it takes no slot.
      ended: job === undefined,
      cancelled: Latch.makeUnsafe(job === undefined),
      dropped: Latch.makeUnsafe(false),
    };
    // Held before the fiber starts: its first report may come before the fork returns.
    this.#held.set(key, held);
    const changed = () => {
      // A placement stopped meanwhile reports nothing: the hub no longer wants it.
      if (this.#held.get(key) === held) this.#onChange();
    };
    const forget = () => {
      if (this.#held.get(key) === held) this.#held.delete(key);
    };
    const openedBefore = this.#opened.has(ref.jobId);
    if (job) this.#opened.add(ref.jobId);
    const settings = this.#agent;
    const runs = this.#runs;
    const previous = runs.get(ref.jobId);

    /** Runs the job, keeping its journal open after it ends, until interrupted. */
    const working = (content: JobContent) =>
      Effect.gen(function* () {
        // Looked up once the earlier run is done, since it may have created the journal. A journal
        // that cannot be looked up may be there.
        held.journalKnownAbsent =
          !openedBefore &&
          !(yield* journalExists(settings, ref.jobId).pipe(
            Effect.tapError((error) =>
              Effect.logError("A job's journal could not be looked up", error),
            ),
            Effect.orElseSucceed(() => true),
          ));
        const agent = {
          ...settings,
          generation: ref.generation,
          openedBefore,
          // Held as soon as it opens, so whatever fails after that is read from the journal.
          journalOpened: (journal: Harness) => {
            held.journal = journal;
          },
        };
        yield* Effect.scoped(
          Effect.gen(function* () {
            const agentRun = yield* openJob(ref.jobId, content, agent);
            changed();
            yield* agentRun;
          }).pipe(
            // A fault in the Runner itself, such as its storage failing, fails the job rather than
            // leaving it running for as long as the Runner stays connected, though its journal
            // records no end. Telling such a failure apart is planned with structured failure reasons. A stopping Runner interrupts
            // the job instead, which reports nothing.
            Effect.catchDefect((defect) =>
              Effect.logError("A job's agent failed", ensureError(defect)).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    held.failedAt = new Date().toISOString();
                  }),
                ),
              ),
            ),
            Effect.tap(() =>
              Effect.sync(() => {
                held.ended = true;
                changed();
              }),
            ),
            // The journal stays open until the hub drops the placement, which it does once it has
            // stored the job's end with every entry before it.
            Effect.andThen(Effect.never),
          ),
        );
      });

    /**
     * Records the job's cancel once its run has stopped, and keeps the journal open for its reports
     * until the hub drops the placement, which it does once it has stored the job's end.
     */
    const recording = Effect.scoped(
      Effect.gen(function* () {
        const journal = yield* recordCancel(ref.jobId, ref.generation, settings);
        held.journal = journal;
        held.journalKnownAbsent = journal === undefined;
        held.intent = "cancel";
        held.ended = true;
        changed();
        yield* held.dropped.await;
      }),
    ).pipe(
      // A cancel that cannot be recorded, as when the journal cannot be looked up or opened, is
      // tried again: the placement is forgotten, so the hub's next answer, which lists the cancel
      // again, records it anew. Its run has stopped, so it takes no slot meanwhile.
      Effect.catchCause((cause) =>
        Effect.logError("A job's cancel could not be recorded", cause).pipe(
          Effect.andThen(
            Effect.sync(() => {
              held.ended = true;
              forget();
            }),
          ),
        ),
      ),
    );

    const run = Effect.gen(function* () {
      // The hub may place a job here again while its earlier run is still closing the journal.
      if (previous) yield* Fiber.await(previous);
      // A cancel or a drop interrupts the job's run at once, which leaves its work pending in the
      // journal; recording a cancel then settles it.
      const stopped = yield* Effect.raceFirst(
        job ? working(job) : Effect.never,
        Effect.raceFirst(
          held.dropped.await.pipe(Effect.as("dropped" as const)),
          held.cancelled.await.pipe(Effect.as("cancelled" as const)),
        ).pipe(
          // Let go of the journal before the run's scope closes it, so no report reads it closed.
          Effect.tap(() =>
            Effect.sync(() => {
              held.journal = undefined;
            }),
          ),
        ),
      );
      if (stopped === "cancelled") yield* recording;
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (runs.get(ref.jobId) === fiber) runs.delete(ref.jobId);
        }),
      ),
    );
    const fiber = Effect.runSync(Effect.forkIn(run, this.#scope));
    runs.set(ref.jobId, fiber);
  }
}

function keyOf(placement: PlacementRef): string {
  return `${placement.jobId}:${placement.generation}`;
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
