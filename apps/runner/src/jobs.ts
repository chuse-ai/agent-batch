import type { DesiredState, JobContent, JobReport, Placement, PlacementRef } from "contract";
import { Array, Effect, Fiber, Latch, Order, type Scope } from "effect";
import ensureError from "ensure-error";

import { type AgentSettings, hasJournal, runAgent } from "./agent.ts";

// The Runner's jobs: each placement the hub gives it runs as one supervised fiber (ADR 0022), and
// what becomes of it is reported to the hub with the next sync. The hub's answer to every sync is
// the desired state, which this brings the jobs to: placements it does not hold start, and held
// placements the hub no longer lists are stopped and forgotten (ADR 0026).

/**
 * One placement the Runner holds: whether its job has ended, and the latch the hub's dropping it
 * opens, which aborts the job's agent.
 */
interface HeldJob {
  placement: PlacementRef;
  ended: boolean;
  dropped: Latch.Latch;
}

/**
 * Runs the Runner's jobs, up to `maxJobs` at once, in `scope`, and collects their reports. Closing
 * the scope stops every job.
 */
export class JobRunner {
  readonly #maxJobs: number;
  readonly #agent: AgentSettings;
  /** Called when a job ends, so the Runner reports it and takes more work. */
  readonly #onChange: () => void;
  readonly #held = new Map<string, HeldJob>();
  /** Each job's latest run, by job ID, until it ends and has closed the job's journal. */
  readonly #runs = new Map<string, Fiber.Fiber<void>>();
  #reports: JobReport[] = [];
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

  /** The reports collected since the last take, which the caller sends or returns. */
  takeReports(): JobReport[] {
    const reports = this.#reports;
    this.#reports = [];
    return reports;
  }

  /** Puts back reports whose sending failed, ahead of any collected since. */
  returnReports(reports: JobReport[]): void {
    this.#reports = [...reports, ...this.#reports];
  }

  /**
   * Brings the jobs to `desired`: stops held placements no longer listed, and starts those not held
   * while slots are free.
   */
  reconcile(desired: DesiredState): void {
    const listed = new Set(desired.placements.map(keyOf));
    for (const [key, job] of this.#held) {
      if (listed.has(key)) continue;
      this.#held.delete(key);
      job.dropped.openUnsafe();
    }
    // A Runner that starts with fewer slots than the jobs placed on it, as when its allowance was
    // lowered, continues only as many as it has slots for, those with a journal first, since they
    // have work to continue. The rest stay unheld: the hub sends them again with every answer, and
    // the answer to the report of a job's end starts the next.
    const waiting = desired.placements.filter(
      (placement) => !this.#held.has(keyOf(placement)) && placement.intent === "run",
    );
    const lacksJournal = (placement: Placement) =>
      !hasJournal(this.#agent.jobsDir, placement.jobId);
    for (const placement of Array.sortWith(waiting, lacksJournal, Order.Boolean)) {
      if (this.#running() >= this.#maxJobs) break;
      if (!placement.job) continue;
      this.#start(placement, placement.job);
    }
  }

  /** How many held jobs are running: an ended job occupies no slot. */
  #running(): number {
    return Array.countBy(this.#held.values(), (job) => !job.ended);
  }

  #start(placement: Placement, job: JobContent): void {
    const ref = { jobId: placement.jobId, generation: placement.generation };
    const key = keyOf(ref);
    const held: HeldJob = { placement: ref, ended: false, dropped: Latch.makeUnsafe(false) };
    // Held before the fiber starts: its first report may come before the fork returns.
    this.#held.set(key, held);
    const report = (jobReport: JobReport) => {
      // A placement stopped meanwhile reports nothing: the hub no longer wants it.
      if (this.#held.get(key) !== held) return;
      this.#reports.push(jobReport);
      this.#onChange();
    };
    const agent = { ...this.#agent, dropped: held.dropped.await };
    const runs = this.#runs;
    const previous = runs.get(ref.jobId);
    const run = Effect.gen(function* () {
      // The hub may place a job here again while its dropped run is still closing the journal.
      if (previous) yield* Fiber.await(previous);
      report({ ...ref, intent: "run", outcome: { state: "running", startedAt: isoNow() } });
      const outcome = yield* runAgent(ref.jobId, job, agent).pipe(
        // A fault in the Runner itself, such as its storage failing, fails the job rather than
        // leaving it running for as long as the Runner stays connected: no outcome hands a job back
        // to the hub. Telling such a failure apart is planned with structured failure reasons. A stopping Runner interrupts the job
        // instead, which reports nothing.
        Effect.catchDefect((defect) =>
          Effect.logError("A job's agent failed", ensureError(defect)).pipe(
            Effect.as({ state: "failed" } as const),
          ),
        ),
      );
      held.ended = true;
      report({ ...ref, intent: "run", outcome: { ...outcome, endedAt: isoNow() } });
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

function isoNow(): string {
  return new Date().toISOString();
}
