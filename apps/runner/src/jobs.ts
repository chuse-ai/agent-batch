import type { DesiredState, JobContent, JobReport, Placement, PlacementRef } from "contract";
import { Effect, Fiber, type Scope } from "effect";

// The Runner's jobs: each placement the hub gives it runs as one supervised fiber (ADR 0022), and
// what becomes of it is reported to the hub with the next sync. The hub's answer to every sync is
// the desired state, which this brings the jobs to: placements it does not hold start, and held
// placements the hub no longer lists are stopped and forgotten (ADR 0026).

/** One placement the Runner holds: its job's fiber once forked, and whether the job has ended. */
interface HeldJob {
  placement: PlacementRef;
  fiber: Fiber.Fiber<void> | undefined;
  ended: boolean;
}

/**
 * Runs a job's agent. For now a stub that returns the prompt as the result, in place of Pi, so the
 * path from placement to result runs end to end; the agent loop on pi-durable replaces it.
 */
function runAgent(job: JobContent): Effect.Effect<unknown> {
  return Effect.succeed({ echo: job.prompt });
}

/**
 * Runs the Runner's jobs, up to `maxJobs` at once, in `scope`, and collects their reports. Closing
 * the scope stops every job.
 */
export class JobRunner {
  readonly #maxJobs: number;
  /** Called when a job ends, so the Runner reports it and takes more work. */
  readonly #onChange: () => void;
  readonly #held = new Map<string, HeldJob>();
  #reports: JobReport[] = [];
  readonly #scope: Scope.Scope;

  constructor(maxJobs: number, scope: Scope.Scope, onChange: () => void) {
    this.#maxJobs = maxJobs;
    this.#scope = scope;
    this.#onChange = onChange;
  }

  /**
   * The Runner's load, as every sync tells the hub: what it holds and how many more it can take. A
   * placement is held until the hub's answer drops it, but an ended job occupies no slot, so the
   * report of its end already offers its slot.
   */
  load(): { held: PlacementRef[]; free: number } {
    const jobs = [...this.#held.values()];
    const running = jobs.filter((job) => !job.ended).length;
    return { held: jobs.map((job) => job.placement), free: Math.max(0, this.#maxJobs - running) };
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

  /** Brings the jobs to `desired`: starts placements not held, stops held ones no longer listed. */
  reconcile(desired: DesiredState): void {
    const listed = new Set(desired.placements.map(keyOf));
    for (const [key, job] of this.#held) {
      if (listed.has(key)) continue;
      this.#held.delete(key);
      if (job.fiber) Effect.runFork(Fiber.interrupt(job.fiber));
    }
    for (const placement of desired.placements) {
      if (this.#held.has(keyOf(placement)) || placement.intent !== "run") continue;
      if (!placement.job) continue;
      this.#start(placement, placement.job);
    }
  }

  #start(placement: Placement, job: JobContent): void {
    const ref = { jobId: placement.jobId, generation: placement.generation };
    const key = keyOf(ref);
    const held: HeldJob = { placement: ref, fiber: undefined, ended: false };
    // Held before the fiber starts: its first report may come before the fork returns.
    this.#held.set(key, held);
    const report = (jobReport: JobReport) => {
      // A placement stopped meanwhile reports nothing: the hub no longer wants it.
      if (this.#held.get(key) !== held) return;
      this.#reports.push(jobReport);
      this.#onChange();
    };
    const run = Effect.gen(function* () {
      report({ ...ref, intent: "run", outcome: { state: "running", startedAt: isoNow() } });
      const result = yield* runAgent(job);
      held.ended = true;
      report({ ...ref, intent: "run", outcome: { state: "succeeded", result, endedAt: isoNow() } });
    });
    held.fiber = Effect.runSync(Effect.forkIn(run, this.#scope));
  }
}

function keyOf(placement: PlacementRef): string {
  return `${placement.jobId}:${placement.generation}`;
}

function isoNow(): string {
  return new Date().toISOString();
}
