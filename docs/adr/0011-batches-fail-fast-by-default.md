# Batches fail fast by default

Jobs submitted together form a batch, which carries a `failFast` policy so that a flaw shared by every job (a cap set too low, a broken prompt or contract, bad input files) is caught after a few jobs instead of after all of them. By default the first jobs of the batch (the canary) run before the rest are released, and the batch pauses when the share of failed jobs among its finished jobs exceeds the tolerance; paused batches hold their jobs that have not started, let running ones finish, and are resumed or cancelled by a person or a program. `failFast` is `true` by default, `false` to turn it off, or `{ canary, tolerance }` to tune it, where `tolerance` is a percentage of finished jobs.

Jobs stopped by a limit count as failures, because a limit that every job hits is exactly the kind of flaw this catches; jobs a person cancels do not count.

## Considered options

- Labels alone, with no batch: nothing to pause when a shared flaw shows up.
- Measuring the tolerance against the batch's total size, as AWS Step Functions does: ten percent of 500 jobs allows 50 failures before anything stops.
- A sliding window over recent jobs, by default or as an option: reacts faster to breakage late in a run, but such breakage mostly makes jobs fail quickly and cheaply, so it is left out until evidence shows it is needed.
- A separate rule for failures in a row: a second mechanism for the same late-breakage case.
- A tolerance given as a count as well as a percentage: a second form to explain, while a percentage works for batches of any size.
- `canary` and the threshold as independent top-level fields: they only make sense together, and separate fields allowed combinations such as a canary with no threshold.
