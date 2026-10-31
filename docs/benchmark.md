# Benchmark: one Runner against one container per job

How many agent jobs one small server holds, and what each costs in CPU, when the agent loops share a Runner process instead of each getting a container. Measured on 2026-10-25 and 26 with the Runner from agent-batch 0.2.0, its model calls pointed at the replay server described below. Later releases raised the Runner's throughput, so these figures are conservative for current versions.

## Setup

- **Server:** a Hetzner CAX11: arm64 (Neoverse-N1), 2 vCPU, 4 GB RAM, Debian 13, about €7 a month. One Runner, with real commands in its per-job OS sandboxes.
- **Hub:** on another machine, so the server's figures are the Runner's alone.
- **Model:** each workload's jobs were run once against a real model (`gpt-6-luna`, 8 jobs at once) and recorded. In the benchmark a replay server on another machine answered every model call with the recorded response at the recorded timing, or slowed down to stand for a slower model. Tools and sandboxes ran for real. A real endpoint cannot be the load source: the one used was full at about 8 jobs at once, and at 1,000 at once its answers changed. Accuracy was measured against the real model at a modest level; the framework's cost, by replay.
- **Baseline:** the Runner's own image as one container per job (`RUNNER_MAX_JOBS=1`, `--memory 1g`) on the same server. One job per container is the fair comparison; the tables also give 3 jobs per container in turn, which spreads a container's start cost.

## Workloads

| Workload          | What each job does                                                  | Pool         | Model turns per job |
| ----------------- | ------------------------------------------------------------------- | ------------ | ------------------- |
| A, extraction     | Read one generated invoice, CV or ticket and submit JSON            | 60 documents | 2                   |
| B, data scripting | Write and run a Python script over a 20,000–60,000-line messy CSV   | 20 files     | 4.4                 |
| C, code fixes     | Fix a Python project with one mutated operator until its tests pass | 31 projects  | 6.9                 |
| D, large data     | B's task on 30–46 MB CSVs                                           | 8 files      | 4.8                 |

Each workload ramped up to its break point, then a coarse search found its limit.

## Results

| Workload | Limit, jobs at once                       | What broke first                            | Memory per open job                 | Host CPU-s per job                        | Peak throughput       | Accuracy     |
| -------- | ----------------------------------------- | ------------------------------------------- | ----------------------------------- | ----------------------------------------- | --------------------- | ------------ |
| A        | 350; 425 with systemd's `TasksMax` raised | systemd's task cap, then memory             | 6.5 MiB                             | 0.20 (recorded speed) – 0.33 (20× slower) | 6.8 jobs/s at 100–200 | 87% of facts |
| B        | about 75 allowed (64 open)                | CPU, from about 40                          | 20 MiB                              | 1.05–1.17                                 | 1.6 jobs/s at 75      | 70%          |
| C        | about 400                                 | CPU from about 100; memory near full at 400 | 6.8 MiB                             | 0.70–0.88                                 | 2.2 jobs/s at 100     | 90%          |
| D        | about 12                                  | CPU                                         | up to 320 MiB while its script runs | 7.3–7.8                                   | 0.2 jobs/s            | 75%          |

Accuracy measures the model on these tasks, not agent-batch.

## 500 with the Runner from 0.5.0

On 2026-10-29 workload A's 20 recorded invoices, repeated to 500 jobs, ran again with the Runner from agent-batch 0.5.0, the model replayed 20 times slower, to find the largest level that runs clean on the same server. For these runs the server was stripped of its own services, leaving about 440 MiB used at idle, and the Runner's process cap was lifted (`--pids-limit` for the container, `TasksMax` for the plain process).

| Jobs | Runner              | Result                                                                                                 | Server memory at most | CPU at most |
| ---- | ------------------- | ------------------------------------------------------------------------------------------------------ | --------------------- | ----------- |
| 500  | in Docker           | all succeeded, no call differed from its recording, none killed for memory; 495 at once after the Canary | 3,697 of 3,817 MiB    | 96%         |
| 500  | plain process       | the same                                                                                               | 3,720 of 3,817 MiB    | 91%         |
| 550  | plain process       | not clean: memory ran out, the kernel killed job processes, and 224 jobs had tool calls fail               | 3,761 of 3,817 MiB    | 98%         |

500 was the largest clean level tested, with about 100 MiB to spare, so not a level to run at. Containers were not measured on 0.5.0; scaling their limit of about 30 by the Runner's gain (425 to 495) gives about 35, the figure the README compares against.

## Against one container per job

| Measure                                      | One Runner | Containers                       | Ratio             |
| -------------------------------------------- | ---------- | -------------------------------- | ----------------- |
| A, jobs open at once (model 20× slower)      | 350–425    | about 30, then the host thrashed | 12–14×            |
| A, memory per open job                       | 6.5 MiB    | about 90 MiB                     | 14×               |
| A, host CPU-s per job (model 20× slower)     | 0.33       | 1.50                             | 4.5×              |
| B, jobs open at once                         | 64         | about 25                         | 2.6×              |
| B, host CPU-s per job, one job per container | 1.05–1.17  | 4.0–4.1                          | 3.6×              |
| C, host CPU-s per job, one job per container | 0.73       | 3.6                              | 4.9×              |
| D, host CPU-s per job, one job per container | 7.3        | 10.5–10.9                        | 1.45×             |
| B, C and D with 3 jobs per container in turn |            |                                  | 2×, 2.6× and 1.2× |

A container costs about 75–80 MiB for its own Runner before its job does anything, and about 3 CPU-seconds to start and run, measured with one B, C or D job per container. So the gap is widest when jobs mostly wait on the model and narrows towards the jobs' own work when they compute: D, bound by its own scripts, gains least. Containers also failed worse at the edge: one too many hung the host for about 30 minutes instead of failing a job.

## Throughput and density

At the recorded model speed the host's CPU caps throughput early, so holding more jobs helps only until the CPU is full (A: 5.3 jobs/s at 50 allowed, 6.8 at 200, 4.6 at 400). With a slower model, jobs mostly wait, and the number of jobs to hold is throughput times job duration: replayed 20 times slower, A's throughput kept rising to 2.9 jobs/s at 400 open. Density matters for slow models and long jobs; CPU per job matters for fast ones.

## Limits of these numbers

- These are particular workloads on one machine, not a capacity guarantee. The job sandboxes, the model's speed and the jobs' own work set the limits.
- Under replay a job whose sandbox failed would still finish, since its recorded turn is answered regardless. Tool failures were counted from the replay server's log of tool results that differed from the recording.
- Defaults broke before the hardware did: systemd's `TasksMax`, the WebSocket library's maximum message size and Node's maximum string length. Later releases fixed the ones in agent-batch's own code; `TasksMax` is a host setting, to raise for several hundred jobs at once.
- The benchmark harness is not published.
