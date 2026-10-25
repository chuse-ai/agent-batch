# Runners are Node processes holding many agents, with each job's tools in an execution environment

A runner is a Node process that runs many jobs' agent loops at once, each on pi-durable with its journal in SQLite on local disk. The same runner image runs on hosts and on VPSes. Where a job's tools read and write files and run commands is an execution environment behind pi-durable's `ExecutionEnv` interface; the default runs them in an OS sandbox of the job's own (ADR 0027).

Measured with a scripted 30-step job of light commands on one 4-core machine, with its commands in just-bash shells, 100 agents in one Node process took about 7 MB each (831 MB in all, including four shell processes). As Durable Objects with Computer workspaces in standalone workerd, the same agents took about 23 to 26 MB each, and the jobs ran 2.8 times longer under that load. Most of the memory difference is Computer's shell, which runs each workspace's commands in its own isolate with its own copy of just-bash: a Computer workspace holding only files costs about 0.8 MB.

## Considered options

- Hosts running standalone workerd with Computer, one runtime on hosts and on Cloudflare: the memory and slowdown above, plus our own wrapper to generate workerd configs, pull images, remove leftover containers, and allow local containers through the host firewall.
- Agent loops in Durable Objects on Cloudflare: an object waiting on a model response is billed for wall-clock time at a fixed 128 MB, and pi-durable's journal is billed per row written.
- A job's tools in the runner's own process, as just-bash could run them: about 3 MB per agent, but code escaping the tools would reach the process that holds the model keys.
- One container or VM per agent: adds a separate runtime and lifecycle for every agent.

## Consequences

The hub keeps one Durable Object per runner connection (ADR 0008, ADR 0014), and hub and runner stay separate deployables (ADR 0005). In a runner, pi-durable's writes go to local disk, where no service bills them per row.
