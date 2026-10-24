# Runners are Node processes holding many agents, with pluggable execution environments

> Historical design: the current OS sandbox is described in ADR 0027. Overflow, capability-based placement and remote heavy-command containers discussed here remain planned. Measurements below are development probes of the stated workloads, not general capacity guarantees.

_The default environment, the paragraphs on containment and on `python3`, `js-exec` and `sqlite3`, and the measurements of just-bash are replaced by ADR 0027; Runners as Node processes holding many agents, with pluggable environments, stand._

A runner is a Node process that runs many jobs' agent loops at once, each on pi-durable with its journal in SQLite on local disk. The same runner image runs on hosts, on VPSes, and in cloud containers when work spills over. Where a job's tools read and write files and run commands is an execution environment behind pi-durable's `ExecutionEnv` interface. Each environment declares what it can run, how durable its files are, how it isolates commands, and how files reach it, and placement matches a job's declared needs against them.

The default environment is just-bash on a real directory per job. Its shells run in separate processes started under Node's permission model, with no secrets, file access limited to the workspaces they serve, and no network. A shell reaches the network only through the runner, which applies the job's allow-list and adds secrets on the way out (ADR 0012). Heavy programs, the ones just-bash cannot run, use containers: each host has a few container slots, and a heavy command runs in a local container with the job's workspace mounted when a slot is free, otherwise it waits. Sending heavy commands to remote containers comes later, planned on Cloudflare Computer's incremental sync: a Computer workspace holding only files, and `computerd` in the container image.

A job's commands are contained in layers. just-bash interprets commands in JavaScript instead of running programs. The shell process holds no secrets, and Node's permission model limits it to its workspaces and no network. Node documents the permission model as a guard against mistakes, not malicious code: it follows symbolic links outside the granted paths and does not check file descriptors already open, and standalone workerd says the same of its isolates. These layers contain agent mistakes and instructions injected through a job's inputs; code that escaped both just-bash and the permission model would have what the runner's operating-system user has. A boundary against that is an OS sandbox or a VM around the shell processes, as workerd's documentation recommends.

just-bash's `python3`, `js-exec` and `sqlite3` stay off. They run in worker threads, which the permission model allows only with `--allow-worker`, and a process allowed to start worker threads can start one without the permission model. They also cost memory: with Python and JavaScript in two of every three commands, 25 agents peaked at about 91 MB each, against 7 MB without. Jobs run scripts as heavy programs, in containers with real Python and Node.

Measured with a scripted 30-step job of light commands on one 4-core machine, 100 agents in this shape took about 7 MB each (831 MB in all, including four shell processes), with the shells' containment confirmed by the permission model. As Durable Objects with Computer workspaces in standalone workerd, the same agents took about 23 to 26 MB each, and the jobs ran 2.8 times longer under that load. Most of the memory difference is Computer's shell, which runs each workspace's commands in its own isolate with its own copy of just-bash: a Computer workspace holding only files costs about 0.8 MB.

## Considered options

- Hosts running standalone workerd with Computer, one runtime on hosts and on Cloudflare: the memory and slowdown above, plus our own wrapper to generate workerd configs, pull images, remove leftover containers, and allow local containers through the host firewall.
- Agent loops in Durable Objects on Cloudflare: an object waiting on a model response is billed for wall-clock time at a fixed 128 MB, and pi-durable's journal is billed per row written. A runner that spills to Cloudflare runs as a container holding many agents instead.
- just-bash in the runner's own process: about 3 MB per agent, but code escaping the shell would reach the process that holds the model keys.
- One container or VM per agent: adds a separate runtime and lifecycle for every agent.

## Consequences

The runner package becomes a Node application; the Durable Object runner of the walking skeleton is replaced when runners on hosts are built. The hub is unchanged and keeps one Durable Object per runner connection (ADR 0008, ADR 0014). Hub and runner stay separate deployables (ADR 0005).

In a runner, pi-durable's writes go to local disk, so ADR 0016's patches are dropped and its one-second progress interval stays for the runner's CPU; per-row billing applies only to jobs run inside Durable Objects. Pi extensions follow the same rule as the agent loop: heavy work goes through the execution environment, not the runner process. Agents in one shell process share it, so damage from one is limited by spreading agents over several shell processes. Computer stays available as an execution environment for jobs whose workspace should live in Cloudflare.
