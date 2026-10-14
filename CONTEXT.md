# agent-batch

Runs many one-off AI agent jobs unattended, each returning a result that meets a declared contract, on owned machines first and on Cloudflare when they are full.

## Language

**Job**:
One self-contained piece of agent work: a task, its inputs (including any scripts, sent as data), and an output contract. Runs once, with no ongoing conversation.
_Avoid_: task, run, session, conversation

**Output contract**:
The schema a job's result must satisfy before the job counts as succeeded.
_Avoid_: output format, response schema

**Hub**:
The single central service that accepts jobs, keeps the record of every job and runner, and decides where each job runs.
_Avoid_: control, control plane, server, coordinator

**Runner**:
A service that executes jobs; there can be many, on Cloudflare and on hosts.
_Avoid_: runtime, worker, agent, executor

**Workspace**:
A job's files and the place its commands run, provided by Cloudflare Computer and owned by the job's runner; it survives restarts.
_Avoid_: sandbox, filesystem, disk

**Container image**:
The Linux image Cloudflare Computer's container backend starts when a job's commands need real programs; ours is published as `agent-batch-container`.
_Avoid_: job image, sandbox image

**Host**:
A machine owned by the user that runs runners.
_Avoid_: node, server, box

**Placement**:
The hub's choice of runner for a job: hosts with free capacity first, Cloudflare when they are full.
_Avoid_: scheduling, routing, spill-over

**Extension**:
A Pi extension listed in configuration and bundled into runners at deploy time, changing how the agent loop behaves.
_Avoid_: plugin, add-on
