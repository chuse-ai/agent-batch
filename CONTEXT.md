# agent-batch

Runs many one-off AI agent jobs unattended, each returning a result that meets a declared contract, on the organization's own machines first and on Cloudflare when they are full.

## Language

**Organization**:
The company a deployment serves; it owns every job, member, key and host in that deployment.
_Avoid_: tenant, account, workspace, team

**Member**:
A person in the organization, signed in with a password or through the organization's identity provider.
_Avoid_: user, account, seat

**API key**:
A named, revocable credential owned by the organization that a service uses to call the hub, limited to the actions it is granted.
_Avoid_: token, service account, secret

**Secret**:
A named credential the organization stores with the hosts it may be sent to; a job may use it without its agent ever seeing the value.
_Avoid_: API key, token, credential, environment variable

**Job**:
One self-contained piece of agent work: a prompt, its input files, and an output contract. Runs once, unattended; a person may pause, resume or cancel it.
_Avoid_: task, run, session, conversation

**Label**:
A key and value attached to a job when it is submitted, such as `customer = acme`, used to find jobs, group them, and attribute their cost.
_Avoid_: tag, metadata, category

**Job state**:
Where a job is in its life: queued, running, paused, succeeded, failed or cancelled. Paused jobs can be resumed; succeeded, failed and cancelled are final. A running job whose Runner restarts stays running and continues where it left off; it is not resumed. Why a job stopped is its reason, not a further state.
_Avoid_: status, phase

**Usage record**:
The small, lasting record of a job's outcome and cost (labels, model, state, tokens, cost, times), kept after the job's content expires or is deleted.
_Avoid_: billing record, receipt, audit log

**Prompt**:
The instruction text that tells a job's agent what to do.
_Avoid_: task, instructions, query

**Input file**:
A file written into a job's workspace before its agent starts, such as a document, data or a script to run.
_Avoid_: attachment, upload, input data

**Output contract**:
The JSON Schema, and optional validation script, that a job's result must satisfy before the job counts as succeeded.
_Avoid_: output format, response schema

**Output file**:
A file a job's agent puts in its output folder, delivered to the caller with the result and kept even if the job's host goes away.
_Avoid_: artifact, attachment, export

**Result item**:
One element of a job's result when the result is a list, validated and kept the moment it is accepted.
_Avoid_: checkpoint, partial result, record, chunk

**Batch**:
The jobs submitted together, released and paused as one.
_Avoid_: run, group, campaign

**Canary**:
The first jobs of a batch, run before the rest are released.
_Avoid_: preflight, pilot, trial run

**Event**:
One entry in a job's history: a message, tool call or tool result from the job's agent, or something the hub records about the job, such as a state change or a limit notice; a job's events, in order, are its history, kept during and after the run.
_Avoid_: log line, update, notification

**Current activity**:
What a running job is doing at this moment, such as waiting on a model or running a tool, and since when; it is replaced as the job moves on and is not part of its history.
_Avoid_: live view, status

**Hub**:
The single central service that every caller and runner connects to; it accepts jobs, keeps the record of every job and runner, decides where each job runs, and relays each job's events.
_Avoid_: control, control plane, server, coordinator

**Runner**:
A process that runs many jobs' agents at once; there can be many, on hosts and in the cloud.
_Avoid_: runtime, worker, agent, executor

**Workspace**:
A job's files and the place its commands run, provided by its execution environment and owned by the job's runner; it survives restarts.
_Avoid_: sandbox, filesystem, disk

**Execution environment**:
Where a job's commands run and its files live, such as a sandboxed shell beside the runner, a container, or Cloudflare Computer; each declares what it can run and how it isolates and keeps files.
_Avoid_: backend, sandbox, runtime

**Container image**:
The Linux image a container starts when a job's commands need real programs; ours is published as `agent-batch-container`.
_Avoid_: job image, sandbox image

**Host**:
A machine the organization controls, owned or rented, that runs runners.
_Avoid_: node, server, box

**Placement**:
The hub's choice of runner for a job: hosts with free capacity first, Cloudflare when they are full.
_Avoid_: scheduling, routing, spill-over

**Extension**:
A Pi extension listed in configuration and bundled into runners at deploy time, changing how the agent loop behaves.
_Avoid_: plugin, add-on
