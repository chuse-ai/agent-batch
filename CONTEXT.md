# agent-batch

Runs many one-off AI agent jobs unattended, each returning a result that meets a declared contract, on the organization's own machines first and on Cloudflare when they are full.

## Language

**Organization**:
The company a deployment serves; it owns every job, member, key and Runner in that deployment.
_Avoid_: tenant

**Member**:
A person in the organization, signed in with a password or through the organization's identity provider, with a role there. Better Auth calls the person a user and their place in the organization a member; with one organization per deployment the two coincide, and the API speaks of members.
_Avoid_: seat

**API key**:
A named, revocable credential owned by the organization that a service uses to call the hub, limited to the actions it is granted. Each key is an identity of its own, as a service account is elsewhere; the member who created it is recorded but does not act through it. Not a Secret, which a job uses to call other services.

**Secret**:
A named credential the organization stores with the hosts it may be sent to; a job may use it without its agent ever seeing the value. Not an API key, which calls the hub.

**Job**:
One self-contained piece of agent work: a prompt, its input files, and a result contract. It succeeds at most once, unattended, possibly over several runs; a caller may pause, resume or cancel it. Its agent works in conversations, pi's records within the job.

**Label**:
A key and value attached to a job when it is submitted, such as `customer = acme`, used to find jobs, group them, and attribute their cost.
_Avoid_: tag

**Job status**:
Where a job is in its life: queued, running, paused, succeeded, failed or cancelled. Succeeded, failed and cancelled are final; a paused, failed or cancelled job that started can be resumed while its journal exists. A running job whose Runner restarts stays running: it is recovered from its journal and carries on where it left off; it is not resumed. Where a status has more than one cause, the job's reason says which. Status is the one word for where a piece of work is in its life, so a job and each of its tool calls have one.
_Avoid_: phase

**Control**:
What a caller asks of a job besides submitting and reading it: to pause, resume, cancel or delete it. A pause holds a job whose work is still owed, so its Batch and a caller's wait stay open; a cancel withdraws it, though a cancelled job that started can still be resumed. One its Runner must carry out waits for it, shown beside the job's status until the Runner confirms it or the job ends first.

**Journal**:
A job's durable record on its Runner, one file per job: the agent's entries and conversation records, which are copied to the hub as the job's events, and pi's own task records, documents and submissions. The job is recovered from it after a restart, and it is deleted with the job's files.
_Avoid_: checkpoint

**Recovery**:
A running job carrying on from its journal after its Runner stopped and started again: it stays running, completed tool calls are not repeated, a tool call cut off mid-flight gets an interrupted result, and a model call in flight is made again. Not a resume, which is a control a caller gives a stopped job.

**Usage record**:
The small, lasting record of a job's outcome and cost (labels, model, status, tokens, cost, times), kept after the job's content expires or is deleted. A job resumed after it ended has one for each end, each with what it used since the previous one.
_Avoid_: billing record, receipt

**Prompt**:
The instruction text that tells a job's agent what to do.
_Avoid_: instructions, query

**Input file**:
A file written into a job's workspace before its agent starts, such as a document, data or a script to run.
_Avoid_: attachment, input data

**Result contract**:
The JSON Schema, and optional validation script, that a job's result must satisfy before the job counts as succeeded. It governs the result only, not the Output files.
_Avoid_: output contract, output format, response schema

**Output file**:
A file a job's agent puts in its output folder, delivered to the caller with the result and kept even if the machine that ran the job goes away; what CI systems call an artifact.
_Avoid_: attachment, export

**Result item**:
One element of a job's result when the result is a list, validated and kept the moment it is accepted.
_Avoid_: partial result, chunk

**Batch**:
The jobs submitted together, released and paused as one.
_Avoid_: group, campaign

**Canary**:
The first jobs of a batch, run before the rest are released.
_Avoid_: preflight, pilot, trial run

**Event**:
One item of what a job did: a message, tool call or tool result from the job's agent, or something the Runner records about the job, such as its start, recovery or end; a job's events are kept in order, across its runs and after it ends.
_Avoid_: log line

**Current activity**:
What a running job is doing at this moment, such as waiting on a model or running a tool, and since when; it is replaced as the job moves on and is not one of its events, nor its status.
_Avoid_: live view

**Hub**:
The single central service, the control plane, that every caller and runner connects to; it accepts jobs, keeps the record of every job and runner, decides where each job runs (the organization's self-hosted Runners first, Cloudflare when they are full), and relays each job's events.
_Avoid_: coordinator

**Runner**:
A process that runs many jobs' agents at once; there can be many, self-hosted on the organization's machines or in the cloud. What other systems call a worker; not a Cloudflare Worker, which the hub may run on, nor a job's agent, which it runs. A restart is a process, such as a Runner or the hub, stopping and starting again; a job is never restarted, reset, replayed or retried.
_Avoid_: executor

**Workspace**:
A job's files and the place its commands run, provided by its execution environment and owned by the job's runner; it survives restarts. Not the OS sandbox its commands run in (ADR 0027).
_Avoid_: job directory

**Execution environment**:
Where a job's commands run and its files live, such as a sandboxed shell beside the runner, a container, or Cloudflare Computer; each declares what it can run and how it isolates and keeps files.
_Avoid_: backend

**Container image**:
The Linux image a container starts when a job's commands need real programs; ours is published as `agent-batch-container`.
_Avoid_: job image, sandbox image

**Run**:
One stretch of a job running on a runner. It begins with one input, the job's prompt or a resume's note, when the hub places the job there or resumes it, and ends at a decided stop, when the job pauses or ends, or when the hub drops it; a restart of its runner does not end it. Its ID fences what the runner reports, so nothing a past run does touches the next.
_Avoid_: execution, invocation

**Extension**:
A Pi extension listed in configuration and bundled into runners at deploy time, changing how the agent loop behaves.
_Avoid_: plugin, add-on
