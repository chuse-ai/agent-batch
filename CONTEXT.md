# agent-batch

Runs many one-off AI agent jobs unattended, each returning a result that meets a declared contract, on the organization's provisioned Runners.

## Language

**Organization**:
The company a deployment serves; it owns every job, member, key and Runner in that deployment.
_Avoid_: tenant

**Member**:
A person in the organization, signed in with a password, with a role there. Better Auth calls the person a user and their place in the organization a member; with one organization per deployment the two coincide, and the API speaks of members.
_Avoid_: seat

**API key**:
A named, revocable credential owned by the organization that a service uses to call the hub, limited to the actions it is granted. Each key is an identity of its own, as a service account is elsewhere; the member who created it is recorded but does not act through it.

**Job**:
One self-contained piece of agent work: a prompt, its input files, and a result contract. It succeeds at most once, unattended, possibly over several runs; a caller may pause, resume or cancel it. Its agent works in conversations, pi's records within the job.

**Job status**:
Where a job is in its life: queued, running, paused, succeeded, failed or cancelled. Succeeded, failed and cancelled are final; a paused, failed or cancelled job that started can be resumed while its journal exists. A running job whose Runner restarts stays running: it is recovered from its journal and carries on where it left off; it is not resumed. Where a status has more than one cause, the job's reason says which. Status is the one word for where a piece of work is in its life, so a job and each of its tool calls have one.
_Avoid_: phase

**Control**:
What a caller asks of a job besides submitting and reading it: to pause, resume, cancel or delete it. A pause holds a job whose work is still owed, so a caller's wait stays open; a cancel withdraws it, though a cancelled job that started can still be resumed. One its Runner must carry out waits for it, shown beside the job's status until the Runner confirms it or the job ends first.

**Journal**:
A job's durable record on its Runner, one file per job: the agent's entries and conversation records, which are copied to the hub as the job's events, and pi's own task records, documents and submissions. The job is recovered from it after a restart, and it is deleted with the job's files.
_Avoid_: checkpoint

**Recovery**:
A running job carrying on from its journal after its Runner stopped and started again: it stays running, completed tool calls are not repeated, a tool call cut off mid-flight gets an interrupted result, and a model call in flight is made again. Not a resume, which is a control a caller gives a stopped job.

**Usage record**:
The small, lasting record of a job's outcome and cost (model, status, tokens, cost, times), kept after the job's content is deleted. A job resumed after it ended has one for each end, each with what it used since the previous one.
_Avoid_: billing record, receipt

**Prompt**:
The instruction text that tells a job's agent what to do.
_Avoid_: instructions, query

**Input file**:
A file written into a job's workspace before its agent starts, such as a document, data or a script to run.
_Avoid_: attachment, input data

**Result contract**:
The JSON Schema a job's result must satisfy before the job counts as succeeded. It governs the result only.
_Avoid_: output contract, output format, response schema

**Event**:
One item of what a job did: a message, tool call or tool result from the job's agent, or something the Runner records about the job, such as its start, recovery or end; a job's events are kept in order, across its runs and after it ends.
_Avoid_: log line

**Current activity**:
What a running job is doing at this moment, such as waiting on a model or running a tool, and since when; it is replaced as the job moves on and is not one of its events, nor its status.
_Avoid_: live view

**Hub**:
The single central service, the control plane, that every caller and runner connects to; it accepts jobs, keeps the record of every job and runner, decides where each job runs (among connected Runners with available capacity), and relays each job's events.
_Avoid_: coordinator

**Runner**:
A process that runs many jobs' agents at once; there can be many, self-hosted on the organization's machines or in the cloud. What other systems call a worker; not a Cloudflare Worker, which the hub may run on, nor a job's agent, which it runs. A restart is a process, such as a Runner or the hub, stopping and starting again; a job is never restarted, reset, replayed or retried.
_Avoid_: executor

**Workspace**:
A job's files and the place its commands run, provided by its execution environment and owned by the job's runner; it survives restarts. Not the OS sandbox its commands run in (ADR 0027).
_Avoid_: job directory

**Execution environment**:
Where a job's commands run and its files live, such as an OS sandbox of the job's own on its Runner's host.
_Avoid_: backend

**Run**:
One stretch of a job running on a runner. It begins with one input, the job's prompt or a resume's note, when the hub places the job there or resumes it, and ends at a decided stop, when the job pauses or ends, or when the hub drops it; a restart of its runner does not end it. Its ID fences what the runner reports, so nothing a past run does touches the next.
_Avoid_: execution, invocation
