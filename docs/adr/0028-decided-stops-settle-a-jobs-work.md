# A stop someone decides on settles a job's work; one nobody decided on leaves it pending

pi-durable stops a conversation's work in two ways, for two different events. Closing the Harness is a process letting go of the journal, as at a shutdown, and a crash does the same: nothing is recorded, the work stays pending, and the next process carries it on as if nothing had happened. `abort()` is a decision about the conversation's work, the one pi's Esc makes: it settles the work as aborted, writing the cut-off partial answer and the cut-off tool calls' results into the conversation with their usage, and asks a deferred model call's provider to cancel it. Each has to survive a restart in the opposite way, as a dishwasher picks its cycle up again after a power cut but stays cancelled after its Cancel button: a host restarting must not stop a job's work, and a restart must not undo a decision to stop it.

So a job's stop is sorted by whether someone decided it. A cancel is a decision, so its Runner settles the job's work before writing the job's end, as the job's own end does. It first interrupts the run, closing the Harness and then the sandbox, so the stop takes effect at once and the run cannot write its own end meanwhile; then it reopens the journal without letting any work run, aborts the conversation's work, background work included, and writes the end. A cancel the Runner learns of after its restart is recorded the same way. A placement the hub drops and a Runner's own stop decide nothing about the job, so they only interrupt it, and a job placed again carries on from its pending work.

A cancelled job's work is over, and callers read its events and cost. Left pending, the cut-off answer, the input tokens it used and the cut-off tool calls' output would never reach them.

## Considered options

- Interrupting for every stop: the cancelled job's last step stays out of its events and usage, and nothing in pi marks its work stopped, so only the Runner's care never to start pi's scheduler on that journal keeps it from running again.
- Aborting the running Harness at a cancel: the run, whose answer the abort settles, would write its own end first, and a cancel learned of after a restart would still need the reopening path.
