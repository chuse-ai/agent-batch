# agent-batch

Run one-off agent jobs unattended on your own machines, each with a validated result contract.

## The command

`agent-batch` calls a hub's API with an API key: one command for each call a key can make, with the API's names, flags and help. It needs Node 22.19 or later.

- `AGENT_BATCH_URL`: the hub's URL.
- `AGENT_BATCH_API_KEY`: an API key holding the permissions the calls need, such as `job: ["create", "read"]`.

```sh
agent-batch jobs submit --input job.json
agent-batch jobs wait --id <job ID>
agent-batch jobs --help
```

Each command takes its input as one flag per field, or whole as JSON: from a file with `--input <file>`, from stdin with `--input -`, or inline with `--json`. `jobs submit` takes JSON only, since a job's `input` field would otherwise take the `--input` flag. A command prints the API's answer as one line of JSON. An error the hub answers prints its Problem Details on stderr and exits with 1.

- `jobs submit` submits one job as a Batch of one, without a failure breaker: it creates the Batch, running the job's `agent`, and adds the job. It retries each call that got no answer or a server error, at most twice, under one custom ID, which names both the Batch and the job: yours, as `customId` in the job's JSON, or one it makes for this run and prints on stderr before it sends anything. Submitting again with that ID tells whether the job was accepted, also after the command was stopped; give a job an ID of your own to make submitting it safe to repeat.
- `batches create` retries the same way, under the Batch's `customId` or one it makes and prints.
- `jobs wait` reads the job until it ends, waiting longer between reads, up to half a minute; a read that got no answer or a server error is retried as a submission is, and an error that remains ends the wait. It prints the job and exits with 1 unless it succeeded. A paused job keeps it waiting, as its work is still owed, until the job is resumed and ends or is cancelled. Stopping it leaves the job running.

Calls only a signed-in member can make, such as creating API keys, are not commands.

## Development

```bash
vp install
vp test
vp pack
```
