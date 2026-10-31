# The agent-batch command

`agent-batch` calls a hub's API with an API key: one command for each call a key can make, with the API's names, flags and help. It needs Node 22.19 or later, and runs with `npx agent-batch`. `agent-batch --help` lists every command, and `agent-batch <command> --help` its flags.

Calls only a signed-in member can make, such as creating API keys, are not commands.

## Which hub it calls

- `AGENT_BATCH_URL`: the hub's URL.
- `AGENT_BATCH_API_KEY`: an API key holding the permissions the calls need, such as `job: ["create", "read"]`.

Without either, the commands call the hub `agent-batch stack` runs with the key it made, and say so on stderr.

## Input and output

Each command takes its input as one flag per field, or whole as JSON: from a file with `--input <file>`, from stdin with `--input -`, or inline with `--json`. `jobs submit` takes JSON only, since a job's `input` field would otherwise take the `--input` flag. A command prints the API's answer as one line of JSON. An error the hub answers prints its Problem Details on stderr and exits with 1. The `blobs` calls, which check and upload file content, have no commands: `batches submit --each` hashes local files and uploads the ones the hub lacks.

## Jobs and Batches

```sh
agent-batch jobs submit --input job.json
agent-batch jobs wait --id <job ID>
agent-batch batches submit --each invoices/*.pdf --instructions "Extract the total." --output-schema schema.json --wait --output-dir ./results
agent-batch jobs --help
```

- `jobs submit` submits one job as a Batch of one, without a failure breaker: it creates the Batch, running the job's `agent`, and adds the job. It retries each call that got no answer or a server error, at most twice, under one ID, the job's custom ID and its Batch's ID: yours, as `customId` in the job's JSON, in a Batch ID's syntax (1 to 100 letters, digits, `_` and `-`, not beginning with `-`), or a UUID it makes for this run. A made ID is printed on stderr only if the command fails or is stopped with Ctrl-C, which exits with 130. Submitting again with that ID tells whether the job was accepted; give a job an ID of your own to make submitting it safe to repeat, also after the command was killed outright.
- `batches create` retries the same way, under the Batch's `--id` or one it makes and prints only if it stops early.
- `batches submit` runs a whole Batch, its agent from `--instructions` and `--output-schema`, a file holding the JSON Schema each output must match. Its jobs come from a JSONL file, one job's JSON per line as `batches add-jobs` takes it, or from `--each` with files and folders; the two cannot be mixed. A line that is not a job's JSON, one holding only whitespace included, is refused, named by its place among the jobs.
  - With `--each`, a file becomes a job with that file as its one Input file, and a folder a job with every file in its tree at its path in the folder. Hidden files and folders are left out, given or inside a folder, and a link to a folder inside a folder, or a folder it cannot read, is refused. A job's custom ID is its path as given, normalized, so `./invoices/a.pdf` is the job `invoices/a.pdf`. The command hashes each file, asks the hub which it already holds, and uploads the rest, five at a time, each retried as a call is.
  - It adds the jobs in their order, at most 100 at a time, so the first are the Canary, retrying as `batches create` does under `--id` or one it makes, which it prints only if it stops early, in the hint for running it again; running the same command again with that ID adds only the jobs the Batch lacks. It prints the Batch.
  - With `--wait`, it waits until no job is queued, running or paused, then prints the Batch, as `batches get` does; when stdout is a terminal, it prints in its place one line for a person: the tokens and cost the Batch used, with its ID when it made the ID. With `--output-dir`, it first saves one JSON line per job there, in order: `{"customId", "status", "output"}` in `output.jsonl` for each job that succeeded, and in `error.jsonl` for the rest, `"failure"` in place of `"output"` for a failed job and neither for one cancelled or not ended. A file that would hold no line is not saved, and when stderr is a terminal, it names each file saved. The folder is made if absent, in a parent that must exist, as are the files, readable by their owner alone; a folder already holding either file, even as a link, is refused before anything is sent, as is `--output-dir` or `--wait-timeout` without `--wait`. Saving counts toward `--wait-timeout`, and when it fails the Batch is still printed and the command exits with 1. A timeout or Ctrl-C during saving leaves the files it had begun, which a rerun into the same folder then refuses. It exits with 0 when every job succeeded; 1 when one did not, when the Batch was cancelled, or when its failure breaker paused it, which ends the wait and says how to resume; and 124 when `--wait-timeout` passes, a duration such as `30m`, `90m` or `1.5h`; `0` waits without limit, as does leaving it out, and a number without a unit is refused. Ctrl-C stops waiting, exits with 130 and leaves the Batch running.
  - Progress, of uploads and of the wait, ending with `Completed: N succeeded, N failed.` once no job is unfinished, the Batch's ID when the wait starts if `--id` gave it, and the hint after a submit without `--wait` go to stderr only when stderr is a terminal, as git shows its progress, so stdout can go to a pipe or a file while a person watches.
- `jobs wait` reads the job until it ends, every 10 seconds; a read that got no answer or a server error is retried as a submission is, and an error that remains ends the wait. It prints the job and exits with 1 unless it succeeded. A paused job keeps it waiting, as its work is still owed, until the job is resumed and ends or is cancelled. Stopping it leaves the job running.

## A hub and a Runner on this machine

`agent-batch stack start` runs a hub, a Runner and Postgres with Docker, from the images released with this version of the command. It returns once the hub is healthy and the Runner has started, and leaves the stack running in the background. The containers restart with Docker unless explicitly stopped. `--provider` and `--model` select the model every job runs on, using Pi's exact names (`openrouter` and `openrouter/free` initially, then the last ones given). `--port` sets the local hub port, initially 8787.

```sh
agent-batch stack start
agent-batch stack start --provider anthropic --model claude-sonnet-5-5
```

The `stack` commands manage this machine regardless of the hub selected for `jobs` and `batches`.

### Credentials

An interactive start offers login if the selected provider has no credentials. `stack login --provider NAME` also works before the first start, and adds or replaces credentials without changing the selected model. Pi supplies the supported methods: provider API keys, account sign-in, or a choice between them. Typed keys are hidden. Login saves to the Runner's volume, using a temporary container that is removed afterwards. For scripted setup, `stack login --provider NAME --with-api-key` reads a provider API key from stdin and saves it; providers needing additional inputs require interactive login. `stack logout --provider NAME` removes only that provider's saved credential.

For externally managed credentials, repeat `--env NAME` to forward variables under Pi's existing names, only to the Runner:

```sh
OPENROUTER_API_KEY="$MY_KEY" agent-batch stack start --env OPENROUTER_API_KEY
```

The stack imports no provider keys merely because they exist in your shell. It saves forwarded names, never their values. Docker retains the values in the existing container, so an unchanged start or a restart after `stack stop` needs no original variables. Recreating the Runner requires them again. Omitting `--env` retains the names; an explicit list replaces them and refreshes their values; `--clear-env` removes them. Saved provider login takes precedence over ordinary environment credentials, as in Pi. A failed sign-in refresh requires repair rather than silently switching to an environment key.

### Operating the stack

`stack stop` keeps the containers, database, Runner data and credentials. `stack delete` removes the database and Runner volumes, including saved logins, and the stack's saved configuration, without a prompt. `stack status` shows the hub URL, container state and health, image versions, and authentication source where available. `stack logs` prints logs; `--follow` keeps following them.

`stack env` prints the hub's address and jobs API key as shell exports. `AGENT_BATCH_VERSION` selects another image version. To upgrade, stop the stack, back up its volumes if the data matters, then start with the new CLI version. Supply forwarded variables again for Runner replacement; saved logins survive in its volume.
