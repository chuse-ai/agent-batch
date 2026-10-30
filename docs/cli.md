# The agent-batch command

`agent-batch` calls a hub's API with an API key: one command for each call a key can make, with the API's names, flags and help. It needs Node 22.19 or later, and runs with `npx agent-batch`. `agent-batch --help` lists every command, and `agent-batch <command> --help` its flags.

Calls only a signed-in member can make, such as creating API keys, are not commands.

## Which hub it calls

- `AGENT_BATCH_URL`: the hub's URL.
- `AGENT_BATCH_API_KEY`: an API key holding the permissions the calls need, such as `job: ["create", "read"]`.

Without either, the commands call the hub `agent-batch stack` runs with the key it made, and say so on stderr.

## Input and output

Each command takes its input as one flag per field, or whole as JSON: from a file with `--input <file>`, from stdin with `--input -`, or inline with `--json`. `jobs submit` takes JSON only, since a job's `input` field would otherwise take the `--input` flag. A command prints the API's answer as one line of JSON. An error the hub answers prints its Problem Details on stderr and exits with 1. Uploading a file's bytes, `PUT /blobs/{digest}`, takes no JSON, so it has no command; `blobs check` tells which files the hub already holds.

## Jobs and Batches

```sh
agent-batch jobs submit --input job.json
agent-batch jobs wait --id <job ID>
agent-batch jobs --help
```

- `jobs submit` submits one job as a Batch of one, without a failure breaker: it creates the Batch, running the job's `agent`, and adds the job. It retries each call that got no answer or a server error, at most twice, under one custom ID, which names both the Batch and the job: yours, as `customId` in the job's JSON, or one it makes for this run and prints on stderr before it sends anything. Submitting again with that ID tells whether the job was accepted, also after the command was stopped; give a job an ID of your own to make submitting it safe to repeat.
- `batches create` retries the same way, under the Batch's `customId` or one it makes and prints.
- `jobs wait` reads the job until it ends, waiting longer between reads, up to half a minute; a read that got no answer or a server error is retried as a submission is, and an error that remains ends the wait. It prints the job and exits with 1 unless it succeeded. A paused job keeps it waiting, as its work is still owed, until the job is resumed and ends or is cancelled. Stopping it leaves the job running.

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
