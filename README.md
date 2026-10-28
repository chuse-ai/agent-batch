# agent-batch

Run one-off agent jobs unattended on your own machines, each with a validated result contract.

The hub runs on Cloudflare Workers or Node/Docker; Runners execute jobs on the machines you provision. Automatic cloud overflow is planned. See [release scope and limitations](docs/release-scope.md).

## Quickstart

From nothing to a finished job on one machine with Docker Engine 29 or later and Compose 5 or later, Node 22.19 or later and `jq`: `agent-batch stack start` runs a hub, a Runner and the hub's Postgres from the images released with it, and jobs go through the same `agent-batch` command. To run them for other machines, see [Deploying with Docker Compose](#deploying-with-docker-compose). On Ubuntu 24.04, first let the Runner's job sandboxes create user namespaces: `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`, and a file in `/etc/sysctl.d` to keep it; on Ubuntu 25.04 and later they cannot run yet (see [the Runner's README](apps/runner/README.md)).

1. **Start the hub and a Runner**, with the model every job runs on and its provider's key; here OpenRouter's free models. It returns once the hub is healthy and keeps running in the background, even after you close the terminal.

   ```sh
   OPENROUTER_API_KEY=<your key> npx --yes agent-batch stack start
   ```

   Another model goes in `--model-provider` and `--model-id`, as Pi names them, with the provider's key in `<PROVIDER>_API_KEY`, such as `--model-provider anthropic --model-id claude-sonnet-5-5` with `ANTHROPIC_API_KEY`; `--model-key-env` names another variable, such as `GEMINI_API_KEY` for `google`. `npx agent-batch stack login --model-provider <provider>` signs the Runner in to a provider's subscription instead, where Pi offers one; then start it with that provider and a model. `npx agent-batch stack stop` stops it and keeps its data, keys and sign-in for the next start, which remembers the model too. `npx agent-batch stack delete` stops it and deletes its database and Runner volumes and saved credentials, without a prompt.

   On its first start it creates the hub's owner and an API key for jobs. `agent-batch` commands use that hub and key while `AGENT_BATCH_URL` and `AGENT_BATCH_API_KEY` are unset, and `npx agent-batch stack env` prints both for other clients, such as `curl`.

2. **Submit a job**: the agent it runs, with the Result contract its result must match, the input it works on, Input files the agent finds in its Workspace's `inputs/` folder, and a custom ID. The job's `id` goes into `ID` for the steps that follow.

   ```sh
   cat > job.json <<'EOF'
   {
     "customId": "quickstart-1",
     "agent": { "resultContract": { "schema": { "type": "integer" } } },
     "input": "Count the lines of inputs/notes.txt and submit the count.",
     "inputFiles": { "notes.txt": { "content": "one\ntwo\nthree\n" } }
   }
   EOF
   ID=$(npx --yes agent-batch jobs submit --input job.json | jq -er .id)
   ```

   The command makes the job a Batch of one. Submitting again with the same custom ID returns the same job, so the command retries a submission whose answer did not arrive. Many jobs that run one agent go into one Batch, created with `batches create` and added to with `batches add-jobs`, 100 at a time: its first jobs, its Canary, run before the rest, and it pauses itself when too many of its jobs fail.

3. **Wait for it.** The command prints the job once it ends, and exits with 1 unless it succeeded.

   ```sh
   npx agent-batch jobs wait --id "$ID"
   ```

   A job stays `queued` until a Runner has room for it; the Runner's log, which `npx agent-batch stack logs --follow` prints, shows any error that keeps it from connecting.

   What it did is in its events, a page at a time: read again `after` the last event's `id` while `hasMore` is true, and stop once a page without more has a final `status`.

   ```sh
   npx agent-batch jobs events --id "$ID" --after 0
   ```

4. **Read its result, and control jobs.** A job that succeeded has its `result`, which matches its Result contract: here, `3`.

   ```sh
   npx agent-batch jobs get --id "$ID"
   ```

   Jobs can also be paused, resumed, carrying on where they stopped, and cancelled, and deleted once they have ended; a refused control says why, and the reference at `/` says when each applies. `npx agent-batch --help` lists every command.

   ```sh
   npx agent-batch jobs cancel --id "$ID"   # likewise pause, resume and delete
   ```

`npx agent-batch stack status` shows whether the stack is running, the hub's URL and health, and the image versions in use. `npx agent-batch stack logs` prints its logs without following them.

To upgrade, run `npx agent-batch stack stop`, back up the volumes if the data matters, then run `npx --yes agent-batch@<new-version> stack start` with the new CLI version and the provider key as above.

## Deploying with Docker Compose

The hub and a Runner from their published images on one machine, as a deployment that other machines' Runners and callers can reach once the hub's address is opened to them (see `compose.yaml`). It needs Docker Engine 29 or later and Compose 5 or later, `curl`, `jq` and `openssl`, and on Ubuntu the host setting above. Use current patch releases of [Docker Engine](https://docs.docker.com/engine/install/) and [Compose](https://docs.docker.com/compose/install/).

1. **Start the hub.** It creates its owner from these settings on its first start and serves the API at `http://localhost:8787`. To upgrade later, rerun only the `curl` line from the current README, then `docker compose up -d`.

   <!-- x-release-please-start-version -->

   ```sh
   curl -fsSLO https://raw.githubusercontent.com/chuse-ai/agent-batch/v0.4.0/compose.yaml
   cat > .env <<EOF
   BETTER_AUTH_SECRET=$(openssl rand -base64 32)
   POSTGRES_PASSWORD=$(openssl rand -hex 24)
   ADMIN_EMAIL=you@example.com
   ADMIN_PASSWORD=choose-a-password
   EOF
   # Returns once the hub has migrated its database and created its owner; if the hub itself fails, `docker compose logs hub` says why
   docker compose up --wait hub
   HUB=http://localhost:8787
   ```

   <!-- x-release-please-end -->

2. **Create keys.** Creating API keys takes a signed-in member, and the command cannot sign in yet, so this step uses `curl`. Each key comes back once, as `key`: the one Runners register with goes into `.env`, and the one for jobs into `AGENT_BATCH_API_KEY`.

   ```sh
   curl -sS --fail-with-body -c cookies -H 'content-type: application/json' \
     -d '{"email":"you@example.com","password":"choose-a-password"}' "$HUB/api/auth/sign-in/email"
   # The key Runners register with, kept in .env for the Runner
   key=$(curl -sS --fail-with-body -b cookies -H 'content-type: application/json' \
     -d '{"name":"runners","permissions":{"runner":["register"]}}' "$HUB/api-keys" | jq -er .key) &&
     echo "RUNNER_REGISTRATION_KEY=$key" >> .env
   # A key for submitting and controlling jobs, for the command
   AGENT_BATCH_API_KEY=$(curl -sS --fail-with-body -b cookies -H 'content-type: application/json' \
     -d '{"name":"jobs","permissions":{"job":["create","read","pause","resume","cancel","delete"]}}' "$HUB/api-keys" | jq -er .key) &&
     export AGENT_BATCH_API_KEY
   ```

3. **Start a Runner** with the model every job runs on and its provider's key; it registers with the key step 2 put in `.env`:

   ```sh
   cat >> .env <<EOF
   RUNNER_MODEL_PROVIDER=openrouter
   RUNNER_MODEL_ID=openrouter/free
   OPENROUTER_API_KEY=<your key>
   EOF
   docker compose up -d runner
   ```

   Another model goes in `RUNNER_MODEL_ID` the same way, such as `anthropic/claude-haiku-5.5` or `openai/gpt-6-luna` for real work.

4. **Submit jobs** as in the [quickstart](#quickstart), from step 2, with `export AGENT_BATCH_URL=http://localhost:8787`; `docker compose logs runner` shows any error that keeps the Runner from connecting.

## API

The hub serves its OpenAPI document at `/spec.json`, generated from the contract, and a browsable reference of it at `/`, which shows each call with a `curl` example. Each call takes an API key in the `x-api-key` header holding the permissions the document lists for it, a member's session cookie, or either; errors are RFC 9457 Problem Details. To wait for a job, read it until its status is final, waiting longer between reads, as the command does.

## Development

- Check everything is ready:

```bash
vp run ready
```

- Run the tests:

```bash
vp run -r test
```

- Build the monorepo:

```bash
vp run -r build
```

- Run the development server:

```bash
vp run dev
```

## Contributing

Issues are welcome; outside pull requests are not accepted for now. See [CONTRIBUTING.md](CONTRIBUTING.md).
