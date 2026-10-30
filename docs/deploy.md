# Deploying agent-batch

The README starts one machine with `npx agent-batch stack start`. This page has what running it for longer needs: host requirements, operating the local stack, a Compose deployment that Runners on other machines connect to, and how to call the hub's API directly.

## Host requirements

Docker Engine 29 or later with Compose 5 or later, Node 22.19 or later, and `jq` for the README's examples. Use current patch releases of [Docker Engine](https://docs.docker.com/engine/install/) and [Compose](https://docs.docker.com/compose/install/).

On Ubuntu 24.04, let the Runner's job sandboxes create user namespaces before starting it: `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`, plus a file in `/etc/sysctl.d` to keep the setting across reboots. On Ubuntu 25.04 and later, jobs cannot run in a sandbox yet. [The Runner's README](../apps/runner/README.md) explains both, and covers running a Runner outside Docker.

## One machine: the local stack

`npx agent-batch stack start` runs a hub, its Postgres and a Runner from the images released with the CLI, in the background, and keeps running after the terminal closes. It returns once the hub is healthy and the Runner has started. On its first start it creates the hub's owner and an API key for jobs; `agent-batch` commands use that hub and key while `AGENT_BATCH_URL` and `AGENT_BATCH_API_KEY` are unset, and `npx agent-batch stack env` prints both for other clients, such as `curl`.

- **Models.** The default is OpenRouter's free models. Choose another with `--provider` and `--model`, as Pi names them, such as `--provider anthropic --model claude-sonnet-5-5`. On first use the command offers provider login and saves it in the Runner's volume; keys are hidden as you type. `npx agent-batch stack login --provider <provider>` adds or replaces one provider's credential, offering an API key or account sign-in where Pi supports them; `--with-api-key` reads a key from stdin. To supply a key from your environment instead, forward its variable by name: `OPENROUTER_API_KEY="$MY_KEY" npx agent-batch stack start --env OPENROUTER_API_KEY`, where `MY_KEY` already holds the key, so it stays out of your shell history.
- **Status and logs.** `npx agent-batch stack status` shows whether the stack is running, the hub's URL and health, the image versions in use and the Runner's authentication source. `npx agent-batch stack logs` prints the logs; `--follow` keeps printing them. A job stays `queued` until a Runner has room for it, and the Runner's log shows any error that keeps it from connecting.
- **Stopping.** `npx agent-batch stack stop` keeps the containers, data and credentials for the next start. `npx agent-batch stack delete` removes the database and Runner volumes, including saved logins, without a prompt.
- **Upgrading.** Run `npx agent-batch stack stop`, back up the volumes if the data matters, then `npx agent-batch@<new-version> stack start` with the new CLI version. Saved logins survive; if you used `--env`, supply those variables again.

## Several machines: Docker Compose

The hub and a Runner from their published images on one machine, as a deployment that other machines' Runners and callers can reach once the hub's address is opened to them (see `compose.yaml`). It needs Docker Engine 29 or later and Compose 5 or later, `curl`, `jq` and `openssl`, and on Ubuntu 24.04 the host setting in [Host requirements](#host-requirements). Use current patch releases of [Docker Engine](https://docs.docker.com/engine/install/) and [Compose](https://docs.docker.com/compose/install/).

1. **Start the hub.** It creates its owner from these settings on its first start and serves the API at `http://localhost:8787`. To upgrade later, rerun only the `curl` line from the current version of this page, then `docker compose up -d`, and upgrade Runners on other machines after it: a Runner older than its hub is supported, one newer than its hub is not, as a Kubernetes kubelet must not be newer than its API server.

   <!-- x-release-please-start-version -->

   ```sh
   curl -fsSLO https://raw.githubusercontent.com/chuse-ai/agent-batch/v0.5.0/compose.yaml
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
   docker compose up --wait runner
   ```

   Another model goes in `RUNNER_MODEL_ID` the same way, such as `anthropic/claude-haiku-5.5` or `openai/gpt-6-luna` for real work.

4. **Submit jobs** as in [the README's first job](../README.md#an-everyday-chore-done-by-an-agent), from writing `job.json` on, with `export AGENT_BATCH_URL=http://localhost:8787`; `docker compose logs runner` shows any error that keeps the Runner from connecting.

## Calling the API directly

The hub serves its OpenAPI document at `/spec.json`, generated from the contract, and a browsable reference of it at `/`, which shows each call with a `curl` example. Each call takes an API key in the `x-api-key` header holding the permissions the document lists for it, a member's session cookie, or either; errors are RFC 9457 Problem Details.

- **Waiting for a job:** read it until its status is final, waiting longer between reads, as `agent-batch jobs wait` does.
- **Reading events:** a job's events come a page at a time. Read again `after` the last event's `id` while `hasMore` is true, and stop once a page without more has a final `status`. `agent-batch jobs events --id <id> --after <event id>` does the same from the command line.
- **Controls:** jobs and Batches can be paused, resumed (carrying on where they stopped) and cancelled, and jobs deleted once they have ended. A refused control says why, and the reference at `/` says when each applies.
