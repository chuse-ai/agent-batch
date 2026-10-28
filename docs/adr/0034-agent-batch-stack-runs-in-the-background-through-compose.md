# `agent-batch stack` runs in the background through Docker Compose

`agent-batch stack` runs a hub, a Runner and the hub's Postgres on this machine from the images released with the same version of the CLI (ADR 0032). It serves both first tries and daily use: users can be done with their terminal before their jobs finish. The other namespaces, such as `jobs` and `batches`, act on the selected hub; `stack` always manages this machine's deployment.

The command writes a Compose file following the repository's `compose.yaml`, with its saved settings in place of `.env`, its own project name, `agent-batch-stack`, and the hub published on `127.0.0.1` only. All three containers use `restart: unless-stopped`, so Docker brings them back after a restart unless the user stopped them. `start` runs `docker compose up -d --wait`, first for the hub so it can create the initial keys, then for the whole stack. It returns with the hub healthy and prints its URL. Repeating it leaves an unchanged, healthy stack running. A proper-lockfile lock covers each mutating command (`start`, `stop`, `delete`, `login`), never the lifetime of the deployment.

| Command        | Effect                                                                                                                                 |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `stack start`  | Starts in the background and waits for the hub to be healthy; keeps existing data.                                                     |
| `stack stop`   | Runs `compose stop`; keeps containers, volumes and credentials.                                                                        |
| `stack delete` | Runs `compose down --remove-orphans --volumes`; removes the database and Runner data and the saved credentials file, without a prompt. |
| `stack status` | Shows Compose's container state, health and image versions, and the saved hub URL.                                                     |
| `stack logs`   | Prints Compose's logs; `--follow` keeps following them.                                                                                |
| `stack login`  | Saves a provider sign-in in the Runner's volume.                                                                                       |
| `stack env`    | Prints the saved hub URL and jobs API key as shell exports.                                                                            |

What a start creates is kept for the next: the database and the Runner's data, which holds its registration and any subscription sign-in, in the project's volumes; the secrets the database and hub were created with, the owner's password and the keys made on the hub, in one file readable only by the user, in their data folder, never the working directory. The Postgres password is fixed when its volume is created, so `delete` removes that file only once the volumes are gone. On its first start the command signs in as the owner the hub created and makes the two keys the Compose deployment's users make with `curl`: one Runners register with, one for jobs. Other `agent-batch` commands use the saved hub and key as a pair when neither `AGENT_BATCH_URL` nor `AGENT_BATCH_API_KEY` is set, so a key is never sent to a hub it was not made for.

`delete` names the destructive operation directly. `reset` left unclear whether the command starts again afterwards, while a deletion flag on `stop` would make a normally non-destructive operation capable of erasing data. Neither command prompts. `status` and `logs` replace watching a foreground terminal; `stop` and `delete` make cleanup explicit. To upgrade, stop the stack, back up its volumes if the data matters, then start with the new CLI version.

## Precedents

Verified against documentation or source on 2026-10-28:

- [Supabase's `start`, `stop` and `status`](https://supabase.com/docs/reference/cli/start) manage a background local stack; its [container specifications use `unless-stopped`](https://github.com/supabase/cli/blob/develop/apps/cli/src/command-internal/db-bootstrap/docker-create-args.ts).
- [MongoDB's `atlas local start`, `stop` and `delete`](https://www.mongodb.com/docs/atlas/cli/current/command/atlas-local/) distinguish starting, pausing and deleting a deployment.
- [Redpanda's `rpk container`](https://docs.redpanda.com/streaming/current/reference/rpk/rpk-container/rpk-container/) provides `start`, `stop`, `status` and `purge`; its [command description](https://github.com/redpanda-data/redpanda/blob/dev/src/go/rpk/pkg/cli/container/container.go) is “Manage a local container cluster”.
- [minikube's commands](https://minikube.sigs.k8s.io/docs/commands/) distinguish `start`, `stop` (keeping data) and `delete` (removing the cluster).
- [Colima's `start`](https://colima.run/docs/commands/#colima-start) runs in the background by default, with `--foreground` optional.

## Considered options

- A foreground server that removes its containers when the terminal closes: this tied job availability to a terminal's lifetime. The earlier objection that detached containers keep running after the user is done is reversed: daily users are done with their terminal before their jobs. Explicit lifecycle commands cover cleanup.
- Bundling the hub and the Runner into the CLI and running them as processes on the host, with only Postgres in a container: the hub needs Node 26 while the CLI supports Node 22 (ADR 0024), and a Runner on the host needs bubblewrap, socat and ripgrep installed on Linux, which the image carries.
- The Docker API through a library, such as dockerode or Testcontainers: Compose already declares the services, their order, health checks and volumes, and either library would add a Docker client to a bundle every user of the CLI downloads.
- A fresh database on every start: the Runner would register again each time, and a subscription sign-in, which needs a person at a browser, would be lost with its volume.
- Running jobs without a sandbox where the host refuses one: unsandboxed jobs see the Runner's environment, holding the provider's key, and its data, holding any sign-in. The Runner still refuses an unsupported host; its logs explain why. On Ubuntu 24.04 with its default AppArmor setting, neither the required container options, `--privileged`, nor root with `CAP_SYS_ADMIN` lets the sandbox's user namespaces keep their capabilities, so the host setting must change.

## When to reconsider

When the CLI bundles the hub and the Runner, or when the Runner's sandbox can start on Ubuntu's default AppArmor setting without a host change.
