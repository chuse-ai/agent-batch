# `agent-batch dev` runs the released images through Docker Compose

`agent-batch dev` runs a hub, a Runner and the hub's Postgres on the user's machine from the images released with the same version of the CLI (ADR 0032), by running `docker compose` on a Compose file the command writes. It follows the repository's `compose.yaml`, with the command's settings in place of `.env` (the secrets it saved, the hub's port and address, the model and the variable holding its provider's key) and three differences: its own project name, `agent-batch-dev`, so its containers and volumes stay apart from a Compose deployment's; the hub published on `127.0.0.1` only; and no restart policy, so containers left behind by a run that was killed do not start again with Docker.

It runs in the foreground. Ctrl-C, `SIGTERM` or a closed terminal runs `docker compose down`, which removes the containers and network and keeps the volumes. A run killed outright cannot clean up, so the next start, and `agent-batch dev stop`, remove what it left. A lock on the command's state keeps a second run from removing the first one's containers.

What a run creates is kept for the next: the database and the Runner's data, which holds its registration and any subscription sign-in, in the project's volumes; the secrets the database and hub were created with, the owner's password and the keys made on the hub, in one file readable only by the user, in their data folder, never the working directory. The Postgres password is fixed when its volume is created, so `agent-batch dev reset` deletes the volumes and that file together. On its first start the command signs in as the owner the hub created and makes the two keys the Compose deployment's users make with `curl`: one Runners register with, one for jobs. Other `agent-batch` commands use the saved hub and key, as a pair, when neither `AGENT_BATCH_URL` nor `AGENT_BATCH_API_KEY` is set, so a key is never sent to a hub it was not made for.

The Runner's sandbox is not weakened to start on a host that refuses it: on Ubuntu 24.04 with its default AppArmor setting, no container option lets the sandbox's user namespaces keep their capabilities (the three the image already needs, `--privileged`, and root with `CAP_SYS_ADMIN` were each tried), so the Runner refuses to start, and the command stops and points to the host setting.

## Considered options

- Bundling the hub and the Runner into the CLI and running them as processes on the host, with only Postgres in a container: the hub needs Node 26 while the CLI supports Node 22 (ADR 0024), and a Runner on the host needs bubblewrap, socat and ripgrep installed on Linux, which the image carries.
- The Docker API through a library, such as dockerode or Testcontainers: Compose already declares the services, their order, health checks and volumes, and either library would add a Docker client to a bundle every user of the CLI downloads. Testcontainers' Ryuk container would remove a killed run's containers without another run, at the cost of a further container with the Docker socket.
- A detached start and stop pair: containers would keep running after the user is done, and a foreground command removes them on the ordinary way out.
- A fresh database on every start: the Runner would register again each time, and a subscription sign-in, which needs a person at a browser, would be lost with its volume.
- Running jobs without a sandbox where the host refuses one: unsandboxed jobs see the Runner's environment, holding the provider's key, and its data, holding any sign-in.

## When to reconsider

When the CLI bundles the hub and the Runner, or when the Runner's sandbox can start on Ubuntu's default AppArmor setting without a host change.
