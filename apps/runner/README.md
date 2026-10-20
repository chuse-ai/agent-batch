# runner

A Node process that connects out to the hub and runs many jobs' agents at once (ADR 0019). It runs on its user's Node, from 22.19, so it uses only what Node 22 has (ADR 0024). The CLI bundles it and calls `startRunner`; after `vp pack`, `vp run start` starts one from the environment:

- `HUB_URL`: the hub's URL.
- `RUNNER_REGISTRATION_KEY`: an API key granted `runner: ["register"]`, shared by Runners. A Runner uses it on its first start to register and receive a key of its own (ADR 0008).
- `RUNNER_MODEL_PROVIDER` and `RUNNER_MODEL_ID`: the model every job runs on, as Pi names it, such as `anthropic` and `claude-sonnet-5-5`. The provider's key is read from the environment, as Pi reads it (for example `ANTHROPIC_API_KEY`). The Runner refuses to start when Pi does not know the model or finds no key for its provider, and lists the models it has keys for.
- `RUNNER_THINKING_LEVEL`: how hard the model thinks, one of Pi's levels from `off` to `max`; `medium` by default, as in Pi's coding agent.
- `RUNNER_DATA_DIR`: where the Runner keeps its own key, its jobs' journals and Workspaces, and anything else that must survive restarts; `data` by default.
- `RUNNER_MAX_JOBS`: how many jobs the Runner runs at once; 8 by default.
- `RUNNER_JOB_MEMORY_LIMIT`: how much memory each job's processes may use together, in systemd's notation; `1G` by default. A command that goes over is stopped and the agent is told.
- `RUNNER_JOB_READ_PATHS`: folders jobs may read besides the system's, separated by `:` as in `PATH`, such as where mise, nvm or Homebrew keep the tools jobs should use.
- `RUNNER_RUN_WITHOUT_SANDBOX`: `true` runs jobs without a sandbox where none can start, instead of refusing to start; only for jobs whose inputs you trust.

Once connected, the Runner says hello and syncs with the hub: it offers its free slots, the hub places queued jobs on them, and the Runner reports what becomes of each job (ADR 0026). It syncs at the interval the hub gives it, when the hub hints that it has something new, and when a job ends. Each job's agent runs on pi-durable, with a journal, a Workspace and a temp directory of its own: the job's Input files are in its `inputs/` folder, and the agent ends the job by submitting a result that matches the Output contract. The agent's file operations and commands run in an OS sandbox of the job's own (ADR 0027): they read the system's programs and libraries, the folders in `RUNNER_JOB_READ_PATHS`, and the job's Workspace and temp directory, write only the last two, and have no network, no access to other jobs or the Runner's data, and none of the Runner's environment variables, so not the provider's key. Within its own folders a job may write anything, `.git/hooks` and `.git/config` included, so running git in a job's Workspace outside a sandbox runs what the job wrote there.

A Runner that stops, or is killed outright (by the out-of-memory killer, `kill -9` or a crash), continues its jobs when it starts again on the same `RUNNER_DATA_DIR`: the hub sends it the jobs still placed on it, and each continues from its journal where it left off, with its Workspace as it was. A tool call that had completed does not run again. One the stop cut off does not run again either: the job's commands stop with the Runner, and the agent gets pi-durable's `interrupted` result, saying the call may have partly run. A Runner that starts with fewer slots than the jobs placed on it, as after lowering `RUNNER_MAX_JOBS`, continues as many as it has slots for, and the rest as those end. This covers the Runner's process, not the host: after a power loss or an operating-system crash, a job's last journal commits may be lost, and the job then continues from slightly earlier.

## What a host needs

- **Linux**: `bwrap` (bubblewrap), `socat` and `rg` (ripgrep) on `PATH`, for example `sudo apt install bubblewrap socat ripgrep`. On Ubuntu 24.04, AppArmor denies bubblewrap the user namespaces it needs; load the profile Ubuntu ships for it, as Codex CLI documents (Ubuntu 25.04 and later load it already):

  ```sh
  sudo apt install apparmor-profiles apparmor-utils
  sudo install -m 0644 /usr/share/apparmor/extra-profiles/bwrap-userns-restrict /etc/apparmor.d/bwrap-userns-restrict
  sudo apparmor_parser -r /etc/apparmor.d/bwrap-userns-restrict
  ```

  If that does not let the Runner start, `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0` lifts the restriction for every program. The memory limit needs a user systemd manager of systemd 254 or later (Ubuntu 24.04, Debian 13): a desktop session has one; on a server, `loginctl enable-linger <user>` keeps one running for the Runner's user. Without it the Runner logs that jobs run without a memory limit.

- **macOS**: nothing to install; the sandbox is Seatbelt, and jobs run without a memory limit.
- **Windows**: run the Runner in WSL 2, as on Linux.
- **Docker**: Docker's default seccomp and AppArmor profiles prevent bubblewrap inside a container, so run the Runner on the host.

The Runner proves the sandbox works when it starts, and refuses to start, naming what is missing, if it does not.

A Runner the hub has removed, or a hub that no longer knows (for example after a reset), stops with a message instead of retrying; `vp run start register` discards its key and registers it again. Any other refusal, such as from a proxy or from a hub that cannot reach its database, is retried.

To remove a compromised host, delete its Runner on the hub (`DELETE /runners/{id}`), then revoke the registration key and create a new one for future hosts: the host still holds the old one and could otherwise register again. Runners already registered keep working, since they connect with their own keys. A registration key can also be created to expire (`expiresIn`), for a key meant for one batch of hosts.
