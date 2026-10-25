# Release scope and limitations

The current implementation runs individual unattended jobs on provisioned Runners. A job supplies a prompt, inline text input files and a JSON Schema result contract. The hub records events, activity, results and usage; callers can pause, resume, cancel and delete jobs, and set run-time and model-spend limits. The CLI calls the hub's HTTP API.

The hub runs on Cloudflare Workers with Postgres through Hyperdrive, or on Node/Docker with Postgres directly. Each Runner is a separate Node process. The hub assigns jobs to connected Runners that report available capacity. A job's journal and Workspace remain on its Runner's host.

## Planned capabilities

Automatic cloud overflow, moving jobs between hosts, batch submission and canaries, labels, per-job model selection and governance, secret injection and network egress, richer file upload/download flows, validation scripts, incremental result submission, a dashboard, SSO, and CLI login/local orchestration are planned.

Events currently remain in Postgres. Object storage, event compaction and retention policies are planned. Deployments must account for database growth. The Docker hub runs as one process; multiple replicas do not yet coordinate Runner connections.

## Operating constraints

- Run one Runner process per data directory and one process per registered Runner identity. Do not clone a registered data directory onto another live host or use overlapping rolling updates. The application does not yet enforce exclusive ownership; duplicates can execute the same job twice.
- Restart recovery assumes an intact journal and Workspace on the same host. Deleting a journal or restoring an older copy can repeat work and model spend. This differs from an ordinary process restart and is not yet detected reliably.
- A job whose Runner stops communicating can continue to display `running` with stale activity and usage. That status does not prove the host is reachable. Pause and cancel requests for that host wait for it to reconnect.
- Restoring the hub's database or failing over to a replica that lost acknowledged writes can lose placement decisions. Run IDs fence ordinary stale reports, but do not provide complete reconciliation after a database rollback. A job can be interrupted or placed again. Do not assume exactly-once external effects.
- Deleting a job removes its content at the hub and asks its Runner to purge local files. A silent host cannot confirm that purge. Removing the Runner does not erase files on its host; operators remain responsible for that disk.
- Job commands have no network access in the default sandbox. Model-provider credentials stay in the Runner, outside job sandboxes. This is a single-organization deployment, not a hostile multi-tenant service.
- Per-job memory caps require a suitable Linux user systemd manager. macOS and Docker without that manager have no individual job cap; a Docker memory limit covers the Runner and all its jobs together.
- Linux sandbox support depends on host policy. The Runner's Docker image needs explicit security options, and some Ubuntu/AppArmor combinations are unsupported. Follow the [Runner requirements](../apps/runner/README.md#what-a-host-needs); disabling the sandbox changes the trust boundary.

The default Runner concurrency is eight jobs. Historical density measurements describe particular workloads and environments; they are not a capacity guarantee for an arbitrary deployment.

## Development history

Development began privately in October 2026. This repository retains technical development history edited before the initial public release to remove private metadata and internal context. Pre-release discussions remain private. Commit dates describe the original development; signatures on rewritten commits attest to the edited records. Historical intermediate versions are not release candidates.
