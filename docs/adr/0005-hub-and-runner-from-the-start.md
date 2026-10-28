# Hub and runner are separate deployables from the start

The hub (job submission, the record of jobs and runners, placement) and the runner (job execution) are separate private packages and separate deployables, with a contract between them from the first implementation. The hub runs on Workers or Node/Docker, and Runners are separate Node processes (ADR 0019). They can run in different places, fail independently, and carry different load, so the boundary is drawn now, while it costs little: the hub-runner contract gets designed first, runner code cannot come to depend on hub internals, and local development already matches production. `agent-batch dev` runs both on one machine from their images (ADR 0034); the CLI otherwise calls an existing hub and does not deploy either service.

## Consequences

The hub will also serve the future web dashboard; whether the dashboard lives in the hub package (for example as a full-stack app with Cloudflare's Vite plugin) is decided when the dashboard is built.
