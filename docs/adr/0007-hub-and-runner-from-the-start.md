# Hub and runner are separate Workers from the start

The hub (job submission, the record of jobs and runners, placement) and the runner (job execution) are separate private packages and separate Workers, even while 0.1 has a single runner. They will run in different places (one hub on Cloudflare; runners on Cloudflare and on every host), fail independently, and carry different load, so the boundary is drawn now, while it costs little: the hub-runner contract gets designed first, runner code cannot come to depend on hub internals, and local development already matches production. `agent-batch dev` and `agent-batch deploy` start or deploy both, so users still see one command.

## Consequences

The hub will also serve the future web dashboard; whether the dashboard lives in the hub package (for example as a full-stack app with Cloudflare's Vite plugin) is decided when the dashboard is built.
