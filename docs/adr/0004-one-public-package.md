# One public package; the hub and runner stay private

agent-batch publishes one npm package, `agent-batch`: the CLI, which calls the hub over HTTP. The hub and runner are private workspace packages distributed as separate images. Callers submit prompts, input files and output schemas through the API. Configured Pi extensions and additional script-based customization remain planned. Publishing a library now would freeze internals that are still changing.

## Consequences

Publish the runner as `@chuse-ai/agent-batch-runner` once the job API has settled and a user has a concrete need that scripts, config and the API cannot meet (custom runner code, embedding, full control of its configuration). Publishing it later only adds a package.
