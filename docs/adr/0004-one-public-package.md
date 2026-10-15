# One public package; the hub and runner stay private

agent-batch publishes one npm package, `agent-batch`: the CLI plus the prebuilt hub and runner it runs locally and deploys. The hub and runner are private workspace packages that the CLI bundles. Users customise jobs by sending scripts as data, the agent loop through extensions listed in config, and everything around it by calling the hub's API; none of that needs code of their own in the hub or runner, and a public library would freeze internals that are still changing.

## Consequences

Publish the runner as `@chuse-ai/agent-batch-runner` once the job API has settled and a user has a concrete need that scripts, config and the API cannot meet (custom runner code, embedding, full control of its configuration). Publishing it later only adds a package.
