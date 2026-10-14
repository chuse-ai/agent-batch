# One public package; the hub and runner stay private

agent-batch publishes one npm package, `agent-batch`: the CLI plus the prebuilt hub and runner Workers it runs locally and deploys. The hub and runner are private workspace packages that the CLI bundles. Users customise jobs by sending scripts as data, the agent loop through extensions listed in config, and everything around it by calling the hub's API; none of that needs Worker code of their own, and a public library would freeze internals that are still changing.

## Consequences

Publish the runner as `@chuse-ai/agent-batch-runner` once the job API has settled and a user has a concrete need that scripts, config and the API cannot meet (custom Worker code, embedding, full control of bindings). Publishing it later only adds a package.
