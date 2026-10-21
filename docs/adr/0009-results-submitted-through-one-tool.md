# Results are submitted through one tool chosen by the result's shape

The result contract is a JSON Schema plus an optional validation script. The runner gives the agent exactly one submission tool, chosen by the schema's top level: `submit_result(value)` for a single result, or `submit_items(items, final)` for a list, whose items are validated and kept as each batch is accepted, so a job that stops early still has its accepted items. Either tool takes the value inline or as the path of a JSON file in the workspace, so results produced by scripts are not retyped by the model. Every submission is checked against the schema and the script, and errors go back to the agent while it can still fix them. Completion ends the run; an agent that stops without completing is reminded a bounded number of times, then the job fails, keeping accepted items.

## Considered options

- Reading the result from the final message or a results file after the agent stops: problems are found only once it can no longer fix them.
- Treating whatever valid files land in an output folder as the result: scripts write files the runner does not see being written, so feedback cannot be immediate, and a completion signal is still needed.
- Always submitting items and finishing with a separate call, even for single results: adds a step the model can forget on the most common kind of job.
- A per-item key, or a caller switch for incremental submission: rejected items are never stored, so nothing needs replacing, and the agent choosing its batch size makes a switch unnecessary.
- Constrained generation of tool arguments: our validation runs anyway and an invalid submission costs one tool round, while provider support and supported schema features vary.
