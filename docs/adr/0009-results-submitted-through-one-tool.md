# Results are submitted through one tool that checks them

The result contract is a JSON Schema. The runner gives the agent one submission tool, `submit_result`, which takes the value inline or as the path of a JSON file in the workspace, so results produced by scripts are not retyped by the model. Every submission is checked against the schema, and errors go back to the agent while it can still fix them. An accepted result ends the run.

## Considered options

- Reading the result from the final message or a results file after the agent stops: problems are found only once it can no longer fix them.
- Treating whatever valid files land in an output folder as the result: scripts write files the runner does not see being written, so feedback cannot be immediate, and a completion signal is still needed.
- Constrained generation of tool arguments: our validation runs anyway and an invalid submission costs one tool round, while provider support and supported schema features vary.
