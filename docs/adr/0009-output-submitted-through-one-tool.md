# The output is submitted through one tool that checks it

A job's Output contract holds a JSON Schema. The runner gives the agent one submission tool, `submit_output`, which takes the output inline or as the path of a JSON file in the workspace, so output produced by scripts is not retyped by the model. Every submission is checked against the schema, and errors go back to the agent while it can still fix them. An accepted output ends the run.

The value is called the job's output because agent tooling, the closest match to this contract, calls the validated value of a declared schema that: the Claude Agent SDK's `output_format`, the OpenAI Agents SDK's `output_type`, Pydantic AI's `output_type` (renamed from `result_type`), ADK's `output_schema` and Codex's `--output-schema`. Those are multi-turn agent runs that also end once the value validates; durability, Batches and a waiting caller change where the value lives, not what it is.

## Considered options

- Reading the output from the final message or a file after the agent stops: problems are found only once it can no longer fix them.
- Treating whatever valid files land in a folder as the output: scripts write files the runner does not see being written, so feedback cannot be immediate, and a completion signal is still needed.
- Constrained generation of tool arguments: our validation runs anyway and an invalid submission costs one tool round, while provider support and supported schema features vary.
- Calling the value the result, as Temporal and Celery do: job systems are split, with Dagster and Kubeflow saying output, so they set no convention to follow, and the name the agent tooling uses wins.
