# runner

A Node process that connects out to the hub and runs many jobs' agents at once (ADR 0019). The CLI bundles it and calls `startRunner`; `vp run start` starts one from `HUB_URL` and `RUNNER_ID` after `vp pack`.
