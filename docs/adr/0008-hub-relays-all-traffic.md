# The hub is the only address; runners reach it over plain HTTPS

Callers and runners connect only to the hub, and hosts are never reachable from outside. Runners start every exchange over plain HTTPS: they post each job's events in batches every few seconds, receive pending commands (new work, pause, cancel, resume) in the responses, and poll when idle. The hub stores results and the event history and serves them to callers, so polling, inspecting a running job, and later steering it and passing messages between jobs all go through one place.

We considered the opposite: the hub assigns a runner and returns its address, and callers then talk to that runner directly, keeping long-lived traffic off a central service. We rejected it because company hosts sit behind NAT and firewalls, so every host would have to be made reachable; every runner would have to check identities and keep its own audit trail; a dead host would take finished results with it; and jobs on different hosts could not reach each other at all. A hub that everyone connects out to solves all four.

We also considered runners holding WebSocket connections to the hub. Plain requests work identically on Cloudflare Workers and in an ordinary server, need no object to hold each connection, and keep the hub stateless so any instance can serve any runner; commands still arrive within seconds, at once for a busy runner.

## Consequences

The runner remains the source of truth for executing a job: pi-durable's state and the workspace stay on its host, and resuming happens there. The hub keeps the complete event history and the results, forwarded as they happen, so a job can be inspected at any time, including after its host has gone away; retention bounds what this stores.

Pushing live updates to people watching a job is designed with the dashboard. If a heavy stream ever needs to bypass the hub, the hub can hand the client a short-lived direct address to the runner; the client hides this, so the API does not change.
