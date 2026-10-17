# runner

A Node process that connects out to the hub and runs many jobs' agents at once (ADR 0019). The CLI bundles it and calls `startRunner`; after `vp pack`, `vp run start` starts one from the environment:

- `HUB_URL`: the hub's URL.
- `RUNNER_REGISTRATION_KEY`: an API key granted `runner: ["register"]`, shared by Runners. A Runner uses it on its first start to register and receive a key of its own (ADR 0008).
- `RUNNER_DATA_DIR`: where the Runner keeps its own key and anything else that must survive restarts; `data` by default.
- `RUNNER_MAX_JOBS`: how many jobs the Runner runs at once; 8 by default.

Once connected, the Runner says hello and syncs with the hub: it offers its free slots, the hub places queued jobs on them, and the Runner reports what becomes of each job (ADR 0026). It syncs at the interval the hub gives it, when the hub hints that it has something new, and when a job ends. For now the agent is a stub that returns the prompt as the result; the agent loop on pi-durable replaces it.

A Runner the hub has removed, or a hub that no longer knows (for example after a reset), stops with a message instead of retrying; `vp run start register` discards its key and registers it again. Any other refusal, such as from a proxy or from a hub that cannot reach its database, is retried.

To remove a compromised host, delete its Runner on the hub (`DELETE /runners/{id}`), then revoke the registration key and create a new one for future hosts: the host still holds the old one and could otherwise register again. Runners already registered keep working, since they connect with their own keys. A registration key can also be created to expire (`expiresIn`), for a key meant for one batch of hosts.
