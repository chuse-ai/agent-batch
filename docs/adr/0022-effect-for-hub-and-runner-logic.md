# Effect structures the hub's and runners' own logic

The hub and runners write their own logic with Effect 4: a runner's job lifecycle (one supervised scope per job, retries with backoff, timeouts per attempt and per job, cancellation passed to pi-durable, and shutdown that stops taking jobs, pauses them, flushes events and closes resources within a deadline), and the hub's placement, deadlines, event compaction and request handling. What differs between platforms is a service with one Layer per platform: runner connections, timers (Durable Object alarms on Workers, in-process timers in a Docker hub, ADR 0014) and the database connection (Hyperdrive or direct). The libraries already chosen stay: Hono and oRPC, with handlers written as Effects through oRPC's Effect adapter at the same version; Kysely with Kanel's types, each call wrapped in `Effect.tryPromise`, as Effect's migration guide recommends since its Kysely integration was removed in version 4; Zod for contracts; and Better Auth. Fibers do not survive a restart, so pi-durable keeps resuming jobs, and a deadline that must survive is stored in Postgres and re-armed when the process starts.

Effect 4 is a long-term support release with at least three years of fixes. Only APIs without a `@stability unstable` or `experimental` tag are used; Effect's own HTTP, RPC, SQL, Schema and workflow modules are tagged unstable and would replace libraries chosen for reasons of their own, such as Kysely checking queries against the database's types at compile time (ADR 0020). Version 4.0.0 shipped some modules without their unstable tags, corrected in 4.0.2, so each upgrade checks the tags of what we import. On Workers nothing outlives a request: runtimes and resources are scoped to the request or the Durable Object.

## Considered options

- Effect throughout, with its HTTP API, RPC, SQL and Schema modules: they are tagged unstable, replace established libraries, and Effect's SQL types results by schemas checked at runtime rather than by the compiler against the database.
- Effect only for retry policies, as OpenStatus records in its ADR: supervision, cancellation and shutdown would stay hand-written across many places.
- No Effect: p-retry, `AbortSignal` composition, neverthrow and plain interfaces each cover a piece; combining them into supervision with typed errors and testable time is what Effect provides.
- Runners only: a Docker hub is a long-running process with the same needs for shutdown, timers and resource cleanup, and the platform seam is the same problem.

## Consequences

`effect` is pinned like other runtime dependencies, and the oRPC adapter, an experimental package, is pinned with oRPC. Tests of Effect code use `@effect/vitest`, which needs Vitest 5, as Vite+ provides, and its test clock lets timing logic run without waiting. A Worker grows by about 30 to 37 KB gzipped. The shared contract package stays plain TypeScript with Zod, so clients need no Effect. Contributors learn Effect's model: lazy effects, typed failures and defects, scopes and Layers.
