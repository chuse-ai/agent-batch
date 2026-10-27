# Hono and oRPC, contract-first, on the newest major versions

The job API is an oRPC contract in a shared `contract` package, with schemas in Zod Mini; the runner protocol's method schemas live there too, but it is JSON-RPC over the Runner's socket, not oRPC (ADR 0026). The hub implements them; the TypeScript client and the dashboard consume them with full types and no code generation, and the HTTP API is served as REST through oRPC's OpenAPI handler, which also produces an OpenAPI document for other languages. Hono hosts the API, the runner connection route and, later, Better Auth.

The package exports two contracts. `contract` is the public API, for people and their programs: the CLI, the OpenAPI document and the TypeScript client derive from it, under the public API's compatibility. `runnerContract` holds the two HTTP calls only a Runner makes, registering and downloading its jobs' Input files; they belong to the runner protocol and its version (ADR 0026), so the public tools leave them out. The hub implements both, composed into one router; every procedure declares its own path, so the composition changes no route. Which tool sees which call is decided by which contract holds it, not inferred from a call's permissions.

We start on the newest major line of each dependency even before it is stable, pinning exact versions while pre-release: oRPC 2 changed its wire format, so clients on version 1 could not talk to a version 2 hub, and starting on the line going forward avoids a coordinated migration later.

## Considered options

- One contract, each tool filtering out the Runner's calls: oRPC's `generate(router, { filter })` reaches only the OpenAPI document, OpenAPI tags and an `x-internal` extension only label operations for tools that read them, trpc-cli has no option to leave a procedure out, and the TypeScript client would still carry the calls. The CLI filtered by the permissions a call needs, which a future person's key holding a `runner` permission would break.
- Hono alone, with its validators and typed client: the client is tied to the server's route types, and OpenAPI needs another package.
- Elysia: its Cloudflare Workers adapter is experimental, OpenAPI type generation does not work there, and its client is typed from the server instead of a separate contract.
- Classic Zod: about 80 KB more in the Worker bundle; Zod Mini shares the Zod core that Better Auth already brings and converts to JSON Schema natively.
