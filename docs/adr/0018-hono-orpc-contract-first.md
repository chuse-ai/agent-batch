# Hono and oRPC, contract-first, on the newest major versions

The job API and the runner protocol are oRPC contracts in a shared `contract` package, with schemas in Zod Mini. The hub implements them; the TypeScript client and the dashboard consume them with full types and no code generation, and the HTTP API is served as REST through oRPC's OpenAPI handler, which also produces an OpenAPI document for other languages. Hono hosts the API, the runner connection route and, later, Better Auth.

We start on the newest major line of each dependency even before it is stable, pinning exact versions while pre-release: oRPC 2 changed its wire format, so clients on version 1 could not talk to a version 2 hub, and starting on the line going forward avoids a coordinated migration later.

## Considered options

- Hono alone, with its validators and typed client: the client is tied to the server's route types, and OpenAPI needs another package.
- Elysia: its Cloudflare Workers adapter is experimental, OpenAPI type generation does not work there, and its client is typed from the server instead of a separate contract.
- Classic Zod: about 80 KB more in the Worker bundle; Zod Mini shares the Zod core that Better Auth already brings and converts to JSON Schema natively.
