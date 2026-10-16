# The API answers with bare resources and RFC 9457 errors

The HTTP API is used directly from any language, not only through the TypeScript client and the CLI, so its responses follow standards rather than oRPC's own format. A successful response is the resource itself, without an envelope, as Stripe, GitHub, OpenAI, Anthropic, Google's API design rules (AIP-131) answer; Cloudflare Workflows' binding, the model for our conventions (ADR 0010), returns bare objects too. An error is an RFC 9457 Problem Details document, `application/problem+json`, with `type: "about:blank"`: the HTTP status carries the meaning, and `title` and `detail` are for people. A problem gets a `type` URI of its own only once two errors share a status and callers must tell them apart. Inside the hub and the TypeScript client, errors keep oRPC's codes, one per status, so code still branches on `UNAUTHORIZED` or `NOT_FOUND`.

oRPC supports this through its own hooks and plugins: the handler's error body encoder, the link's decoder, and oRPC's response validation plugin, which marks errors the contract declares for a call as typed, as oRPC's handler does. Two of oRPC's defaults assume its own format and are overridden: its fetch adapter labels every JSON body `application/json`, so the hub sets the Problem Details type on error responses, and its client reads only that type as JSON by itself, so the contract tells it that every body of the API is JSON. The shared link lives in the contract package, so every TypeScript client gets it from one place.

The Runner protocol is not part of the API: both ends are ours. The hub's refusal of a Runner it does not know keeps its own small body, a 401 with `{ "code": "runnerNotFound" }`.

## Considered options

- oRPC's own error format, `{ defined, code, message, data }`: no other API uses it, `defined` describes oRPC's internals, and it changes with oRPC's major versions (version 2 dropped `status`), which would break callers' generated clients.
- Cloudflare's v4 envelope, `{ success, result, errors, messages }`: the minority among comparable APIs, and Cloudflare's own Workflows binding does not use it.
- An `{ error: { code, message } }` body, as Stripe, OpenAI and Azure answer: the most common shape, but each API defines its own fields, so ours would be a format of our own.
- A `code` member or a `type` URI for each error: a second identifier beside the status, or URIs to keep stable, for errors that the status already tells apart.
- Problem Details sent as `application/json`: less code, but tools identify Problem Details by its media type.

## Consequences

Paths outside the API answer 404 as Problem Details too, while Better Auth's own routes under `/api/auth` keep Better Auth's error format. When the hub publishes its OpenAPI document, the error responses there are described as Problem Details, correcting the generator's `application/json`.
