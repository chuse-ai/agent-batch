# agent-batch

Run hundreds of one-off agent jobs unattended, each with a result contract. On your own machines first, spilling over to Cloudflare® Workers.

## API

The hub serves its OpenAPI document at `/spec.json`, generated from the contract, and a browsable reference of it at `/`. Each call takes an API key in the `x-api-key` header holding the permissions the document lists for it, a member's session cookie, or either; errors are RFC 9457 Problem Details.

## Development

- Check everything is ready:

```bash
vp run ready
```

- Run the tests:

```bash
vp run -r test
```

- Build the monorepo:

```bash
vp run -r build
```

- Run the development server:

```bash
vp run dev
```
