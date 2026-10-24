# hub

The central service every caller and Runner connects to (ADR 0008). It runs on Cloudflare Workers, with Postgres through Hyperdrive, and as a Node process in a Docker image, with Postgres directly (ADR 0014).

## Settings

On Workers, set them in `wrangler.jsonc` and as Wrangler secrets; for `wrangler dev`, copy `.dev.vars.example` to `.dev.vars`. In Docker, set them as environment variables.

- `BETTER_AUTH_SECRET` (required): signs members' sessions. Generate one with `openssl rand -base64 32`.
- `HUB_URL` (required): the hub's public URL, the same setting a Runner dials. Better Auth uses it as its base URL, for its links, its trusted origin and whether session cookies are `Secure`. Without either, the hub logs what to set: in Docker it exits, and on Workers it answers every request but `/healthz` with 503.
- `CLIENT_IP_HEADER`: the request header carrying the client's IP address, for Better Auth's rate limits: `cf-connecting-ip` on Workers, `x-real-ip` on Railway. Unset, Better Auth reads `x-forwarded-for`, and a request whose address it cannot resolve shares one rate-limit bucket per path.
- `REPORT_INTERVAL_MS`: how often each Runner reports its jobs' new events, usage and activity, in milliseconds; 3000 by default. A job's activity is up to one interval old.
- `TRUSTED_PROXIES`: the proxies in front of the hub, as IPs or CIDRs separated by commas. A forwarded chain is read from the right past these, so a client cannot name its own address; unset, only a chain of one address counts.
- `JOB_RUN_TIMEOUT_SECONDS_DEFAULT` and `JOB_RUN_TIMEOUT_SECONDS_MAX`: the run timeout a job gets when it gives none, and the most it may give, in seconds; 1800 and 86400 by default.
- `JOB_MODEL_BUDGET_USD_DEFAULT` and `JOB_MODEL_BUDGET_USD_MAX`: the model budget a job gets when it gives none, and the most it may give, in US dollars; 1 and 10 by default.

A job's limits are resolved when it is submitted, so lowering a maximum reaches no job already submitted. A default above its maximum is refused as a missing setting is.

In Docker, also:

- `DATABASE_URL` (required): the hub's Postgres database.
- `ADMIN_EMAIL` and `ADMIN_PASSWORD`: the owner to create on first start, when no Organization exists (ADR 0015). The hub applies migrations and sets up as it starts, then ignores both.
- `PORT`: where the hub listens; 8787 by default.

## Docker

Each release publishes the hub's image as `ghcr.io/chuse-ai/agent-batch-hub`. Run one container per hub: it runs as a single process (ADR 0014). Its health check fetches `/healthz`, which answers once the hub serves requests; platforms that ignore the image's check, such as Kubernetes, can probe the same path. To build it from the repository's root:

```sh
docker build . --target hub --tag agent-batch-hub
```

`vp run build` builds the same Node hub into `dist/`, and `vp run start` runs it.

## Scripts

Run with `DATABASE_URL` set to the hub's database.

- `vp run setup`: applies migrations, then creates the Organization and its owner from `ADMIN_EMAIL` and `ADMIN_PASSWORD`, unless the Organization exists (ADR 0015). Run it before deploying.
- `vp run reset-password`: sets a new password, `ADMIN_PASSWORD`, for the member with `ADMIN_EMAIL`.

These start Postgres themselves with Testcontainers:

- `vp run db:types`: regenerates the database types from the migrations (ADR 0020).
- `vp run db:draft-auth-migration`: prints the SQL Better Auth still needs, to copy into a migration after a Better Auth upgrade.
