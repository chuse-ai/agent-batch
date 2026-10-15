# hub

The central service every caller and Runner connects to (ADR 0008), on Cloudflare Workers with Postgres through Hyperdrive (ADR 0014).

## Settings

For `wrangler dev`, copy `.dev.vars.example` to `.dev.vars`.

- `BETTER_AUTH_SECRET` (required): signs members' sessions. Generate one with `openssl rand -base64 32`.
- `HUB_URL` (required): the hub's public URL, the same setting a Runner dials. Better Auth uses it as its base URL, for its links, its trusted origin and whether session cookies are `Secure`. Without either, the hub answers every request with 503 and says what to set.
- `CLIENT_IP_HEADER`: the request header carrying the client's IP address, for Better Auth's rate limits; `cf-connecting-ip` on Workers.

## Scripts

Run with `DATABASE_URL` set to the hub's database.

- `vp run setup`: applies migrations, then creates the Organization and its owner from `ADMIN_EMAIL` and `ADMIN_PASSWORD`, unless the Organization exists (ADR 0015). Run it before deploying.
- `vp run reset-password`: sets a new password, `ADMIN_PASSWORD`, for the member with `ADMIN_EMAIL`.

These start Postgres themselves with Testcontainers:

- `vp run db:types`: regenerates the database types from the migrations (ADR 0020).
- `vp run db:draft-auth-migration`: prints the SQL Better Auth still needs, to copy into a migration after a Better Auth upgrade.
