# Better Auth provides members, SSO and API keys

The hub's identities come from Better Auth on the hub's Postgres: members sign in through the organization's identity provider (OIDC first, SAML later), the CLI signs in with a device login, and services use organization-owned API keys that are named, scoped and revocable. Every job records who submitted it. Its organization, SSO, API key and device authorization plugins cover these needs without authentication code of our own.

## Considered options

- Writing our own token handling: small at first, but members, SSO, scoped keys and device login together are a large, security-sensitive subsystem.
- Cloudflare Access in front of the hub: it cannot protect a hub on localhost or outside Cloudflare, so it stays an optional outer layer for deployments that want it.
