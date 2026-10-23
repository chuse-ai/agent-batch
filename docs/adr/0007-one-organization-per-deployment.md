# One organization self-hosts each deployment; the organization owns everything

agent-batch is built for company use. Each company runs its own deployment, on its own machines and Cloudflare account, for its own people and services; companies never share one. Everything a deployment stores (jobs, members, keys, Runners) belongs to an organization from the start, even though a deployment has only one, so ownership is explicit everywhere and serving more than one organization would not mean reworking it.

## Consequences

Members, teams, roles and SSO are part of the core design, not later additions. Isolation between organizations is not built while a deployment serves only one.
