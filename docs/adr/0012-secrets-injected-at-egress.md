# Secrets are injected at egress; agents never see their values

An agent can leak anything it can read: into its messages, which the hub stores, into output files, or to a host a malicious page or document tricks it into contacting. So secrets for the services a job uses (OCR, scraping, databases) are stored by the organization with the hosts each may be sent to, a job names the secrets it may use, and the workspace holds only placeholders. Every outbound request from the workspace passes through Cloudflare Computer's `http-gateway` egress mode, where the runner adds the real value for allowed hosts; Cloudflare Containers' outbound interception covers HTTPS. Model provider keys never enter the workspace at all, because the agent loop runs in the runner and makes model calls itself.

## Considered options

- Real values as environment variables in the workspace: every tool works unchanged, but the agent can read and leak them.
- Secret values sent with each job: they travel in requests and are stored with jobs.
- A separate credential-injecting proxy: adds a second proxy and a second secret store on every host, duplicating egress control that Computer and workerd already provide.

## Consequences

Injection covers request headers and URLs; an API that needs a key elsewhere, such as in a request body, needs its own handling. Secret values may later come from an external secret manager instead of being stored by the hub, without changing how jobs use them.
