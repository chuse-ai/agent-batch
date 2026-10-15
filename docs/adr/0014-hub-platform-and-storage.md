# The hub runs on Workers first, with Postgres and S3-compatible storage

The hub runs on Cloudflare Workers first and is written to Web standards, with Durable Objects used only to hold runner connections, so the same code can later ship as a Docker image for Railway, a VPS, Kubernetes, or a company's own machine. Its only database is Postgres, reached through Hyperdrive on Workers and directly elsewhere. Its tables live in their own schema, `agent_batch`, named in every statement, so the hub can share a database with other software; Hyperdrive resets `search_path` after each transaction, so the schema cannot be set once per connection. Files and full event histories go to S3-compatible object storage, R2 by default; input files are uploaded straight to it through presigned URLs. Events land in Postgres as runners send them, and a timer compacts them every 30 to 60 seconds into chunked JSON Lines objects per job, so the database holds only recent events while complete histories live in object storage.

## Considered options

- D1: one SQLite database with a single writer, near its limits from the event stream at around a thousand concurrent jobs; billed per row written, so its cost grows with load; unavailable outside Cloudflare, so a Docker hub would need a second SQL dialect; and without interactive transactions.
- A Durable Object per job holding that job's events and state, with D1 as an index: scales furthest, but is billed per row and per request and exists only on Cloudflare.
- Supporting both SQLite (D1 or a file) and Postgres: a common pattern in self-hosted software, but a test matrix of two dialects across two runtimes before the first feature.
- Keeping full histories in the database: tens of gigabytes a day at a thousand concurrent jobs.
- Runners uploading history chunks to object storage themselves: the events already reach the hub, and the loss window would grow from one batch to one chunk.
- A log or stream store on object storage, or Cloudflare's Pipelines: an extra service in every deployment, or one that exists only on Cloudflare; such stores fit better as optional destinations for the compacted chunks.

## Consequences

`agent-batch dev` runs Postgres locally next to the Docker setup the container backend already needs. The chunk layout is documented, so tools such as DuckDB can query histories in place, and optional event sinks (OpenTelemetry export, analytics) can consume the chunks. A Docker hub holds runner connections with oRPC's Node WebSocket adapter instead of Durable Objects. So Durable Objects only hold connections (sockets, alarms, hibernation); what the hub records and decides about runners and jobs lives in modules that use no Cloudflare APIs, and a Docker hub replaces only the connection layer. If load ever outgrows one Postgres database, the per-job event store can move to Durable Objects behind the same interface. If Workers prove unsuitable for the hub, the Docker image is the first alternative considered.
