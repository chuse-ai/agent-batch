# A Batch has one ID, which its caller may set

A Batch has one `id`, which names it in its path, its controls (`/batches/{id}/pause` and the rest), a job's `batchId` and the jobs list's `batchId` filter. The caller may set it when creating the Batch; when it is left out, the hub makes a UUIDv4. Creating a Batch again with the same `id` and the same content, its `agent`, `labels` and breaker as it resolves, returns the Batch already created, so a creation can be retried safely; different content is refused with 422. A Batch has no `customId`. The hub keeps its own UUIDv7 as the row's key, `rowId`, which other tables refer to and Batches are locked in the order of (ADR 0033); the API never shows it.

A Batch is a managed object: jobs are added to it over time, and it is paused, resumed and cancelled, with an agent and a breaker of its own, closer to a Kubernetes Job than to a one-shot batch request. Where a platform lets its caller name such an object, the field called `id`, if any, is the address: Cloudflare Workflows has `id` alone, set by the caller or generated; Kubernetes has `name` beside a `uid` it shows; Temporal has a `workflowId` beside a `runId`. A name the caller sets beside a server ID that is the address matches none of them, and leaves two IDs a caller must tell apart. A shown system ID, as Kubernetes's `uid`, serves to tell apart objects recreated under one name, which cannot happen while Batches are never deleted; adding it later changes nothing callers use.

The generated ID is a UUIDv4, as AWS Step Functions generates a UUID for an execution with no name and Stripe recommends a v4 UUID for an idempotency key; the ordering a UUIDv7 gives is the row key's job. The syntax is Cloudflare Workflows' for an instance ID: 1 to 100 letters, digits, `_` and `-`, not beginning with `-`, compared exactly. Every character is carried as it is in a URL path, and the form is near the `custom_id` AI batch APIs take (Anthropic's is `^[a-zA-Z0-9_-]{1,64}$`). Compared exactly, `Invoices` and `invoices` are two Batches.

`jobs submit` makes a Batch of one under the job's custom ID, so the custom ID it takes follows the Batch ID's syntax: a run again under the same custom ID then finds the same Batch.

## Considered options

- A caller's `customId` beside a hub-made `id` that is the address, as before: two IDs for one Batch, the caller's not usable in paths, and no precedent among the platforms above.
- `name` with a shown `uid`, as Kubernetes: only worth it once Batches can be deleted and their names reused.
- A server ID only, as OpenAI's batches: its reasons, one ID scheme across many object types and global uniqueness at scale, hardly hold for a hub scoped to its Organization, and a script retrying a creation would need a key of its own beside the ID.
- A UUIDv7 as the generated ID: its time order is the row key's job, and an outward ID needs only to be unique.
- Lowercase, DNS-style IDs, as Kubernetes and Google's AIP-122: further from what callers of AI batch APIs already use. Broad Unicode with a list of forbidden characters, as Step Functions and Temporal: an ID would need escaping in paths.
- Zod's `base64url` or `nanoid` formats for the syntax: the first adds length rules of its own, the second a fixed length of 21.

## Consequences

Looking up a Batch by its ID reads its row by the unique `(id, organizationId)` index, the ID first since a deployment serves one Organization (ADR 0007). Reconsider whether an ID can be used again, and whether to show a `uid`, when Batches can be deleted: AWS Step Functions keeps an execution's name for 90 days, Cloudflare an instance ID until its retention ends. A list of Batches, when one comes, can page by `after=<id>`, the hub continuing from that Batch's row key.
