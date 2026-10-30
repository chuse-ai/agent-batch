# Input files are uploaded before the jobs that name them

A job names each Input file by `{ path, digest, sizeBytes }`, where `digest` is `sha256:` and the hex SHA-256 of its bytes, and the request that adds jobs stays JSON. The content is uploaded first. `POST /blobs/check` takes up to 1,000 `{ digest, sizeBytes }` and answers, for each, that it is stored or where to upload it; `PUT /blobs/{digest}` takes one file's raw bytes as `application/octet-stream`, at most 100 MiB, with its `Content-Length`. The hub streams the body into chunks as it reads it, verifies its size and SHA-256, and only then publishes it, in one transaction, as a blob (ADR 0031). A caller hashes its files, checks them, uploads only what is missing, and adds its jobs.

An Organization holds content while its upload is under seven days old, each upload renewing that, or while one of its own jobs or Batches uses it. It is told content is stored, and may name it in a job, only while it holds it, though the bytes are stored once however many Organizations upload them: a digest alone is no proof of having the bytes. A submission locks the blobs it names and refuses any it does not hold. A sweep, hourly from a Cron Trigger on Workers and a scoped fiber on Node, deletes expired holds, the chunks of uploads that never published within a day, and blobs nothing holds or uses. A job's deletion still deletes at once the content nothing else holds, so a deleted job's content outlasts it only while an upload holds it.

oRPC hands a route declared with `requestBodyHint: "octet-stream"` its body as a stream without reading it, and `RequestLimitHandlerPlugin` caps the bytes read per handler, so the hub serves uploads from a second `OpenAPIHandler` capped at 100 MiB and every other request from one capped at 16 MiB. The check's upload target is a `url` with `transfer: "put"`, so the target can change without changing the check.

## Considered options

- Files inside the submission as `multipart/form-data`: one request per submission, but the hub reads the whole request into memory before validating it, which on Node peaked at 2 to 4.5 times the body, so the cap stays small; bracket notation loses empty objects, arrays and `null`; and a file every job shares is sent with each job.
- Base64 content in JSON: a third larger on the wire, held encoded and decoded at once, and still bound by the body cap.
- Opaque file IDs returned by an upload, as OpenAI's and Anthropic's Files APIs do: a caller would keep a map from its files to IDs to avoid uploading twice, and the hub could not tell a caller its content is already stored before the bytes are sent. A digest is computed locally, so a resubmission uploads nothing.
- A resumable protocol such as tus now: more mechanism than a single request of 100 MiB needs; a failed upload is sent again whole.
- Presigned URLs to an object store: the hub stores bytes in Postgres (ADR 0031) and has no object store.

## Consequences

Adding jobs with files takes at least two requests, and three when content is missing. Uploads arrive one per request, each opening its own database connection on Workers, so a caller bounds how many it sends at once. A Batch's agent has no Input files yet; when it has, it names them by digest the same way. Reconsider with tus, when a file needs more than 100 MiB or uploads over unreliable links must resume; its claimed digest would be kept apart from the verified `upload.sha256`.
