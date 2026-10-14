# Runners on Durable Objects reduce pi-durable's row writes

On Cloudflare, Durable Object SQLite is billed per row written, index entries and deletes included, and pi-durable's journal accounts for about 95% of a job's writes (the workspace's files for about 5%). Runners therefore set pi-durable's progress interval to one second instead of its 100 ms default, and apply two patches to pi-durable through `pnpm patch`: the task upsert sets only the columns that change after a task is created (status, record and the abort flag), so each state change writes three rows instead of five; and the `document_revisions_by_kind` index is dropped, because a document's revisions are read through the primary key anyway. Measured on a 30-step scripted job, rows written fell from 6,455 to 3,950 with the interval and to 2,921 with the patches, 55% fewer in all; a job killed mid-stream still resumed with each completed tool call run exactly once. The patches are meant to be proposed upstream.

## Considered options

- pi-durable's JSONL storage engine over an append-only table: 48% fewer rows, but it keeps the whole history in memory, which grows with job length inside a 128 MB Durable Object.
- Recomputing the commit counters on open instead of updating them on every commit: the commit sequence is not stored on every record, so it could be reused after a crash.
- A longer progress interval: 10 seconds saved only 11% more than one second, at the cost of a slower live view.
