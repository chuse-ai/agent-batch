# pi-durable's row writes are reduced

_Runners keep their journals on local disk (ADR 0019): the two patches were never applied and are dropped, and the one-second progress interval stands, for the reason given below._

On Cloudflare, Durable Object SQLite is billed per row written, index entries and deletes included, and pi-durable's journal accounts for about 95% of a job's writes (the workspace's files for about 5%). Runners therefore set pi-durable's progress interval to one second instead of its 100 ms default, and apply two patches to pi-durable through `pnpm patch`: the task upsert sets only the columns that change after a task is created (status, record and the abort flag), so each state change writes three rows instead of five; and the `document_revisions_by_kind` index is dropped, because a document's revisions are read through the primary key anyway. Measured on a 30-step scripted job, rows written fell from 6,455 to 3,950 with the interval and to 2,921 with the patches, 55% fewer in all; a job killed mid-stream was still recovered with each completed tool call run exactly once. The patches are meant to be proposed upstream.

Runners are Node processes (ADR 0019), where these writes go to local disk and cost nothing. The patches are dropped there: each would need redoing at every pi-durable upgrade, and the index is not redundant, since SQLite plans pi-durable's lookup of a document's latest base revision with it (`storage/sqlite/storage.js` in 1.3.0). The progress interval stays at one second for the Runner's CPU: the saves run on the thread all of a Runner's jobs share, and with 100 jobs streaming answers at once for about 10.6 seconds, they took 3.3 seconds of CPU at 100 ms, 1.9 at 500 ms, 1.5 at one second and 1.1 with no saves. On a Runner only a restart reads them, and it then loses at most a second of the output of a command it cut off. The billing reason applies to jobs run inside Durable Objects, such as Cloudflare Computer.

## Considered options

- pi-durable's JSONL storage engine over an append-only table: 48% fewer rows, but it keeps the whole journal in memory, which grows with job length inside a 128 MB Durable Object.
- Recomputing the commit counters on open instead of updating them on every commit: the commit sequence is not stored on every record, so it could be reused after a crash.
- A longer progress interval: 10 seconds saved only 11% more than one second, at the cost of a slower live view.
