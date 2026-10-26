# Text Postgres cannot store is refused, not cleaned

In a UTF-8 database Postgres refuses two things in `text` and `jsonb`: NUL (U+0000) and half of a surrogate pair, which JSON can carry as an escape such as `\ud800`; it stores every other code point (checked on Postgres 18). node-postgres does not refuse a half pair in a `text` parameter but writes U+FFFD in its place, so the stored text would differ from what was sent. `textSchema` refuses both, and `jsonTextSchema` applies it to every string and key of a JSON value, wherever text enters:

| Source                                                        | What happens to such text                                                                                                                                                                 | Why                                                                                    |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| A caller: prompts, Input files, file names, result contracts  | The request is refused, naming the field                                                                                                                                                  | The caller can fix it and send again                                                   |
| A model's output in a job's events                            | That job's report is refused                                                                                                                                                              | Never seen from a provider; cleaning would alter the transcript                        |
| A tool's streamed output, such as a command's                 | Never arrives: pi-durable bounds it with `OutputBuffer` from `@earendil-works/chord`, which decodes invalid UTF-8 to U+FFFD and drops control characters, NUL included (`sanitizeOutput`) | The source most likely to carry it, a binary file printed, is handled where it is read |
| A provider's error text: a failure's message, a retry's error | That job's report is refused                                                                                                                                                              | As for model output                                                                    |
| The Runner's own cut of a failure's message                   | Repaired with `toWellFormed`, since the cut can split a pair                                                                                                                              | The damage is ours                                                                     |

A refused report is refused for its job alone and logged by the hub and the Runner (ADR 0026). The Runner sends it again in each pass over every job, so the job's later events, its end included, never reach the hub, and it shows as running there.

## Considered options

- Cleaning where outside text enters, as the Runner did for a provider's error text: it alters data silently, and one field cleaned while the events beside it are refused is one more rule to keep in mind.
- A schema that cleans as it parses: the hub would rewrite what it receives in some fields only, and the transform has no place in the OpenAPI document the schemas produce.
- Storing such text as `bytea` or escaped: every reader would decode it, for text no source is known to send.
- Leaving out a message that is not storable text: it loses the provider's whole explanation to keep a rule no provider is known to break.

## Consequences

Reconsider when a report is refused for this text from a pi source: then clean once, for all text that comes from pi, rather than per field.
