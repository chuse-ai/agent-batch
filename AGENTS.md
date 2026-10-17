## Agent skills

### Issue tracker

Issues live in GitHub Issues for `chuse-ai/agent-batch`, used through the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five default triage labels, unchanged. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

## Conventions

- `CONTEXT.md`'s terms apply in code, comments and docs too, not only in the domain model; its _Avoid_ words, such as account, secret and token, mean something else here.
- After `vp add`, check `package.json`: with `catalogMode: prefer`, it turns `workspace:*` dependencies into `catalog:` entries; set them back to `workspace:*`.
- After changing a migration, run `vp run hub#db:types`; after upgrading Better Auth, run `vp run hub#db:draft-auth-migration`. CI fails on either otherwise.
- A comment that records a decision stays until the decision is re-opened; changing the behaviour means re-opening the decision, not deleting the comment.
- A name says what the code does, effects included; an abstract name is justified only where the difference it hides is one no caller cares about. A read-sounding name must not hide a write. Abstracting over how (a join behind `listRunners`) is fine; abstracting over what happens is not.
- A caught value becomes an `Error` through `ensure-error` before it is logged or inspected.
- A Node emitter the code holds, such as a socket, a pool, a server or a child process, gets an `error` listener when it is acquired and keeps it until it is destroyed: an unhandled `error` event ends the process, outside any fiber or promise.
- Prefer optional over `null` unless there is a domain reason, such as the database's explicit `NULL` against no update. Never remap external data to swap `null` and `undefined`; keep the source's shape.

## Schema

- Before writing the queries that use a schema change, show the maintainer its tables, columns and constraints, one table per message, and wait for approval.
- Until a migration is released, it is edited in place; no fix-up migration follows it.
- Every foreign key states, in the migration, what deleting the parent means for the row. Where the row must outlive the parent, the parent's ID is copied and not referenced, as the job's submitter and Runner are; `kanel.config.cjs` keeps such a column typed as the parent's ID.
- Every nullable column states, in the migration, why it can be null. A counter or flag is not null with a database default.
- A column carrying a protocol field uses the field's name. Every column names its reader, present or decided; what cannot be recorded later is stored before anything reads it.
- A CHECK or unique constraint reflects a rule the data must hold; do not add one to make the schema look rigorous.
- Instants are `timestamptz`, written as `Date`, never as strings. Where a table's columns record different clocks, such as the hub's and a Runner's, a comment says which clock each column records.
- Kysely write semantics: `undefined` omits the column, `null` writes SQL `NULL`, any other value is written. Do not use `value ?? null` unless omission deliberately means clearing the column.
- Inserts and updates are typed with the generated `New<Table>` and `<Table>Update` types, Kanel's `Insertable` and `Updateable` of the table; `<Table>Update` is already partial.

## Queries

- Prefer typed Kysely expressions over SQL text wherever Kysely can represent the query: CASE via `eb.case()`, arithmetic via `eb(lhs, op, rhs)`, window functions via `eb.fn.agg().over()`, `eb.fn.coalesce`, `eb.val` for a parameter and `eb.lit` only for an inline number, boolean or null, `eb.cast` for a SQL cast, `eb.fn("name", [...])` for any other function. Nested results follow ADR 0020.
- Rows from the caller enter a statement as one JSON parameter expanded with `jsonb_array_elements`, each field read with `->>` and cast; not as a union of one select per row.
- A `WITH` clause with `UNION ALL`: build each branch separately, pass `a.unionAll(b)` to `with()`, and keep the branches' column types identical.
- Merge related queries when that is clear. On the Workers hub every round trip crosses Hyperdrive, so accept more involved Kysely to remove an avoidable one, keeping transaction, lock and error semantics.
- `executeTakeFirst()` adds no `LIMIT 1`; add `.limit(1)` to a non-unique lookup.
- Filter a parent by its children with `EXISTS`, not a join and `GROUP BY`. In a grouped query every selected non-aggregate column is grouped or aggregated: Kysely type-checks what Postgres rejects.
- An empty child collection is `[]`, never an object of nulls.
- pg returns a `date` as a calendar string, a `timestamptz` as a `Date`, and a `numeric`, `SUM` and `AVG` included, as a string: convert with `Number()` before arithmetic.

## API

- The contract defines the caller-visible input, output and errors (ADR 0018); handlers stay thin and the logic lives in the hub's modules.
- Each procedure's authorisation is chosen explicitly from what it requires; nothing is served from an unguarded base.
- A route's method and full path are declared on the procedure and verified at runtime and in the generated OpenAPI document; contract nesting does not build the path.
- An output schema's nullability follows its source: a not-null column passed through is non-null, a nullable one is `.nullable()`, a computed field that may be absent is `.optional()`; a `T | null | undefined` from a module is normalised before the contract.
- A stored enum in the API matches the database enum; a value SQL widened to `string` is parsed with the schema, not asserted.

## Tests

- Add a test only for behaviour a caller depends on (ADR 0003). A fix or a new edge case is not by itself a reason for one; a comment can record why code behaves as it does.
- Assert through the interface under test. Read the database only for a persistence invariant that no public interface exposes.
- Set up state through typed APIs where one exists, and through SQL only when none does or the scenario needs internal state.
- Choose failure and recovery cases by likelihood and impact; no elaborate tests for speculative edges.

## Review

Questions for a change touching the database or the API, beside the general review:

- Do nullable and optional fields mean distinct, intentional states?
- Do writes preserve `undefined` against `null`?
- Are numeric results converted before arithmetic?
- Do insert and conflict-update omissions preserve defaults and stored values?
- Does a non-unique `executeTakeFirst()` need `.limit(1)`?
- Does nested aggregation preserve the parent's cardinality and empty arrays?
- Does every grouped query account for each selected column?
- Does each contract field exist on the handler's return value with the promised nullability and enum shape?

<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, and it invokes Vite through `vp dev` and `vp build`. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

Docs are local at `node_modules/vite-plus/docs` or online at https://viteplus.dev/guide/.

## Built-in Commands vs Scripts

`vp <name>` runs a built-in command. `vp run <name>` runs a `package.json` script or a `vite.config.ts` task. Scripts cannot overwrite built-ins, so `vp dev` and `vp run dev` may do different things. Check `package.json` and `vite.config.ts` first, and run `vp run <name>` when the project defines a script or task with that name.

## Tool Versions

Run `vp toolchain` to show versions and relationships in the active Vite+
release. Add a tool name to select part of the graph. For example, run
`vp toolchain vite`. Use `--global` to ignore the local `vite-plus` package. Use
`vp why <package>` to show the package-manager dependency graph.

## Review Checklist

- [ ] Run `vp install` after pulling remote changes and before getting started.
- [ ] Run `vp check` and `vp test` to format, lint, type check and test changes.
- [ ] Check if there are `vite.config.ts` tasks or `package.json` scripts necessary for validation, run via `vp run <script>`.
- [ ] If setup, runtime, or package-manager behavior looks wrong, run `vp env doctor` and include its output when asking for help.

<!--VITE PLUS END-->
