# json-schema-library checks Result contracts on the hub and results on the Runner

A Result contract is a JSON Schema a caller submits at runtime, so it is data, not code we write. The hub checks the schema when a job is submitted, and the Runner will validate each result against it (ADR 0009). Both use json-schema-library, so a schema the hub accepts is read the same way when results are checked.

It was chosen on four grounds. It interprets schemas without generating code, which Workers forbid (`eval` and `new Function` are unavailable), and which would turn caller-supplied schemas into code. On the official JSON Schema Test Suite for 2020-12 it passed 1,249 of 1,266 required tests (remote references and vocabularies left out), more than any other validator measured. It checks the schema itself, reporting problems such as `type: 5` in `schemaErrors` rather than throwing. And it never fetches a schema over the network; a reference it cannot resolve is an error.

A schema without `$schema` is read as 2020-12, the library's default. The hub adds no remote schemas, so it refuses any `$ref` or `$dynamicRef` that does not resolve within the schema: the library's own check covers only references into `$defs`, so the hub resolves every reference with `resolveRef` over the nodes `toSchemaNodes` lists, as validation does.

## Considered options

- Ajv and @exodus/schemasafe (1,197 and 1,201 tests passed): both generate code, which Workers forbid and which caller-supplied schemas should not become.
- @cfworker/json-schema (1,204 passed): smaller (about 5 KiB gzipped against 31 KiB) and built for Workers, but its errors were noisy and partly wrong, and it has had no release since January 2025.
- @hyperjump/json-schema: a global schema registry, and it fetches unknown references over the network, which on the hub would let a caller make it send requests.
- Zod's `fromJSONSchema` (691 passed) and jsonschema (979 passed): too far from the specification.

## Consequences

The library does not validate a schema against the 2020-12 meta-schema, so a few invalid keyword values, such as `minLength: -1`, pass the hub's check; they only make the schema accept more. `resolveRef`, which the hub relies on for references, is typed as returning a node although it returns an error for a reference it cannot resolve, and the hub checks for that error explicitly. The library has a single maintainer, active since 2016; a replacement would need to pass the same suite and keep these properties.
