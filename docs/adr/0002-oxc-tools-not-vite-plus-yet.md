# oxc tools and Vitest 4, not Vite+ yet

We lint with oxlint (type-aware through tsgolint, built on TypeScript 7), format with oxfmt, test with Vitest 4, and build with tsdown, installed as separate tools. Vite+ bundles the same tools, but it ships Vitest 5, and Cloudflare's Workers test pool (`@cloudflare/vitest-pool-workers`) supports only Vitest 4, so tests that must run inside workerd could not run under `vp test`. Tools built on TypeScript's classic compiler API (typescript-eslint, ts-morph, some declaration bundlers) are ruled out regardless, because TypeScript 7 exposes only unstable programmatic APIs.

The repo is therefore scaffolded with each part's official generator rather than `vp create`: create-cloudflare for Worker packages, create-tsdown for the CLI, and `tsc --init` for the shared strict tsconfig, followed by minimal edits. Output from `vp create vite:monorepo` serves as a reference for layout and conventions, so the eventual move stays small.

## Consequences

When the Workers pool supports Vitest 5 (cloudflare/workers-sdk#15618), re-scaffold onto Vite+ while keeping the source: try `vp migrate` first, or regenerate with `vp create` and move the code across. Record that move in a new ADR that supersedes this one.
