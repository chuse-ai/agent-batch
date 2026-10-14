# Workspace packages resolve each other's source through a custom condition

Inside the repo, packages import each other's TypeScript source through a namespaced `@chuse-ai/source` export condition, enabled by the repo's tsconfig and tooling; published packages ignore it and resolve to `dist/`. Changes show up across packages without a build step, and TypeScript 7's speed removes the main reason to use project references.

## Consequences

Every tool that resolves imports in the repo (type checker, test runner, the Worker bundler) must be configured with the condition, or it silently falls back to `dist/`.
