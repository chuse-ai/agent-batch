# One version for the whole system, released by release-please

agent-batch is released as one version: the hub's and the Runner's images, the `agent-batch` CLI on npm and every workspace package take the release's version. release-please manages it as one root component: it reads the conventional commits on `main` since the last release, keeps a release PR with the next version and changelog, applies the version to every `package.json`, and creates the `vX.Y.Z` tag and GitHub release when that PR merges. Before 1.0 a breaking change bumps the minor version (`bump-minor-pre-major`), so no commit moves the system to 1.0 by itself; a commit whose body has `Release-As: x.y.z` sets the next release's version instead.

The CLI is published at every release, even when it has not changed, so `agent-batch@X` is always the CLI for hub X. Callers match the command to their hub by version alone, at the cost of npm releases that differ only in their version.

## Considered options

- Per-package versions with Changesets: each PR would add a changeset restating what its conventional commits already record, and the CLI's version would no longer say which hub it belongs to.
- pnpm's own release management (`pnpm change` with `pnpm version -r`): nothing flags a PR that records no change intent; `maxBump` fails a breaking change before 1.0 rather than capping it, and must be removed by hand at 1.0; in lockstep the per-package changelogs come out thin; applying the intents left the root `package.json` behind; and intents duplicate the conventional commits.
- Publishing the CLI only when it changes: its versions would drift from the hub's, and a caller could not tell from a version which CLI matches a hub.

## When to reconsider

When a package needs its own release cadence, such as the published runner package ADR 0004 anticipates, or once Vite+'s release command (voidzero-dev/vite-plus#1180) is available.
