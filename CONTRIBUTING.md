# Contributing

agent-batch does not accept outside code contributions for now: only the repository's collaborators can open pull requests.

Issues are welcome: a bug report with the steps that reproduce it, or a request that describes what you need.

## Releasing

release-please releases agent-batch from the conventional commits on `main`. It keeps a release PR open with the next version, applied to every `package.json`, and the changelog. Merging that PR tags `vX.Y.Z` and creates the GitHub release, which publishes the hub's and the Runner's images and the `agent-batch` CLI. Until 1.0, a breaking change bumps the minor version. To release a different version, merge a commit whose body has `Release-As: x.y.z`.
