# Contributing

agent-batch does not accept outside code contributions for now: only the repository's collaborators can open pull requests.

Issues are welcome: a bug report with the steps that reproduce it, or a request that describes what you need.

## Releasing

To release, merge the release PR that release-please keeps open. It publishes the hub's and the Runner's images and stages the `agent-batch` CLI on npm; approve the staged version on npmjs.com, or with `npm stage approve`, to publish it. To release a version other than the one it proposes, merge a commit whose body has `Release-As: x.y.z`.
