# Integration-first testing in real workerd, no mocks

Tests exercise the public API as outside callers: Vitest runs in Node and starts the hub and runner Workers from their own Wrangler configs in real workerd with Wrangler's `createTestHarness()`, alongside a real Postgres from Testcontainers. Unit tests are written only for dense pure logic that is awkward to reach through the API. Pi and the execution environments are never mocked; pi-ai's faux model stands in for the LLM so runs stay deterministic and free.

Container tests (real programs through the container backend) live in the same suite but are tagged and skipped by default, because they need Docker and an image build: the default `vp test` run excludes them, a separate run includes them, and CI runs them on container-related changes and nightly.
