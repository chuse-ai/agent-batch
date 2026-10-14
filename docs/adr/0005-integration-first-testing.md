# Integration-first testing in real workerd, no mocks

Tests run in one Vitest suite on Cloudflare's Workers pool, inside real workerd, and exercise the public API. Unit tests are written only for dense pure logic that is awkward to reach through the API. Pi and Cloudflare Computer are never mocked; pi-ai's faux model stands in for the LLM so runs stay deterministic and free.

Container tests (real programs through the container backend) live in the same suite but are tagged and skipped by default, because they need Docker and an image build: `pnpm test` excludes them, `pnpm test:containers` includes them, and CI runs them on container-related changes and nightly.
