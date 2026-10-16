import { externals } from "nf3/plugin";
import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    // The Node hub, which the Docker image runs; the Worker is built by Wrangler.
    entry: ["src/main.ts"],
    dts: false,
    // nf3, Nitro's tracer, copies only the third-party files the hub loads into `dist/node_modules`,
    // manifests included, so image scanners still see each package. `contract` exports TypeScript
    // source, so it is bundled instead (ADR 0006).
    deps: { alwaysBundle: ["contract"] },
    plugins: [externals({ exclude: ["contract"] })],
  },
  test: {
    globalSetup: ["./tests/global-setup.ts"],
    // Test files share one Postgres, so they run one at a time.
    fileParallelism: false,
    // Tests of the Docker image need Docker and an image build, so the default run skips them;
    // `vp test --tags-filter=container` runs only them (ADR 0003).
    tags: [{ name: "container", timeout: 600_000 }],
    tagsFilter: ["!container"],
    // The hub runs on Workers and in Docker alike, so the suite runs against each (ADR 0003), each
    // project with a Postgres of its own.
    projects: [
      { extends: true, test: { name: "workers", provide: { hub: "workers" } } },
      { extends: true, test: { name: "node", provide: { hub: "node" } } },
    ],
  },
});
