import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    globalSetup: ["./tests/global-setup.ts"],
    // Test files share one Postgres, so they run one at a time.
    fileParallelism: false,
  },
});
