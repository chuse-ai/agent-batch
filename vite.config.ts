import { defineConfig } from "vite-plus";

export default defineConfig({
  fmt: {
    // Off by default in Oxfmt; on, so import order is the formatter's, not a review point.
    sortImports: true,
  },
  staged: {
    "*": "vp check --fix",
  },
  lint: {
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    categories: { correctness: "error", suspicious: "error" },
    rules: {
      "vite-plus/prefer-vite-plus-imports": "error",
      // An assertion that narrows is a claim the compiler cannot check; parse or narrow instead.
      "typescript/no-unsafe-type-assertion": "error",
      "typescript/no-non-null-assertion": "error",
      // Zod Mini names its default wrapper `_default`, as `default` is reserved.
      "no-underscore-dangle": ["error", { allow: ["_default"] }],
    },
    overrides: [
      {
        // A pending timer keeps a Durable Object from hibernating, so it is billed for as long as the
        // Runner stays connected; see RunnerConnection.
        files: ["apps/hub/src/runner-connection.ts"],
        rules: {
          "no-restricted-globals": [
            "error",
            ...["setTimeout", "setInterval"].map((name) => ({
              name,
              message:
                "Timers keep RunnerConnection from hibernating; it schedules nothing itself, Runners' syncs bring the work.",
            })),
          ],
        },
      },
    ],
    options: { typeAware: true, typeCheck: true },
  },
  run: {
    cache: true,
  },
});
