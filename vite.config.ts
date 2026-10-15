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
    rules: { "vite-plus/prefer-vite-plus-imports": "error" },
    options: { typeAware: true, typeCheck: true },
  },
  run: {
    cache: true,
  },
});
