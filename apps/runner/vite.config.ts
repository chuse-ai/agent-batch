import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    // The package exports its TypeScript source, like `contract` (ADR 0006); the build is only for
    // starting a Runner with `start`.
    entry: ["src/main.ts"],
    dts: false,
    // `contract` exports TypeScript source, which the built Runner must not have to load.
    deps: { alwaysBundle: ["contract"] },
  },
});
