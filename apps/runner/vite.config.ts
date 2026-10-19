import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      // Not cached: Vite Task traces a cached task's file reads by preloading a library into every
      // process it starts (`LD_PRELOAD`), which the jobs' sandboxes cannot load, so their commands
      // would print its errors.
      test: { command: "vp test", cache: false },
    },
  },
  pack: {
    // The package exports its TypeScript source, like `contract` (ADR 0006); the build is only for
    // starting a Runner with `start`.
    entry: ["src/main.ts"],
    dts: false,
    // `contract` exports TypeScript source, which the built Runner must not have to load.
    deps: { alwaysBundle: ["contract"] },
  },
});
