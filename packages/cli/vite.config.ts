import path from "node:path";
import { fileURLToPath } from "node:url";

import rollupPluginLicense from "rollup-plugin-license";
import { defineConfig } from "vite-plus";

// rollup-plugin-license exports the function itself (`module.exports`), but its types declare an
// ES default export, which TypeScript, resolving as Node does, reads as an object holding it
// (mjeanroy/rollup-plugin-license#2113).
const license =
  "default" in rollupPluginLicense ? rollupPluginLicense.default : rollupPluginLicense;

// Optional peers of trpc-cli this CLI does not use, tRPC and Valibot, which trpc-cli imports inside
// a `catch`: they stay out of the bundle and are never installed.
const UNUSED_PEERS = ["@trpc/server", "valibot", "@valibot/to-json-schema"];

export default defineConfig({
  run: {
    tasks: {
      // Vite Task does not cache a task that reads a file it also writes, as the copy below does
      // with README.md, so README.md is left out of the inputs; it stays an output, which a cache
      // hit puts back. The package's `prepack` runs `vp pack` rather than this task: Vite Task inlines
      // a `vp run` written in a task's command, but one started by a process a task runs, as pnpm
      // runs `prepack` when the packed test packs, fails to spawn (voidzero-dev/vite-task#119).
      build: { command: "vp pack", cache: { input: [{ auto: true }, "!README.md"] } },
    },
  },
  pack: {
    // The package is the `agent-batch` command alone: a library export would freeze internals that
    // are still changing (ADR 0004), so it exports none, and needs no type declarations.
    entry: ["src/bin.ts"],
    dts: false,
    // In the workspace the bin points to the source, as the other packages' exports do (ADR 0006),
    // so the hub's tests run the command unbuilt; publishing takes `publishConfig`'s, which points
    // to `dist`.
    exports: { devExports: true, bin: true, exclude: ["bin"] },
    // The bundle carries everything the command imports, its own dev dependencies and `contract`
    // with what `contract` imports, as create-vite's does: one package to install, with exactly the
    // versions its tests ran. Besides the unused peers, the output may import only Node's
    // built-ins, so a package left out of the bundle fails the build.
    deps: { onlyBundle: false, neverBundle: UNUSED_PEERS, onlyImport: UNUSED_PEERS },
    // Parts of trpc-cli and Effect this CLI imports but never runs, each carrying a copy of another
    // library without the licence notice the bundle needs, so each becomes an empty module, `false`
    // in rolldown's aliases; tsdown's own `alias` takes only paths. Of trpc-cli: module mode, which
    // makes commands of a module's functions and carries TypeBox, as this CLI passes a router; the
    // converter of Zod 3 schemas, zod-to-json-schema, as its schemas are all Zod 4; and cli-table,
    // which trpc-cli's logger draws only for a list of objects in a command's result, as these
    // commands print their own and return none. Of Effect, which trpc-cli imports whole for its
    // `Schema`: `Random`, whose seeded generator copies ISAAC, and `Arbitrary`, which imports it.
    // TODO: remove the aliases of module mode, zod-to-json-schema and Effect once trpc-cli keeps
    // them out of a router's bundle, as mmkal/trpc-cli#232 proposes: module mode on an entry of its
    // own, converters passed in, only `effect/Schema` imported. If it declines the last, patch the
    // import, which puts about 2.9 MB of Effect in the bundle that this CLI never runs.
    // TODO: remove the alias of cli-table before a command returns its result for trpc-cli to
    // print, as the fix of mmkal/trpc-cli#228 would let these commands do.
    // Two more parts this CLI never uses stay in the bundle: all of Zod's locales, about 270 KB,
    // which trpc-cli's import of Zod's core brings and the converters mmkal/trpc-cli#232 proposes
    // would leave out, and json-rpc-2.0, about 30 KB, which the Runner protocol in `contract` uses.
    // It is CommonJS without `"sideEffects": false`, so the bundle runs it although it leaves out
    // the module that imports it; an entry of `contract`'s own for the Runner protocol would leave
    // it out too.
    inputOptions: {
      resolve: {
        alias: {
          [fileURLToPath(import.meta.resolve("trpc-cli/dist/module-commands.js"))]: false,
          [fileURLToPath(import.meta.resolve("trpc-cli/dist/zod-to-json-schema/index.js"))]: false,
          [fileURLToPath(import.meta.resolve("trpc-cli/dist/cli-table.js"))]: false,
          [fileURLToPath(import.meta.resolve("effect/Random"))]: false,
          [fileURLToPath(import.meta.resolve("effect/Arbitrary"))]: false,
        },
      },
    },
    // npm shows the package's README as its page, so the package carries the repository's: a copy,
    // since npm and pnpm leave a symlink pointing outside the package out of the tarball. `to` puts
    // it at the package root, where npm looks; without it, it would go to `dist`.
    copy: [{ from: "../../README.md", to: "." }],
    // The licences of the packages the bundle carries, beside it.
    plugins: [
      license({
        thirdParty: { output: path.join(import.meta.dirname, "dist/THIRD_PARTY_NOTICES.txt") },
      }),
    ],
  },
});
