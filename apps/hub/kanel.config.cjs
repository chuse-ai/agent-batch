const { makePgTsGenerator, markAsGenerated } = require("kanel");
const { kyselyTypeFilter, makeKyselyHook } = require("kanel-kysely");

/** Generates the hub's Kysely types from the database at `DATABASE_URL`, migrated to the latest version. */
/** @type {import("kanel").ConfigV4} */
module.exports = {
  connection: process.env.DATABASE_URL,
  schemaNames: ["agent_batch"],
  filter: kyselyTypeFilter,
  outputPath: "./src/db/generated",
  // Removes files for dropped tables, so CI notices when they are still committed.
  preDeleteOutputFolder: true,
  typescriptConfig: { enumStyle: "literal-union", tsModuleFormat: "esm" },
  generators: [makePgTsGenerator({ preRenderHooks: [makeKyselyHook()] })],
  postRenderHooks: [markAsGenerated],
};
