const { makePgTsGenerator, markAsGenerated } = require("kanel");
const { kyselyTypeFilter, makeKyselyHook } = require("kanel-kysely");

/** The contract's type for each JSON column, as ADR 0020 asks, by table and column. */
const JSON_COLUMN_TYPES = { job: { outputContract: "OutputContract" } };

/** Types a JSON column with the contract's type for it, importing it from the contract package. */
function getPropertyMetadata(property, details, generateFor, builtinMetadata) {
  const type = JSON_COLUMN_TYPES[details.name]?.[property.name];
  if (!type) return builtinMetadata;
  const typeImport = {
    name: type,
    path: "contract",
    isAbsolute: true,
    isDefault: false,
    importAsType: true,
  };
  return { ...builtinMetadata, typeOverride: { name: type, typeImports: [typeImport] } };
}

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
  generators: [makePgTsGenerator({ getPropertyMetadata, preRenderHooks: [makeKyselyHook()] })],
  postRenderHooks: [markAsGenerated],
};
