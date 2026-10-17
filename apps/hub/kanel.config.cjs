const { makePgTsGenerator, markAsGenerated } = require("kanel");
const { kyselyTypeFilter, makeKyselyHook } = require("kanel-kysely");

const OUTPUT_PATH = "./src/db/generated";

/** The contract's type for each JSON column, as ADR 0020 asks, by table and column. */
const JSON_COLUMN_TYPES = { job: { outputContract: "OutputContract", result: "JsonValue" } };

/**
 * Columns holding another table's ID without referencing it, by table and column: the row's ID is
 * copied so that it outlives the row. Kanel brands only referencing columns, so these are branded here.
 */
const COPIED_ID_COLUMNS = {
  job: { runnerId: "Runner", submitterKeyId: "Apikey", submitterMemberId: "Member" },
};

/**
 * Types a JSON column with the contract's type for it, importing it from the contract package, and a
 * copied ID column with the ID type of the table it copies, as Kanel types a referencing column.
 */
function getPropertyMetadata(property, details, generateFor, builtinMetadata) {
  const jsonType = JSON_COLUMN_TYPES[details.name]?.[property.name];
  if (jsonType) {
    const typeImport = {
      name: jsonType,
      path: "contract",
      isAbsolute: true,
      isDefault: false,
      importAsType: true,
    };
    return { ...builtinMetadata, typeOverride: { name: jsonType, typeImports: [typeImport] } };
  }
  const table = COPIED_ID_COLUMNS[details.name]?.[property.name];
  if (table) {
    const typeImport = {
      name: `${table}Id`,
      path: `${OUTPUT_PATH}/${details.schemaName}/${table}`,
      isAbsolute: false,
      isDefault: false,
      importAsType: true,
    };
    return { ...builtinMetadata, typeOverride: { name: `${table}Id`, typeImports: [typeImport] } };
  }
  return builtinMetadata;
}

/**
 * Declares a table's ID type with Effect's brand instead of Kanel's own `{ __brand }`, so a string
 * becomes an ID through a `Brand` constructor (ADR 0020), not an assertion. The built-in definition
 * is `<type> & { __brand: '<schema>.<table>' }`; its first part is the column's own type.
 */
function generateIdentifierType(column, details, builtinType) {
  const [innerType] = builtinType.typeDefinition[0].split(" & ");
  return {
    ...builtinType,
    typeDefinition: [`Brand.Branded<${innerType}, "${details.schemaName}.${details.name}">`],
    typeImports: [
      ...builtinType.typeImports,
      { name: "Brand", path: "effect", isAbsolute: true, isDefault: false, importAsType: true },
    ],
  };
}

/** Generates the hub's Kysely types from the database at `DATABASE_URL`, migrated to the latest version. */
/** @type {import("kanel").ConfigV4} */
module.exports = {
  connection: process.env.DATABASE_URL,
  schemaNames: ["agent_batch"],
  filter: kyselyTypeFilter,
  outputPath: OUTPUT_PATH,
  // Removes files for dropped tables, so CI notices when they are still committed.
  preDeleteOutputFolder: true,
  typescriptConfig: { enumStyle: "literal-union", tsModuleFormat: "esm" },
  generators: [
    makePgTsGenerator({
      getPropertyMetadata,
      generateIdentifierType,
      preRenderHooks: [makeKyselyHook()],
    }),
  ],
  postRenderHooks: [markAsGenerated],
};
