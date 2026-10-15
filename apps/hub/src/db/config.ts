import { defineConfig } from "@mikro-orm/postgresql";

import { entities } from "./entities.ts";

/**
 * The Postgres schema holding the hub's tables. The hub may share a database with other software, so
 * it keeps to its own schema, and every statement names it: Hyperdrive resets `search_path` after
 * each transaction.
 */
export const DB_SCHEMA = "agent_batch";

/** MikroORM options shared by the Worker, migrations and the CLI. */
export const baseConfig = defineConfig({
  entities,
  schema: DB_SCHEMA,
  migrations: {
    path: "./migrations",
    pathTs: "./migrations",
    // Only `migration:create` writes the snapshot; applying migrations (in tests, or on deploy) must
    // not rewrite files in the source tree from whichever database it ran against.
    snapshotOnMigrate: false,
  },
});
