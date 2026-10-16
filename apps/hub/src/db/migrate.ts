import { promises as fs } from "node:fs";
import path from "node:path";

import { Kysely, PostgresDialect } from "kysely";
import { FileMigrationProvider, Migrator } from "kysely/migration";
import { Pool } from "pg";

import { DB_SCHEMA } from "./database.ts";

/**
 * Applies all pending migrations to the database at `connectionString`, from `migrationFolder`. Runs
 * in Node, not in the Worker.
 */
export async function migrateDatabase(
  connectionString: string,
  migrationFolder = path.join(import.meta.dirname, "../../migrations"),
) {
  await using db = new Kysely<unknown>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString }) }),
  });
  const migrator = new Migrator({
    db,
    provider: new FileMigrationProvider({
      fs,
      path,
      migrationFolder,
    }),
    // Kysely's own tables, which record the applied migrations, live with the hub's.
    migrationTableSchema: DB_SCHEMA,
  });
  const { error } = await migrator.migrateToLatest();
  if (error) throw error;
}
