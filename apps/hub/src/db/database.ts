import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";

import type { Database } from "./types.ts";

/**
 * The Postgres schema holding the hub's tables. The hub may share a database with other software, so
 * it keeps to its own schema, and every statement names it: Hyperdrive resets `search_path` after
 * each transaction.
 */
export const DB_SCHEMA = "agent_batch";

/**
 * Opens a Kysely instance on the hub's database without naming the schema, for Better Auth, which
 * names it itself. Close it with `destroy()`, or declare it with `await using`.
 */
export function openDatabase(connectionString: string): Kysely<Database> {
  return new Kysely<Database>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString }) }),
  });
}

/**
 * Runs `work` with a Kysely instance of its own, closing its connections when `work` ends.
 *
 * Workers forbid using a connection in any request but the one that opened it, so every request
 * opens its own; Hyperdrive pools the real connections to Postgres.
 */
export async function withDatabase<T>(
  connectionString: string,
  work: (db: Kysely<Database>) => Promise<T>,
): Promise<T> {
  await using db = openDatabase(connectionString);
  return await work(db.withSchema(DB_SCHEMA));
}
