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
 * Runs `work` with a Kysely instance of its own, closing its connections when `work` ends.
 *
 * Workers forbid using a connection in any request but the one that opened it, so every request
 * opens its own; Hyperdrive pools the real connections to Postgres.
 */
export async function withDatabase<T>(
  connectionString: string,
  work: (db: Kysely<Database>) => Promise<T>,
): Promise<T> {
  const db = new Kysely<Database>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString }) }),
  }).withSchema(DB_SCHEMA);
  try {
    return await work(db);
  } finally {
    await db.destroy();
  }
}
