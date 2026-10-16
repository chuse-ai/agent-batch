import { type Cause, Context, Effect, Layer } from "effect";
import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";

import type { Database } from "./types.ts";

/**
 * The Postgres schema holding the hub's tables. The hub may share a database with other software, so
 * it keeps to its own schema, and every statement names it: Hyperdrive resets `search_path` after
 * each transaction.
 */
export const DB_SCHEMA = "agent_batch";

/** The hub's database. */
export class Db extends Context.Service<
  Db,
  {
    /** Runs one Kysely call on the hub's database, naming its schema in every statement. */
    readonly query: <A>(
      run: (db: Kysely<Database>) => Promise<A>,
    ) => Effect.Effect<A, Cause.UnknownError>;
  }
>()("hub/Db", {
  /** The service on `db`, a Kysely instance that does not name the schema itself. */
  make: (db: Kysely<Database>) => {
    const inSchema = db.withSchema(DB_SCHEMA);
    return Effect.succeed({ query: (run) => Effect.tryPromise(() => run(inSchema)) });
  },
}) {}

/**
 * Opens a Kysely instance on the hub's database without naming the schema, for Better Auth, which
 * names it itself. Close it with `destroy()`, or declare it with `await using`.
 */
export function openDatabase(connectionString: string): Kysely<Database> {
  return new Kysely<Database>({
    dialect: new PostgresDialect({ pool: new Pool({ connectionString }) }),
  });
}

/** Opens a Kysely instance on the hub's database for the scope, closing its connections with it. */
export function openScopedDatabase(connectionString: string) {
  return Effect.acquireRelease(
    Effect.sync(() => openDatabase(connectionString)),
    (db) => Effect.promise(() => db.destroy()),
  );
}

/**
 * The hub's database for a Layer's lifetime. Workers forbid using a connection in any request but
 * the one that opened it, so there the Layer is built per request; Hyperdrive pools the real
 * connections to Postgres.
 */
export function databaseLayer(connectionString: string) {
  return Layer.effect(Db, Effect.flatMap(openScopedDatabase(connectionString), Db.make));
}
