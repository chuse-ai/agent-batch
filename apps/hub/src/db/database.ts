import { type Cause, Context, Effect, Layer } from "effect";
import ensureError from "ensure-error";
import { Kysely, PostgresDialect } from "kysely";
import { Pool, TypeOverrides, types } from "pg";

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
    dialect: new PostgresDialect({ pool: openPool(connectionString) }),
  });
}

/**
 * How the hub reads Postgres values: as pg does, except that an `int8`, such as a count or a sum of
 * integers, becomes a number, as pg-types' README suggests, and one beyond `Number.MAX_SAFE_INTEGER`
 * fails its query instead of being rounded. A `numeric` stays a string.
 */
const typeParsers = new TypeOverrides();
typeParsers.setTypeParser(types.builtins.INT8, (text) => {
  const value = Number(text);
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(`The int8 ${text} is beyond what a JavaScript number holds exactly`);
  }
  return value;
});

/**
 * A connection pool to the hub's database. An idle connection that fails, as when Postgres restarts,
 * is reported on the pool; without a listener Node would end the process. The pool drops that
 * connection, and the next query opens a new one.
 */
export function openPool(connectionString: string): Pool {
  const pool = new Pool({ connectionString, types: typeParsers });
  pool.on("error", (error) => {
    console.error("A database connection failed:", ensureError(error).message);
  });
  return pool;
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
