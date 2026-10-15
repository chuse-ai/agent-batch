import { betterAuth } from "better-auth";
import { Kysely, PostgresDialect } from "kysely";
import { Pool } from "pg";

import { offlineAuthOptions } from "../src/auth/options.ts";

// The configuration Better Auth's CLI loads to draft the schema, against the database at
// `DATABASE_URL`.
export const auth = betterAuth(
  offlineAuthOptions(
    new Kysely({
      dialect: new PostgresDialect({
        pool: new Pool({ connectionString: process.env.DATABASE_URL }),
      }),
    }),
  ),
);
