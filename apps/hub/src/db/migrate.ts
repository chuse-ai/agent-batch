import { Migrator } from "@mikro-orm/migrations";
import { MikroORM } from "@mikro-orm/postgresql";

import { baseConfig } from "./config.ts";

/** Applies all pending migrations to the database at `clientUrl`. Runs in Node, not in the Worker. */
export async function migrateDatabase(clientUrl: string) {
  const orm = await MikroORM.init({ ...baseConfig, clientUrl, extensions: [Migrator] });
  try {
    await orm.migrator.up();
  } finally {
    await orm.close();
  }
}

/** Deletes all rows from every table, keeping the schema. Runs in Node, for tests. */
export async function clearDatabase(clientUrl: string) {
  const orm = await MikroORM.init({ ...baseConfig, clientUrl });
  try {
    await orm.schema.clear();
  } finally {
    await orm.close();
  }
}
