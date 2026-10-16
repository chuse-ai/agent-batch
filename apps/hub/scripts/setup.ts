import { migrateDatabase } from "../src/db/migrate.ts";
import { setUpOrganization } from "../src/setup.ts";

// FIXME: operators get the CLI, not this repository (ADR 0004), so this command moves into the
// CLI once it exists.
// Prepares the hub's database at `DATABASE_URL`: applies migrations, then creates the Organization
// and its owner from `ADMIN_EMAIL` and `ADMIN_PASSWORD`, unless the Organization exists. A hub on
// Workers has no start-up of its own, so this runs as a deploy step; the Docker hub does the same as
// it starts.
const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  console.error("Set DATABASE_URL to the hub's database");
  process.exit(1);
}
try {
  await migrateDatabase(databaseUrl);
  await setUpOrganization(databaseUrl, {
    email: process.env.ADMIN_EMAIL,
    password: process.env.ADMIN_PASSWORD,
  });
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}
