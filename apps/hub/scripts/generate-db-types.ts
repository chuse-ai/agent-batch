import { execFileSync } from "node:child_process";

import { PostgreSqlContainer } from "@testcontainers/postgresql";

import { migrateDatabase } from "../src/db/migrate.ts";

// Runs Kanel against a fresh database migrated to the latest version.
const container = await new PostgreSqlContainer("postgres:18-alpine").start();
try {
  await migrateDatabase(container.getConnectionUri());
  execFileSync("kanel", {
    stdio: "inherit",
    env: { ...process.env, DATABASE_URL: container.getConnectionUri() },
  });
} finally {
  await container.stop();
}
