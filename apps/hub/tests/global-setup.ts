import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { TestProject } from "vite-plus/test/node";

import { migrateDatabase } from "../src/db/migrate.ts";

declare module "vite-plus/test" {
  interface ProvidedContext {
    databaseUrl: string;
  }
}

/** Starts one migrated Postgres for the test run; the hub reaches it through Hyperdrive's local connection string. */
export default async function setup(project: TestProject) {
  const container = await new PostgreSqlContainer("postgres:18-alpine").start();
  await migrateDatabase(container.getConnectionUri());
  project.provide("databaseUrl", container.getConnectionUri());
  return async () => {
    await container.stop();
  };
}
