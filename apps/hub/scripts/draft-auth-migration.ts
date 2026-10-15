import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { PostgreSqlContainer } from "@testcontainers/postgresql";

import { migrateDatabase } from "../src/db/migrate.ts";

// Prints the SQL Better Auth still needs on a database migrated to the latest version, drafted by its
// CLI. The SQL goes into a hand-written migration, reviewed like any other (ADR 0020); run this
// after every Better Auth upgrade or plugin change. With `--check`, as in CI, it fails when SQL is
// still needed.
const container = await new PostgreSqlContainer("postgres:18-alpine").start();
try {
  await migrateDatabase(container.getConnectionUri());
  const output = path.join(mkdtempSync(path.join(tmpdir(), "auth-schema-")), "schema.sql");
  execFileSync(
    "auth",
    ["generate", "--config", "scripts/auth.config.ts", "--output", output, "--yes"],
    {
      stdio: "inherit",
      env: { ...process.env, DATABASE_URL: container.getConnectionUri() },
    },
  );
  // The CLI writes no file when the schema already has everything Better Auth needs.
  if (existsSync(output)) {
    process.stdout.write(readFileSync(output, "utf8"));
    if (process.argv.includes("--check")) {
      console.error("Better Auth needs the SQL above: add it in a migration");
      process.exitCode = 1;
    }
  }
} finally {
  await container.stop();
}
