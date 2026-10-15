import type { Kysely } from "kysely";

// Migrations stay as written once released, so they name the schema here instead of importing it.
// Names follow Better Auth's conventions, whose tables share the schema: singular table names and
// camelCase columns (ADR 0020).

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema.createSchema("agent_batch").ifNotExists().execute();
  await db.schema
    .withSchema("agent_batch")
    .createTable("runner")
    .addColumn("id", "text", (col) => col.primaryKey())
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("lastSeenAt", "timestamptz", (col) => col.notNull())
    // The Runner's own API key, from which the hub takes the Runner's identity.
    .addColumn("keyId", "text", (col) => col.notNull().unique())
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.withSchema("agent_batch").dropTable("runner").execute();
}
