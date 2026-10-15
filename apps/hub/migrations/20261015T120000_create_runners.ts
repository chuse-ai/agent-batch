import type { Kysely } from "kysely";

// Migrations stay as written once released, so they name the schema here instead of importing it.

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema.createSchema("agent_batch").ifNotExists().execute();
  await db.schema
    .withSchema("agent_batch")
    .createTable("runners")
    .addColumn("id", "text", (col) => col.primaryKey())
    .addColumn("name", "text", (col) => col.notNull())
    .addColumn("last_seen_at", "timestamptz", (col) => col.notNull())
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.withSchema("agent_batch").dropTable("runners").execute();
}
