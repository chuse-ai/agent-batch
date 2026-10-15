import type { Kysely } from "kysely";

// A Runner's row goes with its own key: deleting the key, through the hub or through Better Auth,
// deletes the Runner in the same statement. Better Auth's tables are created after the Runner's, so
// the reference is added here.

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .withSchema("agent_batch")
    .alterTable("runner")
    .addForeignKeyConstraint("runner_keyId_fkey", ["keyId"], "apikey", ["id"], (constraint) =>
      constraint.onDelete("cascade"),
    )
    .execute();
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .withSchema("agent_batch")
    .alterTable("runner")
    .dropConstraint("runner_keyId_fkey")
    .execute();
}
