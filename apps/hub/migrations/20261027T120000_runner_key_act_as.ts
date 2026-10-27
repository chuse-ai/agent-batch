import { type Kysely, sql } from "kysely";

// A Runner's own key grants `runner:actAs`, which was `runner:connect` until it also covered the
// Runner's HTTP calls. Better Auth stores a key's permissions as JSON text, so each stored Runner
// key is rewritten, or it would stop working. Where the previous hub still serves after the
// migration, as on Workers until the deploy, a Runner reconnecting then is refused as unknown and
// exits; it reconnects once restarted.

export async function up(db: Kysely<unknown>): Promise<void> {
  await renameRunnerAction(db, "connect", "actAs");
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await renameRunnerAction(db, "actAs", "connect");
}

/** Renames the action `from` to `to` in every stored key's `runner` permissions. */
async function renameRunnerAction(db: Kysely<unknown>, from: string, to: string) {
  await sql`
    update agent_batch.apikey
    set permissions = jsonb_set(
      permissions::jsonb,
      '{runner}',
      (
        select jsonb_agg(case when action = ${from} then ${to} else action end order by position)
        from jsonb_array_elements_text(permissions::jsonb -> 'runner')
          with ordinality as actions(action, position)
      )
    )::text
    where permissions::jsonb -> 'runner' ? ${from}
  `.execute(db);
}
