import { ORPCError } from "@orpc/server";
import type { Kysely } from "kysely";

import { getOrganization, type Auth } from "./auth/auth.ts";
import { RUNNER_KEY_CONFIG } from "./auth/options.ts";
import type { ApikeyId, Database, RunnerId } from "./db/types.ts";

// A Runner starts with a key shared by many Runners and registers once, receiving a key of its own
// that it keeps and connects with, as GitHub Actions and Buildkite runners do. The hub records which
// key belongs to which Runner and looks the Runner up by the key, so one Runner cannot connect as
// another. The link is not kept in the key's metadata, which Better Auth lets clients edit.

/** Registers a Runner named `name`, returning its ID and its own key. */
export async function registerRunner(auth: Auth, db: Kysely<Database>, name: string) {
  const organization = await getOrganization(db);
  // Better Auth creates an organization's key on behalf of a member allowed to; the owner always is.
  const owner = await db
    .selectFrom("member")
    .select("userId")
    .where("organizationId", "=", organization.id)
    .where("role", "=", "owner")
    .executeTakeFirstOrThrow();
  const runnerId = crypto.randomUUID() as RunnerId;
  // The key first: the Runner's row references it.
  const key = await auth.api.createApiKey({
    body: {
      configId: RUNNER_KEY_CONFIG,
      organizationId: organization.id,
      userId: owner.userId,
      name: "Runner",
      permissions: { runner: ["connect"] },
    },
  });
  await db
    .insertInto("runner")
    .values({ id: runnerId, name, lastSeenAt: new Date(), keyId: key.id as ApikeyId })
    .execute();
  return { id: runnerId, key: key.key };
}

/** The Runner whose own key has the ID `keyId`, if any. */
export async function findRunnerByKey(
  db: Kysely<Database>,
  keyId: ApikeyId,
): Promise<RunnerId | undefined> {
  const runner = await db
    .selectFrom("runner")
    .select("id")
    .where("keyId", "=", keyId)
    .executeTakeFirst();
  return runner?.id;
}

/**
 * Removes the Runner `runnerId` by deleting its own key through Better Auth, which checks that the
 * member signed in with `headers` may delete the Organization's keys; the database deletes the
 * Runner's row with its key. The caller drops the Runner's connection.
 */
export async function removeRunner(
  auth: Auth,
  db: Kysely<Database>,
  headers: Headers,
  runnerId: RunnerId,
): Promise<void> {
  const runner = await db
    .selectFrom("runner")
    .select("keyId")
    .where("id", "=", runnerId)
    .executeTakeFirst();
  if (!runner) throw new ORPCError("NOT_FOUND");
  await auth.api.deleteApiKey({
    headers,
    body: { keyId: runner.keyId, configId: RUNNER_KEY_CONFIG },
  });
}
