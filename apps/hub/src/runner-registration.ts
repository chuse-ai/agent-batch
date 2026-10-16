import { NotFound } from "contract";
import { Effect } from "effect";

import { Auth, getOrganization } from "./auth/auth.ts";
import { RUNNER_KEY_CONFIG } from "./auth/options.ts";
import { Db } from "./db/database.ts";
import type { ApikeyId, RunnerId } from "./db/types.ts";

// A Runner starts with a key shared by many Runners and registers once, receiving a key of its own
// that it keeps and connects with, as GitHub Actions and Buildkite runners do. The hub records which
// key belongs to which Runner and looks the Runner up by the key, so one Runner cannot connect as
// another. The link is not kept in the key's metadata, which Better Auth lets clients edit.

/** Registers a Runner named `name`, returning its ID and its own key. */
export function registerRunner(name: string) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const auth = yield* Auth;
    const organization = yield* getOrganization;
    // Better Auth creates an organization's key on behalf of a member allowed to; the owner always is.
    const owner = yield* database.query((db) =>
      db
        .selectFrom("member")
        .select("userId")
        .where("organizationId", "=", organization.id)
        .where("role", "=", "owner")
        .executeTakeFirstOrThrow(),
    );
    const runnerId = crypto.randomUUID() as RunnerId;
    // The key first: the Runner's row references it.
    const key = yield* auth.call((betterAuth) =>
      betterAuth.api.createApiKey({
        body: {
          configId: RUNNER_KEY_CONFIG,
          organizationId: organization.id,
          userId: owner.userId,
          name: "Runner",
          permissions: { runner: ["connect"] },
        },
      }),
    );
    yield* database.query((db) =>
      db
        .insertInto("runner")
        .values({ id: runnerId, name, lastSeenAt: new Date(), keyId: key.id as ApikeyId })
        .execute(),
    );
    return { id: runnerId, key: key.key };
  });
}

/** The Runner whose own key has the ID `keyId`, if any. */
export function findRunnerByKey(keyId: ApikeyId) {
  return Db.use((database) =>
    database.query((db) =>
      db.selectFrom("runner").select("id").where("keyId", "=", keyId).executeTakeFirst(),
    ),
  ).pipe(Effect.map((runner) => runner?.id));
}

/**
 * Removes the Runner `runnerId` by deleting its own key through Better Auth, which checks that the
 * member signed in with `headers` may delete the Organization's keys; the database deletes the
 * Runner's row with its key. The caller drops the Runner's connection.
 */
export function removeRunner(headers: Headers, runnerId: RunnerId) {
  return Effect.gen(function* () {
    const database = yield* Db;
    const auth = yield* Auth;
    const runner = yield* database.query((db) =>
      db.selectFrom("runner").select("keyId").where("id", "=", runnerId).executeTakeFirst(),
    );
    if (!runner) return yield* Effect.fail(new NotFound());
    yield* auth.call((betterAuth) =>
      betterAuth.api.deleteApiKey({
        headers,
        body: { keyId: runner.keyId, configId: RUNNER_KEY_CONFIG },
      }),
    );
  });
}
