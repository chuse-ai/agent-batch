import { ORPCError, type ORPCErrorCode } from "@orpc/server";
import { betterAuth } from "better-auth";
import { isAPIError } from "better-auth/api";
import { HubNotSetUp, Unauthorized } from "contract";
import { Cause, Context, Effect, Layer } from "effect";
import type { Kysely } from "kysely";
import * as z from "zod/mini";

import { Db, openScopedDatabase } from "../db/database.ts";
import type { Database } from "../db/types.ts";
import { authOptions, type AuthSettings } from "./options.ts";

/** A Better Auth instance with the hub's options. */
type BetterAuth = ReturnType<typeof betterAuth<ReturnType<typeof authOptions>>>;

/** Better Auth on the hub's database. */
export class Auth extends Context.Service<
  Auth,
  {
    /**
     * Calls Better Auth. Its errors become oRPC's, which name statuses as Better Auth's do, such as
     * `BAD_REQUEST` and `NOT_FOUND`; oRPC would answer one it does not know with a 500.
     */
    readonly call: <A>(
      run: (betterAuth: BetterAuth) => Promise<A>,
    ) => Effect.Effect<A, ORPCError<ORPCErrorCode, unknown> | Cause.UnknownError>;
  }
>()("hub/Auth", {
  /** Better Auth for the hub, on the database `db`. */
  make: (db: Kysely<Database>, settings: Omit<AuthSettings, "db">) =>
    Effect.sync(() => {
      const instance = betterAuth(authOptions({ ...settings, db }));
      return {
        call: (run) =>
          Effect.tryPromise({
            try: () => run(instance),
            catch: (error) =>
              isAPIError(error)
                ? new ORPCError(String(error.status), { message: error.body?.message })
                : new Cause.UnknownError(error),
          }),
      };
    }),
}) {}

/**
 * The hub's database and Better Auth on it, for the Layer's lifetime. On Workers the Layer is built
 * per request: instances reused across requests can wait forever after a cancelled request leaves a
 * promise they share unsettled (better-auth/better-auth#10315, better-auth/better-auth#11651).
 */
export function databaseAndAuthLayer(connectionString: string, settings: Omit<AuthSettings, "db">) {
  return Layer.effectContext(
    Effect.gen(function* () {
      const db = yield* openScopedDatabase(connectionString);
      return Context.make(Db, yield* Db.make(db)).pipe(
        Context.add(Auth, yield* Auth.make(db, settings)),
      );
    }),
  );
}

/**
 * The deployment's Organization (ADR 0007), which owns every member and key, or `undefined` before
 * the hub is set up. There is only one, so the first row is it.
 */
export async function findOrganization(db: Kysely<Database>) {
  return await db.selectFrom("organization").select("id").executeTakeFirst();
}

/** The deployment's Organization, or a 503 error before the hub is set up. */
export const getOrganization = Effect.gen(function* () {
  const database = yield* Db;
  const organization = yield* database.query(findOrganization);
  if (!organization) {
    return yield* Effect.fail(new HubNotSetUp());
  }
  return organization;
});

/** The signed-in member making the request, or a 401 error. */
export function requireMember(headers: Headers) {
  return Effect.gen(function* () {
    const auth = yield* Auth;
    const session = yield* auth.call((betterAuth) => betterAuth.api.getSession({ headers }));
    if (!session) return yield* Effect.fail(new Unauthorized());
    return session.user;
  });
}

/**
 * The API key sent in `x-api-key`, verified to hold `permissions`, or a 401 error. Better Auth answers
 * a key lacking the permissions as it answers an unknown key, so the answer reveals nothing about it.
 */
export function requireKey(headers: Headers, permissions: Record<string, string[]>) {
  return Effect.gen(function* () {
    const key = headers.get("x-api-key");
    if (!key) return yield* Effect.fail(new Unauthorized());
    const auth = yield* Auth;
    const result = yield* auth.call((betterAuth) =>
      betterAuth.api.verifyApiKey({ body: { key, permissions } }),
    );
    if (!result.valid || !result.key) return yield* Effect.fail(new Unauthorized());
    return result.key;
  });
}

/**
 * What the hub records in the metadata of a key a member creates: the member, whom Better Auth does
 * not keep for an Organization's key. Better Auth types metadata as any object, so the hub checks it.
 */
export const keyMetadataSchema = z.object({ createdBy: z.string() });
export type KeyMetadata = z.infer<typeof keyMetadataSchema>;

/**
 * The API key sent in `x-api-key`, verified to hold `permissions`, or else the signed-in member; a
 * 401 error if neither is given.
 */
export function requireKeyOrMember(headers: Headers, permissions: Record<string, string[]>) {
  return headers.has("x-api-key") ? requireKey(headers, permissions) : requireMember(headers);
}
