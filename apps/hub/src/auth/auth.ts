import { ORPCError } from "@orpc/server";
import { betterAuth } from "better-auth";
import type { Kysely } from "kysely";

import { DB_SCHEMA, openDatabase } from "../db/database.ts";
import type { Database } from "../db/types.ts";
import { authOptions, type AuthSettings } from "./options.ts";

/** The settings the hub's Better Auth instances take from its deployment. */
export type AuthEnvironment = Omit<AuthSettings, "db"> & { connectionString: string };

/** Better Auth for the hub, on the request's database. */
export function createAuth(db: Kysely<Database>, settings: Omit<AuthSettings, "db">) {
  return betterAuth(authOptions({ ...settings, db }));
}
export type Auth = ReturnType<typeof createAuth>;

/**
 * Runs `work` with Better Auth and the hub's database, both its own, closing the connections when
 * `work` ends. Each request builds its own Better Auth instance: its database connection is the
 * request's own, and on Workers, instances reused across requests can wait forever after a cancelled
 * request leaves a promise they share unsettled (better-auth/better-auth#10315, better-auth/better-auth#11651).
 */
export async function withAuth<T>(
  environment: AuthEnvironment,
  work: (auth: Auth, db: Kysely<Database>) => Promise<T>,
): Promise<T> {
  const { connectionString, ...settings } = environment;
  await using db = openDatabase(connectionString);
  return await work(createAuth(db, settings), db.withSchema(DB_SCHEMA));
}

/**
 * The deployment's Organization (ADR 0007), which owns every member and key, or `undefined` before
 * the hub is set up. There is only one, so the first row is it.
 */
export async function findOrganization(db: Kysely<Database>) {
  return await db.selectFrom("organization").select("id").executeTakeFirst();
}

/** The deployment's Organization, or a 503 error before the hub is set up. */
export async function getOrganization(db: Kysely<Database>) {
  const organization = await findOrganization(db);
  if (!organization)
    throw new ORPCError("SERVICE_UNAVAILABLE", { message: "The hub is not set up" });
  return organization;
}

/** The signed-in member making the request, or a 401 error. */
export async function requireMember(auth: Auth, headers: Headers) {
  const session = await auth.api.getSession({ headers });
  if (!session) throw new ORPCError("UNAUTHORIZED");
  return session.user;
}

/**
 * The API key sent in `x-api-key`, verified to hold `permissions`, or a 401 error. Better Auth answers
 * a key lacking the permissions as it answers an unknown key, so the answer reveals nothing about it.
 */
export async function requireKey(
  auth: Auth,
  headers: Headers,
  permissions: Record<string, string[]>,
) {
  const key = headers.get("x-api-key");
  if (!key) throw new ORPCError("UNAUTHORIZED");
  const result = await auth.api.verifyApiKey({ body: { key, permissions } });
  if (!result.valid || !result.key) throw new ORPCError("UNAUTHORIZED");
  return result.key;
}
