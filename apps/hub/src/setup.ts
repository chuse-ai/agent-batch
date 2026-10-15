import { betterAuth, type BetterAuthOptions } from "better-auth";
import type { Kysely } from "kysely";

import { findOrganization } from "./auth/auth.ts";
import { offlineAuthOptions } from "./auth/options.ts";
import { DB_SCHEMA, openDatabase } from "./db/database.ts";
import type { Database } from "./db/types.ts";

// Setting up a hub follows Grafana, Authentik and Keycloak: environment variables create the
// Organization and its owner once, and are ignored once the Organization exists. A forgotten
// password is reset with a command run with database access, as with Grafana's
// `grafana cli admin reset-admin-password` and GitLab's `gitlab:password:reset`.

/** The owner to create, from `ADMIN_EMAIL` and `ADMIN_PASSWORD`. */
export interface OwnerSettings {
  email?: string | undefined;
  password?: string | undefined;
}

/**
 * Creates the Organization and its owner unless the Organization exists. Throws when there is no
 * Organization and no owner settings, so a hub is never left without one.
 */
export async function setUpOrganization(
  connectionString: string,
  owner: OwnerSettings,
  log: (message: string) => void = console.info,
): Promise<void> {
  await using db = openDatabase(connectionString);
  if (await findOrganization(db.withSchema(DB_SCHEMA))) {
    if (owner.email) log("The Organization exists, so ADMIN_EMAIL and its password are ignored");
    return;
  }
  if (!owner.email || !owner.password) {
    throw new Error("Set ADMIN_EMAIL and ADMIN_PASSWORD to create the Organization's owner");
  }
  // Better Auth commits the owner's `user` row before the Organization is created, and cannot
  // include both in one transaction (better-auth/better-auth#10609). A run stopped in between leaves
  // the row behind, so a later run uses it instead of signing up again.
  const auth = offlineAuth(db, {
    // Public sign-up stays off; only this instance may sign up the owner.
    disableSignUp: false,
    autoSignIn: false,
  });
  const userId =
    (await findUserId(db, owner.email)) ??
    (
      await auth.api.signUpEmail({
        body: { email: owner.email, password: owner.password, name: "Owner" },
      })
    ).user.id;
  await auth.api.createOrganization({
    body: { name: "Organization", slug: "organization", userId },
  });
  log(`Created the Organization and its owner, ${owner.email}`);
}

/** Sets a new password for the member with `email`, for a member who has forgotten theirs. */
export async function resetPassword(
  connectionString: string,
  member: { email: string; password: string },
) {
  await using db = openDatabase(connectionString);
  // Better Auth resets a password with a token it would email; this instance keeps the token instead.
  let token: string | undefined;
  const auth = offlineAuth(db, {
    sendResetPassword: async (data) => {
      token = data.token;
    },
  });
  await auth.api.requestPasswordReset({ body: { email: member.email } });
  if (!token) throw new Error(`No member has the email ${member.email}`);
  await auth.api.resetPassword({ body: { newPassword: member.password, token } });
}

/** The ID of the `user` row with `email`, if any; Better Auth stores emails in lower case. */
async function findUserId(db: Kysely<Database>, email: string): Promise<string | undefined> {
  const user = await db
    .withSchema(DB_SCHEMA)
    .selectFrom("user")
    .select("id")
    .where("email", "=", email.toLowerCase())
    .executeTakeFirst();
  return user?.id;
}

/** Better Auth for setup and recovery, with the hub's options and `emailAndPassword` overrides. */
function offlineAuth(
  db: Kysely<Database>,
  emailAndPassword: Partial<NonNullable<BetterAuthOptions["emailAndPassword"]>>,
) {
  const options = offlineAuthOptions(db);
  return betterAuth({
    ...options,
    emailAndPassword: { ...options.emailAndPassword, ...emailAndPassword },
  });
}
