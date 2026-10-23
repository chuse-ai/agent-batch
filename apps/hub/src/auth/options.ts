import { apiKey } from "@better-auth/api-key";
import type { BetterAuthOptions } from "better-auth";
import { generateRandomString } from "better-auth/crypto";
import { organization } from "better-auth/plugins";
import { createAccessControl } from "better-auth/plugins/access";
import {
  adminAc,
  defaultStatements,
  memberAc,
  ownerAc,
} from "better-auth/plugins/organization/access";
import { KEY_PERMISSIONS } from "contract";
import type { Kysely } from "kysely";

import { DB_SCHEMA } from "../db/database.ts";

// Loading this module has no side effects (no environment reads or connections), so Better Auth's
// CLI can load it to draft the schema (ADR 0020).

/** What an API key or a member's role may do, in Better Auth's `{ resource: actions }` shape. */
const statements = {
  ...defaultStatements,
  // Better Auth's own names for managing organization-owned keys. Only members manage keys: Better
  // Auth lists and deletes keys for a signed-in member's session, not for an API key.
  apiKey: ["create", "read", "update", "delete"],
  ...KEY_PERMISSIONS,
} as const;

const ac = createAccessControl(statements);

/**
 * The permissions a member may grant an API key, as a role whose `authorize` checks a request. A
 * Runner's own key, with `runner: ["connect"]`, is created only by the hub when the Runner registers.
 */
export const GRANTABLE_KEY_PERMISSIONS = ac.newRole({
  runner: ["register"],
  job: KEY_PERMISSIONS.job,
});

/**
 * Better Auth's default organization roles, with key management granted to owners and admins, as in
 * Better Auth's API key docs. Its key routes let owners do everything anyway; granting it in the role
 * makes a check of the role's permissions, as the hub makes before removing a Runner, decide as they
 * do. Members have none, so keys stay with admins and owners, as Buildkite's agent tokens do.
 */
const roles = {
  owner: ac.newRole({ ...ownerAc.statements, apiKey: ["create", "read", "update", "delete"] }),
  admin: ac.newRole({ ...adminAc.statements, apiKey: ["create", "read", "update", "delete"] }),
  member: memberAc,
};

/** The API key configurations: keys for services, and each Runner's own key. */
export const API_KEY_CONFIG = "default";
export const RUNNER_KEY_CONFIG = "runner";

export interface AuthSettings {
  /** The hub's database, without a schema plugin: Better Auth names `schemaName` itself. */
  // oxlint-disable-next-line typescript/no-explicit-any -- Better Auth types its database as Kysely<any>.
  db: Kysely<any>;
  secret: string;
  /** The hub's public URL, which Better Auth uses for its routes and cookies. */
  baseURL: string;
  /**
   * The request headers that carry the client's IP address, for rate limiting; when unset, Better
   * Auth's default, `x-forwarded-for`.
   */
  ipAddressHeaders?: string[] | undefined;
  /**
   * The proxies in front of the hub, as IPs or CIDRs. Better Auth reads a forwarded chain from the
   * right, skipping these, so a client cannot name its own address; with none, only a chain of one
   * address counts.
   */
  trustedProxies: string[];
  /** Whether Better Auth's rate limits apply; tests turn them off. */
  rateLimit: boolean;
}

/** Better Auth's options for the hub. */
export function authOptions(settings: AuthSettings) {
  return {
    secret: settings.secret,
    baseURL: settings.baseURL,
    database: { db: settings.db, type: "postgres", schemaName: DB_SCHEMA },
    // Members sign in with a password (ADR 0015); only the hub's setup signs anyone up.
    emailAndPassword: {
      enabled: true,
      disableSignUp: true,
      // A reset is often because the password leaked, so whoever signed in with it is signed out.
      revokeSessionsOnPasswordReset: true,
    },
    rateLimit: {
      // Set explicitly: Better Auth enables rate limits only when `NODE_ENV` is `production`, which
      // Workers do not set.
      enabled: settings.rateLimit,
      // Counts kept in memory are per process, so they would split across hub replicas and Workers
      // isolates; the database holds one count for every instance.
      storage: "database",
    },
    advanced: {
      database: {
        // Each request builds its own instance, which would otherwise introspect the database on its
        // first auth call; the schema comes from migrations. See better-auth/better-auth#11651.
        validateSchema: false,
      },
      // Which header carries the client's address, and which proxies may have written it, depend on
      // the deployment: Cloudflare sets `cf-connecting-ip`, Railway `x-real-ip`.
      ipAddress: {
        ipAddressHeaders: settings.ipAddressHeaders,
        trustedProxies: settings.trustedProxies,
      },
    },
    // The hub's own routes manage API keys, adding which permissions may be granted and closing a
    // removed Runner's connection; Better Auth's would skip both. Its server-side calls still work.
    disabledPaths: [
      "/api-key/create",
      "/api-key/get",
      "/api-key/list",
      "/api-key/update",
      "/api-key/delete",
    ],
    logger: {
      // Better Auth's lines go to the console like the hub's own, without terminal colours, so Workers
      // Logs keeps them with the request they belong to.
      log: (level, message, ...args) => console[level](`[Better Auth] ${message}`, ...args),
    },
    plugins: [
      // One Organization per deployment (ADR 0007): only the hub's setup creates it, and nothing
      // deletes it, since a hub without it answers no member or key.
      organization({
        ac,
        roles,
        allowUserToCreateOrganization: false,
        disableOrganizationDeletion: true,
      }),
      apiKey([
        {
          configId: API_KEY_CONFIG,
          references: "organization",
          // API rate limits are a separate concern; per-key limits would add a write to every request.
          rateLimit: { enabled: false },
          // Better Auth keeps an Organization's key without the member who created it, so the hub
          // records them in the key's metadata. Only the hub writes it: Better Auth's routes that
          // create and update keys are disabled.
          enableMetadata: true,
        },
        {
          configId: RUNNER_KEY_CONFIG,
          references: "organization",
          rateLimit: { enabled: false },
        },
      ]),
    ],
  } satisfies BetterAuthOptions;
}

/**
 * Options for Better Auth instances that serve no requests: setup, recovery and the CLI's schema
 * draft. Nothing they create is signed or served, so a fresh secret (64 characters, as Better Auth's
 * `auth secret` makes) and a placeholder URL do. Rate limits stay on so the CLI's draft includes the
 * table they are kept in; calls made without a request are never limited.
 */
// oxlint-disable-next-line typescript/no-explicit-any -- Better Auth types its database as Kysely<any>.
export function offlineAuthOptions(db: Kysely<any>) {
  return authOptions({
    db,
    secret: generateRandomString(64),
    baseURL: "http://localhost",
    ipAddressHeaders: [],
    trustedProxies: [],
    rateLimit: true,
  });
}
