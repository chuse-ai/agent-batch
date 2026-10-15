import { sql, type Kysely } from "kysely";

// Better Auth's tables, as drafted by its CLI (`vp run hub#db:draft-auth-migration`) for Better Auth
// 1.7.8 with the hub's plugins. Migrations stay as written once released, so the SQL is copied here
// rather than generated at run time. The driver runs one statement per query.
const statements = [
  'create table "agent_batch"."user" ("id" text not null primary key, "name" text not null, "email" text not null unique, "emailVerified" boolean not null, "image" text, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz default CURRENT_TIMESTAMP not null)',
  'create table "agent_batch"."session" ("id" text not null primary key, "expiresAt" timestamptz not null, "token" text not null unique, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz not null, "ipAddress" text, "userAgent" text, "userId" text not null references "agent_batch"."user" ("id") on delete cascade, "activeOrganizationId" text)',
  'create table "agent_batch"."account" ("id" text not null primary key, "accountId" text not null, "providerId" text not null, "userId" text not null references "agent_batch"."user" ("id") on delete cascade, "accessToken" text, "refreshToken" text, "idToken" text, "accessTokenExpiresAt" timestamptz, "refreshTokenExpiresAt" timestamptz, "scope" text, "password" text, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz not null)',
  'create table "agent_batch"."verification" ("id" text not null primary key, "identifier" text not null, "value" text not null, "expiresAt" timestamptz not null, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "updatedAt" timestamptz default CURRENT_TIMESTAMP not null)',
  'create table "agent_batch"."organization" ("id" text not null primary key, "name" text not null, "slug" text not null unique, "logo" text, "createdAt" timestamptz not null, "metadata" text)',
  'create table "agent_batch"."member" ("id" text not null primary key, "organizationId" text not null references "agent_batch"."organization" ("id") on delete cascade, "userId" text not null references "agent_batch"."user" ("id") on delete cascade, "role" text not null, "createdAt" timestamptz not null)',
  'create table "agent_batch"."invitation" ("id" text not null primary key, "organizationId" text not null references "agent_batch"."organization" ("id") on delete cascade, "email" text not null, "role" text, "status" text not null, "expiresAt" timestamptz not null, "createdAt" timestamptz default CURRENT_TIMESTAMP not null, "inviterId" text not null references "agent_batch"."user" ("id") on delete cascade)',
  'create table "agent_batch"."apikey" ("id" text not null primary key, "configId" text not null, "name" text, "start" text, "referenceId" text not null, "prefix" text, "key" text not null, "refillInterval" integer, "refillAmount" integer, "lastRefillAt" timestamptz, "enabled" boolean, "rateLimitEnabled" boolean, "rateLimitTimeWindow" integer, "rateLimitMax" integer, "requestCount" integer, "remaining" integer, "lastRequest" timestamptz, "expiresAt" timestamptz, "createdAt" timestamptz not null, "updatedAt" timestamptz not null, "permissions" text, "metadata" text)',
  'create table "agent_batch"."rateLimit" ("id" text not null primary key, "key" text not null unique, "count" integer not null, "lastRequest" bigint not null)',
  'create index "session_userId_idx" on "agent_batch"."session" ("userId")',
  'create index "account_userId_idx" on "agent_batch"."account" ("userId")',
  'create index "verification_identifier_idx" on "agent_batch"."verification" ("identifier")',
  'create index "member_organizationId_idx" on "agent_batch"."member" ("organizationId")',
  'create index "member_userId_idx" on "agent_batch"."member" ("userId")',
  'create index "invitation_organizationId_idx" on "agent_batch"."invitation" ("organizationId")',
  'create index "invitation_email_idx" on "agent_batch"."invitation" ("email")',
  'create index "apikey_configId_idx" on "agent_batch"."apikey" ("configId")',
  'create index "apikey_referenceId_idx" on "agent_batch"."apikey" ("referenceId")',
  'create index "apikey_key_idx" on "agent_batch"."apikey" ("key")',
];

export async function up(db: Kysely<unknown>): Promise<void> {
  for (const statement of statements) await sql.raw(statement).execute(db);
}

export async function down(db: Kysely<unknown>): Promise<void> {
  for (const table of [
    "rateLimit",
    "apikey",
    "invitation",
    "member",
    "organization",
    "verification",
    "account",
    "session",
    "user",
  ]) {
    await db.schema.withSchema("agent_batch").dropTable(table).execute();
  }
}
