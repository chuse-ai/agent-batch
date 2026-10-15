import { resetPassword } from "../src/setup.ts";

// FIXME: operators get the CLI, not this repository (ADR 0004), so this command moves into the
// CLI once it exists.
// Sets a new password, `ADMIN_PASSWORD`, for the member with `ADMIN_EMAIL` in the hub's database at
// `DATABASE_URL`: the way back in for a member who has forgotten theirs. It needs database access,
// which only those who run the hub have.
const { DATABASE_URL: databaseUrl, ADMIN_EMAIL: email, ADMIN_PASSWORD: password } = process.env;
if (!databaseUrl || !email || !password) {
  console.error("Set DATABASE_URL, ADMIN_EMAIL and ADMIN_PASSWORD to reset a member's password");
  process.exit(1);
}
try {
  await resetPassword(databaseUrl, { email, password });
  console.info(`Reset the password of ${email}`);
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}
