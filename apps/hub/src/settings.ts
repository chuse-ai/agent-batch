import { HEARTBEAT_TIMEOUT_MS } from "contract";
import { Cause, Config, ConfigProvider, Effect, Exit, Option, Redacted } from "effect";

import type { AuthSettings } from "./auth/options.ts";

// The hub's settings, defined once for Workers and Docker: on Workers they come from the Worker's
// environment, in Docker from the process's. Each platform adds its own database connection.

/** How long a Runner may go without a heartbeat; tests set it shorter. */
export const heartbeatTimeoutMs = Config.Int("HEARTBEAT_TIMEOUT_MS").pipe(
  Config.withDefault(HEARTBEAT_TIMEOUT_MS),
);

export const hubSettings = Config.all({
  /** Signs members' sessions. */
  secret: Config.Redacted("BETTER_AUTH_SECRET"),
  /** The hub's public URL, Better Auth's base URL. */
  hubUrl: Config.String("HUB_URL"),
  /** The request header carrying the client's address, for rate limits; unset, Better Auth's own. */
  clientIpHeader: Config.String("CLIENT_IP_HEADER").pipe(Config.option),
  /** Whether Better Auth's rate limits apply; tests turn them off. */
  authRateLimit: Config.Boolean("AUTH_RATE_LIMIT").pipe(Config.withDefault(true)),
  heartbeatTimeoutMs,
});
export type HubSettings = Config.Success<typeof hubSettings>;

/** Better Auth's settings for the hub, apart from its database, which each platform provides. */
export function authSettings(settings: HubSettings): Omit<AuthSettings, "db"> {
  return {
    secret: Redacted.value(settings.secret),
    baseURL: settings.hubUrl,
    ipAddressHeaders: Option.getOrUndefined(Option.map(settings.clientIpHeader, (h) => [h])),
    rateLimit: settings.authRateLimit,
  };
}

/**
 * Reads `config` from `env`, an environment such as a Worker's, whose string values are its
 * variables; throws an error saying what to set when it does not hold.
 */
export function readConfig<A>(config: Config.Config<A>, env: object): A {
  const variables = Object.fromEntries(
    Object.entries(env).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
  );
  // A Worker's environment is not the process's, so it is given explicitly, as Effect documents for
  // runtimes without one.
  const exit = Effect.runSyncExit(
    Effect.gen(function* () {
      return yield* config;
    }).pipe(
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnvRecord(variables)),
    ),
  );
  if (Exit.isSuccess(exit)) return exit.value;
  // A missing or malformed setting fails with a ConfigError whose cause says which one.
  const { cause } = Cause.squash(exit.cause) as { cause?: unknown };
  const detail = cause instanceof Error ? cause.message : Cause.pretty(exit.cause);
  throw new Error(`The hub is not configured: ${detail.replace(/\s+/g, " ")}`);
}
