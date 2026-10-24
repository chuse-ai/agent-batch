import { getDotPath } from "@standard-schema/utils";
import {
  Cause,
  Config,
  ConfigProvider,
  Effect,
  Exit,
  Option,
  Predicate,
  Record,
  Redacted,
  Schema,
  SchemaIssue,
} from "effect";

import type { AuthSettings } from "./auth/options.ts";

// The hub's settings, defined once for Workers and Docker: on Workers they come from the Worker's
// environment, in Docker from the process's. Each platform adds its own database connection.

/**
 * How often Runners sync with the hub when nothing prompts them sooner, told to each Runner in the
 * answer to its hello (ADR 0026), as Buildkite's server gives its agents their polling interval;
 * tests set it shorter.
 */

export const hubSettings = Config.all({
  /** Signs members' sessions. */
  secret: Config.Redacted("BETTER_AUTH_SECRET"),
  /** The hub's public URL, Better Auth's base URL. */
  hubUrl: Config.String("HUB_URL"),
  /** The request header carrying the client's address, for rate limits; unset, Better Auth's own. */
  clientIpHeader: Config.String("CLIENT_IP_HEADER").pipe(Config.option),
  /** The proxies in front of the hub, as IPs or CIDRs, whose entries in a forwarded chain are skipped. */
  trustedProxies: Config.Array(Schema.String, "TRUSTED_PROXIES").pipe(Config.withDefault([])),
  /** Whether Better Auth's rate limits apply; tests turn them off. */
  authRateLimit: Config.Boolean("AUTH_RATE_LIMIT").pipe(Config.withDefault(true)),
  /**
   * How often Runners report their jobs' new events, which decides how far behind a running job's
   * events may be; told to each Runner in the answer to its hello.
   */
  reportIntervalMs: Config.Int("REPORT_INTERVAL_MS").pipe(Config.withDefault(3_000)),
});
export type HubSettings = Config.Success<typeof hubSettings>;

/** Better Auth's settings for the hub, apart from its database, which each platform provides. */
export function authSettings(settings: HubSettings): Omit<AuthSettings, "db"> {
  return {
    secret: Redacted.value(settings.secret),
    baseURL: settings.hubUrl,
    ipAddressHeaders: Option.getOrUndefined(Option.map(settings.clientIpHeader, (h) => [h])),
    trustedProxies: [...settings.trustedProxies],
    rateLimit: settings.authRateLimit,
  };
}

/**
 * Reads `config` from `env`, an environment such as a Worker's, whose string values are its
 * variables; throws an error saying what to set when it does not hold.
 */
export function readConfig<A>(config: Config.Config<A>, env: object): A {
  // A Worker's environment is not the process's, so it is given explicitly, as Effect documents for
  // runtimes without one. It is copied first, as an interface such as the Worker's `Env` has no
  // index signature for `Record` to take.
  const variables = Record.filter({ ...env }, Predicate.isString);
  const exit = Effect.runSyncExit(config.parse(ConfigProvider.fromEnvRecord(variables)));
  if (Exit.isSuccess(exit)) return exit.value;
  const detail = Option.match(Cause.findErrorOption(exit.cause), {
    onSome: settingsProblems,
    onNone: () => Cause.pretty(exit.cause),
  });
  throw new Error(`The hub is not configured: ${detail}`);
}

/**
 * What `error`, a missing or malformed setting, finds wrong with the settings, on one line, as the
 * Runner and the CLI report their own: each setting it names, then the problem. Each of the three
 * keeps a copy, as the contract they share has no Effect (ADR 0022).
 */
export function settingsProblems(error: Config.ConfigError): string {
  if (!Schema.isSchemaError(error.cause)) return error.message;
  const { issues } = SchemaIssue.makeFormatterStandardSchemaV1()(error.cause.issue);
  return issues
    .map((issue) => {
      const setting = getDotPath(issue);
      return setting === null ? issue.message : `${setting}: ${issue.message}`;
    })
    .join("; ");
}
