import { getDotPath } from "@standard-schema/utils";
import {
  Cause,
  Config,
  ConfigProvider,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Redacted,
  Schema,
  SchemaIssue,
} from "effect";

import type { AuthSettings } from "./auth/options.ts";

// The hub's settings, defined once for Workers and Docker and read on both from `process.env`,
// which on Workers holds the Worker's variables and secrets. Each platform adds its own database
// connection.

/** A run timeout in seconds, as the job's column holds it: above zero, within a Postgres `integer`. */
const runTimeoutSeconds = Schema.Int.check(Schema.isInt32(), Schema.isGreaterThan(0));

/** A model budget in US dollars, as the API reads it: above zero. */
const modelBudgetUsd = Schema.Finite.check(Schema.isGreaterThan(0));

/**
 * Refuses the default `value` of the limit `name` names, above its maximum `max`, at the default's
 * setting, so it reads as any malformed setting does.
 */
function refuseDefaultAboveMax(name: string, value: number, max: number) {
  return Effect.fail(
    new Config.ConfigError(
      new Schema.SchemaError(
        new SchemaIssue.Pointer(
          [`JOB_${name}_DEFAULT`],
          new SchemaIssue.InvalidValue({
            message: `Expected a value less than or equal to JOB_${name}_MAX (${max}), got ${value}`,
          }),
        ),
      ),
    ),
  );
}

/**
 * The limits the hub gives jobs: the run timeout and model budget a job gets when it gives
 * none, and the most it may give, above which a submission or a resume is refused. A job's limits
 * are resolved when it is submitted, so lowering a maximum reaches no job already submitted. A
 * default above its maximum would give jobs what no caller may ask for, so the hub refuses such
 * settings as it refuses any malformed one.
 */
const jobLimits = Config.all({
  runTimeoutSecondsDefault: Config.schema(
    runTimeoutSeconds,
    "JOB_RUN_TIMEOUT_SECONDS_DEFAULT",
  ).pipe(Config.withDefault(30 * 60)),
  runTimeoutSecondsMax: Config.schema(runTimeoutSeconds, "JOB_RUN_TIMEOUT_SECONDS_MAX").pipe(
    Config.withDefault(24 * 60 * 60),
  ),
  modelBudgetUsdDefault: Config.schema(modelBudgetUsd, "JOB_MODEL_BUDGET_USD_DEFAULT").pipe(
    Config.withDefault(1),
  ),
  modelBudgetUsdMax: Config.schema(modelBudgetUsd, "JOB_MODEL_BUDGET_USD_MAX").pipe(
    Config.withDefault(10),
  ),
}).pipe(
  Config.mapEffect((limits) => {
    if (limits.runTimeoutSecondsDefault > limits.runTimeoutSecondsMax) {
      return refuseDefaultAboveMax(
        "RUN_TIMEOUT_SECONDS",
        limits.runTimeoutSecondsDefault,
        limits.runTimeoutSecondsMax,
      );
    }
    if (limits.modelBudgetUsdDefault > limits.modelBudgetUsdMax) {
      return refuseDefaultAboveMax(
        "MODEL_BUDGET_USD",
        limits.modelBudgetUsdDefault,
        limits.modelBudgetUsdMax,
      );
    }
    return Effect.succeed(limits);
  }),
);

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
  jobLimits,
});
export type HubSettings = Config.Success<typeof hubSettings>;

/** The hub's defaults and maximums for jobs' limits, for its handlers. */
export class JobLimitSettings extends Context.Service<JobLimitSettings, HubSettings["jobLimits"]>()(
  "hub/JobLimitSettings",
) {}

/** Gives the hub's handlers the settings for jobs' limits in `settings`. */
export function jobLimitSettingsLayer(settings: HubSettings) {
  return Layer.succeed(JobLimitSettings, settings.jobLimits);
}

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
 * Reads `config` from `provider`, by default the environment, as Effect's default `Config` source
 * reads it, and throws an error saying what to set when it does not hold. On Workers the
 * environment is the variables and secrets that `nodejs_compat` puts in `process.env`, as it does
 * from the compatibility date 2025-04-01.
 */
export function readConfig<A>(config: Config.Config<A>, provider = ConfigProvider.fromEnv()): A {
  const exit = Effect.runSyncExit(config.parse(provider));
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
