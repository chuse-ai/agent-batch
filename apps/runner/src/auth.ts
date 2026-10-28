import type { Readable } from "node:stream";

import {
  type AuthInteraction,
  type AuthType,
  type CredentialStore,
  type LoginOptions,
  ModelsError,
  type MutableModels,
} from "@earendil-works/pi-ai";
import * as NodeStream from "@effect/platform-node-shared/NodeStream";
import { Effect } from "effect";
import ensureError from "ensure-error";

/**
 * A sign-in's requests belong to its provider's endpoint. Sending its tokens to a replacement
 * URL would disclose them, and Pi shapes subscription requests for the original endpoint.
 */
const SIGN_IN_WITH_BASE_URL =
  "RUNNER_MODEL_BASE_URL takes a provider API key, not a subscription sign-in. Unset it, or use logout and then login with an API key; unset ANTHROPIC_OAUTH_TOKEN if supplied.";

export interface ModelAuthSettings {
  provider: string;
  modelId: string;
  baseUrl?: URL;
}

export interface AuthProblem {
  kind: "missing" | "invalid";
  message: string;
}

/** Read a piped key through Effect's interruptible, bounded Node stream adapter. */
export const readApiKey = (input: Readable) =>
  NodeStream.toString(() => input, {
    // A mistaken pipe of a file must not consume unbounded memory before login starts.
    maxBytes: 64 * 1024,
    onError: () => new Error("Could not read the API key from stdin (maximum 64 KiB)."),
  }).pipe(
    Effect.map((key) => key.trim()),
    Effect.flatMap((key) =>
      key.length === 0
        ? Effect.fail(new Error("The API key on stdin is empty."))
        : Effect.succeed(key),
    ),
  );

/**
 * Provider authentication for startup and credential commands. Pi owns credential resolution,
 * refresh, persistence and provider-specific prompts; this module owns the Runner's restrictions
 * and projects status without ever returning credentials to its callers.
 */
export function createRunnerAuth(models: MutableModels, credentials: CredentialStore) {
  /** Append available alternatives without resolving or refreshing unrelated sign-ins. */
  const withAvailable = async (message: string, signal?: AbortSignal) => {
    const available = (await models.getAvailable(undefined, { signal })).map(
      (model) => `${model.provider} ${model.id}`,
    );
    return available.length > 0
      ? `${message}. Models with configured authentication: ${available.join(", ")}`
      : `${message}. No provider has configured authentication.`;
  };

  return {
    /**
     * Resolve authentication before startup, renewing OAuth through Pi. Only absent credentials
     * invite onboarding; an incomplete saved credential or a failed refresh requires repair.
     * On success, apply the model URL override and disable OAuth for subsequent model calls too.
     */
    async prepareModel(
      { provider, modelId, baseUrl }: ModelAuthSettings,
      signal?: AbortSignal,
    ): Promise<AuthProblem | undefined> {
      try {
        signal?.throwIfAborted();
        if (!models.getModel(provider, modelId)) {
          return {
            kind: "invalid",
            message: await withAvailable(
              `Pi does not know the model "${modelId}" of provider "${provider}"`,
              signal,
            ),
          };
        }
        // Azure's own environment settings take precedence over the model's base URL.
        if (baseUrl && provider === "azure") {
          return {
            kind: "invalid",
            message:
              "RUNNER_MODEL_BASE_URL is not used with Azure OpenAI: set AZURE_OPENAI_BASE_URL to call its models at another URL.",
          };
        }
        if (baseUrl && (await models.checkAuth(provider, { signal }))?.type === "oauth") {
          return { kind: "invalid", message: SIGN_IN_WITH_BASE_URL };
        }
        const auth = await models.getAuth(provider, { signal });
        if (!auth) {
          const saved = await credentials.read(provider, { signal });
          if (saved !== undefined) {
            return {
              kind: "invalid",
              message: `Saved authentication for provider "${provider}" is incomplete or unsupported. Repair it with login, or remove it with logout before using environment authentication.`,
            };
          }
          return {
            kind: "missing",
            message: await withAvailable(
              `Pi finds no authentication for provider "${provider}". Run agent-batch stack login --provider ${provider}, or supply its environment explicitly with stack start --env NAME`,
              signal,
            ),
          };
        }
        // Pi accepts this subscription token through its API-key resolver and identifies its source.
        if (baseUrl && auth.source === "ANTHROPIC_OAUTH_TOKEN") {
          return { kind: "invalid", message: SIGN_IN_WITH_BASE_URL };
        }
        const definition = models.getProvider(provider);
        const apiKey = definition?.auth.apiKey;
        if (baseUrl && definition && apiKey) {
          const url = baseUrl.href;
          const atUrl = <M extends { baseUrl: string }>(model: M): M => ({
            ...model,
            baseUrl: url,
          });
          models.setProvider({
            ...definition,
            baseUrl: url,
            // A subscription saved by login beside a running Runner must not reach this URL.
            auth: { apiKey },
            getModels: () => definition.getModels().map(atUrl),
            getAllModels:
              definition.getAllModels && (() => definition.getAllModels?.().map(atUrl) ?? []),
          });
        }
        return undefined;
      } catch (cause) {
        signal?.throwIfAborted();
        const error = ensureError(cause);
        // Pi's top-level messages describe the operation. Causes can contain provider responses
        // or malformed credential-file contents, so do not print them.
        return {
          kind: "invalid",
          message:
            error instanceof ModelsError && error.code === "oauth"
              ? `Pi cannot renew the sign-in to provider "${provider}". Retry when the provider is reachable, or repair the sign-in with login.`
              : `Pi cannot read or resolve authentication for provider "${provider}". Check its configuration and saved credentials.`,
        };
      }
    },

    async login(
      provider: string,
      interaction: AuthInteraction,
      options: LoginOptions & { apiKey?: string; baseUrl?: URL } = {},
    ): Promise<void> {
      interaction.signal?.throwIfAborted();
      const definition = models.getProvider(provider);
      if (!definition) throw new Error(`Unknown provider "${provider}".`);
      const methods: { id: AuthType; label: string }[] = [];
      if (definition.auth.oauth && !options.baseUrl) {
        methods.push({
          id: "oauth",
          label: definition.auth.oauth.loginLabel ?? definition.auth.oauth.name,
        });
      }
      if (definition.auth.apiKey?.login) {
        methods.push({ id: "api_key", label: definition.auth.apiKey.name });
      }
      let type: AuthType;
      if (options.apiKey !== undefined) {
        if (!definition.auth.apiKey?.login) {
          throw new Error(`Provider "${provider}" does not support API-key login.`);
        }
        if (options.apiKey.trim().length === 0) throw new Error("The API key on stdin is empty.");
        type = "api_key";
      } else {
        const [only] = methods;
        if (!only) {
          throw new Error(
            options.baseUrl && definition.auth.oauth
              ? SIGN_IN_WITH_BASE_URL
              : `Provider "${provider}" has no login method; configure its environment instead.`,
          );
        }
        const selected =
          methods.length === 1
            ? only.id
            : await interaction.prompt({
                type: "select",
                message: `How do you want to authenticate to ${definition.name}?`,
                options: methods,
                signal: interaction.signal,
              });
        const method = methods.find((candidate) => candidate.id === selected);
        if (!method) throw new Error("No authentication method selected.");
        type = method.id;
      }
      if (type === "oauth") {
        // OpenAI's Sign in with ChatGPT Terms ask for express consent before background use.
        interaction.notify({
          type: "info",
          message:
            "The Runner will make its jobs' model calls on this sign-in in the background, without asking each time. Run only your own jobs on it.",
        });
      }
      let supplied = false;
      await models.login(
        provider,
        type,
        {
          ...interaction,
          prompt: async (prompt) => {
            interaction.signal?.throwIfAborted();
            prompt.signal?.throwIfAborted();
            if (options.apiKey === undefined) return interaction.prompt(prompt);
            // Pi owns the flow. A single piped key cannot answer provider-specific selections,
            // account IDs, or a second secret; refusing leaves the previous credential untouched.
            if (prompt.type !== "secret" || supplied) {
              throw new Error(
                `Provider "${provider}" requires additional input. Run login interactively; --with-api-key supplies one secret only.`,
              );
            }
            supplied = true;
            return options.apiKey;
          },
        },
        options,
      );
    },

    /** Configuration only: no OAuth refresh, token validation or credential values. */
    async status(provider: string, signal?: AbortSignal) {
      if (!models.getProvider(provider)) throw new Error(`Unknown provider "${provider}".`);
      const auth = await models.checkAuth(provider, { signal });
      return auth === undefined
        ? { provider, configured: false as const }
        : { provider, configured: true as const, type: auth.type, source: auth.source };
    },

    async logout(provider: string, signal?: AbortSignal): Promise<void> {
      // An unused data directory has no credential file or lock parent. Removing nothing is a
      // successful logout; existing credentials still use Pi's locked deletion alongside refresh.
      if ((await credentials.read(provider, { signal })) !== undefined) {
        await models.logout(provider, { signal });
      }
    },
  };
}
