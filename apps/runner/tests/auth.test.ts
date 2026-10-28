import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Readable } from "node:stream";

import {
  type AuthInteraction,
  type CredentialStore,
  createFauxCore,
  createModels,
  createProvider,
  envApiKeyAuth,
  InMemoryCredentialStore,
  type OAuthCredential,
} from "@earendil-works/pi-ai";
import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { expect, onTestFinished, vi } from "vite-plus/test";

import { createRunnerAuth, readApiKey } from "../src/auth.ts";
import { fileCredentialStore } from "../src/credentials.ts";

const noPrompt: AuthInteraction = {
  prompt: () => Promise.reject(new Error("No interactive prompt expected")),
  notify: () => {},
};

const signIn = (expires = Date.now() + 3_600_000): OAuthCredential => ({
  type: "oauth",
  access: "fake-access",
  refresh: "fake-refresh",
  expires,
});

/** Real Pi resolution with no access to the test process's environment or credential files. */
function fixture({
  credentials = new InMemoryCredentialStore(),
  env = {},
  provider = "test",
  refresh = async () => signIn(),
}: {
  credentials?: CredentialStore;
  env?: Record<string, string>;
  provider?: string;
  refresh?: () => Promise<OAuthCredential>;
} = {}) {
  const models = createModels({
    credentials,
    authContext: { env: async (name) => env[name], fileExists: async () => false },
  });
  const faux = createFauxCore({ models: [{ id: "model" }] });
  models.setProvider(
    createProvider({
      id: provider,
      auth: {
        apiKey: envApiKeyAuth("Test API key", ["TEST_API_KEY"]),
        oauth: {
          name: "Test subscription",
          loginLabel: "Sign in to Test",
          login: async () => signIn(),
          refresh,
          toAuth: async (credential) => ({ apiKey: credential.access }),
        },
      },
      models: faux.models,
      api: { stream: faux.stream, streamSimple: faux.streamSimple },
    }),
  );
  return {
    models,
    credentials,
    auth: createRunnerAuth(models, credentials),
    selection: { provider, modelId: "model" },
  };
}

async function credentialFile() {
  const dir = await mkdtemp(path.join(tmpdir(), "runner-auth-"));
  onTestFinished(() => rm(dir, { recursive: true, force: true }));
  return path.join(dir, "auth.json");
}

it.live(
  "a piped key persists for the selected provider, supersedes its environment, and logout exposes the environment again",
  () =>
    Effect.gen(function* () {
      const credentials = fileCredentialStore(yield* Effect.promise(() => credentialFile()));
      const { auth, models } = fixture({ credentials, env: { TEST_API_KEY: "fake-environment" } });
      yield* Effect.promise(() =>
        credentials.modify("other", async () => ({ type: "api_key", key: "fake-other" })),
      );

      const login = auth.login("test", noPrompt, { apiKey: "fake-saved" });
      yield* login;
      // Reusing an Effect starts a new login, including its allowance for one piped secret.
      yield* login;

      const restarted = fixture({ credentials, env: { TEST_API_KEY: "fake-environment" } });
      expect((yield* Effect.promise(() => restarted.models.getAuth("test")))?.auth.apiKey).toBe(
        "fake-saved",
      );
      expect(yield* auth.status("test")).toEqual({
        provider: "test",
        configured: true,
        type: "api_key",
        source: "stored credential",
      });
      yield* auth.logout("test");
      expect((yield* Effect.promise(() => models.getAuth("test")))?.auth.apiKey).toBe(
        "fake-environment",
      );
      expect(yield* Effect.promise(() => credentials.read("other"))).toEqual({
        type: "api_key",
        key: "fake-other",
      });
    }),
);

it.live("interactive login offers Pi's supported methods and requests an API key as a secret", () =>
  Effect.gen(function* () {
    const { auth, models } = fixture();
    const prompt = vi.fn<AuthInteraction["prompt"]>(async (question) =>
      question.type === "select" ? "api_key" : "fake-interactive-key",
    );

    yield* auth.login("test", { ...noPrompt, prompt });

    expect(prompt).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "select",
        options: expect.arrayContaining([
          expect.objectContaining({ id: "oauth", label: "Sign in to Test" }),
          expect.objectContaining({ id: "api_key", label: "Test API key" }),
        ]),
      }),
    );
    expect(prompt).toHaveBeenCalledWith(expect.objectContaining({ type: "secret" }));
    expect((yield* Effect.promise(() => models.getAuth("test")))?.auth.apiKey).toBe(
      "fake-interactive-key",
    );
  }),
);

it.live.each(["amazon-bedrock", "google-vertex", "cloudflare-workers-ai", "cloudflare-ai-gateway"])(
  "stdin login refuses %s's additional prompts without replacing saved credentials",
  (provider) =>
    Effect.gen(function* () {
      const credentials = new InMemoryCredentialStore();
      const original = { type: "api_key" as const, key: "fake-original" };
      yield* Effect.promise(() => credentials.modify(provider, async () => original));
      const models = builtinModels({
        credentials,
        authContext: { env: async () => undefined, fileExists: async () => false },
      });

      const error = yield* createRunnerAuth(models, credentials)
        .login(provider, noPrompt, {
          apiKey: "fake-replacement",
        })
        .pipe(Effect.flip);
      expect(error.message).toContain("requires additional input");
      expect(yield* Effect.promise(() => credentials.read(provider))).toEqual(original);
    }),
);

it.live("an OAuth-only provider refuses a piped API key without starting sign-in", () =>
  Effect.gen(function* () {
    const credentials = new InMemoryCredentialStore();
    const models = builtinModels({ credentials });
    const error = yield* createRunnerAuth(models, credentials)
      .login("openai-codex", noPrompt, { apiKey: "fake-key" })
      .pipe(Effect.flip);
    expect(error.message).toContain("does not support API-key login");
    expect(yield* Effect.promise(() => credentials.list())).toEqual([]);
  }),
);

it.live(
  "status does not refresh OAuth or disclose tokens; preflight fails a broken refresh without falling back to the environment",
  () =>
    Effect.gen(function* () {
      const refresh = vi.fn(async (): Promise<OAuthCredential> => {
        throw new Error("fake-private-provider-response");
      });
      const { auth, credentials, selection } = fixture({
        env: { TEST_API_KEY: "fake-key" },
        refresh,
      });
      const expired = signIn(0);
      yield* Effect.promise(() => credentials.modify("test", async () => expired));

      expect(yield* auth.status("test")).toEqual({
        provider: "test",
        configured: true,
        type: "oauth",
        source: "OAuth",
      });
      expect(refresh).not.toHaveBeenCalled();
      expect(yield* auth.prepareModel(selection)).toEqual({
        kind: "invalid",
        message: expect.stringContaining("cannot renew"),
      });
      expect(refresh).toHaveBeenCalledOnce();
      expect(yield* Effect.promise(() => credentials.read("test"))).toEqual(expired);
    }),
);

it.effect(
  "startup bounds stalled credential resolution, cancels the wait, and preserves saved credentials",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<AbortSignal>();
      const { models, auth, credentials, selection } = fixture();
      const provider = models.getProvider("test");
      if (!provider) throw new Error("The fixture has no provider");
      models.setProvider({
        ...provider,
        auth: {
          apiKey: {
            name: "Stalled credential source",
            resolve: ({ signal }) =>
              new Promise((_resolve, reject) => {
                signal.addEventListener("abort", () => reject(signal.reason), { once: true });
                Deferred.doneUnsafe(started, Exit.succeed(signal));
              }),
          },
        },
      });
      const original = { type: "api_key" as const, key: "fake-saved" };
      yield* Effect.promise(() => credentials.modify("test", async () => original));
      const checking = yield* Effect.forkScoped(auth.prepareModel(selection));
      const signal = yield* Deferred.await(started);
      yield* TestClock.adjust("60 seconds");
      expect(yield* Fiber.join(checking)).toEqual({
        kind: "invalid",
        message: expect.stringContaining("did not resolve within 60 seconds"),
      });
      expect(signal.aborted).toBe(true);
      expect(yield* Effect.promise(() => credentials.read("test"))).toEqual(original);
    }),
);

it.live(
  "only missing authentication is eligible for onboarding; unknown models and unresolved saved credentials require repair",
  () =>
    Effect.gen(function* () {
      const { auth, credentials, selection } = fixture();
      expect((yield* auth.prepareModel(selection))?.kind).toBe("missing");
      expect((yield* auth.prepareModel({ ...selection, modelId: "unknown" }))?.kind).toBe(
        "invalid",
      );
      yield* Effect.promise(() => credentials.modify("test", async () => ({ type: "api_key" })));
      expect((yield* auth.prepareModel(selection))?.kind).toBe("invalid");
    }),
);

it.live(
  "Pi resolves an environment API key even with a keyless saved entry, without a provider request",
  () =>
    Effect.gen(function* () {
      const { auth, credentials, selection } = fixture({
        env: { TEST_API_KEY: "fake-unvalidated-key" },
      });
      yield* Effect.promise(() => credentials.modify("test", async () => ({ type: "api_key" })));
      // The faux model makes no request during authentication. Pi resolves the environment only.
      expect(yield* auth.prepareModel(selection)).toBeUndefined();
      expect(yield* auth.status("test")).toEqual({
        provider: "test",
        configured: true,
        type: "api_key",
        source: "TEST_API_KEY",
      });
    }),
);

it.live(
  "a replacement endpoint refuses saved OAuth before refreshing and keeps rejecting a sign-in saved after startup",
  () =>
    Effect.gen(function* () {
      const refresh = vi.fn(async () => signIn());
      const { auth, models, credentials, selection } = fixture({ refresh });
      const baseUrl = new URL("http://localhost:4000/v1");
      yield* Effect.promise(() => credentials.modify("test", async () => signIn(0)));
      expect((yield* auth.prepareModel({ ...selection, baseUrl }))?.message).toContain(
        "not a subscription sign-in",
      );
      expect(refresh).not.toHaveBeenCalled();

      yield* auth.login("test", noPrompt, { apiKey: "fake-key", baseUrl });
      expect(yield* auth.prepareModel({ ...selection, baseUrl })).toBeUndefined();
      expect(models.getModel("test", "model")?.baseUrl).toBe(baseUrl.href);
      yield* Effect.promise(() => credentials.modify("test", async () => signIn()));
      expect(yield* Effect.promise(() => models.getAuth("test"))).toBeUndefined();
    }),
);

it.live(
  "replacement endpoints reject Azure's conflicting setting and Anthropic's environment subscription token",
  () =>
    Effect.gen(function* () {
      const baseUrl = new URL("http://localhost:4000/v1");
      const azure = fixture({ provider: "azure", env: { TEST_API_KEY: "fake-key" } });
      expect((yield* azure.auth.prepareModel({ ...azure.selection, baseUrl }))?.message).toContain(
        "AZURE_OPENAI_BASE_URL",
      );

      const credentials = new InMemoryCredentialStore();
      const models = builtinModels({
        credentials,
        authContext: {
          env: async (name) => (name === "ANTHROPIC_OAUTH_TOKEN" ? "fake-token" : undefined),
          fileExists: async () => false,
        },
      });
      const [model] = models.getModels("anthropic");
      if (!model) throw new Error("Pi has no Anthropic models");
      const problem = yield* createRunnerAuth(models, credentials).prepareModel({
        provider: "anthropic",
        modelId: model.id,
        baseUrl,
      });
      expect(problem?.message).toContain("not a subscription sign-in");
    }),
);

it.live("logout succeeds before the data directory exists", () =>
  Effect.gen(function* () {
    const file = path.join(
      yield* Effect.promise(() => credentialFile()),
      "not-created",
      "auth.json",
    );
    const { auth } = fixture({ credentials: fileCredentialStore(file) });
    yield* auth.logout("test");
    expect(yield* auth.status("test")).toEqual({
      provider: "test",
      configured: false,
    });
  }),
);

it.live.each(["interaction", "operation"] as const)(
  "cancelling a login through the %s signal cancels its prompt and preserves the saved credential",
  (source) =>
    Effect.gen(function* () {
      const { auth, credentials } = fixture();
      yield* auth.login("test", noPrompt, { apiKey: "fake-original" });
      const controller = new AbortController();
      let promptSignal: AbortSignal | undefined;
      // This runtime boundary lets the operation signal interrupt login without interrupting the test.
      yield* Effect.promise(() =>
        expect(
          Effect.runPromise(
            auth.login("test", {
              ...noPrompt,
              signal: source === "interaction" ? controller.signal : undefined,
              prompt: async (prompt) => {
                promptSignal = prompt.signal;
                controller.abort(new Error("cancelled"));
                promptSignal?.throwIfAborted();
                return "api_key";
              },
            }),
            { signal: source === "operation" ? controller.signal : undefined },
          ),
        ).rejects.toThrow(),
      );
      expect(promptSignal?.aborted).toBe(true);
      expect(yield* Effect.promise(() => credentials.read("test"))).toEqual({
        type: "api_key",
        key: "fake-original",
      });
    }),
);

it.live("stdin accepts a newline-terminated key and rejects empty or oversized input", () =>
  Effect.gen(function* () {
    expect(yield* readApiKey(Readable.from(["fake-key\n"]))).toBe("fake-key");
    expect((yield* readApiKey(Readable.from([" \n"])).pipe(Effect.flip)).message).toContain(
      "empty",
    );
    expect(
      (yield* readApiKey(Readable.from(["x".repeat(65_537)])).pipe(Effect.flip)).message,
    ).toContain("64 KiB");
  }),
);
