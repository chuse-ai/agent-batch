import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { PassThrough, Readable } from "node:stream";

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
import { Effect } from "effect";
import { expect, onTestFinished, test, vi } from "vite-plus/test";

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

test("a piped key persists for the selected provider, supersedes its environment, and logout exposes the environment again", async () => {
  const credentials = fileCredentialStore(await credentialFile());
  const { auth, models } = fixture({ credentials, env: { TEST_API_KEY: "fake-environment" } });
  await credentials.modify("other", async () => ({ type: "api_key", key: "fake-other" }));

  await auth.login("test", noPrompt, { apiKey: "fake-saved" });

  const restarted = fixture({ credentials, env: { TEST_API_KEY: "fake-environment" } });
  expect((await restarted.models.getAuth("test"))?.auth.apiKey).toBe("fake-saved");
  expect(await auth.status("test")).toEqual({
    provider: "test",
    configured: true,
    type: "api_key",
    source: "stored credential",
  });
  await auth.logout("test");
  expect((await models.getAuth("test"))?.auth.apiKey).toBe("fake-environment");
  expect(await credentials.read("other")).toEqual({ type: "api_key", key: "fake-other" });
});

test("interactive login offers Pi's supported methods and requests an API key as a secret", async () => {
  const { auth } = fixture();
  const prompt = vi.fn<AuthInteraction["prompt"]>(async (question) =>
    question.type === "select" ? "api_key" : "fake-interactive-key",
  );

  await auth.login("test", { ...noPrompt, prompt });

  expect(prompt).toHaveBeenNthCalledWith(1, {
    type: "select",
    message: expect.any(String),
    options: [
      { id: "oauth", label: "Sign in to Test" },
      { id: "api_key", label: "Test API key" },
    ],
    signal: undefined,
  });
  expect(prompt).toHaveBeenNthCalledWith(2, { type: "secret", message: "Enter Test API key" });
  expect((await auth.status("test")).configured).toBe(true);
});

test.each(["amazon-bedrock", "google-vertex", "cloudflare-workers-ai", "cloudflare-ai-gateway"])(
  "stdin login refuses %s's additional prompts without replacing saved credentials",
  async (provider) => {
    const credentials = new InMemoryCredentialStore();
    const original = { type: "api_key" as const, key: "fake-original" };
    await credentials.modify(provider, async () => original);
    const models = builtinModels({
      credentials,
      authContext: { env: async () => undefined, fileExists: async () => false },
    });

    await expect(
      createRunnerAuth(models, credentials).login(provider, noPrompt, {
        apiKey: "fake-replacement",
      }),
    ).rejects.toThrow("requires additional input");

    expect(await credentials.read(provider)).toEqual(original);
  },
);

test("an OAuth-only provider refuses a piped API key without starting sign-in", async () => {
  const credentials = new InMemoryCredentialStore();
  const models = builtinModels({ credentials });
  await expect(
    createRunnerAuth(models, credentials).login("openai-codex", noPrompt, { apiKey: "fake-key" }),
  ).rejects.toThrow("does not support API-key login");
  expect(await credentials.list()).toEqual([]);
});

test("status does not refresh OAuth or disclose tokens; preflight fails a broken refresh without falling back to the environment", async () => {
  const refresh = vi.fn(async (): Promise<OAuthCredential> => {
    throw new Error("fake-private-provider-response");
  });
  const { auth, credentials, selection } = fixture({ env: { TEST_API_KEY: "fake-key" }, refresh });
  const expired = signIn(0);
  await credentials.modify("test", async () => expired);

  expect(await auth.status("test")).toEqual({
    provider: "test",
    configured: true,
    type: "oauth",
    source: "OAuth",
  });
  expect(refresh).not.toHaveBeenCalled();
  expect(await auth.prepareModel(selection)).toEqual({
    kind: "invalid",
    message: expect.stringContaining("cannot renew"),
  });
  expect(refresh).toHaveBeenCalledOnce();
  expect(await credentials.read("test")).toEqual(expired);
});

test("only missing authentication is eligible for onboarding; unknown models and incomplete saved credentials require repair", async () => {
  const { auth, credentials, selection } = fixture();
  expect((await auth.prepareModel(selection))?.kind).toBe("missing");
  expect((await auth.prepareModel({ ...selection, modelId: "unknown" }))?.kind).toBe("invalid");
  await credentials.modify("test", async () => ({ type: "api_key" }));
  expect((await auth.prepareModel(selection))?.kind).toBe("invalid");
});

test("a configured API key passes preflight without a provider request", async () => {
  const { auth, selection } = fixture({ env: { TEST_API_KEY: "fake-unvalidated-key" } });
  // The faux model makes no request during authentication. Pi resolves the environment only.
  expect(await auth.prepareModel(selection)).toBeUndefined();
  expect(await auth.status("test")).toEqual({
    provider: "test",
    configured: true,
    type: "api_key",
    source: "TEST_API_KEY",
  });
});

test("a replacement endpoint refuses saved OAuth before refreshing and keeps rejecting a sign-in saved after startup", async () => {
  const refresh = vi.fn(async () => signIn());
  const { auth, models, credentials, selection } = fixture({ refresh });
  const baseUrl = new URL("http://localhost:4000/v1");
  await credentials.modify("test", async () => signIn(0));
  expect((await auth.prepareModel({ ...selection, baseUrl }))?.message).toContain(
    "not a subscription sign-in",
  );
  expect(refresh).not.toHaveBeenCalled();

  await auth.login("test", noPrompt, { apiKey: "fake-key", baseUrl });
  expect(await auth.prepareModel({ ...selection, baseUrl })).toBeUndefined();
  expect(models.getModel("test", "model")?.baseUrl).toBe(baseUrl.href);
  await credentials.modify("test", async () => signIn());
  expect(await models.getAuth("test")).toBeUndefined();
});

test("replacement endpoints reject Azure's conflicting setting and Anthropic's environment subscription token", async () => {
  const baseUrl = new URL("http://localhost:4000/v1");
  const azure = fixture({ provider: "azure", env: { TEST_API_KEY: "fake-key" } });
  expect((await azure.auth.prepareModel({ ...azure.selection, baseUrl }))?.message).toContain(
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
  const problem = await createRunnerAuth(models, credentials).prepareModel({
    provider: "anthropic",
    modelId: model.id,
    baseUrl,
  });
  expect(problem?.message).toContain("not a subscription sign-in");
});

test("logout succeeds before the data directory exists", async () => {
  const file = path.join(await credentialFile(), "not-created", "auth.json");
  const { auth } = fixture({ credentials: fileCredentialStore(file) });
  await expect(auth.logout("test")).resolves.toBeUndefined();
  expect(await auth.status("test")).toEqual({ provider: "test", configured: false });
});

test("cancelling a login preserves the saved credential", async () => {
  const { auth, credentials } = fixture();
  await auth.login("test", noPrompt, { apiKey: "fake-original" });
  const controller = new AbortController();
  await expect(
    auth.login("test", {
      ...noPrompt,
      signal: controller.signal,
      prompt: async () => {
        controller.abort(new Error("cancelled"));
        return "api_key";
      },
    }),
  ).rejects.toThrow("cancelled");
  expect(await credentials.read("test")).toEqual({ type: "api_key", key: "fake-original" });
});

test("stdin accepts a newline-terminated key and rejects empty or oversized input", async () => {
  expect(await Effect.runPromise(readApiKey(Readable.from(["fake-key\n"])))).toBe("fake-key");
  await expect(Effect.runPromise(readApiKey(Readable.from([" \n"])))).rejects.toThrow("empty");
  await expect(Effect.runPromise(readApiKey(Readable.from(["x".repeat(65_537)])))).rejects.toThrow(
    "64 KiB",
  );
});

test("cancelling stdin input releases the stream", async () => {
  const input = new PassThrough();
  const controller = new AbortController();
  const reading = Effect.runPromise(readApiKey(input), { signal: controller.signal });
  controller.abort();
  await expect(reading).rejects.toThrow();
  expect(input.destroyed).toBe(true);
});
