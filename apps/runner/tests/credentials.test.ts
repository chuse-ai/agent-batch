import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import {
  type AuthInteraction,
  createModels,
  createProvider,
  type OAuthCredential,
} from "@earendil-works/pi-ai";
import { createFauxCore } from "@earendil-works/pi-ai/providers/faux";
import { expect, onTestFinished, test } from "vite-plus/test";

import { fileCredentialStore } from "../src/credentials.ts";

// A sign-in's refresh token works once, so everything that uses the Runner's sign-in, its model
// calls and a `login` beside it, must see each refresh and save each new token: these hold the
// Runner's store to that through Pi's own `Models`, which refreshes inside the store's lock.

/** A provider whose sign-in hands out numbered tokens and counts its refreshes. */
function subscriptionProvider() {
  let issued = 0;
  const token = (): OAuthCredential => {
    issued++;
    return { type: "oauth", access: `access-${issued}`, refresh: `refresh-${issued}`, expires: 0 };
  };
  const refreshes: string[] = [];
  // The model is pi-ai's faux one: these calls only resolve its provider's sign-in.
  const faux = createFauxCore({ models: [{ id: "model" }] });
  const provider = createProvider({
    id: "subscription",
    auth: {
      oauth: {
        name: "Subscription",
        login: async () => ({ ...token(), expires: Date.now() + 3_600_000 }),
        refresh: async (credential) => {
          refreshes.push(credential.refresh);
          // Slow enough that the other callers arrive while it runs.
          await new Promise((resolve) => setTimeout(resolve, 50));
          return { ...token(), expires: Date.now() + 3_600_000 };
        },
        toAuth: async (credential) => ({ apiKey: credential.access }),
      },
    },
    models: faux.models,
    api: { stream: faux.stream, streamSimple: faux.streamSimple },
  });
  return { provider, token, refreshes };
}

const noInteraction: AuthInteraction = {
  prompt: () => Promise.reject(new Error("no prompt expected")),
  notify: () => {},
};

async function credentialsFile() {
  const dir = await mkdtemp(path.join(tmpdir(), "runner-credentials-"));
  onTestFinished(() => rm(dir, { recursive: true, force: true }));
  return path.join(dir, "auth.json");
}

/** Pi's models with the store at `file`, as a Runner or a `login` beside it has them. */
function modelsAt(file: string, provider: ReturnType<typeof subscriptionProvider>["provider"]) {
  const models = createModels({ credentials: fileCredentialStore(file) });
  models.setProvider(provider);
  return models;
}

test("an expired sign-in is refreshed once however many model calls need it at once, and all use the new token", async () => {
  const file = await credentialsFile();
  const subscription = subscriptionProvider();
  const expired = subscription.token();
  await fileCredentialStore(file).modify("subscription", async () => expired);
  // Two collections over one file, as a Runner and a `login` beside it are.
  const runner = modelsAt(file, subscription.provider);
  const other = modelsAt(file, subscription.provider);

  const auths = await Promise.all(
    [runner, other, runner, other, runner, other].map((models) => models.getAuth("subscription")),
  );

  expect(subscription.refreshes).toEqual([expired.refresh]);
  expect(new Set(auths.map((auth) => auth?.auth.apiKey))).toEqual(new Set(["access-2"]));
});

test("a sign-in saved beside a running Runner is used by its next model call, and after a restart, readable only by its user", async () => {
  const file = await credentialsFile();
  const subscription = subscriptionProvider();
  const runner = modelsAt(file, subscription.provider);
  expect(await runner.getAuth("subscription")).toBeUndefined();

  await modelsAt(file, subscription.provider).login("subscription", "oauth", noInteraction);

  expect((await runner.getAuth("subscription"))?.auth.apiKey).toBe("access-1");
  const restarted = modelsAt(file, subscription.provider);
  expect((await restarted.getAuth("subscription"))?.auth.apiKey).toBe("access-1");
  expect((await stat(file)).mode & 0o777).toBe(0o600);
});
