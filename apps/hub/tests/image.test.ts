/**
 * @module-tag container
 */
import { execFile } from "node:child_process";
import path from "node:path";
import { text } from "node:stream/consumers";
import { promisify } from "node:util";

import { createORPCClient } from "@orpc/client";
import type { ContractRouterClient } from "@orpc/contract";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import {
  contract,
  createProblemDetailsLink,
  RUNNER_CONNECT_PATH,
  RUNNER_NOT_FOUND,
  RUNNER_PROTOCOL_VERSION,
} from "contract";
import { GenericContainer, Network, Wait } from "testcontainers";
import { describe, expect, inject, test } from "vite-plus/test";
import WebSocket from "ws";

import { OWNER } from "./harness.ts";

// The hub's and a Runner's Docker images as an operator runs them: built from the repository, the hub
// started against a fresh database, which it migrates and sets up as it starts (ADR 0014, ADR 0015),
// and the Runner with the options its sandbox needs. They need Docker and image builds, so they
// are tagged and the default run skips them (ADR 0003).

const HUB_ORIGIN = "http://hub.test";
const run = promisify(execFile);

// Build through Buildx as the release workflow does, including its context handling and Dockerfile
// frontend. Testcontainers starts and manages the resulting images.
// TODO: Recheck Testcontainers builds when the WSL2 build-context issue is resolved upstream:
// https://github.com/testcontainers/testcontainers-node/issues/1361
async function buildImage(target: "hub" | "runner") {
  const name = `agent-batch-${target}:test`;
  await run("docker", ["buildx", "build", "--load", "--target", target, "--tag", name, "."], {
    cwd: path.join(import.meta.dirname, "../../.."),
    timeout: 540_000,
    maxBuffer: 10 * 1024 * 1024,
  });
  return new GenericContainer(name);
}

/**
 * A container whose `/proc` Docker leaves unmasked, as `docker run --security-opt
 * systempaths=unconfined` does: the Engine API takes that as empty masked and read-only paths.
 */
class UnmaskedProcContainer extends GenericContainer {
  constructor(image: string) {
    super(image);
    this.hostConfig.MaskedPaths = [];
    this.hostConfig.ReadonlyPaths = [];
  }
}

// The image is the same for both projects, so one of them runs it.
describe.skipIf(inject("hub") !== "node")("the Docker images", () => {
  test("sets up a fresh database as it starts, reports healthy, serves the API, and stops gracefully", async () => {
    await using network = await new Network().start();
    await using database = await new PostgreSqlContainer("postgres:18-alpine")
      .withNetwork(network)
      .withNetworkAliases("database")
      .start();
    const image = await buildImage("hub");
    const hub = await image
      .withNetwork(network)
      .withEnvironment({
        DATABASE_URL: `postgres://${database.getUsername()}:${database.getPassword()}@database:5432/${database.getDatabase()}`,
        BETTER_AUTH_SECRET: "a-secret-only-for-tests-0123456789abcdef",
        HUB_URL: HUB_ORIGIN,
        ADMIN_EMAIL: OWNER.email,
        ADMIN_PASSWORD: OWNER.password,
      })
      .withExposedPorts(8787)
      .withWaitStrategy(Wait.forHealthCheck())
      .start();
    const url = `http://${hub.getHost()}:${hub.getMappedPort(8787)}`;

    const signIn = await fetch(`${url}/api/auth/sign-in/email`, {
      method: "POST",
      headers: { "content-type": "application/json", origin: HUB_ORIGIN },
      body: JSON.stringify(OWNER),
    });
    expect(signIn.status).toBe(200);

    const socket = new WebSocket(
      `${url.replace("http", "ws")}${RUNNER_CONNECT_PATH}`,
      [RUNNER_PROTOCOL_VERSION],
      { headers: { "x-api-key": "not a key" } },
    );
    socket.on("error", () => {});
    const refusal = await new Promise<{ status?: number; body: unknown }>((resolve) => {
      socket.once("unexpected-response", (_request, response) => {
        void text(response).then((body) =>
          resolve({ status: response.statusCode, body: JSON.parse(body) }),
        );
      });
    });
    socket.terminate();
    expect(refusal).toMatchObject({ status: 401, body: { code: RUNNER_NOT_FOUND } });

    // Docker sends SIGTERM, then kills the container after the timeout; a graceful stop is quicker.
    const stopping = Date.now();
    await hub.stop({ timeout: 10_000 });
    expect(Date.now() - stopping).toBeLessThan(5_000);
  });

  test("a Runner's image passes its sandbox check and registers with the hub", async () => {
    await using network = await new Network().start();
    await using database = await new PostgreSqlContainer("postgres:18-alpine")
      .withNetwork(network)
      .withNetworkAliases("database")
      .start();
    const [hubImage] = await Promise.all([buildImage("hub"), buildImage("runner")]);
    await using hub = await hubImage
      .withNetwork(network)
      .withNetworkAliases("hub")
      .withEnvironment({
        DATABASE_URL: `postgres://${database.getUsername()}:${database.getPassword()}@database:5432/${database.getDatabase()}`,
        BETTER_AUTH_SECRET: "a-secret-only-for-tests-0123456789abcdef",
        HUB_URL: HUB_ORIGIN,
        ADMIN_EMAIL: OWNER.email,
        ADMIN_PASSWORD: OWNER.password,
      })
      .withExposedPorts(8787)
      .withWaitStrategy(Wait.forHealthCheck())
      .start();
    const url = `http://${hub.getHost()}:${hub.getMappedPort(8787)}`;
    const signIn = await fetch(`${url}/api/auth/sign-in/email`, {
      method: "POST",
      headers: { "content-type": "application/json", origin: HUB_ORIGIN },
      body: JSON.stringify(OWNER),
    });
    const owner: ContractRouterClient<typeof contract> = createORPCClient(
      createProblemDetailsLink(contract, {
        origin: url,
        headers: { cookie: signIn.headers.getSetCookie().join("; "), origin: HUB_ORIGIN },
      }),
    );
    const { key } = await owner.apiKeys.create({
      name: "runners",
      permissions: { runner: ["register"] },
    });

    // The Runner registers only once a sandboxed command has run: without one it refuses to start.
    await using runner = await new UnmaskedProcContainer("agent-batch-runner:test")
      .withNetwork(network)
      .withSecurityOpt("seccomp=unconfined", "apparmor=unconfined")
      .withEnvironment({
        HUB_URL: "http://hub:8787",
        RUNNER_REGISTRATION_KEY: key,
        // Pi needs a model it knows and a key for its provider; no job runs here.
        RUNNER_MODEL_PROVIDER: "anthropic",
        RUNNER_MODEL_ID: "claude-haiku-4-5",
        ANTHROPIC_API_KEY: "not a key",
      })
      .start();
    // A Runner registers under its host's name, which Docker makes the start of the container's ID.
    await expect
      .poll(() => owner.runners.list(), { timeout: 30_000 })
      .toMatchObject([{ name: runner.getId().slice(0, 12) }]);
  });
});
