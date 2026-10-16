/**
 * @module-tag container
 */
import path from "node:path";
import { text } from "node:stream/consumers";

import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { RUNNER_CONNECT_PATH, RUNNER_NOT_FOUND } from "contract";
import { GenericContainer, Network, Wait } from "testcontainers";
import { describe, expect, inject, test } from "vite-plus/test";
import WebSocket from "ws";

import { OWNER } from "./harness.ts";

// The hub's Docker image as an operator runs it: built from the repository and started against a
// fresh database, which it migrates and sets up as it starts (ADR 0014, ADR 0015). It needs Docker
// and an image build, so it is tagged and the default run skips it (ADR 0003).

const HUB_ORIGIN = "http://hub.test";

// The image is the same for both projects, so one of them runs it.
describe.skipIf(inject("hub") !== "node")("the hub's Docker image", () => {
  test("sets up a fresh database as it starts, serves the API, and stops gracefully", async () => {
    await using network = await new Network().start();
    await using database = await new PostgreSqlContainer("postgres:18-alpine")
      .withNetwork(network)
      .withNetworkAliases("database")
      .start();
    const image = await GenericContainer.fromDockerfile(path.join(import.meta.dirname, "../../.."))
      .withBuildkit()
      .withTarget("hub")
      .build();
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
      .withWaitStrategy(Wait.forHttp("/runners", 8787).forStatusCode(401))
      .start();
    const url = `http://${hub.getHost()}:${hub.getMappedPort(8787)}`;

    const signIn = await fetch(`${url}/api/auth/sign-in/email`, {
      method: "POST",
      headers: { "content-type": "application/json", origin: HUB_ORIGIN },
      body: JSON.stringify(OWNER),
    });
    expect(signIn.status).toBe(200);

    const socket = new WebSocket(`${url.replace("http", "ws")}${RUNNER_CONNECT_PATH}`, {
      headers: { "x-api-key": "not a key" },
    });
    socket.on("error", () => {});
    const refusal = await new Promise<{ status?: number; body: string }>((resolve) => {
      socket.once("unexpected-response", (_request, response) => {
        void text(response).then((body) => resolve({ status: response.statusCode, body }));
      });
    });
    socket.terminate();
    expect(refusal).toEqual({ status: 401, body: JSON.stringify({ code: RUNNER_NOT_FOUND }) });

    // Docker sends SIGTERM, then kills the container after the timeout; a graceful stop is quicker.
    const stopping = Date.now();
    await hub.stop({ timeout: 10_000 });
    expect(Date.now() - stopping).toBeLessThan(5_000);
  });
});
