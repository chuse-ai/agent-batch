import { execFile } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";

const run = promisify(execFile);

/**
 * Builds the image of `target` from the repository and returns its tag, through Buildx as the
 * release workflow does, including its context handling and Dockerfile frontend.
 */
// TODO: Recheck Testcontainers builds when the WSL2 build-context issue is resolved upstream:
// https://github.com/testcontainers/testcontainers-node/issues/1361
export async function buildRepositoryImage(target: "hub" | "runner") {
  const name = `agent-batch-${target}:test`;
  await run("docker", ["buildx", "build", "--load", "--target", target, "--tag", name, "."], {
    cwd: path.join(import.meta.dirname, "../../.."),
    timeout: 540_000,
    maxBuffer: 10 * 1024 * 1024,
  });
  return name;
}
