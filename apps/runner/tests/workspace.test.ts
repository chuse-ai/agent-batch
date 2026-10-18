import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createEnvConformance, createExpectAssertions } from "@earendil-works/pi-durable/testing";
import { expect, it } from "vite-plus/test";

import { WorkspaceEnv } from "../src/workspace.ts";

// pi-durable's tools reach a job's Workspace only through this environment, so it is held to the
// cases pi-durable defines for every environment. The Workspace refuses symbolic links, and it
// cannot be watched: only hosts that show files as they change watch them, and none does yet.
const cases = createEnvConformance({
  assertions: createExpectAssertions(expect),
  withEnv: async (use) => {
    const root = await mkdtemp(path.join(tmpdir(), "workspace-"));
    try {
      await use(new WorkspaceEnv("conformance", root));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  },
  symlinks: false,
});

for (const conformance of cases.filter((test) => !test.name.startsWith("watch "))) {
  it(conformance.name, () => conformance.run(), conformance.timeoutMs);
}

// pi-durable's cases leave out the byte-order mark, which files saved by Windows tools, such as
// Excel's CSVs, begin with: an agent's edit must keep it.
it("a file's text keeps its byte-order mark, which pi-durable's edit tool writes back", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "workspace-"));
  try {
    await writeFile(path.join(root, "q3.csv"), "\uFEFFquarter,revenue\r\n");
    const text = await new WorkspaceEnv("bom", root).readTextFile("/q3.csv", BACKGROUND_CONTEXT);
    expect(text).toEqual({ ok: true, value: await readFile(path.join(root, "q3.csv"), "utf8") });
    expect(text.ok && text.value.startsWith("\uFEFF")).toBe(true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
