import { expect, test } from "vite-plus/test";

import { agentBatchCli } from "../src/index.ts";

// The commands, their flags and their help come from the contract through trpc-cli. This
// pins them, so a change to the contract or a trpc-cli upgrade that changes a command shows.
test("the commands and their flags", () => {
  expect(agentBatchCli("0.0.0").toJSON()).toMatchSnapshot();
});
