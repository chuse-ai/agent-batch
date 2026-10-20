import { writeSync } from "node:fs";

import { startRunner } from "runner";

import { callTool, scriptedModel, untilAborted } from "./scripted-model.ts";

// A Runner in a process of its own, which `runner-restart.test.ts` kills outright. Its arguments are
// the hub's URL, the key Runners register with, the data directory, and the phase, which sets what
// its scripted model does: before the kill, the agent appends a line to a file, and the model call
// after that tells the test, on file descriptor 3, that it is waiting; after the kill, the agent
// continues and submits the file's line count as its result.

const [hubUrl, registrationKey, dataDir, phase] = process.argv.slice(2);
if (!hubUrl || !registrationKey || !dataDir) {
  throw new Error("Usage: runner-restart.runner.ts <hub URL> <key> <data dir> <phase>");
}
const { faux, models, model } = scriptedModel();
faux.setResponses(
  phase === "before"
    ? [
        callTool("bash", { command: "echo marked >> marks.txt" }),
        // pi-durable calls the model again only once the command's result is in the journal.
        (...args) => {
          writeSync(3, "waiting\n");
          return untilAborted(...args);
        },
      ]
    : [
        (context) => {
          // A job that started over would show the model no earlier tool result.
          if (!context.messages.some((message) => message.role === "toolResult")) {
            throw new Error("The job started over instead of continuing");
          }
          return callTool("bash", {
            command: `printf '{"lines":%d}' "$(wc -l < marks.txt)" > result.json`,
          });
        },
        callTool("submit_result", { path: "result.json" }),
      ],
);
await startRunner({ hubUrl, registrationKey, dataDir, models, model });
