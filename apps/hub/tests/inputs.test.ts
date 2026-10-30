import { getOpenAPIMeta } from "@orpc/openapi";
import { walkProcedureContractsSync } from "@orpc/server";
import { apiContract } from "contract";
import { expect, test } from "vite-plus/test";

// Every API call refuses a parameter it does not define rather than run without it (ADR 0010).
// Checked on each input's top-level object; the batches tests send an unknown field nested in a job.
// The Runner's own calls ignore unknown fields instead, as the Runner protocol does (ADR 0026).

test("every call's input refuses a parameter it does not define", async () => {
  const checks: Promise<void>[] = [];
  walkProcedureContractsSync({ apiContract }, (procedure, path) => {
    for (const schema of procedure["~orpc"].inputSchemas ?? []) {
      // The upload's input is oRPC's request parts: oRPC fills the outer object and `headers` with
      // more than the call reads, so they stay open, and only `params` is the caller's to check.
      const stray =
        getOpenAPIMeta(procedure)?.inputStructure === "detailed"
          ? { params: { strayParameter: 1 } }
          : { strayParameter: 1 };
      checks.push(
        Promise.resolve(schema["~standard"].validate(stray)).then((result) => {
          const messages = (result.issues ?? []).map((issue) => issue.message);
          expect(messages.join("\n"), path.join(".")).toContain('"strayParameter"');
        }),
      );
    }
  });
  await Promise.all(checks);
});
