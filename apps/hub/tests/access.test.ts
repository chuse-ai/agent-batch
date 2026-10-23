import { walkProcedureContractsSync } from "@orpc/server";
import { contract, getAccess } from "contract";
import { expect, test } from "vite-plus/test";

import { GRANTABLE_KEY_PERMISSIONS } from "../src/auth/options.ts";

// Who may make each call, as the contract declares it: the hub refuses to run a call that declares
// no one, and no key can make a call that needs a permission no member can grant.

test("every call says who may make it, with key permissions a member can grant", () => {
  walkProcedureContractsSync(contract, (procedure, path) => {
    const access = getAccess(procedure);
    expect(access, path.join(".")).toBeDefined();
    if (access?.key) {
      expect(GRANTABLE_KEY_PERMISSIONS.authorize(access.key).success, path.join(".")).toBe(true);
    }
  });
});
