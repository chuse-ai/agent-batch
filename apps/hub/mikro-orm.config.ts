import { Migrator } from "@mikro-orm/migrations";
import { defineConfig } from "@mikro-orm/postgresql";

import { baseConfig } from "./src/db/config.ts";

export default defineConfig({
  ...baseConfig,
  // The CLI names the schema snapshot after the database, so a fixed name keeps one snapshot for
  // everyone. `migration:create` and `migration:check` read the snapshot instead of connecting.
  dbName: "agent_batch",
  extensions: [Migrator],
});
