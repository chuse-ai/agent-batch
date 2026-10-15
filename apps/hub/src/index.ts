import { OpenAPIHandler } from "@orpc/openapi/fetch";
import { implement } from "@orpc/server";
import { contract } from "contract";
import { Hono } from "hono";
import type { Kysely } from "kysely";

import { withDatabase } from "./db/database.ts";
import type { Database } from "./db/types.ts";
import { listRunners } from "./runners.ts";

export { RunnerConnection } from "./runner-connection.ts";

interface ApiContext {
  db: Kysely<Database>;
}

const os = implement<typeof contract, ApiContext>(contract);

const router = os.router({
  runners: {
    list: os.runners.list.handler(({ context }) => listRunners(context.db)),
  },
});

const api = new OpenAPIHandler(router);

const app = new Hono<{ Bindings: Env }>();

app.get("/runners/:id/connect", (c) =>
  c.env.RUNNER_CONNECTION.getByName(c.req.param("id")).fetch(c.req.raw),
);

app.all("*", async (c) => {
  const { matched, response } = await withDatabase(c.env.HYPERDRIVE.connectionString, (db) =>
    api.handle(c.req.raw, { context: { db } }),
  );
  return matched ? response : c.notFound();
});

export default app;
