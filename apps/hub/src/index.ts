import type { EntityManager } from "@mikro-orm/postgresql";
import { OpenAPIHandler } from "@orpc/openapi/fetch";
import { implement } from "@orpc/server";
import { contract } from "contract";
import { Hono } from "hono";

import { RunnerSchema } from "./db/entities.ts";
import { withEntityManager } from "./db/orm.ts";

export { RunnerConnection } from "./runner-connection.ts";

interface ApiContext {
  em: EntityManager;
}

const os = implement<typeof contract, ApiContext>(contract);

const router = os.router({
  runners: {
    list: os.runners.list.handler(async ({ context }) => {
      const runners = await context.em.findAll(RunnerSchema);
      return runners.map((runner) => ({
        id: runner.id,
        name: runner.name,
        lastSeenAt: runner.lastSeenAt.toISOString(),
      }));
    }),
  },
});

const api = new OpenAPIHandler(router);

const app = new Hono<{ Bindings: Env }>();

app.get("/runners/:id/connect", (c) =>
  c.env.RUNNER_CONNECTION.getByName(c.req.param("id")).fetch(c.req.raw),
);

app.all("*", async (c) => {
  const { matched, response } = await withEntityManager((em) =>
    api.handle(c.req.raw, { context: { em } }),
  );
  return matched ? response : c.notFound();
});

export default app;
