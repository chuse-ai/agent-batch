import { MikroORM, type EntityManager } from "@mikro-orm/postgresql";
import { env } from "cloudflare:workers";

import compiledFunctions from "./compiled-functions.js";
import { baseConfig } from "./config.ts";

let shared: { orm: MikroORM; clientUrl: string } | undefined;

/**
 * Runs `work` with an entity manager of its own.
 *
 * Creating a MikroORM runs entity discovery, which costs more with every entity, so one instance is
 * created on the first request (workerd forbids it at module scope) and every call forks it. Workers
 * forbid using a connection in any request but the one that opened it, so the `pg` pool never keeps
 * one: `maxUses: 1` closes each connection when it is released, and an unbounded `max` lets every
 * request open its own instead of waiting for another request's. Hyperdrive pools the real
 * connections to Postgres. The instance is replaced if Hyperdrive's connection string changes.
 */
export async function withEntityManager<T>(work: (em: EntityManager) => Promise<T>): Promise<T> {
  const clientUrl = env.HYPERDRIVE.connectionString;
  if (shared?.clientUrl !== clientUrl) {
    shared = {
      clientUrl,
      orm: new MikroORM({
        ...baseConfig,
        clientUrl,
        compiledFunctions,
        driverOptions: { max: Number.POSITIVE_INFINITY, maxUses: 1 },
      }),
    };
  }
  return work(shared.orm.em.fork());
}
