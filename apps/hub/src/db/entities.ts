import { defineEntity, p, type InferKyselyDB } from "@mikro-orm/postgresql";

export const RunnerSchema = defineEntity({
  name: "Runner",
  tableName: "runners",
  properties: {
    id: p.string().primary(),
    name: p.string(),
    /** When the hub last heard from the Runner, by subscription or heartbeat; it only moves forward. */
    lastSeenAt: p.datetime(),
  },
});

export const entities = [RunnerSchema];

/** The database as Kysely sees it, typed from the entities: table and column names as in SQL. */
export type Database = InferKyselyDB<(typeof entities)[number]>;
