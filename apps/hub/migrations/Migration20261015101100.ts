import { Migration } from "@mikro-orm/migrations";

export class Migration20261015101100 extends Migration {
  override name = "Migration20261015101100";

  override up(): void | Promise<void> {
    this.addSql(`create schema if not exists "agent_batch";`);
    this.addSql(
      `create table "agent_batch"."runners" ("id" varchar(255) not null, "name" varchar(255) not null, "last_seen_at" timestamptz not null, primary key ("id"));`,
    );
  }

  override down(): void | Promise<void> {
    this.addSql(`drop table if exists "agent_batch"."runners" cascade;`);

    this.addSql(`drop schema if exists "agent_batch";`);
  }
}
