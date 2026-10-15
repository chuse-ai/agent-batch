import { oc, type } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import * as z from "zod/mini";

/** A Runner as the hub reports it. */
export const runnerSchema = z.object({
  id: z.string(),
  name: z.string(),
  /**
   * When the hub last heard from the Runner: its latest subscription, or a heartbeat the hub has
   * recorded. A connected Runner's is at most one heartbeat timeout old.
   */
  lastSeenAt: z.iso.datetime(),
});
export type Runner = z.infer<typeof runnerSchema>;

/** The hub's public HTTP API. */
export const contract = {
  runners: {
    list: oc.meta(openapi({ method: "GET", path: "/runners" })).output(z.array(runnerSchema)),
  },
};

/** The hub's route where a Runner opens its WebSocket, with the Runner's ID as `:id`. */
export const RUNNER_CONNECT_ROUTE = "/runners/:id/connect";

/** The path where the Runner `runnerId` opens its WebSocket to the hub. */
export function runnerConnectPath(runnerId: string): string {
  return RUNNER_CONNECT_ROUTE.replace(":id", encodeURIComponent(runnerId));
}

/** How often a Runner sends a heartbeat. */
export const HEARTBEAT_INTERVAL_MS = 30_000;
/** After this long without heartbeats (three missed), each side treats the other as gone. */
export const HEARTBEAT_TIMEOUT_MS = 3 * HEARTBEAT_INTERVAL_MS;
/**
 * A heartbeat on the wire: the Runner sends `HEARTBEAT_PING` as a text message, and the hub's runtime
 * answers `HEARTBEAT_PONG` without waking the hub. Neither is an oRPC message, so oRPC ignores both.
 */
export const HEARTBEAT_PING = "ping";
export const HEARTBEAT_PONG = "pong";

/** What the hub tells a Runner to do. None exist yet; jobs will be the first. */
export type Command = never;

/** What a Runner calls on the hub over its WebSocket. */
export const runnerProtocol = {
  /**
   * Registers the Runner and streams the hub's commands to it while the socket stays open; a Runner
   * subscribes again whenever the stream ends. The output is declared with `type` because the hub
   * answers with oRPC's hibernating iterator, which a validating `eventIterator` schema would wrap.
   */
  commands: oc.input(z.object({ name: z.string() })).output(type<AsyncIteratorObject<Command>>()),
};
