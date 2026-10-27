import { defineMeta, error, oc } from "@orpc/contract";
import { openapi } from "@orpc/openapi";
import { en } from "zod/locales";
import * as z from "zod/mini";

import { PROBLEM_DETAILS_CONTENT_TYPE } from "./problem-details.ts";

// What every procedure of the API and of the Runner protocol is built from: the errors they share,
// the builder and who may make a call.

// `zod/mini` leaves messages out unless asked; without them every problem reads "Invalid input".
z.config(en());

/**
 * The permissions a call can require of an API key, by resource: `register` for the shared key
 * Runners start with, `actAs` for a Runner's own key, acting as that Runner over the socket and its
 * HTTP calls, and the job calls. The hub's access control holds them among its statements.
 */
export const KEY_PERMISSIONS = {
  runner: ["register", "actAs"],
  job: ["create", "read", "pause", "resume", "cancel", "delete"],
} as const;

/** Actions of one resource of `KEY_PERMISSIONS` a call requires, such as `{ job: ["pause"] }`. */
export type KeyPermissions = {
  [Resource in keyof typeof KEY_PERMISSIONS]: {
    [Only in Resource]: (typeof KEY_PERMISSIONS)[Resource][number][];
  };
}[keyof typeof KEY_PERMISSIONS];

/** A call made without a signed-in member or an API key holding the permission it needs. */
export const Unauthorized = error("UNAUTHORIZED");
/** A call that acts for the Organization, made before the hub is set up. */
export const HubNotSetUp = error("SERVICE_UNAVAILABLE", { message: "The hub is not set up" });
/** A call naming a Runner, an API key or a job the hub does not have. */
export const NotFound = error("NOT_FOUND");

/**
 * Every HTTP call, the API's and the Runner's, whose bodies are JSON unless the call says otherwise.
 * oRPC's client reads only `application/json` as JSON by itself, and errors arrive as Problem
 * Details. Every call says who may make it (`byKey`, `byMember` or `byKeyOrMember`), and the hub
 * refuses anyone else with `UNAUTHORIZED`.
 */
export const api = oc.errors({ [Unauthorized.code]: Unauthorized }).meta(
  openapi({ responseBodyHint: "json" }),
  // oRPC's OpenAPI document labels every error response `application/json`, with no setting to
  // change it (middleapi/orpc#2317).
  openapi.spec((operation) => ({
    ...operation,
    responses: Object.fromEntries(
      Object.entries(operation.responses ?? {}).map(([status, response]) => {
        const body = "content" in response ? response.content?.["application/json"] : undefined;
        return /^[45]/.test(status) && body
          ? [status, { ...response, content: { [PROBLEM_DETAILS_CONTENT_TYPE]: body } }]
          : [status, response];
      }),
    ),
  })),
);
/**
 * How the OpenAPI document describes an API key, sent in `x-api-key`. A call's security lists the
 * permissions the key needs as role names.
 */
export const API_KEY_SECURITY_SCHEME = {
  type: "apiKey",
  in: "header",
  name: "x-api-key",
  description:
    "An API key, sent in `x-api-key`. A call lists the permissions the key needs as `resource:action`, all of them required. A call that also takes a member's session checks the key whenever one is sent, whatever session the request carries.",
} as const;

/**
 * Who may make a call: with an API key holding the permissions in `key`, as a signed-in member, or
 * either. The hub checks it before every call, and the OpenAPI document gives it as the call's
 * `security`.
 */
export interface Access {
  key?: KeyPermissions;
  member?: true;
}

const [accessMeta, getAccess] = defineMeta("access", (incoming: Access) => incoming);
/** The access a procedure declares; the hub refuses to run a procedure that declares none. */
export { getAccess };

/** A key's permissions as OpenAPI role names, `resource:action`, such as `job:pause`. */
function roleNamesOf(permissions: KeyPermissions) {
  return Object.entries<string[]>(permissions).flatMap(([resource, actions]) =>
    actions.map((action) => `${resource}:${action}`),
  );
}

/**
 * Declares who may make a call, as the hub's access and as the OpenAPI document's `security`, where
 * an API key (`apiKey`) and a signed-in member (`session`, the hub's session cookie, which the hub
 * names for its deployment) are alternatives.
 */
function calledBy(access: Access) {
  const security: Record<string, string[]>[] = [];
  if (access.key) security.push({ apiKey: roleNamesOf(access.key) });
  if (access.member) security.push({ session: [] });
  return [accessMeta(access), openapi.spec((operation) => ({ ...operation, security }))] as const;
}
export const byKey = (permissions: KeyPermissions) => calledBy({ key: permissions });
export const byMember = calledBy({ member: true });
export const byKeyOrMember = (permissions: KeyPermissions) =>
  calledBy({ key: permissions, member: true });

/** Calls that act for the Organization, which exists once the hub is set up. */
export const forOrganization = api.errors({ [HubNotSetUp.code]: HubNotSetUp });
