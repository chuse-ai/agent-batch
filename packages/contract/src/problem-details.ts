import { STATUS_CODES } from "node:http";

import { COMMON_ERROR_STATUS_MAP, ORPCError, type ClientContext } from "@orpc/client";
import type { RouterContract } from "@orpc/contract";
import { ResponseValidationLinkPlugin } from "@orpc/contract/plugins";
import { SmartCoercionLinkPlugin } from "@orpc/json-schema";
import { OpenAPILink, type OpenAPILinkOptions } from "@orpc/openapi/fetch";
import { ZodToJsonSchemaConverter } from "@orpc/zod";
import { parse as parseContentType } from "content-type";
import * as z from "zod/mini";

// The API's errors as RFC 9457 Problem Details (ADR 0023), as clients read them back into oRPC's
// errors; the hub produces them.

/** The content type of the API's errors. */
export const PROBLEM_DETAILS_CONTENT_TYPE = "application/problem+json; charset=utf-8";

/** An error as the API answers it: a Problem Details document, which RFC 9457 lets carry more members. */
export const problemDetailsSchema = z
  .looseObject({
    /** Always `about:blank`: a client reads any problem by its status (ADR 0023). */
    type: z.string(),
    /** The status's reason phrase, such as "Not Found". */
    title: z.string(),
    status: z.int(),
    /** What went wrong, when it says more than the title. */
    detail: z.optional(z.string()),
  })
  .register(z.globalRegistry, { id: "ProblemDetails" });
export type ProblemDetails = z.infer<typeof problemDetailsSchema>;

/** The oRPC code for `status`, such as `NOT_FOUND` for 404. */
function codeOf(status: number): string {
  return (
    Object.entries(COMMON_ERROR_STATUS_MAP).find(([, codeStatus]) => codeStatus === status)?.[0] ??
    String(status)
  );
}

/**
 * The Problem Details an error from the API carries, which the link below keeps as the error's
 * cause; an error oRPC raises itself, such as an answer the contract does not allow, has none.
 */
export function problemOf(error: unknown): ProblemDetails | undefined {
  if (!(error instanceof ORPCError)) return undefined;
  const problem = problemDetailsSchema.safeParse(error.cause);
  return problem.success ? problem.data : undefined;
}

/**
 * A link to the API at `contract` for oRPC's client. It reads Problem Details back into oRPC's errors
 * and checks every answer against the contract, which marks the errors the contract declares for a
 * call as typed. Values JSON cannot carry, such as dates, arrive as strings and are turned back into
 * the contract's types first.
 */
export function createProblemDetailsLink<T extends ClientContext>(
  contract: RouterContract,
  options: Omit<OpenAPILinkOptions<T>, "customErrorResponseBodyDecoder">,
): OpenAPILink<T> {
  const send = options.fetch ?? ((url: string, init: RequestInit) => globalThis.fetch(url, init));
  return new OpenAPILink<T>(contract, {
    ...options,
    // oRPC fetches with `redirect: "manual"` and reads any status below 400 as an answer, so a
    // redirect, as from an `http://` URL to its `https://` one, would be read as the call's answer;
    // the API never redirects, so the caller is to call the URL it names instead.
    //
    // An error page that is not Problem Details, as a proxy or gateway in front of the hub sends
    // while the hub restarts, in HTML or in JSON of its own, would fail to parse as the JSON the
    // contract says every body is, or as a problem, and lose its status; it reads as the problem
    // of its status, `about:blank`, as RFC 9457 has it for a problem that says no more than its
    // status, titled with its reason phrase, since HTTP/2 carries none of its own. The phrase comes
    // from Node's table, which workerd's `nodejs_compat` has too; a status it lacks, such as
    // Cloudflare's 52x, is titled with its number. A problem is known by its media type, which every
    // error of the hub carries.
    fetch: async (url, init, clientOptions, path) => {
      const response = await send(url, init, clientOptions, path);
      if (response.status >= 300 && response.status < 400) {
        await response.body?.cancel();
        throw new Error(
          `The API redirects to ${response.headers.get("location") ?? "another URL"}; call it there`,
        );
      }
      if (response.ok) return response;
      const contentType = parseContentType(response.headers.get("content-type") ?? "");
      if (contentType.type === "application/problem+json") {
        // Read as JSON also for a call whose answer is a file, which oRPC reads by its headers
        // rather than as the JSON every other call answers: the `standard-server` header is how
        // oRPC's client is told a body's kind. oRPC itself counts only `application/json` as JSON
        // (middleapi/orpc#2317).
        const headers = new Headers(response.headers);
        headers.set("standard-server", "json");
        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers,
        });
      }
      await response.body?.cancel();
      const title = STATUS_CODES[response.status] ?? String(response.status);
      return Response.json(
        { type: "about:blank", title, status: response.status },
        {
          status: response.status,
          statusText: response.statusText,
          headers: { "content-type": PROBLEM_DETAILS_CONTENT_TYPE, "standard-server": "json" },
        },
      );
    },
    customErrorResponseBodyDecoder: (body) => {
      const problem = problemDetailsSchema.safeParse(body);
      if (!problem.success) return undefined;
      const { status, title, detail } = problem.data;
      // The document itself is kept as the cause, for a client that shows it as the hub sent it.
      return new ORPCError(codeOf(status), { message: detail ?? title, cause: problem.data });
    },
    plugins: [
      ...(options.plugins ?? []),
      new ResponseValidationLinkPlugin(contract),
      // oRPC's Zod converter is what describes a date as one; Zod's own JSON Schema has no dates.
      new SmartCoercionLinkPlugin(contract, { converters: [new ZodToJsonSchemaConverter()] }),
    ],
  });
}
