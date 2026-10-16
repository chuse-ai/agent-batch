import {
  COMMON_ERROR_STATUS_MAP,
  ORPCError,
  type AnyORPCError,
  type ClientContext,
} from "@orpc/client";
import { ValidationError, type RouterContract } from "@orpc/contract";
import { ResponseValidationLinkPlugin } from "@orpc/contract/plugins";
import { SmartCoercionLinkPlugin } from "@orpc/json-schema";
import { OpenAPILink, type OpenAPILinkOptions } from "@orpc/openapi/fetch";
import { getOwn } from "@orpc/shared";
import { ZodToJsonSchemaConverter } from "@orpc/zod";
import statuses from "statuses";
import * as z from "zod/mini";

// The API answers errors as RFC 9457 Problem Details, so callers in any language read them as a
// standard rather than oRPC's own format. Every problem is `about:blank`: the HTTP status carries
// the meaning, and `title` and `detail` are for people. Inside the hub and the TypeScript client,
// errors keep oRPC's codes, one per status.

/** The media type of a Problem Details document. */
export const PROBLEM_JSON = "application/problem+json";

/** An error as the API answers it: a Problem Details document. */
export const problemDetailsSchema = z.object({
  /** Always `about:blank` for now; a client reads any type by its status. */
  type: z.string(),
  /** The status's reason phrase, such as "Not Found". */
  title: z.string(),
  status: z.int(),
  /** What went wrong, when it says more than the title. */
  detail: z.optional(z.string()),
});
export type ProblemDetails = z.infer<typeof problemDetailsSchema>;

/** The status oRPC's handler answers `code` with, by its own rule: codes it does not know answer 500. */
function statusOf(code: string): number {
  return getOwn(COMMON_ERROR_STATUS_MAP, code) ?? 500;
}

/** The oRPC code for `status`, such as `NOT_FOUND` for 404. */
function codeOf(status: number): string {
  return (
    Object.entries(COMMON_ERROR_STATUS_MAP).find(([, codeStatus]) => codeStatus === status)?.[0] ??
    String(status)
  );
}

/** The Problem Details document for `error`, for the API's handler to answer with. */
export function toProblemDetails(error: AnyORPCError): ProblemDetails {
  const status = statusOf(error.code);
  const title = statuses.message[status] ?? codeOf(status);
  // An error created without a message carries oRPC's default for its code, which the title says.
  const message = error.message === new ORPCError(error.code).message ? undefined : error.message;
  // Invalid input carries its problems, one per line in Zod's format, as oRPC's recipe shows.
  const issues = error.cause instanceof ValidationError ? z.prettifyError(error.cause) : undefined;
  const detail = [message, issues].filter(Boolean).join("\n");
  return { type: "about:blank", title, status, ...(detail ? { detail } : {}) };
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
  return new OpenAPILink<T>(contract, {
    ...options,
    customErrorResponseBodyDecoder: (body) => {
      const problem = problemDetailsSchema.safeParse(body);
      if (!problem.success) return undefined;
      const { status, title, detail } = problem.data;
      return new ORPCError(codeOf(status), { message: detail ?? title });
    },
    plugins: [
      ...(options.plugins ?? []),
      new ResponseValidationLinkPlugin(contract),
      // oRPC's Zod converter is what describes a date as one; Zod's own JSON Schema has no dates.
      new SmartCoercionLinkPlugin(contract, { converters: [new ZodToJsonSchemaConverter()] }),
    ],
  });
}
