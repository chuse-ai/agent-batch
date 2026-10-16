import { COMMON_ERROR_STATUS_MAP, ORPCError, type ClientContext } from "@orpc/client";
import type { RouterContract } from "@orpc/contract";
import { ResponseValidationLinkPlugin } from "@orpc/contract/plugins";
import { SmartCoercionLinkPlugin } from "@orpc/json-schema";
import { OpenAPILink, type OpenAPILinkOptions } from "@orpc/openapi/fetch";
import { ZodToJsonSchemaConverter } from "@orpc/zod";
import * as z from "zod/mini";

// The API's errors as RFC 9457 Problem Details (ADR 0023), as clients read them back into oRPC's
// errors; the hub produces them.

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

/** The oRPC code for `status`, such as `NOT_FOUND` for 404. */
function codeOf(status: number): string {
  return (
    Object.entries(COMMON_ERROR_STATUS_MAP).find(([, codeStatus]) => codeStatus === status)?.[0] ??
    String(status)
  );
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
