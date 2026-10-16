import {
  COMMON_ERROR_STATUS_MAP,
  ORPCError,
  ValidationError,
  type AnyORPCError,
} from "@orpc/server";
import { problemDetails, type ProblemDetails } from "hono-problem-details";
import { prettifyError } from "zod/mini";

// The API's errors as RFC 9457 Problem Details (ADR 0023): oRPC's errors through the handler's
// encoder here, and errors outside oRPC through the same library's handler in the app.

/** The Problem Details document for `error`, for oRPC's handler to answer with. */
export function toProblemDetails(error: AnyORPCError): ProblemDetails {
  return problemDetails({ status: statusOf(error.code), detail: detailOf(error) }).problemDetails;
}

/** The status oRPC's handler answers `code` with: codes it does not know answer 500. */
function statusOf(code: string): number {
  return Object.hasOwn(COMMON_ERROR_STATUS_MAP, code)
    ? COMMON_ERROR_STATUS_MAP[code as keyof typeof COMMON_ERROR_STATUS_MAP]
    : 500;
}

/**
 * What a caller learns beyond the status. Invalid input lists its problems, one per line in Zod's
 * format, as oRPC's recipe shows. Otherwise only errors the contract declares for the call are
 * public, and only with a message of their own; anything else, such as the hub's own output failing
 * its schema, answers with its title alone.
 */
function detailOf(error: AnyORPCError): string | undefined {
  if (error.code === "BAD_REQUEST" && error.cause instanceof ValidationError) {
    return prettifyError(error.cause);
  }
  // An error created without a message carries oRPC's default for its code, which the title says.
  const hasOwnMessage = error.message !== new ORPCError(error.code).message;
  return error.defined && hasOwnMessage ? error.message : undefined;
}
