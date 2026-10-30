import { OpenAPIGenerator } from "@orpc/openapi";
import { ZodToJsonSchemaConverter } from "@orpc/zod";
import { getCookies } from "better-auth/cookies";
import {
  API_KEY_SECURITY_SCHEME,
  apiContract,
  MAX_BODY_BYTES,
  MAX_FILE_BYTES,
  problemDetailsSchema,
} from "contract";

import packageJson from "../package.json" with { type: "json" };
import { Auth } from "./auth/auth.ts";

// The API's OpenAPI document (ADR 0018), generated from the public contract by oRPC, so every
// public procedure appears in it as it is added; the Runner's own calls are not in it. The contract
// says who may make each call and labels its errors as Problem Details (ADR 0023); the hub adds what
// depends on its deployment.

const converter = new ZodToJsonSchemaConverter();
const generator = new OpenAPIGenerator({ converters: [converter] });

/** The cookie a member's session travels in, as Better Auth names it for the hub's settings. */
export const sessionCookieName = Auth.use((auth) =>
  auth.call(async (betterAuth) => getCookies(betterAuth.options).sessionToken.name),
);

/** Generates the hub's OpenAPI document, with members' sessions in the cookie `sessionCookie`. */
export function generateOpenAPIDocument(sessionCookie: string) {
  return generator.generate(apiContract, {
    base: {
      // The hub's own version, which a release sets to the release's.
      info: {
        title: "agent-batch",
        version: packageJson.version,
        // A limit every call shares, which no one operation's description would carry.
        description: `A request's body may hold at most ${MAX_BODY_BYTES / 1024 / 1024} MiB, and an upload's at most ${MAX_FILE_BYTES / 1024 / 1024} MiB; a larger body is refused with 413. A field a request's body does not define is refused with 400, naming it.`,
      },
      components: {
        securitySchemes: {
          apiKey: API_KEY_SECURITY_SCHEME,
          session: { type: "apiKey", in: "cookie", name: sessionCookie },
        },
      },
    },
    customErrorResponseBodySchema: converter.convert(problemDetailsSchema, "output")[0],
  });
}
