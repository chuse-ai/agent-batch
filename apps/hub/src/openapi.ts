import { OpenAPIGenerator } from "@orpc/openapi";
import { ZodToJsonSchemaConverter } from "@orpc/zod";
import { getCookies } from "better-auth/cookies";
import { API_KEY_SECURITY_SCHEME, contract, problemDetailsSchema } from "contract";

import { Auth } from "./auth/auth.ts";

// The API's OpenAPI document (ADR 0018), generated from the contract by oRPC, so every procedure
// appears in it as it is added. The contract says who may make each call and labels its errors as
// Problem Details (ADR 0023); the hub adds what depends on its deployment.

const converter = new ZodToJsonSchemaConverter();
const generator = new OpenAPIGenerator({ converters: [converter] });

/** The cookie a member's session travels in, as Better Auth names it for the hub's settings. */
export const sessionCookieName = Auth.use((auth) =>
  auth.call(async (betterAuth) => getCookies(betterAuth.options).sessionToken.name),
);

/** Generates the hub's OpenAPI document, with members' sessions in the cookie `sessionCookie`. */
export function generateOpenAPIDocument(sessionCookie: string) {
  return generator.generate(contract, {
    base: {
      // Releases are not versioned yet; publishing them gives the document its version.
      info: { title: "agent-batch", version: "0.0.0" },
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
