# TypeScript 7, strict

agent-batch is written in TypeScript with strict type checking, on TypeScript 7 (the native compiler). Pi and Cloudflare Computer, whose interfaces we implement, are TypeScript, and strict types are the main guard against script-quality code.

## Consequences

TypeScript 7's package exposes only unstable programmatic APIs, not the classic compiler API. Tools built on that API (typescript-eslint, ts-morph, some declaration bundlers) are ruled out; prefer tools with their own native type awareness.
