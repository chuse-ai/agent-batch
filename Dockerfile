# syntax=docker/dockerfile:1

# Vite+'s Docker guide, "Production: SSR / Node.js server app", for the hub (ADR 0014). Base images
# are pinned by digest for reproducible builds; Dependabot keeps them current.

# --- build stage: the official Vite+ toolchain image ---
FROM ghcr.io/voidzero-dev/vite-plus:1.3.0@sha256:61860c1b58bf35878605280802809973c668f1594675298c3be553a98ac06e59 AS build
WORKDIR /app

# Install dependencies first so this layer is cached across source changes.
# Workspace packages resolve only with every workspace manifest present; no install scripts run.
COPY --chown=vp:vp package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY --chown=vp:vp apps/hub/package.json apps/hub/
COPY --chown=vp:vp apps/runner/package.json apps/runner/
COPY --chown=vp:vp packages/cli/package.json packages/cli/
COPY --chown=vp:vp packages/contract/package.json packages/contract/
RUN vp install --frozen-lockfile --ignore-scripts

# Build. The hub's build also copies the third-party files it loads into `dist/node_modules`, in
# place of a production-dependencies stage.
COPY --chown=vp:vp . .
RUN vp run hub#build

# --- runtime stage: distroless's Node image, which brings the libraries Node needs ---
FROM gcr.io/distroless/nodejs26-debian13:nonroot@sha256:044d090d0c3ccfd30722126ebe46704885698970c8a303419dc0d704d628f0da AS hub
WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app/apps/hub/dist/main.mjs ./dist/
COPY --from=build /app/apps/hub/dist/node_modules ./node_modules
# Applied as the hub starts (ADR 0015); the manifest makes Node load them as modules.
COPY --from=build /app/apps/hub/migrations ./migrations
COPY --from=build /app/apps/hub/package.json ./

EXPOSE 8787
CMD ["dist/main.mjs"]
