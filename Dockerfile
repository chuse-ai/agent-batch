# syntax=docker/dockerfile:1

# Vite+'s Docker guide, "Production: SSR / Node.js server app", for the hub (ADR 0014) and a Runner.
# Base images are pinned by digest for reproducible builds; Dependabot keeps them current.

# --- build stage: the official Vite+ toolchain image ---
FROM ghcr.io/voidzero-dev/vite-plus:1.3.0@sha256:61860c1b58bf35878605280802809973c668f1594675298c3be553a98ac06e59 AS build
WORKDIR /app

# Install dependencies first so this layer is cached across source changes. Every workspace manifest
# is copied first, as pnpm's `fetch` docs describe, since the install resolves the workspace's
# packages from them. No lifecycle script runs: no build here needs one, and the root's `prepare`
# sets up Git hooks (`vp config`).
COPY --chown=vp:vp package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY --chown=vp:vp apps/hub/package.json apps/hub/
COPY --chown=vp:vp apps/runner/package.json apps/runner/
COPY --chown=vp:vp packages/cli/package.json packages/cli/
COPY --chown=vp:vp packages/contract/package.json packages/contract/
# pnpm applies the workspace's patches as it installs.
COPY --chown=vp:vp patches/ patches/
RUN vp install --frozen-lockfile --ignore-scripts
COPY --chown=vp:vp . .

# --- the hub's build, in a stage of its own as the Runner's is, so building one image skips the
# other's build (Docker's multi-stage builds). It also copies the third-party files the hub loads into
# `dist/node_modules`, in place of a production-dependencies stage, and writes the notices of every
# package it ships. Vite Task's cache traces each process by preloading a library, which aborts under
# QEMU, as the arm64 release build runs (voidzero-dev/vite-task#879), so it is off; Docker caches the
# layers ---
FROM build AS hub-build
RUN vp run --no-cache hub#build && vp run --no-cache hub#notices

# --- the hub's runtime: distroless's Node image, which brings the libraries Node needs, libatomic1
# among them, which the guide's `distroless/cc` tip lacks (voidzero-dev/vite-plus#3219). The hub runs
# distroless's Node, which Dependabot keeps current, rather than the workspace's exact one ---
FROM gcr.io/distroless/nodejs26-debian13:nonroot@sha256:96455baef11b178fc135b32206e3922c515dbed2400676c94f907f8f84e6abf6 AS hub
WORKDIR /app
ENV NODE_ENV=production

COPY --from=hub-build /app/apps/hub/dist/main.mjs /app/apps/hub/dist/THIRD_PARTY_NOTICES.txt ./dist/
COPY --from=hub-build /app/apps/hub/dist/node_modules ./node_modules
# Applied as the hub starts (ADR 0015); the manifest makes Node load them as modules.
COPY --from=hub-build /app/apps/hub/migrations ./migrations
COPY --from=hub-build /app/apps/hub/package.json ./
COPY --from=hub-build /app/LICENSE ./LICENSE

EXPOSE 8787
# Healthy once the hub answers its health route, after migrating and setting up. For its first
# minute, until it first answers, Docker checks each second and counts no failure; after that, every
# 30 seconds. The image has no curl, so its Node fetches the route.
HEALTHCHECK --start-period=60s --start-interval=1s \
  CMD ["/nodejs/bin/node", "-e", "fetch(`http://localhost:${process.env.PORT ?? 8787}/healthz`).then((r) => process.exit(r.ok ? 0 : 1), () => process.exit(1))"]
CMD ["dist/main.mjs"]

# --- the Runner's build, its notices and its deployment: `pnpm deploy`, pnpm's way to build an image
# of one workspace package, copies the Runner's files (`files` in its manifest) with a fresh
# production install of its dependencies, whole, as the Runner loads some of their files by path,
# pi-env's daemon and sandbox-runtime's seccomp helpers among them. Vite+ runs it through a script
# (voidzero-dev/vite-plus#1038). Then the exact Node the Runner runs on, as the guide exports it, with
# its licence; the cache is off as for the hub ---
FROM build AS runner-build
RUN vp run --no-cache runner#build && vp run --no-cache runner#notices \
  && vp run --no-cache runner#pnpm-deploy /tmp/runner \
  && node="$(vp env which node | head -1)" \
  && cp "$node" /tmp/node && cp "$(dirname "$node")/../LICENSE" /tmp/node-LICENSE

# --- the Runner's runtime: Debian's current stable, slim, as the hub's distroless base is, with the
# programs each job's sandbox needs (ADR 0027) and libatomic1, which Node's official Linux builds link
# from Node 25 on (nodejs/node#60790, voidzero-dev/vite-plus#3219). It runs as a named user with a home
# and the data directory, as Node's official image runs as its `node` user, uid 1000; `nobody` has
# neither. The container needs `--security-opt seccomp=unconfined --security-opt
# systempaths=unconfined`, and `apparmor=unconfined` where the host has AppArmor, as BuildKit's
# rootless image does ---
FROM debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS runner
RUN apt-get update \
  && apt-get install -y --no-install-recommends bubblewrap socat ripgrep libatomic1 \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --uid 1000 --create-home runner \
  && install -d -o runner -g runner /data
COPY --from=runner-build /tmp/node /usr/local/bin/node
COPY --from=runner-build /tmp/node-LICENSE /usr/local/share/doc/node/LICENSE
COPY --from=runner-build /tmp/runner /app
COPY --from=runner-build /app/LICENSE /app/LICENSE
ENV NODE_ENV=production RUNNER_DATA_DIR=/data
USER runner
WORKDIR /app
CMD ["node", "dist/main.mjs"]
