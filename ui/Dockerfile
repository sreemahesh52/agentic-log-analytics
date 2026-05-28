# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Build
# npm ci is used instead of npm install: it reads package-lock.json exactly,
# fails if lock file is out of sync, and never updates it. This guarantees
# reproducible builds between developer machines and CI.
# ─────────────────────────────────────────────────────────────────────────────
FROM node:18.20-alpine AS builder

WORKDIR /app

# Copy package.json always; package-lock.json only if it exists.
# If the lock file is present (as it should be after the first `npm install`),
# `npm ci` installs exactly the locked versions and fails fast if lock is stale.
# If the lock file is absent (first-time checkout before lock is committed),
# `npm install` generates it. Commit package-lock.json after the first build.
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi

COPY . .
RUN npm run build

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Serve
# nginx:1.26-alpine serves the static dist/ output. nginx handles thousands of
# concurrent connections with negligible memory, which Node.js cannot match for
# pure static file serving. The custom nginx.conf also handles SPA routing and
# proxies /api to the gateway so the browser has a single origin.
# ─────────────────────────────────────────────────────────────────────────────
FROM nginx:1.26-alpine AS final

# Remove default nginx config and html
RUN rm /etc/nginx/conf.d/default.conf && rm -rf /usr/share/nginx/html/*

COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/app.conf

EXPOSE 3001

HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=15s \
  CMD curl -f http://localhost:3001 || exit 1

CMD ["nginx", "-g", "daemon off;"]
