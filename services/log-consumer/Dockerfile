# ─── Stage 1: Build ───────────────────────────────────────────────────────────
# golang:1.22-alpine: Alpine base keeps the builder image small.
# CGO is disabled in the build command below so the binary is statically linked
# and can run in the distroless final image which has no libc.
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Copy go.mod and go.sum first — Docker caches this layer separately.
# go mod download fetches all dependencies declared in go.sum.
# If only source files change (not go.mod/go.sum), this layer is reused.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0: produce a statically-linked binary (no dynamic C library deps).
# GOOS=linux: cross-compile for Linux even if building on macOS/Windows.
# -o /app/server: place the binary at a known path for the COPY in stage 2.
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server .

# ─── Stage 2: Minimal runtime ─────────────────────────────────────────────────
# distroless/static:nonroot contains no shell, no package manager, no libc.
# The attack surface is minimal: the only executable in the image is our binary.
# nonroot: the image's default user is uid=65532 — we never run as root.
FROM gcr.io/distroless/static:nonroot

# Copy only the compiled binary — no source, no build tools in the final image.
COPY --from=builder /app/server /app/server

# USER nonroot:nonroot ensures the process runs as a non-root user.
# This satisfies Standard 11 and reduces the blast radius of any compromise.
USER nonroot:nonroot

EXPOSE 8084

# TZ=UTC: Standard 2 — UTC everywhere, even in the final container runtime.
ENV TZ=UTC

# HEALTHCHECK: Docker uses the exit code of this command to mark the container
# healthy or unhealthy. The -health-check flag triggers a lightweight HTTP probe.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
  CMD ["/app/server", "-health-check"]

CMD ["/app/server"]
