# ─── Stage 1: Build ───────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Download dependencies before copying source so this layer is cached
# unless go.mod / go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server .

# ─── Stage 2: Minimal runtime ─────────────────────────────────────────────────
# distroless/static:nonroot contains no shell, no package manager, no libc.
# Attack surface is minimal: the only executable is our binary.
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /app/server /app/server

USER nonroot:nonroot

EXPOSE 8082

ENV TZ=UTC

HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
  CMD ["/app/server", "-health-check"]

CMD ["/app/server"]
