# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — builder: install Python dependencies into /install
# Multi-stage build: the builder has gcc and dev headers; the runtime stage
# does not. This keeps the final image small and free of build tooling.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

WORKDIR /build

# gcc: needed to compile any C extensions (asyncpg uses Cython).
# libssl-dev: confluent-kafka's binary wheel links against OpenSSL at runtime;
#             installing libssl-dev in builder ensures the .so files are present.
# libpq-dev: asyncpg includes optional libpq support for some auth methods.
# --no-install-recommends: keeps the layer lean by skipping suggested packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libssl-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# --prefix=/install collects all installed packages under /install so the
# runtime stage can copy them in a single COPY --from=builder layer.
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime: minimal image, non-root user, no build tools
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11.9-slim

# PYTHONDONTWRITEBYTECODE=1: skip .pyc files — not needed in containers.
# PYTHONUNBUFFERED=1: stdout/stderr are unbuffered — Docker captures logs live.
# TZ=UTC: Standard 2 — all timestamps in UTC, regardless of host OS locale.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

# curl: used by the Docker health check (curl -f http://localhost:8083/metrics).
# ca-certificates: needed for TLS connections (confluent-kafka → Kafka over SSL).
# libssl3: confluent-kafka's binary wheel requires libssl at runtime on Bookworm.
# Create a non-root user: Standard 11 — never run application code as root.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1001 appuser \
    && useradd --uid 1001 --gid 1001 --no-create-home appuser

# Copy installed packages from the builder stage into the runtime Python path.
COPY --from=builder /install /usr/local

WORKDIR /app
COPY . .

# Give appuser ownership of /app so it can write logs or temp files if needed.
RUN chown -R appuser:appuser /app

# Switch to non-root user before CMD — any runtime vulnerability has
# limited privileges (cannot write outside /app, cannot bind low ports).
USER appuser

# Prometheus metrics server listens on 8083.
EXPOSE 8083

# Health check: the Prometheus metrics endpoint is always served while the
# service is running. A failed curl means the service has crashed.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
    CMD curl -f http://localhost:8083/metrics || exit 1

# Run main.py as a module with python — not uvicorn. This is a Kafka consumer
# (no HTTP server), so uvicorn is not appropriate here.
CMD ["python", "main.py"]
