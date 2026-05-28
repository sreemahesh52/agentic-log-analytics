# --- Multi-stage Dockerfile for the RCA Agent service ---
# Standard 11: two stages keep the final image minimal — no build tools shipped
# to production. The builder compiles native extensions (asyncpg, confluent-kafka);
# the final stage copies only the installed packages and source.
#
# Why python:3.11.9-slim for both stages?
# asyncpg and confluent-kafka both require ctypes and C stdlib present in slim.
# Distroless Python images lack these, causing import failures at runtime.
# slim is the smallest image that ships a complete CPython runtime.
#
# Why a venv instead of --prefix=/install?
# A venv is the standard Python isolation unit. pip resolves paths, scripts,
# and data files correctly relative to the venv root. --prefix can silently
# miscalculate site-packages paths for packages that use pkg_resources.

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

# PYTHONDONTWRITEBYTECODE=1: skip .pyc files — reduces image size slightly.
# PYTHONUNBUFFERED=1: flush stdout/stderr immediately — required for Docker
#   log capture (buffered output is silently lost on container kill signals).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Install build tools required by native extensions:
#   build-essential: gcc + g++ + make — g++ is required by chroma-hnswlib (C++)
#                    which chromadb depends on for its HNSW vector index.
#   librdkafka-dev:  confluent-kafka C library headers for compilation.
# --no-install-recommends: skip optional packages to keep the builder lean.
# Clean apt lists in the same RUN layer — each RUN creates a Docker layer;
# cleaning in a separate layer does NOT reduce image size.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Create an isolated virtualenv.
# Using a venv (not --prefix) makes COPY /venv self-contained and ensures
# pip, scripts, and data files all resolve to the correct paths at runtime.
RUN python -m venv /venv

# Copy requirements first to exploit Docker's layer cache.
# If only source files change (not requirements.txt), the pip install layer
# is reused from cache — significantly faster CI rebuilds.
COPY requirements.txt .
RUN /venv/bin/pip install --no-cache-dir --upgrade pip \
    && /venv/bin/pip install --no-cache-dir -r requirements.txt

# ── Stage 2: final image ──────────────────────────────────────────────────────
FROM python:3.11.9-slim AS final

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # TZ=UTC: Standard 2 — container clock is always UTC. Belt-and-suspenders
    # alongside server_settings={"timezone": "UTC"} on every asyncpg connection.
    TZ=UTC \
    # PATH: add venv bin so python/pip/scripts resolve without activation.
    PATH="/venv/bin:$PATH"

WORKDIR /app

# Install librdkafka runtime library — required by confluent-kafka at runtime.
# The builder had the -dev package (headers + lib); the final image needs only
# the shared library (.so). No gcc or headers needed here.
RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user (Standard 11: never run as root in production containers).
# uid/gid 1001: avoids collision with uid 1000 (common host user), reducing
# the risk of accidental privilege escalation via mounted volumes.
RUN addgroup --gid 1001 appgroup \
    && adduser --uid 1001 --gid 1001 --disabled-password --gecos "" appuser

# Copy installed packages from builder stage.
# /venv contains all installed packages. Copying it is faster than re-installing.
COPY --from=builder /venv /venv

# Copy service source. chown at COPY time avoids a separate RUN chown layer.
COPY --chown=appuser:appgroup . /app

# Set ownership on venv so appuser can read the packages.
RUN chown -R appuser:appgroup /venv

# Switch to non-root before any CMD / HEALTHCHECK runs.
USER appuser

# Port 8090: Prometheus metrics scrape endpoint (no HTTP API — Kafka consumer only).
# Matches settings.metrics_port default and the docker-compose healthcheck.
EXPOSE 8090

# HEALTHCHECK: verify the Prometheus endpoint responds within 3 seconds.
# Docker marks the container unhealthy if this exits non-zero, triggering
# restart via restart: unless-stopped in docker-compose.
# start_period=60s: the cross-encoder model takes ~30s to load from disk on
# first startup. Docker waits 60s before starting healthcheck evaluation.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=60s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8090/metrics', timeout=3)" || exit 1

CMD ["python", "main.py"]
