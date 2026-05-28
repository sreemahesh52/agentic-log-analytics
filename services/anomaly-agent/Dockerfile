# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: builder
# Install all Python dependencies into a venv so the final image only needs
# to COPY the venv — no build tools (gcc, pip) end up in the production image.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

# PYTHONDONTWRITEBYTECODE: skip .pyc files — not needed in containers (no reuse).
# PYTHONUNBUFFERED: stdout/stderr go straight to Docker logs, no buffering delay.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# --- System dependencies required by confluent-kafka (librdkafka C bindings) ---
# confluent-kafka is a C extension. It needs build-essential + librdkafka-dev at
# build time. The final stage does NOT need them — only the compiled .so file.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# --- Create isolated virtualenv ---
# Using a venv (not site-packages) makes COPY /venv clean and self-contained.
RUN python -m venv /venv

# --- Install Python dependencies ---
# Copy requirements first to leverage Docker layer caching:
# if requirements.txt unchanged, this layer is reused on subsequent builds.
COPY requirements.txt .
RUN /venv/bin/pip install --no-cache-dir --upgrade pip \
    && /venv/bin/pip install --no-cache-dir -r requirements.txt

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: final image
# Minimal runtime image — no build tools, no pip, no gcc.
# python:3.11.9-slim is pinned (never :latest) for reproducible builds.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11.9-slim

# PYTHONDONTWRITEBYTECODE + PYTHONUNBUFFERED: same as builder stage.
# TZ=UTC: Standard 2 — container clock always UTC regardless of host OS.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    # PATH: add venv bin to PATH so python/pip commands use the venv without activation.
    PATH="/venv/bin:$PATH"

# --- Runtime system libraries ---
# confluent-kafka's .so file links against librdkafka at runtime.
# libpq5 is the PostgreSQL client library needed by psycopg2-binary.
# These are smaller than the dev headers installed in the builder stage.
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# --- Non-root user ---
# Standard 11: never run as root. Create appuser with no login shell.
# UID 10001 is above the reserved range (0–999 system, 1000 standard user).
RUN groupadd --gid 10001 appuser \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

# --- Copy virtualenv from builder ---
# /venv contains all installed packages. Copying it is faster than re-installing.
COPY --from=builder /venv /venv

# --- Copy application source ---
# Copy service source files into /app
COPY . .

# --- Set ownership ---
# chown after COPY so all files are owned by appuser, not root.
RUN chown -R appuser:appuser /app /venv

# Switch to non-root user for all subsequent commands and the final CMD.
USER appuser

# EXPOSE documents the Prometheus metrics port — not published unless docker-compose maps it.
EXPOSE 8085

# HEALTHCHECK: curl /metrics endpoint exposed by prometheus_client.start_http_server().
# interval=10s: check every 10 seconds.
# timeout=5s: curl must respond within 5 seconds.
# retries=5: mark unhealthy after 5 consecutive failures.
# start_period=30s: don't count failures during the first 30 seconds (startup time).
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8085/metrics', timeout=3)" || exit 1

# Run the anomaly-agent consumer loop.
CMD ["python", "main.py"]
