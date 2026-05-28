# --- Multi-stage Dockerfile for the Semantic Cache service ---
# Standard 11: two stages keep the final image minimal — no build tools shipped.
#
# Stage 1 (builder): installs all Python dependencies into a virtual environment.
# Stage 2 (final):   copies only the venv and source — no pip, no gcc, no cache.
#
# Why python:3.11.9-slim for both stages?
# The builder needs a full Python install for pip and native extension compilation.
# The final stage also needs a full CPython runtime (not distroless) because
# aiokafka, openai, and numpy all require ctypes and other stdlib modules that
# are absent from distroless Python images. slim is the smallest image that
# provides a complete CPython runtime.

# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

# PYTHONDONTWRITEBYTECODE=1 skips .pyc file creation — reduces image size.
# PYTHONUNBUFFERED=1 forces stdout/stderr flush immediately — required for
# Docker log capture (buffered output can be lost on container kill).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Install build dependencies needed for native extensions (numpy, cryptography).
# --no-install-recommends keeps the layer small; we only need the compilers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first so Docker can cache this layer.
# If requirements.txt does not change, pip install is skipped on rebuild.
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: final image ──────────────────────────────────────────────────────
FROM python:3.11.9-slim AS final

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # TZ=UTC: Standard 2 — container clock is always UTC.
    TZ=UTC \
    # Tell Python where to find the packages installed in stage 1.
    PYTHONPATH=/install/lib/python3.11/site-packages

WORKDIR /app

# Create a non-root user (Standard 11: never run as root in production).
# uid/gid 1001 avoids collision with typical system accounts (1000 is often
# the first normal user on the host, so 1001 is safer for containerised apps).
RUN addgroup --gid 1001 appgroup \
    && adduser --uid 1001 --gid 1001 --disabled-password --gecos "" appuser

# Copy installed packages from the builder stage — no pip in the final image.
COPY --from=builder /install /install

# Copy service source code.
COPY --chown=appuser:appgroup . /app

# Switch to non-root user before the CMD runs.
USER appuser

# Port 8088: Prometheus metrics scrape endpoint.
# No HTTP API — this service is a Kafka consumer, not a REST server.
EXPOSE 8088

# HEALTHCHECK: verify the Prometheus endpoint is reachable.
# If /metrics returns non-200, Docker marks the container unhealthy and
# the orchestrator can restart it automatically.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8088/metrics', timeout=3)" || exit 1

# Run as a Python module so the working directory is added to sys.path
# and relative imports (cache, config, kafka.handler, etc.) resolve correctly.
CMD ["python", "main.py"]
