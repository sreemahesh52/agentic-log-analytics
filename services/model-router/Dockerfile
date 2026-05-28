# --- Multi-stage Dockerfile for the Model Router service ---
# Standard 11: two stages keep the final image minimal — no build tools shipped
# to production. The builder compiles native extensions; the final stage copies
# only the installed packages and source.
#
# Why python:3.11.9-slim for both stages (not distroless)?
# asyncpg and aiokafka both require ctypes and other stdlib modules absent from
# distroless Python images. slim is the smallest image that ships a complete
# CPython runtime. The size difference vs distroless is small (~40MB) and the
# operational benefit of a predictable stdlib is significant.

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

# PYTHONDONTWRITEBYTECODE=1: skip .pyc creation — reduces image size.
# PYTHONUNBUFFERED=1: flush stdout/stderr immediately — required for Docker
#   log capture (buffered output is lost on container kill).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Install C compiler needed for asyncpg's native extension.
# --no-install-recommends keeps the layer small; we only need gcc.
# Clean apt cache in the same layer to avoid bloating the image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first so Docker caches this layer.
# If requirements.txt is unchanged, pip install is skipped on rebuild —
# saving significant CI time for code-only changes.
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: final image ──────────────────────────────────────────────────────
FROM python:3.11.9-slim AS final

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # TZ=UTC: Standard 2 — container clock is always UTC.
    TZ=UTC \
    # PYTHONPATH: tell Python where to find packages installed by the builder.
    # /install/lib/python3.11/site-packages is the --prefix install layout.
    PYTHONPATH=/install/lib/python3.11/site-packages

WORKDIR /app

# Create a non-root user (Standard 11: never run as root in containers).
# uid/gid 1001: avoids collision with uid 1000 (often the host's first user),
# reducing the risk of accidental privilege escalation via shared volumes.
RUN addgroup --gid 1001 appgroup \
    && adduser --uid 1001 --gid 1001 --disabled-password --gecos "" appuser

# Copy installed packages from the builder stage.
# No pip in the final image — smaller attack surface, smaller image.
COPY --from=builder /install /install

# Copy service source code. chown at COPY time is more efficient than a
# separate RUN chown because it avoids creating an extra layer.
COPY --chown=appuser:appgroup . /app

# Switch to non-root user before CMD runs.
# All subsequent instructions (HEALTHCHECK, CMD) run as appuser.
USER appuser

# Port 8089: Prometheus metrics scrape endpoint.
# This service has no HTTP API — it is a Kafka consumer only.
EXPOSE 8089

# HEALTHCHECK: verify the Prometheus endpoint responds within 3 seconds.
# Docker marks the container unhealthy if this command exits non-zero,
# triggering automatic restart if restart: unless-stopped is configured.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8089/metrics', timeout=3)" || exit 1

# Run as a module so the working directory (/app) is in sys.path, allowing
# relative imports (config, router, postgres.repository, etc.) to resolve.
CMD ["python", "main.py"]
