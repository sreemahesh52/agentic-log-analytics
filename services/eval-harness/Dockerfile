# --- Multi-stage Dockerfile for the Eval Harness service ---
# Standard 11: two stages keep the final image minimal — no build tools in production.
# The builder installs native extensions (asyncpg, chromadb); the final stage
# copies only the compiled packages and source.
#
# Why python:3.11.9-slim for both stages?
# asyncpg requires C stdlib present in slim. chromadb's chroma-hnswlib (HNSW vector
# index) requires g++. Distroless Python images lack both — slim is the smallest
# image with a complete CPython runtime and build toolchain available.

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

# PYTHONDONTWRITEBYTECODE=1: skip .pyc files — reduces image size.
# PYTHONUNBUFFERED=1: flush stdout/stderr immediately — required for Docker
#   log capture (buffered output is silently dropped on container kill).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Install build tools required by native extensions:
#   build-essential: gcc + g++ + make — g++ is required by chroma-hnswlib (C++)
#   libpq-dev:       PostgreSQL client headers for asyncpg C extension
#   curl:            Used in healthcheck (copied to final stage)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment in /venv.
# Using a venv (not --prefix) makes COPY /venv self-contained.
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Copy requirements first to leverage Docker layer caching.
# pip install only re-runs when requirements.txt changes, not on every source change.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Stage 2: final ────────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS final

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/venv/bin:$PATH" \
    # Standard 2: TZ=UTC ensures datetime.now() == datetime.utcnow() in this container.
    # The asyncpg pool also sets server_settings={"timezone":"UTC"} per-connection.
    TZ=UTC

# Install curl for the Prometheus metrics healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled venv from the builder stage — no build tools needed at runtime.
COPY --from=builder /venv /venv

# Create a non-root user. Running as root in a container violates the principle of
# least privilege — a process breakout would have root on the host.
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --no-create-home appuser

WORKDIR /app

# Copy service source. chown at COPY time avoids a separate RUN chown layer
# (each RUN creates a new layer, increasing image size).
COPY --chown=appuser:appgroup . /app

USER appuser

# The eval harness starts the Kafka consumer loop via main.py.
# No HTTP port is exposed — metrics are served on port 8091 (Prometheus scrape only).
CMD ["python", "main.py"]
