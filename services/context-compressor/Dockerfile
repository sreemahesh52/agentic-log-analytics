# ─────────────────────────────────────────────────────────────────────────────
# Context Compressor — multi-stage Dockerfile (Standard 11)
#
# Stage 1 (builder): installs Python dependencies into /venv.
#   tiktoken includes a Rust-compiled C extension. build-essential + rustc-dep
#   are required at build time only — they do NOT ship in the final image.
#
# Stage 2 (final):  copies only /venv + source; runs as non-root appuser.
#   The final image has no compilers, no pip, no build tools — smaller attack
#   surface and smaller image size.
#
# Why python:3.11.9-slim?
#   Pinned minor+patch version prevents silent base-image changes. "slim"
#   omits documentation and test files — sufficient for a runtime image.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# tiktoken requires Rust build tools at compile time (pre-built wheels usually
# available on PyPI for Linux x86_64 — this is a safety fallback for other arches).
# libpq-dev is needed by asyncpg's C extension.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create an isolated virtualenv so the final stage only needs to copy /venv.
RUN python -m venv /venv

# Copy requirements first: Docker layer-caches this step so pip install only
# re-runs when requirements.txt changes (not on source-code changes).
COPY requirements.txt .
RUN /venv/bin/pip install --no-cache-dir --upgrade pip \
 && /venv/bin/pip install --no-cache-dir -r requirements.txt


# ── Stage 2: final runtime ───────────────────────────────────────────────────
FROM python:3.11.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    # PATH: venv first so python resolves to the venv without activation.
    PATH="/venv/bin:$PATH"

# libpq5 is the runtime PostgreSQL client library linked by asyncpg's .so file.
# The dev headers (libpq-dev) are NOT needed in the final stage — only the lib.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Non-root user: running as root inside a container is a security risk.
# UID 10001 is above the reserved range — avoids collisions with system accounts.
RUN groupadd --gid 10001 appuser \
 && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

# Copy the populated venv from the builder — no pip in the final stage.
COPY --from=builder /venv /venv

# Copy application source into /app.
COPY . .

# chown after COPY so appuser owns all files from the start.
RUN chown -R appuser:appuser /app /venv

USER appuser

# EXPOSE documents the Prometheus metrics + healthcheck port.
EXPOSE 8087

# HEALTHCHECK: Python urllib hits the Prometheus metrics endpoint.
# curl is not present in slim — urllib is stdlib, always available.
# --start-period=30s: give the service time to connect to Kafka before checking.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
    CMD python -c \
    "import urllib.request; urllib.request.urlopen('http://localhost:8087/metrics', timeout=3)" \
    || exit 1

CMD ["python", "main.py"]
