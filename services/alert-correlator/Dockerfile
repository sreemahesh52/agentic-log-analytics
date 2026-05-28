# ─────────────────────────────────────────────────────────────────────────────
# Alert Correlator — multi-stage Dockerfile (Standard 11)
#
# Stage 1 (builder): installs Python dependencies into a venv.
# Stage 2 (final):   copies only the venv + source; runs as non-root appuser.
#
# Why multi-stage?
#   The builder stage installs build tools (gcc, etc.) needed to compile some
#   Python extensions. The final stage copies only the compiled artefacts, so
#   the shipped image contains no compilers — smaller attack surface, smaller size.
#
# Why python:3.11.9-slim?
#   Pinned minor+patch version prevents silent base-image changes breaking the
#   build. "slim" omits documentation and test files — enough for a runtime image.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM python:3.11.9-slim AS builder

WORKDIR /build

# Create a virtual environment so the final stage can simply copy /opt/venv.
# Using a venv (not --user install) means the path is predictable and does not
# depend on which user runs the container.
RUN python -m venv /opt/venv
# Activate venv for subsequent RUN commands by putting it first on PATH.
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements first: Docker layer-caches this step and only re-runs
# pip install when requirements.txt changes (not on source-code changes).
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


# ── Stage 2: final runtime ───────────────────────────────────────────────────
FROM python:3.11.9-slim

# Non-root user: running as root inside a container is a security risk —
# a container escape would give the attacker root on the host.
RUN useradd --no-create-home --shell /bin/false appuser

# Copy the populated venv from the builder — no pip install in the final stage.
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
# Copy application source code.
COPY . .

# PYTHONDONTWRITEBYTECODE: prevents .pyc files cluttering the container layer.
# PYTHONUNBUFFERED: ensures stdout/stderr are flushed immediately — critical
# for structured log lines to appear in `docker logs` without buffering delay.
# TZ=UTC: container clock is UTC; prevents any host OS timezone leaking in.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

# Prometheus metrics + healthcheck endpoint port.
EXPOSE 8086

# HEALTHCHECK: Docker marks the container healthy/unhealthy based on this command.
# curl is not present in slim; we use Python's built-in urllib instead.
# --interval: check every 10s after start_period passes.
# --start-period: give the service 30s to connect to Kafka before health checks fire.
HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=30s \
    CMD python -c \
    "import urllib.request; urllib.request.urlopen('http://localhost:8086/metrics', timeout=3)" \
    || exit 1

# Switch to non-root user before CMD.
USER appuser

CMD ["python", "main.py"]
