# --- Simulate router ---
# Provides the flood endpoint for deterministic anomaly detection testing.
# Why simulate/flood is a first-class API endpoint (not a test script):
#   A CLI script only works locally. An API endpoint:
#     1. Works from the UI — no terminal access needed.
#     2. Respects tenant isolation — only floods logs for the caller's tenant.
#     3. Goes through the full production pipeline (security middleware, log consumer,
#        anomaly detection) — tests the real system, not a stub.
#     4. Is accessible from CI/CD pipelines for automated smoke tests.
# Why 100 logs in batches of 10:
#   The statistical detector needs enough ERROR events in one time bucket to exceed
#   the Z-score threshold (default 3.0 std deviations above the mean).
#   10 logs per batch × 10 batches = 100 errors in ~0.5 seconds — far above any
#   realistic baseline, guaranteeing detection.
# Why 10 varied messages (not the same message repeated):
#   "test error 1", "test error 2" look artificial and the LLM verifier may
#   return NO ("this looks like a test, not a real anomaly"). Realistic error
#   messages increase the probability of the verifier returning YES.

import asyncio
from typing import Any

import httpx
import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from auth import verify_api_key
from config import settings
from dependencies import get_http_client
from exceptions import UpstreamServiceError

logger = structlog.get_logger()
router = APIRouter()

# Ten realistic error messages that cycle across the 100 flood logs.
# These are deliberately diverse to:
#   1. Exercise statistical detection (volume spike from 100 errors).
#   2. Exercise semantic detection (novel error patterns for this service).
#   3. Pass LLM verification (look like real production errors, not test strings).
_FLOOD_ERROR_MESSAGES = [
    "Database connection pool exhausted: max_connections=100 reached, waiting clients=47",
    "Request timeout after 30000ms: upstream service did not respond within deadline",
    "Memory allocation failed: OOM killer sent SIGKILL to process pid=12847",
    "TLS certificate verification failed: certificate expired on 2024-01-01T00:00:00Z",
    "Redis ECONNREFUSED: failed to connect to 127.0.0.1:6379 after 3 retries",
    "HTTP 503 Service Unavailable from downstream payment gateway: circuit breaker open",
    "Deadlock detected in transaction t-8842: rolled back after 3 retry attempts",
    "Kafka consumer lag exceeded threshold: group=order-processor lag=52847 messages",
    "File descriptor limit exhausted: ulimit -n 1024 reached, cannot accept connections",
    "JWT signature verification failed: signing key rotation in progress, retry in 5s",
]

# Number of log batches to send.
_BATCH_COUNT = 10
# Number of log messages per batch.
_LOGS_PER_BATCH = 10
# Total = _BATCH_COUNT * _LOGS_PER_BATCH = 100 logs
# Delay between batches in seconds: 50ms per spec.
_INTER_BATCH_SLEEP_SECONDS = 0.05


class FloodRequest(BaseModel):
    """Request body for POST /api/v1/simulate/flood.
    service: the service name whose logs will be flooded with errors.
    """
    service: str


@router.post("/api/v1/simulate/flood", status_code=200)
async def flood_errors(
    body: FloodRequest,
    tenant: dict[str, Any] = Depends(verify_api_key),
    http_client: httpx.AsyncClient = Depends(get_http_client),
) -> JSONResponse:
    """Send 100 realistic ERROR logs to the log-ingestion service.
    Sends in 10 batches of 10 with 50ms sleep between batches.
    Each batch cycles through 10 varied realistic error messages.
    Injects tenant_id into metadata so the anomaly-agent scopes detections correctly.
    Returns immediately after all 100 logs are sent — does not wait for
    anomaly detection (which happens asynchronously downstream in the pipeline).
    """
    log = logger.bind(tenant_id=tenant["tenant_id"], service=body.service)

    # Log-ingestion endpoint — same URL used by logs.py for the ingest proxy.
    target_url = f"{settings.log_ingestion_url}/api/v1/logs"

    total_sent = 0

    for batch_idx in range(_BATCH_COUNT):
        for msg_idx in range(_LOGS_PER_BATCH):
            # Cycle through the 10 varied messages — each log gets a different message.
            log_idx = batch_idx * _LOGS_PER_BATCH + msg_idx
            message = _FLOOD_ERROR_MESSAGES[log_idx % len(_FLOOD_ERROR_MESSAGES)]

            # Build the log payload. tenant_id injected into metadata so the
            # Go log-ingestion service passes it through to logs.raw unchanged.
            # The security middleware and log-consumer preserve metadata fields.
            payload = {
                "service": body.service,
                "level": "ERROR",
                "message": message,
                "metadata": {
                    # tenant_id in metadata: the Go log-ingestion service includes this
                    # in the Kafka message; the log-consumer extracts it for DB insert.
                    "tenant_id": tenant["tenant_id"],
                    "flood_simulation": True,
                },
            }

            try:
                response = await http_client.post(target_url, json=payload)
                if response.status_code == 202:
                    total_sent += 1
                elif response.status_code >= 500:
                    log.warning(
                        "flood_upstream_error",
                        status=response.status_code,
                        log_index=log_idx,
                    )
                    raise UpstreamServiceError("Log ingestion service returned server error")
            except httpx.TimeoutException as exc:
                log.error("flood_upstream_timeout", error=str(exc))
                raise UpstreamServiceError("Log ingestion service timed out during flood")
            except httpx.RequestError as exc:
                log.error("flood_upstream_unreachable", error=str(exc))
                raise UpstreamServiceError("Log ingestion service unreachable during flood")

        # Sleep between batches to avoid overwhelming the log-ingestion service.
        # asyncio.sleep is non-blocking — other requests can be handled during
        # this wait. time.sleep would block the entire FastAPI event loop.
        # Only sleep between batches, not after the final batch.
        if batch_idx < _BATCH_COUNT - 1:
            await asyncio.sleep(_INTER_BATCH_SLEEP_SECONDS)

    log.info("flood_complete", logs_sent=total_sent, service=body.service)
    return JSONResponse(
        content={
            "status": "flooding",
            "logs_sent": total_sent,
            "service": body.service,
        }
    )
