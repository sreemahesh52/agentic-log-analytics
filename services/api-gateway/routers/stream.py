# --- SSE streaming endpoint ---
# Consumes the rca.stream Kafka topic with aiokafka and pushes each reasoning
# step to the browser as a Server-Sent Event. This creates the live AI reasoning
# effect where users watch the agent think in real time.
# Why SSE instead of WebSocket?
#   SSE is one-directional (server → browser). The browser only needs to receive
#   steps — it never sends data back mid-investigation. SSE is simpler to
#   implement, automatically reconnects on disconnect, and uses plain HTTP/1.1.
#   WebSocket adds bidirectional complexity with no benefit here.
# Why query param auth instead of X-API-Key header?
#   The browser's native EventSource API accepts only a URL string — there is no
#   way to pass custom headers. Passing the key as ?api_key=... is the
#   industry-standard workaround for SSE authentication.
#   In production HTTPS prevents the key from being visible in transit.
# Why unique consumer group per connection?
#   Each SSE connection creates its own Kafka consumer group (sse-{rca_id}-{rand}).
#   Consumer groups track offsets independently: if browser tab A reads 5 steps,
#   tab B connecting later still starts from offset 0 (step 1). Without unique
#   groups, two tabs would share one offset counter and each would see only half
#   the steps.
# Why auto_offset_reset="earliest"?
#   A user may open the investigation page after several steps have already been
#   published (e.g. the investigation started 30 seconds ago). "earliest" ensures
#   all steps are delivered, not just future ones. "latest" would miss every step
#   published before the SSE connection was established.
# Why filter by payload rca_id, not message key?
#   The RCA Agent keys messages on tenant_id (for Kafka partition locality) — see
#   streaming.py in services/rca-agent/. The rca_id is inside the JSON payload.
#   Filtering by message.key would match all investigations for a tenant, not just
#   the one the browser is watching. fixing the root cause (wrong key),
#   not working around it with an incorrect filter.

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from typing import Any, AsyncGenerator

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

# _tenant_cache is the module-level singleton from auth.py.
# Importing it (not creating a new instance) means both verify_api_key and
# this endpoint share the same in-memory cache — one DB lookup populates both.
from auth import _tenant_cache
from config import settings

logger = structlog.get_logger()

router = APIRouter()

# How long to wait for the next Kafka message before declaring the stream dead.
# 60 s matches the maximum expected investigation duration plus a safety buffer.
STREAM_TIMEOUT_SECONDS: int = 60

# Safety guard: stop scanning after this many total messages regardless of matches.
# Prevents a runaway consumer on topics with many historical messages from other
# investigations — rca_id filtering may not match for a long time on a busy broker.
MAX_MESSAGES_SCANNED: int = 1000

# Kafka stream topic — must match the RCA Agent's STREAM_TOPIC constant.
_STREAM_TOPIC: str = "rca.stream"


# ---------------------------------------------------------------------------
# Helper functions — extracted to keep each function ≤40 lines
# ---------------------------------------------------------------------------


def _build_consumer(group_id: str, bootstrap_servers: str) -> AIOKafkaConsumer:
    """Create a configured AIOKafkaConsumer for the rca.stream topic.
    Extracted from the generator so the constructor args are readable.
 aiokafka is async-native — consumer.getone does not block the event loop."""
    # auto_offset_reset="earliest": new consumer group starts from the beginning
    # of the topic, delivering all historical steps for this investigation.
    return AIOKafkaConsumer(
        _STREAM_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        # enable_auto_commit=True: Kafka tracks this group's offset progress.
        # Committing offsets does not affect other groups — each is independent.
        enable_auto_commit=True,
    )


def _try_parse_message(raw_value: bytes) -> dict[str, Any] | None:
    """Decode and JSON-parse a Kafka message value. Returns None on any error.
    Returning None (not raising) lets the caller continue the loop cleanly
 rather than crashing the entire SSE stream on one malformed message."""
    try:
        # .decode("utf-8") converts bytes → str; json.loads parses to dict.
        return json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


async def _verify_api_key_for_sse(
    api_key: str,
    db_pool: asyncpg.Pool,
) -> dict[str, Any] | None:
    """Verify the query-param API key and return the tenant dict, or None if invalid.
    Cannot use verify_api_key (FastAPI Depends) because StreamingResponse must
    be returned synchronously from the route function — Depends runs after the
    route returns, which is too late to change the HTTP status code.
    Replicates the same cache-first, DB-fallback pattern as verify_api_key
 in auth.py to ensure consistent behaviour between header and query-param auth."""
    # sha256.hexdigest produces a 64-char lowercase hex string.
    # Only the hash is compared — the raw key never touches the database.
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    # --- Cache-first lookup ---
    # _tenant_cache is the same singleton used by verify_api_key. A tenant
    # verified via the header auth path is already cached here — no second DB hit.
    cached = _tenant_cache.get(key_hash)
    if cached is not None:
        logger.debug("sse_auth_cache_hit", key_prefix=key_hash[:8])
        return cached

    # --- Database fallback ---
    # Parameterised query — never f-strings or concatenation in SQL.
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT tenant_id::text, name, model_tier FROM tenants WHERE api_key_hash = $1",
            key_hash,
        )

    if row is None:
        logger.warning("sse_auth_failed", key_prefix=key_hash[:8])
        return None

    tenant = dict(row)
    # Populate the shared cache so the next authenticated request skips the DB.
    _tenant_cache.set(key_hash, tenant)
    logger.info("sse_auth_verified", tenant_id=tenant["tenant_id"])
    return tenant


# ---------------------------------------------------------------------------
# SSE route
# ---------------------------------------------------------------------------


# response_model=None: FastAPI cannot generate a Pydantic response schema for
# StreamingResponse or JSONResponse — they are Starlette Response subclasses,
# not Pydantic models. Without response_model=None, FastAPI raises FastAPIError
# at startup when it tries to introspect the union return annotation.
@router.get("/api/v1/stream/{rca_id}", response_model=None)
async def stream_investigation_steps(
    rca_id: str,
    # Query param: EventSource API cannot set custom headers.
    # ?api_key=... is the standard SSE authentication pattern.
    api_key: str = Query(..., description="API key — EventSource cannot send custom headers"),
    request: Request = None,
) -> StreamingResponse | JSONResponse:
    """Stream live reasoning steps for a running RCA investigation via SSE.
    Each reasoning step published to rca.stream by the RCA Agent is forwarded
    to the browser as a 'data: {json}\\n\\n' SSE event. The connection closes
    automatically on receiving a {type:'complete'} event or after a 60-second
    silence.
    Returns StreamingResponse (text/event-stream) on success.
    Returns JSONResponse 401 if api_key is invalid.
    """
    # --- Auth before creating StreamingResponse ---
    # The HTTP response headers (status 200, Content-Type: text/event-stream) are
    # sent when StreamingResponse is returned, before the generator yields anything.
    # Once headers are sent, the status code cannot be changed. Auth must run here,
    # returning JSONResponse(401) if invalid, so the status code can still be set.
    tenant = await _verify_api_key_for_sse(
        api_key=api_key,
        db_pool=request.app.state.db_pool,
    )
    if tenant is None:
        # SSE clients may display this
        # differently from XHR clients — the EventSource onerror handler fires.
        return JSONResponse(
            status_code=401,
            content={"error": {
                "code": "AUTHENTICATION_ERROR",
                "message": "Invalid or missing API key",
            }},
        )

    tenant_id = tenant["tenant_id"]

    # --- Unique consumer group per connection ---
    # The 8-char hex suffix differentiates concurrent connections to the same rca_id
    # (e.g. two browser tabs) and reconnects from the same tab after a disconnect.
    connection_id = uuid.uuid4().hex[:8]
    group_id = f"sse-{rca_id}-{connection_id}"

    async def event_generator() -> AsyncGenerator[str, None]:
        """Async generator that reads from rca.stream and yields SSE events.
        Lifecycle is bounded by try/finally so the aiokafka consumer is always
        stopped — even on client disconnect (GeneratorExit thrown by Python when
        the response is closed) or on uncaught exception.
        """
        consumer = _build_consumer(group_id, settings.kafka_bootstrap_servers)
        # Initialise before try so it is always in scope for the finally logger.
        messages_scanned = 0

        try:
            # consumer.start joins the consumer group and seeks to earliest offset.
            # Raises KafkaConnectionError if the broker is unreachable.
            await consumer.start()
            logger.info(
                "sse_consumer_started",
                rca_id=rca_id,
                tenant_id=tenant_id,
                group_id=group_id,
            )

            while messages_scanned < MAX_MESSAGES_SCANNED:
                try:
                    # wait_for wraps getone with a per-iteration deadline.
                    # asyncio.TimeoutError fires if no message arrives within 60 s.
                    message = await asyncio.wait_for(
                        consumer.getone(),
                        timeout=float(STREAM_TIMEOUT_SECONDS),
                    )
                except asyncio.TimeoutError:
                    logger.warning("sse_stream_timeout", rca_id=rca_id)
                    yield f"data: {json.dumps({'type': 'timeout', 'rca_id': rca_id})}\n\n"
                    break

                messages_scanned += 1
                data = _try_parse_message(message.value)
                if data is None:
                    logger.warning("sse_malformed_message", rca_id=rca_id)
                    continue

                # Filter by rca_id in the payload — NOT by message.key.
                # The RCA Agent uses tenant_id as the Kafka key (partition locality);
                # rca_id is in the JSON body. Filtering by key would match all
                # investigations for the tenant, flooding this connection with
                # unrelated steps. Root cause fix per
                if data.get("rca_id") != rca_id:
                    continue

                # SSE wire format: "data: <json>\n\n"
                # The double newline ends the event. EventSource parses this automatically.
                yield f"data: {json.dumps(data)}\n\n"
                logger.debug(
                    "sse_step_yielded",
                    rca_id=rca_id,
                    step_type=data.get("type"),
                    step_number=data.get("step_number"),
                )

                # type='complete' is the sentinel published by publish_complete.
                # Close the stream after forwarding it — the browser switches to
                # static display mode on receiving this event type.
                if data.get("type") == "complete":
                    logger.info("sse_stream_complete", rca_id=rca_id)
                    break

        except Exception as exc:
            # Covers: consumer.start failure, unexpected loop errors.
            # Yield a structured error event so the browser can fall back gracefully
            # rather than hanging until a network timeout fires.
            logger.error("sse_consumer_error", rca_id=rca_id, error=str(exc))
            yield f"data: {json.dumps({'type': 'error', 'rca_id': rca_id})}\n\n"

        finally:
            # Always stop the consumer. This runs on: normal completion, timeout,
            # client disconnect (GeneratorExit), or exception. Without stop,
            # the aiokafka heartbeat background task keeps running — leaking a
            # Kafka group membership slot until the session timeout (default 10 s).
            await consumer.stop()
            logger.info(
                "sse_consumer_stopped",
                rca_id=rca_id,
                group_id=group_id,
                messages_scanned=messages_scanned,
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            # no-cache: browser must not cache SSE responses — each is live data.
            "Cache-Control": "no-cache",
            # X-Accel-Buffering: no — instructs nginx NOT to buffer this response.
            # By default nginx accumulates data and flushes in chunks, which breaks
            # real-time streaming. Steps would arrive all at once instead of one by one.
            # This header disables buffering at the nginx layer.
            "X-Accel-Buffering": "no",
            # keep-alive: prevents load balancers from closing idle SSE connections
            # before the investigation completes (some proxies timeout at 60 s).
            "Connection": "keep-alive",
        },
    )
