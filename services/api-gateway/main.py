# --- FastAPI application entry point ---
# Execution order on startup:
#   1. _configure_logging runs immediately at module import
#   2. FastAPI app object is created with the lifespan context manager
#   3. Middleware is registered (CORS runs outermost — first in, last out)
#   4. Exception handlers are registered
#   5. Routers are included (routes become active)
#   6. Prometheus instrumentator hooks into the app
#   7. uvicorn calls lifespan — db_pool and http_client are created
#   8. App begins serving requests

import logging
import sys
import uuid
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import httpx
import structlog
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from redis.asyncio import Redis

from config import settings
from exceptions import AuthenticationError, TenantNotFoundError, UpstreamServiceError
from routers import alerts, cache, eval, health, investigations, knowledge_base, logs, security, simulate, stream


def _configure_logging() -> None:
    """Set up structlog with JSON output. Must run before any logger is used."""
    # getattr with fallback: if LOG_LEVEL is misspelled, default to INFO
    # rather than crashing — a startup warning is better than a hard failure.
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            # merge_contextvars attaches any context bound with structlog.contextvars
            # (e.g. trace_id set in middleware) to every log line automatically.
            structlog.contextvars.merge_contextvars,
            # add_log_level inserts "level": "info" into the JSON output.
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            # JSONRenderer serialises the event dict to a JSON string for stdout.
            structlog.processors.JSONRenderer(),
        ],
        # make_filtering_bound_logger sets the minimum log level. DEBUG lines
        # are compiled away at runtime when LOG_LEVEL=INFO, zero overhead.
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        # PrintLoggerFactory writes to stdout. In Docker, stdout is captured
        # by the container runtime and forwarded to the log aggregator.
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    # Configure the stdlib logging level so that uvicorn and asyncpg
    # access logs respect the same LOG_LEVEL setting.
    # format="%(message)s" passes the message through unchanged — structlog
    # has already formatted the JSON string before stdlib sees it.
    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")


# configure logging as the VERY FIRST action, before any import
# that might call logging.getLogger and capture a pre-configuration logger.
_configure_logging()
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create shared resources at startup; close them cleanly at shutdown.
    FastAPI calls this once when the process starts and once when it stops.
 Resources stored on app.state are accessible from any request via Depends."""
    # Log the DB host portion only — never log full URLs containing passwords.
    db_host = settings.postgres_url.split("@")[-1]
    logger.info("gateway_starting", db_host=db_host)

    # --- Create asyncpg connection pool ---
    app.state.db_pool = await asyncpg.create_pool(
        settings.postgres_url,
        min_size=2,
        max_size=10,
        server_settings={"timezone": "UTC"},
    )
    logger.info("db_pool_ready", min_size=2, max_size=10)

    # --- Create shared httpx client ---
    # A single AsyncClient maintains a connection pool to upstream services.
    # Timeout(10.0, connect=5.0): 5 s to establish TCP, 10 s total per request.
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=5.0),
    )
    logger.info("http_client_ready")

    # --- Create async Redis client ---
    # from_url is lazy — the connection is established on first command, not here.
    # decode_responses=True: all Redis values returned as strings (not bytes) so
    # the cache stats router can use int directly without .decode calls.
    # This client is shared across all requests via app.state (connection pooling).
    app.state.redis_client = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )
    logger.info("redis_client_ready", url=settings.redis_url.split("@")[-1])

    # --- Create aiokafka producer for investigations/trigger endpoint ---
    # AIOKafkaProducer publishes to incidents.ready when a manual RCA is triggered.
    # acks="all": wait for all in-sync replicas — ensures the message is durable
    # before the trigger endpoint returns 202 to the UI.
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
    )
    await app.state.kafka_producer.start()
    logger.info("kafka_producer_ready", bootstrap_servers=settings.kafka_bootstrap_servers)

    # yield hands control to FastAPI — the app serves requests from here.
    yield

    # --- Graceful shutdown: close resources in reverse order ---
    # Stop Kafka producer before closing DB/Redis — drain any pending sends first.
    await app.state.kafka_producer.stop()
    # aclose drains in-flight requests and closes TCP connections cleanly.
    # Without this, the OS closes sockets abruptly — upstream sees broken pipes.
    await app.state.http_client.aclose()
    # Redis aclose flushes pending commands and releases the connection pool.
    await app.state.redis_client.aclose()
    # pool.close waits for all borrowed connections to be returned, then
    # closes them. Without this, PostgreSQL holds zombie connections open
    # until its own timeout fires (default 10 min).
    await app.state.db_pool.close()
    logger.info("gateway_shutdown_complete")


app = FastAPI(
    title="Agentic Log Analytics — API Gateway",
    description="Multi-tenant API gateway. All endpoints require X-API-Key header.",
    version="1.0.0",
    lifespan=lifespan,
)

# --- CORS middleware ---
# allow_origins restricts cross-origin requests to the local UI dev server.
# In production this list would contain the deployed frontend domain.
# Middleware is applied outermost-first: CORS runs before any route handler.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _error_body(code: str, message: str, request_id: str) -> dict[str, Any]:
    """Build the standard error response envelope.
 Every error across every endpoint uses this exact shape — never deviate."""
    return {"error": {"code": code, "message": message, "request_id": request_id}}


# --- Exception handlers ---
# FastAPI matches the most specific handler first. Each handler:
#   1. Generates a fresh UUID so callers can correlate errors with server logs
#   2. Logs with context (never swallows the error silently)
#   3. Returns the standard error envelope — never a stack trace


@app.exception_handler(AuthenticationError)
async def handle_authentication_error(
    request: Request, exc: AuthenticationError
) -> JSONResponse:
    """Map AuthenticationError → 401 with AUTHENTICATION_ERROR code."""
    rid = str(uuid.uuid4())
    # Log path to help correlate which endpoint triggered the auth failure.
    logger.warning("auth_error", request_id=rid, path=request.url.path)
    return JSONResponse(
        status_code=401,
        content=_error_body("AUTHENTICATION_ERROR", "Invalid or missing API key", rid),
    )


@app.exception_handler(TenantNotFoundError)
async def handle_tenant_not_found(
    request: Request, exc: TenantNotFoundError
) -> JSONResponse:
    """Map TenantNotFoundError → 401 — same code as AuthenticationError intentionally.
    Returning a different code for 'key exists but revoked' vs 'key not found'
 leaks information that helps attackers enumerate valid keys."""
    rid = str(uuid.uuid4())
    logger.warning("tenant_not_found", request_id=rid)
    return JSONResponse(
        status_code=401,
        content=_error_body("AUTHENTICATION_ERROR", "Invalid or missing API key", rid),
    )


@app.exception_handler(UpstreamServiceError)
async def handle_upstream_error(
    request: Request, exc: UpstreamServiceError
) -> JSONResponse:
    """Map UpstreamServiceError → 503 Service Unavailable."""
    rid = str(uuid.uuid4())
    # Log the detail string — it contains the upstream service name and error type.
    logger.error("upstream_error", request_id=rid, detail=str(exc))
    return JSONResponse(
        status_code=503,
        content=_error_body("UPSTREAM_SERVICE_ERROR", "Upstream service unavailable", rid),
    )


@app.exception_handler(RequestValidationError)
async def handle_validation_error(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Map Pydantic RequestValidationError → 422 Unprocessable Entity."""
    rid = str(uuid.uuid4())
    return JSONResponse(
        status_code=422,
        content=_error_body("VALIDATION_ERROR", "Request body failed validation", rid),
    )


@app.exception_handler(Exception)
async def handle_generic_error(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all handler: map any unhandled exception → 500.
    Logs the exception type (never the message or stack trace) so developers
 can find the root cause in logs without exposing internals to callers."""
    rid = str(uuid.uuid4())
    # exc_type only — never log str(exc) here, it may contain sensitive data.
    logger.error("unhandled_exception", request_id=rid, exc_type=type(exc).__name__)
    return JSONResponse(
        status_code=500,
        content=_error_body("INTERNAL_ERROR", "An internal error occurred", rid),
    )


# --- Register routers ---
# Each router is defined in its own module (Single Responsibility).
# include_router mounts all routes from that module onto the app.
app.include_router(health.router)
app.include_router(logs.router)
# security router: GET /api/v1/security/events
app.include_router(security.router)
# alerts router: GET /api/v1/alerts, GET /api/v1/alerts/{alert_id}
app.include_router(alerts.router)
# simulate router: POST /api/v1/simulate/flood
app.include_router(simulate.router)
# cache router: GET /api/v1/cache/stats — reads Redis semantic cache counters.
app.include_router(cache.router)
# investigations router: GET/POST /api/v1/investigations — RCA results + trigger.
# Route ordering matters: failed endpoint registered before {rca_id} in router.
app.include_router(investigations.router)
# eval router: GET /api/v1/eval/summary — aggregate evaluation statistics.
app.include_router(eval.router)
# knowledge_base router: GET /api/v1/knowledge-base/stats — past_incidents counts.
app.include_router(knowledge_base.router)
# stream router: GET /api/v1/stream/{rca_id} — SSE live reasoning steps.
# Uses query-param auth (?api_key=...) because EventSource cannot send custom headers.
app.include_router(stream.router)

# --- Prometheus instrumentation ---
# instrument(app) wraps every route with request duration and count metrics.
# expose(app, endpoint="/metrics") adds the GET /metrics Prometheus scrape endpoint.
# This must run AFTER include_router so all routes are visible to the instrumentator.
Instrumentator().instrument(app).expose(app, endpoint="/metrics")
