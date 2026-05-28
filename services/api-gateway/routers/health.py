# --- Health check router ---
# No authentication required. Called by Docker's HEALTHCHECK and load balancers
# to decide whether the container is ready to serve traffic.
# Returns UTC ISO 8601 timestamp so callers can detect clock skew.

from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

# APIRouter groups related endpoints. main.py calls app.include_router
# to register all routes from this module onto the FastAPI app.
router = APIRouter()

# Named constant — no magic strings scattered in code.
SERVICE_VERSION = "1.0.0"


@router.get("/health", include_in_schema=True)
async def health_check() -> JSONResponse:
    """Return service status, version, and current UTC timestamp."""
    # --- Build UTC timestamp in ISO 8601 Z format ---
    # .isoformat(timespec="milliseconds") produces "2024-01-15T10:23:45.123+00:00".
    # .replace("+00:00", "Z") converts to the compact Z suffix form required
    # by API responses must end with Z, not +00:00.
    timestamp = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    return JSONResponse(
        status_code=200,
        content={"status": "ok", "version": SERVICE_VERSION, "timestamp": timestamp},
    )
