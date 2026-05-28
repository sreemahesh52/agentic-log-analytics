# --- Custom typed exceptions ---
# Each exception has exactly one meaning. FastAPI exception handlers in main.py
# match on type, not on message strings, so the mapping is unambiguous.
# Typed exceptions also make caller intent explicit: raise AuthenticationError
# is self-documenting in a way that raise Exception("auth failed") is not.


class AuthenticationError(Exception):
    """X-API-Key header is missing, malformed, or not found in the database."""


class TenantNotFoundError(Exception):
    """Tenant lookup succeeded but returned no row.
    Mapped to 401 (same as AuthenticationError) — never reveal whether a key
 exists or not, to prevent enumeration attacks."""


class UpstreamServiceError(Exception):
    """A downstream service (log-ingestion, etc.) is unreachable, timed out,
 or returned a 5xx. Caller receives 503 Service Unavailable."""
