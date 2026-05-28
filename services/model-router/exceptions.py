# --- Typed exceptions for the Model Router service ---
# every error path raises a specific typed exception so callers
# can distinguish failure modes and respond appropriately.
# A bare `except Exception: pass` that swallows errors is never acceptable.
# Why typed exceptions instead of returning None or False?
# Returning None silently hides failure causes. A typed exception forces the
# caller to explicitly decide: retry, DLQ, or skip. This makes all error
# handling paths visible in code review and in production logs.


class KafkaPublishError(Exception):
    """Raised when publishing a message to Kafka fails after all retries.
    The caller (KafkaHandler) must send the original message to the DLQ
    when this is raised — the message would otherwise be silently lost.
    """


class SchemaValidationError(Exception):
    """Raised when an incoming Kafka message fails structural validation.
    Schema errors are not retryable — a malformed payload cannot be fixed
    by retrying. The caller sends directly to the DLQ without retry loops.
    """


class DatabaseQueryError(Exception):
    """Raised when a PostgreSQL query fails unexpectedly.
    The caller (KafkaHandler) decides whether to DLQ the incident or pass
    it through without routing. Wraps the original asyncpg exception with
    enough context to locate the failing query in logs.
    """


class TenantNotFoundError(Exception):
    """Raised when the tenant_id from the incident does not exist in the tenants table.
    Indicates a data inconsistency: an incident arrived for a tenant that
    has been deleted or never existed. The incident is sent to the DLQ.
    """
