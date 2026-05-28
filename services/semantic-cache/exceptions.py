# --- Typed exceptions for the Semantic Cache service ---
# every error path raises a specific typed exception so callers
# can distinguish failure modes and handle them appropriately. A bare
# `except Exception` that swallows errors is never acceptable here.
# Why typed exceptions instead of returning None or False?
# Returning None silently hides failure causes. A typed exception forces the
# caller to explicitly decide: retry, DLQ, or fail-open. This makes error
# handling visible in code review and in production logs.


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


class CacheReadError(Exception):
    """Raised when a Redis read operation fails unexpectedly.
    The SemanticCache catches this internally and fails-open (returns a
    cache miss) so the incident pipeline is never blocked by Redis issues.
    """


class CacheWriteError(Exception):
    """Raised when a Redis write operation fails.
    Failing to write to the cache is non-critical — the RCA can still
    proceed; we just lose the ability to serve this result from cache
    in future. The caller logs at WARN and continues.
    """
