# --- Typed exceptions for context-compressor ---
# every error is a typed exception, not a bare Exception.
# This enables callers to catch specific failure modes without catching
# broad Exception and accidentally swallowing unrelated errors.
# These exceptions follow the same naming pattern used across all services
# so operators reading logs immediately know which subsystem failed.


class KafkaPublishError(Exception):
    """Raised when a Kafka publish fails after all retries are exhausted.
    The caller should write the original message to the DLQ with this
    exception's message as the failure_reason field.
    """


class DatabaseWriteError(Exception):
    """Raised when a PostgreSQL write (insert or update) fails permanently.
    Used by repositories to signal that a DB operation could not complete
    after the asyncpg pool returned an error. The caller decides whether to
    continue (best-effort) or escalate.
    """


class SchemaValidationError(Exception):
    """Raised when an incoming Kafka message fails JSON or field validation.
    A schema-invalid message can never be fixed by retrying — it should be
    sent to the DLQ immediately with failure_reason='schema_validation_error'.
    """
