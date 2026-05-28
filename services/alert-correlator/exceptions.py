# --- Typed exceptions for the alert-correlator service ---
# every error category has its own exception type so callers can
# distinguish a transient Kafka failure from a permanent DB schema error and
# route them to the correct handler (retry vs DLQ vs raise).


class DatabaseWriteError(Exception):
    """Raised when an INSERT or UPDATE to PostgreSQL fails after retries.
    Callers should log at ERROR and continue processing (Kafka is the
    source of truth; the incident is still published even if the DB write fails).
    """


class KafkaPublishError(Exception):
    """Raised when publishing a message to a Kafka topic fails after retries.
    Callers should attempt to write the original payload to the DLQ topic
    before giving up so the message can be replayed later.
    """


class SchemaValidationError(Exception):
    """Raised when an incoming Kafka message cannot be parsed or is missing
    required fields. The message is immediately dead-lettered — retrying a
    malformed message will never succeed.
    """
