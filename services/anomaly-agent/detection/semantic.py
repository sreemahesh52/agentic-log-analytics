"""Semantic anomaly detector — Strategy implementation using ChromaDB and OpenAI embeddings.
Detects 'new_error_pattern' anomalies: error messages that are semantically
dissimilar to anything previously seen for that service and tenant.
Why semantic detection complements statistical detection:
  Statistical: catches RATE spikes — many errors of a KNOWN type occurring at once.
  Semantic: catches NOVEL patterns — even a SINGLE new error type at normal rate.
A database running out of connections fires thousands of identical errors (statistical).
A brand-new TLS certificate failure fires once with a unique message (semantic).
Both matter. Neither detector catches what the other does alone.
Design decisions:
  One ChromaDB collection per TENANT (not per service):
    Avoids O(tenants × services) collection proliferation. Service is stored as
    metadata and used as a where-filter at query time. ChromaDB handles per-service
    isolation within a single collection via metadata filtering.
  Cosine distance (not euclidean):
    Text embeddings encode DIRECTION (meaning), not magnitude. Two messages with
    the same meaning but different lengths will have different magnitude but similar
    direction. Cosine similarity is magnitude-invariant — the right metric for text.
  Fail-open on both OpenAI and ChromaDB errors:
    Missing one anomaly during an API outage is preferable to stopping all log
    processing. The anomaly detection pipeline must never be the reason messages
    are dropped or consumers crash.
"""

import structlog

from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

# openai exceptions are caught specifically so we can log at different levels:
#   RateLimitError → WARN (transient, expected under high load)
#   APIError → ERROR (unexpected, may indicate quota exhaustion or service issues)
from openai import APIError, RateLimitError

from detection.base import AnomalyDetectionResult, BaseAnomalyDetector

# Module-level logger — configured in main.py before this module loads.
# structlog ensures JSON output with consistent field names across all services.
logger = structlog.get_logger(__name__)

# --- Severity thresholds for cosine distance ---
# Higher cosine DISTANCE = more different from known patterns = higher severity.
# Distance of 1.0 means fully orthogonal (completely unrelated).
# Distance of 0.0 means identical direction (same meaning).
# Boundaries calibrated: >0.7=very novel=CRITICAL, >0.5=novel=HIGH, >0.3=somewhat new=MEDIUM
_DISTANCE_CRITICAL_BOUNDARY = 0.7
_DISTANCE_HIGH_BOUNDARY = 0.5
_DISTANCE_MEDIUM_BOUNDARY = 0.3

# Valid severity strings — must match the DB CHECK constraint on the alerts table
_SEVERITY_LOW = "LOW"
_SEVERITY_MEDIUM = "MEDIUM"
_SEVERITY_HIGH = "HIGH"
_SEVERITY_CRITICAL = "CRITICAL"

# Only embed ERROR and FATAL messages.
# INFO/WARN/DEBUG are routine operational messages — embedding them would:
#   1. Waste OpenAI API calls on non-anomalous content
#   2. Pollute the ChromaDB collection with irrelevant patterns
#   3. Risk false-positive suppression (a routine INFO msg "cached" as "known")
_ANOMALY_LEVELS = frozenset({"ERROR", "FATAL"})

# OpenAI embedding model — 1536-dimensional vectors.
# text-embedding-3-small: strong quality-to-cost ratio.
# Changing this constant is a breaking change: existing ChromaDB vectors would
# be incomparable to new ones (different embedding space dimensions).
_EMBEDDING_MODEL = "text-embedding-3-small"

# Maximum length of message_preview stored in ChromaDB metadata.
# ChromaDB metadata values have practical size limits; truncating prevents issues.
_MESSAGE_PREVIEW_MAX_LEN = 200


def _distance_to_severity(distance: float) -> str:
    """Map cosine distance to a severity level string.
    Higher distance = message is more novel = higher urgency to investigate.
    Boundaries are checked from highest to lowest so the most severe case is
    returned first — same pattern as _map_zscore_to_severity in statistical.py.
    """
    if distance > _DISTANCE_CRITICAL_BOUNDARY:
        return _SEVERITY_CRITICAL
    if distance > _DISTANCE_HIGH_BOUNDARY:
        return _SEVERITY_HIGH
    if distance > _DISTANCE_MEDIUM_BOUNDARY:
        return _SEVERITY_MEDIUM
    # Distance is above (1 - similarity_threshold) but below MEDIUM boundary.
    # The caller guarantees this function is only called when an anomaly was detected,
    # so distance is always above the threshold at this point.
    return _SEVERITY_LOW


class SemanticDetector(BaseAnomalyDetector):
    """Strategy: embedding-based anomaly detection using ChromaDB vector storage.
    Implements BaseAnomalyDetector — the orchestrator depends on that interface,
    not on this concrete class. This is the Open/Closed principle: a third detector
    type could be added without changing the orchestrator or this class.
    Dependency Inversion: chroma_client and openai_client are injected via __init__,
    never instantiated here. Tests inject chromadb.EphemeralClient + unittest.mock.Mock;
    production injects real clients from the service factory.
    """

    def __init__(
        self,
        chroma_client: object,
        openai_client: object,
        similarity_threshold: float,
        max_collection_size: int = 10000,
        eviction_batch_size: int = 1000,
    ) -> None:
        """Initialise with all dependencies injected — no connections created here.
        Args:
            chroma_client: ChromaDB client. Injected so tests use EphemeralClient.
            openai_client: OpenAI client. Injected so tests use Mock.
            similarity_threshold: Cosine similarity below which a message is anomalous.
                                  0.7 per project spec. Lower value = more sensitive.
                                  Tuning: lower threshold = more alerts (less filtering),
                                  higher threshold = fewer alerts (more filtering).
            max_collection_size: Maximum number of embeddings stored per tenant collection.
                                 At this limit, oldest embeddings are evicted in batches.
                                 Default 10000 per spec — prevents unbounded ChromaDB growth.
            eviction_batch_size: How many oldest embeddings to delete per eviction cycle.
                                 Default 1000 — balances cleanup overhead vs memory.
        """
        # All clients stored as instance variables — never created here (Dependency Inversion)
        self._chroma = chroma_client
        self._openai = openai_client
        self._similarity_threshold = similarity_threshold
        self._max_collection_size = max_collection_size
        self._eviction_batch_size = eviction_batch_size

    def detector_type(self) -> str:
        """Return the stable lowercase identifier for this strategy.
        Used in AnomalyDetectionResult.anomaly_type and Prometheus metric labels.
        Changing this string is a breaking change for downstream consumers.
        """
        return "semantic"

    def _collection_name(self, tenant_id: str) -> str:
        """Build the ChromaDB collection name for a tenant.
        Pattern: anomaly_{tenant_id}
        One collection per TENANT (not per service) — service is stored as metadata
        and filtered at query time. This avoids creating O(tenants × services)
        collections, which would multiply ChromaDB memory and metadata overhead.
        The 'anomaly_' prefix prevents name collision with 'past_incidents_{tenant_id}'
        collections used by the RCA agent's hybrid RAG search.
        """
        return f"anomaly_{tenant_id}"

    def _get_or_create_collection(self, tenant_id: str) -> object:
        """Get or create the ChromaDB collection for this tenant.
        get_or_create_collection is idempotent — safe to call on every invocation.
        If the collection already exists, it is returned without modification.
        metadata={'hnsw:space': 'cosine'} sets cosine distance as the index metric.
        This is critical: our "similarity = 1 - distance" math only holds for cosine
        distance (range [0,1]). L2 (default) distances have a different range and
        "1 - distance" would not produce meaningful similarity values.
        """
        return self._chroma.get_or_create_collection(
            name=self._collection_name(tenant_id),
            # hnsw:space=cosine — cosine distance: 0=identical, 1=orthogonal
            # Without this, ChromaDB defaults to L2 (euclidean) distance,
            # which would make "similarity = 1 - distance" mathematically wrong.
            metadata={"hnsw:space": "cosine"},
        )

    def _embed(self, message: str) -> list[float]:
        """Call OpenAI to produce a 1536-dimensional embedding vector for the message.
        The embedding captures semantic meaning: messages with similar meaning will
        produce vectors with high cosine similarity, even if worded differently.
        Example: "DB connection refused" and "database pool exhausted" are semantically
        related — they will have higher similarity than "TLS cert expired".
        Raises:
            openai.RateLimitError: API rate limit exceeded.
            openai.APIError: Other API failure (auth, quota, server error).
        The caller catches these for fail-open behaviour.
        """
        response = self._openai.embeddings.create(
            model=_EMBEDDING_MODEL,
            input=message,
        )
        # response.data is a list of EmbeddingObject; [0] is the first (and only) result
        # .embedding is the list[float] of 1536 dimensions
        return response.data[0].embedding

    def _add_embedding(
        self,
        collection: object,
        embedding: list[float],
        tenant_id: str,
        service: str,
        message: str,
        timestamp: datetime,
    ) -> None:
        """Add a new embedding to the ChromaDB collection with structured metadata.
        Metadata fields stored per embedding:
          service: used by where-filter in queries to scope to one service.
          tenant_id: audit trail (collection is already scoped per tenant, but
                           storing it in metadata aids debugging and future migrations).
          timestamp: Unix float — numerically sortable for oldest-first eviction.
          message_preview: first 200 chars for debugging without full message content.
        UUID is generated fresh per add — ChromaDB requires unique string IDs.
        """
        collection.add(
            # str(uuid4) generates a unique ID — ChromaDB requires unique string IDs per item
            ids=[str(uuid4())],
            embeddings=[embedding],
            metadatas=[{
                "service": service,
                "tenant_id": tenant_id,
                # .timestamp returns Unix seconds as float — sortable numerically
                # for oldest-first eviction. ISO string would require parsing, float is faster.
                "timestamp": timestamp.timestamp(),
                # Truncate message to avoid ChromaDB metadata size limitations
                "message_preview": message[:_MESSAGE_PREVIEW_MAX_LEN],
            }],
        )

    def _evict_oldest(self, collection: object) -> None:
        """Delete the oldest eviction_batch_size embeddings from the collection.
        Why oldest-first (not LRU — Least Recently Used):
          LRU requires tracking access times on every query — extra metadata write
          per similarity search, compounding latency. Oldest-first is simpler and
          nearly as effective: historical errors from months ago have already shaped
          the baseline; recent errors matter more for matching current patterns.
        Eviction is triggered only when count > max_collection_size, so it runs
        infrequently. The small overhead of a full get is acceptable at this cadence.
        """
        # get returns all items with ids and metadatas — needed to sort by timestamp
        all_items = collection.get()

        if not all_items["ids"]:
            # Nothing to evict — defensive guard, should not happen in practice
            return

        # --- Sort (id, metadata) pairs by timestamp ascending (oldest first) ---
        # zip pairs each id with its corresponding metadata dict.
        # key=lambda reads the 'timestamp' float from metadata for sorting.
        # Default 0.0 handles any item missing the timestamp field (should not occur).
        items_by_age = sorted(
            zip(all_items["ids"], all_items["metadatas"]),
            key=lambda pair: pair[1].get("timestamp", 0.0),
        )

        # Take the oldest eviction_batch_size items to delete
        ids_to_delete = [item_id for item_id, _ in items_by_age[: self._eviction_batch_size]]

        if ids_to_delete:
            collection.delete(ids=ids_to_delete)
            logger.info(
                "semantic_detector_evicted_embeddings",
                collection=collection.name,
                evicted_count=len(ids_to_delete),
                remaining_count=collection.count(),
            )

    def update_and_check(
        self,
        tenant_id: str,
        service: str,
        level: str,
        timestamp: datetime,
        message: str = "",
    ) -> Optional[AnomalyDetectionResult]:
        """Embed the log message and check if it is semantically novel for this service.
        The message parameter extends the base interface (which has 4 positional args).
        Default="" maintains Liskov Substitution Principle — any caller using the
        BaseAnomalyDetector interface can still call with 4 positional args without
        knowing about the message parameter.
        Logic (following project spec):
          1. Skip immediately if level not in {ERROR, FATAL} — no embedding, no ChromaDB.
          2. Get or create tenant-scoped ChromaDB collection.
          3. Embed message via OpenAI text-embedding-3-small.
          4. count==0: no baseline yet — add embedding, return None (cold start).
          5. Query nearest neighbour filtered to this service.
          6. No results for this service: add embedding, return None (service cold start).
          7. distance = results['distances'][0][0]; similarity = 1.0 - distance.
          8. similarity >= threshold: known pattern, return None.
          9. similarity < threshold: novel pattern detected.
             Add embedding, evict if collection exceeds max size, return AnomalyDetectionResult.
        Fail-open error handling:
          OpenAI RateLimitError → log WARN, return None
          OpenAI APIError → log ERROR, return None
          Any ChromaDB/other → log ERROR, return None
        The pipeline never stops because of an external API error.
        Args:
            tenant_id: tenant namespace — all ChromaDB collections keyed by this.
            service: service name — used as metadata filter in queries.
            level: log level; only ERROR and FATAL are processed.
            timestamp: event time (must be timezone-aware UTC).
            message: the log message text to embed and compare.
        """
        # Step 1: Only ERROR and FATAL contain novel failure signals worth embedding.
        # INFO, WARN, DEBUG are routine — skipping them prevents wasted API calls
        # and keeps the ChromaDB collection focused on meaningful anomaly patterns.
        if level not in _ANOMALY_LEVELS:
            return None

        try:
            # Step 2: Get or create the per-tenant ChromaDB collection.
            # This call is idempotent — safe to call on every invocation.
            collection = self._get_or_create_collection(tenant_id)

            # Step 3: Embed the message.
            # This is an external API call — the most likely point of failure.
            # Specific OpenAI exceptions are caught below the outer try/except.
            embedding = self._embed(message)

            # Step 4: Cold start — collection empty, no baseline to compare against.
            # We add the first embedding to begin building the knowledge base,
            # but return None because there is nothing to compare against yet.
            # Returning an anomaly on the very first message would be a false positive.
            count = collection.count()
            if count == 0:
                self._add_embedding(collection, embedding, tenant_id, service, message, timestamp)
                logger.debug(
                    "semantic_detector_cold_start_global",
                    tenant_id=tenant_id,
                    service=service,
                )
                return None

            # Step 5: Query the nearest neighbour, filtered to this service only.
            # Why filter by service:
            #   A "database connection refused" from payment-service is NOT anomalous
            #   for auth-service, even though the messages are semantically similar.
            #   Without the service filter, auth-service's first DB error would be
            #   suppressed because payment-service had the same error previously.
            #   This would produce false negatives — missed anomalies.
            # where={"service": service} is ChromaDB's metadata equality filter.
            # It restricts the ANN search to only embeddings with matching service metadata.
            results = collection.query(
                query_embeddings=[embedding],
                n_results=1,
                where={"service": service},
            )

            # Step 6: No embeddings for this service yet — service-level cold start.
            # results['distances'][0] is an empty list when the where filter matches
            # no existing embeddings. Add first baseline for this service, return None.
            if not results["distances"][0]:
                self._add_embedding(collection, embedding, tenant_id, service, message, timestamp)
                logger.debug(
                    "semantic_detector_cold_start_service",
                    tenant_id=tenant_id,
                    service=service,
                )
                return None

            # Step 7: Extract distance and compute similarity.
            # ChromaDB returns cosine DISTANCE (0=identical direction, 1=orthogonal).
            # Cosine SIMILARITY = 1.0 - cosine_distance.
            # results['distances'][0][0] = distance to nearest neighbour (n_results=1).
            distance = results["distances"][0][0]
            similarity = 1.0 - distance

            # Extract nearest message preview for diagnostic details in the result
            nearest_message = ""
            if results.get("metadatas") and results["metadatas"][0]:
                nearest_message = results["metadatas"][0][0].get("message_preview", "")

            # Step 8: Known pattern — this message is similar enough to something seen before.
            # similarity >= threshold means the message is within the known pattern space.
            # Return None to indicate no anomaly.
            if similarity >= self._similarity_threshold:
                return None

            # Step 9: Novel pattern detected — similarity is below the threshold.
            # Add the new pattern to the collection immediately so that:
            #   a) Subsequent identical messages are NOT re-alerted (we learned the pattern).
            #   b) The collection evolves in real time with the service's error landscape.
            self._add_embedding(collection, embedding, tenant_id, service, message, timestamp)

            # Evict oldest embeddings if the collection exceeds max_collection_size.
            # We check AFTER adding the new embedding so the eviction count is correct.
            if collection.count() > self._max_collection_size:
                self._evict_oldest(collection)

            severity = _distance_to_severity(distance)

            # confidence = 1 - similarity: how far below the threshold this message is.
            # At similarity=0 (fully orthogonal): confidence=1.0.
            # At similarity=threshold-ε (just barely detected): confidence≈(1-threshold).
            confidence = 1.0 - similarity

            logger.info(
                "semantic_detector_anomaly_detected",
                tenant_id=tenant_id,
                service=service,
                similarity=round(similarity, 4),
                distance=round(distance, 4),
                severity=severity,
                confidence=round(confidence, 4),
            )

            return AnomalyDetectionResult(
                detected=True,
                tenant_id=tenant_id,
                service=service,
                anomaly_type="new_error_pattern",
                severity=severity,
                confidence=confidence,
                details={
                    "similarity_score": round(similarity, 4),
                    "distance": round(distance, 4),
                    "nearest_message": nearest_message,
                    "threshold": self._similarity_threshold,
                },
                # detected_at: when the anomaly decision was made (now), not when the
                # log was emitted (timestamp). These can differ by minutes in a batched pipeline.
                detected_at=datetime.now(timezone.utc),
            )

        except RateLimitError as exc:
            # OpenAI rate limit — transient, expected under high API load.
            # Log at WARN (not ERROR) because this is recoverable: next message will retry.
            # Fail-open: return None so the consumer continues without stopping.
            logger.warning(
                "semantic_detector_openai_rate_limit",
                tenant_id=tenant_id,
                service=service,
                error=str(exc),
            )
            return None

        except APIError as exc:
            # OpenAI API error — unexpected (auth failure, quota exhausted, server error).
            # Log at ERROR because this may indicate a configuration or quota problem.
            # Fail-open: return None so the consumer continues processing other messages.
            logger.error(
                "semantic_detector_openai_api_error",
                tenant_id=tenant_id,
                service=service,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return None

        except Exception as exc:
            # ChromaDB error or any other unexpected failure.
            # Log at ERROR with full context so on-call engineers can diagnose.
            # Fail-open: returning None means "no anomaly detected" — the pipeline continues.
            logger.error(
                "semantic_detector_unexpected_error",
                tenant_id=tenant_id,
                service=service,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return None
