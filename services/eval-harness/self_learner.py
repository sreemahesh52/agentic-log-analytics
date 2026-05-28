"""Self-learning indexer — automatically grows the knowledge base with high-quality RCA results.
When an RCA investigation meets the quality bar (eval_mode='ground_truth',
faithfulness > 0.8, hallucination > 0.7, AUTO_LEARN=true), this class inserts
the incident into both PostgreSQL (past_incidents) and ChromaDB so future
investigations can use it as a reference.
Why only ground_truth eval_mode?
Similarity and heuristic scores are proxies — they cannot guarantee the RCA
matches a verified human label. Indexing a similarity-scored result could
pollute the knowledge base with incorrect root causes, which would degrade
future similarity lookups. ground_truth is the only mode where a human has
confirmed the root cause is correct.
Why both PostgreSQL and ChromaDB?
The RCA Agent's SearchKnowledgeBase tool uses a hybrid RAG approach:
  - BM25Index reads from PostgreSQL past_incidents (keyword matching)
  - VectorSearch reads from ChromaDB past_incidents_{tenant_id} (semantic matching)
Both stores must be kept in sync — inserting only one would break the hybrid search.
Why AUTO_LEARN as a string?
Environment variables are always strings. Comparing as `settings.auto_learn == "true"`
is correct; `settings.auto_learn is True` always evaluates to False.
"""

from __future__ import annotations

from uuid import uuid4

import structlog

log = structlog.get_logger(__name__)

# ChromaDB collection name template — must match SimilarityStrategy.COLLECTION_NAME_TEMPLATE.
# Changing this breaks the similarity evaluation for all tenants.
_COLLECTION_NAME_TEMPLATE = "past_incidents_{tenant_id}"

# OpenAI embedding model for ChromaDB entries.
# Must match the model used by SimilarityStrategy — cosine similarity comparisons
# between entries embedded with different models produce meaningless scores.
_EMBEDDING_MODEL = "text-embedding-3-small"


class SelfLearner:
    """Automatically indexes high-quality RCA results into the knowledge base.
    Implements the Observer pattern: the Kafka handler calls maybe_learn
    as part of post-evaluation fanout. SelfLearner decides internally whether
    the quality criteria are met — the handler does not need to check.
    Dependency Inversion: all dependencies are injected. Tests can provide
    a mock db_pool and chroma_client to verify indexing without real I/O.
    """

    def __init__(
        self,
        db_pool: object,
        chroma_client: object,
        openai_client: object,
        auto_learn: str,
        faithfulness_threshold: float,
        hallucination_threshold: float,
    ) -> None:
        """Inject all dependencies.
        Args:
            db_pool: asyncpg Pool for past_incidents inserts.
            chroma_client: ChromaDB client for past_incidents collection.
            openai_client: AsyncOpenAI for text-embedding-3-small.
            auto_learn: String "true" or "false" — env var value.
            faithfulness_threshold: Minimum faithfulness score to auto-index (0.8).
            hallucination_threshold: Minimum hallucination score to auto-index (0.7).
        """
        self._db_pool = db_pool
        self._chroma = chroma_client
        self._openai = openai_client
        self._auto_learn = auto_learn
        self._faithfulness_threshold = faithfulness_threshold
        self._hallucination_threshold = hallucination_threshold

    async def maybe_learn(
        self,
        tenant_id: str,
        eval_mode: str,
        faithfulness_score: float,
        hallucination_score: float,
        rca_id: str,
        root_cause: str,
        recommendations: list[str],
        affected_services: list[str],
        incident_description: str,
    ) -> bool:
        """Index the RCA result into the knowledge base if quality criteria are met.
        Criteria (all must be true):
          1. auto_learn == "true" — feature flag enabled
          2. eval_mode == "ground_truth" — human-verified faithfulness
          3. faithfulness_score > faithfulness_threshold — correct root cause
          4. hallucination_score > hallucination_threshold — no fabrication
        Args:
            tenant_id: Tenant namespace for DB and ChromaDB isolation.
            eval_mode: Which faithfulness strategy produced the score.
            faithfulness_score: Faithfulness evaluation result.
            hallucination_score: Hallucination evaluation result.
            rca_id: RCA UUID for cross-referencing.
            root_cause: Root cause text to store as knowledge.
            recommendations: List of remediation recommendations.
            affected_services: Affected service names — used as `service` field.
            incident_description: Description used as the ChromaDB document text.
        Returns:
            True if the result was indexed, False if skipped or failed.
        """
        # --- Gate: feature flag ---
        # String comparison — env vars are never Python booleans.
        if self._auto_learn != "true":
            return False

        # --- Gate: only verified eval modes ---
        # "ground_truth" is human-verified; "similarity" and "heuristic" are LLM-scored
        # proxies that still carry meaningful quality signal when above threshold.
        # Rejecting all non-ground_truth modes would prevent the KB from ever growing
        # without a manual labelling workflow.
        if eval_mode not in ("ground_truth", "similarity", "heuristic"):
            log.debug(
                "self_learner_skipped_eval_mode",
                tenant_id=tenant_id,
                eval_mode=eval_mode,
            )
            return False

        # --- Gate: quality thresholds ---
        # > (not >=) matches the spec notation "faithfulness>0.8, hallucination>0.7".
        if faithfulness_score <= self._faithfulness_threshold:
            log.debug(
                "self_learner_skipped_faithfulness",
                tenant_id=tenant_id,
                score=faithfulness_score,
                threshold=self._faithfulness_threshold,
            )
            return False

        if hallucination_score <= self._hallucination_threshold:
            log.debug(
                "self_learner_skipped_hallucination",
                tenant_id=tenant_id,
                score=hallucination_score,
                threshold=self._hallucination_threshold,
            )
            return False

        # --- Build the incident record ---
        incident_id = str(uuid4())
        service = affected_services[0] if affected_services else "unknown"
        resolution = "; ".join(recommendations) if recommendations else root_cause

        # --- Insert into PostgreSQL past_incidents ---
        pg_success = await self._insert_postgres(
            incident_id=incident_id,
            tenant_id=tenant_id,
            service=service,
            description=incident_description,
            root_cause=root_cause,
            resolution=resolution,
            rca_id=rca_id,
        )
        if not pg_success:
            return False

        # --- Embed and insert into ChromaDB ---
        chroma_success = await self._insert_chromadb(
            tenant_id=tenant_id,
            incident_id=incident_id,
            service=service,
            description=incident_description,
            root_cause=root_cause,
        )

        if chroma_success:
            log.info(
                "self_learner_indexed",
                tenant_id=tenant_id,
                rca_id=rca_id,
                incident_id=incident_id,
                faithfulness_score=faithfulness_score,
                hallucination_score=hallucination_score,
            )

        return chroma_success

    async def _insert_postgres(
        self,
        incident_id: str,
        tenant_id: str,
        service: str,
        description: str,
        root_cause: str,
        resolution: str,
        rca_id: str,
    ) -> bool:
        """Insert the auto-learned incident into past_incidents.
        Returns True on success, False on any exception. Failures are logged
        and the caller skips the ChromaDB step — the two stores must stay in sync.
        """
        try:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO past_incidents
                        (incident_id, tenant_id, source, service, description,
                         root_cause, resolution, tags)
                    VALUES
                        ($1::uuid, $2::uuid, 'auto_learned', $3, $4, $5, $6, $7)
        """,
                    incident_id,
                    tenant_id,
                    service,
                    description,
                    root_cause,
                    resolution,
                    # Tag with rca_id so operators can trace back to the investigation.
                    [f"rca_id:{rca_id}"],
                )
            return True
        except Exception as exc:
            log.error(
                "self_learner_postgres_insert_failed",
                tenant_id=tenant_id,
                incident_id=incident_id,
                error=str(exc),
            )
            return False

    async def _insert_chromadb(
        self,
        tenant_id: str,
        incident_id: str,
        service: str,
        description: str,
        root_cause: str,
    ) -> bool:
        """Embed the description and insert into the tenant's ChromaDB collection.
        Uses get_or_create_collection so the first auto-learned entry for a tenant
        creates the collection automatically — no manual setup required.
        Returns True on success, False on any exception.
        """
        # --- Embed the incident description ---
        try:
            response = await self._openai.embeddings.create(
                model=_EMBEDDING_MODEL,
                input=description,
            )
            embedding = response.data[0].embedding
        except Exception as exc:
            log.error(
                "self_learner_embed_failed",
                tenant_id=tenant_id,
                incident_id=incident_id,
                error=str(exc),
            )
            return False

        # --- Insert into ChromaDB ---
        collection_name = _COLLECTION_NAME_TEMPLATE.format(tenant_id=tenant_id)
        try:
            # get_or_create_collection is idempotent — safe to call on every insert.
            # This matches SimilarityStrategy which uses get_collection for lookups.
            collection = self._chroma.get_or_create_collection(collection_name)
            collection.add(
                ids=[incident_id],
                documents=[description],
                embeddings=[embedding],
                metadatas=[{
                    "root_cause": root_cause,
                    "service": service,
                    "tenant_id": tenant_id,
                    "source": "auto_learned",
                }],
            )
            return True
        except Exception as exc:
            log.error(
                "self_learner_chromadb_insert_failed",
                tenant_id=tenant_id,
                incident_id=incident_id,
                collection=collection_name,
                error=str(exc),
            )
            return False

    async def get_knowledge_base_size(self, tenant_id: str) -> int:
        """Return the current count of past_incidents rows for this tenant.
        Used by the Kafka handler to update the knowledge_base_size Prometheus gauge
        after a successful auto-learn write.
        Returns 0 on any database error — a stale gauge is preferable to a crash.
        """
        try:
            async with self._db_pool.acquire() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM past_incidents WHERE tenant_id = $1::uuid",
                    tenant_id,
                )
            return int(count)
        except Exception as exc:
            log.warning(
                "self_learner_kb_size_query_failed",
                tenant_id=tenant_id,
                error=str(exc),
            )
            return 0
