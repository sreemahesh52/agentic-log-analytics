"""
Vector similarity search over past incidents in ChromaDB.
=============================================================
WHAT IS VECTOR (SEMANTIC) SEARCH?
=============================================================
Vector search converts both the query and every document into high-dimensional
vectors (embeddings). It then finds the documents whose vectors are closest to
the query vector, using a distance metric.
OpenAI's text-embedding-3-small produces 1536-dimensional vectors. Each number
in the vector encodes a latent semantic feature learned from massive text corpora.
Semantically similar texts produce vectors that point in similar directions —
their cosine similarity (or dot product, since embeddings are L2-normalised) is high.
How cosine similarity works:
  cos(A, B) = (A · B) / (|A| × |B|)
  Two identical texts → vectors are parallel → cos = 1.0 (maximum similarity)
  Two unrelated texts → vectors are roughly perpendicular → cos ≈ 0.0
  Antonyms/negations → vectors may point opposite → cos approaches -1.0
ChromaDB stores the past incident embeddings and provides approximate nearest
neighbour (ANN) search via HNSW (Hierarchical Navigable Small Worlds) index.
HNSW finds the top-k closest vectors in O(log N) time rather than O(N) linear scan.
=============================================================
WHEN DOES VECTOR SEARCH WIN OVER BM25?
=============================================================
Vector search is better when:
  - Query and documents use different words for the same concept:
    "out of memory" ↔ "OOM kill" ↔ "heap exhausted" ↔ "gc overhead limit exceeded"
  - Abbreviations: "TLS" ↔ "SSL" ↔ "certificate"
  - Service-specific jargon: "pod restart" ↔ "container restart" ↔ "process respawn"
  - Symptoms described differently: "slow" ↔ "latency spike" ↔ "p99 degraded"
BM25 is better when:
  - Technical tokens appear verbatim: "connection pool", "Kafka consumer lag"
  - Version numbers: "redis 6.2" ↔ "redis 7.x" should NOT match
  - The incident description uses the exact phrase the query uses
The hybrid RAG pipeline uses both, giving the LLM the benefit of exact keyword
matching AND semantic understanding.
=============================================================
MULTI-TENANCY IN CHROMADB
=============================================================
Every tenant gets their own ChromaDB collection named past_incidents_{tenant_id}.
This means:
  - Acme Corp's incidents are never mixed with Startup Co's incidents
  - Vector indices are built per-tenant (appropriate for tenant-specific jargon)
  - Deleting a tenant's data is a single collection.delete call
The collection is created at seed time (scripts/seed_incidents.py) and grown
by the Self-Learning Indexer (Step 14b). If the collection does not exist,
get_collection raises — we return [] rather than crashing the pipeline.
this class only embeds and queries.
chroma_client and openai_client are injected.
OpenAI error or ChromaDB error → return [], not raise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

# structlog produces structured JSON logs for every search call.
log = structlog.get_logger(__name__)

# Embedding model — must match what seed_incidents.py used.
# Changing this requires re-embedding the entire knowledge base.
_EMBEDDING_MODEL = "text-embedding-3-small"

# ChromaDB collection name pattern. Must match seed_incidents.py exactly.
_COLLECTION_NAME_PREFIX = "past_incidents_"


@dataclass
class VectorResult:
    """One result from ChromaDB vector search, ready for fusion with BM25 results.
    similarity_score is derived from the ChromaDB distance.
    ChromaDB returns distances, not similarities. For cosine distance:
      distance = 1 - cosine_similarity
      Therefore: similarity = 1 - distance
    Interpretation:
      similarity_score = 1.0 → identical documents (or duplicates)
      similarity_score = 0.7 → related but distinct
      similarity_score = 0.0 → completely unrelated
    rank is 1-based (rank=1 is closest vector). Used by RRF fusion — see fusion.py.
    """

    incident_id: str
    description: str
    root_cause: str
    resolution: str
    service: str
    # similarity_score derived from ChromaDB cosine distance: 1.0 - distance.
    similarity_score: float
    rank: int


class VectorSearch:
    """Semantic similarity search over past incidents using ChromaDB + OpenAI embeddings.
    Why VectorSearch is a class and not a plain function:
    It holds two injected clients (chroma_client and openai_client) that are
    expensive to create (TCP connections, API clients). Injecting them once at
    startup and reusing across calls is more efficient than creating them per query.
    both clients are injected via __init__.
    This class never calls chromadb.Client or openai.OpenAI internally —
    those are infrastructure concerns the caller controls.
    For testing, mock clients are injected without touching network or API keys.
    """

    def __init__(self, chroma_client: Any, openai_client: Any) -> None:
        """Inject ChromaDB client and OpenAI client — never create them here.
        Args:
            chroma_client: A chromadb.Client or chromadb.EphemeralClient instance.
                           The calling code controls which type is used.
            openai_client: An openai.OpenAI instance (sync client).
                           The async search method calls embeddings.create
                           synchronously inside the coroutine — acceptable for
                           an in-process ChromaDB and low-concurrency usage.
                           For high-concurrency production, wrap in run_in_executor.
        """
        self._chroma_client = chroma_client
        self._openai_client = openai_client

    async def search(
        self,
        tenant_id: str,
        query: str,
        top_k: int = 10,
    ) -> list[VectorResult]:
        """Embed the query and find the top_k semantically similar past incidents.
        Failure modes handled:
          - Collection does not exist (new tenant, no incidents seeded): return []
          - Collection is empty: return []
          - OpenAI API error: log WARN, return []
          - ChromaDB query error: log WARN, return []
        Args:
            tenant_id: UUID string of the tenant. Used to select their collection.
            query: Natural language description of the current incident.
            top_k: Maximum number of results to return. Defaults to 10.
        Returns:
            list[VectorResult] sorted by similarity_score descending (rank=1 closest).
            Empty list on any failure.
        """
        # --- Stage 1: Get or validate the ChromaDB collection ---
        collection_name = f"{_COLLECTION_NAME_PREFIX}{tenant_id}"

        try:
            # get_collection raises ValueError/InvalidCollectionException if the
            # collection does not exist. A missing collection means the tenant
            # has no seeded incidents — not an error, just no history yet.
            collection = self._chroma_client.get_collection(name=collection_name)
        except Exception:
            # Log at DEBUG (not WARN) because a missing collection is expected
            # for new tenants and is not an operational problem.
            log.debug(
                "vector_search_collection_not_found",
                tenant_id=tenant_id,
                collection=collection_name,
            )
            return []

        # --- Stage 2: Check if collection has any documents ---
        count = collection.count()
        if count == 0:
            log.info("vector_search_empty_collection", tenant_id=tenant_id)
            return []

        # min(top_k, count): ChromaDB raises if n_results > number of documents.
        n_results = min(top_k, count)

        # --- Stage 3: Embed the query with OpenAI ---
        try:
            # text-embedding-3-small produces L2-normalised 1536-dim vectors.
            # L2-normalised means |v| = 1 for every vector, so cosine similarity
            # equals the dot product: cos(A, B) = A · B (since |A| = |B| = 1).
            response = self._openai_client.embeddings.create(
                model=_EMBEDDING_MODEL,
                input=query,
            )
            # response.data is a list; [0] is the first (and only) embedding.
            # .embedding is a list[float] of length 1536.
            embedding = response.data[0].embedding
        except Exception as exc:
            log.warning(
                "vector_search_embedding_error",
                tenant_id=tenant_id,
                error=str(exc),
            )
            # Return empty so BM25 results can still proceed through the pipeline.
            return []

        # --- Stage 4: Query ChromaDB with the embedding ---
        try:
            # query_embeddings: list of query vectors — we pass one query at a time.
            # include: which result fields to return. 'distances' gives us
            # cosine distances that we convert to similarity scores.
            results = collection.query(
                query_embeddings=[embedding],
                n_results=n_results,
                include=["documents", "metadatas", "distances"],
            )
        except Exception as exc:
            log.warning(
                "vector_search_chroma_error",
                tenant_id=tenant_id,
                error=str(exc),
            )
            return []

        # --- Stage 5: Convert ChromaDB results to VectorResult dataclasses ---
        # results['documents'][0]: list of document strings (one per result)
        # results['metadatas'][0]: list of metadata dicts (one per result)
        # results['distances'][0]: list of cosine distances (one per result)
        # The [0] indexing is because we submitted one query — results are batched.
        documents = results["documents"][0]
        metadatas = results["metadatas"][0]
        distances = results["distances"][0]

        vector_results = []
        for i in range(len(documents)):
            meta = metadatas[i]

            # Cosine distance → cosine similarity conversion.
            # ChromaDB with cosine space returns distance = 1 - cosine_similarity.
            # Therefore: cosine_similarity = 1 - distance.
            # With L2-normalised OpenAI embeddings, cosine_similarity ∈ [0, 1].
            similarity = 1.0 - distances[i]

            vector_results.append(
                VectorResult(
                    incident_id=meta.get("incident_id", ""),
                    description=documents[i],
                    root_cause=meta.get("root_cause", ""),
                    resolution=meta.get("resolution", ""),
                    service=meta.get("service", ""),
                    similarity_score=similarity,
                    # rank is 1-based. ChromaDB returns results sorted by distance
                    # (ascending) so index 0 is the closest match → rank 1.
                    rank=i + 1,
                )
            )

        log.debug(
            "vector_search_complete",
            tenant_id=tenant_id,
            results_count=len(vector_results),
            top_similarity=vector_results[0].similarity_score if vector_results else None,
        )
        return vector_results
