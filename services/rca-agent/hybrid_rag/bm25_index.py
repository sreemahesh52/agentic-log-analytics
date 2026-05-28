"""
BM25 keyword index over past incidents stored in PostgreSQL.
=============================================================
WHAT IS BM25?
=============================================================
BM25 (Best Match 25) is a probabilistic ranking function used by most text
search engines — Elasticsearch's default similarity is BM25.
Given a query Q and a corpus of documents D, BM25 scores each document by:
  score(D, Q) = Σ IDF(qi) × TF(qi, D) × (k1 + 1)
                i ————————————————————————————
                            TF(qi, D) + k1 × (1 - b + b × |D| / avgdl)
  Where:
    qi = each query term
    TF(qi, D) = term frequency — how often qi appears in D
    IDF(qi) = inverse document frequency — log((N - n(qi) + 0.5) / (n(qi) + 0.5))
                  Rare words across the corpus score higher than common words
    |D| = length of document D (number of tokens)
    avgdl = average document length across the corpus
    k1, b = tuning constants (BM25Okapi defaults: k1=1.5, b=0.75)
  In plain English: a document scores high when:
    - It contains many of the query's rare words
    - Its length is similar to the average (not inflated with filler)
Example — why BM25 beats simple keyword counting:
  Query: "connection pool exhausted"
  Doc A: "database connection pool exhausted — all 10 connections occupied"
  Doc B: "connection connection connection pool pool pool pool pool pool pool"
  BM25 scores Doc A higher because the word "pool" appears 11 times in Doc B
  (indicating a possibly spam/irrelevant document) while Doc A uses each
  relevant term naturally once.
=============================================================
WHEN DOES BM25 WIN OVER VECTOR SEARCH?
=============================================================
BM25 is better when:
  - Query uses exact technical terminology: "connection pool", "TLS certificate",
    "Kafka consumer lag", "OOM kill"
  - The document corpus uses those exact words (not synonyms)
  - Relevance is about presence of specific technical tokens, not concept
Vector search is better when:
  - Query and documents use different words for the same concept:
    query "out of memory" ↔ document "OOM error"
    query "service timing out" ↔ document "upstream timeout"
  - Semantic relationship matters more than exact word overlap
The hybrid RAG pipeline uses both and merges their results via RRF, letting
each method compensate for the other's weakness.
=============================================================
CORPUS CACHING STRATEGY
=============================================================
Why cache the corpus?
  BM25 requires building an in-memory index from the entire corpus on each call.
  With 20–200 past incidents, the PostgreSQL query + BM25 index build takes 50–200ms.
  Multiple tool calls per RCA investigation (the agent calls SearchKnowledgeBase
  once per incident, and there may be dozens of concurrent investigations) would
  hammer the database unnecessarily.
Why 5 minutes (300 seconds)?
  The past_incidents table changes rarely — only when the Self-Learning Indexer
  (Step 14b) adds a high-quality RCA result. 5 minutes is short enough that new
  knowledge appears within one investigation cycle, long enough to serve most
  in-flight investigations from memory.
Why per-tenant caching?
  Each tenant has their own incidents. Acme Corp's incident history must never
  bleed into Startup Co's cache entry. The cache is keyed by tenant_id (UUID string).
this class queries the corpus and scores it.
  It does not embed text, it does not call ChromaDB, it does not fuse results.
db_pool is injected via __init__.
  This class never creates a database connection — it receives one.
all SQL uses $N positional parameters.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import asyncpg
import structlog

# structlog produces structured JSON logs with all context fields included.
log = structlog.get_logger(__name__)

# Cache TTL in seconds. The corpus is refreshed from PostgreSQL when the
# cached entry is older than this value. Tunable without code changes.
_CORPUS_CACHE_TTL_SECONDS = 300

# Maximum incidents to retrieve from the DB. 200 is enough to cover even
# large tenants while keeping BM25 index build time under 50ms.
_MAX_CORPUS_SIZE = 200


@dataclass
class BM25Result:
    """One scored result from the BM25 index, ready for fusion with vector results.
    this dataclass holds BM25-specific
    scoring metadata. The final_rank field is not here — ranking is determined
    by the cross-encoder (Step 4), not by BM25 alone.
    """

    incident_id: str
    description: str
    root_cause: str
    resolution: str
    service: str
    # bm25_score is the raw BM25 relevance score. Not normalised to [0,1].
    # Scores are relative within a single query — do not compare across queries.
    bm25_score: float
    # rank is 1-based (rank=1 is the most relevant). Used by RRF — see fusion.py.
    rank: int


class BM25Index:
    """Keyword retrieval over past_incidents using BM25Okapi from rank_bm25.
    Why BM25Okapi and not a plain TF-IDF?
    BM25 adds two improvements over TF-IDF:
      1. Term saturation: TF(qi, D) is sub-linear — doubling term frequency
         does not double the score. This prevents document stuffing.
      2. Length normalisation: long documents are penalised (parameter b=0.75).
         A 2000-word incident description does not automatically beat a 100-word
         one just because it mentions the query term more times.
    BM25Okapi is the standard BM25 variant (Okapi BM25 from the Okapi IR system,
    Robertson et al. 1994). rank_bm25 is a pure-Python implementation.
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        """Inject the asyncpg connection pool — never create connections here.
        db_pool is the abstraction.
        The caller (Kafka consumer or test fixture) controls pool lifecycle.
        """
        self._db_pool = db_pool

        # In-memory corpus cache. Key: tenant_id (str).
        # Value: (corpus: list[str], incidents: list[dict], cached_at: float)
        # cached_at uses time.monotonic — not wall clock — so timezone/DST
        # changes do not affect cache expiry. monotonic only moves forward.
        self._cache: dict[str, tuple[list[str], list[dict], float]] = {}

        # TTL as an instance attribute so tests can override it directly.
        # Setting idx._cache_ttl_seconds = 0 forces cache misses in tests.
        self._cache_ttl_seconds: int = _CORPUS_CACHE_TTL_SECONDS

    async def search(
        self,
        tenant_id: str,
        query: str,
        top_k: int = 10,
    ) -> list[BM25Result]:
        """Score all past incidents against query using BM25 and return top_k.
        Returns [] on any error (fail-open) so the RRF fusion step can still
        proceed with vector results alone.
        Args:
            tenant_id: UUID string of the tenant whose incidents to search.
            query: Natural language description of the current incident.
            top_k: Maximum number of results to return. Defaults to 10
                       (enough candidates for RRF to re-order effectively).
        Returns:
            list[BM25Result] sorted by bm25_score descending (rank=1 highest).
            Empty list if corpus is empty or all scores are zero.
        """
        # --- Stage 1: load or refresh the corpus ---
        # _get_corpus handles caching internally — this call may hit the DB
        # or return the in-memory cache depending on TTL state.
        corpus, incidents = await self._get_corpus(tenant_id)

        if not corpus:
            # No incidents seeded for this tenant yet. This is not an error —
            # a new tenant simply has no history. The agent will receive:
            # "No similar past incidents found. This appears to be a novel incident."
            log.info("bm25_empty_corpus", tenant_id=tenant_id)
            return []

        # --- Stage 2: build BM25 index and score the query ---
        # Import here (not at module level) so the module loads fast even if
        # rank_bm25 is not installed — tests that mock BM25 never hit this path.
        from rank_bm25 import BM25Okapi  # type: ignore[import]

        # BM25 requires tokenised documents. Lowercase + split is the minimum
        # tokenisation pipeline. Production improvement: remove punctuation,
        # apply stemming, strip stop words. For technical incident text (which
        # uses distinctive tokens like "connection pool", "OOM", "Kafka"), basic
        # splitting is effective without a full NLP pipeline.
        tokenised_corpus = [doc.lower().split() for doc in corpus]

        # Build the BM25 index over the tokenised corpus.
        # Time complexity: O(N × avg_doc_length). With 200 incidents of ~100
        # words each, this takes ~5ms — acceptable per tool call.
        bm25 = BM25Okapi(tokenised_corpus)

        # Score the query against every document.
        # Returns a numpy array of scores, one per document.
        tokenised_query = query.lower().split()
        scores = bm25.get_scores(tokenised_query)

        # --- Stage 3: rank documents by score and return top_k ---
        # Sort indices by score descending. We want the highest scores first.
        top_indices = sorted(
            range(len(scores)),
            key=lambda i: scores[i],
            reverse=True,
        )[:top_k]

        results = []
        for rank, idx in enumerate(top_indices):
            # scores[idx] == 0 means the document contained none of the query
            # terms. Including zero-score documents in the merged list would give
            # them undeserved RRF score just from appearing. Filter them out.
            if scores[idx] <= 0:
                continue

            incident = incidents[idx]
            results.append(
                BM25Result(
                    # asyncpg returns UUID columns as uuid.UUID objects.
                    # Convert to str for consistency with JSON serialisation.
                    incident_id=str(incident["incident_id"]),
                    description=incident["description"],
                    root_cause=incident["root_cause"],
                    resolution=incident["resolution"],
                    service=incident["service"],
                    bm25_score=float(scores[idx]),
                    # rank is 1-based so RRF formula 1/(k + rank) makes sense.
                    rank=rank + 1,
                )
            )

        log.debug(
            "bm25_search_complete",
            tenant_id=tenant_id,
            query_tokens=len(tokenised_query),
            corpus_size=len(corpus),
            results_count=len(results),
        )
        return results

    async def _get_corpus(
        self,
        tenant_id: str,
    ) -> tuple[list[str], list[dict]]:
        """Return (corpus, incidents) from cache or PostgreSQL.
        The corpus is a parallel list of strings — corpus[i] is the BM25-indexed
        text for incidents[i]. They must stay in sync: deleting or reordering
        one without the other would produce wrong incident_id → score mappings.
        Cache structure:
          _cache[tenant_id] = (corpus, incidents, cached_at_monotonic)
          cached_at uses time.monotonic — immune to system clock adjustments.
        """
        # --- Check cache ---
        cached = self._cache.get(tenant_id)
        if cached is not None:
            corpus, incidents, cached_at = cached
            # time.monotonic is guaranteed non-decreasing on the same process.
            # If the elapsed time is under TTL, return the cached corpus.
            if (time.monotonic() - cached_at) < self._cache_ttl_seconds:
                log.debug("bm25_corpus_cache_hit", tenant_id=tenant_id)
                return corpus, incidents

        # --- Cache miss or expired: query PostgreSQL ---
        log.debug("bm25_corpus_cache_miss", tenant_id=tenant_id)

        # Parameterised query — tenant_id from caller, never from user input.
        # ORDER BY created_at DESC ensures the most recent incidents appear in
        # the BM25 index even if LIMIT truncates older ones.
        query_sql = """
            SELECT incident_id, service, description, root_cause, resolution
            FROM past_incidents
            WHERE tenant_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """
        try:
            # acquire borrows one connection from the pool for this query.
            # async with returns it to the pool even if the query raises.
            async with self._db_pool.acquire() as conn:
                rows = await conn.fetch(query_sql, tenant_id, _MAX_CORPUS_SIZE)
        except Exception as exc:
            log.warning(
                "bm25_corpus_db_error",
                tenant_id=tenant_id,
                error=str(exc),
            )
            # Return empty on DB error so vector search can still proceed.
            return [], []
        # Convert asyncpg Record objects to plain dicts for easier attribute access.
        incidents = [dict(r) for r in rows]
        # BM25 indexes description + root_cause together.
        # Why both? Description explains what users observed; root_cause explains why.
        # Queries about symptoms ("service returning 503s") match description;
        # queries about causes ("connection pool exhausted") match root_cause.
        # Concatenating both gives BM25 the best chance of relevance.
        corpus = [
            f"{r['description']} {r['root_cause']}"
            for r in incidents
        ]
        # Store in cache with current monotonic timestamp.
        # time.monotonic returns a float in seconds — suitable as a timestamp
        # for relative comparisons (cache age), not for display or logging.
        self._cache[tenant_id] = (corpus, incidents, time.monotonic())
        log.info(
            "bm25_corpus_loaded",
            tenant_id=tenant_id,
            incident_count=len(incidents),
        )
        return corpus, incidents
