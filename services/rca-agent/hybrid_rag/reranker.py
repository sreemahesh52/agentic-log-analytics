"""
Cross-encoder reranker — re-scores the fused shortlist for final ranking.
=============================================================
BI-ENCODER VS CROSS-ENCODER: THE ACCURACY VS SPEED TRADEOFF
=============================================================
The vector search stage (vector_search.py) used a BI-ENCODER:
  - Query is embedded independently → query_vector
  - Each document is embedded independently → doc_vector
  - Relevance = cosine_similarity(query_vector, doc_vector)
  Advantage: embeddings can be pre-computed and stored (ChromaDB)
  Disadvantage: query and document never "see" each other during scoring —
  the model cannot reason about how the specific query relates to the
  specific document.
A CROSS-ENCODER processes query and document TOGETHER:
  Input to the model: [CLS] query [SEP] document [SEP]
  Output: a single relevance score for this (query, document) pair
  The model attends to both texts simultaneously — it can reason about whether
  specific phrases in the document directly answer the specific question in the query.
Example where cross-encoder outperforms bi-encoder:
  Query: "payment service connection pool exhausted — how was it fixed?"
  Document: "Redis OOM eviction causing cache stampede fixed by enabling maxmemory-policy"
  Bi-encoder: payment + service + connection + pool = some overlap → moderate score
  Cross-encoder: "this document talks about Redis, not connection pools" → low score
Cross-encoder accuracy is higher because it sees the interaction between texts.
Cross-encoder speed is lower because it cannot pre-compute document scores.
Every new query requires re-scoring every candidate from scratch.
=============================================================
WHY CROSS-ENCODER AS A SECOND STAGE, NOT FIRST?
=============================================================
Cross-encoders are expensive: ~50ms per (query, document) pair on CPU.
Scoring all 20 past incidents on every tool call: 20 × 50ms = 1 second.
That is too slow for an interactive investigation.
The two-stage approach:
  Stage 1: BM25 + vector search → fast, returns top-10 candidates each
  Stage 2: RRF merges → typically 10–20 unique candidates
  Stage 3: Cross-encoder re-scores only the merged 10–20 candidates → 0.5–1s
The cross-encoder never sees the full corpus — only the pre-filtered candidates
that both BM25 and vector search agreed are plausibly relevant.
=============================================================
MODEL: cross-encoder/ms-marco-MiniLM-L-6-v2
=============================================================
ms-marco-MiniLM-L-6-v2 is fine-tuned on the MS MARCO passage ranking dataset —
350,000 query–passage pairs rated for relevance.
MiniLM-L-6-v2 means: 6 Transformer layers, MiniLM architecture (distilled from
a larger model). Approximately 22M parameters. Downloads as ~85MB.
Scores are raw logits (not probabilities) — suitable for ranking but not
for interpreting as "84% relevant". Higher is more relevant.
=============================================================
SINGLETON PATTERN: WHY ONE INSTANCE ONLY
=============================================================
Loading the cross-encoder model involves:
  1. Downloading ~85MB of weights (first run only, then cached by HuggingFace)
  2. Deserialising the model weights from disk (~0.5s)
  3. Allocating the model in memory (~200MB RAM)
  4. Allocating the tokeniser and vocabulary
Steps 2–4 happen on every instantiation. In a long-running service that processes
many incidents, recreating the model on every tool call would:
  - Add 500ms latency to every SearchKnowledgeBase call
  - Allocate and free 200MB RAM repeatedly (causing GC pressure)
  - Potentially exhaust memory if multiple coroutines load simultaneously
The Singleton pattern guarantees the model loads once at service startup
(via load) and is reused for the process lifetime.
How the Singleton works:
  __new__ is called before __init__ on every constructor call.
  _instance = None at class level → first call: creates the instance.
  _instance is not None → subsequent calls: return the existing instance.
  _model = None at class level → shared across all "instances" (which are the same).
this class only loads and runs the
  cross-encoder. It does not query databases or call external APIs.
if load was never called, rerank returns
  candidates in RRF order (fallback) rather than raising.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    # Only imported during type-checking — not at runtime — to avoid loading
    # sentence_transformers unless load is explicitly called.
    from sentence_transformers import CrossEncoder  # type: ignore[import]

# structlog produces structured JSON logs — model load latency is logged
# so operators can verify the startup time is within expectations.
log = structlog.get_logger(__name__)

# Model identifier for sentence-transformers.
# HuggingFace Hub caches downloaded models in ~/.cache/huggingface/
_RERANKER_MODEL_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"


@dataclass
class RerankedResult:
    """Final result after cross-encoder reranking.
    rerank_score is the raw cross-encoder logit. Higher = more relevant.
    final_rank is 1-based — rank=1 is the result returned first to the LLM.
    """

    incident_id: str
    description: str
    root_cause: str
    resolution: str
    service: str
    # rerank_score: raw cross-encoder logit OR rrf_score (if model not loaded).
    rerank_score: float
    # final_rank: 1-based position in the reranked output.
    final_rank: int


class CrossEncoderReranker:
    """Singleton cross-encoder reranker using ms-marco-MiniLM-L-6-v2.
    Usage pattern in the Kafka consumer (Step 13d):
      reranker = CrossEncoderReranker
      reranker.load # called once at startup, loads ~85MB model
      ...
      results = reranker.rerank(query, fused_candidates, top_k=3)
    Why a class and not a module-level object?
    A class enables:
      - Deferred loading (load is separate from __init__)
      - Clean testability (override _model directly in tests without patching)
      - Standard Singleton API that callers recognise
    A module-level `reranker = CrossEncoderReranker` would eagerly load the
    model on import, even in tests that only test BM25 or RRF.
    """

    # _instance: the single CrossEncoderReranker instance or None before first call.
    # Class variable — shared across all apparent "instances" of the class.
    _instance: "CrossEncoderReranker | None" = None

    # _model: the loaded CrossEncoder or None before load is called.
    # Class variable — shared so the model is only ever in memory once.
    _model: "CrossEncoder | None" = None

    def __new__(cls) -> "CrossEncoderReranker":
        """Return the single existing instance, or create it on the first call.
        __new__ is called BEFORE __init__. This is the key to the Singleton pattern:
        by returning an existing object from __new__, Python skips __init__ on
        subsequent calls (since __init__ only runs on newly created objects).
        Without __new__ override, every CrossEncoderReranker call creates a
        new object, and _model would be duplicated across all of them.
        """
        # _instance is None only on the very first call.
        if cls._instance is None:
            # super.__new__(cls) allocates the new object.
            # We save it to the class-level _instance for all future calls.
            cls._instance = super().__new__(cls)
        # Every call — including the first — returns the same object.
        return cls._instance

    def load(self) -> None:
        """Load the cross-encoder model into memory. Safe to call multiple times.
        This method is idempotent — calling it twice does not reload the model.
        The second call is a no-op because `_model is not None` after the first.
        Call this once at service startup, not on every request. The model
        weights (~85MB) take ~0.5s to deserialise from the HuggingFace cache.
        On first-ever run (no cache), it downloads ~85MB from huggingface.co.
        """
        if self._model is not None:
            # Model already loaded — idempotent no-op.
            return

        # Import inside load so the module imports fast without triggering
        # model loading. Tests that mock _model never reach this import.
        from sentence_transformers import CrossEncoder  # type: ignore[import]

        log.info("reranker_loading", model=_RERANKER_MODEL_NAME)
        # CrossEncoder constructor: downloads weights on first call (if not cached),
        # deserialises, and returns a ready-to-use model.
        CrossEncoderReranker._model = CrossEncoder(_RERANKER_MODEL_NAME)
        log.info("reranker_loaded", model=_RERANKER_MODEL_NAME)

    def rerank(
        self,
        query: str,
        candidates: list[Any],  # list[FusedResult] — avoid circular import with Any
        top_k: int = 3,
    ) -> list[RerankedResult]:
        """Re-score FusedResult candidates using the cross-encoder and return top_k.
        If the model has not been loaded (load never called), falls back to RRF
        order — candidates are returned as-is (by rrf_score), not re-scored.
        This ensures the pipeline produces results even in test environments where
        the 85MB model download is intentionally skipped.
        Args:
            query: The incident query string sent to SearchKnowledgeBase.
            candidates: FusedResult list from reciprocal_rank_fusion. Already
                        sorted by rrf_score descending. Typically 10–20 items.
            top_k: Number of results to return after reranking. Default 3.
        Returns:
            list[RerankedResult] of length min(top_k, len(candidates)), sorted
            by rerank_score descending (final_rank=1 is the best match).
        """
        if not candidates:
            return []

        # --- Fallback: model not loaded → return candidates in RRF order ---
        if self._model is None:
            log.warning(
                "reranker_model_not_loaded_using_rrf_fallback",
                candidate_count=len(candidates),
            )
            # Use RRF score as the rerank_score so callers can still sort/display.
            return [
                RerankedResult(
                    incident_id=c.incident_id,
                    description=c.description,
                    root_cause=c.root_cause,
                    resolution=c.resolution,
                    service=c.service,
                    # rrf_score used as proxy when cross-encoder is unavailable.
                    rerank_score=c.rrf_score,
                    final_rank=i + 1,
                )
                for i, c in enumerate(candidates[:top_k])
            ]

        # --- Cross-encoder scoring ---
        # Build (query, document) pairs for every candidate.
        # The cross-encoder attends to both the query and document simultaneously —
        # this is what distinguishes it from bi-encoder similarity search.
        pairs = [
            (query, f"{c.description} {c.root_cause}")
            for c in candidates
        ]

        # model.predict scores all pairs in one batch.
        # Returns a list/array of raw logit scores, one per pair.
        # Higher logit = more relevant to the query.
        scores = self._model.predict(pairs)

        # --- Sort by cross-encoder score and take top_k ---
        # zip pairs each candidate with its score for sorting.
        ranked = sorted(
            zip(candidates, scores),
            key=lambda x: x[1],
            reverse=True,  # highest score first
        )

        return [
            RerankedResult(
                incident_id=c.incident_id,
                description=c.description,
                root_cause=c.root_cause,
                resolution=c.resolution,
                service=c.service,
                # float ensures score is a plain Python float, not numpy scalar.
                rerank_score=float(score),
                # final_rank is 1-based: best cross-encoder score = rank 1.
                final_rank=i + 1,
            )
            for i, (c, score) in enumerate(ranked[:top_k])
        ]
