"""
Reciprocal Rank Fusion — merges BM25 and vector ranked lists into one.
=============================================================
THE CORE PROBLEM: TWO INCOMPATIBLE SCORING SCALES
=============================================================
BM25 scores:
  Score for "database connection pool" query: [0.92, 0.71, 0.58, ...]
  These are logarithmic IDF-weighted term frequency scores.
  Maximum is unbounded — adding more matching tokens increases the score.
  A score of "0.92" in isolation means nothing without knowing the corpus size
  and query terms.
Vector similarity scores:
  Score for same query: [0.94, 0.87, 0.76, ...]
  These are cosine similarities: bounded in [0, 1].
  0.94 means "94% cosine similarity to the query embedding".
You cannot add, average, or compare these two scales directly.
  BM25 score 0.92 vs vector score 0.94: are these "about the same"? Unknowable.
  One method might use different magnitude ranges in different query conditions.
=============================================================
THE RRF SOLUTION: RANK-BASED FUSION
=============================================================
Reciprocal Rank Fusion (Cormack, Clarke, and Buettcher, SIGIR 2009) solves
this by ignoring the actual scores entirely and using only rank positions.
For each document d across all ranked lists L:
  RRF_score(d) = Σ 1 / (k + rank_L(d))
                 L ∈ Lists
  Where k = 60 (empirically determined constant — see note below)
        rank_L(d) = position of document d in list L (1-based; absent = excluded)
Example with k=60:
  Incident A: ranked 1st in BM25, ranked 1st in vector
    RRF = 1/(60+1) + 1/(60+1) = 0.0328
  Incident B: ranked 1st in BM25 only, not in vector results
    RRF = 1/(60+1) = 0.0164
  Incident C: ranked 5th in BM25, ranked 2nd in vector
    RRF = 1/(60+5) + 1/(60+2) = 0.0154 + 0.0161 = 0.0315
  Sorting: A (0.0328) > C (0.0315) > B (0.0164)
Interpretation:
  Incident A was the top result in BOTH retrieval methods — very high confidence.
  Incident C appeared in both lists but lower down — still good (cross-validated).
  Incident B was top in one method only — could be a false positive.
=============================================================
WHY k=60?
=============================================================
k controls how much rank differences matter:
  Lower k: rank differences are amplified. Rank 1 scores much higher than rank 2.
  Higher k: rank differences are smoothed. All ranks score more similarly.
k=60 was the experimentally optimal value in Cormack et al. (2009) across
multiple TREC (Text REtrieval Conference) test collections. It remains the
standard default in information retrieval systems today.
With k=60:
  Rank 1 contribution: 1/61 ≈ 0.0164
  Rank 10 contribution: 1/70 ≈ 0.0143
  Ratio: only 14% difference between rank 1 and rank 10
This relatively flat weighting means RRF is robust to noise: if one method
has a mildly wrong top-1 result, it does not devastate the merged ranking.
=============================================================
WHY NOT WEIGHTED COMBINATION?
=============================================================
Alternative: multiply each score by a weight: 0.5 × BM25_norm + 0.5 × vector_sim
Problems:
  1. Requires normalising BM25 scores first (what is the max BM25 score?)
  2. The optimal weights are query-dependent and hard to tune offline
  3. Score scale changes between BM25 versions or corpus sizes
RRF has no tuning parameters beyond k (which the literature already optimised).
It consistently outperforms score-based combination in cross-encoder benchmarks.
this module only merges ranked lists.
  It does not query PostgreSQL, it does not embed text, it does not rerank.
  It is a pure function — inputs in, FusedResult list out, zero side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from hybrid_rag.bm25_index import BM25Result
from hybrid_rag.vector_search import VectorResult

# RRF smoothing constant. k=60 from Cormack et al. (2009).
# Larger k → smoother (rank differences matter less).
# Smaller k → sharper (top ranks dominate more).
_RRF_K = 60


@dataclass
class FusedResult:
    """One incident after RRF merging — carries provenance from both retrieval stages.
    The rrf_score is the combined RRF score from BM25 and vector rankings.
    bm25_rank and vector_rank are None if the incident did not appear in that
    retrieval method's results — useful for debugging why a result appeared.
    Example: bm25_rank=1, vector_rank=None means this incident was the top BM25
    match but did not appear at all in the ChromaDB vector results.
    This could indicate a terminology match without semantic similarity — worth
    flagging for manual review.
    """

    incident_id: str
    description: str
    root_cause: str
    resolution: str
    service: str
    # rrf_score: sum of 1/(k + rank) across all lists where this incident appeared.
    rrf_score: float
    # None if the incident was not in that retrieval method's results.
    bm25_rank: Optional[int]
    vector_rank: Optional[int]


def reciprocal_rank_fusion(
    bm25_results: list[BM25Result],
    vector_results: list[VectorResult],
    k: int = _RRF_K,
) -> list[FusedResult]:
    """Merge BM25 and vector ranked lists into a single list using RRF.
    Why a pure function instead of a class method?
    Fusion has no state — it takes two lists and returns one. There is no
    configuration to inject, no cache to maintain, no side effects. A pure
    function is the correct abstraction: easy to test, easy to reason about,
    impossible to misuse through shared state.
    Args:
        bm25_results: Ranked results from BM25Index.search. Can be empty.
        vector_results: Ranked results from VectorSearch.search. Can be empty.
        k: RRF constant. Default 60 (Cormack 2009 standard).
    Returns:
        list[FusedResult] sorted by rrf_score descending. The first item is
        the incident most strongly supported by both retrieval methods.
        Empty list if both inputs are empty.
    """
    # incident_scores accumulates RRF contributions from all ranked lists.
    # Key: incident_id (str). Value: dict of accumulated score and metadata.
    incident_scores: dict[str, dict] = {}

    # --- Process BM25 results ---
    # Each BM25Result has a 1-based rank. Contribution: 1 / (k + rank).
    for result in bm25_results:
        iid = result.incident_id

        if iid not in incident_scores:
            # First time seeing this incident — initialise its entry.
            # Store the description/root_cause/resolution from this result.
            # If the same incident appears in vector_results too, these fields
            # should be identical (same row from past_incidents table).
            incident_scores[iid] = {
                "rrf_score": 0.0,
                "bm25_rank": None,
                "vector_rank": None,
                "description": result.description,
                "root_cause": result.root_cause,
                "resolution": result.resolution,
                "service": result.service,
            }

        # Add BM25 contribution to the running RRF score.
        # 1 / (k + rank): higher rank (lower number) → larger contribution.
        incident_scores[iid]["rrf_score"] += 1.0 / (k + result.rank)
        incident_scores[iid]["bm25_rank"] = result.rank

    # --- Process vector results ---
    # Same logic as BM25 — each vector result contributes 1/(k + rank).
    for result in vector_results:
        iid = result.incident_id

        if iid not in incident_scores:
            # This incident appeared in vector results but not BM25 results.
            # It will have rrf_score = 1/(k + vector_rank) only.
            incident_scores[iid] = {
                "rrf_score": 0.0,
                "bm25_rank": None,
                "vector_rank": None,
                "description": result.description,
                "root_cause": result.root_cause,
                "resolution": result.resolution,
                "service": result.service,
            }

        # Add vector contribution. An incident appearing in both lists now
        # has rrf_score = 1/(k + bm25_rank) + 1/(k + vector_rank).
        incident_scores[iid]["rrf_score"] += 1.0 / (k + result.rank)
        incident_scores[iid]["vector_rank"] = result.rank

    if not incident_scores:
        # Both input lists were empty — return empty list, not an error.
        return []

    # --- Sort by RRF score descending ---
    # Items that appeared in both lists have the highest scores.
    # Items that appeared in only one list are ranked lower.
    sorted_items = sorted(
        incident_scores.items(),
        key=lambda x: x[1]["rrf_score"],
        reverse=True,
    )

    # Convert to FusedResult dataclasses.
    return [
        FusedResult(
            incident_id=iid,
            rrf_score=data["rrf_score"],
            bm25_rank=data["bm25_rank"],
            vector_rank=data["vector_rank"],
            description=data["description"],
            root_cause=data["root_cause"],
            resolution=data["resolution"],
            service=data["service"],
        )
        for iid, data in sorted_items
    ]
