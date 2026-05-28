"""
hybrid_rag — multi-stage retrieval pipeline for past incident knowledge.
The four-stage pipeline this package implements:
  Stage 1 — BM25 keyword search (bm25_index.py):
    Queries PostgreSQL past_incidents table, scores each document by term
    frequency × inverse document frequency. Exact terminology matches score high.
  Stage 2 — Vector similarity search (vector_search.py):
    Embeds the query with OpenAI text-embedding-3-small, queries ChromaDB
    per-tenant collection. Conceptual/semantic similarity scores high.
  Stage 3 — Reciprocal Rank Fusion (fusion.py):
    Merges the two ranked lists using rank position (not raw scores, which are
    on incompatible scales). Items ranked high in BOTH lists score highest.
  Stage 4 — Cross-encoder reranking (reranker.py):
    Re-scores the fused shortlist by feeding (query, document) pairs into a
    small transformer model. More accurate than either retrieval stage alone
    because the model sees both texts together rather than comparing embeddings.
Why four stages instead of one?
  BM25 alone: misses "OOM error" for query "out of memory" (different words).
  Vector alone: misses "connection pool" exact match buried in a long document.
  RRF alone: merges well but does not re-evaluate relevance.
  All four: each stage compensates for the prior stage's weakness.
each module has exactly one job.
new retrieval strategies are added as new modules;
  fusion.py and search_knowledge_base.py never need modification.
every stage returns empty list on error — the pipeline
  degrades gracefully to whatever results remain.
"""

# Re-export the public surface used by tools/search_knowledge_base.py.
# Any module that orchestrates the pipeline imports from here, not from
# individual submodules — this keeps the import graph flat and stable.
from hybrid_rag.bm25_index import BM25Index, BM25Result
from hybrid_rag.fusion import FusedResult, reciprocal_rank_fusion
from hybrid_rag.reranker import CrossEncoderReranker, RerankedResult
from hybrid_rag.vector_search import VectorResult, VectorSearch

__all__ = [
    # Stage 1 — BM25
    "BM25Index",
    "BM25Result",
    # Stage 2 — Vector
    "VectorSearch",
    "VectorResult",
    # Stage 3 — Fusion
    "reciprocal_rank_fusion",
    "FusedResult",
    # Stage 4 — Reranking
    "CrossEncoderReranker",
    "RerankedResult",
]
