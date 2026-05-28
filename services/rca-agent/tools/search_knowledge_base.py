"""
SearchKnowledgeBase tool — orchestrates the full hybrid RAG pipeline.
This is the fourth RCA Agent tool, added in Step 13c. The first three tools
(QueryLogs, GetDependencies, BuildTimeline) retrieve data from the live system.
This tool retrieves institutional knowledge: what similar incidents occurred before
and what resolved them.
=============================================================
HOW THIS TOOL FITS INTO A REAL INVESTIGATION
=============================================================
The agent's prompt instructs it to call SearchKnowledgeBase FIRST, before querying
logs. This is the correct investigation strategy:
  Without prior knowledge: agent starts from scratch every time.
  Example: payment-service connection pool exhausted → agent calls QueryLogs,
  GetDependencies, BuildTimeline, reasons for 5+ iterations, concludes:
  "Increase pool max_size." Time: 15 iterations × 2s/LLM call = 30s.
  With SearchKnowledgeBase: agent calls SearchKnowledgeBase first.
  Result: "Incident #17 last week: same service, same error. Resolution:
  increased pool max_size from 10 to 25 and added statement_timeout = 30s."
  Agent verifies with QueryLogs (one call), concludes in 3 iterations.
  Time: 6s. Confidence: high. Token cost: 60% lower.
The knowledge base grows via the Self-Learning Indexer (Step 14b): high-quality
RCA results (faithfulness > 0.8, hallucination > 0.7) are automatically added
to past_incidents and re-embedded into ChromaDB. The more incidents the system
investigates, the better future investigations become.
=============================================================
FUNCTION SIGNATURE DESIGN (DEPENDENCY INVERSION)
=============================================================
Like the other three tools, search_knowledge_base accepts infrastructure
dependencies (bm25_index, vector_search, reranker) as parameters that are
pre-bound via functools.partial at registration time.
Why inject BM25Index and VectorSearch instead of creating them inside the tool?
  1. Tests: pass mock objects without touching PostgreSQL, ChromaDB, or OpenAI
  2. Shared instances: the BM25Index and VectorSearch are created once at
     consumer startup and reused across all investigations (pool management)
  3. Open/Closed: swapping BM25 for Elasticsearch or vector search for Weaviate
     only changes the registration code — not this function
The LLM sees only: query (str) and top_k (int).
Infrastructure arguments (tenant_id, bm25_index, vector_search, reranker) are
pre-bound by the caller and invisible to the LLM schema.
this function orchestrates the pipeline.
  It does not implement BM25, it does not embed text, it does not rank.
  It calls the right component in the right order and formats the output.
BM25Index, VectorSearch, CrossEncoderReranker are
  all Strategy implementations behind their class interfaces. Swapping any one
  requires no change to this function.
each stage degrades gracefully. If BM25 returns empty,
  only vector results proceed. If both return empty, "novel incident" message.
"""

from __future__ import annotations

import structlog

from hybrid_rag.bm25_index import BM25Index
from hybrid_rag.fusion import reciprocal_rank_fusion
from hybrid_rag.reranker import CrossEncoderReranker
from hybrid_rag.vector_search import VectorSearch
from tools.base import ToolSchema

# structlog produces structured JSON logs for every SearchKnowledgeBase call.
log = structlog.get_logger(__name__)

# Default number of final results to return to the LLM.
# 3 is enough context without overwhelming the LLM's context budget.
# Each result uses ~200 tokens (description + root_cause + resolution).
_DEFAULT_TOP_K = 3

# Number of candidates fetched from each retrieval stage before fusion/reranking.
# 10 per stage → up to 20 unique candidates after RRF → cross-encoder re-scores 20.
_RETRIEVAL_TOP_K = 10


async def search_knowledge_base(
    tenant_id: str,
    bm25_index: BM25Index,
    vector_search: VectorSearch,
    reranker: CrossEncoderReranker,
    query: str,
    top_k: int = _DEFAULT_TOP_K,
) -> str:
    """Search past incidents using BM25 + vector search + RRF + cross-encoder reranking.
    tenant_id, bm25_index, vector_search, and reranker are pre-bound via
    functools.partial at registration time. The LLM calls this with only:
      query: str (description of the current incident)
      top_k: int (optional, default 3)
    Args:
        tenant_id: UUID string of the tenant (pre-bound at registration).
        bm25_index: BM25Index instance with db_pool injected (pre-bound).
        vector_search: VectorSearch instance with clients injected (pre-bound).
        reranker: CrossEncoderReranker singleton, loaded at startup (pre-bound).
        query: Description of the current incident from the LLM.
        top_k: Number of ranked results to return. Default 3.
    Returns:
        str: Formatted text the LLM reads as a tool observation. Either:
          - Ranked past incidents with root_cause and resolution
          - "No similar past incidents found. This appears to be a novel incident."
    """
    log.info(
        "search_knowledge_base_start",
        tenant_id=tenant_id,
        query_preview=query[:80],
        top_k=top_k,
    )

    # --- Stage 1 & 2: BM25 and vector search run conceptually in sequence.
    # In a production system these would be awaited concurrently with asyncio.gather.
    # Sequential here for clarity and because each call completes in <200ms.
    bm25_results = await bm25_index.search(tenant_id, query, top_k=_RETRIEVAL_TOP_K)
    vector_results = await vector_search.search(tenant_id, query, top_k=_RETRIEVAL_TOP_K)

    # --- Check if both retrieval stages returned nothing ---
    if not bm25_results and not vector_results:
        log.info(
            "search_knowledge_base_no_results",
            tenant_id=tenant_id,
        )
        # Return a clear message so the LLM knows this is an unprecedented incident.
        # The LLM should then rely entirely on the live data tools.
        return (
            "No similar past incidents found. This appears to be a novel incident. "
            "Proceed with QueryLogs and GetDependencies to gather evidence from the live system."
        )

    # --- Stage 3: Reciprocal Rank Fusion ---
    # Merges BM25 and vector ranked lists using rank positions (not raw scores).
    # An incident appearing in both lists scores much higher than one in only one.
    fused = reciprocal_rank_fusion(bm25_results, vector_results)

    # --- Stage 4: Cross-encoder reranking ---
    # Re-scores the merged shortlist by reading query + document together.
    # More accurate than bi-encoder similarity — sees the specific query–document
    # interaction rather than comparing independent embeddings.
    reranked = reranker.rerank(query, fused, top_k=top_k)

    log.info(
        "search_knowledge_base_complete",
        tenant_id=tenant_id,
        bm25_count=len(bm25_results),
        vector_count=len(vector_results),
        reranked_count=len(reranked),
    )

    # --- Format output for the LLM ---
    # The LLM reads this as a plain text observation in the ReAct loop.
    # Formatting choices:
    #   - === header and --- dividers make the structure scannable
    #   - Explicit field labels (Service:, Root Cause:, Resolution:) so the LLM
    #     can parse even with varied whitespace
    #   - Score shown so the LLM can weight lower-confidence results appropriately
    lines = ["=== Similar Past Incidents (Hybrid RAG) ==="]

    for r in reranked:
        lines.append(f"\nRank {r.final_rank} (score: {r.rerank_score:.4f})")
        lines.append(f"Service: {r.service}")
        lines.append(f"Description: {r.description}")
        lines.append(f"Root Cause: {r.root_cause}")
        lines.append(f"Resolution: {r.resolution}")
        lines.append("---")

    # Footer summarises the retrieval pipeline for the LLM (useful context
    # when the LLM reasons about how thorough the search was).
    lines.append(
        f"\n(BM25: {len(bm25_results)} candidates, "
        f"Vector: {len(vector_results)} candidates, "
        f"Reranked to top {top_k})"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SEARCH_KNOWLEDGE_BASE_SCHEMA — OpenAI function calling schema for search_knowledge_base.
# ---------------------------------------------------------------------------

# The description tells the LLM to call this tool FIRST, before QueryLogs.
# This matches the optimal investigation strategy: check what worked before
# before diving into live data that may take many iterations to interpret.
SEARCH_KNOWLEDGE_BASE_SCHEMA: ToolSchema = {
    "name": "SearchKnowledgeBase",
    "description": (
        "Search the historical incident database for similar past problems. "
        "Uses hybrid retrieval (keyword + semantic + reranking) for high accuracy. "
        "Call this FIRST before QueryLogs — a matching past incident provides "
        "the root cause and resolution immediately. "
        "Returns top-3 most relevant past incidents with root cause and resolution."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "Describe the current problem in detail including: "
                    "the affected service, observed error messages, "
                    "any symptoms (high latency, connection failures, restarts). "
                    "More detail = more accurate retrieval."
                ),
            },
            "top_k": {
                "type": "integer",
                "description": "Number of past incidents to return. Default: 3.",
                "minimum": 1,
                "maximum": 10,
                "default": 3,
            },
        },
        # query is required — top_k has a sensible default.
        "required": ["query"],
    },
}
