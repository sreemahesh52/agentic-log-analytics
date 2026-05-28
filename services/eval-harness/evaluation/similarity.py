"""Similarity-based faithfulness evaluation strategy.
This is Tier 2 of the faithfulness pipeline — medium reliability.
When no human ground truth is available, this strategy finds the most similar
past incident in ChromaDB and uses its root_cause as a proxy ground truth.
Why is this less reliable than ground_truth?
The proxy ground truth is the root_cause of a similar-but-not-identical
past incident. If the nearest neighbour is a different class of problem
that happens to use similar words, the score will be misleading. The
0.85 similarity threshold filters out low-confidence matches.
Why 0.85 as the threshold?
0.85 is high enough to ensure the nearest incident is genuinely related
to the current one, not just sharing a few technical terms. Setting it
too low (0.5) would return misleading proxy ground truths; too high (0.95)
would rarely find a match, making this tier useless.
Returns None when:
  - context has no tenant_id
  - The ChromaDB collection does not exist or is empty
  - No past incident has similarity >= SIMILARITY_THRESHOLD
  - The LLM returns an unparseable response
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import structlog

from .base import FaithfulnessResult, FaithfulnessStrategy

if TYPE_CHECKING:
    pass

log = structlog.get_logger(__name__)

# Minimum cosine similarity between the RCA conclusion embedding and the
# nearest past incident embedding to use that incident as proxy ground truth.
# Below this threshold, the match is too uncertain to be a useful proxy.
SIMILARITY_THRESHOLD: float = 0.85

# Maximum characters from conclusion and proxy ground truth sent to LLM.
MAX_CONCLUSION_CHARS = 2000
MAX_PROXY_CHARS = 1000

# ChromaDB collection name template. One collection per tenant for isolation.
# Seeded in scripts/seed_incidents.py with text-embedding-3-small vectors.
COLLECTION_NAME_TEMPLATE = "past_incidents_{tenant_id}"


class SimilarityStrategy(FaithfulnessStrategy):
    """Faithfulness evaluation using nearest past incident as proxy ground truth.
    Dependency Inversion: openai_client, prompt_registry, and chroma_client are
    injected. This class never imports chromadb or openai directly — tests replace
    these with mocks that return deterministic embeddings and query results.
    """

    def __init__(
        self,
        openai_client: object,
        prompt_registry: object,
        chroma_client: object,
    ) -> None:
        """Inject all dependencies.
        Args:
            openai_client: AsyncOpenAI client for embeddings and completions.
            prompt_registry: PromptRegistry instance for faithfulness_v1 template.
            chroma_client: ChromaDB client for querying past_incidents collections.
        """
        self._openai_client = openai_client
        self._prompt_registry = prompt_registry
        self._chroma_client = chroma_client

    async def evaluate(
        self, rca_conclusion: str, context: dict
    ) -> FaithfulnessResult | None:
        """Find most similar past incident and use it as proxy ground truth.
        Steps:
          1. Embed the RCA conclusion using text-embedding-3-small.
          2. Query ChromaDB for the nearest past incident in this tenant's collection.
          3. If similarity >= SIMILARITY_THRESHOLD, use that incident's root_cause
             as proxy ground truth and call the LLM faithfulness evaluator.
          4. Return None on any failure so the pipeline falls through to Tier 3.
        Returns FaithfulnessResult with eval_mode='similarity' on success.
        """
        # --- Guard: tenant_id is required to scope ChromaDB collection ---
        tenant_id = context.get("tenant_id")
        if not tenant_id:
            return None

        # --- Embed the RCA conclusion for similarity search ---
        try:
            embed_response = await self._openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=rca_conclusion[:MAX_CONCLUSION_CHARS],
            )
            # .data[0].embedding is a list of 1536 floats for text-embedding-3-small.
            embedding = embed_response.data[0].embedding
        except Exception as exc:
            # OpenAI API unavailable or rate limited — cannot embed, cannot search.
            log.warning("Failed to embed conclusion for similarity eval", error=str(exc))
            return None

        # --- Query ChromaDB for the nearest past incident ---
        collection_name = COLLECTION_NAME_TEMPLATE.format(tenant_id=tenant_id)
        try:
            # get_collection raises if the collection does not exist.
            # This happens when the tenant has no seeded or learned incidents.
            collection = self._chroma_client.get_collection(collection_name)
            count = collection.count()
            if count == 0:
                # Collection exists but is empty — nothing to compare against.
                return None

            # n_results=1: we only need the single nearest neighbour.
            results = collection.query(
                query_embeddings=[embedding],
                n_results=1,
                include=["documents", "metadatas", "distances"],
            )
        except Exception as exc:
            log.warning(
                "ChromaDB query failed in similarity strategy", error=str(exc)
            )
            return None

        # --- Check if nearest neighbour is similar enough ---
        if not results["documents"][0]:
            return None

        # ChromaDB returns cosine distance, not similarity.
        # cosine_similarity = 1.0 - cosine_distance
        similarity = 1.0 - results["distances"][0][0]
        if similarity < SIMILARITY_THRESHOLD:
            # Nearest neighbour is too different to be a reliable proxy.
            log.debug(
                "Similarity below threshold — skipping",
                similarity=similarity,
                threshold=SIMILARITY_THRESHOLD,
            )
            return None

        # --- Extract proxy ground truth from the nearest incident's metadata ---
        proxy_ground_truth = results["metadatas"][0][0].get("root_cause", "")
        if not proxy_ground_truth:
            # Metadata exists but root_cause field is missing or empty.
            return None

        # --- Call the LLM faithfulness evaluator with proxy ground truth ---
        prompt = self._prompt_registry.load(
            "evaluator",
            "faithfulness_v1",
            variables={
                "ground_truth": proxy_ground_truth[:MAX_PROXY_CHARS],
                "rca_conclusion": rca_conclusion[:MAX_CONCLUSION_CHARS],
            },
        )

        try:
            response = await self._openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=200,
            )
            raw = response.choices[0].message.content
            parsed = json.loads(raw)
            return FaithfulnessResult(
                score=float(parsed["score"]),
                reasoning=parsed.get("reasoning", ""),
                eval_mode="similarity",
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError, Exception):
            # Parse failure or API error — fall through to heuristic tier.
            log.warning("Failed to parse similarity faithfulness response")
            return None

    def strategy_name(self) -> str:
        """Return the stable identifier for this evaluation strategy."""
        return "similarity"
