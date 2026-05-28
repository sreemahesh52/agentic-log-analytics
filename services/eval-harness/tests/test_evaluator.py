"""
Tests for the three-tier faithfulness evaluation pipeline, hallucination
evaluator, evaluation factory, and cost model.
Testing philosophy:
  - All external dependencies (OpenAI, PostgreSQL, ChromaDB) are mocked.
  - Tests are runnable with zero external services — pure in-memory.
  - Every test asserts specific expected values, never just "no exception".
  - Each test covers a single behaviour — easy to diagnose on failure.
  - chromadb.EphemeralClient provides a real in-memory ChromaDB for
    similarity tests without a running server.
Why AsyncMock for OpenAI methods?
The OpenAI client's chat.completions.create is a coroutine — it must be
awaited. AsyncMock is a unittest.mock.Mock subclass that returns a coroutine
when called, allowing await mock to work in pytest-asyncio tests.
Why unittest.mock.MagicMock for DB pool?
asyncpg pool's context manager (async with pool.acquire as conn) requires
an async context manager. MagicMock with __aenter__/__aexit__ set to
AsyncMock handles this pattern cleanly.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import chromadb
import pytest

# Evaluation modules under test
from evaluation.base import FaithfulnessResult
from evaluation.factory import EvaluatorFactory, evaluate_faithfulness
from evaluation.ground_truth import GroundTruthStrategy
from evaluation.hallucination import HallucinationEvaluator, HallucinationResult
from evaluation.heuristic import HeuristicStrategy
from evaluation.similarity import SimilarityStrategy
from models import EvalResult, calculate_cost


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_openai() -> MagicMock:
    """OpenAI client with chat.completions.create and embeddings.create mocked.
    Returns the faithfulness_v1 JSON schema response by default.
    Individual tests override return_value for specific scenarios.
    """
    client = MagicMock()

    # Default chat completion returns a valid faithfulness score.
    # json.dumps ensures the mock returns valid parseable JSON.
    default_chat_response = MagicMock()
    default_chat_response.choices = [
        MagicMock(
            message=MagicMock(
                content=json.dumps({"score": 0.8, "reasoning": "Matches ground truth"})
            )
        )
    ]
    # AsyncMock wraps the coroutine so 'await client.chat.completions.create'
    # resolves to default_chat_response.
    client.chat.completions.create = AsyncMock(return_value=default_chat_response)

    # Default embedding response — 1536-dimensional unit vector.
    default_embed_response = MagicMock()
    default_embed_response.data = [MagicMock(embedding=[0.1] * 1536)]
    client.embeddings.create = AsyncMock(return_value=default_embed_response)

    return client


@pytest.fixture
def mock_prompt_registry() -> MagicMock:
    """PromptRegistry that returns a predictable template string."""
    registry = MagicMock()
    # load is a synchronous method — plain MagicMock is correct here.
    registry.load.return_value = "Evaluate faithfulness: {ground_truth} vs {rca_conclusion}"
    return registry


@pytest.fixture
def mock_db_pool() -> MagicMock:
    """asyncpg Pool mock supporting 'async with pool.acquire as conn:' pattern.
    asyncpg uses an async context manager for acquire. We set __aenter__ to
    return an AsyncMock connection and __aexit__ to a no-op coroutine.
    """
    pool = MagicMock()
    mock_conn = AsyncMock()
    # fetchrow returns None by default — individual tests override this.
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_conn.fetch = AsyncMock(return_value=[])
    # async context manager protocol: __aenter__ returns the connection.
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool


@pytest.fixture
def ephemeral_chroma() -> chromadb.EphemeralClient:
    """Real in-memory ChromaDB client — no server needed.
    EphemeralClient stores everything in memory and resets on garbage collection.
    This gives us a real ChromaDB implementation to test collection creation,
    querying, and distance calculations without a running server.
    """
    # chromadb.EphemeralClient creates a fresh in-memory instance per test.
    return chromadb.EphemeralClient()


@pytest.fixture
def sample_rca_conclusion() -> str:
    """Realistic RCA conclusion text for use across multiple tests."""
    return (
        "Database connection pool exhausted due to long-running queries "
        "caused by a missing index on the orders table. The payment-service "
        "experienced 47 connection pool errors between 14:22 and 14:35 UTC."
    )


@pytest.fixture
def sample_reasoning_steps() -> list[dict]:
    """Realistic reasoning step list with tool observations."""
    return [
        {
            "step_number": 1,
            "thought": "Check for similar past incidents first",
            "action": "SearchKnowledgeBase",
            "action_input": {"query": "database connection pool exhausted"},
            "observation": "Found 2 similar incidents: connection pool exhausted due to missing index",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        {
            "step_number": 2,
            "thought": "Verify with current logs",
            "action": "QueryLogs",
            "action_input": {"service": "payment-service", "level": "ERROR"},
            "observation": "47 errors: 'connection pool exhausted, max=10'",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    ]


# ---------------------------------------------------------------------------
# TestGroundTruthStrategy
# ---------------------------------------------------------------------------


class TestGroundTruthStrategy:
    """Tests for the Tier 1 faithfulness evaluation strategy."""

    @pytest.mark.asyncio
    async def test_returns_none_when_no_alert_ids(
        self, mock_openai: MagicMock, mock_prompt_registry: MagicMock, mock_db_pool: MagicMock
    ) -> None:
        """Returns None when context has no alert_ids."""
        strategy = GroundTruthStrategy(mock_openai, mock_prompt_registry, mock_db_pool)

        result = await strategy.evaluate("Some conclusion", context={})

        assert result is None
        # DB should not be queried if alert_ids is absent.
        mock_db_pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_none_when_no_ground_truth_in_db(
        self, mock_openai: MagicMock, mock_prompt_registry: MagicMock, mock_db_pool: MagicMock
    ) -> None:
        """Returns None when the DB has no ground_truth set for these alerts."""
        # fetchrow returns None — no row found in alerts table.
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value=None)

        strategy = GroundTruthStrategy(mock_openai, mock_prompt_registry, mock_db_pool)
        result = await strategy.evaluate(
            "Some conclusion",
            context={"alert_ids": ["alert-uuid-1"]},
        )

        assert result is None
        # OpenAI should not be called when no ground truth exists.
        mock_openai.chat.completions.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_faithfulness_result_when_ground_truth_set(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns FaithfulnessResult with eval_mode='ground_truth' when ground truth exists."""
        # Simulate an alert row with ground_truth set by a human operator.
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(
            return_value={"ground_truth": "Database connection pool exhausted — missing index"}
        )

        strategy = GroundTruthStrategy(mock_openai, mock_prompt_registry, mock_db_pool)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"alert_ids": ["alert-uuid-1"]},
        )

        assert result is not None
        assert result.eval_mode == "ground_truth"
        assert result.score == 0.8  # from mock_openai default response
        assert "Matches ground truth" in result.reasoning

    @pytest.mark.asyncio
    async def test_returns_none_when_llm_response_unparseable(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns None when the LLM returns non-JSON text."""
        # Simulate ground truth present in DB.
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(
            return_value={"ground_truth": "Some ground truth"}
        )
        # LLM returns free text instead of JSON — should be handled gracefully.
        mock_openai.chat.completions.create = AsyncMock(
            return_value=MagicMock(
                choices=[MagicMock(message=MagicMock(content="I think this is good"))]
            )
        )

        strategy = GroundTruthStrategy(mock_openai, mock_prompt_registry, mock_db_pool)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"alert_ids": ["alert-uuid-1"]},
        )

        # Parse failure must return None, not raise.
        assert result is None

    def test_strategy_name_is_ground_truth(
        self, mock_openai: MagicMock, mock_prompt_registry: MagicMock, mock_db_pool: MagicMock
    ) -> None:
        """strategy_name returns the stable identifier 'ground_truth'."""
        strategy = GroundTruthStrategy(mock_openai, mock_prompt_registry, mock_db_pool)
        assert strategy.strategy_name() == "ground_truth"


# ---------------------------------------------------------------------------
# TestSimilarityStrategy
# ---------------------------------------------------------------------------


class TestSimilarityStrategy:
    """Tests for the Tier 2 faithfulness evaluation strategy."""

    @pytest.mark.asyncio
    async def test_returns_none_when_collection_empty(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns None when the ChromaDB collection is empty."""
        # Create an empty collection — no past incidents seeded.
        tenant_id = "tenant-abc"
        ephemeral_chroma.create_collection(f"past_incidents_{tenant_id}")

        strategy = SimilarityStrategy(mock_openai, mock_prompt_registry, ephemeral_chroma)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"tenant_id": tenant_id},
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_tenant_id(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns None when tenant_id is missing from context."""
        strategy = SimilarityStrategy(mock_openai, mock_prompt_registry, ephemeral_chroma)
        result = await strategy.evaluate(sample_rca_conclusion, context={})
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_similarity_below_threshold(
        self,
        mock_prompt_registry: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns None when the nearest incident has similarity below 0.85."""
        tenant_id = "tenant-xyz"
        collection = ephemeral_chroma.create_collection(
            f"past_incidents_{tenant_id}",
            # Use cosine distance metric — same as production config.
            metadata={"hf:space": "cosine"},
        )
        # Add a past incident with a very different embedding (far from query).
        # Using [1.0, 0, 0, ...] vs query [0.1, 0.1, ...] gives low similarity.
        collection.add(
            ids=["incident-1"],
            embeddings=[[1.0] + [0.0] * 1535],
            documents=["Completely unrelated incident"],
            metadatas=[{"incident_id": "incident-1", "root_cause": "Unrelated", "service": "other"}],
        )

        # Mock embedding returns a vector pointing in a different direction.
        mock_openai = MagicMock()
        # Query vector orthogonal to stored vector → similarity near 0.
        mock_openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=[0.0] + [1.0] + [0.0] * 1534)])
        )

        strategy = SimilarityStrategy(mock_openai, mock_prompt_registry, ephemeral_chroma)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"tenant_id": tenant_id},
        )

        # Low similarity → returns None → pipeline falls through to heuristic.
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_result_when_high_similarity_past_incident_found(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns FaithfulnessResult with eval_mode='similarity' on high-similarity match."""
        tenant_id = "tenant-sim"
        collection = ephemeral_chroma.create_collection(f"past_incidents_{tenant_id}")

        # Identical embedding vector → cosine similarity = 1.0.
        same_vector = [0.1] * 1536
        collection.add(
            ids=["past-1"],
            embeddings=[same_vector],
            documents=["Database connection pool exhausted due to missing index"],
            metadatas=[{
                "incident_id": "past-1",
                "root_cause": "Missing index caused full table scans under load",
                "resolution": "Added index on orders.customer_id",
                "service": "payment-service",
            }],
        )

        # Mock OpenAI to return the same vector as what is in ChromaDB.
        # Same vector → cosine distance ≈ 0 → similarity ≈ 1.0 >> 0.85 threshold.
        mock_openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=same_vector)])
        )

        strategy = SimilarityStrategy(mock_openai, mock_prompt_registry, ephemeral_chroma)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"tenant_id": tenant_id},
        )

        # High similarity match → should produce a result.
        assert result is not None
        assert result.eval_mode == "similarity"
        assert 0.0 <= result.score <= 1.0

    @pytest.mark.asyncio
    async def test_returns_none_when_chromadb_unavailable(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns None gracefully when ChromaDB raises an exception."""
        # Simulate ChromaDB being unreachable.
        mock_chroma = MagicMock()
        mock_chroma.get_collection.side_effect = Exception("ChromaDB unavailable")

        strategy = SimilarityStrategy(mock_openai, mock_prompt_registry, mock_chroma)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"tenant_id": "tenant-fail"},
        )

        # Exception must be caught — returns None, not raised.
        assert result is None


# ---------------------------------------------------------------------------
# TestHeuristicStrategy
# ---------------------------------------------------------------------------


class TestHeuristicStrategy:
    """Tests for the Tier 3 faithfulness evaluation strategy (final fallback)."""

    @pytest.mark.asyncio
    async def test_always_returns_a_result_never_none(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        sample_rca_conclusion: str,
        sample_reasoning_steps: list[dict],
    ) -> None:
        """Heuristic strategy must always return FaithfulnessResult, never None."""
        strategy = HeuristicStrategy(mock_openai, mock_prompt_registry)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"reasoning_steps": sample_reasoning_steps},
        )

        assert result is not None
        assert isinstance(result, FaithfulnessResult)
        assert result.eval_mode == "heuristic"

    @pytest.mark.asyncio
    async def test_returns_low_score_when_no_observations(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns a default low score (0.3) when reasoning_steps has no observations."""
        strategy = HeuristicStrategy(mock_openai, mock_prompt_registry)
        # Empty reasoning steps — agent called no tools.
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"reasoning_steps": []},
        )

        assert result is not None
        assert result.eval_mode == "heuristic"
        # 0.75 is the NO_OBSERVATIONS_DEFAULT_SCORE constant.
        assert result.score == 0.75
        # OpenAI should NOT be called when there is nothing to evaluate against.
        mock_openai.chat.completions.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_evaluates_against_reasoning_observations(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        sample_rca_conclusion: str,
        sample_reasoning_steps: list[dict],
    ) -> None:
        """Calls LLM evaluator when observations are available."""
        strategy = HeuristicStrategy(mock_openai, mock_prompt_registry)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"reasoning_steps": sample_reasoning_steps},
        )

        assert result is not None
        # LLM was called because we had observations to evaluate against.
        mock_openai.chat.completions.create.assert_called_once()
        assert result.eval_mode == "heuristic"

    @pytest.mark.asyncio
    async def test_returns_result_when_llm_response_unparseable(
        self,
        mock_prompt_registry: MagicMock,
        sample_rca_conclusion: str,
        sample_reasoning_steps: list[dict],
    ) -> None:
        """Returns a default score when LLM returns non-JSON — never raises, never None."""
        mock_openai_failing = MagicMock()
        mock_openai_failing.chat.completions.create = AsyncMock(
            return_value=MagicMock(
                choices=[MagicMock(message=MagicMock(content="This looks correct to me!"))]
            )
        )

        strategy = HeuristicStrategy(mock_openai_failing, mock_prompt_registry)
        result = await strategy.evaluate(
            sample_rca_conclusion,
            context={"reasoning_steps": sample_reasoning_steps},
        )

        # Parse failure → default score, not None, not an exception.
        assert result is not None
        assert result.score == 0.75
        assert result.eval_mode == "heuristic"

    def test_strategy_name_is_heuristic(
        self, mock_openai: MagicMock, mock_prompt_registry: MagicMock
    ) -> None:
        """strategy_name returns the stable identifier 'heuristic'."""
        strategy = HeuristicStrategy(mock_openai, mock_prompt_registry)
        assert strategy.strategy_name() == "heuristic"


# ---------------------------------------------------------------------------
# TestEvaluationPipeline
# ---------------------------------------------------------------------------


class TestEvaluationPipeline:
    """Tests for the factory and pipeline orchestration."""

    def test_pipeline_order_is_ground_truth_similarity_heuristic(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
    ) -> None:
        """Factory creates pipeline in the exact required order."""
        pipeline = EvaluatorFactory.create_faithfulness_pipeline(
            mock_openai, mock_prompt_registry, mock_db_pool, ephemeral_chroma
        )

        assert len(pipeline) == 3
        assert pipeline[0].strategy_name() == "ground_truth"
        assert pipeline[1].strategy_name() == "similarity"
        assert pipeline[2].strategy_name() == "heuristic"

    @pytest.mark.asyncio
    async def test_uses_ground_truth_when_available(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
    ) -> None:
        """Pipeline stops at ground_truth when ground truth is set in DB."""
        # Ground truth is available in the DB.
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(
            return_value={"ground_truth": "Connection pool exhausted"}
        )

        pipeline = EvaluatorFactory.create_faithfulness_pipeline(
            mock_openai, mock_prompt_registry, mock_db_pool, ephemeral_chroma
        )
        result = await evaluate_faithfulness(
            pipeline,
            sample_rca_conclusion,
            context={"alert_ids": ["alert-1"]},
        )

        assert result.eval_mode == "ground_truth"

    @pytest.mark.asyncio
    async def test_falls_back_to_similarity_when_no_ground_truth(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
    ) -> None:
        """Pipeline falls through to similarity when ground_truth is not set."""
        # No ground truth in DB — GroundTruthStrategy will return None.
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value=None)

        # Seed a similar past incident in ChromaDB so similarity strategy can match.
        tenant_id = "tenant-fallback"
        collection = ephemeral_chroma.create_collection(f"past_incidents_{tenant_id}")
        same_vector = [0.5] * 1536
        collection.add(
            ids=["past-inc-1"],
            embeddings=[same_vector],
            documents=["Connection pool exhausted under load"],
            metadatas=[{
                "incident_id": "past-inc-1",
                "root_cause": "Missing connection pool config",
                "resolution": "Increased pool size",
                "service": "payment-service",
            }],
        )
        # Return the same vector as stored → high similarity.
        mock_openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=same_vector)])
        )

        pipeline = EvaluatorFactory.create_faithfulness_pipeline(
            mock_openai, mock_prompt_registry, mock_db_pool, ephemeral_chroma
        )
        result = await evaluate_faithfulness(
            pipeline,
            sample_rca_conclusion,
            context={"tenant_id": tenant_id, "alert_ids": ["alert-1"]},
        )

        assert result.eval_mode == "similarity"

    @pytest.mark.asyncio
    async def test_falls_back_to_heuristic_when_no_similarity_match(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
        sample_reasoning_steps: list[dict],
    ) -> None:
        """Pipeline reaches heuristic when both ground_truth and similarity fail."""
        # No ground truth in DB.
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value=None)
        # No past incidents in ChromaDB — similarity collection does not exist.

        pipeline = EvaluatorFactory.create_faithfulness_pipeline(
            mock_openai, mock_prompt_registry, mock_db_pool, ephemeral_chroma
        )
        result = await evaluate_faithfulness(
            pipeline,
            sample_rca_conclusion,
            context={
                "alert_ids": ["alert-x"],
                "tenant_id": "tenant-no-incidents",
                "reasoning_steps": sample_reasoning_steps,
            },
        )

        assert result.eval_mode == "heuristic"

    @pytest.mark.asyncio
    async def test_heuristic_is_always_final_fallback(
        self,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        ephemeral_chroma: chromadb.EphemeralClient,
        sample_rca_conclusion: str,
    ) -> None:
        """Pipeline always completes — never exhausts all strategies without a result."""
        # All upstream strategies return None (no DB data, no ChromaDB collections).
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow = AsyncMock(return_value=None)

        mock_openai = MagicMock()
        mock_openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=[0.1] * 1536)])
        )
        mock_openai.chat.completions.create = AsyncMock(
            return_value=MagicMock(
                choices=[MagicMock(message=MagicMock(content=json.dumps({"score": 0.4, "reasoning": "Low evidence"})))]
            )
        )

        pipeline = EvaluatorFactory.create_faithfulness_pipeline(
            mock_openai, mock_prompt_registry, mock_db_pool, ephemeral_chroma
        )
        result = await evaluate_faithfulness(
            pipeline,
            sample_rca_conclusion,
            context={},  # Empty context — no data for any tier
        )

        # Must always produce a result — the pipeline cannot fail.
        assert result is not None
        assert result.eval_mode == "heuristic"


# ---------------------------------------------------------------------------
# TestHallucinationEvaluator
# ---------------------------------------------------------------------------


class TestHallucinationEvaluator:
    """Tests for the independent hallucination detection evaluator."""

    @pytest.mark.asyncio
    async def test_scores_1_when_all_claims_supported(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns score=1.0 when LLM finds no hallucinated claims."""
        # Simulate logs present in DB for the service.
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(
            return_value=[
                {
                    "ts": datetime(2024, 1, 15, 14, 22, 0, tzinfo=timezone.utc),
                    "level": "ERROR",
                    "message": "connection pool exhausted, max=10",
                }
            ]
        )
        # LLM returns score=1.0 — no hallucinations detected.
        mock_openai.chat.completions.create = AsyncMock(
            return_value=MagicMock(
                choices=[MagicMock(message=MagicMock(
                    content=json.dumps({
                        "score": 1.0,
                        "hallucinated_claims": [],
                        "reasoning": "All claims supported by log evidence",
                    })
                ))]
            )
        )

        evaluator = HallucinationEvaluator(mock_openai, mock_prompt_registry, mock_db_pool)
        result = await evaluator.evaluate(sample_rca_conclusion, "tenant-1", "payment-service")

        assert result.score == 1.0
        assert result.hallucinated_claims == []

    @pytest.mark.asyncio
    async def test_scores_lower_when_unsupported_claims(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns score < 1.0 and populates hallucinated_claims list."""
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(return_value=[])  # No logs

        # LLM identifies specific hallucinated claims.
        mock_openai.chat.completions.create = AsyncMock(
            return_value=MagicMock(
                choices=[MagicMock(message=MagicMock(
                    content=json.dumps({
                        "score": 0.4,
                        "hallucinated_claims": ["47 connection pool errors between 14:22 and 14:35 UTC"],
                        "reasoning": "Time range not verifiable from available logs",
                    })
                ))]
            )
        )

        evaluator = HallucinationEvaluator(mock_openai, mock_prompt_registry, mock_db_pool)
        result = await evaluator.evaluate(sample_rca_conclusion, "tenant-1", "payment-service")

        assert result.score == 0.4
        assert len(result.hallucinated_claims) == 1
        assert "47 connection pool errors" in result.hallucinated_claims[0]

    @pytest.mark.asyncio
    async def test_handles_no_logs_gracefully(
        self,
        mock_openai: MagicMock,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Proceeds with evaluation even when no logs are available in the DB."""
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(return_value=[])  # Empty result set

        evaluator = HallucinationEvaluator(mock_openai, mock_prompt_registry, mock_db_pool)
        result = await evaluator.evaluate(sample_rca_conclusion, "tenant-1", "payment-service")

        # Evaluation must still complete — uses placeholder evidence text.
        assert result is not None
        assert isinstance(result, HallucinationResult)
        # LLM should have been called with the NO_LOGS_MESSAGE placeholder.
        mock_openai.chat.completions.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_default_score_when_llm_response_unparseable(
        self,
        mock_prompt_registry: MagicMock,
        mock_db_pool: MagicMock,
        sample_rca_conclusion: str,
    ) -> None:
        """Returns PARSE_FAILURE_DEFAULT_SCORE (0.5) when LLM response is not JSON."""
        mock_conn = mock_db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch = AsyncMock(return_value=[])

        mock_openai_broken = MagicMock()
        mock_openai_broken.chat.completions.create = AsyncMock(
            return_value=MagicMock(
                choices=[MagicMock(message=MagicMock(content="I cannot determine this"))]
            )
        )

        evaluator = HallucinationEvaluator(mock_openai_broken, mock_prompt_registry, mock_db_pool)
        result = await evaluator.evaluate(sample_rca_conclusion, "tenant-1", "payment-service")

        assert result.score == 0.75
        assert "Failed to parse" in result.reasoning


# ---------------------------------------------------------------------------
# TestCostCalculation
# ---------------------------------------------------------------------------


class TestCostCalculation:
    """Tests for the token cost calculation function."""

    def test_gpt4_cost_higher_than_gpt35_for_same_tokens(self) -> None:
        """GPT-4 must always cost more than GPT-3.5 for the same token counts."""
        gpt4_cost = calculate_cost("gpt-4-turbo", input_tokens=1000, output_tokens=500)
        gpt35_cost = calculate_cost("gpt-3.5-turbo", input_tokens=1000, output_tokens=500)

        assert gpt4_cost > gpt35_cost

    def test_unknown_model_defaults_to_gpt35_pricing(self) -> None:
        """Unknown model names fall back to gpt-3.5-turbo pricing."""
        # 'gpt-5-hypothetical' is not in MODEL_PRICING.
        unknown_cost = calculate_cost("gpt-5-hypothetical", 1000, 500)
        gpt35_cost = calculate_cost("gpt-3.5-turbo", 1000, 500)

        # Unknown model should use gpt-3.5-turbo pricing exactly.
        assert unknown_cost == pytest.approx(gpt35_cost)

    def test_cost_formula_correct_for_gpt4(self) -> None:
        """Verifies the cost formula: (input*rate_in + output*rate_out) / 1000."""
        # gpt-4-turbo: input=$0.01/1K, output=$0.03/1K
        # 1000 input + 500 output = (1000*0.01 + 500*0.03) / 1000
        #                          = (10 + 15) / 1000 = 0.025
        cost = calculate_cost("gpt-4-turbo", input_tokens=1000, output_tokens=500)
        assert cost == pytest.approx(0.025, rel=1e-6)

    def test_cost_formula_correct_for_gpt35(self) -> None:
        """Verifies the cost formula for gpt-3.5-turbo."""
        # gpt-3.5-turbo: input=$0.001/1K, output=$0.002/1K
        # 1000 input + 500 output = (1000*0.001 + 500*0.002) / 1000
        #                          = (1 + 1) / 1000 = 0.002
        cost = calculate_cost("gpt-3.5-turbo", input_tokens=1000, output_tokens=500)
        assert cost == pytest.approx(0.002, rel=1e-6)

    def test_zero_tokens_produces_zero_cost(self) -> None:
        """Zero token counts always produce $0.00 cost."""
        cost = calculate_cost("gpt-4-turbo", input_tokens=0, output_tokens=0)
        assert cost == 0.0


# ---------------------------------------------------------------------------
# TestEvalResult
# ---------------------------------------------------------------------------


class TestEvalResult:
    """Tests for the EvalResult dataclass and its compute_passed method."""

    def test_compute_passed_true_when_both_scores_above_threshold(self) -> None:
        """passed=True when faithfulness > 0.7 AND hallucination > 0.7."""
        result = EvalResult()
        result.faithfulness_score = 0.8
        result.hallucination_score = 0.8
        result.compute_passed()

        assert result.passed is True

    def test_compute_passed_false_when_faithfulness_at_threshold(self) -> None:
        """passed=False when faithfulness == 0.7 (must be strictly > 0.7)."""
        result = EvalResult()
        result.faithfulness_score = 0.7  # Not > 0.7, exactly at threshold.
        result.hallucination_score = 0.9
        result.compute_passed()

        assert result.passed is False

    def test_compute_passed_false_when_hallucination_below_threshold(self) -> None:
        """passed=False when hallucination score is too low."""
        result = EvalResult()
        result.faithfulness_score = 0.9
        result.hallucination_score = 0.6  # Below 0.7 threshold.
        result.compute_passed()

        assert result.passed is False

    def test_compute_passed_false_when_both_below_threshold(self) -> None:
        """passed=False when both scores fail."""
        result = EvalResult()
        result.faithfulness_score = 0.5
        result.hallucination_score = 0.5
        result.compute_passed()

        assert result.passed is False

    def test_evaluated_at_is_utc_iso8601(self) -> None:
        """evaluated_at field uses UTC timezone-aware ISO 8601 format."""
        result = EvalResult()
        # Parse the timestamp — fromisoformat handles the +00:00 offset.
        dt = datetime.fromisoformat(result.evaluated_at)
        # timezone.utc check: the parsed datetime must be UTC-aware.
        assert dt.tzinfo is not None
        # UTC offset must be zero (either +00:00 or the utc singleton).
        assert dt.utcoffset().total_seconds() == 0

    def test_eval_id_is_unique_per_instance(self) -> None:
        """Each EvalResult gets a unique UUID eval_id."""
        r1 = EvalResult()
        r2 = EvalResult()
        assert r1.eval_id != r2.eval_id

    def test_default_passed_is_false(self) -> None:
        """passed defaults to False — must be explicitly set by compute_passed."""
        result = EvalResult()
        assert result.passed is False
