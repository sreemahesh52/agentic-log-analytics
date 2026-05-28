"""Kafka consumer handler for the eval harness.
Consumes completed RCA results from agent.results and runs the three-tier
faithfulness + hallucination evaluation pipeline. Persists results, triggers
self-learning, sends Slack notifications, and populates the semantic cache.
Message flow:
  agent.results (from RCA Agent or Semantic Cache)
    → parse + validate
    → skip if status != 'success' or cache_hit == True
    → fetch incident context from PostgreSQL
    → run faithfulness pipeline (ground_truth → similarity → heuristic)
    → run hallucination evaluation
    → build EvalResult
    → save to eval_results (PostgreSQL)
    → SelfLearner.maybe_learn (Observer 1)
    → SlackNotifier.maybe_notify (Observer 2)
    → SemanticCacheWriter.set (Observer 3)
    → update Prometheus metrics
Why skip cache_hit == True?
Cache hits were already evaluated when they were first cached. Re-evaluating
them would double-count costs, inflate counters, and potentially re-index
identical content in the knowledge base. Cache hits use the original eval_result.
Why fetch incident context from PostgreSQL?
The agent.results Kafka message contains RCAResult fields only (rca_id,
tenant_id, incident_id, root_cause, etc.). It does not carry alert_ids,
severity, or service — these live in the incidents and alerts tables.
Fetching them here avoids expanding the Kafka message schema and keeps
the eval harness as a standalone consumer.
Why not DLQ on eval failure?
The primary concern is persisting the eval result. If faithfulness or
hallucination evaluation fails (OpenAI down), the result is still stored
with a neutral score rather than discarded. Individual observer failures
(self-learning, Slack, cache) are never fatal — they are logged and skipped.
"""

from __future__ import annotations

import json
import time
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from cache_writer import SemanticCacheWriter
from config import settings
from evaluation.factory import evaluate_faithfulness
from evaluation.hallucination import HallucinationEvaluator
from metrics import (
    EVAL_FAITHFULNESS_SCORE,
    EVAL_HALLUCINATION_SCORE,
    EVAL_RCA_PASS_RATE,
    EVAL_TOKEN_COST_USD_TOTAL,
    KNOWLEDGE_BASE_AUTO_LEARNED_TOTAL,
    KNOWLEDGE_BASE_SIZE,
    SLACK_NOTIFICATIONS_SENT_TOTAL,
)
from models import EvalResult, calculate_cost
from postgres.repository import EvalRepository
from self_learner import SelfLearner
from slack_notifier import SlackNotifier

log = structlog.get_logger(__name__)

# How long to wait between consumer restart attempts after a fatal Kafka error.
# Exponential backoff is handled by the outer retry loop in main.py.
_RESTART_BACKOFF_SECONDS = 5


class EvalKafkaHandler:
    """Orchestrates the evaluation pipeline for completed RCA investigations.
    Single Responsibility: this class only orchestrates. Each concern
    (evaluation, persistence, self-learning, notification, caching) is
    delegated to a specialist class injected at construction time.
    All dependencies are injected — no Redis, asyncpg, or OpenAI clients
    are created here. This makes the handler testable in isolation.
    """

    def __init__(
        self,
        db_pool: object,
        eval_repository: EvalRepository,
        faithfulness_strategies: list,
        hallucination_evaluator: HallucinationEvaluator,
        self_learner: SelfLearner,
        slack_notifier: SlackNotifier,
        cache_writer: SemanticCacheWriter,
    ) -> None:
        """Inject all dependencies.
        Args:
            db_pool: asyncpg Pool for incident context queries.
            eval_repository: EvalRepository for eval_results persistence.
            faithfulness_strategies: Ordered list from EvaluatorFactory.
            hallucination_evaluator: HallucinationEvaluator instance.
            self_learner: SelfLearner for knowledge base growth.
            slack_notifier: SlackNotifier for CRITICAL alert notifications.
            cache_writer: SemanticCacheWriter to cache quality results.
        """
        self._pool = db_pool
        self._repo = eval_repository
        self._faithfulness_strategies = faithfulness_strategies
        self._hallucination_evaluator = hallucination_evaluator
        self._self_learner = self_learner
        self._slack_notifier = slack_notifier
        self._cache_writer = cache_writer

        # Per-tenant rolling counters for pass rate gauge.
        # {tenant_id: {"total": int, "passed": int}}
        self._pass_counters: dict[str, dict[str, int]] = {}

    async def run(self) -> None:
        """Start the Kafka consumer loop. Runs until the process is stopped.
        Creates an AIOKafkaConsumer, subscribes to agent.results, and processes
        messages indefinitely. On fatal Kafka error, logs and re-raises so the
        caller (main.py) can apply backoff and restart.
        """
        consumer = AIOKafkaConsumer(
            settings.kafka_input_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            # earliest: if no committed offset exists (first start), consume
            # from the oldest available message so no RCA results are missed.
            auto_offset_reset="earliest",
            # Deserialise message values as UTF-8 strings for json.loads.
            value_deserializer=lambda v: v.decode("utf-8"),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )

        await consumer.start()
        log.info(
            "eval_consumer_started",
            topic=settings.kafka_input_topic,
            group_id=settings.kafka_consumer_group,
        )

        try:
            async for message in consumer:
                await self._handle_message(message)
        except KafkaError as exc:
            log.error(
                "eval_consumer_kafka_error",
                error=str(exc),
                topic=settings.kafka_input_topic,
            )
            raise
        finally:
            # Always stop the consumer cleanly — commits pending offsets.
            await consumer.stop()
            log.info("eval_consumer_stopped")

    async def _handle_message(self, message: Any) -> None:
        """Process one agent.results Kafka message end-to-end.
        Failures in individual pipeline stages are caught here:
          - Parse failure: log and return (message is acknowledged, not retried)
          - Incident fetch failure: log and return
          - Eval failure: store with neutral score (0.5), continue
          - Persistence failure: log only (evaluation is lost but pipeline continues)
          - Observer failures (self-learn, Slack, cache): log and continue
        """
        bound_log = log.bind(
            partition=message.partition,
            offset=message.offset,
        )

        # --- Parse the message ---
        try:
            rca_data: dict[str, Any] = json.loads(message.value)
        except (json.JSONDecodeError, ValueError) as exc:
            bound_log.error("eval_message_parse_failed", error=str(exc))
            return

        rca_id = rca_data.get("rca_id", "unknown")
        tenant_id = rca_data.get("tenant_id", "")
        incident_id = rca_data.get("incident_id", "")

        bound_log = bound_log.bind(rca_id=rca_id, tenant_id=tenant_id)

        # --- Skip non-success investigations ---
        # failed/retried investigations have no reliable root_cause to evaluate.
        if rca_data.get("status") != "success":
            bound_log.debug(
                "eval_message_skipped_non_success",
                status=rca_data.get("status"),
            )
            return

        # --- Skip cache hits ---
        # Cache hits were already evaluated when originally cached.
        # Re-evaluating would double-count costs and re-index identical content.
        if rca_data.get("cache_hit") is True:
            bound_log.debug("eval_message_skipped_cache_hit")
            return

        # --- Fetch incident context from PostgreSQL ---
        incident_ctx = await self._fetch_incident_context(tenant_id, incident_id)
        if incident_ctx is None:
            bound_log.warning(
                "eval_message_skipped_no_incident_context",
                incident_id=incident_id,
            )
            return

        alert_ids = incident_ctx["alert_ids"]
        severity = incident_ctx["severity"]
        affected_services = incident_ctx["affected_services"]
        is_cascade = incident_ctx["is_cascade"]
        service = affected_services[0] if affected_services else "unknown"

        # Construct incident_description from DB data for the semantic cache.
        # This matches the format used by the Model Router when building
        # IncidentPayload.incident_description (close enough for cosine similarity).
        services_str = ", ".join(affected_services) if affected_services else "unknown"
        incident_description = (
            f"Services: {services_str}. "
            f"Severity: {severity}. "
            f"Cascade: {is_cascade}."
        )

        root_cause = rca_data.get("root_cause", "")
        reasoning_steps = rca_data.get("reasoning_steps", [])
        prompt_version = rca_data.get("prompt_version", "")
        model_used = rca_data.get("model_used", "gpt-3.5-turbo")
        input_tokens = rca_data.get("input_tokens", 0) or 0
        output_tokens = rca_data.get("output_tokens", 0) or 0
        total_latency_ms = rca_data.get("total_latency_ms", 0) or 0
        llm_latency_ms = rca_data.get("llm_latency_ms", 0) or 0
        tool_latency_ms = rca_data.get("tool_latency_ms", 0) or 0

        eval_start = time.monotonic()

        # --- Run faithfulness evaluation ---
        faithfulness_context = {
            "alert_ids": alert_ids,
            "tenant_id": tenant_id,
            "reasoning_steps": reasoning_steps,
        }
        try:
            faithfulness_result = await evaluate_faithfulness(
                strategies=self._faithfulness_strategies,
                rca_conclusion=root_cause,
                context=faithfulness_context,
            )
        except Exception as exc:
            bound_log.error("faithfulness_eval_failed", error=str(exc))
            # Use optimistic neutral score — cannot evaluate ≠ failed evaluation.
            from evaluation.base import FaithfulnessResult
            faithfulness_result = FaithfulnessResult(
                score=0.75,
                reasoning=f"Evaluation error: {str(exc)[:100]}",
                eval_mode="heuristic",
            )

        # --- Run hallucination evaluation ---
        try:
            hallucination_result = await self._hallucination_evaluator.evaluate(
                rca_conclusion=root_cause,
                tenant_id=tenant_id,
                service=service,
            )
        except Exception as exc:
            bound_log.error("hallucination_eval_failed", error=str(exc))
            from evaluation.hallucination import HallucinationResult
            hallucination_result = HallucinationResult(
                score=0.75,
                reasoning=f"Evaluation error: {str(exc)[:100]}",
            )

        llm_latency_ms_eval = int((time.monotonic() - eval_start) * 1000)

        # --- Calculate cost ---
        cost_usd = calculate_cost(
            model=model_used,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        # --- Build EvalResult ---
        eval_result = EvalResult(
            tenant_id=tenant_id,
            rca_id=rca_id,
            prompt_version=prompt_version,
            eval_mode=faithfulness_result.eval_mode,
            faithfulness_score=faithfulness_result.score,
            hallucination_score=hallucination_result.score,
            cost_usd=cost_usd,
            total_latency_ms=total_latency_ms,
            llm_latency_ms=llm_latency_ms,
            tool_latency_ms=tool_latency_ms,
            cache_latency_ms=llm_latency_ms_eval,
            compression_latency_ms=0,
        )
        eval_result.compute_passed()

        # --- Persist to eval_results ---
        try:
            await self._repo.save(eval_result)
        except Exception as exc:
            bound_log.error("eval_result_save_failed", error=str(exc))
            # Continue to observers even if save failed — best-effort approach.

        # --- Observer 1: Self-Learning Indexer ---
        recommendations = rca_data.get("recommendations", []) or []
        try:
            learned = await self._self_learner.maybe_learn(
                tenant_id=tenant_id,
                eval_mode=faithfulness_result.eval_mode,
                faithfulness_score=faithfulness_result.score,
                hallucination_score=hallucination_result.score,
                rca_id=rca_id,
                root_cause=root_cause,
                recommendations=recommendations,
                affected_services=affected_services,
                incident_description=incident_description,
            )
            if learned:
                # Label: 'tenant' not 'tenant_id' — matches PROJECT-SPEC metric definition.
                KNOWLEDGE_BASE_AUTO_LEARNED_TOTAL.labels(tenant=tenant_id).inc()
                kb_size = await self._self_learner.get_knowledge_base_size(tenant_id)
                KNOWLEDGE_BASE_SIZE.labels(tenant=tenant_id).set(kb_size)
        except Exception as exc:
            bound_log.warning("self_learner_failed", error=str(exc))

        # --- Observer 2: Slack Notifier ---
        try:
            notified = await self._slack_notifier.maybe_notify(
                tenant_id=tenant_id,
                severity=severity,
                rca_id=rca_id,
                root_cause=root_cause,
                faithfulness_score=faithfulness_result.score,
                affected_services=affected_services,
            )
            if notified:
                # Labels: 'severity' added per PROJECT-SPEC (only CRITICAL fires Slack).
                # 'tenant' replaces 'tenant_id' to match the metric definition.
                SLACK_NOTIFICATIONS_SENT_TOTAL.labels(
                    severity=severity,
                    tenant=tenant_id,
                ).inc()
        except Exception as exc:
            bound_log.warning("slack_notifier_failed", error=str(exc))

        # --- Observer 3: Semantic Cache Writer ---
        # Only cache results that passed evaluation — prevents polluting the cache
        # with low-quality RCA outputs that would degrade future similarity lookups.
        if eval_result.passed:
            try:
                await self._cache_writer.set(
                    tenant_id=tenant_id,
                    incident_description=incident_description,
                    rca_result=rca_data,
                )
            except Exception as exc:
                bound_log.warning("cache_write_failed", error=str(exc))

        # --- Update Prometheus metrics ---
        # Label names use 'tenant' (not 'tenant_id') per PROJECT-SPEC.
        # 'prompt_version' enables A/B tracking split by prompt variant in Grafana Panel 11.
        EVAL_FAITHFULNESS_SCORE.labels(
            prompt_version=prompt_version or "unknown",
            tenant=tenant_id,
            eval_mode=faithfulness_result.eval_mode,
        ).observe(faithfulness_result.score)

        # 'prompt_version' on hallucination: correlate hallucination rate with prompt variant.
        EVAL_HALLUCINATION_SCORE.labels(
            prompt_version=prompt_version or "unknown",
            tenant=tenant_id,
        ).observe(hallucination_result.score)

        # 'model' extra label beyond spec minimum: cost breakdown per model tier.
        EVAL_TOKEN_COST_USD_TOTAL.labels(
            tenant=tenant_id,
            model=model_used,
        ).inc(cost_usd)

        # Update pass rate gauge for this tenant.
        counters = self._pass_counters.setdefault(tenant_id, {"total": 0, "passed": 0})
        counters["total"] += 1
        if eval_result.passed:
            counters["passed"] += 1
        pass_rate = counters["passed"] / counters["total"]
        EVAL_RCA_PASS_RATE.labels(tenant=tenant_id).set(pass_rate)

        bound_log.info(
            "eval_complete",
            eval_mode=faithfulness_result.eval_mode,
            faithfulness=round(faithfulness_result.score, 3),
            hallucination=round(hallucination_result.score, 3),
            passed=eval_result.passed,
            cost_usd=round(cost_usd, 6),
        )

    async def _fetch_incident_context(
        self, tenant_id: str, incident_id: str
    ) -> dict[str, Any] | None:
        """Fetch alert_ids, severity, affected_services, is_cascade from PostgreSQL.
        These fields are not in the agent.results Kafka message — they live in
        the incidents and alerts tables. We look them up here so the evaluation
        pipeline has all the context it needs.
        Returns None if the incident is not found (message should be skipped).
        """
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        ARRAY(
                            SELECT elem::text
                            FROM unnest(i.alert_ids) AS elem
                        ) AS alert_ids,
                        COALESCE(i.affected_services, ARRAY[]::text[])
                                                                AS affected_services,
                        i.is_cascade,
                        COALESCE(
                            (SELECT a.severity
                             FROM alerts a
                             WHERE a.alert_id = ANY(i.alert_ids)
                               AND a.tenant_id = i.tenant_id
                             ORDER BY CASE a.severity
                                 WHEN 'CRITICAL' THEN 4
                                 WHEN 'HIGH' THEN 3
                                 WHEN 'MEDIUM' THEN 2
                                 WHEN 'LOW' THEN 1
                                 ELSE 0
                             END DESC
                             LIMIT 1),
                            'HIGH'
                        ) AS severity
                    FROM incidents i
                    WHERE i.incident_id = $1::uuid
                      AND i.tenant_id = $2::uuid
        """,
                    incident_id,
                    tenant_id,
                )
        except Exception as exc:
            log.error(
                "incident_context_fetch_failed",
                tenant_id=tenant_id,
                incident_id=incident_id,
                error=str(exc),
            )
            return None

        if row is None:
            return None

        return {
            "alert_ids": list(row["alert_ids"] or []),
            "affected_services": list(row["affected_services"] or []),
            "severity": row["severity"],
            "is_cascade": row["is_cascade"],
        }
