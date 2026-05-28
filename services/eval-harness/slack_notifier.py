"""Slack notifier for the eval harness — Observer pattern implementation.
Sends a Slack notification when a CRITICAL incident RCA achieves faithfulness
above the configured threshold. This surfaces actionable, trustworthy findings
to on-call engineers without alert fatigue from low-confidence results.
Why Observer pattern?
The eval pipeline emits notifications as a side-effect of evaluation. Using a
notifier class with a maybe_notify method keeps the notification concern
separate from the evaluation logic (Single Responsibility). The Kafka handler
calls maybe_notify as part of the post-eval fanout without knowing whether
a notification will actually be sent.
Why threshold-gated notifications?
Low-faithfulness results (score ≤ 0.7) indicate the RCA agent may have
produced unreliable output. Sending a Slack alert for a low-quality RCA
would send engineers chasing a potentially wrong root cause. The threshold
ensures only trustworthy findings reach on-call channels.
Slack webhook URL security:
  - Never logged (not even at DEBUG level)
  - Never included in structured log fields
  - The URL is injected at construction time — callers never see it
"""

from __future__ import annotations

import structlog

log = structlog.get_logger(__name__)

# Slack message colour coding matches severity.
_CRITICAL_COLOR = "#b71c1c"  # deep red — matches UI CRITICAL badge colour

# Maximum characters of root_cause included in the Slack message.
# Prevents oversized payloads for verbose RCA conclusions.
_MAX_ROOT_CAUSE_CHARS = 500


class SlackNotifier:
    """Sends Slack notifications for high-confidence CRITICAL RCA results.
    Single Responsibility: this class only decides whether to notify and
    formats the Slack payload. It has no knowledge of the evaluation pipeline
    or the Kafka consumer — it only receives the data it needs.
    Dependency Inversion: the httpx AsyncClient is injected, not created here.
    Tests can inject a mock client to verify payload structure without making
    real HTTP calls.
    """

    def __init__(
        self,
        http_client: object,
        webhook_url: str,
        faithfulness_threshold: float,
    ) -> None:
        """Inject all dependencies.
        Args:
            http_client: httpx.AsyncClient for POST requests.
            webhook_url: Slack incoming webhook URL — never logged.
            faithfulness_threshold: Minimum faithfulness score to send notification.
        """
        self._client = http_client
        # Store webhook URL in a private attribute — never logged or exposed.
        self._webhook_url = webhook_url
        self._threshold = faithfulness_threshold

    async def maybe_notify(
        self,
        tenant_id: str,
        severity: str,
        rca_id: str,
        root_cause: str,
        faithfulness_score: float,
        affected_services: list[str],
    ) -> bool:
        """Send a Slack notification if severity is CRITICAL and score passes.
        Args:
            tenant_id: Tenant UUID string — included in the message.
            severity: Incident severity — only CRITICAL triggers notification.
            rca_id: UUID of the RCA investigation — links to the UI.
            root_cause: Root cause text from the RCA result.
            faithfulness_score: Faithfulness evaluation score (0.0–1.0).
            affected_services: List of affected service names.
        Returns:
            True if a notification was sent, False if skipped or failed.
        """
        # --- Gate: only notify for CRITICAL severity ---
        if severity != "CRITICAL":
            return False

        # --- Gate: only notify when faithfulness exceeds the threshold ---
        if faithfulness_score <= self._threshold:
            log.info(
                "slack_notification_skipped_low_faithfulness",
                tenant_id=tenant_id,
                rca_id=rca_id,
                faithfulness_score=faithfulness_score,
                threshold=self._threshold,
            )
            return False

        # --- Build the Slack payload ---
        services_str = ", ".join(affected_services) if affected_services else "unknown"
        truncated_cause = root_cause[:_MAX_ROOT_CAUSE_CHARS]
        if len(root_cause) > _MAX_ROOT_CAUSE_CHARS:
            truncated_cause += "…"

        payload = {
            "attachments": [
                {
                    "color": _CRITICAL_COLOR,
                    "title": f"🚨 CRITICAL RCA Complete — {services_str}",
                    "fields": [
                        {
                            "title": "Root Cause",
                            "value": truncated_cause,
                            "short": False,
                        },
                        {
                            "title": "Faithfulness Score",
                            "value": f"{faithfulness_score:.2f}",
                            "short": True,
                        },
                        {
                            "title": "Affected Services",
                            "value": services_str,
                            "short": True,
                        },
                        {
                            "title": "RCA ID",
                            "value": rca_id,
                            "short": False,
                        },
                    ],
                    "footer": f"Tenant: {tenant_id}",
                }
            ]
        }

        # --- POST to Slack webhook ---
        # The webhook URL is never included in logs — only "slack_notified" is logged.
        try:
            response = await self._client.post(
                self._webhook_url,
                json=payload,
                # 5-second timeout — Slack is always available; long waits indicate
                # transient network issues, not permanent failures.
                timeout=5.0,
            )
            response.raise_for_status()

            log.info(
                "slack_notification_sent",
                tenant_id=tenant_id,
                rca_id=rca_id,
                severity=severity,
                faithfulness_score=faithfulness_score,
            )
            return True

        except Exception as exc:
            # Slack failure is non-critical — the evaluation result is still
            # persisted to PostgreSQL. Log the error type only, not the URL.
            log.error(
                "slack_notification_failed",
                tenant_id=tenant_id,
                rca_id=rca_id,
                error_type=type(exc).__name__,
            )
            return False
