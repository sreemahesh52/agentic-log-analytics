"""
Seed script: inserts 20 past incidents per tenant into PostgreSQL and embeds
each one into the tenant-scoped ChromaDB collection past_incidents_{tenant_id}.
Idempotent — if a tenant already has >= 20 rows in past_incidents, that tenant
is skipped entirely. Embeddings use text-embedding-3-small via OpenAI.
"""

import os
import sys
import uuid
from pathlib import Path
from typing import Any

import chromadb
import psycopg2
import structlog
from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

# Probe both conventional .env locations so the script works whether it is run
# from the repo root, from scripts/, or from infra/.
# load_dotenv silently skips a path that doesn't exist and never overwrites
# variables already set in the shell environment.
_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_REPO_ROOT / "infra" / ".env")  # docker-compose convention
load_dotenv(_REPO_ROOT / ".env")            # repo-root fallback

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

log = structlog.get_logger(service="seed-incidents")

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://admin:admin@localhost:5432/loganalytics",
)
CHROMADB_URL = os.getenv("CHROMADB_URL", "http://localhost:8081")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
EMBEDDING_MODEL = "text-embedding-3-small"
SEED_COUNT_THRESHOLD = 20

INCIDENTS: list[dict[str, Any]] = [
    {
        "service": "payment-service",
        "description": (
            "The payment service began throwing 'connection pool exhausted' errors "
            "during peak checkout traffic. All 20 database connections were held by "
            "long-running transactions that were waiting on row-level locks. New "
            "requests queued until the pool timeout fired, resulting in 503 responses."
        ),
        "root_cause": (
            "A missing database index on the orders.customer_id column caused full "
            "table scans under load, holding locks far longer than expected and "
            "exhausting the connection pool."
        ),
        "resolution": (
            "Added a B-tree index on orders.customer_id and increased pool max_size "
            "from 20 to 50 as a temporary buffer while the slow queries were tuned."
        ),
        "tags": ["database", "connection-pool", "performance", "locks", "postgresql"],
    },
    {
        "service": "auth-service",
        "description": (
            "Redis reported OOM and began evicting cached session tokens using the "
            "allkeys-lru policy. This caused a cache stampede where every evicted "
            "session triggered a synchronous database lookup, overwhelming PostgreSQL "
            "with authentication queries."
        ),
        "root_cause": (
            "Session tokens were stored without TTL, so memory grew unbounded until "
            "Redis hit its maxmemory limit and started evicting entries."
        ),
        "resolution": (
            "Added a 24-hour TTL on all session cache keys and increased the Redis "
            "maxmemory allocation. Introduced a probabilistic early expiration "
            "strategy to prevent future stampedes."
        ),
        "tags": ["redis", "cache-stampede", "oom", "session", "ttl"],
    },
    {
        "service": "order-service",
        "description": (
            "Kafka consumer lag on the orders topic grew to 500,000 messages over "
            "30 minutes. Order confirmation emails were delayed by up to 45 minutes "
            "and real-time inventory deductions fell behind, causing overselling on "
            "three product lines."
        ),
        "root_cause": (
            "A new deserialiser introduced in the last deployment threw an unhandled "
            "exception on malformed order payloads, causing the consumer to retry "
            "indefinitely on the same poisoned message."
        ),
        "resolution": (
            "Added dead-letter routing for schema validation errors and deployed a "
            "schema-compatible fix. Replayed DLQ messages after validating them."
        ),
        "tags": ["kafka", "consumer-lag", "dlq", "deserialization", "order-processing"],
    },
    {
        "service": "gateway-service",
        "description": (
            "The API gateway began returning HTTP 503 to all clients for 8 minutes. "
            "Downstream services were timing out after 30 seconds, filling the "
            "gateway's connection pool. Circuit breakers did not open because the "
            "timeout threshold was set above the observed p99 latency."
        ),
        "root_cause": (
            "The inventory service experienced a database failover that caused "
            "5-second query delays. The gateway's downstream timeout was 30 seconds, "
            "so connections piled up before circuit breakers activated."
        ),
        "resolution": (
            "Reduced downstream timeouts from 30s to 5s and lowered the circuit "
            "breaker threshold to open after 10 consecutive failures. Added timeout "
            "hedging for inventory calls."
        ),
        "tags": ["circuit-breaker", "timeout", "503", "cascade", "gateway"],
    },
    {
        "service": "notification-service",
        "description": (
            "The notification service pod was OOM-killed and restarted four times "
            "over two hours. Each restart caused a brief gap in email and push "
            "notification delivery. Heap usage grew steadily from 256 MB to 1.2 GB "
            "over 90 minutes before the kernel killed the process."
        ),
        "root_cause": (
            "An event listener was registered inside a loop body, creating a new "
            "listener on every iteration without deregistering the previous one. "
            "The accumulated listeners held references to template objects, "
            "preventing garbage collection."
        ),
        "resolution": (
            "Moved listener registration outside the loop, added deregistration on "
            "cleanup, and added a memory_rss Prometheus metric with an alert at "
            "80% of the container limit."
        ),
        "tags": ["memory-leak", "oom-kill", "listeners", "garbage-collection", "heap"],
    },
    {
        "service": "inventory-service",
        "description": (
            "Inventory service write latency spiked from 5ms to 4 seconds over "
            "6 hours. Log rotation had been disabled after a misconfigured cron job "
            "failed silently. Application logs filled the disk partition, causing "
            "every fsync to block waiting for space."
        ),
        "root_cause": (
            "The log rotation cron entry used a relative path that broke when the "
            "working directory changed after a deployment. Logs accumulated on the "
            "data partition that was also used by PostgreSQL WAL files."
        ),
        "resolution": (
            "Restored log rotation with absolute paths, moved logs to a separate "
            "mount point, and added a disk-usage alert at 80% capacity."
        ),
        "tags": ["disk-io", "log-rotation", "fsync", "cron", "postgresql-wal"],
    },
    {
        "service": "auth-service",
        "description": (
            "TLS handshakes to the auth service began failing at 02:14 UTC, causing "
            "all downstream services that call the auth endpoint to log certificate "
            "verification errors. User logins failed entirely for 18 minutes until "
            "an on-call engineer renewed the certificate manually."
        ),
        "root_cause": (
            "The TLS certificate for the auth service endpoint expired. The "
            "automated renewal process had failed 30 days earlier due to a DNS "
            "propagation issue and the failure alert was not routed to an active "
            "on-call channel."
        ),
        "resolution": (
            "Renewed the certificate and fixed the DNS record. Implemented a "
            "certificate expiry Prometheus metric and alert firing 30 days before "
            "expiry. Verified automated renewal end-to-end."
        ),
        "tags": ["tls", "certificate-expiry", "auth", "dns", "monitoring"],
    },
    {
        "service": "payment-service",
        "description": (
            "P99 latency on payment processing endpoints grew from 120ms to 8 seconds "
            "after the Black Friday traffic surge began. The service remained "
            "functional but SLA targets were breached. Database CPU hit 95% and "
            "query plan caches were being evicted."
        ),
        "root_cause": (
            "A frequently executed query on the transactions table lacked an index "
            "on the high-cardinality created_at column. At low traffic the query "
            "planner chose an index scan; at high traffic it switched to a sequential "
            "scan, causing a 60x latency increase."
        ),
        "resolution": (
            "Created a BRIN index on transactions.created_at and a composite index "
            "on (tenant_id, created_at DESC). Added EXPLAIN ANALYZE to the CI "
            "pipeline for queries touching tables over 1M rows."
        ),
        "tags": ["slow-query", "missing-index", "postgresql", "query-planner", "latency"],
    },
    {
        "service": "order-service",
        "description": (
            "Order processing requests originating from the EU region began timing "
            "out intermittently with a 30-second timeout at a rate of about 2% of "
            "requests. US region requests succeeded normally. The failures were "
            "scattered across all order types with no clear pattern by order size "
            "or product category."
        ),
        "root_cause": (
            "A routing misconfiguration caused EU-origin requests to be sent to the "
            "US-east availability zone, adding 180ms of cross-zone latency per hop. "
            "With 6 synchronous service calls per order, total latency exceeded the "
            "timeout for 2% of requests during congestion periods."
        ),
        "resolution": (
            "Fixed the load balancer affinity rules to keep EU traffic within the "
            "EU-west zone. Reduced the number of synchronous hops using an async "
            "event pattern for non-critical order enrichment steps."
        ),
        "tags": ["network-timeout", "availability-zone", "latency", "routing", "cross-region"],
    },
    {
        "service": "gateway-service",
        "description": (
            "An automated deployment pipeline rolled back version 2.4.1 of the "
            "gateway service after detecting a 12% 5xx error rate, up from a "
            "baseline of 0.1%. The rollback restored service in 4 minutes. Post "
            "mortems confirmed the error rate began exactly at the deployment "
            "completion timestamp."
        ),
        "root_cause": (
            "Version 2.4.1 introduced a middleware registration order bug that "
            "caused the authentication middleware to run after the rate limiter, "
            "allowing unauthenticated requests to consume rate limit quota and "
            "then fail with 500 instead of 401."
        ),
        "resolution": (
            "Rolled back to 2.4.0. Fixed middleware ordering in 2.4.2 and added "
            "an integration test that verifies unauthenticated requests return 401 "
            "before hitting any business logic."
        ),
        "tags": ["deployment", "rollback", "5xx-rate", "middleware", "integration-test"],
    },
    {
        "service": "auth-service",
        "description": (
            "Customer support reported a surge of 'too many requests' complaints "
            "from legitimate enterprise users during a product launch event. "
            "Investigation showed the rate limiter was applying the free-tier "
            "limit of 100 req/min to all users regardless of their plan."
        ),
        "root_cause": (
            "A configuration deployment overwrote the rate limit tier mappings "
            "with default values. The deployment validation step only checked that "
            "the config file was valid YAML, not that the tier values matched the "
            "expected schema."
        ),
        "resolution": (
            "Restored the correct tier mappings from the previous config version. "
            "Added a JSON Schema validation step to the config deployment pipeline "
            "and a canary check that verifies enterprise accounts receive the "
            "correct limit after any config change."
        ),
        "tags": ["rate-limiter", "misconfiguration", "config-deployment", "enterprise", "canary"],
    },
    {
        "service": "inventory-service",
        "description": (
            "Inventory update requests began failing with 'deadlock detected' "
            "PostgreSQL errors at a rate of 5% during flash sale events. Failed "
            "transactions were retried by clients, worsening lock contention and "
            "driving the deadlock rate to 18% at peak."
        ),
        "root_cause": (
            "Two concurrent code paths updated inventory rows in opposite order: "
            "the checkout service locked product then warehouse, while the restocking "
            "service locked warehouse then product. Under concurrent load, each "
            "transaction waited on the other, triggering PostgreSQL deadlock "
            "detection."
        ),
        "resolution": (
            "Standardised on a canonical lock ordering: always lock rows by "
            "ascending primary key within a transaction. Added a deadlock rate "
            "Prometheus alert and a load test that exercises concurrent inventory "
            "updates to catch regressions."
        ),
        "tags": ["deadlock", "transaction", "lock-ordering", "postgresql", "concurrency"],
    },
    {
        "service": "order-service",
        "description": (
            "A Kafka consumer group rebalance took 3 minutes to complete after "
            "a rolling deployment added a new consumer pod. During rebalancing all "
            "partition assignments were revoked, halting order processing entirely. "
            "A backlog of 80,000 messages accumulated and took 12 minutes to drain."
        ),
        "root_cause": (
            "The consumer group used the default eager rebalance protocol, which "
            "revokes all partition assignments from all members before redistributing "
            "them. With 12 partitions and 4 consumers the rebalance round-trip took "
            "3 minutes due to the session timeout setting."
        ),
        "resolution": (
            "Migrated to the incremental cooperative rebalance protocol, which only "
            "revokes and reassigns the partitions that need to move. Reduced the "
            "session timeout from 180s to 30s. Backlog drain time fell from 12 "
            "minutes to under 2 minutes in subsequent deployments."
        ),
        "tags": ["kafka", "rebalance", "consumer-group", "cooperative-rebalance", "backlog"],
    },
    {
        "service": "user-service",
        "description": (
            "After a planned JWT signing key rotation, approximately 8% of active "
            "user sessions began receiving 401 responses. Affected users were "
            "required to log in again. The issue self-resolved after 15 minutes "
            "as cached old-key validation results expired."
        ),
        "root_cause": (
            "The JWT validation service cached the public key in memory with a "
            "60-minute TTL. During key rotation the new key was deployed to the "
            "signing service before the validation service had fetched it, creating "
            "a 15-minute window where tokens signed with the new key were rejected."
        ),
        "resolution": (
            "Implemented a key rotation protocol: serve both old and new keys from "
            "a JWKS endpoint during a 30-minute overlap window. Updated the "
            "validation service to accept tokens from any key in the active key set."
        ),
        "tags": ["jwt", "key-rotation", "auth", "jwks", "session"],
    },
    {
        "service": "payment-service",
        "description": (
            "Product prices displayed on the checkout page were stale for some "
            "users for up to 4 hours after a promotional price update. Affected "
            "users saw old prices but were charged the correct amount, leading to "
            "customer complaints about price discrepancy."
        ),
        "root_cause": (
            "A CDN cache node was serving stale responses because the Cache-Control "
            "header on the pricing API response was missing a surrogate-key tag. "
            "When prices were updated the purge call only invalidated the primary "
            "edge but not the shield layer, which continued serving cached data."
        ),
        "resolution": (
            "Added surrogate-key headers to all pricing API responses and updated "
            "the invalidation logic to purge both edge and shield layers. Added a "
            "cache-age monitoring alert that fires when any pricing endpoint "
            "returns content older than 10 minutes."
        ),
        "tags": ["cdn", "cache-poisoning", "surrogate-key", "pricing", "cache-invalidation"],
    },
    {
        "service": "notification-service",
        "description": (
            "Memory usage in the notification service grew by approximately 15 MB "
            "per hour, even under constant load. After 48 hours the service was "
            "consuming 2.1 GB and response times degraded. The growth was linear "
            "and correlated with the number of requests processed, not with time "
            "since startup."
        ),
        "root_cause": (
            "A goroutine was started per incoming webhook event but was only "
            "terminated on successful delivery. Events that received a 5xx response "
            "were retried in a loop with backoff, but the goroutine was never "
            "cancelled on context cancellation, leaving zombie goroutines that "
            "held references to the response body reader."
        ),
        "resolution": (
            "Propagated context cancellation into all retry loops and added "
            "explicit goroutine tracking with a prometheus gauge. Added a "
            "goroutine count alert firing when count exceeds 10,000."
        ),
        "tags": ["goroutine-leak", "memory-growth", "context-cancellation", "webhook", "go"],
    },
    {
        "service": "inventory-service",
        "description": (
            "Inventory availability checks began returning stale data showing items "
            "as in-stock when they had sold out up to 90 seconds earlier. This "
            "caused overselling on high-demand items during a flash sale. The "
            "issue only affected read-heavy API endpoints, not write endpoints."
        ),
        "root_cause": (
            "The read replica used for inventory queries had fallen behind the "
            "primary by up to 90 seconds due to a high write rate during the sale. "
            "The application had no mechanism to detect replica lag and always "
            "routed reads to the replica regardless of staleness."
        ),
        "resolution": (
            "Added replica lag monitoring via pg_stat_replication. Implemented a "
            "lag threshold check: if replica lag exceeds 5 seconds, route reads to "
            "the primary. Accepted the increased primary load as the tradeoff."
        ),
        "tags": ["read-replica", "replication-lag", "stale-reads", "postgresql", "flash-sale"],
    },
    {
        "service": "gateway-service",
        "description": (
            "Approximately 0.5% of API requests were failing with connection reset "
            "errors at the client. Server-side logs showed successful response "
            "dispatch, but clients received truncated responses. The issue only "
            "affected requests with response bodies larger than 16 KB and only "
            "over HTTP/2 connections."
        ),
        "root_cause": (
            "A load balancer firmware bug incorrectly calculated HTTP/2 flow control "
            "window sizes for streams multiplexed on connections that had been idle "
            "for more than 60 seconds. Frames were dropped when the calculated "
            "window size underflowed."
        ),
        "resolution": (
            "Applied the load balancer firmware patch provided by the vendor. "
            "As an interim mitigation, set HTTP/2 max idle connection age to 30 "
            "seconds to prevent connections from reaching the buggy state."
        ),
        "tags": ["http2", "multiplexing", "flow-control", "load-balancer", "connection-reset"],
    },
    {
        "service": "user-service",
        "description": (
            "During a traffic spike, the user service began logging 'too many open "
            "files' errors and refusing new connections. Existing connections were "
            "served normally but no new TCP connections could be accepted. The "
            "issue resolved after a pod restart but recurred under the next traffic "
            "spike 4 hours later."
        ),
        "root_cause": (
            "The default OS file descriptor limit of 1024 was applied to the "
            "container. Each database connection, Kafka connection, and HTTP keep-alive "
            "connection consumes one file descriptor. Under peak load the service "
            "opened more than 1024 simultaneous file handles."
        ),
        "resolution": (
            "Set ulimit nofile to 65535 in the container spec. Added a prometheus "
            "metric tracking open file descriptor count with an alert at 80% of the "
            "limit. Documented the required ulimit setting in the service runbook."
        ),
        "tags": ["file-descriptors", "ulimit", "connection-limit", "container", "tcp"],
    },
    {
        "service": "payment-service",
        "description": (
            "Post-sale reconciliation detected that 23 orders had been charged "
            "twice during a 10-minute window when the payment service was under "
            "high load. Each duplicate charge corresponded to a payment that the "
            "client retried after a timeout, and both the original and the retry "
            "were processed successfully."
        ),
        "root_cause": (
            "The distributed lock used to prevent duplicate payment processing had "
            "a race condition: the lock was acquired after a uniqueness check but "
            "before the payment record was written. Under concurrent load two "
            "threads could both pass the uniqueness check before either wrote the "
            "record, causing both to proceed to charge the card."
        ),
        "resolution": (
            "Replaced the check-then-lock pattern with a database-level unique "
            "constraint on (tenant_id, idempotency_key). The constraint guarantees "
            "atomicity without relying on application-level locking. Added "
            "idempotency key requirements to all payment API clients."
        ),
        "tags": ["race-condition", "distributed-lock", "idempotency", "double-charge", "payment"],
    },
]


def get_tenant_ids(cursor: psycopg2.extensions.cursor) -> dict[str, str]:
    cursor.execute("SELECT tenant_id, name FROM tenants WHERE name IN %s", (("acme-corp", "startup-co"),))
    rows = cursor.fetchall()
    return {row[1]: str(row[0]) for row in rows}


def already_seeded(cursor: psycopg2.extensions.cursor, tenant_id: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM past_incidents WHERE tenant_id = %s",
        (tenant_id,),
    )
    row = cursor.fetchone()
    return row is not None and row[0] >= SEED_COUNT_THRESHOLD


def insert_incident(
    cursor: psycopg2.extensions.cursor,
    tenant_id: str,
    incident: dict[str, Any],
) -> str:
    incident_id = str(uuid.uuid4())
    cursor.execute(
        """
        INSERT INTO past_incidents
            (incident_id, tenant_id, source, service, description, root_cause, resolution, tags)
        VALUES (%s, %s, 'seed', %s, %s, %s, %s, %s)
    """,
        (
            incident_id,
            tenant_id,
            incident["service"],
            incident["description"],
            incident["root_cause"],
            incident["resolution"],
            incident["tags"],
        ),
    )
    return incident_id


def embed_text(client: OpenAI, text: str) -> list[float]:
    response = client.embeddings.create(input=text, model=EMBEDDING_MODEL)
    return response.data[0].embedding


def seed_tenant(
    cursor: psycopg2.extensions.cursor,
    openai_client: OpenAI | None,
    chroma_client: chromadb.HttpClient,
    tenant_id: str,
    tenant_name: str,
) -> int:
    collection_name = f"past_incidents_{tenant_id}"
    collection = chroma_client.get_or_create_collection(collection_name)

    seeded_count = 0
    for incident in INCIDENTS:
        incident_id = insert_incident(cursor, tenant_id, incident)

        if openai_client is None:
            # No API key — PostgreSQL row is still inserted for BM25 keyword search
            # and KB size gauge. ChromaDB embedding is skipped; the similarity
            # evaluation strategy will fall through to the heuristic fallback.
            seeded_count += 1
            continue

        embed_input = incident["description"] + " " + incident["root_cause"]
        try:
            embedding = embed_text(openai_client, embed_input)
        except OpenAIError as exc:
            log.error(
                "embedding_failed",
                tenant=tenant_name,
                service=incident["service"],
                incident_id=incident_id,
                error=str(exc),
            )
            seeded_count += 1
            continue

        collection.add(
            ids=[incident_id],
            embeddings=[embedding],
            metadatas=[
                {
                    "incident_id": incident_id,
                    "service": incident["service"],
                    "tenant_id": tenant_id,
                    "source": "seed",
                    "root_cause": incident["root_cause"],
                    "resolution": incident["resolution"],
                }
            ],
        )
        log.info(
            "incident_embedded",
            tenant=tenant_name,
            service=incident["service"],
            incident_id=incident_id,
        )
        seeded_count += 1

    return seeded_count


def main() -> int:
    if not OPENAI_API_KEY:
        log.warning(
            "openai_key_missing",
            msg=(
                "OPENAI_API_KEY not set — incidents will be seeded into PostgreSQL "
                "only (BM25 search + KB size gauge). ChromaDB embeddings and the "
                "similarity evaluation strategy require a valid key."
            ),
        )

    try:
        conn = psycopg2.connect(POSTGRES_URL)
        conn.autocommit = False
    except psycopg2.OperationalError as exc:
        log.error("database_connection_failed", error=str(exc))
        return 1

    try:
        chroma_client = chromadb.HttpClient(
            host=CHROMADB_URL.replace("http://", "").split(":")[0],
            port=int(CHROMADB_URL.split(":")[-1]),
        )
    except Exception as exc:
        log.error("chromadb_connection_failed", error=str(exc))
        conn.close()
        return 1

    openai_client: OpenAI | None = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

    results: dict[str, int] = {}
    try:
        with conn.cursor() as cur:
            tenant_ids = get_tenant_ids(cur)

            for tenant_name, tenant_id in tenant_ids.items():
                if already_seeded(cur, tenant_id):
                    log.info("tenant_already_seeded", tenant=tenant_name, tenant_id=tenant_id)
                    cur.execute(
                        "SELECT COUNT(*) FROM past_incidents WHERE tenant_id = %s",
                        (tenant_id,),
                    )
                    row = cur.fetchone()
                    results[tenant_name] = row[0] if row else 0
                    continue

                count = seed_tenant(cur, openai_client, chroma_client, tenant_id, tenant_name)
                results[tenant_name] = count
                log.info("tenant_seeded", tenant=tenant_name, count=count)

        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error("seed_failed", error=str(exc))
        return 1
    finally:
        conn.close()

    acme_count = results.get("acme-corp", 0)
    startup_count = results.get("startup-co", 0)
    mode = "PostgreSQL + ChromaDB" if OPENAI_API_KEY else "PostgreSQL only (no OPENAI_API_KEY)"
    print(
        f"Seeded {acme_count} incidents for acme-corp, "
        f"{startup_count} incidents for startup-co "
        f"({mode})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
