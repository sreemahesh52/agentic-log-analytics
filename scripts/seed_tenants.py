"""
Seed script: creates two tenants in the tenants table.
Idempotent — safe to run multiple times. Uses INSERT ... ON CONFLICT DO NOTHING.
API keys are hashed with SHA-256 before storage; raw keys never touch the DB.
"""

import hashlib
import os
import sys
from datetime import timezone, datetime

import psycopg2
import structlog
from dotenv import load_dotenv

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

log = structlog.get_logger(service="seed-tenants")

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://admin:admin@localhost:5432/loganalytics",
)

TENANTS: list[dict] = [
    {
        "name": "acme-corp",
        "api_key": "acme-api-key-2024",
        "model_tier": "premium",
        "token_budget_usd_daily": 10.0,
    },
    {
        "name": "startup-co",
        "api_key": "startup-api-key-2024",
        "model_tier": "standard",
        "token_budget_usd_daily": 3.0,
    },
]


def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def upsert_tenant(cursor: psycopg2.extensions.cursor, tenant: dict) -> bool:
    """Insert tenant row. Returns True if created, False if already existed."""
    key_hash = hash_api_key(tenant["api_key"])

    cursor.execute(
        """
        INSERT INTO tenants (name, api_key_hash, model_tier, token_budget_usd_daily)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (name) DO NOTHING
    """,
        (tenant["name"], key_hash, tenant["model_tier"], tenant["token_budget_usd_daily"]),
    )
    return cursor.rowcount == 1


def fetch_tenant(cursor: psycopg2.extensions.cursor, name: str) -> dict:
    cursor.execute(
        "SELECT tenant_id, name, model_tier FROM tenants WHERE name = %s",
        (name,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"tenant not found after upsert: {name}")
    return {"tenant_id": str(row[0]), "name": row[1], "model_tier": row[2]}


def main() -> int:
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        conn.autocommit = False
    except psycopg2.OperationalError as exc:
        log.error("database_connection_failed", error=str(exc))
        return 1

    try:
        with conn.cursor() as cur:
            for tenant in TENANTS:
                created = upsert_tenant(cur, tenant)
                fetched = fetch_tenant(cur, tenant["name"])

                if created:
                    log.info(
                        "tenant_created",
                        tenant_id=fetched["tenant_id"],
                        name=fetched["name"],
                        model_tier=fetched["model_tier"],
                    )
                else:
                    log.info(
                        "tenant_already_exists",
                        tenant_id=fetched["tenant_id"],
                        name=fetched["name"],
                    )

                print(f"{fetched['tenant_id']}  {fetched['name']}")

        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error("seed_failed", error=str(exc))
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
