# --- Shared resource providers ---
# These functions are FastAPI dependencies injected via Depends.
# They read from app.state, which is populated by the lifespan context manager
# in main.py at startup. This ensures the pool and client are created once
# and reused across all requests — never instantiated per-request.

import asyncpg
import httpx
from aiokafka import AIOKafkaProducer
from fastapi import Request


async def get_db_pool(request: Request) -> asyncpg.Pool:
    """Return the shared asyncpg connection pool from app.state.
    The pool was created once at startup with server_settings={"timezone":"UTC"}.
    Injecting it here (rather than opening a new connection) means every request
 shares the same bounded pool of 2–10 connections."""
    # request.app.state holds objects stored during the lifespan startup block.
    # This is FastAPI's recommended way to share singletons across requests.
    return request.app.state.db_pool


async def get_http_client(request: Request) -> httpx.AsyncClient:
    """Return the shared httpx AsyncClient from app.state.
    A single AsyncClient reuses TCP connections (HTTP keep-alive) across
    requests to the same upstream. Creating a new client per request would
 open a fresh TCP connection every time — wasteful and slower."""
    return request.app.state.http_client


async def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    """Return the shared AIOKafkaProducer from app.state.
    Used by the investigations/trigger endpoint to publish to incidents.ready.
    The producer is started once in the lifespan context manager and shared
 across all requests — aiokafka producers are safe for concurrent use."""
    return request.app.state.kafka_producer
