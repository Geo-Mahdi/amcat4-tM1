from contextlib import asynccontextmanager
from typing import AsyncGenerator

from psycopg import AsyncConnection
from psycopg_pool import PoolTimeout

from amcat4.connections import db

# How long to wait for a spare connection before running the statement after the other one instead
SPARE_TIMEOUT = 0.2


@asynccontextmanager
async def connection() -> AsyncGenerator[AsyncConnection, None]:
    """
    Get a connection from the pool. Connections are in autocommit mode; use `async with conn.transaction()`
    for statements that need to be atomic.
    """
    async with db().connection() as conn:
        yield conn


@asynccontextmanager
async def spare_connection() -> AsyncGenerator[AsyncConnection | None, None]:
    """
    A second connection, to run a statement next to one that is already running (e.g. the count next to the page of
    a search), or None if the pool has none to spare. The caller already holds a connection, so asking for a second
    one could wait for itself on a busy pool: we only take one when at least two are free (idle, or room to open
    one), nobody is waiting, and only for SPARE_TIMEOUT. Otherwise the caller runs its statements one after the other.
    """
    pool = db()
    stats = pool.get_stats()
    free = stats.get("pool_available", 0) + pool.max_size - stats.get("pool_size", 0)
    conn = None
    if free >= 2 and not stats.get("requests_waiting", 0):
        try:
            conn = await pool.getconn(timeout=SPARE_TIMEOUT)
        except PoolTimeout:
            conn = None
    try:
        yield conn
    finally:
        if conn is not None:
            await pool.putconn(conn)


async def execute(query, params=None) -> int:
    """Execute a statement on a pooled connection, returning the number of affected rows"""
    async with connection() as conn:
        cur = await conn.execute(query, params)
        return cur.rowcount


async def fetch_all(query, params=None) -> list[dict]:
    async with connection() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchall()  # type: ignore[return-value]


async def fetch_one(query, params=None) -> dict | None:
    async with connection() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchone()  # type: ignore[return-value]
