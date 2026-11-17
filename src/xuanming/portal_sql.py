"""Bound SQL pool waits and discard uncertain connections on cancellation."""

import asyncio
from contextlib import asynccontextmanager

ACQUIRE_TIMEOUT = 3.0
SQL_TIMEOUT = 10.0


@asynccontextmanager
async def connection(pool):
    acquire = pool.acquire()
    async with asyncio.timeout(ACQUIRE_TIMEOUT):
        conn = await acquire.__aenter__()
    try:
        try:
            async with asyncio.timeout(SQL_TIMEOUT):
                yield conn
        except BaseException:
            # A cancelled/failed command or commit can leave unread packets or
            # an open transaction. Never put that connection back in the pool;
            # server-side disconnect rolls back any uncommitted transaction.
            conn.close()
            raise
    finally:
        await acquire.__aexit__(None, None, None)
