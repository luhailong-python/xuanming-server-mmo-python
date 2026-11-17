import asyncio

import pytest

from xuanming import portal_sql
from xuanming.portal_admin_zones import _query, _retry_write


class FakeConnection:
    def __init__(self):
        self.entered = asyncio.Event()
        self.closed = False
        self.rolled_back = False
        self.committed = False

    async def begin(self): pass
    def cursor(self): return self
    async def __aenter__(self): return self
    async def __aexit__(self, *_): pass
    async def execute(self, *_):
        self.entered.set()
        await asyncio.Event().wait()
    async def rollback(self): self.rolled_back = True
    async def commit(self): self.committed = True
    def close(self): self.closed = True


class FakePool:
    def __init__(self, conn):
        self.conn, self.released = conn, False
    def acquire(self): return self
    async def __aenter__(self): return self.conn
    async def __aexit__(self, *_): self.released = True


def test_stuck_acquire_has_deadline(monkeypatch):
    class BusyPool:
        def acquire(self): return self
        async def __aenter__(self): await asyncio.Event().wait()
        async def __aexit__(self, *_): raise AssertionError("nothing acquired")

    async def run():
        with pytest.raises(TimeoutError):
            await _query(BusyPool(), "SELECT 1")

    monkeypatch.setattr(portal_sql, "ACQUIRE_TIMEOUT", .02)
    asyncio.run(run())


def test_stuck_query_discards_connection(monkeypatch):
    async def run():
        conn, pool = FakeConnection(), None
        pool = FakePool(conn)
        with pytest.raises(TimeoutError):
            await _query(pool, "SELECT 1")
        assert conn.closed and pool.released

    monkeypatch.setattr(portal_sql, "SQL_TIMEOUT", .02)
    asyncio.run(run())


def test_cancelled_transaction_closes_connection_without_committing():
    async def run():
        conn, pool = FakeConnection(), None
        pool = FakePool(conn)
        async def write(cursor): await cursor.execute("UPDATE zone_config SET version=version+1")
        task = asyncio.create_task(_retry_write(pool, write, retry=True))
        await conn.entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert conn.closed and pool.released and not conn.committed

    asyncio.run(run())
