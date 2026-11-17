import asyncio
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("tidb_qualify_test", Path(__file__).with_name("qualify.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_actual_lock_wait_is_required():
    class Root:
        def __init__(self): self.calls = 0
        def cursor(self): return self
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def execute(self, sql, args):
            assert "DATA_LOCK_WAITS" in sql and args == ("%isolated_schema%",)
            self.calls += 1
        async def fetchone(self): return (1 if self.calls == 2 else 0,)

    async def exercise():
        root = Root()
        blocker = asyncio.create_task(asyncio.Event().wait())
        try:
            await module._observe_lock_wait(root, "isolated_schema", blocker, timeout=1)
            assert root.calls == 2
            blocker.cancel()
            await asyncio.gather(blocker, return_exceptions=True)
            finished = asyncio.create_task(asyncio.sleep(0))
            await finished
            with pytest.raises(RuntimeError, match="completed before"):
                await module._observe_lock_wait(root, "isolated_schema", finished, timeout=1)
        finally:
            blocker.cancel()
    asyncio.run(exercise())


def test_missing_lock_wait_times_out_without_claiming_success():
    class Root:
        def cursor(self): return self
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def execute(self, *_): pass
        async def fetchone(self): return (0,)

    async def exercise():
        blocker = asyncio.create_task(asyncio.Event().wait())
        try:
            with pytest.raises(TimeoutError):
                await module._observe_lock_wait(Root(), "isolated_schema", blocker, timeout=.1)
        finally:
            blocker.cancel()
            await asyncio.gather(blocker, return_exceptions=True)
    asyncio.run(exercise())


def test_qualification_rejects_unbound_target_before_database_access():
    async def exercise():
        with pytest.raises(ValueError, match="identity"):
            await module.qualify(4000, "a" * 32, Path("unused"), lambda: None, identity={})
    asyncio.run(exercise())


def test_preexisting_schema_is_never_dropped_on_creation_failure(tmp_path, monkeypatch):
    source = tmp_path / "go/shared/authoritysql"
    source.mkdir(parents=True)
    (source / "catalog.go").write_text("const catalogWriterSlotBootstrapDDL = `CREATE TABLE writer_slot(id INT)`", encoding="utf-8")
    monkeypatch.setenv("XUANMING_IT_SOURCE", str(tmp_path))
    statements = []

    class Database:
        def cursor(self): return self
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def execute(self, sql, *_):
            statements.append(sql)
            if sql.startswith("CREATE DATABASE"):
                raise RuntimeError("schema already exists")
        def close(self): pass

    async def connect(**_): return Database()
    monkeypatch.setattr(module.aiomysql, "connect", connect)
    async def exercise():
        binding = {"run_id": "a" * 32, "project": "xuanming-ha-" + "b" * 16,
                   "container": "c" * 64, "sql_port": 4000}
        with pytest.raises(RuntimeError, match="already exists"):
            await module.qualify(4000, binding["run_id"], tmp_path / "proof.json", lambda: None,
                                 identity=binding)
        assert not any(sql.startswith("DROP") for sql in statements)
    asyncio.run(exercise())
