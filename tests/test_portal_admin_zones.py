import asyncio
import os
from pathlib import Path
import socket
import subprocess
import time
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest

from xuanming import portal_admin_zones as zones


def settings(**changes):
    return dict({"zone_id": 1, "name": "west", "manual_status": 1, "capacity": 100,
                 "maintenance_msg": "later", "open_time": None, "recommended": True,
                 "sort_order": 1}, **changes)


def test_complete_settings_validation():
    assert zones._settings(settings())[0] == 1
    for field in ("name", "manual_status", "capacity", "maintenance_msg", "open_time", "recommended", "sort_order"):
        body = settings()
        del body[field]
        with pytest.raises(zones._BadField) as error:
            zones._settings(body)
        assert error.value.field == field
    for changes in ({"manual_status": 4}, {"capacity": 0}, {"recommended": 1},
                    {"open_time": "2026-11-17T12:00:00.001"}, {"zone_id": True}):
        with pytest.raises(zones._BadField):
            zones._settings(settings(**changes))


def test_admin_auth_fails_closed_without_touching_database():
    class NoDatabase:
        def acquire(self):
            raise AssertionError("database must not be reached")

    async def run():
        app = web.Application()
        zones.register_routes(app, SimpleNamespace(db=NoDatabase(), settings=SimpleNamespace(admin_api_key="secret")))
        async with TestClient(TestServer(app)) as client:
            for method, path in (("GET", "/admin/zones"), ("POST", "/admin/zones/1/maintenance"),
                                 ("POST", "/admin/whitelist"), ("DELETE", "/admin/whitelist/1/2")):
                response = await client.request(method, path)
                assert response.status == 401
            response = await client.get("/admin/zones", headers={"X-Admin-Key": "wrong"})
            assert response.status == 401
    asyncio.run(run())


def test_real_mysql_zone_and_whitelist_commands(tmp_path):
    root = os.environ.get("XUANMING_TEST_MYSQL_HOME")
    if not root:
        pytest.skip("set XUANMING_TEST_MYSQL_HOME to run isolated MySQL integration")
    import aiomysql

    binary = Path(root) / "bin" / "mysqld.exe"
    assert binary.is_file()
    data = tmp_path / "mysql-data"
    data.mkdir()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    flags = [str(binary), "--no-defaults", f"--basedir={root}", f"--datadir={data}"]
    creation = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    result = subprocess.run([*flags, "--initialize-insecure", "--console"], cwd=tmp_path,
                            capture_output=True, timeout=90, creationflags=creation)
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    process = subprocess.Popen([*flags, "--bind-address=127.0.0.1", f"--port={port}", "--mysqlx=0", "--console"],
                               cwd=tmp_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               creationflags=creation)

    async def run():
        deadline = time.monotonic() + 40
        while True:
            assert process.poll() is None, "isolated mysqld exited before it became ready"
            try:
                connection = await aiomysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True)
                break
            except (OSError, aiomysql.Error):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.1)
        async with connection.cursor() as cur:
            await cur.execute("SELECT @@datadir")
            actual = (await cur.fetchone())[0]
            assert Path(actual).resolve() == data.resolve(), "refuse to use a non-isolated MySQL instance"
            await cur.execute("CREATE DATABASE portal_test")
            await cur.execute("USE portal_test")
            schema = (Path(__file__).resolve().parents[1] / "config" / "portal_schema.sql").read_text(encoding="utf-8")
            for statement in schema.split(";"):
                if statement.strip():
                    await cur.execute(statement)
            await cur.execute("CREATE TABLE rollback_probe (id INT PRIMARY KEY) ENGINE=InnoDB")
        connection.close()
        pool = await aiomysql.create_pool(host="127.0.0.1", port=port, user="root", db="portal_test", autocommit=False, maxsize=5)
        app = web.Application()
        zones.register_routes(app, SimpleNamespace(db=pool, settings=SimpleNamespace(admin_api_key="secret")))
        headers = {"X-Admin-Key": "secret"}
        try:
            async with TestClient(TestServer(app)) as client:
                async def call(method, path, **kwargs):
                    return await client.request(method, path, headers={**headers, **kwargs.pop("headers", {})}, **kwargs)

                response = await call("POST", "/admin/zones", json=settings())
                created = await response.json()
                assert response.status == 200
                assert created["recommended"] is True and created["create_role_closed"] is False
                response = await call("POST", "/admin/zones", json=settings())
                assert response.status == 200
                response = await call("POST", "/admin/zones", json=settings(name="east"))
                assert response.status == 409 and (await response.json())["name"] == "west"
                get = await call("GET", "/admin/zones/1")
                tag = get.headers["ETag"]
                assert (await call("GET", "/admin/zones/1", headers={"If-None-Match": tag})).status == 304
                assert (await call("PUT", "/admin/zones/1", json=settings())).status == 428
                first, second = await asyncio.gather(*[
                    call("PUT", "/admin/zones/1", json=settings(name=name), headers={"If-Match": tag})
                    for name in ("north", "south")])
                assert sorted((first.status, second.status)) == [200, 412]
                changed = await call("POST", "/admin/zones/1/maintenance", json={"maintenance_msg": "wait"})
                assert changed.status == 200 and (await changed.json())["maintenance_msg"] == "wait"
                closed = await call("POST", "/admin/zones/1/create-role/close")
                assert closed.status == 200 and (await closed.json())["create_role_closed"] is True
                capacity = await call("PUT", "/admin/zones/1/role-capacity", json={"role_capacity": 12})
                assert capacity.status == 200 and (await capacity.json())["role_capacity"] == 12
                assert (await call("POST", "/admin/zones/1/open")).status == 200
                reopened = await call("POST", "/admin/zones/1/create-role/open")
                assert reopened.status == 200 and (await reopened.json())["create_role_closed"] is False
                assert (await call("PUT", "/admin/zones/99", json=settings(zone_id=99), headers={"If-Match": tag})).status == 404

                for note in ("first", "last"):
                    added = await call("POST", "/admin/whitelist", json={"id": 999, "zone_id": 1, "account_id": 9, "note": note})
                    item = await added.json()
                    assert added.status == 200 and item["id"] != 999 and item["note"] == note
                listed = await call("GET", "/admin/whitelist/1")
                assert len(await listed.json()) == 1
                assert (await call("DELETE", "/admin/whitelist/1/9")).status == 204
                assert (await call("DELETE", "/admin/whitelist/1/9")).status == 204
                assert (await call("DELETE", "/admin/zones/1")).status == 204
                assert (await call("DELETE", "/admin/zones/1")).status == 404
                # A timeout after a write closes its connection; verify against
                # real InnoDB that the uncommitted row is rolled back.
                from xuanming import portal_sql
                old_timeout = portal_sql.SQL_TIMEOUT
                portal_sql.SQL_TIMEOUT = .1
                async def stalled_write(cur):
                    await cur.execute("INSERT INTO rollback_probe VALUES (1)")
                    await cur.execute("SELECT SLEEP(1)")
                try:
                    with pytest.raises(TimeoutError):
                        await zones._retry_write(pool, stalled_write)
                finally:
                    portal_sql.SQL_TIMEOUT = old_timeout
                await asyncio.sleep(1.1)
                async with pool.acquire() as connection:
                    async with connection.cursor() as cursor:
                        await cursor.execute("SELECT COUNT(*) FROM rollback_probe")
                        assert (await cursor.fetchone())[0] == 0
                    await connection.rollback()
        finally:
            pool.close()
            await pool.wait_closed()
    try:
        asyncio.run(run())
    finally:
        process.terminate()
        process.wait(timeout=30)
