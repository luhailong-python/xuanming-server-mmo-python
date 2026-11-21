import asyncio
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from xuanming.portal_admin_announcements import register_routes


def test_admin_auth_and_announcement_lifecycle():
    class DB:
        def __init__(self):
            self.rows = {}

        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        def cursor(self):
            return self

        async def commit(self):
            pass

        async def rollback(self):
            pass

        async def execute(self, query, args=None):
            if query.startswith("INSERT"):
                self.rows[args[0]] = args
            elif query.startswith("DELETE"):
                self.rows.pop(args[0], None)

        async def fetchall(self):
            return list(self.rows.values())

    async def run():
        db = DB()
        app = web.Application()
        register_routes(app, SimpleNamespace(db=db, settings=SimpleNamespace(admin_api_key="secret")))
        async with TestClient(TestServer(app)) as client:
            assert (await client.get("/admin/announcements")).status == 401
            assert (await client.post("/admin/announcements", json={"title": "x"},
                                      headers={"X-Admin-Key": "wrong"})).status == 401
            headers = {"X-Admin-Key": "secret"}
            response = await client.post("/admin/announcements", headers=headers,
                                         json={"id": 12, "title": "test", "type": "notice"})
            assert response.status == 200
            created = await response.json()
            assert created["id"] != 12 and 0 < created["id"] < 2**62
            second = await (await client.post("/admin/announcements", headers=headers,
                                              json={"title": "second", "type": "notice"})).json()
            assert second["id"] != created["id"] and 0 < second["id"] < 2**62
            assert (await client.delete(f"/admin/announcements/{second['id']}", headers=headers)).status == 204
            response = await client.get("/admin/announcements", headers=headers)
            assert (await response.json())[0]["id"] == created["id"]
            assert (await client.delete(f"/admin/announcements/{created['id']}", headers=headers)).status == 204
            assert not db.rows

        app = web.Application()
        register_routes(app, SimpleNamespace(db=db, settings=SimpleNamespace(admin_api_key="")))
        async with TestClient(TestServer(app)) as client:
            assert (await client.get("/admin/announcements", headers={"X-Admin-Key": ""})).status == 401

    asyncio.run(run())
