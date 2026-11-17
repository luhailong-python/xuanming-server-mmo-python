import asyncio
import argparse
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import aiomysql
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from xuanming.portal_admin_announcements import register_routes as announcements
from xuanming.portal_admin_zones import register_routes as zones
from xuanming.portal_catalog import register_routes as catalog
from ownership import assert_owned_tidb


async def run(port, output, project, container, run_id):
    assert_owned_tidb(port, project, container, run_id)
    schema = "portalit_" + uuid.uuid4().hex
    conn = await aiomysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True)
    async with conn.cursor() as cur:
        await cur.execute("SELECT VERSION(), TIDB_VERSION()")
        version, details = await cur.fetchone()
        assert "tidb" in version.lower() and "Store: tikv" in details
        await cur.execute("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s", (schema,))
        assert (await cur.fetchone())[0] == 0
        await cur.execute(f"CREATE DATABASE `{schema}` CHARACTER SET utf8mb4")
        await cur.execute(f"USE `{schema}`")
        ddl = Path("config/portal_schema.sql").read_text(encoding="utf-8")
        for statement in ddl.split(";"):
            if statement.strip():
                await cur.execute(statement)

    pool = await aiomysql.create_pool(host="127.0.0.1", port=port, user="root", db=schema,
                                      minsize=1, maxsize=5, autocommit=False)
    settings = SimpleNamespace(admin_api_key="portal-tidb-only", hotfix_platforms={},
                               cdn_signing_secret="tidb-isolated-fixture", cdn_sign_ttl_seconds=3600,
                               cdn_base_url="https://example.test")
    deps = SimpleNamespace(db=pool, settings=settings)
    app = web.Application()
    zones(app, deps)
    announcements(app, deps)
    catalog(app, deps)
    headers = {"X-Admin-Key": settings.admin_api_key}
    record = {"zone_id": 17, "name": "TiDB zone", "manual_status": 0, "capacity": 100,
              "maintenance_msg": "", "open_time": None, "recommended": True, "sort_order": 1}
    result = {"version": version, "store": "tikv", "schema": schema, "separate_instance": True}
    try:
        async with TestClient(TestServer(app)) as client:
            assert (await client.get("/admin/zones")).status == 401
            response = await client.post("/admin/zones", json=record, headers=headers)
            data = await response.json()
            assert response.status == 200 and data["create_role_closed"] is False and data["recommended"] is True
            response = await client.get("/admin/zones/17", headers=headers)
            tag = response.headers["ETag"]
            requests = [client.put("/admin/zones/17", json=record | {"name": name},
                                   headers=headers | {"If-Match": tag}) for name in ("A", "B")]
            outcomes = await asyncio.gather(*requests)
            assert sorted(response.status for response in outcomes) == [200, 412]
            for value in ("one", "two"):
                response = await client.post("/admin/whitelist", json={"zone_id": 17, "account_id": 123,
                                             "note": value}, headers=headers)
                data = await response.json()
                assert response.status == 200 and data["note"] == value
            response = await client.get("/admin/whitelist/17", headers=headers)
            assert len(await response.json()) == 1
            response = await client.post("/admin/announcements", headers=headers,
                                         json={"title": "TiDB notice", "content": "actual database"})
            data = await response.json()
            assert response.status == 200
            identifier = data["id"]
            response = await client.get("/api/announcement")
            assert any(item["id"] == identifier for item in (await response.json())["items"])
            response = await client.delete(f"/admin/announcements/{identifier}", headers=headers)
            assert response.status == 204
            assert not (await (await client.get("/api/announcement")).json())["items"]
            result.update(ddl=True, etag_concurrency=True, bit_columns=True, whitelist_upsert=True,
                          announcement_crud=True, unauthorized_rejected=True)
    finally:
        pool.close()
        await pool.wait_closed()
        async with conn.cursor() as cur:
            await cur.execute(f"DROP DATABASE `{schema}`")
        conn.close()
    output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Portal integration against a disposable local TiDB")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--container", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("invalid port")
    asyncio.run(run(args.port, args.output, args.project, args.container, args.run_id))
