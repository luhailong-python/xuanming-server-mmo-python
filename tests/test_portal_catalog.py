import asyncio
import hashlib
import hmac
from datetime import datetime
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from xuanming.portal_catalog import check_hotfix, register_routes, validate_releases


RELEASES = {"android": {"latest_version": "1.10", "min_version": "1.2.0",
                        "patch_url": "https://example.test/update", "changelog": "fixes"}}


def test_hotfix_versions_and_optional_fields():
    validate_releases(RELEASES)
    assert check_hotfix({"platform": "other", "client_version": "1.0"}, RELEASES) == {
        "need_update": False, "force_update": False}
    assert check_hotfix({"platform": "android", "client_version": "1.10.0"}, RELEASES) == {
        "need_update": False, "force_update": False, "latest_version": "1.10"}
    assert check_hotfix({"platform": "android", "client_version": "1.1.9"}, RELEASES) == {
        "need_update": True, "force_update": True, "latest_version": "1.10",
        "patch_url": "https://example.test/update", "changelog": "fixes"}
    assert not check_hotfix({"platform": "android", "client_version": "1.2"}, RELEASES)["force_update"]
    for value in ("1.2.3.4.5", "1.-2", "1234567890", "1.a", ""):
        with pytest.raises(ValueError):
            check_hotfix({"platform": "android", "client_version": value}, RELEASES)


def test_bad_release_configuration_is_rejected():
    for invalid in ({"latest_version": "1.0", "min_version": "1.1", "patch_url": "https://a"},
                    {"latest_version": "1.0", "patch_url": " "},
                    {"latest_version": "x", "patch_url": "https://a"}):
        with pytest.raises(ValueError):
            validate_releases({"android": invalid})


def test_public_routes_produce_compatible_json_and_hmac(monkeypatch):
    async def run():
        settings = SimpleNamespace(hotfix_platforms=RELEASES, cdn_signing_secret="private-secret",
                                   cdn_sign_ttl_seconds=3600, cdn_base_url="https://cdn.example")
        app = web.Application()
        register_routes(app, SimpleNamespace(settings=settings))
        async with TestClient(TestServer(app)) as client:
            response = await client.post("/api/cdn-sign", json={"resource_path": "/update.zip"})
            signature = hmac.new(b"private-secret", b"/update.zip|4600", hashlib.sha256).hexdigest()
            assert response.status == 200
            assert await response.json() == {"signed_url": f"https://cdn.example/update.zip?expire=4600&sign={signature}",
                                             "expire_at": 4600}
            response = await client.post("/api/hotfix-check", json={"platform": "android", "client_version": "1.1"})
            assert response.status == 200 and (await response.json())["force_update"]
            response = await client.post("/api/hotfix-check", json={"platform": "android", "client_version": "bad"})
            assert response.status == 400
            response = await client.post("/api/cdn-sign", json={"resource_path": None})
            assert response.status == 400

    monkeypatch.setattr("xuanming.portal_catalog.time.time", lambda: 1000)
    asyncio.run(run())


def test_announcements_include_only_present_utc_timestamps():
    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def execute(self, query):
            assert "UTC_TIMESTAMP()" in query and "ORDER BY created_at DESC" in query

        async def fetchall(self):
            return [(7, "公告", "内容", "notice", datetime(2024, 1, 1), None)]

    class Connection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        def cursor(self):
            return Cursor()

        async def rollback(self):
            pass

    async def run():
        settings = SimpleNamespace(hotfix_platforms={}, cdn_signing_secret="secret",
                                   cdn_sign_ttl_seconds=3600, cdn_base_url="https://cdn.example")
        app = web.Application()
        register_routes(app, SimpleNamespace(settings=settings, db=SimpleNamespace(acquire=Connection)))
        async with TestClient(TestServer(app)) as client:
            response = await client.get("/api/announcement")
            assert await response.json() == {"items": [{"id": 7, "title": "公告", "content": "内容",
                                                         "type": "notice", "start_time": 1704067200}]}

    asyncio.run(run())
