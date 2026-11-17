import asyncio
import hashlib
import hmac
from types import SimpleNamespace

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
import pytest

from xuanming import portal_mall_admin as mall


KEY = "k" * 32
SECRET = "s" * 32


def test_configuration_rejects_dynamic_or_unsafe_destinations():
    assert mall._config(SimpleNamespace()) is None
    for url in ("file:///tmp/x", "http://user:pass@host", "http://host/a/../b", "http://host/a%2fb",
                "http://host/x;param", "http://host/x//y", "http://host?x=y", "http://host/#x"):
        with pytest.raises(ValueError):
            mall._config(SimpleNamespace(mall_admin_base_url=url, mall_admin_secret=SECRET, mall_admin_subject="mall-admin"))
    with pytest.raises(ValueError):
        mall._config(SimpleNamespace(mall_admin_base_url="http://host", mall_admin_secret="weak"))


def test_mall_proxy_signs_exact_bytes_limits_responses_and_never_follows_redirects():
    async def run():
        observed = []
        outcome = {"status": 200, "body": b'{"sku":123}', "redirect": False}
        async def upstream(request):
            body = await request.read()
            headers = request.headers
            assert headers["X-Commerce-Subject"] == "mall-admin"
            expected = "\n".join(("v1", "admin", "mall-admin", headers["X-Commerce-Timestamp"],
                                  headers["X-Commerce-Nonce"], "POST", request.raw_path,
                                  hashlib.sha256(body).hexdigest())).encode()
            assert hmac.compare_digest(headers["X-Commerce-Signature"],
                                       hmac.new(SECRET.encode(), expected, hashlib.sha256).hexdigest())
            observed.append((request.path, body))
            if outcome["redirect"]:
                raise web.HTTPFound("/trap")
            return web.Response(status=outcome["status"], body=outcome["body"])

        server_app = web.Application()
        server_app.router.add_post("/base/internal/admin/{path:.*}", upstream)
        async with TestServer(server_app) as upstream_server:
            settings = SimpleNamespace(admin_api_key=KEY, mall_admin_base_url=str(upstream_server.make_url("/base")),
                                       mall_admin_secret=SECRET, mall_admin_subject="mall-admin")
            app = web.Application(client_max_size=300_000)
            mall.register_routes(app, SimpleNamespace(settings=settings))
            async with TestClient(TestServer(app)) as client:
                page = await client.get("/mall-admin")
                assert page.status == 200 and "frame-ancestors 'none'" in page.headers["Content-Security-Policy"]
                assert page.headers["Cache-Control"] == "no-store" and page.headers["X-Content-Type-Options"] == "nosniff"
                assert (await client.get("/mall-admin.js")).status == 200
                assert (await client.get("/mall-admin.css")).status == 200
                path = "/admin/mall/skus/create"
                assert (await client.post(path, data=b'{}')).status == 401
                headers = {"X-Admin-Key": KEY}
                assert (await client.post(path + "?a=1", data=b'{}', headers=headers)).status == 400
                assert (await client.post(path, data=b'[]', headers=headers)).status == 400
                assert (await client.post(path, data=b'{' + b' ' * 16384 + b'}', headers=headers)).status == 413
                body = b'{"requestId":"unchanged","skuId":"9007199254740993"}'
                response = await client.post(path, data=body, headers=headers)
                assert response.status == 200 and await response.read() == outcome["body"]
                assert response.headers["Cache-Control"] == "no-store"
                assert observed == [("/base/internal/admin/skus/create", body)]
                outcome.update(status=409, body=b'{"code":"conflict"}')
                assert (await client.post(path, data=body, headers=headers)).status == 409
                outcome.update(status=500, body=b'{}')
                assert (await client.post(path, data=body, headers=headers)).status == 502
                outcome.update(status=200, body=b'[]')
                assert (await client.post(path, data=body, headers=headers)).status == 502
                outcome.update(body=b'{"x":"' + b'a' * 262144 + b'"}')
                assert (await client.post(path, data=body, headers=headers)).status == 502
                outcome.update(body=b'{}', redirect=True)
                assert (await client.post(path, data=body, headers=headers)).status == 502
                assert len(observed) == 6
                assert (await client.post("/admin/mall/unknown", data=body, headers=headers)).status == 404
    asyncio.run(run())


def test_disabled_mall_and_weak_operator_key_are_unavailable():
    async def run():
        for api_key in (KEY, "weak"):
            app = web.Application()
            mall.register_routes(app, SimpleNamespace(settings=SimpleNamespace(admin_api_key=api_key)))
            async with TestClient(TestServer(app)) as client:
                response = await client.post("/admin/mall/skus/list", data=b'{}', headers={"X-Admin-Key": api_key})
                assert response.status == 503
    asyncio.run(run())
