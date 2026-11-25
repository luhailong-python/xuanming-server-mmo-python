"""纯 Python 商城（mall_game）+ 门户代理（portal_mall_admin）+ 支付/交易 + GM 后台 + 配置表节点联调测试。"""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import AsyncExitStack
from types import SimpleNamespace

import aiohttp
from aiohttp import web

from data_service_fakes import MemoryStore as DataServiceMemoryStore
from xuanming.config_node.app import ConfigNodeService, create_app as create_config_app
from xuanming.data_service.service import DataService
from xuanming.gm_admin.app import create_gm_app
from xuanming.gm_admin.clients import GameServicesClient
from xuanming.gm_admin.config import GmAdminConfig
from xuanming.gm_admin.journal import MemoryGmJournal
from xuanming.grpc_runtime import GrpcClient, serve_grpc
from xuanming.login.caller_auth import CallerVerifier
from xuanming.login.directory import DataService as LoginDirectoryClient
from xuanming.mail.service import MailService
from xuanming.mail.store import MemoryMailStore
from xuanming.mall_game.app import create_mall_app
from xuanming.mall_game.config import MallGameConfig
from xuanming.mall_game.store import MemoryMallStore
from xuanming.payment.mall_client import HttpMallClient, MallPayment
from xuanming.player_locator.config import PlayerLocatorConfig
from xuanming.player_locator.service import PlayerLocatorService
from xuanming.player_locator.store import MemorySessionStore
from xuanming.portal_mall_admin import register_routes as register_portal_mall_admin
from xuanming.portal_observability import Metrics
from xuanming.protocol import Protocol
from xuanming.trade.mall_adapter import MallAdapter
from xuanming.trade.model import RoleOriginal


async def _start_http(stack: AsyncExitStack, app: web.Application) -> str:
    runner = web.AppRunner(app)
    await runner.setup()
    stack.push_async_callback(runner.cleanup)
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return f"http://127.0.0.1:{port}"


def test_python_commerce_gm_and_config_node_e2e():
    """真实回环 HTTP + gRPC 贯通：portal_mall_admin -> mall_game <- trade/payment，gm_admin -> data_service/mail，config_node。"""

    async def scenario():
        protocol = Protocol()
        gw_secret = "gw-secret-0123456789abcdef0123456789abcdef"
        pay_secret = "pay-secret-0123456789abcdef0123456789abcde"
        del_secret = "del-secret-0123456789abcdef0123456789abcde"
        adm_secret = "adm-secret-0123456789abcdef0123456789abcde"
        portal_key = "portal-admin-key-0123456789abcdef012345678"
        gm_token = "gm-admin-token-0123456789abcdef01234567890"
        mail_secret = "mail-admin-secret-0123456789abcdef01234567"
        ds_secret = b"ds-secret-0123456789abcdef0123456789abcdef"

        async with AsyncExitStack() as stack:
            # 1. 启动纯 Python mall_game HTTP 服务
            mall_cfg = MallGameConfig(
                host="127.0.0.1",
                port=0,
                run_mode="test",
                require_tidb=False,
                gateway_secret=gw_secret,
                payment_secret=pay_secret,
                delivery_secret=del_secret,
                admin_secret=adm_secret,
                allowed_channels=frozenset(("wechat",)),
            )
            mall_store = MemoryMallStore(mall_cfg)
            mall_url = await _start_http(stack, create_mall_app(mall_store, mall_cfg))

            # 2. 启动带 portal_mall_admin 路由的 Portal HTTP 应用，反代到纯 Python mall_game
            portal_app = web.Application()
            deps = SimpleNamespace(
                settings=SimpleNamespace(
                    admin_api_key=portal_key,
                    mall_admin_base_url=mall_url,
                    mall_admin_subject="mall-admin",
                    mall_admin_secret=adm_secret,
                ),
                metrics=Metrics(),
            )
            register_portal_mall_admin(portal_app, deps)
            portal_url = await _start_http(stack, portal_app)

            # 3. 经 Portal /admin/mall/skus/create 创建官方充值 SKU，并经 /admin/mall/skus/list 回查
            async with aiohttp.ClientSession(trust_env=False) as http:
                async with http.post(
                    f"{portal_url}/admin/mall/skus/create",
                    headers={"X-Admin-Key": portal_key},
                    json={
                        "requestId": "req-create-sku-e2e-1",
                        "skuId": 9501,
                        "terms": {
                            "productId": 950,
                            "skuCode": "E2E-PACK-600",
                            "priceMinor": 600,
                            "currency": "CNY",
                            "published": True,
                            "currencyType": 1,
                            "currencyAmount": 60,
                            "title": "测试充值档位",
                        },
                        "stock": 100,
                    },
                ) as resp:
                    assert resp.status == 200, await resp.text()
                    sku_body = await resp.json()
                    assert sku_body["skuId"] == 9501

                async with http.post(
                    f"{portal_url}/admin/mall/skus/list",
                    headers={"X-Admin-Key": portal_key},
                    json={"limit": 20},
                ) as resp:
                    assert resp.status == 200
                    listed = await resp.json()
                    assert any(item["skuId"] == 9501 for item in listed["skus"])

            # 4. Trade MallAdapter 下单 -> Payment HttpMallClient 记账 -> Trade 轮询交付并回执 DELIVERED
            trade_adapter = MallAdapter(
                protocol,
                mall_url,
                gw_secret.encode("utf-8"),
                del_secret.encode("utf-8"),
            )
            stack.push_async_callback(trade_adapter.close)
            buyer = RoleOriginal(player_id=88001, account="e2e_buyer", owner_revision=1)
            order_resp = await trade_adapter.create_official_order(
                buyer,
                protocol.message(
                    "trade.CreateOfficialOrderRequest",
                    sku_id=9501,
                    quantity=1,
                    client_request_id="req-e2e-order-1",
                ),
            )
            order_id = order_resp.order.order_id
            assert order_resp.order.amount == 600

            pay_mall = HttpMallClient(
                mall_url,
                "payment-adapter",
                pay_secret.encode("utf-8"),
            )
            stack.push_async_callback(pay_mall.close)
            outcome = await pay_mall.record_payment(
                MallPayment(
                    order_id=order_id,
                    channel="wechat",
                    transaction_id="wx-e2e-tx-001",
                    amount_minor=600,
                    currency="CNY",
                ),
            )
            assert outcome.order_state == "DELIVERY_PENDING"
            assert outcome.refund_reason == ""

            polled = await trade_adapter.call(
                "delivery", "trade", "POST", "/internal/deliveries/poll", {"limit": 10, "cursor": ""},
            )
            assert any(item["intent"]["orderId"] == order_id for item in polled["entries"])
            await trade_adapter.call(
                "delivery",
                "trade",
                "POST",
                "/internal/deliveries/receipt",
                {
                    "orderId": order_id,
                    "tradeOrderId": "99001",
                    "state": "DELIVERED",
                    "proof": hashlib.sha256(b"e2e-delivery-proof").hexdigest(),
                },
                expect_json=False,
            )
            view = await pay_mall.order_view(order_id)
            assert view.state == "DELIVERED"

            # 5. 启动纯 Python data_service + player_locator + mail gRPC，并由纯 Python gm_admin 经真实 gRPC 查玩家与发邮件
            ds_store = DataServiceMemoryStore(protocol)
            ds_service = DataService(
                protocol,
                ds_store,
                {"gm_admin": ds_secret, "login": ds_secret},
                zone_refresh=60.0,
            )
            await ds_service.start()
            stack.push_async_callback(ds_service.close)
            ds_server, ds_port = await serve_grpc(
                protocol, "127.0.0.1:0", ds_service.services(), pass_context=True,
            )
            stack.push_async_callback(ds_server.stop, 1)

            # 在 DataService 创建真实角色（发号 + 注册路由 + 占名 + 建角）
            ds_client = GrpcClient(f"127.0.0.1:{ds_port}")
            stack.push_async_callback(ds_client.close)
            login_dir = LoginDirectoryClient(protocol, ds_client.call, ds_secret)
            player_id = await login_dir.mint_player_id()
            await login_dir.register_player_zone(player_id, 1, 1_000_000)
            assert await login_dir.reserve_name(player_id, "测试道友") == (0, 0)
            role = protocol.message(
                "AccountSimplePlayer",
                player_id=player_id,
                class_id=1,
                gender=1,
                zone_id=1,
                name="测试道友",
                appearance_id="01_hero",
            )
            roles = await login_dir.create_role("gm_e2e_acc", role)
            assert len(roles) == 1 and roles[0].player_id == player_id

            locator_service = PlayerLocatorService(
                protocol,
                MemorySessionStore(),
                PlayerLocatorConfig(node_id=108, zone_id=1),
            )
            locator_server, locator_port = await serve_grpc(
                protocol, "127.0.0.1:0", locator_service.services(), pass_context=True,
            )
            stack.push_async_callback(locator_server.stop, 1)

            mail_store = MemoryMailStore(protocol)
            mail_service = MailService(
                protocol,
                mail_store,
                caller_verifier=CallerVerifier(()),
                producer_secrets={"admin": mail_secret.encode("utf-8")},
                coordinator_secret=b"mail-coord-secret-0123456789abcdef0123456",
            )
            mail_server, mail_port = await serve_grpc(
                protocol, "127.0.0.1:0", mail_service.services(), pass_context=True,
            )
            stack.push_async_callback(mail_server.stop, 1)

            gm_cfg = GmAdminConfig(
                host="127.0.0.1",
                port=0,
                admin_token=gm_token,
                data_service=f"127.0.0.1:{ds_port}",
                player_locator=f"127.0.0.1:{locator_port}",
                mail_service=f"127.0.0.1:{mail_port}",
                mail_admin_secret=mail_secret.encode("utf-8"),
                caller_secret=ds_secret,
            )
            gm_clients = GameServicesClient(protocol, gm_cfg)
            stack.push_async_callback(gm_clients.close)
            gm_journal = MemoryGmJournal()
            gm_url = await _start_http(stack, create_gm_app(gm_journal, gm_clients, gm_cfg))

            gm_headers = {"X-Admin-Key": gm_token, "X-Gm-Operator": "ops-e2e"}
            async with aiohttp.ClientSession(trust_env=False) as http:
                async with http.get(
                    f"{gm_url}/gm/players/{player_id}",
                    headers=gm_headers,
                ) as resp:
                    assert resp.status == 200, await resp.text()
                    profile = (await resp.json())["result"]
                    assert profile["playerId"] == str(player_id)
                    assert profile["display"]["name"] == "测试道友"
                    assert profile["homeZoneId"] == 1

                async with http.post(
                    f"{gm_url}/gm/mail/drafts",
                    headers=gm_headers,
                    json={
                        "clientRequestId": "11111111-1111-4111-8111-111111111111",
                        "playerId": str(player_id),
                        "title": "纯 Python 联调补发",
                        "body": "邮件链路验证通过",
                        "reason": "e2e verification",
                        "currencies": [{"currencyType": 0, "amount": "100"}],
                        "validDays": 7,
                    },
                ) as resp:
                    assert resp.status == 200, await resp.text()
                    draft = (await resp.json())["result"]
                    cmd_id = draft["id"]

                async with http.post(
                    f"{gm_url}/gm/mail/commands/{cmd_id}/send",
                    headers=gm_headers,
                ) as resp:
                    assert resp.status == 200, await resp.text()
                    sent = (await resp.json())["result"]
                    assert sent["status"] == "DELIVERED"

            # 6. 启动纯 Python config_node 并校验 61 张表与 RankedRule 查询
            cfg_svc = ConfigNodeService()
            cfg_svc.load_all()
            cfg_url = await _start_http(stack, create_config_app(cfg_svc))
            async with aiohttp.ClientSession(trust_env=False) as http:
                async with http.get(f"{cfg_url}/api/table/status") as resp:
                    assert resp.status == 200
                    status_body = await resp.json()
                    assert status_body["tableCount"] == 68
                async with http.get(f"{cfg_url}/api/table/RankedRule/1") as resp:
                    assert resp.status == 200
                    row = await resp.json()
                    assert row["id"] == 1

    asyncio.run(scenario())
