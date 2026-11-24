"""trade 官方商城 HTTP HMAC、付款撤销扣回、人民币寄售（含角色交易）与内容安全端到端测试。"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import re
import uuid
from contextlib import AsyncExitStack
from types import SimpleNamespace

from aiohttp import web
import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.content_guard.engine import Engine as ContentGuardEngine
from xuanming.content_guard.service import ContentGuardService
from xuanming.content_guard.wordlist import Category as CgCategory, Entry as CgEntry, Lexicon, Mode as CgMode
from xuanming.data.asset_ledger import (
    Outcome,
    authorized as asset_authorized,
    find_receipt,
    find_stream,
    record as record_ledger,
)
from xuanming.grpc_runtime import (
    GrpcClient,
    serve_grpc,
    session_metadata,
    sign_service_call,
    verify_service_call,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.protocol import Protocol
from xuanming.trade import (
    ACCOUNT_DIRECTORY_TRADE_SECRET_ENV,
    ASSET_OP_SECRET_ENV,
    ASSET_STREAM_TRADE_CREDIT,
    ASSET_STREAM_TRADE_DEBIT,
    AssetOpCaller,
    AssetOpLoop,
    COMMERCE_AWAITING_PAYMENT,
    COMMERCE_CANCELLED,
    COMMERCE_DELIVERED,
    COMMERCE_DELIVERY_SECRET_ENV,
    COMMERCE_GATEWAY_SECRET_ENV,
    COMMERCE_KIND_OFFICIAL,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    COMMERCE_MANUAL,
    COMMERCE_REFUND_PENDING,
    COMMERCE_REVERSED,
    COMMERCE_REVERSING,
    COMMERCE_SETTLED,
    COORDINATOR_SECRET_ENV,
    CURRENCY_BIND_DIAMOND,
    CURRENCY_DIAMOND,
    CURRENCY_GOLD,
    CommerceEngine,
    ContentCheckResult,
    ERR_INVALID_PARAMETER,
    ERR_LOGIN_ACCOUNT_PLAYER_FULL,
    ERR_TRADE_ASSET_NOT_TRADABLE,
    ERR_TRADE_FEATURE_DISABLED,
    ERR_TRADE_ORDER_STATE_CONFLICT,
    ERR_TRADE_TEXT_CHECK_UNAVAILABLE,
    ERR_TRADE_TITLE_SENSITIVE,
    FREEZE_EVIDENCE_SCENE,
    GrpcContentGuardChecker,
    GrpcRoleSealer,
    GrpcRoleTradeDirectory,
    MANUAL_REASON_PAYMENT_REVERSAL,
    MANUAL_RESOLUTION_CALLER,
    MallAdapter,
    MallPoller,
    MarketLimits,
    MemoryTradeStore,
    MergedZones,
    REQUIRED_TABLES,
    ROLE_FENCE_SECRET_ENV,
    ROLE_TRADE_STATE_CANCELLED,
    ROLE_TRADE_STATE_FREEZING,
    ROLE_TRADE_STATE_FROZEN,
    ROLE_TRADE_STATE_TRANSFERRED,
    RmbConsignments,
    RmbIdentity,
    STAGE_OFFICIAL_REVERSED,
    SceneTarget,
    SqlTradeStore,
    TEXT_SCENE_TRADE_TITLE,
    TEXT_VERDICT_MASKED,
    TEXT_VERDICT_PASS,
    TX_AUCTION_BUY,
    TX_AUCTION_RETURN,
    TX_PAYMENT_REVERSAL,
    TextCheckUnavailableError,
    TradeConfig,
    TradePolicy,
    TradeService,
    commerce_signature,
    decode_workflow,
    mall_order_answer,
    verify_commerce_signature,
)

PROTOCOL = Protocol()

SECRET_COORDINATOR = b"role-fence-coordinator-secret-01234567"
SECRET_ROLE_FENCE_TRADE = b"role-fence-secret-for-trade-caller-012"
SECRET_ASSET_TRADE = b"asset-op-secret-for-trade-caller-01234"
SECRET_MANUAL_OPS = b"trade-manual-resolution-ops-secret-012"
SECRET_MALL_GATEWAY = b"commerce-mall-gateway-secret-0123456789"
SECRET_MALL_DELIVERY = b"commerce-mall-delivery-secret-987654321"
SECRET_ACCT_DIR_TRADE = b"account-directory-trade-secret-01234567"

SCENE_INSTANCE_UUID = "11112222-3333-4444-5555-666677778888"


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeClock:
    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, delta_ms: int) -> int:
        self.now_ms += delta_ms
        return self.now_ms


class SeqIdGen:
    def __init__(self, start: int = 5000) -> None:
        self._next = start

    async def __call__(self) -> int:
        val = self._next
        self._next += 1
        return val


class FakeRoleFenceDirectory:
    def __init__(self) -> None:
        self.states: dict[int, object] = {}

    def register(
        self,
        player_id: int,
        account: str,
        owner_revision: int = 1,
        *,
        freeze_token: int = 0,
        freeze_final: bool = False,
        version: int = 1,
        home_zone: int = 1,
    ):
        state = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=player_id,
            account=account,
            owner_revision=owner_revision,
            freeze_token=freeze_token,
            freeze_final=freeze_final,
            version=version,
            home_zone=home_zone,
        )
        self.states[player_id] = state
        return state

    async def get_role_fence_state(self, player_id: int):
        return self.states.get(player_id)


class FakeHomeZones:
    def __init__(self) -> None:
        self.mapping: dict[int, int] = {}

    async def home_zone(self, player_id: int) -> int:
        return int(self.mapping.get(player_id, 0))


class FakeSceneAndLedger:
    """Scene 资产通道 + SQL 账本模拟器，支持指定玩家扣款拒绝（测试付款撤销扣回失败转人工）。"""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.ledgers: dict[int, object] = {}
        self.reject_debits_for: set[int] = set()
        self.reject_credits_for: set[int] = set()
        self.instance_snapshots: dict[int, object] = {}
        self.debit_tx_types: list[tuple[int, int]] = []
        self.credit_tx_types: list[tuple[int, int]] = []
        self.credit_bundles: list[tuple[int, object]] = []

    def _ledger(self, player_id: int):
        led = self.ledgers.get(player_id)
        if led is None:
            led = self.ledgers[player_id] = PROTOCOL.message("PlayerAssetOpLedgerComp")
        return led

    async def resolve(self, player_id: int):
        return SceneTarget(endpoint="fake-scene:1", call=self._handle_rpc), False

    async def read_sql_committed_ledger(self, player_id: int):
        led = self.ledgers.get(player_id)
        if led is None:
            return None
        out = PROTOCOL.message("PlayerAssetOpLedgerComp")
        out.CopyFrom(led)
        return out

    async def _handle_rpc(self, path: str, data: bytes, *, timeout: float | None = None) -> bytes:
        del timeout
        rpc_map = {
            "/scene_node.SceneNodeGrpc/AssetDebit": "debit",
            "/scene_node.SceneNodeGrpc/AssetCredit": "credit",
            "/scene_node.SceneNodeGrpc/AssetAbortDebit": "abort_debit",
            "/scene_node.SceneNodeGrpc/AssetAckReceipt": "ack_receipt",
        }
        rpc = rpc_map[path]
        req = PROTOCOL.parse("AssetOpRequest", data)
        assert asset_authorized(rpc, req, {"trade": SECRET_ASSET_TRADE}, self.clock())
        pid = int(req.player_id)
        led = self._ledger(pid)
        if rpc == "ack_receipt":
            st = find_stream(led, int(req.stream))
            if st is not None:
                rcpt = find_receipt(st, int(req.seq))
                if rcpt is not None and rcpt.HasField("snapshot"):
                    rcpt.ClearField("snapshot")
            return PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True).SerializeToString()

        if rpc == "abort_debit":
            record_ledger(led, req, Outcome.REJECTED, 0)
            return PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.REJECTED), durable=True).SerializeToString()

        if rpc == "credit":
            self.credit_tx_types.append((pid, int(req.tx_type)))
            self.credit_bundles.append((pid, req.bundle))
            if pid in self.reject_credits_for and int(req.tx_type) != TX_AUCTION_RETURN:
                record_ledger(led, req, Outcome.REJECTED, 27005)
                resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.REJECTED), durable=True)
                resp.reason.id = 27005
                return resp.SerializeToString()
            record_ledger(led, req, Outcome.APPLIED, 0)
            return PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True).SerializeToString()

        # debit
        self.debit_tx_types.append((pid, int(req.tx_type)))
        if pid in self.reject_debits_for:
            record_ledger(led, req, Outcome.REJECTED, 27001)
            resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.REJECTED), durable=True)
            resp.reason.id = 27001
            return resp.SerializeToString()

        snap = None
        if req.bundle.item_uuids:
            snap = PROTOCOL.message("AssetSnapshot")
            for uid in req.bundle.item_uuids:
                inst = self.instance_snapshots.get(int(uid))
                if inst is None:
                    inst = PROTOCOL.message(
                        "AssetItemInstance",
                        item_uuid=int(uid),
                        config_id=1001,
                        stack_size=1,
                        instance_proto=f"rmb-item-{uid}".encode(),
                    )
                snap.items.add().CopyFrom(inst)
        record_ledger(led, req, Outcome.APPLIED, 0, snapshot=snap)
        resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True)
        if snap is not None:
            resp.snapshot.CopyFrom(snap)
        return resp.SerializeToString()


class MockMallServer:
    """真实 aiohttp 本地商城服务：强制校验双角色 HMAC 签名，完整承载官方商城与人民币寄售 HTTP 契约。"""

    def __init__(
        self,
        gateway_secret: bytes = SECRET_MALL_GATEWAY,
        delivery_secret: bytes = SECRET_MALL_DELIVERY,
        clock: FakeClock | None = None,
    ) -> None:
        self.gateway_secret = gateway_secret
        self.delivery_secret = delivery_secret
        self.clock = clock or FakeClock()
        self.seen_nonces: list[str] = []
        self.seen_requests: list[tuple[str, str, dict, bytes]] = []

        # 官方商城状态
        self.skus: list[dict] = [
            {"skuId": 101, "title": "60 Diamonds", "priceMinor": 600, "currency": "CNY", "available": 999},
            {"skuId": 102, "title": "600 Diamonds", "priceMinor": 6000, "currency": "CNY", "available": 500},
            {"skuId": 103, "title": "Spirit Herb Pack", "priceMinor": 1200, "currency": "CNY", "available": 100},
        ]
        self.official_orders: dict[str, dict] = {}
        self.official_order_by_req: dict[tuple[str, str], str] = {}
        self.delivery_queue: list[dict] = []  # {"intent": ..., "paymentRevision": 0}
        self.reversal_queue: list[dict] = []  # {"intent": ..., "revision": 1, "entitled": False}
        self.delivery_receipts: list[dict] = []
        self.reversal_receipts: list[dict] = []

        # 人民币寄售状态
        self.rmb_listings: dict[str, dict] = {}
        self.rmb_orders: dict[str, dict] = {}
        self.rmb_order_by_req: dict[tuple[str, str], str] = {}
        self.rmb_deliveries: list[dict] = []
        self.rmb_returns: list[dict] = []
        self.rmb_return_receipts: list[dict] = []
        self.incomes: dict[str, list[dict]] = {}
        self.withdrawals: dict[str, dict] = {}
        self.withdrawal_by_req: dict[tuple[str, str], str] = {}

        self._runner: web.AppRunner | None = None
        self.base_url: str = ""

    async def start(self) -> str:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._dispatch)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        return self.base_url

    async def close(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    def _verify_hmac(self, request: web.Request, body: bytes) -> tuple[bool, str]:
        role = "delivery" if request.path.startswith("/internal/") else "gateway"
        secret = self.delivery_secret if role == "delivery" else self.gateway_secret
        subject = request.headers.get("X-Commerce-Subject", "")
        ts = request.headers.get("X-Commerce-Timestamp", "")
        nonce = request.headers.get("X-Commerce-Nonce", "")
        sig = request.headers.get("X-Commerce-Signature", "")
        if not subject or not ts.isdigit() or not re.fullmatch(r"[0-9a-f]{32}", nonce):
            return False, "bad headers"
        if not verify_commerce_signature(secret, role, subject, ts, nonce, request.method, request.path, body, sig):
            return False, "bad signature"
        self.seen_nonces.append(nonce)
        return True, subject

    def mark_official_paid(self, order_id: str, *, item_config_id: int = 0, item_count: int = 0, currency_type: int = CURRENCY_DIAMOND, currency_amount: int = 60) -> None:
        o = self.official_orders[order_id]
        o["state"] = "DELIVERY_PENDING"
        intent = {
            "orderId": order_id,
            "buyerId": o["buyerId"],
            "buyerAccount": o["buyerAccount"],
            "buyerOwnerRevision": o["buyerOwnerRevision"],
            "skuId": o["skuId"],
            "quantity": o["quantity"],
            "itemConfigId": item_config_id,
            "itemCount": item_count,
            "title": "Official Pack",
            "amountMinor": o["amountMinor"],
            "currency": o["currency"],
            "currencyType": currency_type,
            "currencyAmount": currency_amount,
        }
        self.delivery_queue.append({"intent": intent, "paymentRevision": o["paymentRevision"]})

    def enqueue_official_reversal(self, order_id: str, revision: int, *, currency_type: int = CURRENCY_DIAMOND, currency_amount: int = 60) -> None:
        o = self.official_orders[order_id]
        o["paymentRevision"] = revision
        o["state"] = "REVERSING"
        intent = {
            "orderId": order_id,
            "buyerId": o["buyerId"],
            "buyerAccount": o["buyerAccount"],
            "buyerOwnerRevision": o["buyerOwnerRevision"],
            "skuId": o["skuId"],
            "quantity": o["quantity"],
            "itemConfigId": 0,
            "itemCount": 0,
            "title": "Official Pack",
            "amountMinor": o["amountMinor"],
            "currency": o["currency"],
            "currencyType": currency_type,
            "currencyAmount": currency_amount,
        }
        self.reversal_queue.append({"intent": intent, "revision": revision, "entitled": (revision % 2 == 0)})

    def mark_rmb_paid(self, order_id: str) -> None:
        o = self.rmb_orders[order_id]
        listing = self.rmb_listings[o["listingId"]]
        o["state"] = "DELIVERY_PENDING"
        self.rmb_deliveries.append({
            "orderId": order_id,
            "buyerPlayerId": o["buyerId"],
            "buyerAccount": o["buyerAccount"],
            "buyerOwnerRevision": o["buyerOwnerRevision"],
            "listingId": listing["listingId"],
            "escrowReference": listing["escrowReference"],
            "escrowProof": listing["escrowProof"],
            "amountFen": listing["priceFen"],
        })

    def enqueue_rmb_return(self, listing_id: str) -> None:
        listing = self.rmb_listings[listing_id]
        listing["state"] = "RETURNING"
        self.rmb_returns.append({
            "listingId": listing_id,
            "sellerPlayerId": listing["sellerPlayerId"],
            "sellerAccount": listing["sellerAccount"],
            "escrowReference": listing["escrowReference"],
            "escrowProof": listing["escrowProof"],
        })

    async def _dispatch(self, request: web.Request) -> web.Response:
        body = await request.read()
        self.seen_requests.append((request.method, request.path, dict(request.headers), body))
        ok, subject = self._verify_hmac(request, body)
        if not ok:
            return web.json_response({"error": subject}, status=401)
        data = json.loads(body.decode("utf-8")) if body else {}
        path = request.path

        # -- Official Mall Gateway --
        if path == "/api/skus/list":
            after = int(data.get("afterSkuId", 0))
            limit = int(data.get("limit", 20))
            skus = [s for s in self.skus if s["skuId"] > after][:limit]
            return web.json_response({"skus": skus})

        if path == "/api/orders":
            req_id = data["requestId"]
            key = (subject, req_id)
            if key in self.official_order_by_req:
                return web.json_response(self.official_orders[self.official_order_by_req[key]])
            sku = next((s for s in self.skus if s["skuId"] == int(data["skuId"])), None)
            if sku is None:
                return web.json_response({"error": "sku not found"}, status=404)
            oid = str(uuid.uuid4())
            order = {
                "orderId": oid,
                "buyerId": subject,
                "buyerAccount": data["buyerAccount"],
                "buyerOwnerRevision": data["buyerOwnerRevision"],
                "skuId": sku["skuId"],
                "quantity": int(data["quantity"]),
                "amountMinor": sku["priceMinor"] * int(data["quantity"]),
                "currency": sku["currency"],
                "state": "AWAITING_PAYMENT",
                "paymentDeadlineMs": self.clock() + 600_000,
                "paymentRevision": 0,
            }
            self.official_orders[oid] = order
            self.official_order_by_req[key] = oid
            return web.json_response(order)

        if path == "/api/orders/query":
            o = self.official_orders.get(data.get("orderId", ""))
            if o is None or o["buyerId"] != subject:
                return web.json_response({"error": "not found"}, status=404)
            return web.json_response(o)

        if path == "/api/orders/cancel":
            o = self.official_orders.get(data.get("orderId", ""))
            if o is None or o["buyerId"] != subject:
                return web.json_response({"error": "not found"}, status=404)
            if o["state"] != "AWAITING_PAYMENT":
                return web.json_response({"error": "conflict"}, status=409)
            o["state"] = "CANCELLED"
            return web.json_response(o)

        if path == "/api/orders/list":
            after = data.get("afterOrderId", "")
            limit = int(data.get("limit", 20))
            mine = sorted(
                (o for o in self.official_orders.values() if o["buyerId"] == subject and o["orderId"] > after),
                key=lambda x: x["orderId"],
            )
            return web.json_response(mine[:limit])

        # -- Official Deliveries & Reversals Poll / Receipt --
        if path == "/internal/deliveries/poll":
            if data.get("cursor"):
                return web.json_response({"entries": []})
            entries = []
            for idx, item in enumerate(self.delivery_queue):
                oid = item["intent"]["orderId"]
                cur = "" if idx == len(self.delivery_queue) - 1 else f"v1:{self.clock()}:{oid}:{self.clock()}:{self.delivery_queue[-1]['intent']['orderId']}"
                entries.append({"intent": item["intent"], "paymentRevision": item["paymentRevision"], "cursor": cur})
            return web.json_response({"entries": entries})

        if path == "/internal/deliveries/receipt":
            self.delivery_receipts.append(data)
            oid = data["orderId"]
            state = data["state"]
            if oid in self.official_orders:
                self.official_orders[oid]["state"] = "CANCELLED" if state == "VOIDED" else state
                self.delivery_queue = [x for x in self.delivery_queue if x["intent"]["orderId"] != oid]
            if oid in self.rmb_orders:
                self.rmb_orders[oid]["state"] = state
                lid = self.rmb_orders[oid]["listingId"]
                if state == "DELIVERED":
                    self.rmb_listings[lid]["state"] = "SOLD"
                    gross = self.rmb_orders[oid]["amountMinor"]
                    fee = gross // 20
                    seller_acc = self.rmb_listings[lid]["sellerAccount"]
                    self.incomes.setdefault(seller_acc, []).append({
                        "orderId": oid,
                        "grossFen": gross,
                        "feeFen": fee,
                        "netFen": gross - fee,
                        "reservedFen": 0,
                        "paidFen": 0,
                        "forfeitedFen": 0,
                        "availableMs": self.clock(),
                    })
                elif state == "REFUND_PENDING":
                    self.enqueue_rmb_return(lid)
                self.rmb_deliveries = [x for x in self.rmb_deliveries if x["orderId"] != oid]
            return web.json_response({"ok": True})

        if path == "/internal/reversals/poll":
            if data.get("cursor"):
                return web.json_response({"entries": []})
            entries = []
            for idx, item in enumerate(self.reversal_queue):
                oid = item["intent"]["orderId"]
                cur = "" if idx == len(self.reversal_queue) - 1 else f"v1:{self.clock()}:{oid}:{self.clock()}:{self.reversal_queue[-1]['intent']['orderId']}"
                entries.append({
                    "intent": item["intent"],
                    "revision": item["revision"],
                    "entitled": item["entitled"],
                    "cursor": cur,
                })
            return web.json_response({"entries": entries})

        if path == "/internal/reversals/receipt":
            self.reversal_receipts.append(data)
            oid = data["orderId"]
            rev = int(data["revision"])
            state = data["state"]
            if oid in self.official_orders:
                self.official_orders[oid]["state"] = state
            self.reversal_queue = [
                x for x in self.reversal_queue if not (x["intent"]["orderId"] == oid and x["revision"] == rev)
            ]
            return web.json_response({"ok": True})

        # -- RMB Consignments --
        if path == "/internal/consignments/register":
            lid = data["listingId"]
            now = self.clock()
            listing = {
                "listingId": lid,
                "sellerPlayerId": data["sellerPlayerId"],
                "sellerAccount": data["sellerAccount"],
                "marketZone": data["marketZone"],
                "assetKind": data["assetKind"],
                "title": data["title"],
                "priceFen": data["priceFen"],
                "feeBps": 500,
                "coolingMs": 86_400_000,
                "noticeEndMs": now + 10_000,
                "saleEndMs": now + 86_400_000,
                "state": "LISTED",
                "phase": "ON_SALE",
                "orderId": "",
                "escrowReference": data["escrowReference"],
                "escrowProof": data["escrowProof"],
            }
            self.rmb_listings[lid] = listing
            return web.json_response(listing)

        if path == "/internal/consignments/get":
            listing = self.rmb_listings.get(data.get("listingId", ""))
            if listing is None:
                return web.json_response({"error": "not found"}, status=404)
            return web.json_response(listing)

        if path == "/internal/consignments/list":
            after = data.get("afterListingId", "")
            limit = int(data.get("limit", 20))
            items = sorted(
                (v for v in self.rmb_listings.values() if v["state"] == "LISTED" and v["listingId"] > after),
                key=lambda x: x["listingId"],
            )
            return web.json_response({"listings": items[:limit]})

        if path == "/internal/consignments/cancel":
            listing = self.rmb_listings.get(data.get("listingId", ""))
            if listing is None or listing["sellerAccount"] != data.get("sellerAccount"):
                return web.json_response({"error": "not found"}, status=404)
            self.enqueue_rmb_return(listing["listingId"])
            return web.json_response(listing)

        if path == "/internal/consignments/buy":
            lid = data["listingId"]
            listing = self.rmb_listings.get(lid)
            if listing is None:
                return web.json_response({"error": "not found"}, status=404)
            if listing["sellerAccount"] == data["buyerAccount"]:
                return web.json_response({"error": "self purchase"}, status=409)
            req_key = (data["buyerAccount"], data["requestId"])
            if req_key in self.rmb_order_by_req:
                return web.json_response(self.rmb_orders[self.rmb_order_by_req[req_key]])
            if listing["state"] != "LISTED":
                return web.json_response({"error": "conflict"}, status=409)
            oid = str(uuid.uuid4())
            listing["state"] = "LOCKED"
            listing["orderId"] = oid
            order = {
                "orderId": oid,
                "listingId": lid,
                "buyerId": data["buyerPlayerId"],
                "buyerAccount": data["buyerAccount"],
                "buyerOwnerRevision": data["buyerOwnerRevision"],
                "sellerAccount": listing["sellerAccount"],
                "skuId": 0,
                "quantity": 1,
                "amountMinor": listing["priceFen"],
                "currency": "CNY",
                "state": "AWAITING_PAYMENT",
                "paymentDeadlineMs": self.clock() + 600_000,
                "paymentRevision": 0,
            }
            self.rmb_orders[oid] = order
            self.rmb_order_by_req[req_key] = oid
            return web.json_response(order)

        if path == "/internal/consignments/orders/get":
            o = self.rmb_orders.get(data.get("orderId", ""))
            acc = data.get("account", "")
            if o is None or acc not in (o["buyerAccount"], o["sellerAccount"]):
                return web.json_response({"error": "not found"}, status=404)
            return web.json_response(o)

        if path == "/internal/consignments/orders/cancel":
            o = self.rmb_orders.get(data.get("orderId", ""))
            if o is None or o["buyerAccount"] != data.get("account"):
                return web.json_response({"error": "not found"}, status=404)
            if o["state"] != "AWAITING_PAYMENT":
                return web.json_response({"error": "conflict"}, status=409)
            o["state"] = "CANCELLED"
            self.rmb_listings[o["listingId"]]["state"] = "LISTED"
            self.rmb_listings[o["listingId"]]["orderId"] = ""
            return web.json_response(o)

        if path == "/internal/consignments/orders/list":
            acc = data.get("account", "")
            after = data.get("afterOrderId", "")
            limit = int(data.get("limit", 20))
            items = sorted(
                (o for o in self.rmb_orders.values() if acc in (o["buyerAccount"], o["sellerAccount"]) and o["orderId"] > after),
                key=lambda x: x["orderId"],
            )
            return web.json_response(items[:limit])

        if path == "/internal/consignments/incomes":
            acc = data.get("account", "")
            after = data.get("afterOrderId", "")
            limit = int(data.get("limit", 20))
            items = sorted((x for x in self.incomes.get(acc, []) if x["orderId"] > after), key=lambda x: x["orderId"])
            return web.json_response({"incomes": items[:limit]})

        if path == "/internal/consignments/withdrawals/create":
            acc = data["account"]
            req_id = data["requestId"]
            key = (acc, req_id)
            if key in self.withdrawal_by_req:
                return web.json_response(self.withdrawals[self.withdrawal_by_req[key]])
            wid = str(uuid.uuid4())
            w_obj = {
                "withdrawalId": wid,
                "account": acc,
                "amountFen": int(data["amountFen"]),
                "state": "REQUESTED",
                "channel": "alipay",
                "transactionId": f"wd-{wid[:8]}",
            }
            self.withdrawals[wid] = w_obj
            self.withdrawal_by_req[key] = wid
            return web.json_response(w_obj)

        if path == "/internal/consignments/withdrawals/get":
            w_obj = self.withdrawals.get(data.get("withdrawalId", ""))
            if w_obj is None or w_obj["account"] != data.get("account"):
                return web.json_response({"error": "not found"}, status=404)
            return web.json_response(w_obj)

        if path == "/internal/consignments/deliveries/poll":
            if data.get("cursor"):
                return web.json_response({"entries": []})
            entries = []
            for idx, item in enumerate(self.rmb_deliveries):
                oid = item["orderId"]
                cur = "" if idx == len(self.rmb_deliveries) - 1 else f"v1:{self.clock()}:{oid}:{self.clock()}:{self.rmb_deliveries[-1]['orderId']}"
                entries.append({"intent": item, "cursor": cur})
            return web.json_response({"entries": entries})

        if path == "/internal/consignments/returns/poll":
            if data.get("cursor"):
                return web.json_response({"entries": []})
            entries = []
            for idx, item in enumerate(self.rmb_returns):
                lid = item["listingId"]
                cur = "" if idx == len(self.rmb_returns) - 1 else f"r1:{lid}:{self.rmb_returns[-1]['listingId']}"
                entries.append({"intent": item, "cursor": cur})
            return web.json_response({"entries": entries})

        if path == "/internal/consignments/returns/receipt":
            self.rmb_return_receipts.append(data)
            lid = data["listingId"]
            if lid in self.rmb_listings:
                self.rmb_listings[lid]["state"] = "RETURNED"
            self.rmb_returns = [x for x in self.rmb_returns if x["listingId"] != lid]
            return web.json_response({"ok": True})

        return web.json_response({"error": f"unknown path {path}"}, status=404)


class MockDataServiceRoleTradeServer:
    """真实 gRPC DataService 角色交易桩：校验 'trade' ServiceCallAuth 签名并驱动完整冻结/封存/过户/取消状态机。"""

    def __init__(self, clock: FakeClock, fence_dir: FakeRoleFenceDirectory, secret: bytes = SECRET_ACCT_DIR_TRADE) -> None:
        self.clock = clock
        self.fence_dir = fence_dir
        self.secret = secret
        self.roles: dict[int, dict] = {}  # player_id -> {account, rev, trade_listing_id}
        self.trades: dict[int, dict] = {}  # listing_id -> {player_id, seller_account, rev, state, drained, tx_id}
        self.full_accounts: set[str] = set()
        self.force_transfer_full_accounts: set[str] = set()
        self.calls: list[str] = []
        self.sealed_listings: set[int] = set()

    def register_role(self, player_id: int, account: str, owner_revision: int = 1) -> None:
        self.roles[player_id] = {"account": account, "rev": owner_revision, "trade_listing_id": 0}

    async def _verify(self, path: str, request, context) -> None:
        self.calls.append(path)
        if not verify_service_call(path, request, "trade", self.clock(), self.secret):
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "invalid trade directory signature")

    def services(self) -> dict:
        return {
            "data_service.DataService": {
                "LookupAccountRole": self.lookup_account_role,
                "BeginAccountRoleTrade": self.begin_account_role_trade,
                "ConfirmAccountRoleTradeFreeze": self.confirm_account_role_trade_freeze,
                "CancelAccountRoleTrade": self.cancel_account_role_trade,
                "TransferAccountRole": self.transfer_account_role,
                "CheckAccountRoleSlot": self.check_account_role_slot,
            },
            "scene_node.SceneNodeGrpc": {
                "SealAccountRoleTrade": self.seal_account_role_trade,
            },
        }

    def _build_receipt(self, lid: int) -> object:
        t = self.trades[lid]
        rec = PROTOCOL.message(
            "data_service.AccountRoleTradeReceipt",
            listing_id=lid,
            player_id=t["player_id"],
            seller_account=t["seller_account"],
            seller_owner_revision=t["rev"],
            state=t["state"],
        )
        holder = rec.holders.add()
        holder.drained = bool(t["drained"])
        holder.seal.listing_id = lid
        holder.seal.player_id = t["player_id"]
        holder.seal.owner_revision = t["rev"]
        holder.seal.scene_instance_id = SCENE_INSTANCE_UUID
        holder.seal.through_generation = 1
        holder.seal.scene_node_id = "101"
        holder.seal.zone_id = 1
        if t["state"] in (ROLE_TRADE_STATE_FROZEN, ROLE_TRADE_STATE_TRANSFERRED):
            rec.freeze_evidence.player_id = t["player_id"]
            rec.freeze_evidence.listing_id = lid
            rec.freeze_evidence.owner_revision = t["rev"]
            rec.freeze_evidence.owner_epoch = 2
            rec.freeze_evidence.save_seq = 5
            rec.freeze_evidence.persisted_digest = hashlib.sha256(f"freeze:{lid}".encode()).digest()
            rec.freeze_evidence.kind = FREEZE_EVIDENCE_SCENE
        return rec

    async def lookup_account_role(self, request, context, path: str):
        await self._verify(path, request, context)
        pid = int(request.player_id)
        info = self.roles.get(pid)
        if info is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "role not found")
        resp = PROTOCOL.message(
            "data_service.AccountRoleOwnership",
            account=info["account"],
            owner_revision=info["rev"],
            trade_listing_id=info["trade_listing_id"],
        )
        resp.role.player_id = pid
        return resp

    async def begin_account_role_trade(self, request, context, path: str):
        await self._verify(path, request, context)
        lid = int(request.listing_id)
        pid = int(request.player_id)
        info = self.roles.get(pid)
        if info is None or info["account"] != request.seller_account or info["rev"] != int(request.expected_owner_revision):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "ownership mismatch")
        if lid not in self.trades:
            self.trades[lid] = {
                "player_id": pid,
                "seller_account": info["account"],
                "rev": info["rev"],
                "state": ROLE_TRADE_STATE_FREEZING,
                "drained": False,
                "tx_id": 0,
            }
            info["trade_listing_id"] = lid
            self.fence_dir.register(
                pid, info["account"], info["rev"], freeze_token=lid, freeze_final=True, version=10, home_zone=1
            )
        return self._build_receipt(lid)

    async def seal_account_role_trade(self, request, context, path: str):
        await self._verify(path, request, context)
        lid = int(request.seal.listing_id)
        self.sealed_listings.add(lid)
        if lid in self.trades:
            self.trades[lid]["drained"] = True
        return PROTOCOL.message("google.protobuf.Empty")

    async def confirm_account_role_trade_freeze(self, request, context, path: str):
        await self._verify(path, request, context)
        lid = int(request.listing_id)
        t = self.trades.get(lid)
        if t is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "trade not found")
        if t["drained"] and t["state"] == ROLE_TRADE_STATE_FREEZING:
            t["state"] = ROLE_TRADE_STATE_FROZEN
        return self._build_receipt(lid)

    async def cancel_account_role_trade(self, request, context, path: str):
        await self._verify(path, request, context)
        lid = int(request.listing_id)
        pid = int(request.player_id)
        t = self.trades.get(lid)
        if t is None:
            self.trades[lid] = {
                "player_id": pid,
                "seller_account": str(request.seller_account),
                "rev": int(request.expected_owner_revision),
                "state": ROLE_TRADE_STATE_CANCELLED,
                "drained": True,
                "tx_id": 0,
            }
            return self._build_receipt(lid)
        if t["state"] == ROLE_TRADE_STATE_TRANSFERRED:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "already transferred")
        t["drained"] = True
        t["state"] = ROLE_TRADE_STATE_CANCELLED
        info = self.roles[pid]
        info["trade_listing_id"] = 0
        self.fence_dir.register(pid, info["account"], info["rev"], freeze_token=0, freeze_final=False, version=20, home_zone=1)
        return self._build_receipt(lid)

    async def transfer_account_role(self, request, context, path: str):
        await self._verify(path, request, context)
        lid = int(request.listing_id)
        pid = int(request.player_id)
        buyer_acc = str(request.buyer_account)
        t = self.trades.get(lid)
        if t is None or t["state"] != ROLE_TRADE_STATE_FROZEN:
            if t is not None and t["state"] == ROLE_TRADE_STATE_TRANSFERRED and t["tx_id"] == int(request.tx_id):
                resp = PROTOCOL.message(
                    "data_service.AccountRoleTransferReceipt",
                    tx_id=int(request.tx_id),
                    listing_id=lid,
                    player_id=pid,
                    seller_account=t["seller_account"],
                    buyer_account=buyer_acc,
                    seller_owner_revision=t["rev"],
                    buyer_owner_revision=t["rev"] + 1,
                    created_ms=self.clock(),
                )
                resp.role.player_id = pid
                return resp
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "not frozen")
        if buyer_acc in self.full_accounts or buyer_acc in self.force_transfer_full_accounts:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "buyer role slots full")
        t["state"] = ROLE_TRADE_STATE_TRANSFERRED
        t["tx_id"] = int(request.tx_id)
        info = self.roles[pid]
        old_rev = info["rev"]
        info["account"] = buyer_acc
        info["rev"] = old_rev + 1
        info["trade_listing_id"] = 0
        self.fence_dir.register(pid, buyer_acc, old_rev + 1, freeze_token=0, freeze_final=False, version=30, home_zone=1)
        resp = PROTOCOL.message(
            "data_service.AccountRoleTransferReceipt",
            tx_id=int(request.tx_id),
            listing_id=lid,
            player_id=pid,
            seller_account=str(request.seller_account),
            buyer_account=buyer_acc,
            seller_owner_revision=old_rev,
            buyer_owner_revision=old_rev + 1,
            created_ms=self.clock(),
        )
        resp.role.player_id = pid
        return resp

    async def check_account_role_slot(self, request, context, path: str):
        await self._verify(path, request, context)
        return PROTOCOL.message(
            "data_service.CheckAccountRoleSlotResponse",
            full=(str(request.account) in self.full_accounts),
            retry=False,
        )


class FakeSceneNodeDirectory:
    def __init__(self, endpoint_host: str, endpoint_port: int) -> None:
        self.endpoint_host = endpoint_host
        self.endpoint_port = endpoint_port

    async def lookup(self, node_type: int, zone_id: int, node_id: int):
        del node_type, zone_id, node_id
        info = PROTOCOL.message("NodeInfo", node_uuid=SCENE_INSTANCE_UUID)
        info.grpc_endpoint.ip = self.endpoint_host
        info.grpc_endpoint.port = self.endpoint_port
        return info


class MallRmbTestRig:
    """全栈测试夹具：真实 TradeService gRPC + 真实 DataService/Scene 角色交易 gRPC + 真实 aiohttp 商城。"""

    def __init__(self, *, rmb_enabled: bool = True, content_guard=None, rmb_currencies=(CURRENCY_GOLD,)) -> None:
        self.clock = FakeClock()
        self.gate_key = Ed25519PrivateKey.generate()
        self.verifier = CallerVerifier([self.gate_key.public_key()], allowed_callers=("gate",))
        self.fence_dir = FakeRoleFenceDirectory()
        self.home_zones = FakeHomeZones()
        self.scene = FakeSceneAndLedger(self.clock)
        self.store = MemoryTradeStore(PROTOCOL, role_directory=self.fence_dir, clock_ms=self.clock)
        self.mall_server = MockMallServer(clock=self.clock)
        self.ds_role_server = MockDataServiceRoleTradeServer(self.clock, self.fence_dir)
        self._content_guard_override = content_guard
        self._rmb_enabled = rmb_enabled
        self._rmb_currencies = rmb_currencies

        self._stack: AsyncExitStack | None = None
        self.client: GrpcClient | None = None
        self.mall: MallAdapter | None = None
        self.poller: MallPoller | None = None
        self.rmb: RmbConsignments | None = None
        self.commerce: CommerceEngine | None = None
        self.asset_loop: AssetOpLoop | None = None
        self.service: TradeService | None = None

    async def __aenter__(self) -> "MallRmbTestRig":
        self._stack = AsyncExitStack()
        await self._stack.__aenter__()

        base_url = await self.mall_server.start()
        self._stack.push_async_callback(self.mall_server.close)

        ds_srv, ds_port = await serve_grpc(PROTOCOL, "127.0.0.1:0", self.ds_role_server.services(), pass_context=True)
        self._stack.push_async_callback(ds_srv.stop, 0)
        ds_client = GrpcClient(f"127.0.0.1:{ds_port}", timeout=5)
        self._stack.push_async_callback(ds_client.close)

        role_dir = GrpcRoleTradeDirectory(PROTOCOL, ds_client.call, SECRET_ACCT_DIR_TRADE, clock_ms=self.clock)
        sealer = GrpcRoleSealer(
            PROTOCOL, FakeSceneNodeDirectory("127.0.0.1", ds_port), SECRET_ACCT_DIR_TRADE, clock_ms=self.clock
        )
        self._stack.push_async_callback(sealer.close)

        caller = AssetOpCaller(
            PROTOCOL, self.scene, SECRET_ASSET_TRADE, requery_delays_sec=(), clock_ms=self.clock
        )
        self.asset_loop = AssetOpLoop(
            PROTOCOL, self.store, caller, self.scene, clock_ms=self.clock, rnd=lambda: 0.5
        )
        policy = TradePolicy(
            items=(1001, 1002),
            item_titles={1001: "Spirit Herb", 1002: "Dragon Scale"},
            pets=(2001,),
            currencies=(CURRENCY_GOLD, CURRENCY_DIAMOND),
            allow_instances=True,
        )
        self.commerce = CommerceEngine(
            PROTOCOL,
            self.store,
            commerce_ids=SeqIdGen(5000),
            op_ids=SeqIdGen(9000),
            policy=policy,
            asset_caller=caller,
            ledger_reader=self.scene,
            clock_ms=self.clock,
        )
        self.mall = MallAdapter(
            PROTOCOL, base_url, SECRET_MALL_GATEWAY, SECRET_MALL_DELIVERY, clock_ms=self.clock
        )
        self._stack.push_async_callback(self.mall.close)
        self.poller = MallPoller(self.mall, self.commerce)
        self.rmb = RmbConsignments(
            PROTOCOL,
            self.commerce,
            self.mall,
            role_dir,
            sealer,
            currency_types=self._rmb_currencies,
        )
        self.commerce.rmb = self.rmb

        self.service = TradeService(
            PROTOCOL,
            self.store,
            caller_verifier=self.verifier,
            coordinator_secret=SECRET_COORDINATOR,
            home_zones=self.home_zones,
            listing_ids=SeqIdGen(100),
            commerce=self.commerce,
            market=MarketLimits(scope="global"),
            zones_provider=lambda: MergedZones(),
            mode="dev",
            mall_url=base_url,
            rmb_enabled=self._rmb_enabled,
            mall=self.mall,
            rmb=self.rmb,
            rmb_identity=RmbIdentity(role_dir),
            content_guard=self._content_guard_override,
            manual_resolution_secret=SECRET_MANUAL_OPS,
            manual_resolution_environment="test-global",
            clock_ms=self.clock,
        )
        server, port = await serve_grpc(PROTOCOL, "127.0.0.1:0", self.service.services(), pass_context=True)
        self._stack.push_async_callback(server.stop, 0)
        self.client = GrpcClient(f"127.0.0.1:{port}", timeout=5)
        self._stack.push_async_callback(self.client.close)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._stack is not None:
            await self._stack.__aexit__(exc_type, exc, tb)

    def register_player(self, player_id: int, account: str, owner_revision: int = 1, home_zone: int = 1) -> None:
        self.fence_dir.register(player_id, account=account, owner_revision=owner_revision, home_zone=home_zone)
        self.ds_role_server.register_role(player_id, account=account, owner_revision=owner_revision)
        self.home_zones.mapping[player_id] = home_zone

    async def call_client(self, method: str, req_msg, resp_name: str, player_id: int, account: str, owner_revision: int = 1):
        assert self.client is not None
        path = f"/trade.ClientPlayerJubaozhai/{method}"
        details = PROTOCOL.message(
            "SessionDetails",
            session_id=101,
            gate_instance_id="gate-1",
            session_incarnation=1,
            player_id=player_id,
            authenticated_account=account,
            owner_revision=owner_revision,
        )
        md = session_metadata(details.SerializeToString(), path, self.gate_key)
        raw = await self.client.call(path, req_msg.SerializeToString(), metadata=md)
        return PROTOCOL.parse(resp_name, raw)

    async def call_admin(self, method: str, req_msg, resp_name: str):
        assert self.client is not None
        path = f"/trade.TradeAdmin/{method}"
        raw = await self.client.call(path, req_msg.SerializeToString())
        return PROTOCOL.parse(resp_name, raw)

    async def pump(self, rounds: int = 6) -> None:
        assert self.commerce is not None and self.asset_loop is not None
        for _ in range(rounds):
            await self.commerce.tick()
            await self.asset_loop.tick()
            await self.commerce.tick()


# -- 1. 商城 HMAC v1 跨语言黄金样例、防篡改、双独立密钥与每次独立 16B nonce ----------------------------------

def test_mall_hmac_cross_language_golden_and_distinct_secret_checks():
    # 与 go/trade/internal/commerce/mall_signature_test.go 逐字节对齐的跨语言黄金样例
    golden_payment_secret = "commerce-golden-payment-secret-0123456789"
    golden_payment_body = (
        b'{"orderId":"00112233-4455-6677-8899-aabbccddeeff","channel":"mock-dev",'
        b'"transactionId":"mockdev-00112233445566778899aabbccddeeff","amountMinor":600,"currency":"CNY"}'
    )
    golden_payment_sig = "886584609f3fb1fa50a5ddb30f02837fb7e75d11fa0075f344447872c7eaf051"
    got_pay = commerce_signature(
        golden_payment_secret,
        "payment",
        "payment-adapter",
        "1700000000123",
        "golden-nonce-0000000001",
        "POST",
        "/internal/payments",
        golden_payment_body,
    )
    assert got_pay == golden_payment_sig
    assert verify_commerce_signature(
        golden_payment_secret,
        "payment",
        "payment-adapter",
        "1700000000123",
        "golden-nonce-0000000001",
        "POST",
        "/internal/payments",
        golden_payment_body,
        golden_payment_sig,
    )

    golden_gateway_secret = "commerce-golden-gateway-secret-0123456789"
    golden_gateway_sig = "3d85985f791cbcf33107071195269b4a64890a3c3637ada6379df8c1c547fae1"
    got_get = commerce_signature(
        golden_gateway_secret,
        "gateway",
        "22",
        "1700000000456",
        "golden-nonce-0000000002",
        "GET",
        "/api/orders/00112233-4455-6677-8899-aabbccddeeff",
        None,
    )
    assert got_get == golden_gateway_sig

    # 规范串任一字段被改动，签名必然变化
    base = [
        golden_payment_secret,
        "payment",
        "payment-adapter",
        "1700000000123",
        "golden-nonce-0000000001",
        "POST",
        "/internal/payments",
        golden_payment_body,
    ]
    for idx in range(len(base)):
        tampered = list(base)
        tampered[idx] = tampered[idx] + (b"x" if isinstance(tampered[idx], bytes) else "x")
        assert commerce_signature(*tampered) != golden_payment_sig

    # 双密钥校验：不足 32 字节、两把密钥相同、URL 含路径或查询串均 fail-closed 拒建
    with pytest.raises(ValueError, match="distinct"):
        MallAdapter(PROTOCOL, "http://127.0.0.1:8080", SECRET_MALL_GATEWAY, SECRET_MALL_GATEWAY)
    with pytest.raises(ValueError, match="distinct"):
        MallAdapter(PROTOCOL, "http://127.0.0.1:8080", b"short", SECRET_MALL_DELIVERY)
    with pytest.raises(ValueError, match="invalid mall base url"):
        MallAdapter(PROTOCOL, "http://127.0.0.1:8080/prefix", SECRET_MALL_GATEWAY, SECRET_MALL_DELIVERY)

    # TradeConfig 启动期校验：双密钥相同或 RMB 启用缺账号目录密钥均拒启
    base_env = {
        ASSET_OP_SECRET_ENV: SECRET_ASSET_TRADE.decode(),
        COORDINATOR_SECRET_ENV: SECRET_COORDINATOR.decode(),
        ROLE_FENCE_SECRET_ENV: SECRET_ROLE_FENCE_TRADE.decode(),
        COMMERCE_GATEWAY_SECRET_ENV: SECRET_MALL_GATEWAY.decode(),
        COMMERCE_DELIVERY_SECRET_ENV: SECRET_MALL_GATEWAY.decode(),  # 故意与 gateway 相同
        ACCOUNT_DIRECTORY_TRADE_SECRET_ENV: SECRET_ACCT_DIR_TRADE.decode(),
    }
    settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="stable",
        data_service="127.0.0.1:50100",
        redis="redis://127.0.0.1:6379/0",
        etcd="127.0.0.1:2379",
        trade_node={
            "node_id": 109,
            "advertise_host": "127.0.0.1",
            "mall_url": "http://127.0.0.1:8080",
            "rmb_enabled": True,
        },
    )
    with pytest.raises(ValueError, match="distinct"):
        TradeConfig.from_settings(settings, environ=base_env)

    # 双密钥不同且合法，但 rmb_currencies 包含绑定灵石 -> 拒启
    good_env = {**base_env, COMMERCE_DELIVERY_SECRET_ENV: SECRET_MALL_DELIVERY.decode()}
    bad_rmb_settings = SimpleNamespace(
        **{**settings.__dict__, "trade_node": {**settings.trade_node, "rmb_currencies": [CURRENCY_BIND_DIAMOND]}}
    )
    with pytest.raises(ValueError, match="non-tradable currency"):
        TradeConfig.from_settings(bad_rmb_settings, environ=good_env)


# -- 2. 官方商城 SKU 列表、下单、查询/撤单/翻页与版本号跨撤销单调递增 ----------------------------------------

@_async_test
async def test_official_mall_skus_order_lifecycle_and_monotonic_version():
    async with MallRmbTestRig(rmb_enabled=False) as rig:
        rig.register_player(1001, "buyer_acc_1", 1, home_zone=1)

        # rmb_enabled=False 时 11 个 RMB RPC 仍返回 20003，而官方商城 2 个 RPC 正常开放
        rmb_disabled = await rig.call_client(
            "ListRmbConsignments",
            PROTOCOL.message("trade.ListRmbConsignmentsRequest", limit=10),
            "trade.ListRmbConsignmentsResponse",
            1001,
            "buyer_acc_1",
        )
        assert rmb_disabled.error_message.id == ERR_TRADE_FEATURE_DISABLED

        # 1) ListOfficialSkus 分页
        p1 = await rig.call_client(
            "ListOfficialSkus",
            PROTOCOL.message("trade.ListOfficialSkusRequest", after_sku_id=0, limit=2),
            "trade.ListOfficialSkusResponse",
            1001,
            "buyer_acc_1",
        )
        assert p1.error_message.id == 0
        assert [s.sku_id for s in p1.skus] == [101, 102]
        assert p1.next_sku_id == 102

        p2 = await rig.call_client(
            "ListOfficialSkus",
            PROTOCOL.message("trade.ListOfficialSkusRequest", after_sku_id=p1.next_sku_id, limit=2),
            "trade.ListOfficialSkusResponse",
            1001,
            "buyer_acc_1",
        )
        assert p2.error_message.id == 0
        assert [s.sku_id for s in p2.skus] == [103]

        # 验证每次 HTTP 请求都使用了新鲜的 16 字节 hex nonce
        assert len(rig.mall_server.seen_nonces) == 2
        assert rig.mall_server.seen_nonces[0] != rig.mall_server.seen_nonces[1]

        # 2) CreateOfficialOrder + GetCommerceOrder(UUID) + ListCommerceOrders(mall: cursor) + CancelCommerceOrder(UUID)
        created = await rig.call_client(
            "CreateOfficialOrder",
            PROTOCOL.message(
                "trade.CreateOfficialOrderRequest", client_request_id="off-req-0001", sku_id=101, quantity=2
            ),
            "trade.CommerceOrderResponse",
            1001,
            "buyer_acc_1",
        )
        assert created.error_message.id == 0
        assert created.order.kind == COMMERCE_KIND_OFFICIAL
        assert created.order.state == COMMERCE_AWAITING_PAYMENT
        assert created.order.amount == 1200
        oid = created.order.order_id

        fetched = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            1001,
            "buyer_acc_1",
        )
        assert fetched.error_message.id == 0 and fetched.order.order_id == oid

        listed = await rig.call_client(
            "ListCommerceOrders",
            PROTOCOL.message("trade.ListCommerceOrdersRequest", limit=10),
            "trade.ListCommerceOrdersResponse",
            1001,
            "buyer_acc_1",
        )
        assert listed.error_message.id == 0
        assert [o.order_id for o in listed.orders] == [oid]
        assert listed.next_cursor == f"mall:{oid}"

        cancelled = await rig.call_client(
            "CancelCommerceOrder",
            PROTOCOL.message("trade.CancelCommerceOrderRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            1001,
            "buyer_acc_1",
        )
        assert cancelled.error_message.id == 0
        assert cancelled.order.state == COMMERCE_CANCELLED

        # 3) 版本号跨 REVERSING(12) -> REVERSED(13) -> REVERSING(rev=2) -> DELIVERED -> MANUAL 严格单调递增
        seq = [
            ("AWAITING_PAYMENT", 0, COMMERCE_AWAITING_PAYMENT),
            ("DELIVERY_PENDING", 0, 4),
            ("DELIVERED", 0, COMMERCE_DELIVERED),
            ("REVERSING", 1, COMMERCE_REVERSING),
            ("REVERSED", 1, COMMERCE_REVERSED),
            ("REVERSING", 2, COMMERCE_REVERSING),
            ("DELIVERED", 2, COMMERCE_DELIVERED),
            ("MANUAL", 2, COMMERCE_MANUAL),
        ]
        prev_ver = 0
        for st_name, rev, want_enum in seq:
            ans = mall_order_answer(
                PROTOCOL,
                {
                    "orderId": "00112233-4455-6677-8899-aabbccddeeff",
                    "buyerId": "1001",
                    "skuId": 101,
                    "state": st_name,
                    "paymentRevision": rev,
                },
                rig.clock(),
            )
            assert ans.order.state == want_enum
            assert ans.order.version > prev_ver
            prev_ver = ans.order.version


# -- 3. MallPoller 交付 + 渠道付款撤销扣回（REVERSING -> REVERSED）+ 补发 + 扣回失败转人工与 reissue 恢复 ---

@_async_test
async def test_mall_poller_delivery_payment_reversal_regrant_and_manual_reissue():
    async with MallRmbTestRig(rmb_enabled=False) as rig:
        assert rig.poller is not None and rig.commerce is not None
        rig.register_player(1002, "buyer_rev", 1, home_zone=1)

        # 下单并标记付款成功
        created = await rig.call_client(
            "CreateOfficialOrder",
            PROTOCOL.message(
                "trade.CreateOfficialOrderRequest", client_request_id="rev-order-0001", sku_id=101, quantity=1
            ),
            "trade.CommerceOrderResponse",
            1002,
            "buyer_rev",
        )
        oid = created.order.order_id
        rig.mall_server.mark_official_paid(oid, currency_type=CURRENCY_DIAMOND, currency_amount=60)

        # 1) MallPoller 拉取交付意图 -> CommerceEngine + AssetOpLoop 发 TRADE_CREDIT -> 回报 DELIVERED
        assert await rig.poller.tick() == []
        await rig.pump()
        assert await rig.poller.tick() == []
        assert any(r["orderId"] == oid and r["state"] == "DELIVERED" for r in rig.mall_server.delivery_receipts)
        assert (1002, TX_AUCTION_BUY) in rig.scene.credit_tx_types

        got_delivered = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            1002,
            "buyer_rev",
        )
        assert got_delivered.order.state == COMMERCE_DELIVERED

        # 2) 渠道撤销付款（revision=1, entitled=False）-> REVERSING(12) -> TRADE_DEBIT(TX_PAYMENT_REVERSAL=31) -> REVERSED(13)
        rig.mall_server.enqueue_official_reversal(oid, revision=1, currency_type=CURRENCY_DIAMOND, currency_amount=60)
        got_reversing = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            1002,
            "buyer_rev",
        )
        assert got_reversing.order.state == COMMERCE_REVERSING

        assert await rig.poller.tick() == []
        await rig.pump()
        assert await rig.poller.tick() == []
        assert (1002, TX_PAYMENT_REVERSAL) in rig.scene.debit_tx_types
        assert any(
            r["orderId"] == oid and r["revision"] == 1 and r["state"] == "REVERSED"
            for r in rig.mall_server.reversal_receipts
        )
        got_reversed = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            1002,
            "buyer_rev",
        )
        assert got_reversed.order.state == COMMERCE_REVERSED

        # 3) 渠道申诉撤销撤回（revision=2, entitled=True）-> official_regrant_2 -> DELIVERED
        rig.mall_server.enqueue_official_reversal(oid, revision=2, currency_type=CURRENCY_DIAMOND, currency_amount=60)
        assert await rig.poller.tick() == []
        await rig.pump()
        assert await rig.poller.tick() == []
        assert any(
            r["orderId"] == oid and r["revision"] == 2 and r["state"] == "DELIVERED"
            for r in rig.mall_server.reversal_receipts
        )

        # 4) 再次撤销（revision=3），玩家灵石已花光导致扣回被拒 -> 转 MANUAL -> 人工 reissue 重发扣回腿 -> 补足后 REVERSED
        rig.scene.reject_debits_for.add(1002)
        rig.mall_server.enqueue_official_reversal(oid, revision=3, currency_type=CURRENCY_DIAMOND, currency_amount=60)
        assert await rig.poller.tick() == []
        await rig.pump()
        assert await rig.poller.tick() == []
        assert any(
            r["orderId"] == oid and r["revision"] == 3 and r["state"] == "MANUAL"
            for r in rig.mall_server.reversal_receipts
        )

        # 通过 TradeAdmin.ListManualContracts + ResolveManualContract(EXECUTE, action="reissue") 解除人工隔离
        list_req = PROTOCOL.message("trade.ListManualContractsRequest", limit=10)
        sign_service_call("/trade.TradeAdmin/ListManualContracts", list_req, MANUAL_RESOLUTION_CALLER, rig.clock(), SECRET_MANUAL_OPS)
        manual_list = await rig.call_admin("ListManualContracts", list_req, "trade.ListManualContractsResponse")
        assert len(manual_list.contracts) == 1
        rec = manual_list.contracts[0]
        w = decode_workflow(PROTOCOL, rec)
        assert w.quarantine.reason == MANUAL_REASON_PAYMENT_REVERSAL

        rig.scene.reject_debits_for.discard(1002)
        res_req = PROTOCOL.message(
            "trade.ResolveManualContractRequest",
            commerce_id=int(rec.commerce_id),
            expect_version=int(rec.version),
            action="reissue",
            operator="ops_alice",
            reason="player replenished diamonds for reversal",
            mode=3,  # EXECUTE
            expect_target="test-global",
        )
        sign_service_call("/trade.TradeAdmin/ResolveManualContract", res_req, MANUAL_RESOLUTION_CALLER, rig.clock(), SECRET_MANUAL_OPS)
        res_out = await rig.call_admin("ResolveManualContract", res_req, "trade.ResolveManualContractResponse")
        assert res_out.verdict == 1  # OK
        assert list(res_out.plan.new_legs) == ["official_reverse_3#2"]

        await rig.pump()
        rec_after = await rig.store.get_commerce(int(rec.commerce_id))
        w_after = decode_workflow(PROTOCOL, rec_after)
        assert int(rec_after.status) == COMMERCE_CANCELLED and w_after.stage == STAGE_OFFICIAL_REVERSED


@_async_test
async def test_official_mall_unissued_revocation_voided_and_owner_changed_refund():
    async with MallRmbTestRig(rmb_enabled=False) as rig:
        assert rig.poller is not None
        rig.register_player(1010, "buyer_void", 1, home_zone=1)
        rig.register_player(1011, "buyer_moved", 1, home_zone=1)

        # 场景 A：尚未发放即收到撤销（paymentRevision=1）-> 不建资产腿，交付回 VOIDED，撤销回 REVERSED
        c1 = await rig.call_client(
            "CreateOfficialOrder",
            PROTOCOL.message("trade.CreateOfficialOrderRequest", client_request_id="void-ord-0001", sku_id=101, quantity=1),
            "trade.CommerceOrderResponse",
            1010,
            "buyer_void",
        )
        oid1 = c1.order.order_id
        rig.mall_server.official_orders[oid1]["paymentRevision"] = 1
        rig.mall_server.mark_official_paid(oid1, currency_type=CURRENCY_DIAMOND, currency_amount=60)
        rig.mall_server.enqueue_official_reversal(oid1, revision=1, currency_type=CURRENCY_DIAMOND, currency_amount=60)
        assert await rig.poller.tick() == []
        assert any(r["orderId"] == oid1 and r["state"] == "VOIDED" for r in rig.mall_server.delivery_receipts)
        assert any(r["orderId"] == oid1 and r["revision"] == 1 and r["state"] == "REVERSED" for r in rig.mall_server.reversal_receipts)
        assert not any(pid == 1010 for pid, _ in rig.scene.credit_tx_types)

        # 场景 B：付款后买家角色已过户（owner_revision 升为 2）-> 首次交付不给新主发资产，直接转 official_refund 并回报 REFUND_PENDING
        c2 = await rig.call_client(
            "CreateOfficialOrder",
            PROTOCOL.message("trade.CreateOfficialOrderRequest", client_request_id="moved-ord-0001", sku_id=101, quantity=1),
            "trade.CommerceOrderResponse",
            1011,
            "buyer_moved",
        )
        oid2 = c2.order.order_id
        rig.mall_server.mark_official_paid(oid2, currency_type=CURRENCY_DIAMOND, currency_amount=60)
        rig.fence_dir.register(1011, "new_owner_acc", owner_revision=2, version=5, home_zone=1)
        await rig.store.merge_role_fence(rig.fence_dir.states[1011], rig.clock())

        assert await rig.poller.tick() == []
        assert any(r["orderId"] == oid2 and r["state"] == "REFUND_PENDING" for r in rig.mall_server.delivery_receipts)
        assert not any(pid == 1011 for pid, _ in rig.scene.credit_tx_types)


# -- 4. 完整人民币物品寄售 11 个 RPC：上架、本地回退读、列表、购买、交付、撤单、收益与提现 -------------------

@_async_test
async def test_rmb_item_consignment_all_11_rpcs_end_to_end():
    async with MallRmbTestRig(rmb_enabled=True) as rig:
        assert rig.rmb is not None
        rig.register_player(2001, "rmb_seller", 1, home_zone=1)
        rig.register_player(2002, "rmb_buyer", 1, home_zone=1)
        rig.scene.instance_snapshots[8888] = PROTOCOL.message(
            "AssetItemInstance",
            item_uuid=8888,
            config_id=1001,
            stack_size=1,
            instance_proto=b"mythic-blade-rmb-v1",
        )

        # 绑定灵石禁售 -> 20015
        bad_cur = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest",
            client_request_id="rmb-bad-bind-01",
            price_fen=1000,
            title="Forbidden Currency",
        )
        bad_cur.assets.currency_type = CURRENCY_BIND_DIAMOND
        bad_cur.assets.currency_amount = 100
        bad_resp = await rig.call_client(
            "CreateRmbConsignment", bad_cur, "trade.RmbConsignmentResponse", 2001, "rmb_seller"
        )
        assert bad_resp.error_message.id == ERR_TRADE_ASSET_NOT_TRADABLE

        # 1) CreateRmbConsignment（唯一实例装备）
        create_req = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest",
            client_request_id="rmb-item-req-01",
            price_fen=10000,
            title="Mythic Flame Blade",
        )
        create_req.assets.item_uuids.append(8888)
        created = await rig.call_client(
            "CreateRmbConsignment", create_req, "trade.RmbConsignmentResponse", 2001, "rmb_seller"
        )
        assert created.error_message.id == 0
        lid = int(created.listing.listing_id)
        assert created.listing.asset_kind == "ITEMS"
        assert created.listing.state == "ESCROWING"

        # 2) GetRmbConsignment（商城尚未登记时命中卖家本地回退视图）
        local_get = await rig.call_client(
            "GetRmbConsignment",
            PROTOCOL.message("trade.GetRmbConsignmentRequest", listing_id=lid),
            "trade.RmbConsignmentResponse",
            2001,
            "rmb_seller",
        )
        assert local_get.error_message.id == 0 and local_get.listing.state == "ESCROWING"

        # 推进托管扣款 -> rmb_ready -> rmb_registering -> 商城登记 LISTED
        await rig.pump(8)

        # 3) ListRmbConsignments & GetRmbConsignment（来自商城）
        listed = await rig.call_client(
            "ListRmbConsignments",
            PROTOCOL.message("trade.ListRmbConsignmentsRequest", limit=10),
            "trade.ListRmbConsignmentsResponse",
            2002,
            "rmb_buyer",
        )
        assert listed.error_message.id == 0
        assert [x.listing_id for x in listed.listings] == [lid]
        assert listed.listings[0].state == "LISTED"

        got_mall = await rig.call_client(
            "GetRmbConsignment",
            PROTOCOL.message("trade.GetRmbConsignmentRequest", listing_id=lid),
            "trade.RmbConsignmentResponse",
            2002,
            "rmb_buyer",
        )
        assert got_mall.error_message.id == 0 and got_mall.listing.state == "LISTED"

        # 4) BuyRmbConsignment -> CancelRmbOrder（未付款取消释放挂单）-> 再次 BuyRmbConsignment
        buy1 = await rig.call_client(
            "BuyRmbConsignment",
            PROTOCOL.message("trade.BuyRmbConsignmentRequest", listing_id=lid, client_request_id="rmb-buy-cancel-01"),
            "trade.CommerceOrderResponse",
            2002,
            "rmb_buyer",
        )
        assert buy1.error_message.id == 0
        assert buy1.order.kind == COMMERCE_KIND_RMB_CONSIGNMENT
        assert buy1.order.currency == "CNY"
        oid1 = buy1.order.order_id

        cancel_ord = await rig.call_client(
            "CancelRmbOrder",
            PROTOCOL.message("trade.CancelCommerceOrderRequest", order_id=oid1),
            "trade.CommerceOrderResponse",
            2002,
            "rmb_buyer",
        )
        assert cancel_ord.error_message.id == 0 and cancel_ord.order.state == COMMERCE_CANCELLED

        buy2 = await rig.call_client(
            "BuyRmbConsignment",
            PROTOCOL.message("trade.BuyRmbConsignmentRequest", listing_id=lid, client_request_id="rmb-buy-final-01"),
            "trade.CommerceOrderResponse",
            2002,
            "rmb_buyer",
        )
        assert buy2.error_message.id == 0
        oid2 = buy2.order.order_id

        # 5) GetRmbOrder & ListRmbOrders
        get_ord = await rig.call_client(
            "GetRmbOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=oid2),
            "trade.CommerceOrderResponse",
            2002,
            "rmb_buyer",
        )
        assert get_ord.error_message.id == 0 and get_ord.order.order_id == oid2

        list_ord = await rig.call_client(
            "ListRmbOrders",
            PROTOCOL.message("trade.ListCommerceOrdersRequest", limit=10),
            "trade.ListCommerceOrdersResponse",
            2002,
            "rmb_buyer",
        )
        assert list_ord.error_message.id == 0
        assert oid2 in [o.order_id for o in list_ord.orders]

        # 6) 商城标记已付款 -> RmbConsignments.tick() 交付原件给买家并回报 DELIVERED
        rig.mall_server.mark_rmb_paid(oid2)
        assert await rig.rmb.tick() == []
        # 交付阶段禁止卖家撤单 -> 20005
        refused_cancel = await rig.call_client(
            "CancelRmbConsignment",
            PROTOCOL.message("trade.CancelRmbConsignmentRequest", listing_id=lid),
            "trade.RmbConsignmentResponse",
            2001,
            "rmb_seller",
        )
        assert refused_cancel.error_message.id == ERR_TRADE_ORDER_STATE_CONFLICT

        await rig.pump(6)
        assert await rig.rmb.tick() == []
        assert any(r["orderId"] == oid2 and r["state"] == "DELIVERED" for r in rig.mall_server.delivery_receipts)
        buyer_items = [b for pid, b in rig.scene.credit_bundles if pid == 2002]
        assert len(buyer_items) == 1 and buyer_items[0].item_instances[0].instance_proto == b"mythic-blade-rmb-v1"

        # 7) ListSellerIncomes + CreateWithdrawal + GetWithdrawal
        incomes = await rig.call_client(
            "ListSellerIncomes",
            PROTOCOL.message("trade.ListSellerIncomesRequest", limit=10),
            "trade.ListSellerIncomesResponse",
            2001,
            "rmb_seller",
        )
        assert incomes.error_message.id == 0 and len(incomes.incomes) == 1
        assert incomes.incomes[0].gross_fen == 10000 and incomes.incomes[0].net_fen == 9500

        wd_create = await rig.call_client(
            "CreateWithdrawal",
            PROTOCOL.message(
                "trade.CreateWithdrawalRequest", client_request_id="wd-req-0001", amount_fen=9500
            ),
            "trade.WithdrawalResponse",
            2001,
            "rmb_seller",
        )
        assert wd_create.error_message.id == 0
        assert wd_create.withdrawal.amount_fen == 9500 and wd_create.withdrawal.state == "REQUESTED"
        wid = wd_create.withdrawal.withdrawal_id

        wd_get = await rig.call_client(
            "GetWithdrawal",
            PROTOCOL.message("trade.GetWithdrawalRequest", withdrawal_id=wid),
            "trade.WithdrawalResponse",
            2001,
            "rmb_seller",
        )
        assert wd_get.error_message.id == 0 and wd_get.withdrawal.withdrawal_id == wid

        # 8) 另一单上架并走 CancelRmbConsignment -> 商城撤单 -> seller_return(TX_AUCTION_RETURN=30) -> returns/receipt
        req2 = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest",
            client_request_id="rmb-cancel-req-02",
            price_fen=5000,
            title="Stackable Herbs",
        )
        req2.assets.items[1001] = 5
        c2 = await rig.call_client("CreateRmbConsignment", req2, "trade.RmbConsignmentResponse", 2001, "rmb_seller")
        lid2 = int(c2.listing.listing_id)
        await rig.pump(8)

        cancel_c2 = await rig.call_client(
            "CancelRmbConsignment",
            PROTOCOL.message("trade.CancelRmbConsignmentRequest", listing_id=lid2),
            "trade.RmbConsignmentResponse",
            2001,
            "rmb_seller",
        )
        assert cancel_c2.error_message.id == 0 and cancel_c2.listing.state == "CANCEL_PENDING"

        await rig.pump(8)
        assert await rig.rmb.tick() == []
        assert any(r["listingId"] == str(lid2) for r in rig.mall_server.rmb_return_receipts)
        assert (2001, TX_AUCTION_RETURN) in rig.scene.credit_tx_types


# -- 5. 角色交易（CHARACTER）：DataService 6 RPC + Scene SealAccountRoleTrade + 满位拒绝与过户失败退款回退 ---

@_async_test
async def test_rmb_role_trade_full_lifecycle_and_slot_full_refund():
    async with MallRmbTestRig(rmb_enabled=True) as rig:
        assert rig.rmb is not None
        rig.register_player(3001, "char_seller_acc", 1, home_zone=1)
        rig.register_player(3002, "char_buyer_acc", 1, home_zone=1)
        rig.register_player(3003, "char_full_acc", 1, home_zone=1)
        rig.ds_role_server.full_accounts.add("char_full_acc")

        # 1) 上架角色寄售 -> Begin -> Seal(Scene) -> Confirm -> 商城登记 LISTED
        create_req = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest",
            client_request_id="role-sell-0001",
            character_player_id=3001,
            price_fen=50000,
            title="Lv99 Swordmaster",
        )
        created = await rig.call_client(
            "CreateRmbConsignment", create_req, "trade.RmbConsignmentResponse", 3001, "char_seller_acc"
        )
        assert created.error_message.id == 0
        lid = int(created.listing.listing_id)
        assert created.listing.asset_kind == "CHARACTER"

        await rig.pump(8)
        assert lid in rig.ds_role_server.sealed_listings
        assert "/data_service.DataService/BeginAccountRoleTrade" in rig.ds_role_server.calls
        assert "/scene_node.SceneNodeGrpc/SealAccountRoleTrade" in rig.ds_role_server.calls
        assert "/data_service.DataService/ConfirmAccountRoleTradeFreeze" in rig.ds_role_server.calls
        assert rig.mall_server.rmb_listings[str(lid)]["state"] == "LISTED"

        # 2) 满位账号购买角色 -> CheckAccountRoleSlot 拦截返回 ERR_LOGIN_ACCOUNT_PLAYER_FULL (2001)
        blocked_buy = await rig.call_client(
            "BuyRmbConsignment",
            PROTOCOL.message("trade.BuyRmbConsignmentRequest", listing_id=lid, client_request_id="role-buy-full-01"),
            "trade.CommerceOrderResponse",
            3003,
            "char_full_acc",
        )
        assert blocked_buy.error_message.id == ERR_LOGIN_ACCOUNT_PLAYER_FULL
        assert "/data_service.DataService/CheckAccountRoleSlot" in rig.ds_role_server.calls

        # 3) 正常买家购买 -> 付款 -> RmbConsignments.tick() -> TransferAccountRole -> DELIVERED
        bought = await rig.call_client(
            "BuyRmbConsignment",
            PROTOCOL.message("trade.BuyRmbConsignmentRequest", listing_id=lid, client_request_id="role-buy-ok-01"),
            "trade.CommerceOrderResponse",
            3002,
            "char_buyer_acc",
        )
        assert bought.error_message.id == 0
        oid = bought.order.order_id
        rig.mall_server.mark_rmb_paid(oid)

        assert await rig.rmb.tick() == []
        await rig.pump(6)
        assert await rig.rmb.tick() == []
        assert "/data_service.DataService/TransferAccountRole" in rig.ds_role_server.calls
        assert rig.ds_role_server.roles[3001]["account"] == "char_buyer_acc"
        assert rig.ds_role_server.roles[3001]["rev"] == 2
        assert any(r["orderId"] == oid and r["state"] == "DELIVERED" for r in rig.mall_server.delivery_receipts)

        # 4) 过户时买家账号恰好满位 -> refundRole 调 CancelAccountRoleTrade -> rmb_refund -> REFUND_PENDING -> 回退收据
        rig.register_player(3004, "char_seller_2", 1, home_zone=1)
        rig.register_player(3005, "char_buyer_2", 1, home_zone=1)
        c2 = await rig.call_client(
            "CreateRmbConsignment",
            PROTOCOL.message(
                "trade.CreateRmbConsignmentRequest",
                client_request_id="role-sell-refund-02",
                character_player_id=3004,
                price_fen=20000,
                title="Lv80 Mage",
            ),
            "trade.RmbConsignmentResponse",
            3004,
            "char_seller_2",
        )
        lid2 = int(c2.listing.listing_id)
        await rig.pump(8)

        b2 = await rig.call_client(
            "BuyRmbConsignment",
            PROTOCOL.message("trade.BuyRmbConsignmentRequest", listing_id=lid2, client_request_id="role-buy-refund-02"),
            "trade.CommerceOrderResponse",
            3005,
            "char_buyer_2",
        )
        oid2 = b2.order.order_id
        # 付款后、过户前买家账号满位
        rig.ds_role_server.force_transfer_full_accounts.add("char_buyer_2")
        rig.mall_server.mark_rmb_paid(oid2)

        assert await rig.rmb.tick() == []
        await rig.pump(6)
        assert await rig.rmb.tick() == []  # 上报 REFUND_PENDING，商城入队回退
        assert "/data_service.DataService/CancelAccountRoleTrade" in rig.ds_role_server.calls
        assert any(r["orderId"] == oid2 and r["state"] == "REFUND_PENDING" for r in rig.mall_server.delivery_receipts)
        # 拉取回退意图 -> 推进到 rmb_returned -> 上报 returns/receipt
        assert await rig.rmb.tick() == []
        await rig.pump(4)
        assert await rig.rmb.tick() == []
        assert any(r["listingId"] == str(lid2) for r in rig.mall_server.rmb_return_receipts)

        # 5) 卖家主动取消在架角色寄售 -> CancelRmbConsignment -> CancelAccountRoleTrade -> rmb_returned -> returns/receipt
        rig.register_player(3006, "char_seller_3", 1, home_zone=1)
        c3 = await rig.call_client(
            "CreateRmbConsignment",
            PROTOCOL.message(
                "trade.CreateRmbConsignmentRequest",
                client_request_id="role-sell-cancel-03",
                character_player_id=3006,
                price_fen=15000,
                title="Lv70 Archer",
            ),
            "trade.RmbConsignmentResponse",
            3006,
            "char_seller_3",
        )
        lid3 = int(c3.listing.listing_id)
        await rig.pump(8)
        canc3 = await rig.call_client(
            "CancelRmbConsignment",
            PROTOCOL.message("trade.CancelRmbConsignmentRequest", listing_id=lid3),
            "trade.RmbConsignmentResponse",
            3006,
            "char_seller_3",
        )
        assert canc3.error_message.id == 0 and canc3.listing.state == "CANCEL_PENDING"
        await rig.pump(8)
        assert await rig.rmb.tick() == []
        assert rig.ds_role_server.roles[3006]["trade_listing_id"] == 0
        assert any(r["listingId"] == str(lid3) for r in rig.mall_server.rmb_return_receipts)


# -- 6. ContentGuard 标题审核（TEXT_SCENE_TRADE_TITLE=9）：敏感词拒、打码拒、不可用 fail-closed 拒、正常通过 --

@_async_test
async def test_rmb_consignment_content_guard_title_rejection_and_fail_closed():
    # 启动真实 ContentGuardService gRPC，加载含敏感词的词库
    cg_engine = ContentGuardEngine()
    cg_engine.swap(1, Lexicon(deny=(CgEntry("forbiddenword", CgCategory.AD, CgMode.SUB),)))
    cg_service = ContentGuardService(PROTOCOL, cg_engine, require_lexicon=True)
    cg_srv, cg_port = await serve_grpc(PROTOCOL, "127.0.0.1:0", cg_service.services(), pass_context=True)
    cg_client = GrpcClient(f"127.0.0.1:{cg_port}", timeout=2)
    try:
        checker = GrpcContentGuardChecker(PROTOCOL, cg_client.call)
        async with MallRmbTestRig(rmb_enabled=True, content_guard=checker) as rig:
            rig.register_player(4001, "cg_seller", 1, home_zone=1)

            # 1) 命中敏感词 -> 20016 (ERR_TRADE_TITLE_SENSITIVE)
            bad = PROTOCOL.message(
                "trade.CreateRmbConsignmentRequest",
                client_request_id="cg-reject-0001",
                price_fen=1000,
                title="Cheap forbiddenword here",
            )
            bad.assets.items[1001] = 1
            resp_bad = await rig.call_client(
                "CreateRmbConsignment", bad, "trade.RmbConsignmentResponse", 4001, "cg_seller"
            )
            assert resp_bad.error_message.id == ERR_TRADE_TITLE_SENSITIVE

            # 2) 空白标题 -> 1005 (ERR_INVALID_PARAMETER)
            blank = PROTOCOL.message(
                "trade.CreateRmbConsignmentRequest",
                client_request_id="cg-blank-0001",
                price_fen=1000,
                title="   ",
            )
            blank.assets.items[1001] = 1
            resp_blank = await rig.call_client(
                "CreateRmbConsignment", blank, "trade.RmbConsignmentResponse", 4001, "cg_seller"
            )
            assert resp_blank.error_message.id == ERR_INVALID_PARAMETER

            # 3) 干净标题 -> PASS 并成功创建寄售合同
            clean = PROTOCOL.message(
                "trade.CreateRmbConsignmentRequest",
                client_request_id="cg-pass-0001",
                price_fen=1000,
                title="Pure Spirit Herb",
            )
            clean.assets.items[1001] = 1
            resp_ok = await rig.call_client(
                "CreateRmbConsignment", clean, "trade.RmbConsignmentResponse", 4001, "cg_seller"
            )
            assert resp_ok.error_message.id == 0 and resp_ok.listing.listing_id > 0
    finally:
        await cg_client.close()
        await cg_srv.stop(0)

    # 4) MASKED 与 ContentGuard 不可用（fail-closed -> 20017）
    seen_scenes: list[int] = []

    class StubChecker:
        def __init__(self, mode: str) -> None:
            self.mode = mode

        async def check_text(self, scene: int, player_id: int, text: str) -> ContentCheckResult:
            del player_id, text
            seen_scenes.append(scene)
            if self.mode == "masked":
                return ContentCheckResult(verdict=TEXT_VERDICT_MASKED, category=2)
            if self.mode == "unavailable":
                raise TextCheckUnavailableError("down")
            return ContentCheckResult(verdict=TEXT_VERDICT_PASS, category=0)

    async with MallRmbTestRig(rmb_enabled=True, content_guard=StubChecker("masked")) as rig:
        rig.register_player(4002, "cg_seller_2", 1, home_zone=1)
        req = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest", client_request_id="cg-mask-0001", price_fen=1000, title="Mildly Bad"
        )
        req.assets.items[1001] = 1
        resp = await rig.call_client("CreateRmbConsignment", req, "trade.RmbConsignmentResponse", 4002, "cg_seller_2")
        assert resp.error_message.id == ERR_TRADE_TITLE_SENSITIVE
        assert seen_scenes[-1] == TEXT_SCENE_TRADE_TITLE

    async with MallRmbTestRig(rmb_enabled=True, content_guard=StubChecker("unavailable")) as rig:
        rig.register_player(4003, "cg_seller_3", 1, home_zone=1)
        req = PROTOCOL.message(
            "trade.CreateRmbConsignmentRequest", client_request_id="cg-down-0001", price_fen=1000, title="Normal Title"
        )
        req.assets.items[1001] = 1
        resp = await rig.call_client("CreateRmbConsignment", req, "trade.RmbConsignmentResponse", 4003, "cg_seller_3")
        assert resp.error_message.id == ERR_TRADE_TEXT_CHECK_UNAVAILABLE


# -- 7. SqlTradeStore 对官方撤销与 RMB workflow_proto 的 UNHEX 持久化验证 ------------------------------------

@_async_test
async def test_sql_trade_store_persists_official_and_rmb_workflows():
    executed: list[tuple[str, tuple]] = []

    class FakeCursor:
        def __init__(self) -> None:
            self.rowcount = 1
            self._rows = [(t,) for t in REQUIRED_TABLES]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def execute(self, sql: str, args=()):
            executed.append((sql, tuple(args) if args else ()))
            self._rows = [(t,) for t in REQUIRED_TABLES] if "SHOW TABLES" in sql else []

        async def fetchall(self):
            return self._rows

        async def fetchone(self):
            return self._rows[0] if self._rows else None

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        async def begin(self):
            return None

        async def commit(self):
            return None

        async def rollback(self):
            return None

    class FakePoolCtx:
        async def __aenter__(self):
            return FakeConn()

        async def __aexit__(self, *args):
            return None

    class FakePool:
        def acquire(self):
            return FakePoolCtx()

    sql_store = SqlTradeStore(FakePool(), PROTOCOL)
    rmb_wf = PROTOCOL.message("trade.TradeCommerceWorkflow", stage="rmb_listed")
    rmb_wf.rmb.seller_account = "acc_sql"
    rmb_wf.rmb.seller_owner_revision = 1
    rmb_wf.rmb.title = "SQL Persisted Sword"
    rmb_wf.rmb.asset_kind = "ITEMS"
    rmb_wf.rmb.price_fen = 8888
    raw_wf = rmb_wf.SerializeToString(deterministic=True)

    rec = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=7001,
        request_key="rmb_key_1",
        request_digest=b"\xab\xcd",
        kind=COMMERCE_KIND_RMB_CONSIGNMENT,
        status=1,
        seller_player_id=5001,
        unit_price=8888,
        workflow_proto=raw_wf,
    )

    async def _insert(tx):
        await tx.insert_commerce(rec)

    await sql_store.run_tx(_insert)
    inserts = [(s, a) for s, a in executed if s.startswith("INSERT INTO `trade_commerce`")]
    assert len(inserts) == 1
    sql_stmt, args = inserts[0]
    assert sql_stmt.count("UNHEX(%s)") == 2
    assert bytes.fromhex(args[18]) == raw_wf
