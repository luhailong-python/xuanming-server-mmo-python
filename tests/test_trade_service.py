"""trade 服务端到端 gRPC、P1 目录、核心交易引擎、资产通道、护栏、人工处置与 SQL 绑定测试。"""

from __future__ import annotations

import asyncio
import functools
import re
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

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
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.protocol import Protocol
from xuanming.trade import (
    ASSET_OP_SECRET_ENV,
    ASSET_STREAM_TRADE_CREDIT,
    ASSET_STREAM_TRADE_DEBIT,
    AssetOpCaller,
    AssetOpLoop,
    COMMERCE_CANCELLED,
    COMMERCE_DELIVERING,
    COMMERCE_ESCROWING,
    COMMERCE_KIND_AUCTION_LOT,
    COMMERCE_KIND_CONSIGNMENT,
    COMMERCE_MANUAL,
    COMMERCE_SETTLED,
    COORDINATOR_SECRET_ENV,
    CURRENCY_BIND_DIAMOND,
    CURRENCY_GOLD,
    CommerceEngine,
    DEFAULT_GRPC_PORT,
    ERR_ASSET_PARTIAL_APPLIED,
    ERR_INVALID_PARAMETER,
    ERR_TRADE_ASSET_DELIVERY_PENDING,
    ERR_TRADE_CANNOT_TRADE_SELF,
    ERR_TRADE_COUNTERPARTY_UNKNOWN,
    ERR_TRADE_FAVORITE_LIMIT_REACHED,
    ERR_TRADE_FEATURE_DISABLED,
    ERR_TRADE_LISTING_NOT_FOUND,
    ERR_TRADE_ORDER_NOT_FOUND,
    ERR_TRADE_REQUEST_CONFLICT,
    LISTING_PHASE_ENDED,
    LISTING_PHASE_ON_SALE,
    LISTING_PHASE_PUBLIC_NOTICE,
    LISTING_SECTION_AUCTION,
    LISTING_SECTION_CONSIGNMENT,
    LISTING_TAB_ON_SALE,
    LISTING_TAB_PUBLIC_NOTICE,
    MANUAL_RESOLUTION_CALLER,
    MARKET_SCOPE_GLOBAL,
    MARKET_SCOPE_ZONE,
    MarketLimits,
    MemoryTradeStore,
    MergedZones,
    OP_STATUS_ABORTED,
    OP_STATUS_APPLIED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_PENDING,
    REQUIRED_TABLES,
    ROLE_BLOCKER_ASSET_OP,
    ROLE_BLOCKER_COMMERCE,
    ROLE_FENCE_SECRET_ENV,
    SCHEMA_FILE,
    SceneTarget,
    SqlTradeStore,
    TRADE_NODE_TYPE,
    TX_AUCTION_BUY,
    TX_AUCTION_RETURN,
    TX_AUCTION_SELL,
    TradeConfig,
    TradePolicy,
    TradeService,
    decode_workflow,
)
from xuanming.trade.errors import RoleFenceUnavailableError

PROTOCOL = Protocol()

SECRET_COORDINATOR = b"role-fence-coordinator-secret-01234567"
SECRET_ROLE_FENCE_TRADE = b"role-fence-secret-for-trade-caller-012"
SECRET_ASSET_TRADE = b"asset-op-secret-for-trade-caller-01234"
SECRET_MANUAL_OPS = b"trade-manual-resolution-ops-secret-012"


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
    def __init__(self, start: int = 1000) -> None:
        self._next = start

    async def __call__(self) -> int:
        val = self._next
        self._next += 1
        return val


class FakeRoleDirectory:
    def __init__(self) -> None:
        self.states: dict[int, object] = {}
        self.unavailable = False

    def register(
        self,
        player_id: int,
        account: str = "acc_1",
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
        if self.unavailable:
            raise RoleFenceUnavailableError("directory unavailable")
        return self.states.get(player_id)


class FakeHomeZones:
    def __init__(self, mapping: dict[int, int] | None = None) -> None:
        self.mapping: dict[int, int] = dict(mapping or {})

    async def home_zone(self, player_id: int) -> int:
        return int(self.mapping.get(player_id, 0))


class FakeSceneAndLedger:
    """可控 Scene + SQL 已提交账本双模拟器：验签 HMAC、支持实例快照、部分发放、拒绝、中止与收据 ACK。"""

    def __init__(self, clock: FakeClock, secret: bytes = SECRET_ASSET_TRADE) -> None:
        self.clock = clock
        self.secret = secret
        self.ledgers: dict[int, object] = {}
        self.commit_to_sql: bool = True
        self.reject_credits_for: set[int] = set()
        self.partial_credits_for: set[int] = set()
        self.instance_snapshots: dict[int, object] = {}  # item_uuid -> AssetItemInstance
        self.calls: list[tuple[str, object]] = []
        self.credit_tx_types: list[tuple[int, int]] = []  # (player_id, tx_type)
        self.credit_bundles: list[tuple[int, object]] = []

    def _ledger(self, player_id: int):
        led = self.ledgers.get(player_id)
        if led is None:
            led = self.ledgers[player_id] = PROTOCOL.message("PlayerAssetOpLedgerComp")
        return led

    async def resolve(self, player_id: int):
        return SceneTarget(endpoint="fake-scene:1", call=self._handle_rpc), False

    async def read_sql_committed_ledger(self, player_id: int):
        if not self.commit_to_sql:
            return None
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
        self.calls.append((rpc, req))
        if not asset_authorized(rpc, req, {"trade": self.secret}, self.clock()):
            resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.UNKNOWN))
            resp.reason.id = 27008
            return resp.SerializeToString()

        pid = int(req.player_id)
        led = self._ledger(pid)
        if rpc == "ack_receipt":
            st = find_stream(led, int(req.stream))
            if st is not None:
                rcpt = find_receipt(st, int(req.seq))
                if rcpt is not None and rcpt.HasField("snapshot"):
                    rcpt.ClearField("snapshot")
            resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True)
            return resp.SerializeToString()

        if rpc == "abort_debit":
            record_ledger(led, req, Outcome.REJECTED, 0)
            resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.REJECTED), durable=True)
            return resp.SerializeToString()

        if rpc == "credit":
            self.credit_tx_types.append((pid, int(req.tx_type)))
            self.credit_bundles.append((pid, req.bundle))
            if pid in self.reject_credits_for and int(req.tx_type) != TX_AUCTION_RETURN:
                record_ledger(led, req, Outcome.REJECTED, 27005)
                resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.REJECTED), durable=True)
                resp.reason.id = 27005
                return resp.SerializeToString()
            if pid in self.partial_credits_for:
                record_ledger(led, req, Outcome.APPLIED, ERR_ASSET_PARTIAL_APPLIED, partial=True)
                resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True, partial=True)
                resp.reason.id = ERR_ASSET_PARTIAL_APPLIED
                return resp.SerializeToString()
            record_ledger(led, req, Outcome.APPLIED, 0)
            resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True)
            return resp.SerializeToString()

        # debit
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
                        instance_proto=f"raw-item-{uid}".encode(),
                    )
                snap.items.add().CopyFrom(inst)
        record_ledger(led, req, Outcome.APPLIED, 0, snapshot=snap)
        resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True)
        if snap is not None:
            resp.snapshot.CopyFrom(snap)
        return resp.SerializeToString()


class TradeTestRig:
    """启动真实 gRPC 服务（serve_grpc(..., pass_context=True)）与客户端封装。"""

    def __init__(
        self,
        *,
        market: MarketLimits = MarketLimits(scope="global", max_favorites_per_player=3),
        zones: MergedZones | None = None,
        mode: str = "dev",
        policy: TradePolicy | None = None,
        manual_secret: bytes = SECRET_MANUAL_OPS,
        manual_target: str = "test-global",
    ) -> None:
        self.clock = FakeClock()
        self.gate_key = Ed25519PrivateKey.generate()
        self.verifier = CallerVerifier([self.gate_key.public_key()], allowed_callers=("gate",))
        self.roles = FakeRoleDirectory()
        self.home_zones = FakeHomeZones()
        self.zones = zones or MergedZones()
        self.scene = FakeSceneAndLedger(self.clock)
        self.store = MemoryTradeStore(PROTOCOL, role_directory=self.roles, clock_ms=self.clock)
        self.caller = AssetOpCaller(
            PROTOCOL,
            self.scene,
            SECRET_ASSET_TRADE,
            requery_delays_sec=(),
            clock_ms=self.clock,
        )
        self.asset_loop = AssetOpLoop(
            PROTOCOL,
            self.store,
            self.caller,
            self.scene,
            clock_ms=self.clock,
            rnd=lambda: 0.5,
        )
        self.policy = policy or TradePolicy(
            items=(1001, 1002),
            item_titles={1001: "Spirit Herb", 1002: "Dragon Scale"},
            pets=(2001,),
            currencies=(CURRENCY_GOLD,),
            allow_instances=True,
        )
        self.commerce = CommerceEngine(
            PROTOCOL,
            self.store,
            commerce_ids=SeqIdGen(5000),
            op_ids=SeqIdGen(9000),
            policy=self.policy,
            asset_caller=self.caller,
            ledger_reader=self.scene,
            clock_ms=self.clock,
        )
        self.service = TradeService(
            PROTOCOL,
            self.store,
            caller_verifier=self.verifier,
            coordinator_secret=SECRET_COORDINATOR,
            home_zones=self.home_zones,
            listing_ids=SeqIdGen(100),
            commerce=self.commerce,
            market=market,
            zones_provider=lambda: self.zones,
            mode=mode,
            mall_url="",
            rmb_enabled=False,
            manual_resolution_secret=manual_secret,
            manual_resolution_environment=manual_target,
            clock_ms=self.clock,
        )
        self._stack: AsyncExitStack | None = None
        self.client: GrpcClient | None = None

    async def __aenter__(self) -> "TradeTestRig":
        self._stack = AsyncExitStack()
        await self._stack.__aenter__()
        server, port = await serve_grpc(PROTOCOL, "127.0.0.1:0", self.service.services(), pass_context=True)
        self._stack.push_async_callback(server.stop, 0)
        self.client = GrpcClient(f"127.0.0.1:{port}", timeout=5)
        self._stack.push_async_callback(self.client.close)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._stack is not None:
            await self._stack.__aexit__(exc_type, exc, tb)

    def register_player(self, player_id: int, account: str, owner_revision: int = 1, home_zone: int = 1) -> None:
        self.roles.register(player_id, account=account, owner_revision=owner_revision, home_zone=home_zone)
        self.home_zones.mapping[player_id] = home_zone

    def client_md(
        self,
        player_id: int,
        account: str,
        owner_revision: int,
        path: str,
        *,
        signer: Ed25519PrivateKey | None = None,
    ) -> tuple:
        details = PROTOCOL.message(
            "SessionDetails",
            session_id=101,
            gate_instance_id="gate-1",
            session_incarnation=1,
            player_id=player_id,
            authenticated_account=account,
            owner_revision=owner_revision,
        )
        return session_metadata(details.SerializeToString(), path, signer or self.gate_key)

    async def call_client(
        self,
        method: str,
        req_msg,
        resp_name: str,
        player_id: int,
        account: str,
        owner_revision: int = 1,
        *,
        signer: Ed25519PrivateKey | None = None,
    ):
        assert self.client is not None
        path = f"/trade.ClientPlayerJubaozhai/{method}"
        md = self.client_md(player_id, account, owner_revision, path, signer=signer)
        raw = await self.client.call(path, req_msg.SerializeToString(), metadata=md)
        return PROTOCOL.parse(resp_name, raw)

    async def call_admin(self, method: str, req_msg, resp_name: str, *, metadata: tuple = ()):
        assert self.client is not None
        path = f"/trade.TradeAdmin/{method}"
        raw = await self.client.call(path, req_msg.SerializeToString(), metadata=metadata)
        return PROTOCOL.parse(resp_name, raw)

    async def call_role_fence(self, method: str, req_msg, resp_name: str, *, metadata: tuple = ()):
        assert self.client is not None
        path = f"/rolefence.RoleFenceParticipant/{method}"
        raw = await self.client.call(path, req_msg.SerializeToString(), metadata=metadata)
        return PROTOCOL.parse(resp_name, raw)

    async def pump(self, rounds: int = 6) -> None:
        """交替驱动合同推进与资产通道重投直至收敛。"""
        for _ in range(rounds):
            await self.commerce.tick()
            await self.asset_loop.tick()
            await self.commerce.tick()


# -- 1. P1 Catalog: SeedListing / BrowseListings / GetListingDetail / SetFavorite / GetMyShelf ----------

@_async_test
async def test_p1_catalog_phase_merged_zones_and_favorites():
    # 区 2 已合入区 1：zone 模式下 1 与 2 互通，3 隔离。
    zones = MergedZones({2: 1})
    async with TradeTestRig(
        market=MarketLimits(scope="zone", default_page_size=10, max_page_size=20, max_favorites_per_player=2),
        zones=zones,
        mode="dev",
    ) as rig:
        rig.register_player(10, "acc_s1", 1, home_zone=1)
        rig.register_player(11, "acc_s2", 1, home_zone=2)
        rig.register_player(12, "acc_s3", 1, home_zone=3)
        rig.register_player(20, "acc_b1", 1, home_zone=1)

        s1 = await rig.call_admin(
            "SeedListing",
            PROTOCOL.message(
                "trade.SeedListingRequest",
                seller_player_id=10,
                category=3,  # WEAPON
                subcategory=1,
                title="Azure Blade",
                level=80,
                price_fen=5000,
                summary="Epic sword",
                description="Forged in flame",
                icon_key="weapon_01",
                notice_duration_ms=10_000,
                sale_duration_ms=60_000,
            ),
            "trade.SeedListingResponse",
        )
        assert s1.error_message.id == 0 and s1.listing_id > 0 and s1.market_zone == 1

        s2 = await rig.call_admin(
            "SeedListing",
            PROTOCOL.message(
                "trade.SeedListingRequest",
                seller_player_id=11,
                category=3,
                subcategory=1,
                title="Merged Zone Blade",
                level=90,
                price_fen=6000,
                notice_duration_ms=0,
                sale_duration_ms=60_000,
            ),
            "trade.SeedListingResponse",
        )
        assert s2.error_message.id == 0 and s2.market_zone == 2

        s3 = await rig.call_admin(
            "SeedListing",
            PROTOCOL.message(
                "trade.SeedListingRequest",
                seller_player_id=12,
                category=3,
                subcategory=1,
                title="Foreign Zone Blade",
                level=95,
                price_fen=7000,
                notice_duration_ms=0,
                sale_duration_ms=60_000,
            ),
            "trade.SeedListingResponse",
        )
        assert s3.error_message.id == 0 and s3.market_zone == 3

        # 公示期浏览：看到 s1 (PUBLIC_NOTICE)，看不到已进入寄售的 s2 与别区 s3
        pub = await rig.call_client(
            "BrowseListings",
            PROTOCOL.message(
                "trade.BrowseListingsRequest",
                tab=LISTING_TAB_PUBLIC_NOTICE,
                section=LISTING_SECTION_CONSIGNMENT,
                category=3,
            ),
            "trade.BrowseListingsResponse",
            20,
            "acc_b1",
        )
        assert pub.error_message.id == 0
        assert pub.market_scope == MARKET_SCOPE_ZONE
        assert [x.listing_id for x in pub.listings] == [s1.listing_id]
        assert pub.listings[0].phase == LISTING_PHASE_PUBLIC_NOTICE

        # 竞价分区在 P1 目录返回 20003
        auct = await rig.call_client(
            "BrowseListings",
            PROTOCOL.message(
                "trade.BrowseListingsRequest",
                tab=LISTING_TAB_ON_SALE,
                section=LISTING_SECTION_AUCTION,
                category=3,
            ),
            "trade.BrowseListingsResponse",
            20,
            "acc_b1",
        )
        assert auct.error_message.id == ERR_TRADE_FEATURE_DISABLED

        # 推进时间越过 s1 公示期：s1 和合服同区的 s2 均可见，别区 s3 不可见
        rig.clock.advance(15_000)
        sale = await rig.call_client(
            "BrowseListings",
            PROTOCOL.message(
                "trade.BrowseListingsRequest",
                tab=LISTING_TAB_ON_SALE,
                section=LISTING_SECTION_CONSIGNMENT,
                category=3,
            ),
            "trade.BrowseListingsResponse",
            20,
            "acc_b1",
        )
        assert {x.listing_id for x in sale.listings} == {s1.listing_id, s2.listing_id}
        assert all(x.phase == LISTING_PHASE_ON_SALE and x.market_zone == 1 for x in sale.listings)

        # 别区商品详情对买家表现为不存在（20000），不泄露别区商品
        det_foreign = await rig.call_client(
            "GetListingDetail",
            PROTOCOL.message("trade.GetListingDetailRequest", listing_id=s3.listing_id),
            "trade.GetListingDetailResponse",
            20,
            "acc_b1",
        )
        assert det_foreign.error_message.id == ERR_TRADE_LISTING_NOT_FOUND

        # 本区商品详情与收藏上限（max_favorites_per_player=2）
        det1 = await rig.call_client(
            "GetListingDetail",
            PROTOCOL.message("trade.GetListingDetailRequest", listing_id=s1.listing_id),
            "trade.GetListingDetailResponse",
            20,
            "acc_b1",
        )
        assert det1.error_message.id == 0 and det1.detail.description == "Forged in flame"

        f1 = await rig.call_client(
            "SetFavorite",
            PROTOCOL.message("trade.SetFavoriteRequest", listing_id=s1.listing_id, favorite=True),
            "trade.SetFavoriteResponse",
            20,
            "acc_b1",
        )
        f2 = await rig.call_client(
            "SetFavorite",
            PROTOCOL.message("trade.SetFavoriteRequest", listing_id=s2.listing_id, favorite=True),
            "trade.SetFavoriteResponse",
            20,
            "acc_b1",
        )
        assert f1.favorite is True and f2.favorite is True

        # 第 3 条收藏触发软上限 20002
        s4 = await rig.call_admin(
            "SeedListing",
            PROTOCOL.message(
                "trade.SeedListingRequest",
                seller_player_id=10,
                category=3,
                title="Third Sword",
                price_fen=1000,
                sale_duration_ms=60_000,
            ),
            "trade.SeedListingResponse",
        )
        f3 = await rig.call_client(
            "SetFavorite",
            PROTOCOL.message("trade.SetFavoriteRequest", listing_id=s4.listing_id, favorite=True),
            "trade.SetFavoriteResponse",
            20,
            "acc_b1",
        )
        assert f3.error_message.id == ERR_TRADE_FAVORITE_LIMIT_REACHED

        # 卖家货架在寄售过期后仍可见且 phase 推导为 ENDED
        rig.clock.advance(100_000)
        shelf = await rig.call_client(
            "GetMyShelf",
            PROTOCOL.message("trade.GetMyShelfRequest", page=1, page_size=10),
            "trade.GetMyShelfResponse",
            10,
            "acc_s1",
        )
        assert shelf.error_message.id == 0 and shelf.total_count == 2
        assert all(x.is_mine and x.phase == LISTING_PHASE_ENDED for x in shelf.listings)


# -- 2. Consignment 全生命周期：实例快照转移 + 先买家后卖家 + 自动退回(TX_AUCTION_RETURN=30) + 托管中撤单 + 幂等冲突 --

@_async_test
async def test_consignment_full_lifecycle_snapshot_and_ack_receipts():
    async with TradeTestRig() as rig:
        rig.register_player(100, "seller_acc", 1, home_zone=1)
        rig.register_player(200, "buyer_acc", 1, home_zone=1)
        rig.scene.instance_snapshots[7777] = PROTOCOL.message(
            "AssetItemInstance",
            item_uuid=7777,
            config_id=1001,
            stack_size=1,
            instance_proto=b"unique-sword-blob-v1",
        )

        # 上架唯一实例装备
        create_req = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="consign-req-0001",
            currency_type=CURRENCY_GOLD,
            price=500,
            duration_ms=300_000,
        )
        create_req.assets.item_uuids.append(7777)
        created = await rig.call_client(
            "CreateConsignment", create_req, "trade.CommerceOrderResponse", 100, "seller_acc"
        )
        assert created.error_message.id == 0
        cid = int(created.order.order_id)

        # 同 client_request_id 相同载荷重放幂等，修改价格触发 20006 冲突
        replay = await rig.call_client(
            "CreateConsignment", create_req, "trade.CommerceOrderResponse", 100, "seller_acc"
        )
        assert replay.error_message.id == 0 and int(replay.order.order_id) == cid
        conflict_req = PROTOCOL.message("trade.CreateConsignmentRequest")
        conflict_req.CopyFrom(create_req)
        conflict_req.price = 999
        conflict = await rig.call_client(
            "CreateConsignment", conflict_req, "trade.CommerceOrderResponse", 100, "seller_acc"
        )
        assert conflict.error_message.id == ERR_TRADE_REQUEST_CONFLICT

        # 推进托管扣款至 open，并验证 escrow_snapshot 落库与 AckReceipts 释放 Scene 收据
        await rig.pump()
        offers = await rig.call_client(
            "BrowseCommerceOffers",
            PROTOCOL.message("trade.BrowseCommerceOffersRequest", kind=COMMERCE_KIND_CONSIGNMENT, limit=10),
            "trade.BrowseCommerceOffersResponse",
            200,
            "buyer_acc",
        )
        assert offers.error_message.id == 0 and len(offers.offers) == 1
        assert offers.offers[0].item_instances[0].instance_id == 7777

        rec = await rig.store.get_commerce(cid)
        w = decode_workflow(PROTOCOL, rec)
        debit_op_id = int(w.legs[0].op_id)
        debit_op = rig.store.get_asset_op_record(debit_op_id)
        assert debit_op.escrow_snapshot != b""
        assert debit_op.receipt_ack_pending is False  # AckReceipts 已在 pump 中完成

        # 买家下单购买 -> 推进 buyer_debit -> 进入 deliver（验证先买家、后卖家顺序）
        bought = await rig.call_client(
            "BuyConsignment",
            PROTOCOL.message("trade.BuyConsignmentRequest", listing_id=str(cid), client_request_id="buy-req-0001"),
            "trade.CommerceOrderResponse",
            200,
            "buyer_acc",
        )
        assert bought.error_message.id == 0

        # 逐步推进，确认 buyer_receive 发出时 seller_receive 尚未发出（op_id == 0）
        await rig.commerce.tick()  # 发 buyer_debit
        await rig.asset_loop.tick()  # buyer_debit APPLIED
        await rig.commerce.tick()  # 进入 deliver，仅发 buyer_receive
        rec_mid = await rig.store.get_commerce(cid)
        w_mid = decode_workflow(PROTOCOL, rec_mid)
        buyer_leg = next(l for l in w_mid.legs if l.role == "buyer_receive")
        seller_leg = next(l for l in w_mid.legs if l.role == "seller_receive")
        assert int(buyer_leg.op_id) != 0 and int(seller_leg.op_id) == 0

        # 继续推进完成交付，确认买家收到完整 item_instances 原件（而非 item_uuids 或 config_id 重铸）
        await rig.pump()
        final = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=str(cid)),
            "trade.CommerceOrderResponse",
            200,
            "buyer_acc",
        )
        assert final.error_message.id == 0 and final.order.state == COMMERCE_SETTLED
        buyer_credits = [b for pid, b in rig.scene.credit_bundles if pid == 200]
        assert len(buyer_credits) == 1
        assert len(buyer_credits[0].item_instances) == 1
        assert buyer_credits[0].item_instances[0].instance_proto == b"unique-sword-blob-v1"
        assert len(buyer_credits[0].item_uuids) == 0


@_async_test
async def test_consignment_auto_unwind_and_cancel_during_escrow():
    async with TradeTestRig() as rig:
        rig.register_player(101, "seller_acc", 1, home_zone=1)
        rig.register_player(201, "buyer_acc", 1, home_zone=1)

        # 场景 A：买家被获取封禁 -> buyer_receive 终局拒绝 -> 自动整单退回（TX_AUCTION_RETURN=30）
        req = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="unwind-consign-01",
            currency_type=CURRENCY_GOLD,
            price=300,
            duration_ms=300_000,
        )
        req.assets.items[1001] = 2
        created = await rig.call_client("CreateConsignment", req, "trade.CommerceOrderResponse", 101, "seller_acc")
        cid = int(created.order.order_id)
        await rig.pump()

        rig.scene.reject_credits_for.add(201)
        await rig.call_client(
            "BuyConsignment",
            PROTOCOL.message("trade.BuyConsignmentRequest", listing_id=str(cid), client_request_id="unwind-buy-01"),
            "trade.CommerceOrderResponse",
            201,
            "buyer_acc",
        )
        await rig.pump(8)
        got = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=str(cid)),
            "trade.CommerceOrderResponse",
            101,
            "seller_acc",
        )
        assert got.order.state == COMMERCE_CANCELLED
        # 确认两笔退回均使用 TX_AUCTION_RETURN (30)
        return_txs = [(pid, tx) for pid, tx in rig.scene.credit_tx_types if tx == TX_AUCTION_RETURN]
        assert (101, TX_AUCTION_RETURN) in return_txs and (201, TX_AUCTION_RETURN) in return_txs

        # 场景 B：托管阶段撤单 -> 截止 seller_debit -> AssetAbortDebit -> ABORTED -> Cancelled
        req2 = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="cancel-escrow-01",
            currency_type=CURRENCY_GOLD,
            price=400,
            duration_ms=300_000,
        )
        req2.assets.items[1001] = 1
        c2 = await rig.call_client("CreateConsignment", req2, "trade.CommerceOrderResponse", 101, "seller_acc")
        cid2 = int(c2.order.order_id)
        await rig.commerce.tick()  # 发出 seller_debit（未投递 Scene）
        cancelled = await rig.call_client(
            "CancelConsignment",
            PROTOCOL.message("trade.CancelConsignmentRequest", listing_id=str(cid2)),
            "trade.CommerceOrderResponse",
            101,
            "seller_acc",
        )
        assert cancelled.error_message.id == 0
        await rig.pump(6)
        rec2 = await rig.store.get_commerce(cid2)
        assert int(rec2.status) == COMMERCE_CANCELLED
        w2 = decode_workflow(PROTOCOL, rec2)
        assert int(w2.legs[0].outcome) == OP_STATUS_ABORTED


# -- 3. P2P 双方交易 ------------------------------------------------------------------------------------

@_async_test
async def test_p2p_player_trade_lifecycle():
    async with TradeTestRig() as rig:
        rig.register_player(301, "p2p_seller", 1, home_zone=1)
        rig.register_player(302, "p2p_buyer", 1, home_zone=1)

        # 未知对手回 20009，自交易回 20008
        bad_buyer = PROTOCOL.message("trade.CreatePlayerTradeRequest", client_request_id="p2p-bad-0001", buyer_player_id=9999)
        bad_buyer.seller_assets.items[1001] = 1
        bad_buyer.buyer_assets.currency_type = CURRENCY_GOLD
        bad_buyer.buyer_assets.currency_amount = 100
        r_unk = await rig.call_client("CreatePlayerTrade", bad_buyer, "trade.CommerceOrderResponse", 301, "p2p_seller")
        assert r_unk.error_message.id == ERR_TRADE_COUNTERPARTY_UNKNOWN

        self_req = PROTOCOL.message("trade.CreatePlayerTradeRequest", client_request_id="p2p-self-001", buyer_player_id=301)
        self_req.seller_assets.items[1001] = 1
        self_req.buyer_assets.currency_type = CURRENCY_GOLD
        self_req.buyer_assets.currency_amount = 100
        r_self = await rig.call_client("CreatePlayerTrade", self_req, "trade.CommerceOrderResponse", 301, "p2p_seller")
        assert r_self.error_message.id == ERR_TRADE_CANNOT_TRADE_SELF

        req = PROTOCOL.message("trade.CreatePlayerTradeRequest", client_request_id="p2p-ok-00001", buyer_player_id=302)
        req.seller_assets.items[1001] = 3
        req.buyer_assets.currency_type = CURRENCY_GOLD
        req.buyer_assets.currency_amount = 250
        created = await rig.call_client("CreatePlayerTrade", req, "trade.CommerceOrderResponse", 301, "p2p_seller")
        assert created.error_message.id == 0
        oid = created.order.order_id

        # 卖家托管完成进入 confirm
        await rig.pump()
        c_buyer = await rig.call_client(
            "ConfirmPlayerTrade",
            PROTOCOL.message("trade.ConfirmPlayerTradeRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            302,
            "p2p_buyer",
        )
        assert c_buyer.error_message.id == 0 and c_buyer.order.buyer_confirmed is True

        # 买家托管完成回到 confirm -> 卖家最终确认 -> 交付结算
        await rig.pump()
        c_seller = await rig.call_client(
            "ConfirmPlayerTrade",
            PROTOCOL.message("trade.ConfirmPlayerTradeRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            301,
            "p2p_seller",
        )
        assert c_seller.error_message.id == 0 and c_seller.order.seller_confirmed is True

        await rig.pump(8)
        done = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=oid),
            "trade.CommerceOrderResponse",
            301,
            "p2p_seller",
        )
        assert done.order.state == COMMERCE_SETTLED


# -- 4. 材料限价盘口撮合、on_book=1 与深度 --------------------------------------------------------------

@_async_test
async def test_market_order_matching_on_book_and_depth():
    async with TradeTestRig() as rig:
        rig.register_player(401, "mk_sell", 1, home_zone=1)
        rig.register_player(402, "mk_buy", 1, home_zone=1)

        markets = await rig.call_client(
            "ListTradeMarkets", PROTOCOL.message("trade.ListTradeMarketsRequest"), "trade.ListTradeMarketsResponse", 401, "mk_sell"
        )
        assert markets.error_message.id == 0 and len(markets.markets) == 2

        # 卖单挂出 10 个 @100（托管未完成前 on_book=0，不进深度）
        sell_resp = await rig.call_client(
            "PlaceMarketOrder",
            PROTOCOL.message(
                "trade.PlaceMarketOrderRequest",
                client_request_id="mk-sell-0001",
                market_id=1,
                item_config_id=1001,
                side=2,
                quantity=10,
                unit_price=100,
                currency_type=CURRENCY_GOLD,
            ),
            "trade.MarketOrderResponse",
            401,
            "mk_sell",
        )
        assert sell_resp.error_message.id == 0
        sell_id = int(sell_resp.order.market_order_id)

        depth_before = await rig.call_client(
            "GetMarketDepth",
            PROTOCOL.message("trade.GetMarketDepthRequest", market_id=1, item_config_id=1001, limit=10),
            "trade.GetMarketDepthResponse",
            402,
            "mk_buy",
        )
        assert len(depth_before.asks) == 0  # on_book=0 不计入盘口深度

        # 推进托管完成后 on_book=1，深度可见 10@100
        await rig.pump()
        depth_open = await rig.call_client(
            "GetMarketDepth",
            PROTOCOL.message("trade.GetMarketDepthRequest", market_id=1, item_config_id=1001, limit=10),
            "trade.GetMarketDepthResponse",
            402,
            "mk_buy",
        )
        assert [(a.unit_price, a.quantity) for a in depth_open.asks] == [(100, 10)]

        # 买单 4 个 @120（按 maker 价格 100 成交，退回差价 4*20=80 给买家，剩余卖单 6@100）
        buy_resp = await rig.call_client(
            "PlaceMarketOrder",
            PROTOCOL.message(
                "trade.PlaceMarketOrderRequest",
                client_request_id="mk-buy-00001",
                market_id=1,
                item_config_id=1001,
                side=1,
                quantity=4,
                unit_price=120,
                currency_type=CURRENCY_GOLD,
            ),
            "trade.MarketOrderResponse",
            402,
            "mk_buy",
        )
        assert buy_resp.error_message.id == 0
        await rig.pump(8)

        depth_after = await rig.call_client(
            "GetMarketDepth",
            PROTOCOL.message("trade.GetMarketDepthRequest", market_id=1, item_config_id=1001, limit=10),
            "trade.GetMarketDepthResponse",
            402,
            "mk_buy",
        )
        assert [(a.unit_price, a.quantity) for a in depth_after.asks] == [(100, 6)]
        assert (402, TX_AUCTION_RETURN) in rig.scene.credit_tx_types  # buyer_surplus

        # 撤销剩余 6 个卖单 -> 退回剩余 6 个材料
        cancel_sell = await rig.call_client(
            "CancelMarketOrder",
            PROTOCOL.message("trade.CancelMarketOrderRequest", market_order_id=str(sell_id)),
            "trade.MarketOrderResponse",
            401,
            "mk_sell",
        )
        assert cancel_sell.error_message.id == 0
        await rig.pump(6)
        sell_rec = await rig.store.get_commerce(sell_id)
        assert int(sell_rec.status) == COMMERCE_CANCELLED


# -- 5. 拍卖：上架 -> 出价(20013) -> 超价退款 -> 到期成交结算 -------------------------------------------

@_async_test
async def test_auction_lot_bid_outbid_refund_and_expiry_settle():
    async with TradeTestRig() as rig:
        rig.register_player(501, "auc_sell", 1, home_zone=1)
        rig.register_player(502, "auc_b1", 1, home_zone=1)
        rig.register_player(503, "auc_b2", 1, home_zone=1)

        lot_req = PROTOCOL.message(
            "trade.CreateAuctionLotRequest",
            client_request_id="auc-lot-0001",
            currency_type=CURRENCY_GOLD,
            opening_price=100,
            minimum_increment=20,
            duration_ms=120_000,
        )
        lot_req.assets.items[1002] = 1
        created = await rig.call_client("CreateAuctionLot", lot_req, "trade.AuctionLotResponse", 501, "auc_sell")
        assert created.error_message.id == 0
        lot_id = created.lot.lot_id
        await rig.pump()

        # 出价 1：100 -> 立即回 20013 (async pending)
        b1 = await rig.call_client(
            "PlaceAuctionBid",
            PROTOCOL.message("trade.PlaceAuctionBidRequest", lot_id=lot_id, client_request_id="auc-bid-0001", amount=100),
            "trade.AuctionLotResponse",
            502,
            "auc_b1",
        )
        assert b1.error_message.id == ERR_TRADE_ASSET_DELIVERY_PENDING
        await rig.pump()

        lot_v1 = await rig.call_client(
            "GetAuctionLot", PROTOCOL.message("trade.GetAuctionLotRequest", lot_id=lot_id), "trade.AuctionLotResponse", 501, "auc_sell"
        )
        assert lot_v1.lot.highest_bid == 100 and lot_v1.lot.highest_bidder == 502

        # 出价 2：150 超价 -> b1 退款 (TX_AUCTION_RETURN=30)，b2 成为最高出价人
        b2 = await rig.call_client(
            "PlaceAuctionBid",
            PROTOCOL.message("trade.PlaceAuctionBidRequest", lot_id=lot_id, client_request_id="auc-bid-0002", amount=150),
            "trade.AuctionLotResponse",
            503,
            "auc_b2",
        )
        assert b2.error_message.id == ERR_TRADE_ASSET_DELIVERY_PENDING
        await rig.pump(8)
        assert (502, TX_AUCTION_RETURN) in rig.scene.credit_tx_types

        # 推进时间到期 -> 拍品与获胜出价双双 Settled
        rig.clock.advance(130_000)
        await rig.pump(10)
        lot_end = await rig.call_client(
            "GetAuctionLot", PROTOCOL.message("trade.GetAuctionLotRequest", lot_id=lot_id), "trade.AuctionLotResponse", 501, "auc_sell"
        )
        assert lot_end.lot.state == "done" and lot_end.lot.highest_bidder == 503
        lot_rec = await rig.store.get_commerce(int(lot_id))
        assert int(lot_rec.status) == COMMERCE_SETTLED
        win_bid_id = int(decode_workflow(PROTOCOL, lot_rec).winning_bid_id)
        win_bid = await rig.store.get_commerce(win_bid_id)
        assert int(win_bid.status) == COMMERCE_SETTLED


# -- 6. AssetOpLoop SQL 账本强制门禁 + 粘性 27007 转人工 + TradeAdmin 人工处置 --------------------------

@_async_test
async def test_asset_op_sql_gate_sticky_partial_27007_and_manual_resolution():
    async with TradeTestRig() as rig:
        rig.register_player(601, "man_sell", 1, home_zone=1)
        rig.register_player(602, "man_buy", 1, home_zone=1)

        req = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="sql-gate-001",
            currency_type=CURRENCY_GOLD,
            price=200,
            duration_ms=300_000,
        )
        req.assets.items[1001] = 1
        created = await rig.call_client("CreateConsignment", req, "trade.CommerceOrderResponse", 601, "man_sell")
        cid = int(created.order.order_id)

        # 关闭 SQL 账本可见性：即使 Scene 返回 APPLIED+durable=True，AssetOpLoop 绝不终结
        rig.scene.commit_to_sql = False
        await rig.commerce.tick()
        await rig.asset_loop.tick()
        rec = await rig.store.get_commerce(cid)
        debit_op_id = int(decode_workflow(PROTOCOL, rec).legs[0].op_id)
        assert int(rig.store.get_asset_op_record(debit_op_id).status) == OP_STATUS_PENDING

        # 恢复 SQL 已提交账本后立刻终结并上架
        rig.scene.commit_to_sql = True
        rig.clock.advance(1_000)
        await rig.pump()

        # 买家购买，且 Scene 对买家回 partial=True (27007) -> 合同转入 Manual (10)
        rig.scene.partial_credits_for.add(602)
        await rig.call_client(
            "BuyConsignment",
            PROTOCOL.message("trade.BuyConsignmentRequest", listing_id=str(cid), client_request_id="partial-buy-01"),
            "trade.CommerceOrderResponse",
            602,
            "man_buy",
        )
        await rig.pump(8)
        manual_rec = await rig.store.get_commerce(cid)
        assert int(manual_rec.status) == COMMERCE_MANUAL

        # TradeAdmin.ListManualContracts（合法 ops-commerce HMAC）
        list_req = PROTOCOL.message("trade.ListManualContractsRequest", limit=10)
        sign_service_call(
            "/trade.TradeAdmin/ListManualContracts",
            list_req,
            MANUAL_RESOLUTION_CALLER,
            rig.clock(),
            SECRET_MANUAL_OPS,
        )
        listed = await rig.call_admin("ListManualContracts", list_req, "trade.ListManualContractsResponse")
        assert len(listed.contracts) == 1 and int(listed.contracts[0].commerce_id) == cid
        assert listed.target == "test-global"

        # ResolveManualContract SHOW & target_mismatch REFUSED
        show_req = PROTOCOL.message("trade.ResolveManualContractRequest", commerce_id=cid, mode=1)
        sign_service_call(
            "/trade.TradeAdmin/ResolveManualContract",
            show_req,
            MANUAL_RESOLUTION_CALLER,
            rig.clock(),
            SECRET_MANUAL_OPS,
        )
        shown = await rig.call_admin("ResolveManualContract", show_req, "trade.ResolveManualContractResponse")
        assert shown.verdict == 1 and int(shown.record.commerce_id) == cid
        assert any(a.action == "partial" and a.allowed for a in shown.actions)

        bad_target = PROTOCOL.message(
            "trade.ResolveManualContractRequest",
            commerce_id=cid,
            mode=2,
            action="resume",
            expect_version=int(manual_rec.version),
            expect_target="wrong-env",
            operator="oncall",
            reason="ticket-1",
        )
        sign_service_call(
            "/trade.TradeAdmin/ResolveManualContract",
            bad_target,
            MANUAL_RESOLUTION_CALLER,
            rig.clock(),
            SECRET_MANUAL_OPS,
        )
        refused = await rig.call_admin("ResolveManualContract", bad_target, "trade.ResolveManualContractResponse")
        assert refused.verdict == 3 and refused.refusal == "target_mismatch"

        # 构造一条因卖家所得退回被拒而转人工的合同，测试 ResolveManualContract EXECUTE (reissue) 并收敛到 Settled
        req2 = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="reissue-exec-01",
            currency_type=CURRENCY_GOLD,
            price=250,
            duration_ms=300_000,
        )
        req2.assets.items[1001] = 1
        c2 = await rig.call_client("CreateConsignment", req2, "trade.CommerceOrderResponse", 601, "man_sell")
        cid2 = int(c2.order.order_id)
        await rig.pump()

        rig.scene.partial_credits_for.discard(602)
        rig.scene.reject_credits_for.add(601)  # 卖家收款被拒 -> 货已交付、卖家所得失败 -> 转 Manual
        await rig.call_client(
            "BuyConsignment",
            PROTOCOL.message("trade.BuyConsignmentRequest", listing_id=str(cid2), client_request_id="reissue-buy-01"),
            "trade.CommerceOrderResponse",
            602,
            "man_buy",
        )
        await rig.pump(8)
        m2 = await rig.store.get_commerce(cid2)
        assert int(m2.status) == COMMERCE_MANUAL

        # 解封卖家并通过 TradeAdmin.ResolveManualContract EXECUTE (reissue) 补发卖家所得 -> 最终 Settled
        rig.scene.reject_credits_for.discard(601)
        exec_req = PROTOCOL.message(
            "trade.ResolveManualContractRequest",
            commerce_id=cid2,
            mode=3,  # EXECUTE
            action="reissue",
            expect_version=int(m2.version),
            expect_target="test-global",
            operator="oncall_ops",
            reason="seller unblocked ticket-42",
        )
        sign_service_call(
            "/trade.TradeAdmin/ResolveManualContract",
            exec_req,
            MANUAL_RESOLUTION_CALLER,
            rig.clock(),
            SECRET_MANUAL_OPS,
        )
        exec_resp = await rig.call_admin("ResolveManualContract", exec_req, "trade.ResolveManualContractResponse")
        assert exec_resp.verdict == 1 and "seller_receive#2" in exec_resp.plan.new_legs
        assert "[CommerceManual]" in exec_resp.audit_line and "executed=True" in exec_resp.audit_line

        await rig.pump(8)
        m2_final = await rig.store.get_commerce(cid2)
        assert int(m2_final.status) == COMMERCE_SETTLED
        assert len(decode_workflow(PROTOCOL, m2_final).manual_actions) == 1


@_async_test
async def test_asset_op_two_segment_list_due_and_sticky_partial_27007():
    clock = FakeClock()
    store = MemoryTradeStore(PROTOCOL, clock_ms=clock)
    now = clock()
    await store.ensure_seq_rows(now, [(10, ASSET_STREAM_TRADE_CREDIT)])

    # 插入一条老行 (attempts=5, next_attempt_ms 更早) 与一条新行 (attempts=0, next_attempt_ms 较晚)
    async def _seed_ops(tx):
        aged = PROTOCOL.message(
            "trade.TradeAssetOpRecord",
            op_id=101,
            player_id=10,
            stream=ASSET_STREAM_TRADE_CREDIT,
            stream_epoch=now,
            seq=1,
            kind=2,
            status=OP_STATUS_PENDING,
            attempts=5,
            next_attempt_ms=now - 5_000,
            tx_type=TX_AUCTION_BUY,
            ref_kind=2,
            ref_id=1,
            created_ms=now - 10_000,
            updated_ms=now - 5_000,
        )
        fresh = PROTOCOL.message(
            "trade.TradeAssetOpRecord",
            op_id=102,
            player_id=10,
            stream=ASSET_STREAM_TRADE_CREDIT,
            stream_epoch=now,
            seq=2,
            kind=2,
            status=OP_STATUS_PENDING,
            attempts=0,
            next_attempt_ms=now - 1_000,
            tx_type=TX_AUCTION_BUY,
            ref_kind=2,
            ref_id=2,
            created_ms=now - 1_000,
            updated_ms=now - 1_000,
        )
        await tx.insert_asset_op(aged)
        await tx.insert_asset_op(fresh)

    await store.run_tx(_seed_ops)

    # limit=1 时两段扫描必选新行 (102)，不被老行 (101) 饿死；limit=2 时新行排在老行前
    assert await store.list_due_ops(now, 1) == [102]
    assert await store.list_due_ops(now, 2) == [102, 101]

    # 粘性 27007 测试：首次 Scene 回 partial=True 但 SQL 未提交 -> 重排记住 last_reason=27007；
    # 即使后续 SQL 账本未标 partial_seqs，终结仍保持 OP_STATUS_APPLIED_PARTIAL (5)
    scene = FakeSceneAndLedger(clock)
    scene.commit_to_sql = False
    scene.partial_credits_for.add(10)
    caller = AssetOpCaller(PROTOCOL, scene, SECRET_ASSET_TRADE, requery_delays_sec=(), clock_ms=clock)
    loop = AssetOpLoop(PROTOCOL, store, caller, scene, clock_ms=clock, rnd=lambda: 0.5)

    assert await loop.claim_and_process(102) is True
    mid = store.get_asset_op_record(102)
    assert int(mid.status) == OP_STATUS_PENDING and int(mid.last_reason) == ERR_ASSET_PARTIAL_APPLIED

    # 恢复 SQL 提交（且即便模拟 partial 标记丢失），粘性 27007 仍将 op 终结为 APPLIED_PARTIAL (5)
    scene.commit_to_sql = True
    st = find_stream(scene.ledgers[10], ASSET_STREAM_TRADE_CREDIT)
    del st.partial_seqs[:]
    clock.advance(2_000)
    assert await loop.claim_and_process(102) is True
    final_op = store.get_asset_op_record(102)
    assert int(final_op.status) == OP_STATUS_APPLIED_PARTIAL


@_async_test
async def test_role_fence_prepare_blocked_by_pending_asset_op():
    async with TradeTestRig() as rig:
        rig.register_player(810, "rf_op_acc", 1, home_zone=1)
        now = rig.clock()
        await rig.store.ensure_seq_rows(now, [(810, ASSET_STREAM_TRADE_DEBIT)])

        async def _insert_pending(tx):
            await tx.insert_asset_op(
                PROTOCOL.message(
                    "trade.TradeAssetOpRecord",
                    op_id=9999,
                    player_id=810,
                    stream=ASSET_STREAM_TRADE_DEBIT,
                    stream_epoch=now,
                    seq=1,
                    kind=1,
                    status=OP_STATUS_PENDING,
                    next_attempt_ms=now,
                    tx_type=TX_AUCTION_SELL,
                    ref_kind=2,
                    ref_id=1,
                    created_ms=now,
                    updated_ms=now,
                )
            )

        await rig.store.run_tx(_insert_pending)
        prep = PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", dry_run=True)
        prep.state.CopyFrom(
            PROTOCOL.message(
                "rolefence.RoleFenceState",
                player_id=810,
                account="rf_op_acc",
                owner_revision=1,
                freeze_token=123,
                version=2,
                home_zone=1,
            )
        )
        sign_service_call(
            "/rolefence.RoleFenceParticipant/PrepareRoleFreeze", prep, "coordinator", rig.clock(), SECRET_COORDINATOR
        )
        resp = await rig.call_role_fence("PrepareRoleFreeze", prep, "rolefence.PrepareRoleFreezeResponse")
        assert resp.clean is False and resp.blocker == ROLE_BLOCKER_ASSET_OP


# -- 7. 角色归属隐私过滤 + OwnershipInterceptor ---------------------------------------------------------

@_async_test
async def test_ownership_privacy_filtering_and_interceptor():
    async with TradeTestRig() as rig:
        rig.register_player(701, "old_owner", 1, home_zone=1)
        rig.register_player(702, "peer_acc", 1, home_zone=1)

        # 旧主下了一笔材料买单
        mk = await rig.call_client(
            "PlaceMarketOrder",
            PROTOCOL.message(
                "trade.PlaceMarketOrderRequest",
                client_request_id="priv-mk-0001",
                market_id=1,
                item_config_id=1001,
                side=1,
                quantity=2,
                unit_price=50,
                currency_type=CURRENCY_GOLD,
            ),
            "trade.MarketOrderResponse",
            701,
            "old_owner",
            owner_revision=1,
        )
        assert mk.error_message.id == 0
        mk_id = mk.order.market_order_id

        # 角色过户给新主（owner_revision=2, version=2）
        new_fence = rig.roles.register(701, account="new_owner", owner_revision=2, version=2, home_zone=1)
        apply_req = PROTOCOL.message("rolefence.ApplyRoleFenceRequest")
        apply_req.state.CopyFrom(new_fence)
        sign_service_call(
            "/rolefence.RoleFenceParticipant/ApplyRoleFence",
            apply_req,
            "coordinator",
            rig.clock(),
            SECRET_COORDINATOR,
        )
        await rig.call_role_fence("ApplyRoleFence", apply_req, "rolefence.ApplyRoleFenceResponse")

        # 旧主会话在 OwnershipInterceptor 被拒 PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc_old:
            await rig.call_client(
                "ListMarketOrders",
                PROTOCOL.message("trade.ListMarketOrdersRequest", limit=10),
                "trade.ListMarketOrdersResponse",
                701,
                "old_owner",
                owner_revision=1,
            )
        assert exc_old.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 新主会话放行，但看不到旧主的私有订单与挂单历史
        got = await rig.call_client(
            "GetCommerceOrder",
            PROTOCOL.message("trade.GetCommerceOrderRequest", order_id=mk_id),
            "trade.CommerceOrderResponse",
            701,
            "new_owner",
            owner_revision=2,
        )
        assert got.error_message.id == ERR_TRADE_ORDER_NOT_FOUND

        listed_orders = await rig.call_client(
            "ListCommerceOrders",
            PROTOCOL.message("trade.ListCommerceOrdersRequest", limit=10),
            "trade.ListCommerceOrdersResponse",
            701,
            "new_owner",
            owner_revision=2,
        )
        assert listed_orders.error_message.id == 0 and len(listed_orders.orders) == 0

        listed_mk = await rig.call_client(
            "ListMarketOrders",
            PROTOCOL.message("trade.ListMarketOrdersRequest", limit=10),
            "trade.ListMarketOrdersResponse",
            701,
            "new_owner",
            owner_revision=2,
        )
        assert listed_mk.error_message.id == 0 and len(listed_mk.orders) == 0

        # 目录不可达导致归属预检无法判定 -> ABORTED
        rig.roles.unavailable = True
        with pytest.raises(grpc.aio.AioRpcError) as exc_inc:
            await rig.call_client(
                "ListMarketOrders",
                PROTOCOL.message("trade.ListMarketOrdersRequest", limit=10),
                "trade.ListMarketOrdersResponse",
                8888,
                "unknown_acc",
                owner_revision=1,
            )
        assert exc_inc.value.code() == grpc.StatusCode.ABORTED


# -- 8. RoleFenceParticipant: PrepareRoleFreeze / ApplyRoleFence / PurgeRemovedRole --------------------

@_async_test
async def test_role_fence_participant_freeze_and_purge():
    async with TradeTestRig() as rig:
        rig.register_player(801, "rf_acc", 1, home_zone=1)

        # 造一条收藏和一条活跃寄售合同
        s1 = await rig.call_admin(
            "SeedListing",
            PROTOCOL.message(
                "trade.SeedListingRequest",
                seller_player_id=801,
                category=3,
                title="RF Sword",
                price_fen=1000,
                sale_duration_ms=60_000,
            ),
            "trade.SeedListingResponse",
        )
        await rig.call_client(
            "SetFavorite",
            PROTOCOL.message("trade.SetFavoriteRequest", listing_id=s1.listing_id, favorite=True),
            "trade.SetFavoriteResponse",
            801,
            "rf_acc",
        )
        assert await rig.store.count_favorites(801) == 1

        c_req = PROTOCOL.message(
            "trade.CreateConsignmentRequest",
            client_request_id="rf-consign-001",
            currency_type=CURRENCY_GOLD,
            price=100,
            duration_ms=300_000,
        )
        c_req.assets.items[1001] = 1
        created = await rig.call_client("CreateConsignment", c_req, "trade.CommerceOrderResponse", 801, "rf_acc")
        cid = int(created.order.order_id)

        # 存在活跃合同时 PrepareRoleFreeze 被阻塞 (blocker="trade_commerce")
        prep = PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", dry_run=True)
        prep.state.CopyFrom(
            PROTOCOL.message(
                "rolefence.RoleFenceState",
                player_id=801,
                account="rf_acc",
                owner_revision=1,
                freeze_token=999,
                version=2,
                home_zone=1,
            )
        )
        sign_service_call(
            "/rolefence.RoleFenceParticipant/PrepareRoleFreeze", prep, "coordinator", rig.clock(), SECRET_COORDINATOR
        )
        resp_blk = await rig.call_role_fence("PrepareRoleFreeze", prep, "rolefence.PrepareRoleFreezeResponse")
        assert resp_blk.clean is False and resp_blk.blocker == ROLE_BLOCKER_COMMERCE

        # 推进托管并撤单结案后，PrepareRoleFreeze 通过（clean=True）
        await rig.pump()
        await rig.call_client(
            "CancelConsignment",
            PROTOCOL.message("trade.CancelConsignmentRequest", listing_id=str(cid)),
            "trade.CommerceOrderResponse",
            801,
            "rf_acc",
        )
        await rig.pump(8)

        prep_real = PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", dry_run=False)
        prep_real.state.CopyFrom(prep.state)
        sign_service_call(
            "/rolefence.RoleFenceParticipant/PrepareRoleFreeze", prep_real, "coordinator", rig.clock(), SECRET_COORDINATOR
        )
        resp_ok = await rig.call_role_fence("PrepareRoleFreeze", prep_real, "rolefence.PrepareRoleFreezeResponse")
        assert resp_ok.clean is True

        # PurgeRemovedRole 墓碑：只清理 trade_favorite，绝不删 trade_player_op_seq 和 trade_commerce
        purge = PROTOCOL.message("rolefence.PurgeRemovedRoleRequest")
        purge.state.CopyFrom(
            PROTOCOL.message("rolefence.RoleFenceState", player_id=801, account="", owner_revision=0, version=3)
        )
        sign_service_call(
            "/rolefence.RoleFenceParticipant/PurgeRemovedRole", purge, "coordinator", rig.clock(), SECRET_COORDINATOR
        )
        resp_purge = await rig.call_role_fence("PurgeRemovedRole", purge, "rolefence.PurgeRemovedRoleResponse")
        assert resp_purge.retry is False
        assert await rig.store.count_favorites(801) == 0
        assert (801, ASSET_STREAM_TRADE_DEBIT) in rig.store._state.op_seqs
        assert await rig.store.get_commerce(cid) is not None


# -- 9. 官方商城(2) + 人民币寄售(11) 关闭时均回 20003 kTradeFeatureDisabled ------------------------------

@_async_test
async def test_disabled_official_mall_and_all_11_rmb_rpcs_return_20003():
    async with TradeTestRig() as rig:
        rig.register_player(901, "rmb_acc", 1, home_zone=1)
        cases = [
            ("ListOfficialSkus", "trade.ListOfficialSkusRequest", "trade.ListOfficialSkusResponse"),
            ("CreateOfficialOrder", "trade.CreateOfficialOrderRequest", "trade.CommerceOrderResponse"),
            ("CreateRmbConsignment", "trade.CreateRmbConsignmentRequest", "trade.RmbConsignmentResponse"),
            ("GetRmbConsignment", "trade.GetRmbConsignmentRequest", "trade.RmbConsignmentResponse"),
            ("ListRmbConsignments", "trade.ListRmbConsignmentsRequest", "trade.ListRmbConsignmentsResponse"),
            ("BuyRmbConsignment", "trade.BuyRmbConsignmentRequest", "trade.CommerceOrderResponse"),
            ("CancelRmbConsignment", "trade.CancelRmbConsignmentRequest", "trade.RmbConsignmentResponse"),
            ("GetRmbOrder", "trade.GetCommerceOrderRequest", "trade.CommerceOrderResponse"),
            ("ListRmbOrders", "trade.ListCommerceOrdersRequest", "trade.ListCommerceOrdersResponse"),
            ("CancelRmbOrder", "trade.CancelCommerceOrderRequest", "trade.CommerceOrderResponse"),
            ("ListSellerIncomes", "trade.ListSellerIncomesRequest", "trade.ListSellerIncomesResponse"),
            ("CreateWithdrawal", "trade.CreateWithdrawalRequest", "trade.WithdrawalResponse"),
            ("GetWithdrawal", "trade.GetWithdrawalRequest", "trade.WithdrawalResponse"),
        ]
        for method, req_name, resp_name in cases:
            resp = await rig.call_client(method, PROTOCOL.message(req_name), resp_name, 901, "rmb_acc")
            assert resp.error_message.id == ERR_TRADE_FEATURE_DISABLED, f"{method} expected 20003"


# -- 10. Gate Ed25519 / HMAC / 非客户端会话拒绝 ----------------------------------------------------------

@_async_test
async def test_gate_and_hmac_auth_fail_closed():
    async with TradeTestRig(mode="pro", manual_secret=b"") as rig:
        rig.register_player(950, "auth_acc", 1, home_zone=1)
        assert rig.client is not None

        # 缺失会话调用 ClientPlayerJubaozhai -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc_missing:
            await rig.client.call(
                "/trade.ClientPlayerJubaozhai/GetMyShelf",
                PROTOCOL.message("trade.GetMyShelfRequest").SerializeToString(),
            )
        assert exc_missing.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 错误私钥签名 -> UNAUTHENTICATED
        wrong_key = Ed25519PrivateKey.generate()
        with pytest.raises(grpc.aio.AioRpcError) as exc_bad_sig:
            await rig.call_client(
                "GetMyShelf",
                PROTOCOL.message("trade.GetMyShelfRequest"),
                "trade.GetMyShelfResponse",
                950,
                "auth_acc",
                signer=wrong_key,
            )
        assert exc_bad_sig.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # 带客户端会话调 TradeAdmin / RoleFenceParticipant -> PERMISSION_DENIED
        admin_md = rig.client_md(950, "auth_acc", 1, "/trade.TradeAdmin/SeedListing")
        with pytest.raises(grpc.aio.AioRpcError) as exc_admin_sess:
            await rig.call_admin(
                "SeedListing", PROTOCOL.message("trade.SeedListingRequest"), "trade.SeedListingResponse", metadata=admin_md
            )
        assert exc_admin_sess.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # pro 模式调 SeedListing -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc_pro:
            await rig.call_admin(
                "SeedListing", PROTOCOL.message("trade.SeedListingRequest"), "trade.SeedListingResponse"
            )
        assert exc_pro.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 未配密钥调 ListManualContracts -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc_manual_off:
            await rig.call_admin(
                "ListManualContracts",
                PROTOCOL.message("trade.ListManualContractsRequest", limit=10),
                "trade.ListManualContractsResponse",
            )
        assert exc_manual_off.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 错误 coordinator 密钥调 PrepareRoleFreeze -> PERMISSION_DENIED
        bad_rf = PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", dry_run=True)
        bad_rf.state.CopyFrom(PROTOCOL.message("rolefence.RoleFenceState", player_id=950, account="auth_acc", owner_revision=1, version=1))
        sign_service_call(
            "/rolefence.RoleFenceParticipant/PrepareRoleFreeze",
            bad_rf,
            "coordinator",
            rig.clock(),
            b"wrong-coordinator-secret-01234567890",
        )
        with pytest.raises(grpc.aio.AioRpcError) as exc_rf:
            await rig.call_role_fence("PrepareRoleFreeze", bad_rf, "rolefence.PrepareRoleFreezeResponse")
        assert exc_rf.value.code() == grpc.StatusCode.PERMISSION_DENIED


# -- 11. SqlTradeStore DDL、UNHEX 绑定、READ COMMITTED 与永不删 trade_player_op_seq 守卫 -----------------

def test_sql_trade_store_ddl_unhex_and_never_deletes_seq_rows():
    ddl = SCHEMA_FILE.read_text(encoding="utf-8")
    for table in REQUIRED_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS `{table}`" in ddl or f"CREATE TABLE IF NOT EXISTS {table}" in ddl
    assert "`on_book`" in ddl
    assert "`escrow_snapshot`" in ddl
    assert "`receipt_ack_pending`" in ddl
    assert "`business_consumed`" in ddl

    sql_src = Path(__file__).resolve().parents[1] / "src" / "xuanming" / "trade" / "sql_store.py"
    text = sql_src.read_text(encoding="utf-8")
    assert "SET TRANSACTION ISOLATION LEVEL READ COMMITTED" in text
    assert "UNHEX(%s)" in text
    # 机械守卫：源码中绝不允许对 trade_player_op_seq 执行 DELETE / TRUNCATE / DROP
    assert not re.search(r"\b(DELETE|TRUNCATE|DROP)\b[^\n;]*trade_player_op_seq", text, re.IGNORECASE)


@_async_test
async def test_sql_trade_store_recorded_bindings_with_fake_pool():
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
            if "SHOW TABLES" in sql:
                self._rows = [(t,) for t in REQUIRED_TABLES]
            else:
                self._rows = []

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
    await sql_store.verify_schema()

    rec = PROTOCOL.message(
        "trade.TradeCommerceRecord",
        commerce_id=1,
        request_key="k1",
        request_digest=b"\x00\xff\xab",
        kind=4,
        status=3,
        side=1,
        workflow_proto=PROTOCOL.message("trade.TradeCommerceWorkflow", stage="market_open").SerializeToString(),
    )

    async def _insert(tx):
        await tx.insert_commerce(rec)

    await sql_store.run_tx(_insert)
    insert_calls = [(s, a) for s, a in executed if s.startswith("INSERT INTO `trade_commerce`")]
    assert len(insert_calls) == 1
    sql_stmt, args = insert_calls[0]
    assert sql_stmt.count("UNHEX(%s)") == 2
    assert args[2] == "00ffab"  # request_digest hex
    assert args[-1] == 1  # on_book projected from workflow stage="market_open"


# -- 12. TradeConfig 校验（端口 50800、NodeType=12、canary、禁用绑定币） ---------------------------------

def test_trade_config_defaults_canary_and_validation():
    assert DEFAULT_GRPC_PORT == 50800
    assert TRADE_NODE_TYPE == 12
    env = {
        ASSET_OP_SECRET_ENV: SECRET_ASSET_TRADE.decode(),
        COORDINATOR_SECRET_ENV: SECRET_COORDINATOR.decode(),
        ROLE_FENCE_SECRET_ENV: SECRET_ROLE_FENCE_TRADE.decode(),
    }
    settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="canary",
        data_service="127.0.0.1:50100",
        redis="redis://127.0.0.1:6379/0",
        etcd="127.0.0.1:2379",
        trade_node={
            "node_id": 109,
            "advertise_host": "127.0.0.1",
            "commerce_items": [1001],
            "commerce_item_titles": {"1001": "Herb"},
            "commerce_currencies": [CURRENCY_GOLD],
        },
    )
    cfg = TradeConfig.from_settings(settings, environ=env)
    assert cfg.grpc_port == 50800
    assert cfg.runs_background_work is False

    # 绑定灵石禁售
    bad_settings = SimpleNamespace(
        **{
            **settings.__dict__,
            "trade_node": {
                "node_id": 109,
                "advertise_host": "127.0.0.1",
                "commerce_currencies": [CURRENCY_BIND_DIAMOND],
            },
        }
    )
    with pytest.raises(ValueError, match="non-tradable currency"):
        TradeConfig.from_settings(bad_settings, environ=env)
