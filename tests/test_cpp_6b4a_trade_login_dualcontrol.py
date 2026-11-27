"""对照 C++ 仓 60b0bd6b3..6b4ae7852 增量移植回归测试：

覆盖范围：
1. 共享双人复核（``xuanming.dualcontrol``）与 Mail / Rank / PlayerInbox 严格模式门禁；
2. 聚宝斋悬赏求书求购单（582/583/584）完整托管/揭榜/税费/拒收绑定书/撤单/整单退回全额 + 人工处置双人复核；
3. 登录通行证回调（574 HandlePassportEvent）、LoginAdmin 双人复核、令牌家族按签发时刻吊销与票据账号绑定；
4. 聊天持久化平台补审与 RetractWorldChat 运维口令鉴权 + ContentGuard 折叠态广告/变体联系方式检测。
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from login_fakes import FakeRedis

from xuanming.chat.config import ADMIN_TOKEN_METADATA_KEY, resolve_admin_token
from xuanming.chat.model import (
    ActorIdentity,
    CHANNEL_WORLD,
    ChatRules,
    REVIEW_PASS,
    REVIEW_REJECT,
    REVIEW_UNAVAILABLE,
    ReviewResult,
)
from xuanming.chat.service import ChatService
from xuanming.chat.store import (
    MemoryChatStore,
    PLATFORM_RECHECK_LEASE_MS,
    RECHECK_GAVE_UP,
    RECHECK_PASSED,
    RECHECK_RETRACTED,
)
from xuanming.content_guard.contact import contains_ad
from xuanming.content_guard.engine import Engine as ContentGuardEngine, SceneClass, Verdict
from xuanming.content_guard.textnorm import fold
from xuanming.content_guard.wordlist import Category, Lexicon
from xuanming.dualcontrol import (
    ApprovalInvalidError,
    ApprovalMissingError,
    validate_admin_approval,
)
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata, sign_service_call
from xuanming.login import keys as login_keys, queue as login_queue, tips as login_tips, tokens as login_tokens
from xuanming.login.caller_auth import CallerVerifier
from xuanming.login.config import LoginConfig
from xuanming.login.directory import DataService
from xuanming.login.passport import (
    PASSPORT_AUTH_TYPE,
    PassportSessionEnforcer,
    PassportUserStatus,
    passport_account,
)
from xuanming.login.service import LoginService, ServiceContext
from xuanming.login.sessions import PlayerLocator
from xuanming.login.tokens import AccessRejected, TokenManager
from xuanming.mail.service import MailService
from xuanming.mail.store import MemoryMailStore
from xuanming.player_inbox.registry import InboxRegistry
from xuanming.player_inbox.service import PlayerInboxService
from xuanming.player_inbox.store import MemoryInboxStore
from xuanming.protocol import Protocol
from xuanming.rank.service import RankService
from xuanming.rank.store import MemoryRankStore
from xuanming.trade.commerce_engine import CommerceEngine, decode_workflow, find_leg
from xuanming.trade.errors import (
    ERR_INVALID_PARAMETER,
    ERR_TRADE_ASSET_NOT_TRADABLE,
    ERR_TRADE_CANNOT_TRADE_SELF,
    ERR_TRADE_REQUEST_CONFLICT,
    ERR_TRADE_WANTED_BOOK_MISMATCH,
    ERR_TRADE_WANTED_ORDER_LIMIT_REACHED,
)
from xuanming.trade.model import (
    CURRENCY_BIND_GOLD,
    CURRENCY_GOLD,
    MANUAL_RESOLUTION_CALLER,
    OP_STATUS_APPLIED,
    OP_STATUS_REJECTED,
    SEALED_SKILL_BOOK_CONFIG_ID,
    TradePolicy,
    WANTED_PHASE_CANCELLED,
    WANTED_PHASE_ESCROWING,
    WANTED_PHASE_FULFILLING,
    WANTED_PHASE_OPEN,
    WANTED_PHASE_SETTLED,
    WantedSkillInfo,
    wanted_seller_net_gold,
    wanted_tax_gold,
)
from xuanming.trade.service import TradeService
from xuanming.trade.store import MemoryTradeStore


# ---------------------------------------------------------------------------
# 1. Shared Dual-Control + Mail / Rank / PlayerInbox 严格模式验证
# ---------------------------------------------------------------------------


def _mk_approval(
    protocol: Protocol,
    *,
    approval_id: str = "9007199254740993",
    proposer: str = "alice.w",
    approver: str = "bob@corp.example",
    approved_at_ms: int = 1_800_000_000_000,
):
    return protocol.message(
        "common.base.AdminApproval",
        approval_id=approval_id,
        proposer=proposer,
        approver=approver,
        approved_at_ms=approved_at_ms,
    )


def test_dualcontrol_validation_rules_and_fail_closed():
    protocol = Protocol()
    valid = _mk_approval(protocol)

    # 1) 合格双人复核通过（含字节级区分大小写：alice.w != ALICE.W）
    assert validate_admin_approval(valid, operator="alice.w", request_id=9007199254740993) is True
    case_diff = _mk_approval(protocol, approver="ALICE.W")
    assert validate_admin_approval(case_diff, operator="alice.w") is True

    # 2) 缺失/全空记录：strict 模式拒绝，allow_unapproved=True 放行并返回 False
    empty_msg = protocol.message("common.base.AdminApproval")
    for absent in (None, empty_msg):
        with pytest.raises(ApprovalMissingError):
            validate_admin_approval(absent, operator="alice.w", allow_unapproved=False)
        assert validate_admin_approval(absent, operator="alice.w", allow_unapproved=True) is False

    # 3) allow_unapproved=True 时若带了部分/不完整批准记录，必须依然严格 fail-closed 拒绝！
    partial = protocol.message("common.base.AdminApproval", proposer="alice.w")
    with pytest.raises(ApprovalInvalidError):
        validate_admin_approval(partial, operator="alice.w", allow_unapproved=True)

    # 4) 各类不合格记录（自批、错误 approval_id、不匹配 operator、缺时间戳、非法字符、请求号不符）
    bad_cases = [
        _mk_approval(protocol, approval_id=""),
        _mk_approval(protocol, approval_id="0123"),
        _mk_approval(protocol, approval_id="12345678901234567890"),
        _mk_approval(protocol, approval_id="12a"),
        _mk_approval(protocol, proposer=""),
        _mk_approval(protocol, proposer="alice w"),
        _mk_approval(protocol, approver=""),
        _mk_approval(protocol, approver="a" * 65),
        _mk_approval(protocol, approved_at_ms=0),
        _mk_approval(protocol, approver="alice.w"),  # 自批自己
    ]
    for bad in bad_cases:
        with pytest.raises(ApprovalInvalidError):
            validate_admin_approval(bad, operator=bad.proposer, allow_unapproved=True)

    with pytest.raises(ApprovalInvalidError):
        validate_admin_approval(valid, operator="mallory")
    with pytest.raises(ApprovalInvalidError):
        validate_admin_approval(valid, operator="", require_operator=True)
    with pytest.raises(ApprovalInvalidError):
        validate_admin_approval(valid, operator="alice.w", request_id=9007199254740994)


def test_mail_rank_player_inbox_strict_dualcontrol():
    async def exercise():
        protocol = Protocol()
        now_ms = 1_800_000_000_000
        clock = lambda: now_ms
        sec_admin = b"a" * 32
        sec_sys = b"s" * 32
        sec_coord = b"c" * 32
        admin_tok = "inbox-admin-token-32bytes-secret"

        # -- MailService (strict allow_unapproved=False) --
        mail_store = MemoryMailStore(protocol, clock_ms=clock)
        mail_svc = MailService(
            protocol,
            mail_store,
            caller_verifier=None,
            producer_secrets={"admin": sec_admin, "rank": sec_sys},
            coordinator_secret=sec_coord,
            allow_unapproved=False,
            clock_ms=clock,
        )
        m_server, m_port = await serve_grpc(protocol, "127.0.0.1:0", mail_svc.services(), pass_context=True)
        m_client = GrpcClient(f"127.0.0.1:{m_port}")
        try:
            m_path = "/mailpb.MailAdmin/SendPersonalMail"
            # admin 无批准记录 -> PERMISSION_DENIED
            req_no_app = protocol.message(
                "mailpb.SendMailRequest", request_id=7001, recipient_player_id=101, title="hi", body="body"
            )
            sign_service_call(m_path, req_no_app, "admin", now_ms, sec_admin)
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await m_client.call(m_path, req_no_app.SerializeToString())
            assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # admin 带合格批准记录且 approval_id == str(request_id) -> 成功
            req_ok = protocol.message(
                "mailpb.SendMailRequest",
                request_id=7002,
                recipient_player_id=101,
                title="hi",
                body="body",
                approval=_mk_approval(protocol, approval_id="7002"),
            )
            sign_service_call(m_path, req_ok, "admin", now_ms, sec_admin)
            raw_ok = await m_client.call(m_path, req_ok.SerializeToString())
            resp_ok = protocol.parse("mailpb.SendMailResponse", raw_ok)
            assert resp_ok.error_message.id == 0 and resp_ok.mail_id > 0

            # 系统发件方 rank 无需批准记录照常放行
            req_sys = protocol.message(
                "mailpb.SendMailRequest", request_id=7003, recipient_player_id=101, title="rank", body="reward"
            )
            sign_service_call(m_path, req_sys, "rank", now_ms, sec_sys)
            raw_sys = await m_client.call(m_path, req_sys.SerializeToString())
            assert protocol.parse("mailpb.SendMailResponse", raw_sys).error_message.id == 0
        finally:
            await m_client.close()
            await m_server.stop(None)

        # -- RankService (strict allow_unapproved=False) --
        rank_store = MemoryRankStore(protocol)
        rank_svc = RankService(
            protocol,
            rank_store,
            producer_secrets={"admin": sec_admin, "match": sec_sys},
            allow_unapproved=False,
            clock_ms=clock,
        )
        r_server, r_port = await serve_grpc(protocol, "127.0.0.1:0", rank_svc.services(), pass_context=True)
        r_client = GrpcClient(f"127.0.0.1:{r_port}")
        try:
            r_path = "/rankpb.RankAdmin/RemoveRankEntry"
            r_no_app = protocol.message(
                "rankpb.RemoveRankEntryRequest", board_id=1, generation=1, entity_id=101, request_id=8001
            )
            sign_service_call(r_path, r_no_app, "admin", now_ms, sec_admin)
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await r_client.call(r_path, r_no_app.SerializeToString())
            assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 自批 -> PERMISSION_DENIED
            r_self = protocol.message(
                "rankpb.RemoveRankEntryRequest",
                board_id=1,
                generation=1,
                entity_id=101,
                request_id=8002,
                approval=_mk_approval(protocol, approval_id="8002", proposer="alice.w", approver="alice.w"),
            )
            sign_service_call(r_path, r_self, "admin", now_ms, sec_admin)
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await r_client.call(r_path, r_self.SerializeToString())
            assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # match 自动删角无需批准记录照常放行
            r_match = protocol.message(
                "rankpb.RemoveRankEntryRequest", board_id=1, generation=1, entity_id=101, request_id=8003
            )
            sign_service_call(r_path, r_match, "match", now_ms, sec_sys)
            await r_client.call(r_path, r_match.SerializeToString())
        finally:
            await r_client.close()
            await r_server.stop(None)

        # -- PlayerInboxService (strict allow_unapproved=False) --
        class _FakeInboxDir:
            async def get_role_fence_state(self, player_id: int):
                return protocol.message(
                    "rolefence.RoleFenceState",
                    player_id=player_id,
                    account=f"acc_{player_id}",
                    owner_revision=1,
                    version=1,
                )

        inbox_store = MemoryInboxStore(protocol, InboxRegistry(protocol), _FakeInboxDir(), clock_ms=clock)
        inbox_svc = PlayerInboxService(
            protocol,
            inbox_store,
            scene_secret=sec_sys,
            coordinator_secret=sec_coord,
            admin_token=admin_tok,
            allow_unapproved=False,
            clock_ms=clock,
        )
        i_server, i_port = await serve_grpc(protocol, "127.0.0.1:0", inbox_svc.services(), pass_context=True)
        i_client = GrpcClient(f"127.0.0.1:{i_port}")
        try:
            md = (("x-admin-token", admin_tok),)
            s_path = "/player_inbox.PlayerInboxAdmin/GmSanctionPlayer"
            sanction_no_app = protocol.message(
                "player_inbox.GmSanctionPlayerRequest",
                player_id=101,
                request_id=9001,
                issued_at_ms=now_ms,
                operator="alice.w",
                reason="abuse",
                block_gain={"currency_type": 0, "until_ms": now_ms + 3600_000},
            )
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await i_client.call(s_path, sanction_no_app.SerializeToString(), metadata=md)
            assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED

            sanction_ok = protocol.message(
                "player_inbox.GmSanctionPlayerRequest",
                player_id=101,
                request_id=9002,
                issued_at_ms=now_ms,
                operator="alice.w",
                reason="abuse",
                block_gain={"currency_type": 0, "until_ms": now_ms + 3600_000},
                approval=_mk_approval(protocol, approval_id="9002", proposer="alice.w", approver="bob.k"),
            )
            raw_s = await i_client.call(s_path, sanction_ok.SerializeToString(), metadata=md)
            s_resp = protocol.parse("player_inbox.GmSanctionPlayerResponse", raw_s)
            assert s_resp.error_message.id == 0 and s_resp.created is True

            # VoidItem 无批准记录 -> PERMISSION_DENIED
            v_path = "/player_inbox.PlayerInboxAdmin/VoidItem"
            void_no_app = protocol.message(
                "player_inbox.VoidPlayerInboxItemRequest",
                player_id=101,
                domain=2,
                seq=s_resp.seq,
                reason=1,
                operator="alice.w",
                note="undo",
            )
            with pytest.raises(grpc.aio.AioRpcError) as err:
                await i_client.call(v_path, void_no_app.SerializeToString(), metadata=md)
            assert err.value.code() == grpc.StatusCode.PERMISSION_DENIED
        finally:
            await i_client.close()
            await i_server.stop(None)

    asyncio.run(exercise())


# ---------------------------------------------------------------------------
# 2. Jubaozhai Wanted Skill Book Orders (582..584) + Manual Contract Ops 双人复核
# ---------------------------------------------------------------------------


class _FakeWantedCatalog:
    def __init__(self) -> None:
        self._skills = {
            1001: (WantedSkillInfo(scroll_id=1, skill_name="青莲剑诀", scroll_name="剑道初卷"), True),
            1002: (WantedSkillInfo(scroll_id=2, skill_name="玄冥掌法", scroll_name="掌法初卷"), True),
            9999: (WantedSkillInfo(scroll_id=9, skill_name="稀世绝学", scroll_name="稀世秘卷"), False),
        }

    def wanted_skill(self, skill_table_id: int) -> tuple[WantedSkillInfo, bool]:
        return self._skills.get(int(skill_table_id), (WantedSkillInfo(), False))


def _sealed_book_snapshot(
    protocol: Protocol,
    *,
    item_uuid: int,
    skill_table_id: int,
    bound: bool = False,
    rarity_tier: int = 0,
    config_id: int = SEALED_SKILL_BOOK_CONFIG_ID,
    crafter_name: str = "",
    crafter_player_id: int = 0,
):
    item = protocol.message(
        "ItemComp",
        item_id=item_uuid,
        config_id=config_id,
        size=1,
        bound=bound,
        sealed_skill_book={
            "skill_table_id": skill_table_id,
            "scroll_id": 1,
            "rarity_tier": rarity_tier,
            "crafter_name": crafter_name,
            "crafter_player_id": crafter_player_id,
        },
    )
    snap = protocol.message("AssetSnapshot")
    inst = snap.items.add()
    inst.item_uuid = item_uuid
    inst.config_id = config_id
    inst.stack_size = 1
    inst.instance_proto = item.SerializeToString(deterministic=True)
    return snap


def test_jubaozhai_wanted_skill_book_full_lifecycle_and_manual_dualcontrol():
    async def exercise():
        protocol = Protocol()
        now_ms = [1_800_000_000_000]
        clock = lambda: now_ms[0]
        gate_key = Ed25519PrivateKey.generate()
        coord_sec = b"c" * 32
        manual_sec = b"m" * 32

        class _RoleDir:
            async def get_role_fence_state(self, player_id: int):
                return protocol.message(
                    "rolefence.RoleFenceState",
                    player_id=player_id,
                    account=f"acc_{player_id}",
                    owner_revision=1,
                    version=1,
                    home_zone=1,
                )

        store = MemoryTradeStore(protocol, role_directory=_RoleDir(), clock_ms=clock)
        seq_id = [5000]

        async def next_id() -> int:
            seq_id[0] += 1
            return seq_id[0]

        engine = CommerceEngine(
            protocol,
            store,
            commerce_ids=next_id,
            op_ids=next_id,
            policy=TradePolicy(items=(3414,), currencies=(CURRENCY_GOLD,), allow_instances=True),
            wanted_skills=_FakeWantedCatalog(),
            clock_ms=clock,
        )
        svc = TradeService(
            protocol,
            store,
            caller_verifier=CallerVerifier([gate_key.public_key()]),
            coordinator_secret=coord_sec,
            commerce=engine,
            manual_resolution_secret=manual_sec,
            manual_resolution_environment="prod-cn-1",
            manual_resolution_allow_unapproved=False,
            clock_ms=clock,
        )

        # 验证 582/583/584 已挂载在 ClientPlayerJubaozhai
        wired = svc.services()["trade.ClientPlayerJubaozhai"]
        for rpc_name in (
            "CreateWantedSkillBookOrder",
            "FulfillWantedSkillBookOrder",
            "ListWantedSkillBookOrders",
        ):
            assert rpc_name in wired

        # 确认税费向上取整（5% 集市税销毁）
        assert wanted_tax_gold(1000) == 50 and wanted_seller_net_gold(1000) == 950
        assert wanted_tax_gold(101) == 6 and wanted_seller_net_gold(101) == 95

        # 全局 2026-11-26 合规红线：绑定银两 CURRENCY_BIND_GOLD=3 绝不可用于交易
        sel_bind = protocol.message("trade.TradeAssetSelection", currency_type=CURRENCY_BIND_GOLD, currency_amount=500)
        with pytest.raises(Exception) as bind_err:
            engine.policy.validate_selection(sel_bind)
        assert getattr(bind_err.value, "tip_id", 0) == ERR_TRADE_ASSET_NOT_TRADABLE

        server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")

        def client_md(player_id: int, method: str):
            path = f"/trade.ClientPlayerJubaozhai/{method}"
            sess = protocol.message(
                "SessionDetails",
                session_id=player_id,
                gate_instance_id="gate-1",
                session_incarnation=1,
                player_id=player_id,
                owner_revision=1,
                authenticated_account=f"acc_{player_id}",
            ).SerializeToString()
            return session_metadata(sess, path, gate_key)

        async def finalize_leg_op(cid: int, role: str, status: int, snapshot=None):
            rec = await store.get_commerce(cid)
            w = decode_workflow(protocol, rec)
            leg = find_leg(w, role)
            assert leg is not None and int(leg.op_id) > 0, f"leg {role} not issued yet"
            op = await store.claim_op(int(leg.op_id), clock(), clock() + 30_000, clock() + 60_000, 1)
            assert op is not None
            await store.finalize_op(
                op, status, status, 0, snapshot=snapshot, sql_committed=True, durable=True, now_ms=clock()
            )
            return leg

        try:
            # --- A. 582 发榜 -> 托管流通银两 -> 584 公开榜与筛选 -> 583 揭榜(含拒收绑定书、自揭榜、完整先书后款) ---
            c_path = "/trade.ClientPlayerJubaozhai/CreateWantedSkillBookOrder"
            f_path = "/trade.ClientPlayerJubaozhai/FulfillWantedSkillBookOrder"
            l_path = "/trade.ClientPlayerJubaozhai/ListWantedSkillBookOrders"

            # 非法技能(稀世 9999)与越界金额被拒
            bad_skill = protocol.message(
                "trade.CreateWantedSkillBookOrderRequest",
                client_request_id="req-bad-skill-001",
                skill_table_id=9999,
                reward_gold=1000,
            )
            r_bad = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(c_path, bad_skill.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
            )
            assert r_bad.error_message.id == ERR_INVALID_PARAMETER

            # 发榜人 101 创建 2000 流通银两求购 1001（要求宗师署名）
            req1 = protocol.message(
                "trade.CreateWantedSkillBookOrderRequest",
                client_request_id="req-wanted-create-01",
                skill_table_id=1001,
                require_master_signature=True,
                reward_gold=2000,
            )
            resp1 = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(c_path, req1.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
            )
            assert resp1.error_message.id == 0
            assert resp1.order.phase == WANTED_PHASE_ESCROWING
            assert resp1.order.reward_gold == 2000 and resp1.order.seller_net_gold == 1900
            assert resp1.order.skill_name == "青莲剑诀" and resp1.order.scroll_name == "剑道初卷"
            assert resp1.order.is_mine is True
            cid1 = int(resp1.order.order_id)

            # 同请求号幂等重放回同一张单；同请求号改内容回 REQUEST_CONFLICT
            resp1_replay = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(c_path, req1.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
            )
            assert resp1_replay.error_message.id == 0 and resp1_replay.order.order_id == resp1.order.order_id
            req1_tamper = protocol.message(
                "trade.CreateWantedSkillBookOrderRequest",
                client_request_id="req-wanted-create-01",
                skill_table_id=1001,
                reward_gold=3000,
            )
            resp1_conflict = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(c_path, req1_tamper.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
            )
            assert resp1_conflict.error_message.id == ERR_TRADE_REQUEST_CONFLICT

            # 推进托管腿 buyer_debit（必为流通银两 CURRENCY_GOLD=0）-> OPEN
            await engine.advance_contract(await store.get_commerce(cid1))
            debit_leg = await finalize_leg_op(cid1, "buyer_debit", OP_STATUS_APPLIED)
            assert len(debit_leg.bundle.currencies) == 1
            assert int(debit_leg.bundle.currencies[0].currency_type) == CURRENCY_GOLD
            assert int(debit_leg.bundle.currencies[0].amount) == 2000
            await engine.advance_contract(await store.get_commerce(cid1))

            # 584 公开悬赏榜可见，且按技能过滤生效
            list_req = protocol.message("trade.ListWantedSkillBookOrdersRequest", filter_skill_table_id=1001, limit=10)
            list_resp = protocol.parse(
                "trade.ListWantedSkillBookOrdersResponse",
                await client.call(l_path, list_req.SerializeToString(), metadata=client_md(202, "ListWantedSkillBookOrders")),
            )
            assert list_resp.error_message.id == 0 and len(list_resp.orders) == 1
            assert list_resp.orders[0].order_id == str(cid1) and list_resp.orders[0].phase == WANTED_PHASE_OPEN
            assert list_resp.orders[0].is_mine is False

            # 自揭榜被拒
            self_fulfill = protocol.message(
                "trade.FulfillWantedSkillBookOrderRequest",
                order_id=str(cid1),
                client_request_id="req-self-fulfill-01",
                sealed_item_id=777001,
            )
            r_self = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(f_path, self_fulfill.SerializeToString(), metadata=client_md(101, "FulfillWantedSkillBookOrder")),
            )
            assert r_self.error_message.id == ERR_TRADE_CANNOT_TRADE_SELF

            # 卖家 202 交上一枚「已绑定」封卷道籍 -> 服务端纵深核验拒收并原样退回 -> 重放回 ERR_TRADE_WANTED_BOOK_MISMATCH
            fulfill_bound = protocol.message(
                "trade.FulfillWantedSkillBookOrderRequest",
                order_id=str(cid1),
                client_request_id="req-fulfill-bound-01",
                sealed_item_id=777002,
            )
            r_fb = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(f_path, fulfill_bound.SerializeToString(), metadata=client_md(202, "FulfillWantedSkillBookOrder")),
            )
            assert r_fb.error_message.id == 0 and r_fb.order.phase == WANTED_PHASE_FULFILLING
            await engine.advance_contract(await store.get_commerce(cid1))
            rec_mid = await store.get_commerce(cid1)
            w_mid = decode_workflow(protocol, rec_mid)
            debit_role_1 = next(l.role for l in w_mid.legs if str(l.role).startswith("seller_debit_1_"))
            await finalize_leg_op(
                cid1,
                debit_role_1,
                OP_STATUS_APPLIED,
                snapshot=_sealed_book_snapshot(
                    protocol,
                    item_uuid=777002,
                    skill_table_id=1001,
                    bound=True,  # 已绑定资产严禁成交！
                    crafter_name="宗师甲",
                    crafter_player_id=303,
                ),
            )
            await engine.advance_contract(await store.get_commerce(cid1))
            # 生成原样退书腿 seller_return_1
            await finalize_leg_op(cid1, "seller_return_1", OP_STATUS_APPLIED)
            await engine.advance_contract(await store.get_commerce(cid1))

            # 同请求号重放查这次揭榜结果 -> TradeWantedBookMismatch (20019)
            r_fb_replay = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(f_path, fulfill_bound.SerializeToString(), metadata=client_md(202, "FulfillWantedSkillBookOrder")),
            )
            assert r_fb_replay.error_message.id == ERR_TRADE_WANTED_BOOK_MISMATCH

            # 卖家 202 交上合格的未绑定宗师署名封卷道籍 -> 先发书(buyer_receive)再压闸发税后银两(seller_receive=1900) -> SETTLED
            fulfill_ok = protocol.message(
                "trade.FulfillWantedSkillBookOrderRequest",
                order_id=str(cid1),
                client_request_id="req-fulfill-valid-02",
                sealed_item_id=777003,
            )
            r_fok = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(f_path, fulfill_ok.SerializeToString(), metadata=client_md(202, "FulfillWantedSkillBookOrder")),
            )
            assert r_fok.error_message.id == 0 and r_fok.order.phase == WANTED_PHASE_FULFILLING
            await engine.advance_contract(await store.get_commerce(cid1))
            w_mid2 = decode_workflow(protocol, await store.get_commerce(cid1))
            debit_role_2 = next(l.role for l in w_mid2.legs if str(l.role).startswith("seller_debit_2_"))
            await finalize_leg_op(
                cid1,
                debit_role_2,
                OP_STATUS_APPLIED,
                snapshot=_sealed_book_snapshot(
                    protocol,
                    item_uuid=777003,
                    skill_table_id=1001,
                    bound=False,
                    crafter_name="宗师甲",
                    crafter_player_id=303,
                ),
            )
            await engine.advance_contract(await store.get_commerce(cid1))
            w_deliv = decode_workflow(protocol, await store.get_commerce(cid1))
            # 先书后款闸门：buyer_receive 已发出，seller_receive 尚未发出（op_id == 0）
            assert int(find_leg(w_deliv, "buyer_receive").op_id) > 0
            assert int(find_leg(w_deliv, "seller_receive").op_id) == 0
            assert int(find_leg(w_deliv, "seller_receive").bundle.currencies[0].amount) == 1900

            await finalize_leg_op(cid1, "buyer_receive", OP_STATUS_APPLIED)
            await engine.advance_contract(await store.get_commerce(cid1))
            await finalize_leg_op(cid1, "seller_receive", OP_STATUS_APPLIED)
            await engine.advance_contract(await store.get_commerce(cid1))
            r_settled = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(f_path, fulfill_ok.SerializeToString(), metadata=client_md(202, "FulfillWantedSkillBookOrder")),
            )
            assert r_settled.error_message.id == 0 and r_settled.order.phase == WANTED_PHASE_SETTLED

            # --- B. 余额不足自动撤单 + 撤单全额退回 + 每人最多 3 张活跃单限制 ---
            c_insuff = protocol.message(
                "trade.CreateWantedSkillBookOrderRequest",
                client_request_id="req-wanted-insuff-01",
                skill_table_id=1002,
                reward_gold=500,
            )
            r_insuff = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(c_path, c_insuff.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
            )
            cid_insuff = int(r_insuff.order.order_id)
            await engine.advance_contract(await store.get_commerce(cid_insuff))
            await finalize_leg_op(cid_insuff, "buyer_debit", OP_STATUS_REJECTED)
            await engine.advance_contract(await store.get_commerce(cid_insuff))
            r_insuff_after = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(c_path, c_insuff.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
            )
            assert r_insuff_after.order.phase == WANTED_PHASE_CANCELLED

            # 连续创建 3 张活跃单，第 4 张被限流拒绝（ERR_TRADE_WANTED_ORDER_LIMIT_REACHED = 20018）
            active_cids: list[int] = []
            for idx in range(3):
                req_k = protocol.message(
                    "trade.CreateWantedSkillBookOrderRequest",
                    client_request_id=f"req-wanted-cap-{idx:02d}",
                    skill_table_id=1002,
                    reward_gold=1000 + idx * 100,
                )
                rk = protocol.parse(
                    "trade.WantedSkillBookOrderResponse",
                    await client.call(c_path, req_k.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
                )
                assert rk.error_message.id == 0
                active_cids.append(int(rk.order.order_id))
            req_4 = protocol.message(
                "trade.CreateWantedSkillBookOrderRequest",
                client_request_id="req-wanted-cap-03",
                skill_table_id=1002,
                reward_gold=1500,
            )
            r4 = protocol.parse(
                "trade.WantedSkillBookOrderResponse",
                await client.call(c_path, req_4.SerializeToString(), metadata=client_md(101, "CreateWantedSkillBookOrder")),
            )
            assert r4.error_message.id == ERR_TRADE_WANTED_ORDER_LIMIT_REACHED

            # --- C. buyer_receive 被拒时整单退回：必须退全额 reward_gold（1000），而非税后 950！---
            cid_unwind = active_cids[0]
            await engine.advance_contract(await store.get_commerce(cid_unwind))
            await finalize_leg_op(cid_unwind, "buyer_debit", OP_STATUS_APPLIED)
            await engine.advance_contract(await store.get_commerce(cid_unwind))
            f_unwind = protocol.message(
                "trade.FulfillWantedSkillBookOrderRequest",
                order_id=str(cid_unwind),
                client_request_id="req-fulfill-unwind-01",
                sealed_item_id=777009,
            )
            await client.call(f_path, f_unwind.SerializeToString(), metadata=client_md(202, "FulfillWantedSkillBookOrder"))
            await engine.advance_contract(await store.get_commerce(cid_unwind))
            w_u = decode_workflow(protocol, await store.get_commerce(cid_unwind))
            u_debit = next(l.role for l in w_u.legs if str(l.role).startswith("seller_debit_1_"))
            await finalize_leg_op(
                cid_unwind,
                u_debit,
                OP_STATUS_APPLIED,
                snapshot=_sealed_book_snapshot(protocol, item_uuid=777009, skill_table_id=1002),
            )
            await engine.advance_contract(await store.get_commerce(cid_unwind))
            # 发榜人收书失败 -> 整单退回 -> buyer_return 金额必须是全额 1000（非 950）
            await finalize_leg_op(cid_unwind, "buyer_receive", OP_STATUS_REJECTED)
            await engine.advance_contract(await store.get_commerce(cid_unwind))
            w_u_ret = decode_workflow(protocol, await store.get_commerce(cid_unwind))
            buyer_ret_leg = find_leg(w_u_ret, "buyer_return")
            assert buyer_ret_leg is not None
            assert int(buyer_ret_leg.bundle.currencies[0].amount) == 1000

            # --- D. TradeAdmin ResolveManualContract 双人复核 + approval_id 幂等 ---
            # 构造一条转人工合同：让 seller_return 失败进入 manual
            await finalize_leg_op(cid_unwind, "buyer_return", OP_STATUS_APPLIED)
            await finalize_leg_op(cid_unwind, "seller_return", OP_STATUS_REJECTED)
            await engine.advance_contract(await store.get_commerce(cid_unwind))
            manual_rec = await store.get_commerce(cid_unwind)
            assert int(manual_rec.status) == 10  # COMMERCE_MANUAL

            m_path = "/trade.TradeAdmin/ResolveManualContract"
            # 1) EXECUTE 不带批准记录 -> 带内 REFUSED (dual_control_approval_required)
            m_no_app = protocol.message(
                "trade.ResolveManualContractRequest",
                commerce_id=cid_unwind,
                mode=3,  # EXECUTE
                expect_target="prod-cn-1",
                expect_version=int(manual_rec.version),
                action="reissue",
                operator="alice.w",
                reason="ticket-1001",
            )
            sign_service_call(m_path, m_no_app, MANUAL_RESOLUTION_CALLER, clock(), manual_sec)
            r_no_app = protocol.parse("trade.ResolveManualContractResponse", await client.call(m_path, m_no_app.SerializeToString()))
            assert r_no_app.verdict == 3 and r_no_app.refusal == "dual_control_approval_required"

            # 2) EXECUTE 自批 -> 带内 REFUSED (dual_control_approval_invalid)
            m_self = protocol.message(
                "trade.ResolveManualContractRequest",
                commerce_id=cid_unwind,
                mode=3,
                expect_target="prod-cn-1",
                expect_version=int(manual_rec.version),
                action="reissue",
                operator="alice.w",
                reason="ticket-1001",
                approval=_mk_approval(protocol, approval_id="50001", proposer="alice.w", approver="alice.w"),
            )
            sign_service_call(m_path, m_self, MANUAL_RESOLUTION_CALLER, clock(), manual_sec)
            r_mself = protocol.parse("trade.ResolveManualContractResponse", await client.call(m_path, m_self.SerializeToString()))
            assert r_mself.verdict == 3 and r_mself.refusal.startswith("dual_control_approval_invalid:")

            # 3) EXECUTE 合格双人批准 -> OK，写留痕，且同 approval_id 重放回 already_executed=True
            m_ok = protocol.message(
                "trade.ResolveManualContractRequest",
                commerce_id=cid_unwind,
                mode=3,
                expect_target="prod-cn-1",
                expect_version=int(manual_rec.version),
                action="reissue",
                operator="alice.w",
                reason="ticket-1001",
                approval=_mk_approval(protocol, approval_id="50002", proposer="alice.w", approver="bob.k"),
            )
            sign_service_call(m_path, m_ok, MANUAL_RESOLUTION_CALLER, clock(), manual_sec)
            r_mok = protocol.parse("trade.ResolveManualContractResponse", await client.call(m_path, m_ok.SerializeToString()))
            assert r_mok.verdict == 1 and r_mok.already_executed is False
            assert r_mok.plan.approval_id == "50002" and r_mok.plan.approver == "bob.k"
            assert "executed=True" in r_mok.audit_line

            # 重放同一 approval_id（即使版本已加 1）-> already_executed=True
            sign_service_call(m_path, m_ok, MANUAL_RESOLUTION_CALLER, clock(), manual_sec)
            r_mrep = protocol.parse("trade.ResolveManualContractResponse", await client.call(m_path, m_ok.SerializeToString()))
            assert r_mrep.verdict == 1 and r_mrep.already_executed is True
            assert "executed=False" in r_mrep.audit_line
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())


# ---------------------------------------------------------------------------
# 3. Login 574 HandlePassportEvent, LoginAdmin 双人复核, Token Family & Queue 加固
# ---------------------------------------------------------------------------


class _RedisWithPassport(FakeRedis):
    """在 FakeRedis 上补充 RETIRE_ISSUED_BEFORE、5 元组 LOOKUP_SCRIPT 与账号级 RESERVE_FAST_PATH_SCRIPT。"""

    def __init__(self, *, clock_ms=None):
        super().__init__(clock_ms=clock_ms)
        self.scripts[login_tokens.RETIRE_ISSUED_BEFORE] = self._retire_issued_before
        self.scripts[login_queue.RESERVE_FAST_PATH_SCRIPT] = self._queue_reserve_fast_path_v2

    def _retire_issued_before(self, keys_, argv):
        if self._type(keys_[0]) != "zset":
            return 0
        zset = self.data[keys_[0]]
        cutoff = int(argv[3])
        to_drop = []
        for token in list(zset.keys()):
            raw = self._alive(argv[1] + token) and self.data[argv[1] + token]
            if not raw:
                continue
            data = json.loads(raw)
            if data["account"].encode() != argv[0]:
                raise RuntimeError("invalid refresh ownership")
            if int(data.get("identity_issued_at", 0) or 0) <= cutoff:
                to_drop.append((token, data["family_id"].encode()))
        for token, fid in to_drop:
            fkey = argv[2] + fid
            if self._hget(fkey, "head") == token:
                self.data.pop(fkey, None)
                self.expiry.pop(fkey, None)
            self.data.pop(argv[1] + token, None)
            self.expiry.pop(argv[1] + token, None)
            zset.pop(token, None)
        return len(to_drop)

    def _queue_reserve_fast_path_v2(self, keys_, argv):
        now_ms, budget = int(argv[0]), int(argv[1])
        zset = self._zset(keys_[0])
        for m in [k for k, v in zset.items() if v <= now_ms]:
            zset.pop(m, None)
        if argv[2] not in zset and len(zset) >= budget:
            return 0
        zset[argv[2]] = int(argv[3])
        if self._pttl(keys_[0]) < int(argv[4]):
            self._px(keys_[0], int(argv[4]))
        return 1


class _FakePassportAdmin:
    def __init__(self) -> None:
        self.by_sub: dict[str, PassportUserStatus] = {}
        self.by_name: dict[tuple[str, str], PassportUserStatus] = {}
        self.deleted: list[str] = []

    async def lookup_user(self, sub: str) -> PassportUserStatus:
        return self.by_sub.get(sub, PassportUserStatus(sub=sub, gone=True))

    async def lookup_user_by_name(self, owner: str, name: str) -> PassportUserStatus:
        return self.by_name.get((owner, name), PassportUserStatus(gone=True))

    async def delete_user(self, sub: str) -> None:
        self.deleted.append(sub)
        self.by_sub.pop(sub, None)


def test_login_passport_event_574_admin_dualcontrol_and_token_queue_hardening():
    async def exercise():
        protocol = Protocol()
        now_ms = [1_800_000_000_000]
        rdb = _RedisWithPassport(clock_ms=lambda: now_ms[0])
        tm = TokenManager(rdb)
        key = Ed25519PrivateKey.generate()
        admin_tok = "login-admin-secret-token-32bytes"

        # 简易集群替身（记录踢线与重置实名/注销调用）
        kicked: list[tuple[str, int]] = []
        reset_accounts: list[str] = []

        async def ds_call(path: str, data: bytes, timeout=None) -> bytes:
            method = path.rsplit("/", 1)[1]
            if method == "ResetAccountRealName":
                req = protocol.parse("data_service.ResetAccountRealNameRequest", data)
                reset_accounts.append(req.account)
                return protocol.message("data_service.ResetAccountRealNameResponse", had_real_name=True).SerializeToString()
            if method == "LookupAccountRole":
                req = protocol.parse("data_service.LookupAccountRoleRequest", data)
                return protocol.message(
                    "data_service.AccountRoleOwnership",
                    account=f"acc_{req.player_id}",
                    role={"player_id": req.player_id},
                    owner_revision=1,
                ).SerializeToString()
            if method == "RequestAccountDeletion":
                return protocol.message(
                    "data_service.AccountDeletionResponse",
                    outcome=1,
                    lifecycle={"deletion_state": 1, "deletion_execute_after_ms": now_ms[0] + 7 * 86400_000},
                ).SerializeToString()
            if method == "ReadAccountDirectory":
                return protocol.message("data_service.AccountDirectoryResponse").SerializeToString()
            raise AssertionError(f"unexpected data_service call: {method}")

        ds = DataService(protocol, ds_call, b"d" * 32)
        locator = PlayerLocator(protocol, lambda *_: asyncio.sleep(0))

        passport_admin = _FakePassportAdmin()
        enforcer = PassportSessionEnforcer(
            passport_admin,
            tm,
            kick_account_players=lambda acc, reason: asyncio.sleep(0, result=kicked.append((acc, reason))),
        )
        cfg = LoginConfig(
            zone_id=1,
            gate_token_secret=b"g" * 32,
            directory_secret=b"d" * 32,
            dev_password_secret="dev-secret",
            admin_token=admin_tok,
            admin_allow_unapproved=False,
        )
        ctx = ServiceContext(
            protocol=protocol,
            config=cfg,
            login_redis=rdb,
            player_redis=rdb,
            tokens=tm,
            locator=locator,
            data=ds,
            scene_manager_call=None,
            publish=lambda *_: asyncio.sleep(0),
            directory=None,
            passport_sessions=enforcer,
        )
        service = LoginService(ctx, CallerVerifier([key.public_key()]))
        server, port = await serve_grpc(protocol, "127.0.0.1:0", service.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            # 1) 574 HandlePassportEvent：改密仅吊销旧家族、保留新家族；封禁/删除吊销全部并踢线；正常用户不吊销
            p_path = "/loginpb.LoginPreGate/HandlePassportEvent"
            sub = "user-u1"
            acc = passport_account(sub)
            old_pair = await tm.issue(acc, PASSPORT_AUTH_TYPE, "dev-old", identity_issued_at=1000)
            new_pair = await tm.issue(acc, PASSPORT_AUTH_TYPE, "dev-new", identity_issued_at=2000)

            # 改密发生在 1500：old_pair (1000) 被吊销，new_pair (2000) 保留
            passport_admin.by_sub[sub] = PassportUserStatus(sub=sub, password_changed_at=1500)
            ev_pw = json.dumps({"organization": "xuanming", "user": "alice", "extendedUser": {"owner": "xuanming", "name": "alice", "id": sub}}).encode()
            raw_ev = await client.call(p_path, protocol.message("loginpb.PassportEventRequest", payload=ev_pw).SerializeToString())
            assert protocol.parse("loginpb.PassportEventResponse", raw_ev).checked_users == 1
            with pytest.raises(AccessRejected):
                await tm.validate_access(old_pair.access_token)
            assert (await tm.validate_access(new_pair.access_token)).account == acc
            assert (acc, login_tips.PASSPORT_CREDENTIAL_CHANGED) in kicked

            # 轮换 new_pair 必须保留 identity_issued_at=2000（不能洗成 0 或 now）
            rotated = await tm.refresh(new_pair.refresh_token)
            assert (await tm.read_refresh_payload(rotated.refresh_token)).identity_issued_at == 2000

            # 封禁 -> 吊销全部（含 rotated），且刷新也被拒绝
            kicked.clear()
            passport_admin.by_sub[sub] = PassportUserStatus(sub=sub, forbidden=True)
            raw_ban = await client.call(p_path, protocol.message("loginpb.PassportEventRequest", payload=ev_pw).SerializeToString())
            assert protocol.parse("loginpb.PassportEventResponse", raw_ban).checked_users == 1
            with pytest.raises(AccessRejected):
                await tm.validate_access(rotated.access_token)
            assert (acc, login_tips.PASSPORT_CREDENTIAL_CHANGED) in kicked

            # 解封恢复正常状态 -> 新发令牌后收到事件不会误吊销
            kicked.clear()
            passport_admin.by_sub[sub] = PassportUserStatus(sub=sub, forbidden=False)
            healthy_pair = await tm.issue(acc, PASSPORT_AUTH_TYPE, "dev-ok", identity_issued_at=3000)
            raw_unban = await client.call(p_path, protocol.message("loginpb.PassportEventRequest", payload=ev_pw).SerializeToString())
            assert protocol.parse("loginpb.PassportEventResponse", raw_unban).checked_users == 1
            assert (await tm.validate_access(healthy_pair.access_token)).account == acc
            assert kicked == []

            # 删除用户（gone=True）-> 全部吊销并踢线
            passport_admin.by_sub.pop(sub)
            raw_del = await client.call(p_path, protocol.message("loginpb.PassportEventRequest", payload=ev_pw).SerializeToString())
            assert protocol.parse("loginpb.PassportEventResponse", raw_del).checked_users == 1
            with pytest.raises(AccessRejected):
                await tm.validate_access(healthy_pair.access_token)

            # built-in 组织与应用伪用户（含 "/"）被忽略（checked_users=0）；坏 JSON 回 INVALID_ARGUMENT
            ev_builtin = json.dumps({"organization": "built-in", "user": "admin"}).encode()
            raw_bi = await client.call(p_path, protocol.message("loginpb.PassportEventRequest", payload=ev_builtin).SerializeToString())
            assert protocol.parse("loginpb.PassportEventResponse", raw_bi).checked_users == 0
            with pytest.raises(grpc.aio.AioRpcError) as bad_ev:
                await client.call(p_path, protocol.message("loginpb.PassportEventRequest", payload=b"{bad").SerializeToString())
            assert bad_ev.value.code() == grpc.StatusCode.INVALID_ARGUMENT

            # 2) LoginAdmin 严格双人复核（admin_allow_unapproved=False）
            admin_md = (("x-admin-token", admin_tok),)
            reset_path = "/loginpb.LoginAdmin/ResetAccountRealName"
            # 无批准 -> PERMISSION_DENIED
            req_r0 = protocol.message("loginpb.ResetAccountRealNameRequest", account="acc_1", operator="alice.w", reason="typo")
            with pytest.raises(grpc.aio.AioRpcError) as e_r0:
                await client.call(reset_path, req_r0.SerializeToString(), metadata=admin_md)
            assert e_r0.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 自批 -> PERMISSION_DENIED
            req_r_self = protocol.message(
                "loginpb.ResetAccountRealNameRequest",
                account="acc_1",
                operator="alice.w",
                reason="typo",
                approval=_mk_approval(protocol, approval_id="3001", proposer="alice.w", approver="alice.w"),
            )
            with pytest.raises(grpc.aio.AioRpcError) as e_r1:
                await client.call(reset_path, req_r_self.SerializeToString(), metadata=admin_md)
            assert e_r1.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 合格双人复核 -> 成功
            req_r_ok = protocol.message(
                "loginpb.ResetAccountRealNameRequest",
                account="acc_1",
                operator="alice.w",
                reason="typo",
                approval=_mk_approval(protocol, approval_id="3002", proposer="alice.w", approver="bob.k"),
            )
            r_ok = protocol.parse("loginpb.ResetAccountRealNameResponse", await client.call(reset_path, req_r_ok.SerializeToString(), metadata=admin_md))
            assert r_ok.error_message.id == 0 and r_ok.had_real_name is True and reset_accounts == ["acc_1"]

            # RemovePlayersFromAccounts: dry_run=True 免批，但带了非法批准记录依然 fail-closed 拒止
            rm_path = "/loginpb.LoginAdmin/RemovePlayersFromAccounts"
            rm_dry = protocol.message("loginpb.RemovePlayersFromAccountsRequest", player_ids=[], operator="alice.w", reason="check", dry_run=True)
            assert protocol.parse("loginpb.RemovePlayersFromAccountsResponse", await client.call(rm_path, rm_dry.SerializeToString(), metadata=admin_md)).removed_count == 0
            rm_real_no_app = protocol.message("loginpb.RemovePlayersFromAccountsRequest", player_ids=[], operator="alice.w", reason="cleanup", dry_run=False)
            with pytest.raises(grpc.aio.AioRpcError) as e_rm:
                await client.call(rm_path, rm_real_no_app.SerializeToString(), metadata=admin_md)
            assert e_rm.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 3) Queue 单账号仅占 1 枚快速通道名额 + gate 票据绑定账号摘要
            q = login_queue.LoginQueue(rdb, b"q" * 32, clock_ms=lambda: now_ms[0])
            assert await q.try_reserve_fast_path_slot(1, budget=1, account="player_a") is True
            # 同一账号再次申请不消耗新名额（仍返回 True 且总在途数仍为 1）
            assert await q.try_reserve_fast_path_slot(1, budget=1, account="player_a") is True
            assert await q.admitted_count(1) == 1
            # 另一账号被 budget=1 拦下
            assert await q.try_reserve_fast_path_slot(1, budget=1, account="player_b") is False

            gate = login_queue.GateCandidate(node_id=11, ip="10.0.0.11", port=7001, zone_id=1)
            ticket = login_queue.sign_gate_token(protocol, gate, b"g" * 32, 600, account="player_a")
            payload_msg = protocol.parse("GateTokenPayload", ticket.token_payload)
            assert login_queue.ticket_issued_to(payload_msg.account_digest, "player_a") is True
            assert login_queue.ticket_issued_to(payload_msg.account_digest, "player_b") is False
        finally:
            await client.close()
            await server.stop(None)

    asyncio.run(exercise())


# ---------------------------------------------------------------------------
# 4. Chat 持久化平台补审 + Ops Auth + ContentGuard 折叠态广告检测
# ---------------------------------------------------------------------------


def test_chat_platform_recheck_and_ops_auth():
    async def exercise():
        protocol = Protocol()
        now_ms = [1_800_000_000_000]
        clock = lambda: now_ms[0]
        gate_key = Ed25519PrivateKey.generate()
        ops_token = "chat-ops-admin-secret-token-32b"

        assert resolve_admin_token({}) == ""
        with pytest.raises(ValueError):
            resolve_admin_token({"MMORPG_CHAT_ADMIN_TOKEN": "too-short"})
        assert resolve_admin_token({"MMORPG_CHAT_ADMIN_TOKEN": ops_token}) == ops_token

        class _AllowVerifier:
            async def verify_actor(self, identity: ActorIdentity) -> None:
                return None

        class _Pusher:
            def __init__(self) -> None:
                self.broadcasts = []
                self.retracts = []

            async def push_world(self, message) -> None:
                self.broadcasts.append(message)

            async def push_world_retract(self, notify) -> None:
                self.retracts.append(notify)

        platform_verdicts = [REVIEW_UNAVAILABLE, REVIEW_REJECT]

        class _Reviewer:
            async def check_text(self, scene, player_id, text, *, account="", async_platform=False):
                return ReviewResult(verdict=REVIEW_PASS, pending_platform_review=True)

            async def check_platform_text(self, scene, player_id, text, *, account=""):
                verdict = platform_verdicts.pop(0) if platform_verdicts else REVIEW_UNAVAILABLE
                return ReviewResult(verdict=verdict)

        store = MemoryChatStore(
            protocol,
            _AllowVerifier(),
            ChatRules(world_channel_cooldown_ms=0),
            reviewer=_Reviewer(),
            clock_ms=clock,
        )
        pusher = _Pusher()

        # 1) 持久化补审：先模拟发出带待补审的世界消息，首次平台不可用 -> 租期内不重复认领 -> 租期过后重试判违规并自动撤回
        identity = ActorIdentity(player_id=501, account="acc_501", owner_revision=1)
        send_req = protocol.message(
            "chatpb.SendChatRequest",
            request_id="world-recheck-req-1",
            message={"channel": CHANNEL_WORLD, "content": "hidden bad link"},
        )
        res = await store.send(identity, send_req)
        assert res.tip == 0 and res.pending_platform_review is True
        accepted_ms = int(res.message.send_time_ms)

        svc = ChatService(
            protocol,
            store,
            CallerVerifier([gate_key.public_key()]),
            world_pusher=pusher,
            platform_reviewer=store.reviewer,
            admin_token=ops_token,
        )
        # 第 1 次认领：平台返回 UNAVAILABLE -> 无终态结局（0）
        out1 = await svc.run_platform_recheck_once()
        assert out1 == [(501, accepted_ms, 0)]
        # 租期未到：再次认领为空
        assert await svc.run_platform_recheck_once() == []
        # 推进时钟越过租期 -> 第 2 次认领：平台判 REJECT -> 自动撤回并广播撤回通知，结局 RECHECK_RETRACTED
        now_ms[0] += PLATFORM_RECHECK_LEASE_MS + 100
        out2 = await svc.run_platform_recheck_once()
        assert out2 == [(501, accepted_ms, RECHECK_RETRACTED)]
        assert len(pusher.retracts) == 1 and pusher.retracts[0].reason == "platform_async_reject"
        assert (501, accepted_ms) in store.retracted

        # 2) 连续 3 次平台不可用 -> 第 4 次认领标记 exhausted 并落终态 RECHECK_GAVE_UP
        now_ms[0] += 1000
        send_req2 = protocol.message(
            "chatpb.SendChatRequest",
            request_id="world-recheck-req-2",
            message={"channel": CHANNEL_WORLD, "content": "stays visible"},
        )
        res2 = await store.send(identity, send_req2)
        ms2 = int(res2.message.send_time_ms)
        for _ in range(3):
            assert await svc.run_platform_recheck_once() == [(501, ms2, 0)]
            now_ms[0] += PLATFORM_RECHECK_LEASE_MS + 100
        assert await svc.run_platform_recheck_once() == [(501, ms2, RECHECK_GAVE_UP)]
        assert await svc.run_platform_recheck_once() == []

        # 3) RetractWorldChat 运维口令鉴权（未配停用、错口令拒绝、正确口令放行）
        server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}")
        try:
            r_path = "/chatpb.ClientPlayerChat/RetractWorldChat"
            r_req = protocol.message(
                "chatpb.RetractWorldChatRequest", sender_player_id=501, send_time_ms=ms2, reason="gm_ops"
            )
            with pytest.raises(grpc.aio.AioRpcError) as err_missing:
                await client.call(r_path, r_req.SerializeToString())
            assert err_missing.value.code() == grpc.StatusCode.PERMISSION_DENIED

            with pytest.raises(grpc.aio.AioRpcError) as err_wrong:
                await client.call(
                    r_path,
                    r_req.SerializeToString(),
                    metadata=((ADMIN_TOKEN_METADATA_KEY, "wrong-token-1234567890"),),
                )
            assert err_wrong.value.code() == grpc.StatusCode.PERMISSION_DENIED

            raw_ok = await client.call(
                r_path,
                r_req.SerializeToString(),
                metadata=((ADMIN_TOKEN_METADATA_KEY, ops_token),),
            )
            resp_ok = protocol.parse("chatpb.RetractWorldChatResponse", raw_ok)
            assert resp_ok.error_message.id == 0 and resp_ok.retracted is True
        finally:
            await client.close()
            await server.stop(None)

        # 显式空口令 -> 整个运维撤回面停用（PERMISSION_DENIED）
        svc_disabled = ChatService(
            protocol, store, CallerVerifier([gate_key.public_key()]), admin_token=""
        )
        d_server, d_port = await serve_grpc(protocol, "127.0.0.1:0", svc_disabled.services(), pass_context=True)
        d_client = GrpcClient(f"127.0.0.1:{d_port}")
        try:
            with pytest.raises(grpc.aio.AioRpcError) as err_dis:
                await d_client.call(
                    "/chatpb.ClientPlayerChat/RetractWorldChat",
                    protocol.message("chatpb.RetractWorldChatRequest", sender_player_id=501, send_time_ms=ms2, reason="x").SerializeToString(),
                )
            assert err_dis.value.code() == grpc.StatusCode.PERMISSION_DENIED
        finally:
            await d_client.close()
            await d_server.stop(None)

    asyncio.run(exercise())


def test_content_guard_contact_detector_folded_evasion_and_allowlist():
    # 1) 全角字母/句点 URL 与域名绕过检测
    for evasion in (
        "ｈｔｔｐｓ：／／ｅｖｉｌ．ｃｏｍ",
        "ｗｗｗ．ｅｖｉｌ．ｖｉｐ",
        "快来看 ａｂｃ．ｃｏｍ 送元宝",
        "加微 幺三八零零幺三八零零零",
    ):
        elems, _ = fold(evasion)
        assert contains_ad(evasion, elems) is True, f"expected evasion to be caught: {evasion}"

    # 2) 非广告自然文本不被误伤：www... 笑声、abc.community 单词
    for benign in ("哈哈www...", "see abc.community for docs", "今天打本出了极品装备"):
        elems, _ = fold(benign)
        assert contains_ad(benign, elems) is False, f"unexpected false positive: {benign}"

    # 3) 白名单仅登记主机名 xuanming.com，带 https:// 或 www. 均可豁免，相邻恶意域名不会被吃掉
    eng = ContentGuardEngine()
    eng.swap(1, Lexicon(allow=("xuanming.com",)))

    assert eng.check(SceneClass.CHAT, "官网 https://xuanming.com 欢迎你").verdict == Verdict.PASS
    assert eng.check(SceneClass.CHAT, "官网 www.xuanming.com 欢迎你").verdict == Verdict.PASS
    assert eng.check(SceneClass.CHAT, "官网 ｈｔｔｐｓ：／／ｘｕａｎｍｉｎｇ．ｃｏｍ").verdict == Verdict.PASS

    # 相邻第二个未豁免域名必须被拦截（不会因正则吞边界字符而漏检）
    adj = eng.check(SceneClass.CHAT, "xuanming.com,evil.com")
    assert adj.verdict == Verdict.REJECT and adj.category == Category.AD
