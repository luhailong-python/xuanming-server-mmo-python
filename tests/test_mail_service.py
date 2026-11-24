"""mail 服务端到端 gRPC、资产通道、活动冻结名单、护栏与鉴权测试。"""

from __future__ import annotations

import asyncio
import functools
import os
import re
import time
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from xuanming.cluster import LocalDirectory
from xuanming.data import ConfigTables, PlayerRowCodec, PlayerRowKind, PlayerSnapshot
from xuanming.data.asset_inventory import balance
from xuanming.data.asset_ledger import Outcome, record as record_ledger
from xuanming.grpc_runtime import (
    GrpcClient,
    serve_grpc,
    session_metadata,
    sign_service_call,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.mail import (
    ASSET_STREAM_SYSTEM_CREDIT,
    AssetDeliveryLoop,
    AssetOpCaller,
    CLAIM_CLAIMED,
    CLAIM_MANUAL,
    CLAIM_NONE,
    CLAIM_PENDING,
    CLAIM_UNCLAIMED,
    GrpcDataServiceLedgerReader,
    MAIL_KIND_GUILD,
    MAIL_KIND_PERSONAL,
    MAIL_KIND_SYSTEM,
    MAIL_NODE_TYPE,
    MailConfig,
    MailLimits,
    MailService,
    MemoryMailStore,
    SceneTarget,
    SqlMailStore,
    manifest_hash,
    shard_hash,
)
from xuanming.mail.asset_loop import build_asset_op_request
from xuanming.mail.config import (
    ASSET_OP_SECRET_ENV,
    COORDINATOR_SECRET_ENV,
    PRODUCER_SECRET_ENVS,
    ROLE_FENCE_SECRET_ENV,
)
from xuanming.mail.errors import (
    ERR_ASSET_PARTIAL_APPLIED,
    ERR_FEATURE_UNAVAILABLE,
    ERR_INVALID_PARAMETER,
    ERR_MAIL_ATTACHMENT_INVALID,
    ERR_MAIL_CLAIM_MANUAL,
    ERR_MAIL_CLAIM_PENDING,
    ERR_MAIL_EXPIRED,
    ERR_MAIL_NOT_FOUND,
    ERR_MAIL_RECIPIENT_BOX_FULL,
    ERR_MAIL_RECIPIENT_DELETED,
    ERR_MAIL_REQUEST_CONFLICT,
    RoleFenceUnavailableError,
)
from xuanming.mail.model import (
    OP_APPLIED,
    OP_MANUAL,
    OP_PENDING,
    OP_REJECTED,
    ROLE_BLOCKER_ASSET_OP,
)
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_mail_outbox import MailSender
from xuanming.protocol import Protocol

PROTOCOL = Protocol()


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper

SECRET_ADMIN = b"mail-producer-secret-admin-0123456789"
SECRET_GUILD = b"mail-producer-secret-guild-0123456789"
SECRET_RANK = b"mail-producer-secret-rank--0123456789"
SECRET_TRADE = b"mail-producer-secret-trade-0123456789"
SECRET_BATTLE = b"mail-producer-secret-battle-012345678"
SECRET_COORDINATOR = b"role-fence-coordinator-secret-01234567"
SECRET_ASSET_MAIL = b"asset-op-secret-for-mail-caller-012345"
SECRET_ROLE_FENCE_MAIL = b"role-fence-secret-for-mail-caller-0123"


class FakeClock:
    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, delta_ms: int) -> int:
        self.now_ms += delta_ms
        return self.now_ms


class FakeRoleDirectory:
    def __init__(self, protocol: Protocol) -> None:
        self._protocol = protocol
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
        state = self._protocol.message(
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
            raise RoleFenceUnavailableError("directory down")
        return self.states.get(player_id)


class FakeGuildDirectory:
    def __init__(self) -> None:
        self.memberships: dict[int, int] = {}
        self.fail = False

    async def guild_of(self, player_id: int) -> int:
        if self.fail:
            raise RuntimeError("guild service unavailable")
        return self.memberships.get(player_id, 0)


class RecordingNotifier:
    def __init__(self) -> None:
        self.events: list[tuple[int, int]] = []

    async def personal(self, player_id: int, mail_id: int) -> None:
        self.events.append((player_id, mail_id))


def _make_gate_identity():
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    verifier = CallerVerifier([public_key], allowed_callers=("gate",))
    return private_key, public_key, verifier


def _client_metadata(
    private_key: Ed25519PrivateKey,
    path: str,
    player_id: int,
    account: str = "acc_1",
    owner_revision: int = 1,
    *,
    session_id: int = 11,
    gate_instance_id: str = "gate-inst-1",
    session_incarnation: int = 2,
) -> tuple:
    details = PROTOCOL.message(
        "SessionDetails",
        session_id=session_id,
        player_id=player_id,
        gate_node_id=1,
        gate_instance_id=gate_instance_id,
        session_incarnation=session_incarnation,
        owner_revision=owner_revision,
        authenticated_account=account,
    )
    return session_metadata(details.SerializeToString(), path, private_key)


async def _start_mail_server(
    stack: AsyncExitStack,
    *,
    clock: FakeClock | None = None,
    limits: MailLimits = MailLimits(),
    item_exists=lambda cid: cid in (1001, 1002, 2001),
    guilds: FakeGuildDirectory | None = None,
    role_directory: FakeRoleDirectory | None = None,
    notifier: RecordingNotifier | None = None,
    caller_verifier: CallerVerifier | None = None,
):
    clock = clock or FakeClock()
    role_dir = role_directory if role_directory is not None else FakeRoleDirectory(PROTOCOL)
    guild_dir = guilds if guilds is not None else FakeGuildDirectory()
    priv_key, pub_key, default_verifier = _make_gate_identity()
    verifier = caller_verifier if caller_verifier is not None else default_verifier

    store = MemoryMailStore(
        PROTOCOL,
        item_exists=item_exists,
        guilds=guild_dir,
        role_directory=role_dir,
        limits=limits,
        clock_ms=clock,
    )
    service = MailService(
        PROTOCOL,
        store,
        caller_verifier=verifier,
        producer_secrets={
            "admin": SECRET_ADMIN,
            "guild": SECRET_GUILD,
            "rank": SECRET_RANK,
            "trade": SECRET_TRADE,
            "battle": SECRET_BATTLE,
        },
        coordinator_secret=SECRET_COORDINATOR,
        notifier=notifier,
        clock_ms=clock,
    )
    server, port = await serve_grpc(PROTOCOL, "127.0.0.1:0", service.services(), pass_context=True)
    stack.push_async_callback(server.stop, 1.0)
    client = GrpcClient(f"127.0.0.1:{port}", timeout=5, enable_retries=False)
    stack.push_async_callback(client.close)
    return SimpleNamespace(
        store=store,
        service=service,
        client=client,
        port=port,
        clock=clock,
        role_dir=role_dir,
        guild_dir=guild_dir,
        gate_key=priv_key,
        gate_pub=pub_key,
        notifier=notifier,
    )


async def _call_admin(env, method: str, request, caller: str, secret: bytes, *, metadata: tuple = ()):
    path = f"/mailpb.MailAdmin/{method}"
    sign_service_call(path, request, caller, env.clock(), secret)
    raw = await env.client.call(path, request.SerializeToString(), metadata=metadata)
    resp_type = (
        "mailpb.ImportMailRecipientsResponse"
        if method == "ImportMailRecipients"
        else "mailpb.SendMailResponse"
    )
    return PROTOCOL.parse(resp_type, raw)


async def _call_client(env, method: str, request, player_id: int, account: str = "acc_1", rev: int = 1):
    path = f"/mailpb.ClientPlayerMail/{method}"
    md = _client_metadata(env.gate_key, path, player_id, account=account, owner_revision=rev)
    raw = await env.client.call(path, request.SerializeToString(), metadata=md)
    resp_map = {
        "ListMails": "mailpb.ListMailsResponse",
        "ReadMail": "mailpb.ReadMailResponse",
        "ClaimMailAttachments": "mailpb.ClaimMailAttachmentsResponse",
        "DeleteMails": "mailpb.DeleteMailsResponse",
        "ClearReadMails": "mailpb.ClearReadMailsResponse",
        "NotifyMailChanged": "mailpb.MailChangedEvent",
    }
    return PROTOCOL.parse(resp_map[method], raw)


async def _call_fence(env, method: str, request, *, secret: bytes = SECRET_COORDINATOR, caller: str = "coordinator"):
    path = f"/rolefence.RoleFenceParticipant/{method}"
    sign_service_call(path, request, caller, env.clock(), secret)
    raw = await env.client.call(path, request.SerializeToString())
    return PROTOCOL.parse(f"rolefence.{method}Response", raw)


# ----------------------------------------------------------------------------------------------------
# 1. 发件、幂等重放/冲突、附件校验、容量上限与 Battle Outbox 跨模块兼容性
# ----------------------------------------------------------------------------------------------------


@_async_test
async def test_send_personal_system_guild_replay_conflict_and_limits():
    async with AsyncExitStack() as stack:
        notifier = RecordingNotifier()
        env = await _start_mail_server(
            stack,
            limits=MailLimits(
                max_personal_mails_per_player=2,
                max_active_system_mails=1,
                max_active_guild_mails_per_guild=1,
            ),
            notifier=notifier,
        )
        env.role_dir.register(101, "acc_101", 1)

        # 1) 个人邮件发送 + 幂等重放（仅触发一次提醒）
        req1 = PROTOCOL.message(
            "mailpb.SendMailRequest",
            sender_kind=3,
            request_id=1001,
            recipient_player_id=101,
            title="排行榜奖励",
            body="恭喜获得第一名",
        )
        req1.attachments.currencies.add(currency_type=0, amount=500)
        req1.attachments.items.add(config_id=1001, count=3)

        r1 = await _call_admin(env, "SendPersonalMail", req1, "rank", SECRET_RANK)
        assert r1.error_message.id == 0 and r1.mail_id > 0 and not r1.duplicated
        assert notifier.events == [(101, r1.mail_id)]

        # 2) 再发一封把个人邮箱填满（上限 2）
        req2 = PROTOCOL.message(
            "mailpb.SendMailRequest",
            sender_kind=4,
            request_id=1002,
            recipient_player_id=101,
            title="交易结算",
            body="售出成功",
        )
        r2 = await _call_admin(env, "SendPersonalMail", req2, "trade", SECRET_TRADE)
        assert r2.error_message.id == 0 and not r2.duplicated

        # 3) 第 3 封因满箱被拒
        req3 = PROTOCOL.message(
            "mailpb.SendMailRequest",
            sender_kind=4,
            request_id=1003,
            recipient_player_id=101,
            title="第三封",
            body="满箱测试",
        )
        r3 = await _call_admin(env, "SendPersonalMail", req3, "trade", SECRET_TRADE)
        assert r3.error_message.id == ERR_MAIL_RECIPIENT_BOX_FULL

        # 4) 即使此刻邮箱已满，重放 req1 仍因 replay-before-validation 返回原 mail_id 且 duplicated=True
        r1_replay = await _call_admin(env, "SendPersonalMail", req1, "rank", SECRET_RANK)
        assert r1_replay.error_message.id == 0
        assert r1_replay.mail_id == r1.mail_id
        assert r1_replay.duplicated is True
        assert len(notifier.events) == 2  # 未重复推送

        # 5) 同 (sender_kind, request_id) 但改内容 -> 冲突 17004
        req1_tamper = PROTOCOL.message(
            "mailpb.SendMailRequest",
            sender_kind=3,
            request_id=1001,
            recipient_player_id=101,
            title="改标题",
            body="恭喜获得第一名",
        )
        r1_conflict = await _call_admin(env, "SendPersonalMail", req1_tamper, "rank", SECRET_RANK)
        assert r1_conflict.error_message.id == ERR_MAIL_REQUEST_CONFLICT

        # 6) 系统纯文本邮件：仅 admin 可发，容量上限 1
        sys_denied = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=3, request_id=2001, title="系统", body="无权"
        )
        assert (await _call_admin(env, "SendSystemMail", sys_denied, "rank", SECRET_RANK)).error_message.id == ERR_INVALID_PARAMETER

        sys_ok = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=1, request_id=2002, title="停服公告", body="维护通知"
        )
        rsys = await _call_admin(env, "SendSystemMail", sys_ok, "admin", SECRET_ADMIN)
        assert rsys.error_message.id == 0

        sys_full = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=1, request_id=2003, title="公告2", body="超限"
        )
        assert (await _call_admin(env, "SendSystemMail", sys_full, "admin", SECRET_ADMIN)).error_message.id == ERR_MAIL_RECIPIENT_BOX_FULL

        # 7) 帮会纯文本邮件：仅 guild 可发，按帮会独立计数
        g1 = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=2, request_id=3001, guild_id=88, title="帮战通知", body="集合"
        )
        assert (await _call_admin(env, "SendGuildMail", g1, "guild", SECRET_GUILD)).error_message.id == 0
        g1_full = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=2, request_id=3002, guild_id=88, title="帮战2", body="超限"
        )
        assert (await _call_admin(env, "SendGuildMail", g1_full, "guild", SECRET_GUILD)).error_message.id == ERR_MAIL_RECIPIENT_BOX_FULL
        # 另一个帮会不受 88 号帮会满额影响
        g2 = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=2, request_id=3003, guild_id=99, title="99帮公告", body="独立计数"
        )
        assert (await _call_admin(env, "SendGuildMail", g2, "guild", SECRET_GUILD)).error_message.id == 0


@_async_test
async def test_attachments_validation_boundaries():
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack, item_exists=lambda cid: cid in range(1, 30))

        async def send_att(req_id: int, currencies=(), items=()):
            req = PROTOCOL.message(
                "mailpb.SendMailRequest",
                sender_kind=1,
                request_id=req_id,
                recipient_player_id=10,
                title="附件校验",
                body="body",
            )
            for ctype, amt in currencies:
                req.attachments.currencies.add(currency_type=ctype, amount=amt)
            for cid, cnt in items:
                req.attachments.items.add(config_id=cid, count=cnt)
            return await _call_admin(env, "SendPersonalMail", req, "admin", SECRET_ADMIN)

        # 未知币种 3
        assert (await send_att(1, currencies=[(3, 10)])).error_message.id == ERR_MAIL_ATTACHMENT_INVALID
        # 重复币种
        assert (await send_att(2, currencies=[(0, 10), (0, 20)])).error_message.id == ERR_MAIL_ATTACHMENT_INVALID
        # 零金额
        assert (await send_att(3, currencies=[(1, 0)])).error_message.id == ERR_MAIL_ATTACHMENT_INVALID
        # 未知道具 ID
        assert (await send_att(4, items=[(9999, 1)])).error_message.id == ERR_MAIL_ATTACHMENT_INVALID
        # 超过 16 种道具
        assert (await send_att(5, items=[(i, 1) for i in range(1, 18)])).error_message.id == ERR_MAIL_ATTACHMENT_INVALID
        # 道具总数超过 4096
        assert (await send_att(6, items=[(1, 4000), (2, 97)])).error_message.id == ERR_MAIL_ATTACHMENT_INVALID
        # 边界合法：3 种已知币种 {0,1,2} + 16 种道具且总数恰为 4096
        items_ok = [(i, 1) for i in range(1, 16)] + [(16, 4096 - 15)]
        ok = await send_att(7, currencies=[(0, 1), (1, 2), (2, 3)], items=items_ok)
        assert ok.error_message.id == 0 and ok.mail_id > 0


@_async_test
async def test_scene_mail_outbox_sender_compatibility(monkeypatch):
    """验证真实 nodes/scene_mail_outbox.MailSender (caller=battle, sender_kind=5) 可直接向本 mail 服务发件。"""
    monkeypatch.setenv("MAIL_PRODUCER_SECRET_BATTLE", SECRET_BATTLE.decode("utf-8"))
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack)
        env.role_dir.register(777, "acc_777", 1)

        directory = LocalDirectory()
        info = PROTOCOL.message(
            "NodeInfo",
            node_id=108,
            node_type=MAIL_NODE_TYPE,
            zone_id=1,
            node_uuid="mail-node-uuid-1",
            protocol_type=1,
        )
        info.grpc_endpoint.ip = "127.0.0.1"
        info.grpc_endpoint.port = env.port
        directory.publish(info)

        sender = MailSender(PROTOCOL, directory)
        stack.push_async_callback(sender.close)

        entry = PROTOCOL.message(
            "MailOutboxEntry",
            request_id=55501,
            source=1,
            source_id=9001,
            title="战斗奖励",
            body="背包已满，改由邮件发放",
        )
        entry.items.add(config_id=1001, count=5)

        # 使用实时时钟匹配 MailSender 内部的 time.time_ns() 签名
        env.clock.now_ms = time.time_ns() // 1_000_000
        resp = await sender(777, entry)
        assert resp.error_message.id == 0 and resp.mail_id > 0 and not resp.duplicated

        listed = await _call_client(env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest"), 777, "acc_777", 1)
        assert listed.error_message.id == 0
        assert len(listed.mails) == 1
        assert listed.mails[0].title == "战斗奖励"
        assert listed.mails[0].attachments.items[0].config_id == 1001
        assert listed.mails[0].attachments.items[0].count == 5


# ----------------------------------------------------------------------------------------------------
# 2. 活动冻结名单（Campaign）导入、封存与可见性隔离
# ----------------------------------------------------------------------------------------------------


@_async_test
async def test_campaign_import_seal_and_visibility():
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack)
        env.role_dir.register(10, "acc_10", 1)
        env.role_dir.register(20, "acc_20", 1)
        env.role_dir.register(30, "acc_30", 1)

        h0, u0 = shard_hash([20, 10, 10])  # 去重并排序 -> (10, 20)
        assert u0 == (10, 20)
        mhash = manifest_hash([(0, h0, len(u0))])

        create_req = PROTOCOL.message(
            "mailpb.SendMailRequest",
            sender_kind=1,
            request_id=7001,
            title="全服补偿",
            body="仅冻结名单玩家可见",
            campaign_id=501,
            manifest_generation=1,
            expected_recipient_count=2,
            expected_shard_count=1,
            manifest_hash=mhash,
        )
        create_req.attachments.currencies.add(currency_type=1, amount=100)

        created = await _call_admin(env, "SendSystemMail", create_req, "admin", SECRET_ADMIN)
        assert created.error_message.id == 0 and created.mail_id > 0 and not created.duplicated

        # 导入中重放 SendSystemMail（kind=0 占位行解析活动原 kind=2）-> duplicated=True
        replayed = await _call_admin(env, "SendSystemMail", create_req, "admin", SECRET_ADMIN)
        assert replayed.error_message.id == 0 and replayed.mail_id == created.mail_id and replayed.duplicated is True

        # 封存前不可见
        before_seal = await _call_client(env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest"), 10, "acc_10", 1)
        assert before_seal.error_message.id == 0 and len(before_seal.mails) == 0

        # 缺分片时封存 -> 冲突 17004
        seal_req = PROTOCOL.message("mailpb.SealMailCampaignRequest", sender_kind=1, campaign_id=501, manifest_generation=1)
        assert (await _call_admin(env, "SealMailCampaign", seal_req, "admin", SECRET_ADMIN)).error_message.id == ERR_MAIL_REQUEST_CONFLICT

        # 导入分片 0 + 幂等重放 + 同片不同内容冲突
        imp_req = PROTOCOL.message(
            "mailpb.ImportMailRecipientsRequest",
            sender_kind=1,
            campaign_id=501,
            manifest_generation=1,
            shard_index=0,
            player_ids=[20, 10],
        )
        imp1 = await _call_admin(env, "ImportMailRecipients", imp_req, "admin", SECRET_ADMIN)
        assert imp1.error_message.id == 0 and imp1.imported_count == 2 and not imp1.duplicated

        imp1_dup = await _call_admin(env, "ImportMailRecipients", imp_req, "admin", SECRET_ADMIN)
        assert imp1_dup.error_message.id == 0 and imp1_dup.duplicated is True

        imp_bad = PROTOCOL.message(
            "mailpb.ImportMailRecipientsRequest",
            sender_kind=1,
            campaign_id=501,
            manifest_generation=1,
            shard_index=0,
            player_ids=[30, 10],
        )
        assert (await _call_admin(env, "ImportMailRecipients", imp_bad, "admin", SECRET_ADMIN)).error_message.id == ERR_MAIL_REQUEST_CONFLICT

        # 封存成功 + 二次封存幂等
        sealed = await _call_admin(env, "SealMailCampaign", seal_req, "admin", SECRET_ADMIN)
        assert sealed.error_message.id == 0 and sealed.mail_id == created.mail_id and not sealed.duplicated
        sealed2 = await _call_admin(env, "SealMailCampaign", seal_req, "admin", SECRET_ADMIN)
        assert sealed2.error_message.id == 0 and sealed2.duplicated is True

        # 名单内玩家 10、20 可见；名单外玩家 30 不可见
        for pid, acc, expected in ((10, "acc_10", 1), (20, "acc_20", 1), (30, "acc_30", 0)):
            listed = await _call_client(env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest"), pid, acc, 1)
            assert len(listed.mails) == expected
            if expected:
                assert listed.mails[0].mail_id == created.mail_id
                assert listed.mails[0].kind == MAIL_KIND_SYSTEM
                assert listed.mails[0].claim_state == CLAIM_UNCLAIMED


# ----------------------------------------------------------------------------------------------------
# 3. 游标分页、受保护过期邮件、阅读/删除/一键清理与帮会邮件可见性
# ----------------------------------------------------------------------------------------------------


@_async_test
async def test_cursor_pagination_protected_expired_read_delete_and_clear_read():
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack)
        env.role_dir.register(1, "acc_1", 1)
        env.guild_dir.memberships[1] = 42

        # 发送 3 封个人邮件（2 封同毫秒，1 封更晚）+ 1 封帮会文本邮件
        ids = []
        for idx, (delta, with_att) in enumerate(((0, False), (0, True), (500, False))):
            if delta:
                env.clock.advance(delta)
            req = PROTOCOL.message(
                "mailpb.SendMailRequest",
                sender_kind=1,
                request_id=8000 + idx,
                recipient_player_id=1,
                title=f"m{idx}",
                body=f"body{idx}",
                expire_ms=env.clock() + 10_000,
            )
            if with_att:
                req.attachments.currencies.add(currency_type=0, amount=99)
            resp = await _call_admin(env, "SendPersonalMail", req, "admin", SECRET_ADMIN)
            assert resp.error_message.id == 0
            ids.append(resp.mail_id)

        env.clock.advance(500)
        g_req = PROTOCOL.message(
            "mailpb.SendMailRequest",
            sender_kind=2,
            request_id=8100,
            guild_id=42,
            title="帮会公告",
            body="仅本帮可见",
            expire_ms=env.clock() + 10_000,
        )
        g_resp = await _call_admin(env, "SendGuildMail", g_req, "guild", SECRET_GUILD)
        assert g_resp.error_message.id == 0

        # 按 (sent_ms DESC, mail_id DESC) 分页（limit=2）
        p1 = await _call_client(env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest", limit=2), 1)
        assert p1.error_message.id == 0
        assert [m.mail_id for m in p1.mails] == [g_resp.mail_id, ids[2]]
        assert p1.next_before_mail_id == ids[2]
        assert p1.unread_count == 4 and p1.claimable_count == 1

        p2 = await _call_client(
            env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest", before_mail_id=p1.next_before_mail_id, limit=2), 1
        )
        assert [m.mail_id for m in p2.mails] == [ids[1], ids[0]]
        assert p2.next_before_mail_id == 0

        # 游标不存在 -> 空页但返回准确计数
        p_missing = await _call_client(
            env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest", before_mail_id=999999, limit=10), 1
        )
        assert len(p_missing.mails) == 0 and p_missing.unread_count == 4 and p_missing.claimable_count == 1

        # 阅读 m0 与 m1（m1 有未领附件）
        read0 = await _call_client(env, "ReadMail", PROTOCOL.message("mailpb.ReadMailRequest", mail_id=ids[0]), 1)
        assert read0.error_message.id == 0 and read0.mail.is_read is True
        read1 = await _call_client(env, "ReadMail", PROTOCOL.message("mailpb.ReadMailRequest", mail_id=ids[1]), 1)
        assert read1.error_message.id == 0 and read1.mail.is_read is True

        # ClearReadMails：删掉无附件的 m0，保护有未领附件的 m1
        cleared = await _call_client(env, "ClearReadMails", PROTOCOL.message("mailpb.ClearReadMailsRequest"), 1)
        assert list(cleared.deleted_mail_ids) == [ids[0]]
        assert list(cleared.protected_mail_ids) == [ids[1]]

        # 领取 m1 使其进入 PENDING(2)，随后推进时钟越过全部过期时间
        claimed = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=[ids[1]]), 1
        )
        assert claimed.results[0].claim_state == CLAIM_PENDING
        assert claimed.results[0].error_message.id == ERR_MAIL_CLAIM_PENDING

        env.clock.advance(20_000)

        # 过期后：普通邮件 m2 与帮会邮件不可见，但 PENDING 的 m1 受保护依然可见、可读且不可删
        after_exp = await _call_client(env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest"), 1)
        assert [m.mail_id for m in after_exp.mails] == [ids[1]]
        assert after_exp.claimable_count == 0  # 已过期且非 UNCLAIMED

        read_pending = await _call_client(env, "ReadMail", PROTOCOL.message("mailpb.ReadMailRequest", mail_id=ids[1]), 1)
        assert read_pending.error_message.id == 0 and read_pending.mail.claim_state == CLAIM_PENDING

        read_expired = await _call_client(env, "ReadMail", PROTOCOL.message("mailpb.ReadMailRequest", mail_id=ids[2]), 1)
        assert read_expired.error_message.id == ERR_MAIL_EXPIRED

        del_pending = await _call_client(
            env, "DeleteMails", PROTOCOL.message("mailpb.DeleteMailsRequest", mail_ids=[ids[1]]), 1
        )
        assert list(del_pending.deleted_mail_ids) == []
        assert list(del_pending.protected_mail_ids) == [ids[1]]

        # Sweep 物理清理不会删掉 PENDING 邮件 m1，但会清理已过期无保护的 m2 / 帮会邮件
        sweep_report = await env.store.sweep(env.clock(), 100, True)
        assert sweep_report.personal.deleted >= 1
        assert ids[1] in env.store.state.messages


# ----------------------------------------------------------------------------------------------------
# 4. ClaimMailAttachments + AssetDeliveryLoop 与真实 SceneAssets.AssetCredit 端到端闭环
# ----------------------------------------------------------------------------------------------------


class _FakeScenePlayer:
    def __init__(self, player_id: int, snapshot: PlayerSnapshot) -> None:
        self.player_id = player_id
        self.snapshot = snapshot
        self.disconnect_at = None
        self.asset_anomaly = None


class _FakeSceneHost:
    """挂接真实 SceneAssets.AssetCredit 的最小宿主，支持控制是否同步落盘至 SQL 账本。"""

    def __init__(self, protocol: Protocol, tables: ConfigTables, player_id: int, *, blocked_currencies=()) -> None:
        self.protocol = protocol
        self.rows = PlayerRowCodec(protocol)
        self.info = SimpleNamespace(zone_id=1)
        self.inbox = None
        self.battle = None
        self._locks: dict[int, asyncio.Lock] = {}
        data = protocol.message("player_database")
        for kind in blocked_currencies:
            data.currency.blocked_types.append(kind)
        snapshot = PlayerSnapshot(
            player_id=player_id,
            owner=b"scene-1",
            owner_epoch=1,
            save_seq=1,
            rows=self.rows.project(data),
        )
        player = _FakeScenePlayer(player_id, snapshot)
        world = SimpleNamespace(players={player_id: player})
        self.locations = {player_id: world}
        self.sql_committed_ledgers: dict[int, object] = {}
        self.commit_to_sql = True
        self.player = player
        self.assets = SceneAssets(
            self,
            tables,
            secrets={"mail": SECRET_ASSET_MAIL},
            allocate_item_id=self._alloc_item_id,
        )
        self._next_item_id = 9000

    async def _alloc_item_id(self) -> int:
        self._next_item_id += 1
        return self._next_item_id

    def _lock(self, player_id: int) -> asyncio.Lock:
        return self._locks.setdefault(player_id, asyncio.Lock())

    def mutable(self, player) -> bool:
        return True

    async def flush_player_rows(self, player) -> None:
        return None

    async def commit_player_data(self, player, data, *, on_committed=None) -> None:
        player.snapshot = PlayerSnapshot(
            player_id=player.player_id,
            owner=player.snapshot.owner,
            owner_epoch=player.snapshot.owner_epoch,
            save_seq=player.snapshot.save_seq + 1,
            rows=self.rows.project(data),
        )
        if self.commit_to_sql:
            ledger_copy = self.protocol.message("PlayerAssetOpLedgerComp")
            ledger_copy.CopyFrom(data.asset_op_ledger)
            self.sql_committed_ledgers[player.player_id] = ledger_copy
        if on_committed is not None:
            res = on_committed(player.snapshot)
            if asyncio.iscoroutine(res):
                await res

    def flush_sql_ledger(self, player_id: int) -> None:
        data = self.rows.assemble(self.player.snapshot)
        ledger_copy = self.protocol.message("PlayerAssetOpLedgerComp")
        ledger_copy.CopyFrom(data.asset_op_ledger)
        self.sql_committed_ledgers[player_id] = ledger_copy


class _StaticSceneResolver:
    def __init__(self, target: SceneTarget | None = None, *, absent: bool = False) -> None:
        self.target = target
        self.absent = absent

    async def resolve(self, player_id: int):
        return self.target, self.absent


@_async_test
async def test_claim_and_asset_delivery_loop_with_real_scene_assets_and_sql_gate():
    """使用真实 SceneAssets.AssetCredit gRPC + DataService.GetPlayerAssetOpLedger 强制 SQL 门禁端到端验证。"""
    tables = ConfigTables()
    first_item_id = tables["Item"].rows()[0].id
    async with AsyncExitStack() as stack:
        clock = FakeClock(time.time_ns() // 1_000_000)
        env = await _start_mail_server(stack, clock=clock, item_exists=lambda cid: tables["Item"].get(cid) is not None)
        env.role_dir.register(500, "acc_500", 1)

        # 启动真实 SceneAssets gRPC 服务（AssetCredit）
        scene_host = _FakeSceneHost(PROTOCOL, tables, 500)
        scene_server, scene_port = await serve_grpc(
            PROTOCOL, "127.0.0.1:0", {"scene_node.SceneNodeGrpc": scene_host.assets.grpc_methods()}
        )
        stack.push_async_callback(scene_server.stop, 1.0)
        scene_client = GrpcClient(f"127.0.0.1:{scene_port}", timeout=5, enable_retries=False)
        stack.push_async_callback(scene_client.close)

        # 启动最小 DataService.GetPlayerAssetOpLedger gRPC 服务
        async def get_ledger(req):
            ledger = scene_host.sql_committed_ledgers.get(int(req.player_id))
            if ledger is None:
                return PROTOCOL.message("data_service.GetPlayerAssetOpLedgerResponse", found=False)
            return PROTOCOL.message("data_service.GetPlayerAssetOpLedgerResponse", found=True, ledger=ledger)

        ds_server, ds_port = await serve_grpc(
            PROTOCOL, "127.0.0.1:0", {"data_service.DataService": {"GetPlayerAssetOpLedger": get_ledger}}
        )
        stack.push_async_callback(ds_server.stop, 1.0)
        ds_client = GrpcClient(f"127.0.0.1:{ds_port}", timeout=5, enable_retries=False)
        stack.push_async_callback(ds_client.close)

        resolver = _StaticSceneResolver(SceneTarget(f"127.0.0.1:{scene_port}", scene_client.call))
        caller = AssetOpCaller(PROTOCOL, resolver, SECRET_ASSET_MAIL, requery_delays_sec=(), clock_ms=clock)
        ledger_reader = GrpcDataServiceLedgerReader(PROTOCOL, ds_client.call)
        loop = AssetDeliveryLoop(
            PROTOCOL,
            env.store,
            caller,
            ledger_reader,
            base_backoff_ms=100,
            await_durable_delay_ms=100,
            clock_ms=clock,
            rnd=lambda: 0.5,
        )

        # 发一封带货币与物品的个人邮件并领取
        req = PROTOCOL.message(
            "mailpb.SendMailRequest",
            sender_kind=1,
            request_id=9101,
            recipient_player_id=500,
            title="附件到账测试",
            body="真实 SceneAssets 联调",
        )
        req.attachments.currencies.add(currency_type=0, amount=250)
        req.attachments.items.add(config_id=first_item_id, count=2)
        sent = await _call_admin(env, "SendPersonalMail", req, "admin", SECRET_ADMIN)
        assert sent.error_message.id == 0

        claimed = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=[sent.mail_id]), 500, "acc_500", 1
        )
        op_id = claimed.results[0].asset_op_id
        assert claimed.results[0].claim_state == CLAIM_PENDING and op_id > 0

        # 重复领取返回同一个 op_id，不分配新 seq
        claimed2 = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=[sent.mail_id]), 500, "acc_500", 1
        )
        assert claimed2.results[0].asset_op_id == op_id and claimed2.results[0].claim_state == CLAIM_PENDING

        # 第 1 拍：Scene 已应用且回 durable=True，但 SQL 账本尚未提交 -> 强制门禁拦截，不得终结！
        scene_host.commit_to_sql = False
        assert await loop.tick() == 1
        assert env.store.state.ops[op_id].status == OP_PENDING
        assert env.store.state.states[(500, sent.mail_id)].claim_state == CLAIM_PENDING

        # 随后 SQL 账本落盘，推进时钟过 AwaitDurableDelay -> 第 2 拍核验通过并终结为 CLAIMED(3)
        scene_host.flush_sql_ledger(500)
        clock.advance(200)
        assert await loop.tick() == 1
        assert env.store.state.ops[op_id].status == OP_APPLIED
        assert env.store.state.states[(500, sent.mail_id)].claim_state == CLAIM_CLAIMED

        # 验证真实 Scene 玩家确实入账了金币与物品
        final_data = scene_host.rows.assemble(scene_host.player.snapshot)
        assert balance(final_data, 0) == 250
        assert sum(item.size for _, item in scene_host.assets.entries(final_data.bag_component, 0)) == 2


@_async_test
async def test_asset_loop_rejected_retry_and_sticky_partial_manual():
    """验证 REJECTED 回到 UNCLAIMED 可重试，以及粘性 27007 部分发放最终转 MANUAL(4) + tip 17007。"""
    tables = ConfigTables()
    async with AsyncExitStack() as stack:
        clock = FakeClock(time.time_ns() // 1_000_000)
        env = await _start_mail_server(stack, clock=clock)
        env.role_dir.register(600, "acc_600", 1)

        # ---- 场景 A：真实 SceneAssets 因币种封禁回 REJECTED -> 邮件回退 UNCLAIMED(1) 可再领 ----
        scene_host = _FakeSceneHost(PROTOCOL, tables, 600, blocked_currencies=(0,))
        scene_server, scene_port = await serve_grpc(
            PROTOCOL, "127.0.0.1:0", {"scene_node.SceneNodeGrpc": scene_host.assets.grpc_methods()}
        )
        stack.push_async_callback(scene_server.stop, 1.0)
        scene_client = GrpcClient(f"127.0.0.1:{scene_port}", timeout=5, enable_retries=False)
        stack.push_async_callback(scene_client.close)

        class _DirectLedgerReader:
            def __init__(self) -> None:
                self.override = None

            async def read_sql_committed_ledger(self, player_id: int):
                if self.override is not None:
                    return self.override
                return scene_host.sql_committed_ledgers.get(player_id)

        ledger_reader = _DirectLedgerReader()
        resolver = _StaticSceneResolver(SceneTarget(f"127.0.0.1:{scene_port}", scene_client.call))
        caller = AssetOpCaller(PROTOCOL, resolver, SECRET_ASSET_MAIL, requery_delays_sec=(), clock_ms=clock)
        loop = AssetDeliveryLoop(
            PROTOCOL, env.store, caller, ledger_reader, base_backoff_ms=100, await_durable_delay_ms=100, clock_ms=clock, rnd=lambda: 0.5
        )

        req_rej = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=1, request_id=9201, recipient_player_id=600, title="拒绝重试", body="b"
        )
        req_rej.attachments.currencies.add(currency_type=0, amount=50)
        sent_rej = await _call_admin(env, "SendPersonalMail", req_rej, "admin", SECRET_ADMIN)
        claim1 = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=[sent_rej.mail_id]), 600, "acc_600", 1
        )
        op1_id = claim1.results[0].asset_op_id

        assert await loop.tick() == 1
        assert env.store.state.ops[op1_id].status == OP_REJECTED
        assert env.store.state.states[(600, sent_rej.mail_id)].claim_state == CLAIM_UNCLAIMED

        # 解除封禁后再次领取（分配新 claim_version=2, seq=2）
        data_unblock = scene_host.rows.assemble(scene_host.player.snapshot)
        del data_unblock.currency.blocked_types[:]
        await scene_host.commit_player_data(scene_host.player, data_unblock)

        claim2 = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=[sent_rej.mail_id]), 600, "acc_600", 1
        )
        op2_id = claim2.results[0].asset_op_id
        assert op2_id != op1_id and claim2.results[0].claim_state == CLAIM_PENDING
        assert await loop.tick() == 1
        assert env.store.state.ops[op2_id].status == OP_APPLIED
        assert env.store.state.states[(600, sent_rej.mail_id)].claim_state == CLAIM_CLAIMED

        # ---- 场景 B：粘性 27007 部分发放 -> 经历中间故障仍保留 27007 -> SQL 确认后转 MANUAL(4) + tip 17007 ----
        req_part = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=1, request_id=9202, recipient_player_id=600, title="部分发放", body="b"
        )
        req_part.attachments.currencies.add(currency_type=0, amount=100)
        sent_part = await _call_admin(env, "SendPersonalMail", req_part, "admin", SECRET_ADMIN)
        claim_p = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=[sent_part.mail_id]), 600, "acc_600", 1
        )
        op_p_id = claim_p.results[0].asset_op_id

        # 假 Scene：第一次回 APPLIED+partial+27007（SQL 还没落盘），第二次传输报错，第三次 SQL 落盘且故意不带 partial_seqs（模拟环滑出）
        phase = {"n": 0}

        async def fake_scene_call(path: str, raw: bytes, *, timeout=None) -> bytes:
            phase["n"] += 1
            if phase["n"] == 1:
                resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=False, partial=True)
                resp.reason.id = ERR_ASSET_PARTIAL_APPLIED
                return resp.SerializeToString()
            if phase["n"] == 2:
                raise RuntimeError("transient network drop")
            resp = PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True, partial=False)
            return resp.SerializeToString()

        resolver.target = SceneTarget("fake-scene", fake_scene_call)
        ledger_reader.override = PROTOCOL.message("PlayerAssetOpLedgerComp")  # 尚未包含该 seq

        # 第 1 拍：收到 partial=True, 27007，SQL 未落盘 -> AwaitDurable 重排并记录 last_reason=27007
        assert await loop.tick() == 1
        assert env.store.state.ops[op_p_id].status == OP_PENDING
        assert env.store.state.ops[op_p_id].last_reason == ERR_ASSET_PARTIAL_APPLIED

        # 第 2 拍：传输异常 -> RETRY 重排，但粘性 last_reason=27007 绝不被覆盖为 0！
        clock.advance(200)
        assert await loop.tick() == 1
        assert env.store.state.ops[op_p_id].status == OP_PENDING
        assert env.store.state.ops[op_p_id].last_reason == ERR_ASSET_PARTIAL_APPLIED

        # 第 3 拍：写入 SQL 已提交账本（故意 partial=False 模拟 partial_seqs 被滑窗挤出），靠粘性 27007 仍转 MANUAL(4) + 17007
        op_row = env.store.state.ops[op_p_id]
        fake_ledger = PROTOCOL.message("PlayerAssetOpLedgerComp")
        op_obj = await env.store.claim_op(op_p_id, clock() + 50_000, clock() + 60_000, clock() + 90_000, 999)
        assert op_obj is not None
        req_for_ledger = build_asset_op_request(PROTOCOL, op_obj)
        assert record_ledger(fake_ledger, req_for_ledger, Outcome.APPLIED, 0, partial=False) is not None
        ledger_reader.override = fake_ledger
        clock.advance(70_000)

        assert await loop.tick() == 1
        assert env.store.state.ops[op_p_id].status == OP_MANUAL
        assert env.store.state.ops[op_p_id].reason_tip_id == ERR_MAIL_CLAIM_MANUAL
        st_p = env.store.state.states[(600, sent_part.mail_id)]
        assert st_p.claim_state == CLAIM_MANUAL
        assert st_p.reason_tip_id == ERR_MAIL_CLAIM_MANUAL


@_async_test
async def test_claim_pending_cap_16_enforced():
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack)
        env.role_dir.register(701, "acc_701", 1)

        mail_ids = []
        for i in range(17):
            req = PROTOCOL.message(
                "mailpb.SendMailRequest",
                sender_kind=1,
                request_id=9500 + i,
                recipient_player_id=701,
                title=f"cap-{i}",
                body="b",
            )
            req.attachments.currencies.add(currency_type=0, amount=10)
            resp = await _call_admin(env, "SendPersonalMail", req, "admin", SECRET_ADMIN)
            mail_ids.append(resp.mail_id)

        res = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=mail_ids), 701, "acc_701", 1
        )
        assert len(res.results) == 17
        # 前 16 封成功进入 PENDING，第 17 封因 MaxPending=16 被拒绝（未分配 op_id）
        for r in res.results[:16]:
            assert r.claim_state == CLAIM_PENDING and r.asset_op_id > 0
        assert res.results[16].asset_op_id == 0
        assert res.results[16].error_message.id == ERR_MAIL_CLAIM_PENDING


# ----------------------------------------------------------------------------------------------------
# 5. RoleFenceParticipant（冻结、版本覆盖、删角清理）与在线护栏自愈
# ----------------------------------------------------------------------------------------------------


@_async_test
async def test_role_fence_prepare_apply_purge_and_client_guard():
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack)
        env.role_dir.register(801, "acc_801", 1, version=1)

        # 发 1 封个人带附件邮件 + 1 封已封存活动邮件 + 1 封导入中活动邮件
        p_req = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=1, request_id=9601, recipient_player_id=801, title="个人", body="b"
        )
        p_req.attachments.currencies.add(currency_type=0, amount=10)
        p_sent = await _call_admin(env, "SendPersonalMail", p_req, "admin", SECRET_ADMIN)

        h0, _ = shard_hash([801])
        mh = manifest_hash([(0, h0, 1)])
        for cid, req_id, do_seal in ((901, 9602, True), (902, 9603, False)):
            c_req = PROTOCOL.message(
                "mailpb.SendMailRequest",
                sender_kind=1,
                request_id=req_id,
                title=f"camp-{cid}",
                body="b",
                campaign_id=cid,
                manifest_generation=1,
                expected_recipient_count=1,
                expected_shard_count=1,
                manifest_hash=mh,
            )
            c_req.attachments.currencies.add(currency_type=0, amount=5)
            await _call_admin(env, "SendSystemMail", c_req, "admin", SECRET_ADMIN)
            await _call_admin(
                env,
                "ImportMailRecipients",
                PROTOCOL.message(
                    "mailpb.ImportMailRecipientsRequest",
                    sender_kind=1,
                    campaign_id=cid,
                    manifest_generation=1,
                    shard_index=0,
                    player_ids=[801],
                ),
                "admin",
                SECRET_ADMIN,
            )
            if do_seal:
                await _call_admin(
                    env,
                    "SealMailCampaign",
                    PROTOCOL.message("mailpb.SealMailCampaignRequest", sender_kind=1, campaign_id=cid, manifest_generation=1),
                    "admin",
                    SECRET_ADMIN,
                )

        # 客户端首次访问：副本缺行，自动向 RoleFenceDirectory 自愈后放行
        claimed = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=[p_sent.mail_id]), 801, "acc_801", 1
        )
        op_id = claimed.results[0].asset_op_id
        assert op_id > 0

        # PrepareRoleFreeze（存在 PENDING mail_asset_op）-> 被阻塞，且事务回滚不冻结副本
        freeze_state = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=801,
            account="acc_801",
            owner_revision=1,
            freeze_token=12345,
            freeze_final=False,
            version=2,
            home_zone=1,
        )
        prep_dry = await _call_fence(
            env, "PrepareRoleFreeze", PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", state=freeze_state, dry_run=True)
        )
        assert not prep_dry.clean and prep_dry.blocker == ROLE_BLOCKER_ASSET_OP

        prep_blocked = await _call_fence(
            env, "PrepareRoleFreeze", PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", state=freeze_state)
        )
        assert not prep_blocked.clean and prep_blocked.blocker == ROLE_BLOCKER_ASSET_OP
        assert env.store.state.role_fences[801].version == 1  # 回滚未冻结

        # 终结该资产指令为 APPLIED 后，PrepareRoleFreeze 成功提交冻结
        op_obj = await env.store.claim_op(op_id, env.clock(), env.clock() + 10_000, env.clock() + 60_000, 777)
        assert await env.store.finalize_op(
            op_obj, "applied", int(Outcome.APPLIED), 0, partial=False, sql_committed=True, now_ms=env.clock()
        )
        prep_ok = await _call_fence(
            env, "PrepareRoleFreeze", PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", state=freeze_state)
        )
        assert prep_ok.clean and not prep_ok.stale and prep_ok.blocker == ""
        assert env.store.state.role_fences[801].freeze_token == 12345

        # 权威目录也冻结时，客户端业务读/写被护栏拒绝（回 1005）
        env.role_dir.register(801, "acc_801", 1, freeze_token=12345, version=2)
        frozen_list = await _call_client(env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest"), 801, "acc_801", 1)
        assert frozen_list.error_message.id == ERR_INVALID_PARAMETER

        # 过期版本的 Prepare -> stale=True
        old_freeze = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=801,
            account="acc_801",
            owner_revision=1,
            freeze_token=12345,
            version=1,
            home_zone=1,
        )
        prep_stale = await _call_fence(
            env, "PrepareRoleFreeze", PROTOCOL.message("rolefence.PrepareRoleFreezeRequest", state=old_freeze)
        )
        assert prep_stale.stale and not prep_stale.clean

        # ApplyRoleFence 更新更大版本，低版本不覆盖
        v3 = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=801,
            account="acc_801",
            owner_revision=2,
            freeze_token=0,
            version=3,
            home_zone=1,
        )
        await _call_fence(env, "ApplyRoleFence", PROTOCOL.message("rolefence.ApplyRoleFenceRequest", state=v3))
        assert env.store.state.role_fences[801].version == 3
        await _call_fence(env, "ApplyRoleFence", PROTOCOL.message("rolefence.ApplyRoleFenceRequest", state=freeze_state))
        assert env.store.state.role_fences[801].version == 3

        # PurgeRemovedRole：墓碑写入并清除个人邮件、状态、seq、已封存活动名单、mail_box；保留终态 op 与导入中活动名单
        tombstone = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=801,
            account="",
            owner_revision=2,
            freeze_token=0,
            version=4,
            home_zone=1,
        )
        purged = await _call_fence(env, "PurgeRemovedRole", PROTOCOL.message("rolefence.PurgeRemovedRoleRequest", state=tombstone))
        assert not purged.retry
        assert p_sent.mail_id not in env.store.state.messages
        assert 801 not in env.store.state.boxes
        assert (801, ASSET_STREAM_SYSTEM_CREDIT) not in env.store.state.seqs
        assert (1, 901, 1, 801) not in env.store.state.recipients  # 已封存活动已清理
        assert (1, 902, 1, 801) in env.store.state.recipients  # 导入中活动保留以免封存失败
        assert op_id in env.store.state.ops  # 终态资产审计保留

        # 墓碑后向已删除角色寄个人邮件 -> ERR_MAIL_RECIPIENT_DELETED(17011)
        after_del = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=3, request_id=9699, recipient_player_id=801, title="已删", body="拒收"
        )
        resp_del = await _call_admin(env, "SendPersonalMail", after_del, "rank", SECRET_RANK)
        assert resp_del.error_message.id == ERR_MAIL_RECIPIENT_DELETED


@_async_test
async def test_role_fence_unavailable_maps_to_feature_unavailable():
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack)
        env.role_dir.unavailable = True
        resp = await _call_client(env, "ListMails", PROTOCOL.message("mailpb.ListMailsRequest"), 909, "acc_909", 1)
        assert resp.error_message.id == ERR_FEATURE_UNAVAILABLE


# ----------------------------------------------------------------------------------------------------
# 6. Fail-closed 鉴权测试（Gate Ed25519 / MailAdmin HMAC / RoleFence Coordinator）
# ----------------------------------------------------------------------------------------------------


@_async_test
async def test_fail_closed_gate_hmac_and_coordinator_auth():
    async with AsyncExitStack() as stack:
        env = await _start_mail_server(stack)
        env.role_dir.register(10, "acc_10", 1)

        # 1) ClientPlayerMail 无会话 -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await env.client.call("/mailpb.ClientPlayerMail/ListMails", PROTOCOL.message("mailpb.ListMailsRequest").SerializeToString())
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 2) ClientPlayerMail 错私钥签名 -> UNAUTHENTICATED
        wrong_key = Ed25519PrivateKey.generate()
        bad_md = _client_metadata(wrong_key, "/mailpb.ClientPlayerMail/ListMails", 10)
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await env.client.call(
                "/mailpb.ClientPlayerMail/ListMails",
                PROTOCOL.message("mailpb.ListMailsRequest").SerializeToString(),
                metadata=bad_md,
            )
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # 3) ClientPlayerMail 合法签名但缺少 owner_revision / authenticated_account -> PERMISSION_DENIED
        for kwargs in ({"owner_revision": 0}, {"account": ""}):
            inc_md = _client_metadata(env.gate_key, "/mailpb.ClientPlayerMail/ListMails", 10, **kwargs)
            with pytest.raises(grpc.aio.AioRpcError) as exc:
                await env.client.call(
                    "/mailpb.ClientPlayerMail/ListMails",
                    PROTOCOL.message("mailpb.ListMailsRequest").SerializeToString(),
                    metadata=inc_md,
                )
            assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 4) NotifyMailChanged 即使带合法 Gate 会话也一律拒绝 -> PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await _call_client(env, "NotifyMailChanged", PROTOCOL.message("mailpb.MailChangedEvent", mail_id=1), 10)
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 5) MailAdmin 无签名或篡改签名 -> UNAUTHENTICATED
        raw_req = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=1, request_id=1, recipient_player_id=10, title="t", body="b"
        )
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await env.client.call("/mailpb.MailAdmin/SendPersonalMail", raw_req.SerializeToString())
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED

        sign_service_call("/mailpb.MailAdmin/SendPersonalMail", raw_req, "admin", env.clock(), SECRET_ADMIN)
        raw_req.recipient_player_id = 11  # 篡改载荷
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await env.client.call("/mailpb.MailAdmin/SendPersonalMail", raw_req.SerializeToString())
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # 6) MailAdmin 带客户端会话头 -> PERMISSION_DENIED
        valid_req = PROTOCOL.message(
            "mailpb.SendMailRequest", sender_kind=1, request_id=2, recipient_player_id=10, title="t", body="b"
        )
        client_md = _client_metadata(env.gate_key, "/mailpb.MailAdmin/SendPersonalMail", 10)
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await _call_admin(env, "SendPersonalMail", valid_req, "admin", SECRET_ADMIN, metadata=client_md)
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 7) RoleFenceParticipant 错密钥 -> PERMISSION_DENIED，非法状态 -> INVALID_ARGUMENT
        bad_state = PROTOCOL.message("rolefence.RoleFenceState", player_id=0)
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await _call_fence(
                env,
                "ApplyRoleFence",
                PROTOCOL.message("rolefence.ApplyRoleFenceRequest", state=bad_state),
                secret=b"wrong-coordinator-secret-012345678901",
            )
        assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await _call_fence(
                env,
                "ApplyRoleFence",
                PROTOCOL.message("rolefence.ApplyRoleFenceRequest", state=bad_state),
            )
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT


# ----------------------------------------------------------------------------------------------------
# 7. 配置与 DDL 门禁校验
# ----------------------------------------------------------------------------------------------------


def test_mail_config_and_schema_ddl():
    env = {
        PRODUCER_SECRET_ENVS["admin"]: SECRET_ADMIN.decode(),
        PRODUCER_SECRET_ENVS["battle"]: SECRET_BATTLE.decode(),
        ASSET_OP_SECRET_ENV: SECRET_ASSET_MAIL.decode(),
        COORDINATOR_SECRET_ENV: SECRET_COORDINATOR.decode(),
        ROLE_FENCE_SECRET_ENV: SECRET_ROLE_FENCE_MAIL.decode(),
    }
    fake_settings = SimpleNamespace(
        zone_id=1,
        physical_region=1,
        release_track="canary",
        data_service="127.0.0.1:9000",
        redis="redis://127.0.0.1:6379/0",
        kafka="127.0.0.1:9092",
        kafka_generation=1,
        kafka_partitions=256,
        etcd="127.0.0.1:2379",
        mail_node={"advertise_host": "127.0.0.1"},
    )
    cfg = MailConfig.from_settings(fake_settings, environ=env)
    assert cfg.grpc_port == 50900
    assert cfg.lease_ttl_sec == 60
    assert cfg.runs_background_work is False  # canary 停用后台循环与清理

    # 校验 config/mail_schema.sql 覆盖全部 8 张 mail_* 表 + role_fence 及 4 列 idx_mail_asset_op_0
    ddl = (Path(__file__).resolve().parents[1] / "config" / "mail_schema.sql").read_text("utf-8")
    for table in (
        "mail_message",
        "mail_campaign",
        "mail_campaign_shard",
        "mail_campaign_recipient",
        "mail_box",
        "mail_state",
        "mail_player_op_seq",
        "mail_asset_op",
        "role_fence",
    ):
        assert re.search(rf"CREATE TABLE IF NOT EXISTS `?{table}`?", ddl), f"missing table {table}"
    assert "INDEX `idx_mail_asset_op_0` (`status`,`next_attempt_ms`,`op_id`,`player_id`)" in ddl


@_async_test
async def test_asset_loop_offline_recovery_and_outcome_flip_alert():
    """验证玩家离线达到尝试门槛后可经 SQL 账本终结，以及结局翻转时绝不终结。"""
    async with AsyncExitStack() as stack:
        clock = FakeClock(time.time_ns() // 1_000_000)
        env = await _start_mail_server(stack, clock=clock)
        env.role_dir.register(750, "acc_750", 1)

        # 发送两封邮件并领取
        mids = []
        for req_id in (9701, 9702):
            req = PROTOCOL.message(
                "mailpb.SendMailRequest", sender_kind=1, request_id=req_id, recipient_player_id=750, title="offline", body="b"
            )
            req.attachments.currencies.add(currency_type=0, amount=40)
            sent = await _call_admin(env, "SendPersonalMail", req, "admin", SECRET_ADMIN)
            mids.append(sent.mail_id)

        res = await _call_client(
            env, "ClaimMailAttachments", PROTOCOL.message("mailpb.ClaimMailAttachmentsRequest", mail_ids=mids), 750, "acc_750", 1
        )
        op_offline_id, op_flip_id = res.results[0].asset_op_id, res.results[1].asset_op_id

        # 1) 离线恢复：resolver 返回 (None, True)，ledger_read_min_attempts=1；预先在 SQL 账本记 APPLIED
        ledger = PROTOCOL.message("PlayerAssetOpLedgerComp")
        op_off = await env.store.claim_op(op_offline_id, clock(), clock() + 100, clock() + 1000, 111)
        assert op_off is not None
        assert record_ledger(ledger, build_asset_op_request(PROTOCOL, op_off), Outcome.APPLIED, 0) is not None
        await env.store.reschedule_op(op_off, clock(), int(Outcome.NOT_HERE), 0, False, True, clock())  # attempts -> 1

        class _StaticLedger:
            async def read_sql_committed_ledger(self, player_id: int):
                return ledger

        resolver_offline = _StaticSceneResolver(None, absent=True)
        caller_offline = AssetOpCaller(PROTOCOL, resolver_offline, SECRET_ASSET_MAIL, requery_delays_sec=(), clock_ms=clock)
        loop_offline = AssetDeliveryLoop(
            PROTOCOL,
            env.store,
            caller_offline,
            _StaticLedger(),
            ledger_read_min_attempts=1,
            base_backoff_ms=100,
            clock_ms=clock,
            rnd=lambda: 0.5,
        )
        clock.advance(200)
        assert await loop_offline.claim_and_process(op_offline_id) is True
        assert env.store.state.ops[op_offline_id].status == OP_APPLIED
        assert env.store.state.states[(750, mids[0])].claim_state == CLAIM_CLAIMED

        # 2) 结局翻转：Scene 回 APPLIED，但 SQL 账本记的是 REJECTED -> Alert，绝不终结
        op_flip = await env.store.claim_op(op_flip_id, clock(), clock() + 100, clock() + 1000, 222)
        assert op_flip is not None
        assert record_ledger(ledger, build_asset_op_request(PROTOCOL, op_flip), Outcome.REJECTED, 27005) is not None
        await env.store.reschedule_op(op_flip, clock(), 0, 0, False, False, clock())

        async def scene_says_applied(path: str, raw: bytes, *, timeout=None) -> bytes:
            return PROTOCOL.message("AssetOpResponse", outcome=int(Outcome.APPLIED), durable=True).SerializeToString()

        resolver_flip = _StaticSceneResolver(SceneTarget("flip-scene", scene_says_applied))
        caller_flip = AssetOpCaller(PROTOCOL, resolver_flip, SECRET_ASSET_MAIL, requery_delays_sec=(), clock_ms=clock)
        loop_flip = AssetDeliveryLoop(
            PROTOCOL,
            env.store,
            caller_flip,
            _StaticLedger(),
            max_backoff_ms=5000,
            clock_ms=clock,
            rnd=lambda: 0.5,
        )
        clock.advance(200)
        assert await loop_flip.claim_and_process(op_flip_id) is True
        assert env.store.state.ops[op_flip_id].status == OP_PENDING
        assert env.store.state.states[(750, mids[1])].claim_state == CLAIM_PENDING


@_async_test
async def test_sql_mail_store_isolation_unhex_and_schema_verify():
    """验证 SqlMailStore 的 RR 列表事务、UNHEX(%s) 二进制绑定以及 verify_schema 门禁。"""
    executed: list[tuple[str, tuple]] = []

    class _FakeCursor:
        def __init__(self, responses):
            self._responses = responses
            self.rowcount = 1
            self._current = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, sql: str, args=()):
            executed.append((sql, tuple(args) if args else ()))
            self._current = self._responses.pop(0) if self._responses else []

        async def fetchone(self):
            if not self._current:
                return None
            return self._current[0]

        async def fetchall(self):
            return list(self._current or ())

    class _FakeConn:
        def __init__(self, responses):
            self._responses = responses
            self.begins = 0
            self.commits = 0
            self.rollbacks = 0

        async def begin(self):
            self.begins += 1

        async def commit(self):
            self.commits += 1

        async def rollback(self):
            self.rollbacks += 1

        def cursor(self):
            return _FakeCursor(self._responses)

    class _AcquireCtx:
        def __init__(self, conn):
            self._conn = conn

        async def __aenter__(self):
            return self._conn

        async def __aexit__(self, *args):
            return False

    class _FakePool:
        def __init__(self, responses):
            self.conn = _FakeConn(responses)

        def acquire(self):
            return _AcquireCtx(self.conn)

    # 1) verify_schema 检查 9 张表 + role_fence.account VARBINARY(191) + 4 列 idx_mail_asset_op_0
    responses_ok = [[]] * 9 + [
        [("varbinary", 191)],
        [
            ("idx_mail_asset_op_0", "status"),
            ("idx_mail_asset_op_0", "next_attempt_ms"),
            ("idx_mail_asset_op_0", "op_id"),
            ("idx_mail_asset_op_0", "player_id"),
        ],
    ]
    pool = _FakePool(responses_ok)
    ids_iter = iter(range(10, 100))

    async def next_id():
        return next(ids_iter)

    store = SqlMailStore(
        pool,
        PROTOCOL,
        mail_ids=next_id,
        op_ids=next_id,
        item_exists=lambda _: True,
        role_directory=FakeRoleDirectory(PROTOCOL),
    )
    await store.verify_schema()

    # 2) ApplyRoleFence 使用 UNHEX(%s) 绑定 account 字节
    executed.clear()
    st = PROTOCOL.message(
        "rolefence.RoleFenceState",
        player_id=42,
        account="acct_utf8",
        owner_revision=1,
        version=2,
        home_zone=1,
    )
    await store.apply_role_fence(st)
    assert any("UNHEX(%s)" in sql and "acct_utf8".encode("utf-8").hex() in args for sql, args in executed)

    # 3) ListMails 事务必须显式切 REPEATABLE READ
    executed.clear()
    pool.conn._responses = [
        [],  # SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
        [(b"acct_utf8", 1, 0, 0, 2, 1, 100)],  # lock_role_fence
        [(0, 0)],  # list_visible_counts
        [],  # list_visible_page
    ]
    from xuanming.mail import RoleOriginal

    listed = await store.list_mails(RoleOriginal(42, "acct_utf8", 1), PROTOCOL.message("mailpb.ListMailsRequest"))
    assert listed.unread_count == 0 and len(listed.mails) == 0
    assert executed[0][0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"


