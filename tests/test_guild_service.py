"""guild 服务端到端 gRPC、权限、改名保护、申请、排行榜、经济活动、内部回档闸、护栏与 SQL 绑定测试。"""

from __future__ import annotations

import asyncio
import functools
from contextlib import AsyncExitStack

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata, sign_service_call
from xuanming.guild import (
    ACTIVITY_STATE_DISABLED,
    ACTIVITY_TYPE_TRIAL,
    ASSET_STREAM_GUILD_DEBIT,
    AssetOpRow,
    COORDINATOR_SECRET_ENV,
    DEFAULT_GRPC_PORT,
    ERR_GUILD_ACTIVITY_ALREADY_CLAIMED,
    ERR_GUILD_ACTIVITY_NOT_OPEN,
    ERR_GUILD_ACTIVITY_THRESHOLD_NOT_REACHED,
    ERR_GUILD_ALREADY_IN_GUILD,
    ERR_GUILD_APPLICATION_NOT_FOUND,
    ERR_GUILD_ASSET_PENDING,
    ERR_GUILD_BUSY_RETRY,
    ERR_GUILD_CANNOT_TARGET_SELF,
    ERR_GUILD_FUNDS_INSUFFICIENT,
    ERR_GUILD_LEADER_CANT_LEAVE,
    ERR_GUILD_NAME_INVALID,
    ERR_GUILD_NAME_TAKEN,
    ERR_GUILD_NOT_FOUND,
    ERR_GUILD_NOT_IN_GUILD,
    ERR_GUILD_NOT_LEADER,
    ERR_GUILD_NOT_RANKED,
    ERR_GUILD_NO_PERMISSION,
    ERR_GUILD_OFFICER_LIMIT,
    ERR_GUILD_RANK_TOO_LOW,
    ERR_GUILD_RENAME_COOLDOWN,
    ERR_GUILD_RENAME_SAME_NAME,
    ERR_INVALID_PARAMETER,
    ERR_OK,
    GUILD_NODE_TYPE,
    GuildConfig,
    GuildService,
    MemoryGuildRankStore,
    MemoryGuildStore,
    MergedZones,
    OP_KIND_DONATE,
    OP_STATUS_APPLIED,
    RANK_WRITE_DISBANDED,
    REQUIRED_TABLES,
    RETENTION_REJECTED_PREFIX,
    ROLE_BLOCKER_GUILD_LEADER,
    ROLE_BLOCKER_GUILD_MEMBER,
    ROLE_FENCE_SECRET_ENV,
    ROLE_LEADER,
    ROLE_MEMBER,
    ROLE_OFFICER,
    RoleFenceRow,
    SERVICE_GUILD,
    SERVICE_GUILD_INTERNAL,
    SERVICE_ROLE_FENCE,
    SqlGuildStore,
    guild_name_norm,
    load_schema_statements,
    removal_token,
    role_rank,
    zone_rank_key,
    zone_rank_tombstone_key,
    zone_rank_version_key,
)
from xuanming.guild.errors import RoleFenceUnavailableError
from xuanming.guild.model import MS_PER_DAY
from xuanming.guild.sql_store import (
    SQL_APPLY_ROLE_FENCE,
    SQL_DEBIT_MEMBER_CONTRIBUTION,
    SQL_INSERT_ASSET_OP,
    SQL_LOCK_MEMBER,
    SQL_SET_MEMBER_CONTRIBUTION,
    SQL_UPDATE_MEMBER_ROLE,
    _SqlTx,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.protocol import Protocol

PROTOCOL = Protocol()

SECRET_COORDINATOR = b"role-fence-coordinator-secret-01234567"
SECRET_ROLE_FENCE_GUILD = b"role-fence-secret-for-guild-caller-012"
SECRET_ASSET_GUILD = b"asset-op-secret-for-guild-caller-01234"


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeClock:
    def __init__(self, start_ms: int = 1_750_000_000_000) -> None:
        self.now_ms = start_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, delta_ms: int) -> int:
        self.now_ms += delta_ms
        return self.now_ms


class FakeRoleDirectory:
    def __init__(self) -> None:
        self.states: dict[int, object] = {}
        self.unavailable = False

    def register(
        self,
        player_id: int,
        account: str | None = None,
        owner_revision: int = 1,
        *,
        freeze_token: int = 0,
        freeze_final: bool = False,
        version: int = 1,
        home_zone: int = 1,
    ):
        acc = f"acc_{player_id}" if account is None else account
        state = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=player_id,
            account=acc,
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


class FakeHomeZones:
    def __init__(self, zones: dict[int, int] | None = None) -> None:
        self.zones = dict(zones or {})

    async def get_home_zone(self, player_id: int) -> int:
        return self.zones.get(player_id, 0)


class FakeNames:
    def __init__(self, names: dict[int, str] | None = None) -> None:
        self.names = dict(names or {})

    async def batch_get_names(self, player_ids):
        return {pid: self.names.get(pid, f"P{pid}") for pid in player_ids}


class FakeOnline:
    def __init__(self, online_ids: set[int] | None = None) -> None:
        self.online_ids = set(online_ids or ())

    async def batch_online(self, player_ids, *, strict: bool = False):
        return {pid: (pid in self.online_ids) for pid in player_ids}


class GuildTestHarness:
    def __init__(
        self,
        *,
        merged_zones: MergedZones | None = None,
        asset_op_enabled: bool = False,
        trials_enabled: bool = False,
        terminal_retention_days: int = 30,
    ) -> None:
        self.clock = FakeClock()
        self.gate_key = Ed25519PrivateKey.generate()
        self.verifier = CallerVerifier([self.gate_key.public_key()], allowed_callers=("gate",))
        self.roles = FakeRoleDirectory()
        self.home_zones = FakeHomeZones()
        self.names = FakeNames()
        self.online = FakeOnline()
        self.zones = merged_zones or MergedZones()
        self.store = MemoryGuildStore(
            PROTOCOL,
            role_directory=self.roles,
            merged_zones=self.zones,
            clock_ms=self.clock,
        )
        self.rank_store = MemoryGuildRankStore(merged_zones=self.zones, clock_ms=self.clock)
        self.service = GuildService(
            PROTOCOL,
            self.store,
            rank_store=self.rank_store,
            caller_verifier=self.verifier,
            coordinator_secret=SECRET_COORDINATOR,
            home_zones=self.home_zones,
            player_names=self.names,
            online_resolver=self.online,
            asset_op_enabled=asset_op_enabled,
            terminal_retention_days=terminal_retention_days,
            trials_enabled=trials_enabled,
            clock_ms=self.clock,
        )
        self._stack = AsyncExitStack()
        self.client: GrpcClient | None = None

    def register_player(self, player_id: int, *, zone_id: int = 1, account: str | None = None, online: bool = True):
        acc = f"acc_{player_id}" if account is None else account
        self.roles.register(player_id, account=acc, home_zone=zone_id)
        self.home_zones.zones[player_id] = zone_id
        self.names.names[player_id] = f"Player{player_id}"
        if online:
            self.online.online_ids.add(player_id)
        return acc

    async def __aenter__(self) -> "GuildTestHarness":
        await self._stack.__aenter__()
        server, port = await serve_grpc(
            PROTOCOL,
            "127.0.0.1:0",
            self.service.services(),
            pass_context=True,
        )
        self._stack.push_async_callback(server.stop, 0.5)
        self.client = GrpcClient(f"127.0.0.1:{port}", timeout=3.0)
        self._stack.push_async_callback(self.client.close)
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self._stack.__aexit__(*exc_info)

    def _gate_md(self, player_id: int, path: str, *, account: str | None = None, owner_revision: int = 1):
        acc = f"acc_{player_id}" if account is None else account
        details = PROTOCOL.message(
            "SessionDetails",
            session_id=player_id * 10 + 1,
            gate_instance_id="gate-1",
            session_incarnation=1,
            player_id=player_id,
            authenticated_account=acc,
            owner_revision=owner_revision,
        )
        return session_metadata(details.SerializeToString(deterministic=True), path, self.gate_key)

    async def call_guild(
        self,
        method: str,
        req_msg_name: str,
        resp_msg_name: str,
        *,
        player_id: int | None = None,
        account: str | None = None,
        owner_revision: int = 1,
        body: dict[str, Any] | None = None,
        **fields,
    ):
        assert self.client is not None
        path = f"/{SERVICE_GUILD}/{method}"
        payload_fields = dict(body) if body is not None else fields
        req = PROTOCOL.message(req_msg_name, **payload_fields)
        md = self._gate_md(player_id, path, account=account, owner_revision=owner_revision) if player_id else ()
        raw = await self.client.call(path, req.SerializeToString(deterministic=True), metadata=md)
        return PROTOCOL.parse(resp_msg_name, raw)

    async def call_internal(self, method: str, req_msg_name: str, resp_msg_name: str, *, md=(), **fields):
        assert self.client is not None
        path = f"/{SERVICE_GUILD_INTERNAL}/{method}"
        req = PROTOCOL.message(req_msg_name, **fields)
        raw = await self.client.call(path, req.SerializeToString(deterministic=True), metadata=md)
        return PROTOCOL.parse(resp_msg_name, raw)

    async def call_role_fence(
        self,
        method: str,
        req_msg_name: str,
        resp_msg_name: str,
        *,
        secret: bytes = SECRET_COORDINATOR,
        caller: str = "coordinator",
        sign: bool = True,
        md=(),
        **fields,
    ):
        assert self.client is not None
        path = f"/{SERVICE_ROLE_FENCE}/{method}"
        req = PROTOCOL.message(req_msg_name, **fields)
        if sign:
            sign_service_call(path, req, caller, self.clock(), secret)
        raw = await self.client.call(path, req.SerializeToString(deterministic=True), metadata=md)
        return PROTOCOL.parse(resp_msg_name, raw)


# -- 1. 核心 CRUD / 匿名 GetPlayerGuild / 公告 / 改名保护与冷却 --------------------------------


@_async_test
async def test_core_crud_anonymous_get_player_guild_and_rename_hold_cooldown():
    async with GuildTestHarness() as h:
        h.register_player(101, zone_id=1)
        h.register_player(102, zone_id=1)
        h.register_player(201, zone_id=2)

        # 建帮 + 名字规范化（NFKC + trim + lower）
        bad = await h.call_guild(
            "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=101, name="   "
        )
        assert bad.error_message.id == ERR_GUILD_NAME_INVALID

        created = await h.call_guild(
            "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=101, name=" 青云门 "
        )
        assert created.error_message.id == ERR_OK
        gid = created.guild.guild_id
        assert created.guild.name == "青云门"
        assert created.guild.leader_id == 101
        assert created.guild.leader_name == "Player101"
        assert created.guild.level == 1
        assert created.guild.max_members == 30
        assert created.guild.zone_id == 1

        # 全局重名（不同区、全角/大小写等价）一律拒绝
        h.register_player(103, zone_id=2)
        dup_zone = await h.call_guild(
            "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=103, name="青云门"
        )
        assert dup_zone.error_message.id == ERR_GUILD_NAME_TAKEN

        # 跨区 GetGuild 返回 kGuildNotFound（隐藏别区帮会存在性）
        cross = await h.call_guild(
            "GetGuild", "guildpb.GetGuildRequest", "guildpb.GetGuildResponse", player_id=201, guild_id=gid
        )
        assert cross.error_message.id == ERR_GUILD_NOT_FOUND

        # 客户端 GetPlayerGuild 与匿名内部调用（mail 查帮会）
        client_pg = await h.call_guild(
            "GetPlayerGuild", "guildpb.GetPlayerGuildRequest", "guildpb.GetPlayerGuildResponse", player_id=101
        )
        assert client_pg.error_message.id == ERR_OK
        assert client_pg.guild.guild_id == gid

        anon_pg = await h.call_guild(
            "GetPlayerGuild",
            "guildpb.GetPlayerGuildRequest",
            "guildpb.GetPlayerGuildResponse",
            player_id=None,
            body={"player_id": 101},
        )
        assert anon_pg.error_message.id == ERR_OK
        assert anon_pg.guild.guild_id == gid
        assert anon_pg.guild.pending_application_count == 0

        # 改公告：非成员被拒，帮主成功
        no_perm = await h.call_guild(
            "SetAnnouncement",
            "guildpb.SetAnnouncementRequest",
            "guildpb.SetAnnouncementResponse",
            player_id=102,
            guild_id=gid,
            announcement="hi",
        )
        assert no_perm.error_message.id == ERR_GUILD_NO_PERMISSION

        ann = await h.call_guild(
            "SetAnnouncement",
            "guildpb.SetAnnouncementRequest",
            "guildpb.SetAnnouncementResponse",
            player_id=101,
            guild_id=gid,
            announcement="欢迎加入青云门",
        )
        assert ann.error_message.id == ERR_OK
        assert ann.guild.announcement == "欢迎加入青云门"

        # 帮会改名：资金不足 -> 同名 -> 改名成功扣资金 -> 旧名保护期挡他帮 -> 冷却期拒绝 -> 过期后他帮可抢注
        no_funds = await h.call_guild(
            "RenameGuild",
            "guildpb.RenameGuildRequest",
            "guildpb.RenameGuildResponse",
            player_id=101,
            guild_id=gid,
            name="天剑宗",
        )
        assert no_funds.error_message.id == ERR_GUILD_FUNDS_INSUFFICIENT

        h.store.set_guild_funds_for_test(gid, 5000)
        same = await h.call_guild(
            "RenameGuild",
            "guildpb.RenameGuildRequest",
            "guildpb.RenameGuildResponse",
            player_id=101,
            guild_id=gid,
            name="青云门",
        )
        assert same.error_message.id == ERR_GUILD_RENAME_SAME_NAME

        renamed = await h.call_guild(
            "RenameGuild",
            "guildpb.RenameGuildRequest",
            "guildpb.RenameGuildResponse",
            player_id=101,
            guild_id=gid,
            name="天剑宗",
        )
        assert renamed.error_message.id == ERR_OK
        assert renamed.guild.name == "天剑宗"
        assert renamed.guild.funds == 5000 - h.store.catalog.rule.rename_cost_funds
        assert renamed.guild.renamed_ms == h.clock()
        assert renamed.next_rename_time == (h.clock() + h.store.catalog.rule.rename_cooldown_ms) // 1000

        # 旧名 "青云门" 处于保护期，他帮不可建帮使用
        held = await h.call_guild(
            "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=102, name="青云门"
        )
        assert held.error_message.id == ERR_GUILD_NAME_TAKEN

        # 冷却期内再次改名被拒并带回 next_rename_time
        cd = await h.call_guild(
            "RenameGuild",
            "guildpb.RenameGuildRequest",
            "guildpb.RenameGuildResponse",
            player_id=101,
            guild_id=gid,
            name="无极宫",
        )
        assert cd.error_message.id == ERR_GUILD_RENAME_COOLDOWN
        assert cd.next_rename_time == renamed.next_rename_time

        # 保护期过后他帮可以使用旧名建帮
        h.clock.advance(h.store.catalog.rule.name_hold_ms + 1000)
        reused = await h.call_guild(
            "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=102, name="青云门"
        )
        assert reused.error_message.id == ERR_OK
        assert reused.guild.name == "青云门"


# -- 2. 入帮申请 / 待审数可见性 / 跨区拦截 / 成员管理与 expected_guild_id ----------------------


@_async_test
async def test_applications_visibility_members_and_expected_guild_id():
    async with GuildTestHarness() as h:
        for pid in (101, 102, 103, 104, 105):
            h.register_player(pid, zone_id=1)
        h.register_player(201, zone_id=2)

        g1 = (
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=101, name="宗门甲"
            )
        ).guild
        gid1 = g1.guild_id

        # 跨区申请返回 kGuildNotFound
        cross_apply = await h.call_guild(
            "ApplyJoinGuild", "guildpb.ApplyJoinGuildRequest", "guildpb.ApplyJoinGuildResponse", player_id=201, guild_id=gid1
        )
        assert cross_apply.error_message.id == ERR_GUILD_NOT_FOUND

        # 102、103、104 申请加入 gid1；104 随后撤回
        for pid in (102, 103, 104):
            r = await h.call_guild(
                "ApplyJoinGuild", "guildpb.ApplyJoinGuildRequest", "guildpb.ApplyJoinGuildResponse", player_id=pid, guild_id=gid1
            )
            assert r.error_message.id == ERR_OK

        # 102 查看本人申请列表
        my_apps = await h.call_guild(
            "ListMyGuildApplications",
            "guildpb.ListMyGuildApplicationsRequest",
            "guildpb.ListMyGuildApplicationsResponse",
            player_id=102,
        )
        assert my_apps.error_message.id == ERR_OK
        assert [a.guild_id for a in my_apps.applications] == [gid1]

        # 104 撤回申请
        cancel = await h.call_guild(
            "CancelGuildApplication",
            "guildpb.CancelGuildApplicationRequest",
            "guildpb.CancelGuildApplicationResponse",
            player_id=104,
            guild_id=gid1,
        )
        assert cancel.error_message.id == ERR_OK

        # 帮主看 pending_application_count=2；外人(105)看 0；匿名内部看 0
        leader_view = await h.call_guild(
            "GetGuild", "guildpb.GetGuildRequest", "guildpb.GetGuildResponse", player_id=101, guild_id=gid1
        )
        assert leader_view.guild.pending_application_count == 2
        outsider_view = await h.call_guild(
            "GetGuild", "guildpb.GetGuildRequest", "guildpb.GetGuildResponse", player_id=105, guild_id=gid1
        )
        assert outsider_view.guild.pending_application_count == 0

        # 帮主列出申请者并审批：通过 102，拒绝 103
        apps = await h.call_guild(
            "ListGuildApplications",
            "guildpb.ListGuildApplicationsRequest",
            "guildpb.ListGuildApplicationsResponse",
            player_id=101,
        )
        assert [a.player_id for a in apps.applicants] == [102, 103]

        rev_ok = await h.call_guild(
            "ReviewGuildApplication",
            "guildpb.ReviewGuildApplicationRequest",
            "guildpb.ReviewGuildApplicationResponse",
            player_id=101,
            applicant_player_id=102,
            approve=True,
        )
        assert rev_ok.error_message.id == ERR_OK
        assert len(rev_ok.guild.members) == 2

        rev_no = await h.call_guild(
            "ReviewGuildApplication",
            "guildpb.ReviewGuildApplicationRequest",
            "guildpb.ReviewGuildApplicationResponse",
            player_id=101,
            applicant_player_id=103,
            approve=False,
        )
        assert rev_no.error_message.id == ERR_OK

        # 普通成员(102) 看不到 pending_application_count（先让 104 再申请一次）
        await h.call_guild(
            "ApplyJoinGuild", "guildpb.ApplyJoinGuildRequest", "guildpb.ApplyJoinGuildResponse", player_id=104, guild_id=gid1
        )
        member_view = await h.call_guild(
            "GetPlayerGuild", "guildpb.GetPlayerGuildRequest", "guildpb.GetPlayerGuildResponse", player_id=102
        )
        assert member_view.guild.pending_application_count == 0
        # 任命 102 为长老后可见 pending_application_count=1
        bad_role = await h.call_guild(
            "SetGuildMemberRole",
            "guildpb.SetGuildMemberRoleRequest",
            "guildpb.SetGuildMemberRoleResponse",
            player_id=101,
            target_player_id=102,
            role=ROLE_LEADER,
        )
        assert bad_role.error_message.id == ERR_GUILD_NO_PERMISSION

        promote = await h.call_guild(
            "SetGuildMemberRole",
            "guildpb.SetGuildMemberRoleRequest",
            "guildpb.SetGuildMemberRoleResponse",
            player_id=101,
            target_player_id=102,
            role=ROLE_OFFICER,
            expected_guild_id=gid1,
        )
        assert promote.error_message.id == ERR_OK
        officer_view = await h.call_guild(
            "GetPlayerGuild", "guildpb.GetPlayerGuildRequest", "guildpb.GetPlayerGuildResponse", player_id=102
        )
        assert officer_view.guild.pending_application_count == 1

        # expected_guild_id 不符：成员管理回 kGuildNotInGuild 并附当前帮会快照
        mismatch = await h.call_guild(
            "KickGuildMember",
            "guildpb.KickGuildMemberRequest",
            "guildpb.KickGuildMemberResponse",
            player_id=101,
            target_player_id=102,
            expected_guild_id=999999,
        )
        assert mismatch.error_message.id == ERR_GUILD_NOT_IN_GUILD
        assert mismatch.guild.guild_id == gid1

        # 帮主不可退帮；转让帮主给 102 后 101 降为长老，随后 101 主动退帮成功
        cant_leave = await h.call_guild(
            "LeaveGuild", "guildpb.LeaveGuildRequest", "guildpb.LeaveGuildResponse", player_id=101, expected_guild_id=gid1
        )
        assert cant_leave.error_message.id == ERR_GUILD_LEADER_CANT_LEAVE

        transferred = await h.call_guild(
            "TransferGuildLeader",
            "guildpb.TransferGuildLeaderRequest",
            "guildpb.TransferGuildLeaderResponse",
            player_id=101,
            target_player_id=102,
            expected_guild_id=gid1,
        )
        assert transferred.error_message.id == ERR_OK
        assert transferred.guild.leader_id == 102
        old_leader = next(m for m in transferred.guild.members if m.player_id == 101)
        assert old_leader.role == ROLE_OFFICER

        left = await h.call_guild(
            "LeaveGuild", "guildpb.LeaveGuildRequest", "guildpb.LeaveGuildResponse", player_id=101, expected_guild_id=gid1
        )
        assert left.error_message.id == ERR_OK

        # 102 解散帮会
        disband_wrong = await h.call_guild(
            "DisbandGuild", "guildpb.DisbandGuildRequest", "guildpb.DisbandGuildResponse", player_id=102, expected_guild_id=999999
        )
        assert disband_wrong.error_message.id == ERR_GUILD_NOT_IN_GUILD

        disband_ok = await h.call_guild(
            "DisbandGuild", "guildpb.DisbandGuildRequest", "guildpb.DisbandGuildResponse", player_id=102, expected_guild_id=gid1
        )
        assert disband_ok.error_message.id == ERR_OK


# -- 3. 按区分榜（合服映射 + 忽略 req.zone_id + 解散墓碑拦截旧写入） ----------------------------


@_async_test
async def test_ranking_merged_zones_and_disband_tombstone():
    merged = MergedZones({2: 1})
    async with GuildTestHarness(merged_zones=merged) as h:
        h.register_player(101, zone_id=1)
        h.register_player(102, zone_id=2)  # 2 合服到 1

        g1 = (
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=101, name="合服甲"
            )
        ).guild
        g2 = (
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=102, name="合服乙"
            )
        ).guild
        assert g2.zone_id == 1

        # UpdateGuildScore 忽略错误的 req.zone_id=99，按库内权威 zone_id 映射进最终区 1
        u1 = await h.call_guild(
            "UpdateGuildScore",
            "guildpb.UpdateGuildScoreRequest",
            "guildpb.UpdateGuildScoreResponse",
            guild_id=g1.guild_id,
            score=100,
            zone_id=99,
        )
        u2 = await h.call_guild(
            "UpdateGuildScore",
            "guildpb.UpdateGuildScoreRequest",
            "guildpb.UpdateGuildScoreResponse",
            guild_id=g2.guild_id,
            score=250,
            zone_id=99,
        )
        assert u1.error_message.id == ERR_OK and u2.error_message.id == ERR_OK

        page = await h.call_guild(
            "GetGuildRank",
            "guildpb.GetGuildRankRequest",
            "guildpb.GetGuildRankResponse",
            player_id=101,
            page=1,
            page_size=10,
        )
        assert page.total_count == 2
        assert [e.guild_id for e in page.entries] == [g2.guild_id, g1.guild_id]
        assert [e.rank for e in page.entries] == [1, 2]

        by_guild = await h.call_guild(
            "GetGuildRankByGuild",
            "guildpb.GetGuildRankByGuildRequest",
            "guildpb.GetGuildRankByGuildResponse",
            player_id=102,
            guild_id=g1.guild_id,
        )
        assert by_guild.entry.rank == 2 and by_guild.entry.score == 100

        # 解散 g2 后摘榜并立墓碑：迟到的旧推分被墓碑拒绝
        await h.call_guild(
            "DisbandGuild", "guildpb.DisbandGuildRequest", "guildpb.DisbandGuildResponse", player_id=102
        )
        not_ranked = await h.call_guild(
            "GetGuildRankByGuild",
            "guildpb.GetGuildRankByGuildRequest",
            "guildpb.GetGuildRankByGuildResponse",
            player_id=101,
            guild_id=g2.guild_id,
        )
        assert not_ranked.error_message.id == ERR_GUILD_NOT_RANKED
        assert await h.rank_store.upsert_score(g2.guild_id, 2, 999, 99) == RANK_WRITE_DISBANDED

        assert zone_rank_key(1) == "guild_rank:zone:{gr:1}"
        assert zone_rank_version_key(1) == "guild_rank:version:{gr:1}"
        assert zone_rank_tombstone_key(1) == "guild_rank:tombstone:{gr:1}"


# -- 4. 升级 + 捐献/商店（关闭态与开启态幂等）+ 灯会/团圆/历练 ----------------------------------


@_async_test
async def test_upgrade_economy_and_activities():
    # 先测 asset_op_enabled=False 时的关闭契约 + UpgradeGuild + LightGuildLantern + Trials
    async with GuildTestHarness(asset_op_enabled=False, trials_enabled=False) as h:
        h.register_player(101, zone_id=1, online=True)
        h.register_player(102, zone_id=1, online=True)
        h.register_player(103, zone_id=1, online=True)

        g = (
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=101, name="经济帮"
            )
        ).guild
        gid = g.guild_id

        # 资金不足升级：回 kGuildFundsInsufficient 且附带 GuildInfo
        insuff = await h.call_guild(
            "UpgradeGuild", "guildpb.UpgradeGuildRequest", "guildpb.UpgradeGuildResponse", player_id=101, expected_level=1
        )
        assert insuff.error_message.id == ERR_GUILD_FUNDS_INSUFFICIENT
        assert insuff.guild.guild_id == gid and insuff.guild.level == 1

        # 充入足够资金升到 2 级（花费 20000）
        h.store.set_guild_funds_for_test(gid, 30000)
        up = await h.call_guild(
            "UpgradeGuild", "guildpb.UpgradeGuildRequest", "guildpb.UpgradeGuildResponse", player_id=101, expected_level=1
        )
        assert up.error_message.id == ERR_OK
        assert up.guild.level == 2 and up.guild.funds == 10000 and up.guild.max_members == 35

        # 过期 expected_level=1：幂等回 OK + 最新 GuildInfo，不再扣资金
        stale_up = await h.call_guild(
            "UpgradeGuild", "guildpb.UpgradeGuildRequest", "guildpb.UpgradeGuildResponse", player_id=101, expected_level=1
        )
        assert stale_up.error_message.id == ERR_OK
        assert stale_up.guild.level == 2 and stale_up.guild.funds == 10000

        # asset_op_enabled=False 时捐献与商店购买均回 kGuildAssetPending
        d_off = await h.call_guild(
            "DonateToGuild", "guildpb.DonateToGuildRequest", "guildpb.DonateToGuildResponse", player_id=101, donate_id=1
        )
        b_off = await h.call_guild(
            "BuyGuildShopGoods",
            "guildpb.BuyGuildShopGoodsRequest",
            "guildpb.BuyGuildShopGoodsResponse",
            player_id=101,
            goods_id=101,
            count=1,
        )
        assert d_off.error_message.id == ERR_GUILD_ASSET_PENDING
        assert b_off.error_message.id == ERR_GUILD_ASSET_PENDING

        # 活动视图 + 历练未开放 + 元宵灯会（3 人点灯达阈值，只发一次资金）
        for pid in (102, 103):
            await h.call_guild(
                "ApplyJoinGuild", "guildpb.ApplyJoinGuildRequest", "guildpb.ApplyJoinGuildResponse", player_id=pid, guild_id=gid
            )
            await h.call_guild(
                "ReviewGuildApplication",
                "guildpb.ReviewGuildApplicationRequest",
                "guildpb.ReviewGuildApplicationResponse",
                player_id=101,
                applicant_player_id=pid,
                approve=True,
            )

        acts = await h.call_guild(
            "GetGuildActivities", "guildpb.GetGuildActivitiesRequest", "guildpb.GetGuildActivitiesResponse", player_id=101
        )
        assert acts.error_message.id == ERR_OK
        trial_view = next(a for a in acts.activities if a.type == ACTIVITY_TYPE_TRIAL)
        assert trial_view.state == ACTIVITY_STATE_DISABLED and trial_view.blocked_tip_id == ERR_GUILD_ACTIVITY_NOT_OPEN

        start_t = await h.call_guild(
            "StartGuildTrial", "guildpb.StartGuildTrialRequest", "guildpb.StartGuildTrialResponse", player_id=101, activity_id=3
        )
        resp_t = await h.call_guild(
            "RespondGuildTrialInvite",
            "guildpb.RespondGuildTrialInviteRequest",
            "guildpb.RespondGuildTrialInviteResponse",
            player_id=101,
            lobby_id=1,
            accept=True,
        )
        assert start_t.error_message.id == ERR_GUILD_ACTIVITY_NOT_OPEN
        assert resp_t.error_message.id == ERR_GUILD_ACTIVITY_NOT_OPEN

        # 元宵灯会（activity_id=1，无物品奖励，阈值 3，资金 500）
        l1 = await h.call_guild(
            "LightGuildLantern", "guildpb.LightGuildLanternRequest", "guildpb.LightGuildLanternResponse", player_id=101, activity_id=1
        )
        assert l1.error_message.id == ERR_OK and l1.activity.progress == 1 and not l1.activity.threshold_reached
        # 101 当天再点被拒
        l1_dup = await h.call_guild(
            "LightGuildLantern", "guildpb.LightGuildLanternRequest", "guildpb.LightGuildLanternResponse", player_id=101, activity_id=1
        )
        assert l1_dup.error_message.id == ERR_GUILD_ACTIVITY_ALREADY_CLAIMED

        await h.call_guild(
            "LightGuildLantern", "guildpb.LightGuildLanternRequest", "guildpb.LightGuildLanternResponse", player_id=102, activity_id=1
        )
        l3 = await h.call_guild(
            "LightGuildLantern", "guildpb.LightGuildLanternRequest", "guildpb.LightGuildLanternResponse", player_id=103, activity_id=1
        )
        assert l3.error_message.id == ERR_OK and l3.activity.progress == 3
        assert l3.activity.threshold_reached and l3.activity.funds_granted
        after_g = await h.store.get_guild(gid)
        assert after_g is not None and after_g.funds == 10000 + 500

    # 再测 asset_op_enabled=True 时的捐献/商店入队、幂等键与中秋团圆领奖（带物品奖励）
    async with GuildTestHarness(asset_op_enabled=True) as h:
        h.register_player(101, zone_id=1, online=True)
        h.register_player(102, zone_id=1, online=True)
        h.register_player(103, zone_id=1, online=False)

        gid = (
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=101, name="团圆帮"
            )
        ).guild.guild_id
        for pid in (102, 103):
            await h.call_guild(
                "ApplyJoinGuild", "guildpb.ApplyJoinGuildRequest", "guildpb.ApplyJoinGuildResponse", player_id=pid, guild_id=gid
            )
            await h.call_guild(
                "ReviewGuildApplication",
                "guildpb.ReviewGuildApplicationRequest",
                "guildpb.ReviewGuildApplicationResponse",
                player_id=101,
                applicant_player_id=pid,
                approve=True,
            )

        # 中秋团圆（activity_id=2，阈值 3）：当前仅 2 人在线 -> kGuildActivityThresholdNotReached
        reunion_fail = await h.call_guild(
            "ClaimGuildReunion", "guildpb.ClaimGuildReunionRequest", "guildpb.ClaimGuildReunionResponse", player_id=101, activity_id=2
        )
        assert reunion_fail.error_message.id == ERR_GUILD_ACTIVITY_THRESHOLD_NOT_REACHED
        assert list(reunion_fail.error_message.parameters) == ["2", "3"]

        # 第 3 人上线后领奖成功并锁存，之后 103 下线，102 仍可领取
        h.online.online_ids.add(103)
        r1 = await h.call_guild(
            "ClaimGuildReunion", "guildpb.ClaimGuildReunionRequest", "guildpb.ClaimGuildReunionResponse", player_id=101, activity_id=2
        )
        assert r1.error_message.id == ERR_OK and r1.activity.threshold_reached and r1.activity.my_pending_reward_count == 1
        h.online.online_ids.discard(103)
        r2 = await h.call_guild(
            "ClaimGuildReunion", "guildpb.ClaimGuildReunionRequest", "guildpb.ClaimGuildReunionResponse", player_id=102, activity_id=2
        )
        assert r2.error_message.id == ERR_OK and r2.activity.threshold_reached

        # 捐献 + client_request_id 幂等重放 + 参数不符拒绝
        d1 = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            player_id=101,
            donate_id=1,
            client_request_id=777,
        )
        assert d1.error_message.id == ERR_OK and d1.donation.op_id > 0
        d1_replay = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            player_id=101,
            donate_id=1,
            client_request_id=777,
        )
        assert d1_replay.error_message.id == ERR_OK and d1_replay.donation.op_id == d1.donation.op_id
        d1_mismatch = await h.call_guild(
            "DonateToGuild",
            "guildpb.DonateToGuildRequest",
            "guildpb.DonateToGuildResponse",
            player_id=101,
            donate_id=2,
            client_request_id=777,
        )
        assert d1_mismatch.error_message.id == ERR_INVALID_PARAMETER

        # 商店查询与购买
        h.store.set_member_contribution_for_test(gid, 101, 500, 500)
        shop = await h.call_guild("GetGuildShop", "guildpb.GetGuildShopRequest", "guildpb.GetGuildShopResponse", player_id=101)
        assert shop.error_message.id == ERR_OK and shop.contribution_balance == 500 and len(shop.goods) > 0
        buy = await h.call_guild(
            "BuyGuildShopGoods",
            "guildpb.BuyGuildShopGoodsRequest",
            "guildpb.BuyGuildShopGoodsResponse",
            player_id=101,
            goods_id=101,
            count=2,
            client_request_id=888,
        )
        assert buy.error_message.id == ERR_OK and buy.order.op_id > 0 and buy.contribution_balance == 500 - 60


# -- 5. GuildInternal.ListAppliedAssetOpsSince 校验与保留期截止 ---------------------------------


@_async_test
async def test_guild_internal_list_applied_asset_ops_since():
    async with GuildTestHarness(terminal_retention_days=30) as h:
        now = h.clock()
        for op_id, ts in ((10, now - 5000), (20, now - 3000), (30, now - 1000)):
            h.store.seed_asset_op_for_test(
                AssetOpRow(
                    op_id=op_id,
                    player_id=101,
                    stream=ASSET_STREAM_GUILD_DEBIT,
                    seq=op_id,
                    guild_id=1001,
                    kind=OP_KIND_DONATE,
                    status=OP_STATUS_APPLIED,
                    durable=1,
                    attempts=1,
                    next_attempt_ms=ts,
                    deadline_ms=0,
                    payload=b"",
                    ref_id=1,
                    ref_count=1,
                    period_key=20250601,
                    contribution_delta=10,
                    funds_delta=100,
                    reason_tip_id=0,
                    created_ms=ts - 100,
                    updated_ms=ts,
                    lease_until_ms=0,
                    lease_token=0,
                    tx_type=24,
                    last_outcome=1,
                    last_reason=0,
                    stream_epoch=1,
                )
            )

        # 带客户端会话 metadata 必回 PERMISSION_DENIED
        md = h._gate_md(101, f"/{SERVICE_GUILD_INTERNAL}/ListAppliedAssetOpsSince")
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await h.call_internal(
                "ListAppliedAssetOpsSince",
                "guildpb.ListAppliedAssetOpsSinceRequest",
                "guildpb.ListAppliedAssetOpsSinceResponse",
                md=md,
                player_ids=[101],
                since_ms=now - 10_000,
            )
        assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 非法入参：空 / 含 0 / 重复 / limit > 500 / since_ms == 0
        for kwargs in (
            {"player_ids": [], "since_ms": now - 1000},
            {"player_ids": [0], "since_ms": now - 1000},
            {"player_ids": [101, 101], "since_ms": now - 1000},
            {"player_ids": [101], "since_ms": 0},
            {"player_ids": [101], "since_ms": now - 1000, "limit": 501},
        ):
            with pytest.raises(grpc.aio.AioRpcError) as bad:
                await h.call_internal(
                    "ListAppliedAssetOpsSince",
                    "guildpb.ListAppliedAssetOpsSinceRequest",
                    "guildpb.ListAppliedAssetOpsSinceResponse",
                    **kwargs,
                )
            assert bad.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        # 早于保留期截止：FAILED_PRECONDITION 且带精确前缀
        too_old = now - 31 * MS_PER_DAY
        with pytest.raises(grpc.aio.AioRpcError) as ret_exc:
            await h.call_internal(
                "ListAppliedAssetOpsSince",
                "guildpb.ListAppliedAssetOpsSinceRequest",
                "guildpb.ListAppliedAssetOpsSinceResponse",
                player_ids=[101],
                since_ms=too_old,
            )
        assert ret_exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert ret_exc.value.details().startswith(RETENTION_REJECTED_PREFIX)

        # 清理水位抬高后，cutoff_ms 随之抬高
        h.rank_store.cleanup_watermark_ms = now - 5 * MS_PER_DAY
        with pytest.raises(grpc.aio.AioRpcError) as wm_exc:
            await h.call_internal(
                "ListAppliedAssetOpsSince",
                "guildpb.ListAppliedAssetOpsSinceRequest",
                "guildpb.ListAppliedAssetOpsSinceResponse",
                player_ids=[101],
                since_ms=now - 6 * MS_PER_DAY,
            )
        assert wm_exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert wm_exc.value.details() == f"{RETENTION_REJECTED_PREFIX}{h.rank_store.cleanup_watermark_ms + 3_600_000}"

        # 正常游标分页
        h.rank_store.cleanup_watermark_ms = 0
        p1 = await h.call_internal(
            "ListAppliedAssetOpsSince",
            "guildpb.ListAppliedAssetOpsSinceRequest",
            "guildpb.ListAppliedAssetOpsSinceResponse",
            player_ids=[101],
            since_ms=now - 10_000,
            limit=2,
        )
        assert [o.op_id for o in p1.ops] == [10, 20] and p1.next_after_op_id == 20
        p2 = await h.call_internal(
            "ListAppliedAssetOpsSince",
            "guildpb.ListAppliedAssetOpsSinceRequest",
            "guildpb.ListAppliedAssetOpsSinceResponse",
            player_ids=[101],
            since_ms=now - 10_000,
            after_op_id=20,
            limit=2,
        )
        assert [o.op_id for o in p2.ops] == [30] and p2.next_after_op_id == 0


# -- 6. RoleFenceParticipant（过户阻塞 / 删角试探挡帮主 / 真删角自动让位与单人解散） ------------


@_async_test
async def test_role_fence_participant_freeze_handover_and_solo_disband():
    async with GuildTestHarness() as h:
        for pid in (101, 102, 103, 104):
            h.register_player(pid, zone_id=1)

        gid = (
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=101, name="护栏帮"
            )
        ).guild.guild_id
        for pid in (102, 103):
            h.clock.advance(1000)
            await h.call_guild(
                "ApplyJoinGuild", "guildpb.ApplyJoinGuildRequest", "guildpb.ApplyJoinGuildResponse", player_id=pid, guild_id=gid
            )
            await h.call_guild(
                "ReviewGuildApplication",
                "guildpb.ReviewGuildApplicationRequest",
                "guildpb.ReviewGuildApplicationResponse",
                player_id=101,
                applicant_player_id=pid,
                approve=True,
            )
        # 102 升长老（但帮贡 10），103 普通成员（帮贡 999）：顺位应选职位更高的 102
        await h.call_guild(
            "SetGuildMemberRole",
            "guildpb.SetGuildMemberRoleRequest",
            "guildpb.SetGuildMemberRoleResponse",
            player_id=101,
            target_player_id=102,
            role=ROLE_OFFICER,
        )
        h.store.set_member_contribution_for_test(gid, 102, 10, 10)
        h.store.set_member_contribution_for_test(gid, 103, 999, 999)

        # 过户冻结（非 removal_token）：在帮即阻塞 "guild_member"
        transfer_state = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=101,
            account="acc_101",
            owner_revision=1,
            freeze_token=12345,
            version=2,
            home_zone=1,
        )
        prep_transfer = await h.call_role_fence(
            "PrepareRoleFreeze",
            "rolefence.PrepareRoleFreezeRequest",
            "rolefence.PrepareRoleFreezeResponse",
            state=transfer_state,
            dry_run=False,
        )
        assert not prep_transfer.clean and prep_transfer.blocker == ROLE_BLOCKER_GUILD_MEMBER

        # 删角试探（dry_run=True + removal_token）：帮主阻塞 "guild_leader"
        del_state = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=101,
            account="acc_101",
            owner_revision=1,
            freeze_token=removal_token(101),
            version=2,
            home_zone=1,
        )
        prep_dry = await h.call_role_fence(
            "PrepareRoleFreeze",
            "rolefence.PrepareRoleFreezeRequest",
            "rolefence.PrepareRoleFreezeResponse",
            state=del_state,
            dry_run=True,
        )
        assert not prep_dry.clean and prep_dry.blocker == ROLE_BLOCKER_GUILD_LEADER

        # 真正删角冻结（dry_run=False + removal_token）：放行并冻结本地副本
        prep_real = await h.call_role_fence(
            "PrepareRoleFreeze",
            "rolefence.PrepareRoleFreezeRequest",
            "rolefence.PrepareRoleFreezeResponse",
            state=del_state,
            dry_run=False,
        )
        assert prep_real.clean and not prep_real.blocker

        # 冻结态下：读仍放行，写返回 kGuildBusyRetry（同步到目录状态以防自愈解除冻结）
        h.roles.states[101] = del_state
        read_frozen = await h.call_guild(
            "GetPlayerGuild", "guildpb.GetPlayerGuildRequest", "guildpb.GetPlayerGuildResponse", player_id=101
        )
        assert read_frozen.error_message.id == ERR_OK
        write_frozen = await h.call_guild(
            "SetAnnouncement",
            "guildpb.SetAnnouncementRequest",
            "guildpb.SetAnnouncementResponse",
            player_id=101,
            guild_id=gid,
            announcement="frozen",
        )
        assert write_frozen.error_message.id == ERR_GUILD_BUSY_RETRY

        # PurgeRemovedRole：墓碑态自动把帮主交给 102（长老）
        tomb_101 = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=101,
            account="",
            owner_revision=1,
            freeze_token=removal_token(101),
            freeze_final=True,
            version=3,
            home_zone=1,
        )
        purge_101 = await h.call_role_fence(
            "PurgeRemovedRole",
            "rolefence.PurgeRemovedRoleRequest",
            "rolefence.PurgeRemovedRoleResponse",
            state=tomb_101,
        )
        assert not purge_101.retry
        after_handover = await h.store.get_guild(gid)
        assert after_handover is not None and after_handover.leader_id == 102
        assert after_handover.member(101) is None and after_handover.member(102).role == ROLE_LEADER

        # 单人帮会删角收尾：自动解散并摘榜立墓碑
        solo_gid = (
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=104, name="独行帮"
            )
        ).guild.guild_id
        tomb_104 = PROTOCOL.message(
            "rolefence.RoleFenceState",
            player_id=104,
            account="",
            owner_revision=1,
            freeze_token=removal_token(104),
            freeze_final=True,
            version=3,
            home_zone=1,
        )
        purge_104 = await h.call_role_fence(
            "PurgeRemovedRole",
            "rolefence.PurgeRemovedRoleRequest",
            "rolefence.PurgeRemovedRoleResponse",
            state=tomb_104,
        )
        assert not purge_104.retry
        assert await h.store.get_guild(solo_gid) is None
        assert await h.rank_store.upsert_score(solo_gid, 1, 50, 5) == RANK_WRITE_DISBANDED


# -- 7. Gate Ed25519 / OwnershipInterceptor / HMAC / 推占位鉴权 ---------------------------------


@_async_test
async def test_auth_gate_ownership_and_hmac():
    async with GuildTestHarness() as h:
        h.register_player(101, zone_id=1)

        # 客户端 RPC 缺会话被拒
        with pytest.raises(grpc.aio.AioRpcError) as no_md:
            await h.call_guild(
                "CreateGuild", "guildpb.CreateGuildRequest", "guildpb.CreateGuildResponse", player_id=None, name="无会话"
            )
        assert no_md.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # 非客户端 RPC（UpdateGuildScore / NotifyGuildChanged）带会话被拒
        with pytest.raises(grpc.aio.AioRpcError) as score_md:
            await h.call_guild(
                "UpdateGuildScore",
                "guildpb.UpdateGuildScoreRequest",
                "guildpb.UpdateGuildScoreResponse",
                player_id=101,
                guild_id=1,
                score=10,
            )
        assert score_md.value.code() == grpc.StatusCode.PERMISSION_DENIED

        with pytest.raises(grpc.aio.AioRpcError) as notify_md:
            await h.call_guild(
                "NotifyGuildChanged", "guildpb.GuildChangedS2C", "Empty", player_id=101, guild_id=1
            )
        assert notify_md.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # NotifyGuildChanged 无会话返回 Empty
        empty = await h.call_guild("NotifyGuildChanged", "guildpb.GuildChangedS2C", "Empty", player_id=None, guild_id=1)
        assert empty is not None

        # OwnershipInterceptor：过户后旧会话被 PERMISSION_DENIED 拒绝
        h.roles.register(101, account="new_owner", owner_revision=2, version=5, home_zone=1)
        with pytest.raises(grpc.aio.AioRpcError) as owned_exc:
            await h.call_guild(
                "GetGuild",
                "guildpb.GetGuildRequest",
                "guildpb.GetGuildResponse",
                player_id=101,
                account="acc_101",
                owner_revision=1,
                guild_id=1,
            )
        assert owned_exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

        # RoleFenceParticipant 错误密钥被拒
        st = PROTOCOL.message(
            "rolefence.RoleFenceState", player_id=101, account="new_owner", owner_revision=2, version=6, home_zone=1
        )
        with pytest.raises(grpc.aio.AioRpcError) as hmac_exc:
            await h.call_role_fence(
                "ApplyRoleFence",
                "rolefence.ApplyRoleFenceRequest",
                "rolefence.ApplyRoleFenceResponse",
                secret=b"wrong-coordinator-secret-01234567890",
                state=st,
            )
        assert hmac_exc.value.code() == grpc.StatusCode.PERMISSION_DENIED


# -- 8. 配置校验、权限纯函数与 SqlGuildStore DDL/绑定校验 ---------------------------------------


def test_config_and_domain_invariants():
    assert DEFAULT_GRPC_PORT == 50310
    assert GUILD_NODE_TYPE == 15
    assert role_rank(ROLE_MEMBER) == 1
    assert role_rank(ROLE_OFFICER) == 2
    assert role_rank(ROLE_LEADER) == 3
    assert role_rank(2) == 0  # 空号 2 fail-closed
    assert role_rank(99) == 0
    assert guild_name_norm(" ＡＢＣ ") == "abc"

    cfg = GuildConfig(
        advertise_host="127.0.0.1",
        data_service="127.0.0.1:50051",
        content_guard_optional=True,
        coordinator_secret=SECRET_COORDINATOR,
        role_fence_secret=SECRET_ROLE_FENCE_GUILD,
    )
    cfg.validate()

    # 资产通道开启但缺少密钥时 fail-closed 拒启
    with pytest.raises(ValueError):
        GuildConfig(
            advertise_host="127.0.0.1",
            data_service="127.0.0.1:50051",
            content_guard_optional=True,
            asset_op_enabled=True,
            coordinator_secret=SECRET_COORDINATOR,
            role_fence_secret=SECRET_ROLE_FENCE_GUILD,
        ).validate()

    # 检查环境变量名常量
    assert COORDINATOR_SECRET_ENV == "MMORPG_ROLE_FENCE_SECRET_COORDINATOR"
    assert ROLE_FENCE_SECRET_ENV == "MMORPG_ROLE_FENCE_SECRET_GUILD"


@_async_test
async def test_sql_guild_store_ddl_and_bindings():
    statements = load_schema_statements()
    joined = "\n".join(statements)
    for tbl in REQUIRED_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS `{tbl}`" in joined or f"CREATE TABLE IF NOT EXISTS {tbl}" in joined
    assert "UNIQUE KEY `uk_guild` (`name_norm`)" in joined
    assert "UNIQUE KEY `uk_guild_name_hold` (`name_norm`)" in joined
    assert "UNIQUE KEY `uk_guild_member` (`player_id`)" in joined
    assert "UNIQUE KEY `uk_guild_asset_op` (`player_id`,`stream`,`stream_epoch`,`seq`)" in joined
    assert "score_version" in joined and "renamed_ms" in joined

    # 验证 FORCE INDEX (PRIMARY) 与 UNHEX(%s) 安全绑定
    assert "FORCE INDEX (PRIMARY)" in SQL_LOCK_MEMBER
    assert "FORCE INDEX (PRIMARY)" in SQL_UPDATE_MEMBER_ROLE
    assert "FORCE INDEX (PRIMARY)" in SQL_SET_MEMBER_CONTRIBUTION
    assert "FORCE INDEX (PRIMARY)" in SQL_DEBIT_MEMBER_CONTRIBUTION
    assert "UNHEX(%s)" in SQL_APPLY_ROLE_FENCE
    assert "UNHEX(%s)" in SQL_INSERT_ASSET_OP

    # 使用假游标验证 _SqlTx 参数绑定（role_fence.account / guild_asset_op.payload 走 hex 字符串）
    executed: list[tuple[str, tuple]] = []

    class FakeCursor:
        rowcount = 1

        async def execute(self, sql, params=()):
            executed.append((sql, tuple(params)))

        async def fetchone(self):
            return None

        async def fetchall(self):
            return []

    tx = _SqlTx(FakeCursor())
    await tx.apply_role_fence(
        RoleFenceRow(
            player_id=101,
            account="wx_abc",
            owner_revision=2,
            freeze_token=0,
            freeze_final=False,
            version=3,
            home_zone=1,
        ),
        1_750_000_000_000,
    )
    assert executed[-1][1][1] == b"wx_abc".hex()

    await tx.insert_asset_op(
        AssetOpRow(
            op_id=99,
            player_id=101,
            stream=1,
            seq=1,
            guild_id=10,
            kind=1,
            status=1,
            durable=0,
            attempts=0,
            next_attempt_ms=2000,
            deadline_ms=3000,
            payload=b"\x00\xff\xab",
            ref_id=1,
            ref_count=1,
            period_key=20250601,
            contribution_delta=10,
            funds_delta=100,
            reason_tip_id=0,
            created_ms=1000,
            updated_ms=1000,
            lease_until_ms=2000,
            lease_token=7,
            tx_type=24,
            last_outcome=0,
            last_reason=0,
            stream_epoch=1000,
            last_send_ms=1000,
        )
    )
    assert executed[-1][1][11] == "00ffab"
    assert isinstance(SqlGuildStore, type)
