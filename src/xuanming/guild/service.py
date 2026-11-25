"""guild 三组 gRPC 服务适配层与 fail-closed 鉴权。

对外暴露：
- ``guildpb.GuildService``（全部 29 个 RPC）
- ``guildpb.GuildInternal``（ListAppliedAssetOpsSince）
- ``rolefence.RoleFenceParticipant``（PrepareRoleFreeze / ApplyRoleFence / PurgeRemovedRole）
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable, Sequence

import grpc
from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import verify_service_call
from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .asset_loop import GuildAssetOpLoop
from .errors import (
    ERR_ASSET_CURRENCY_INSUFFICIENT,
    ERR_GUILD_ACTIVITY_NOT_OPEN,
    ERR_GUILD_ANNOUNCEMENT_SENSITIVE,
    ERR_GUILD_ANNOUNCEMENT_TOO_LONG,
    ERR_GUILD_APPLICATION_NOT_FOUND,
    ERR_GUILD_ASSET_PENDING,
    ERR_GUILD_ASSET_REJECTED,
    ERR_GUILD_CANNOT_TARGET_SELF,
    ERR_GUILD_CURRENCY_INSUFFICIENT,
    ERR_GUILD_HOME_ZONE_UNKNOWN,
    ERR_GUILD_ID_GEN_UNAVAILABLE,
    ERR_GUILD_NAME_INVALID,
    ERR_GUILD_NAME_SENSITIVE,
    ERR_GUILD_NOT_FOUND,
    ERR_GUILD_NOT_IN_GUILD,
    ERR_GUILD_NOT_LEADER,
    ERR_GUILD_NOT_RANKED,
    ERR_GUILD_NO_PERMISSION,
    ERR_GUILD_RENAME_COOLDOWN,
    ERR_GUILD_SHOP_GOODS_NOT_FOUND,
    ERR_GUILD_SHOP_LEVEL_TOO_LOW,
    ERR_GUILD_SHOP_LIMIT,
    ERR_GUILD_TARGET_NOT_MEMBER,
    ERR_GUILD_TEXT_CHECK_UNAVAILABLE,
    ERR_INVALID_PARAMETER,
    ERR_RATE_LIMIT_EXCEEDED,
    GuildActivityNotOpenError,
    GuildClientRequestReplayedError,
    GuildError,
    GuildNotFoundError,
    GuildNotInGuildError,
    GuildRankTooLowError,
    GuildRenameCooldownError,
    GuildTextCheckUnavailableError,
    GuildZoneMismatchError,
    InvalidRoleFenceStateError,
    StoreUnavailableError,
    tip_for_error,
)
from .model import (
    ACTIVITY_STATE_DISABLED,
    ACTIVITY_STATE_OPEN,
    ACTIVITY_TYPE_LANTERN,
    ACTIVITY_TYPE_REUNION,
    ACTIVITY_TYPE_TRIAL,
    ASSET_OP_MAX_PENDING,
    ASSET_STREAM_GUILD_CREDIT,
    ASSET_STREAM_GUILD_DEBIT,
    ActivityProgress,
    ActivityRewardStatus,
    ActivityRuleRow,
    AssetOpRow,
    COUNTER_KIND_ACTIVITY,
    COUNTER_KIND_DONATE,
    COUNTER_KIND_SHOP,
    DEFAULT_RANK_PAGE_SIZE,
    FinalizedAssetOp,
    GuildData,
    MAX_APPLIED_OPS_PAGE_LIMIT,
    MAX_APPLIED_OPS_PLAYER_IDS,
    MAX_RANK_PAGE_SIZE,
    MY_APPLICATIONS_LIMIT,
    OP_KIND_DONATE,
    OP_KIND_SHOP,
    OP_STATUS_APPLIED,
    ORDER_STATUS_REJECTED,
    RANK_OFFICER,
    RECENT_RESULT_LIMIT,
    RECENT_RESULT_WINDOW_MS,
    RECENT_SCAN_LIMIT,
    RETENTION_REJECTED_PREFIX,
    ROLE_OFFICER,
    RoleOriginal,
    TEXT_SCENE_GUILD_ANNOUNCEMENT,
    TEXT_SCENE_GUILD_NAME,
    TEXT_VERDICT_PASS,
    activity_blocked,
    activity_state_of,
    announcement_valid,
    day_key,
    guild_period_key,
    is_assignable_role,
    is_terminal_op_status,
    joined_long_enough,
    max_shop_buy_count,
    next_daily_reset_ms,
    next_weekly_reset_ms,
    normalize_guild_name,
    order_status_view,
    period_key,
    pick_activity_for_write,
    provable_cutoff_ms,
    reunion_observed,
    reunion_threshold,
    role_rank,
    select_visible_activities,
    week_key,
)
from .rank_store import GuildRankStore, MemoryGuildRankStore
from .store import (
    ContentGuardChecker,
    GuildNotifier,
    GuildStoreCore,
    HomeZoneDirectory,
    NullContentGuardChecker,
    NullGuildNotifier,
    NullOnlineStatusResolver,
    NullPlayerNameResolver,
    OnlineStatusResolver,
    PlayerNameResolver,
)
from .trials import GuildTrialCoordinator
from .war import GuildWarManager

logger = logging.getLogger(__name__)

SERVICE_GUILD = "guildpb.GuildService"
SERVICE_GUILD_INTERNAL = "guildpb.GuildInternal"
SERVICE_ROLE_FENCE = "rolefence.RoleFenceParticipant"

CODE = grpc.StatusCode

# GuildChangeKind 推送类型常量
CHANGE_MEMBER_JOINED = 1
CHANGE_MEMBER_LEFT = 2
CHANGE_MEMBER_KICKED = 3
CHANGE_ROLE_CHANGED = 4
CHANGE_LEADER_TRANSFERRED = 5
CHANGE_DISBANDED = 6
CHANGE_APPLICATION_RECEIVED = 7
CHANGE_APPLICATION_REJECTED = 8
CHANGE_FUNDS_CHANGED = 9
CHANGE_LEVEL_UP = 10
CHANGE_ANNOUNCEMENT_CHANGED = 11
CHANGE_ACTIVITY_CHANGED = 12
CHANGE_DELIVERY_DONE = 13
CHANGE_RENAMED = 14
CHANGE_WAR_UPDATED = 15


def _has_session_metadata(context) -> bool:
    if context is None:
        return False
    for key, _ in context.invocation_metadata() or ():
        if key.lower() == SESSION_DETAIL:
            return True
    return False


class _SlidingQuota:
    """每玩家 60 秒滑动窗口申请配额（与 Go ApplicationQuotaPerMinute 口径一致）。"""

    def __init__(self, limit_per_minute: int) -> None:
        self._limit = max(1, int(limit_per_minute))
        self._hits: dict[int, deque[int]] = {}

    def allow(self, player_id: int, now_ms: int) -> bool:
        bucket = self._hits.setdefault(player_id, deque())
        cutoff = now_ms - 60_000
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()
        if len(bucket) >= self._limit:
            return False
        bucket.append(now_ms)
        return True


class GuildService:
    """集中挂载 GuildService(29 RPC) / GuildInternal / RoleFenceParticipant 三个 gRPC 服务。"""

    def __init__(
        self,
        protocol: Protocol,
        store: GuildStoreCore,
        *,
        rank_store: GuildRankStore | None = None,
        caller_verifier: CallerVerifier | None = None,
        coordinator_secret: bytes = b"",
        home_zones: HomeZoneDirectory | None = None,
        player_names: PlayerNameResolver | None = None,
        online_resolver: OnlineStatusResolver | None = None,
        content_guard: ContentGuardChecker | None = None,
        notifier: GuildNotifier | None = None,
        asset_loop: GuildAssetOpLoop | None = None,
        trials: GuildTrialCoordinator | None = None,
        war: GuildWarManager | None = None,
        asset_op_enabled: bool = False,
        asset_op_lease_ms: int = 10_000,
        terminal_retention_days: int = 30,
        trials_enabled: bool = False,
        application_quota_per_minute: int = 10,
        request_budget_sec: float = 3.5,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.rank_store: GuildRankStore = rank_store or MemoryGuildRankStore(
            merged_zones=store.merged_zones, clock_ms=clock_ms
        )
        self._caller_verifier = caller_verifier
        self._coordinator_secret = coordinator_secret
        self._home_zones = home_zones
        self._player_names: PlayerNameResolver = player_names or NullPlayerNameResolver()
        self._online: OnlineStatusResolver = online_resolver or NullOnlineStatusResolver()
        self._content_guard: ContentGuardChecker = content_guard or NullContentGuardChecker()
        self._notifier: GuildNotifier = notifier or NullGuildNotifier()
        self._asset_loop = asset_loop
        self._trials = trials
        self._asset_op_enabled = bool(asset_op_enabled)
        self._asset_op_lease_ms = int(asset_op_lease_ms)
        self._terminal_retention_days = int(terminal_retention_days)
        self._trials_enabled = bool(trials_enabled)
        self._quota = _SlidingQuota(application_quota_per_minute)
        self._budget_sec = float(request_budget_sec)
        self._clock_ms = clock_ms
        self.war: GuildWarManager = war if war is not None else GuildWarManager(
            protocol,
            store,
            player_names=self._player_names,
            online_resolver=self._online,
            clock_ms=clock_ms,
        )
        if self.store.on_asset_finalized is None:
            self.store.on_asset_finalized = self._on_asset_finalized

    async def _on_asset_finalized(self, ev: FinalizedAssetOp) -> None:
        await self._notify_best_effort(CHANGE_DELIVERY_DONE, ev.guild_id, ev.player_id, 0, [ev.player_id])
        if ev.kind == OP_KIND_DONATE and ev.status == OP_STATUS_APPLIED and ev.guild_id != 0:
            guild = await self.store.get_guild(ev.guild_id)
            if guild is not None:
                others = [m.player_id for m in guild.members if m.player_id != ev.player_id]
                if others:
                    await self._notify_best_effort(CHANGE_FUNDS_CHANGED, ev.guild_id, ev.player_id, 0, others)

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_GUILD: {
                "CreateGuild": self.create_guild,
                "GetGuild": self.get_guild,
                "GetPlayerGuild": self.get_player_guild,
                "LeaveGuild": self.leave_guild,
                "DisbandGuild": self.disband_guild,
                "SetAnnouncement": self.set_announcement,
                "RenameGuild": self.rename_guild,
                "SetGuildMemberRole": self.set_guild_member_role,
                "KickGuildMember": self.kick_guild_member,
                "TransferGuildLeader": self.transfer_guild_leader,
                "ApplyJoinGuild": self.apply_join_guild,
                "CancelGuildApplication": self.cancel_guild_application,
                "ListMyGuildApplications": self.list_my_guild_applications,
                "ListGuildApplications": self.list_guild_applications,
                "ReviewGuildApplication": self.review_guild_application,
                "NotifyGuildChanged": self.notify_guild_changed,
                "UpdateGuildScore": self.update_guild_score,
                "GetGuildRank": self.get_guild_rank,
                "GetGuildRankByGuild": self.get_guild_rank_by_guild,
                "GetGuildDonateOptions": self.get_guild_donate_options,
                "DonateToGuild": self.donate_to_guild,
                "UpgradeGuild": self.upgrade_guild,
                "GetGuildShop": self.get_guild_shop,
                "BuyGuildShopGoods": self.buy_guild_shop_goods,
                "GetGuildActivities": self.get_guild_activities,
                "LightGuildLantern": self.light_guild_lantern,
                "ClaimGuildReunion": self.claim_guild_reunion,
                "StartGuildTrial": self.start_guild_trial,
                "RespondGuildTrialInvite": self.respond_guild_trial_invite,
                "GetGuildWarOverview": self.get_guild_war_overview,
                "SetGuildWarStance": self.set_guild_war_stance,
                "RegisterGuildWarGarrison": self.register_guild_war_garrison,
                "StartGuildWarBattle": self.start_guild_war_battle,
                "ClaimGuildWarMilestone": self.claim_guild_war_milestone,
                "ClaimGuildWarDividend": self.claim_guild_war_dividend,
                "PostGuildWarBounty": self.post_guild_war_bounty,
                "TrackGuildWarBounty": self.track_guild_war_bounty,
                "GetGuildWarBattleResult": self.get_guild_war_battle_result,
                "NotifyGuildWarAlert": self.notify_guild_war_alert,
                "InspectFieldWarTargets": self.inspect_field_war_targets,
            },
            SERVICE_GUILD_INTERNAL: {
                "ListAppliedAssetOpsSince": self.list_applied_asset_ops_since,
            },
            SERVICE_ROLE_FENCE: {
                "PrepareRoleFreeze": self.prepare_role_freeze,
                "ApplyRoleFence": self.apply_role_fence,
                "PurgeRemovedRole": self.purge_removed_role,
            },
        }

    # -- 入口准入与原角色归属拦截（fail-closed） ------------------------------------------------

    async def _admit_client(self, context, path: str) -> RoleOriginal:
        if not _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "client guild RPC requires verified gate session")
        if self._caller_verifier is None:
            await context.abort(CODE.UNAVAILABLE, "gate caller verifier not configured")
        try:
            raw = self._caller_verifier.authenticate(context.invocation_metadata(), path)
            if raw is None:
                await context.abort(CODE.PERMISSION_DENIED, "missing gate session identity")
            details = parse_details(self.protocol, raw)
        except AuthError as exc:
            await context.abort(CODE.UNAUTHENTICATED, f"invalid gate session signature: {exc}")
        if not details.player_id or not details.owner_revision or not details.authenticated_account:
            await context.abort(CODE.PERMISSION_DENIED, "incomplete online role identity in gate session")
        original = RoleOriginal(
            player_id=int(details.player_id),
            account=str(details.authenticated_account),
            owner_revision=int(details.owner_revision),
        )
        verdict = await self.store.verify_role_owner(original)
        if verdict == "reject":
            await context.abort(CODE.PERMISSION_DENIED, "role no longer belongs to this session identity")
        if verdict != "ok":
            await context.abort(CODE.ABORTED, "role ownership not verified, retry")
        return original

    async def _reject_session_metadata(self, context, what: str) -> None:
        if _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, f"client session cannot invoke {what}")

    async def _admit_coordinator(self, context, request: Message, path: str) -> None:
        await self._reject_session_metadata(context, "RoleFenceParticipant")
        if len(self._coordinator_secret) < 32:
            await context.abort(CODE.UNAVAILABLE, "role fence coordinator secret not configured")
        if not verify_service_call(path, request, "coordinator", self._clock_ms(), self._coordinator_secret):
            await context.abort(CODE.PERMISSION_DENIED, "invalid role fence coordinator signature")

    async def _with_budget(self, coro):
        if self._budget_sec > 0:
            async with asyncio.timeout(self._budget_sec):
                return await coro
        return await coro

    def _tip(self, tip_id: int, *params: str) -> Message:
        return self.protocol.message("TipInfoMessage", id=tip_id, parameters=list(params))

    async def _resolve_home_zone(self, player_id: int) -> tuple[int, Message | None]:
        if self._home_zones is None:
            return 0, self._tip(ERR_GUILD_HOME_ZONE_UNKNOWN, "home zone lookup not configured")
        try:
            zone_id = int(await self._home_zones.get_home_zone(player_id))
        except Exception as exc:
            logger.error("guild home zone lookup failed player=%d err=%s", player_id, exc)
            return 0, tip_for_error(self.protocol, StoreUnavailableError(str(exc)))
        if zone_id <= 0:
            return 0, self._tip(ERR_GUILD_HOME_ZONE_UNKNOWN, "home zone unknown")
        return zone_id, None

    async def _check_content(self, scene: int, player_id: int, text: str, sensitive_tip_id: int) -> Message | None:
        if not text:
            return None
        try:
            verdict = await self._content_guard.check_text(scene, player_id, text)
        except GuildTextCheckUnavailableError as exc:
            logger.error("guild content guard unavailable scene=%d player=%d err=%s", scene, player_id, exc)
            return self._tip(ERR_GUILD_TEXT_CHECK_UNAVAILABLE, "text check unavailable")
        if verdict != TEXT_VERDICT_PASS:
            return self._tip(sensitive_tip_id, "text contains sensitive content")
        return None

    async def _notify_best_effort(
        self, kind: int, guild_id: int, actor_id: int, target_id: int, recipients: Sequence[int]
    ) -> None:
        if not recipients:
            return
        try:
            await self._notifier.notify(kind, guild_id, actor_id, target_id, recipients)
        except Exception as exc:
            logger.error("guild notification failed kind=%d guild=%d err=%s", kind, guild_id, exc)

    # -- GuildInfo 与视图装配 -------------------------------------------------------------------

    async def _guild_info_for(self, guild: GuildData, viewer_id: int) -> Message:
        member_ids = [m.player_id for m in guild.members]
        name_ids = list(member_ids)
        if guild.leader_id > 0:
            name_ids.append(guild.leader_id)
        online_map = await self._online.batch_online(member_ids, strict=False)
        names = await self._player_names.batch_get_names(name_ids)

        lvl_cfg = self.store.catalog.levels.get(guild.level)
        max_officers = lvl_cfg.max_officers if lvl_cfg is not None else 0
        upgrade_cost = lvl_cfg.upgrade_cost_funds if lvl_cfg is not None else 0

        pending_count = 0
        if viewer_id > 0:
            viewer_member = guild.member(viewer_id)
            if viewer_member is not None and role_rank(viewer_member.role) >= RANK_OFFICER:
                try:
                    pending_count = await self.store.count_live_applications(guild.guild_id, self._clock_ms())
                except Exception as exc:
                    logger.error("guild count pending applications failed guild=%d err=%s", guild.guild_id, exc)

        officer_count = sum(1 for m in guild.members if m.role == ROLE_OFFICER)
        info = self.protocol.message(
            "guildpb.GuildInfo",
            guild_id=guild.guild_id,
            name=guild.name,
            leader_id=guild.leader_id,
            leader_name=names.get(guild.leader_id, ""),
            level=guild.level,
            announcement=guild.announcement,
            create_time_ms=guild.create_time_ms,
            max_members=guild.max_members,
            zone_id=self.store.merged_zones.final_zone(guild.zone_id),
            funds=guild.funds,
            max_officers=max_officers,
            officer_count=officer_count,
            upgrade_cost_funds=upgrade_cost,
            pending_application_count=pending_count,
            renamed_ms=guild.renamed_ms,
        )
        for m in guild.members:
            info.members.add(
                player_id=m.player_id,
                role=m.role,
                join_time_ms=m.join_time_ms,
                last_active_ms=m.last_active_ms,
                online=bool(online_map.get(m.player_id, False)),
                name=names.get(m.player_id, ""),
                contribution_total=m.contribution_total,
                contribution_balance=m.contribution_balance,
            )
        return info

    async def _current_guild_info(self, player_id: int) -> Message | None:
        try:
            guild = await self.store.get_player_guild(player_id)
        except Exception as exc:
            logger.warning("guild load current snapshot failed player=%d err=%s", player_id, exc)
            return None
        if guild is None:
            return None
        return await self._guild_info_for(guild, player_id)

    async def _bind_expected_guild(self, player_id: int, expected_guild_id: int) -> tuple[int, bool]:
        """返回 ``(guild_id, mismatched)``。"""
        actual = await self.store.get_player_guild_id(player_id)
        if expected_guild_id != 0:
            if actual != expected_guild_id:
                return 0, True
            return expected_guild_id, False
        return actual, False

    def _donation_view(self, op: AssetOpRow) -> Message:
        order_status, reason = order_status_view(op.status, op.last_reason, op.reason_tip_id)
        currency_type = 0
        cost_amount = 0
        if op.payload:
            try:
                bundle = self.protocol.parse("AssetBundle", op.payload)
                if len(bundle.currencies) == 1:
                    currency_type = int(bundle.currencies[0].currency_type)
                    cost_amount = int(bundle.currencies[0].amount)
            except DecodeError:
                pass
        return self.protocol.message(
            "guildpb.GuildDonationView",
            op_id=op.op_id,
            donate_id=op.ref_id,
            status=order_status,
            currency_type=currency_type,
            cost_amount=cost_amount,
            contribution_gain=op.contribution_delta,
            funds_gain=op.funds_delta,
            reason_tip_id=reason,
            created_ms=op.created_ms,
        )

    def _shop_order_view(self, op: AssetOpRow) -> Message:
        order_status, reason = order_status_view(op.status, op.last_reason, op.reason_tip_id)
        return self.protocol.message(
            "guildpb.GuildShopOrderView",
            op_id=op.op_id,
            goods_id=op.ref_id,
            count=op.ref_count,
            status=order_status,
            cost_contribution=op.contribution_delta,
            reason_tip_id=reason,
            created_ms=op.created_ms,
        )

    # -- 核心 CRUD / 信息 RPC（7 个） -----------------------------------------------------------

    async def create_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        parsed = normalize_guild_name(str(request.name))
        if parsed is None:
            return self.protocol.message(
                "guildpb.CreateGuildResponse",
                error_message=self._tip(ERR_GUILD_NAME_INVALID, "invalid guild name"),
            )
        display_name, name_norm = parsed
        zone_id, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.CreateGuildResponse", error_message=tip)

        tip = await self._check_content(TEXT_SCENE_GUILD_NAME, actor.player_id, display_name, ERR_GUILD_NAME_SENSITIVE)
        if tip is not None:
            return self.protocol.message("guildpb.CreateGuildResponse", error_message=tip)

        now_ms = self._clock_ms()
        try:
            guild = await self._with_budget(
                self.store.create_guild(actor, name=display_name, name_norm=name_norm, zone_id=zone_id, now_ms=now_ms)
            )
        except StoreUnavailableError as exc:
            err_tip = (
                self._tip(ERR_GUILD_ID_GEN_UNAVAILABLE, "id generator unavailable")
                if "guild_id" in str(exc)
                else tip_for_error(self.protocol, exc)
            )
            return self.protocol.message("guildpb.CreateGuildResponse", error_message=err_tip)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.CreateGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )

        try:
            await self.rank_store.upsert_score(guild.guild_id, zone_id, 0, 0)
        except Exception as exc:
            logger.error("guild add new guild to rank failed guild=%d zone=%d err=%s", guild.guild_id, zone_id, exc)

        return self.protocol.message(
            "guildpb.CreateGuildResponse", guild=await self._guild_info_for(guild, actor.player_id)
        )

    async def get_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        zone_id, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.GetGuildResponse", error_message=tip)
        try:
            guild = await self._with_budget(self.store.get_guild(int(request.guild_id)))
        except Exception as exc:
            return self.protocol.message("guildpb.GetGuildResponse", error_message=tip_for_error(self.protocol, exc))
        if guild is None or not self.store.merged_zones.same_zone(guild.zone_id, zone_id):
            return self.protocol.message(
                "guildpb.GetGuildResponse", error_message=self._tip(ERR_GUILD_NOT_FOUND, "guild not found")
            )
        return self.protocol.message(
            "guildpb.GetGuildResponse", guild=await self._guild_info_for(guild, actor.player_id)
        )

    async def get_player_guild(self, request: Message, context, path: str) -> Message:
        # 匿名内部只读例外（mail 领附件按 req.player_id 查所在帮会，pending_application_count 恒 0）
        if _has_session_metadata(context):
            actor = await self._admit_client(context, path)
            player_id = actor.player_id
            viewer_id = actor.player_id
        else:
            player_id = int(request.player_id)
            viewer_id = 0
        if player_id <= 0:
            return self.protocol.message(
                "guildpb.GetPlayerGuildResponse", error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild")
            )
        try:
            guild = await self._with_budget(self.store.get_player_guild(player_id))
        except Exception as exc:
            return self.protocol.message(
                "guildpb.GetPlayerGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )
        if guild is None:
            return self.protocol.message(
                "guildpb.GetPlayerGuildResponse", error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild")
            )
        return self.protocol.message(
            "guildpb.GetPlayerGuildResponse", guild=await self._guild_info_for(guild, viewer_id)
        )

    async def leave_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.LeaveGuildResponse", error_message=tip)

        expected = int(request.expected_guild_id)
        try:
            guild_id, mismatched = await self._bind_expected_guild(actor.player_id, expected)
            if mismatched or guild_id == 0:
                return self.protocol.message(
                    "guildpb.LeaveGuildResponse", error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in guild")
                )
            res = await self._with_budget(self.store.leave_guild(actor, guild_id, self._clock_ms()))
        except (GuildNotFoundError, GuildNotInGuildError) as exc:
            if expected != 0:
                return self.protocol.message(
                    "guildpb.LeaveGuildResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                )
            return self.protocol.message(
                "guildpb.LeaveGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.LeaveGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )

        await self.war.on_member_removed(guild_id, actor.player_id)
        await self._notify_best_effort(
            CHANGE_MEMBER_LEFT,
            guild_id,
            actor.player_id,
            actor.player_id,
            [m.player_id for m in res.guild.members if m.player_id != actor.player_id],
        )
        return self.protocol.message("guildpb.LeaveGuildResponse")

    async def disband_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.DisbandGuildResponse", error_message=tip)

        expected = int(request.expected_guild_id)
        try:
            guild_id, mismatched = await self._bind_expected_guild(actor.player_id, expected)
            if mismatched or guild_id == 0:
                return self.protocol.message(
                    "guildpb.DisbandGuildResponse", error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in guild")
                )
            res = await self._with_budget(self.store.disband_guild(actor, guild_id, self._clock_ms()))
        except (GuildNotFoundError, GuildNotInGuildError) as exc:
            if expected != 0:
                return self.protocol.message(
                    "guildpb.DisbandGuildResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                )
            if isinstance(exc, GuildNotInGuildError):
                return self.protocol.message(
                    "guildpb.DisbandGuildResponse", error_message=self._tip(ERR_GUILD_NOT_LEADER, "not guild leader")
                )
            return self.protocol.message(
                "guildpb.DisbandGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )
        except GuildRankTooLowError:
            return self.protocol.message(
                "guildpb.DisbandGuildResponse", error_message=self._tip(ERR_GUILD_NOT_LEADER, "not guild leader")
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.DisbandGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )

        try:
            known_zones = await self.store.list_guild_zones()
            await self.rank_store.bury_guild(guild_id, res.zone_id, known_zones)
        except Exception as exc:
            logger.error("guild bury rank failed guild=%d zone=%d err=%s", guild_id, res.zone_id, exc)

        await self.war.on_guild_disbanded(guild_id)
        await self._notify_best_effort(
            CHANGE_DISBANDED,
            guild_id,
            actor.player_id,
            0,
            [pid for pid in res.member_ids if pid != actor.player_id],
        )
        return self.protocol.message("guildpb.DisbandGuildResponse")

    async def set_announcement(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        text = str(request.announcement)
        if not announcement_valid(text):
            return self.protocol.message(
                "guildpb.SetAnnouncementResponse",
                error_message=self._tip(ERR_GUILD_ANNOUNCEMENT_TOO_LONG, "announcement too long"),
            )
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.SetAnnouncementResponse", error_message=tip)

        tip = await self._check_content(
            TEXT_SCENE_GUILD_ANNOUNCEMENT, actor.player_id, text, ERR_GUILD_ANNOUNCEMENT_SENSITIVE
        )
        if tip is not None:
            return self.protocol.message("guildpb.SetAnnouncementResponse", error_message=tip)

        try:
            snap = await self._with_budget(self.store.set_announcement(actor, int(request.guild_id), text))
        except Exception as exc:
            return self.protocol.message(
                "guildpb.SetAnnouncementResponse", error_message=tip_for_error(self.protocol, exc)
            )

        await self._notify_best_effort(
            CHANGE_ANNOUNCEMENT_CHANGED,
            snap.guild_id,
            actor.player_id,
            0,
            [m.player_id for m in snap.members if m.player_id != actor.player_id],
        )
        return self.protocol.message(
            "guildpb.SetAnnouncementResponse", guild=await self._guild_info_for(snap, actor.player_id)
        )

    async def rename_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        guild_id = int(request.guild_id)
        if guild_id == 0:
            return self.protocol.message(
                "guildpb.RenameGuildResponse", error_message=self._tip(ERR_GUILD_NOT_FOUND, "guild not found")
            )
        parsed = normalize_guild_name(str(request.name))
        if parsed is None:
            return self.protocol.message(
                "guildpb.RenameGuildResponse", error_message=self._tip(ERR_GUILD_NAME_INVALID, "invalid guild name")
            )
        display_name, new_norm = parsed
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.RenameGuildResponse", error_message=tip)

        tip = await self._check_content(TEXT_SCENE_GUILD_NAME, actor.player_id, display_name, ERR_GUILD_NAME_SENSITIVE)
        if tip is not None:
            return self.protocol.message("guildpb.RenameGuildResponse", error_message=tip)

        try:
            snap, next_allowed_ms = await self._with_budget(
                self.store.rename_guild(
                    actor,
                    guild_id=guild_id,
                    new_name=display_name,
                    new_norm=new_norm,
                    now_ms=self._clock_ms(),
                )
            )
        except GuildRenameCooldownError as exc:
            return self.protocol.message(
                "guildpb.RenameGuildResponse",
                error_message=self._tip(ERR_GUILD_RENAME_COOLDOWN, "guild rename cooldown"),
                next_rename_time=exc.next_rename_ms // 1000,
            )
        except GuildRankTooLowError:
            return self.protocol.message(
                "guildpb.RenameGuildResponse",
                error_message=self._tip(ERR_GUILD_NOT_LEADER, "only leader can rename guild"),
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.RenameGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )

        await self._notify_best_effort(
            CHANGE_RENAMED,
            snap.guild_id,
            actor.player_id,
            0,
            [m.player_id for m in snap.members if m.player_id != actor.player_id],
        )
        return self.protocol.message(
            "guildpb.RenameGuildResponse",
            guild=await self._guild_info_for(snap, actor.player_id),
            next_rename_time=next_allowed_ms // 1000,
        )

    # -- 成员管理 RPC（3 个） -------------------------------------------------------------------

    def _target_tip(self, actor_id: int, target_id: int) -> Message | None:
        if target_id == 0:
            return self._tip(ERR_GUILD_TARGET_NOT_MEMBER, "target player id is zero")
        if target_id == actor_id:
            return self._tip(ERR_GUILD_CANNOT_TARGET_SELF, "cannot target self")
        return None

    async def set_guild_member_role(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.SetGuildMemberRoleResponse", error_message=tip)
        target_id = int(request.target_player_id)
        if (t_tip := self._target_tip(actor.player_id, target_id)) is not None:
            return self.protocol.message("guildpb.SetGuildMemberRoleResponse", error_message=t_tip)
        role = int(request.role)
        if not is_assignable_role(role):
            return self.protocol.message(
                "guildpb.SetGuildMemberRoleResponse",
                error_message=self._tip(ERR_GUILD_NO_PERMISSION, "role not assignable"),
            )

        expected = int(request.expected_guild_id)
        try:
            guild_id, mismatched = await self._bind_expected_guild(actor.player_id, expected)
            if mismatched:
                return self.protocol.message(
                    "guildpb.SetGuildMemberRoleResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                    guild=await self._current_guild_info(actor.player_id),
                )
            if guild_id == 0:
                return self.protocol.message(
                    "guildpb.SetGuildMemberRoleResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            res = await self._with_budget(self.store.set_member_role(actor, guild_id, target_id, role))
        except (GuildNotFoundError, GuildNotInGuildError) as exc:
            if expected != 0:
                return self.protocol.message(
                    "guildpb.SetGuildMemberRoleResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                    guild=await self._current_guild_info(actor.player_id),
                )
            return self.protocol.message(
                "guildpb.SetGuildMemberRoleResponse", error_message=tip_for_error(self.protocol, exc)
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.SetGuildMemberRoleResponse", error_message=tip_for_error(self.protocol, exc)
            )

        if res.changed:
            await self._notify_best_effort(
                CHANGE_ROLE_CHANGED,
                guild_id,
                actor.player_id,
                target_id,
                [m.player_id for m in res.guild.members if m.player_id != actor.player_id],
            )
        return self.protocol.message(
            "guildpb.SetGuildMemberRoleResponse", guild=await self._guild_info_for(res.guild, actor.player_id)
        )

    async def kick_guild_member(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.KickGuildMemberResponse", error_message=tip)
        target_id = int(request.target_player_id)
        if (t_tip := self._target_tip(actor.player_id, target_id)) is not None:
            return self.protocol.message("guildpb.KickGuildMemberResponse", error_message=t_tip)

        expected = int(request.expected_guild_id)
        try:
            guild_id, mismatched = await self._bind_expected_guild(actor.player_id, expected)
            if mismatched:
                return self.protocol.message(
                    "guildpb.KickGuildMemberResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                    guild=await self._current_guild_info(actor.player_id),
                )
            if guild_id == 0:
                return self.protocol.message(
                    "guildpb.KickGuildMemberResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            res = await self._with_budget(self.store.kick_member(actor, guild_id, target_id, self._clock_ms()))
        except (GuildNotFoundError, GuildNotInGuildError) as exc:
            if expected != 0:
                return self.protocol.message(
                    "guildpb.KickGuildMemberResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                    guild=await self._current_guild_info(actor.player_id),
                )
            return self.protocol.message(
                "guildpb.KickGuildMemberResponse", error_message=tip_for_error(self.protocol, exc)
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.KickGuildMemberResponse", error_message=tip_for_error(self.protocol, exc)
            )

        await self.war.on_member_removed(guild_id, target_id)
        recipients = [m.player_id for m in res.guild.members if m.player_id != actor.player_id] + [target_id]
        await self._notify_best_effort(CHANGE_MEMBER_KICKED, guild_id, actor.player_id, target_id, recipients)
        return self.protocol.message(
            "guildpb.KickGuildMemberResponse", guild=await self._guild_info_for(res.guild, actor.player_id)
        )

    async def transfer_guild_leader(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.TransferGuildLeaderResponse", error_message=tip)
        target_id = int(request.target_player_id)
        if (t_tip := self._target_tip(actor.player_id, target_id)) is not None:
            return self.protocol.message("guildpb.TransferGuildLeaderResponse", error_message=t_tip)

        expected = int(request.expected_guild_id)
        try:
            guild_id, mismatched = await self._bind_expected_guild(actor.player_id, expected)
            if mismatched:
                return self.protocol.message(
                    "guildpb.TransferGuildLeaderResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                    guild=await self._current_guild_info(actor.player_id),
                )
            if guild_id == 0:
                return self.protocol.message(
                    "guildpb.TransferGuildLeaderResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            res = await self._with_budget(self.store.transfer_leader(actor, guild_id, target_id))
        except (GuildNotFoundError, GuildNotInGuildError) as exc:
            if expected != 0:
                return self.protocol.message(
                    "guildpb.TransferGuildLeaderResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in expected guild"),
                    guild=await self._current_guild_info(actor.player_id),
                )
            return self.protocol.message(
                "guildpb.TransferGuildLeaderResponse", error_message=tip_for_error(self.protocol, exc)
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.TransferGuildLeaderResponse", error_message=tip_for_error(self.protocol, exc)
            )

        await self._notify_best_effort(
            CHANGE_LEADER_TRANSFERRED,
            guild_id,
            actor.player_id,
            target_id,
            [m.player_id for m in res.guild.members if m.player_id != actor.player_id],
        )
        return self.protocol.message(
            "guildpb.TransferGuildLeaderResponse", guild=await self._guild_info_for(res.guild, actor.player_id)
        )

    # -- 入帮申请 RPC（5 个） -------------------------------------------------------------------

    async def apply_join_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        zone_id, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.ApplyJoinGuildResponse", error_message=tip)
        guild_id = int(request.guild_id)
        if guild_id == 0:
            return self.protocol.message(
                "guildpb.ApplyJoinGuildResponse", error_message=self._tip(ERR_GUILD_NOT_FOUND, "guild id is zero")
            )
        now_ms = self._clock_ms()
        if not self._quota.allow(actor.player_id, now_ms):
            return self.protocol.message(
                "guildpb.ApplyJoinGuildResponse",
                error_message=self._tip(ERR_RATE_LIMIT_EXCEEDED, "too many guild applications, retry later"),
            )
        try:
            res = await self._with_budget(self.store.apply_to_guild(actor, guild_id, zone_id, now_ms))
        except Exception as exc:
            return self.protocol.message(
                "guildpb.ApplyJoinGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )
        if res.inserted and res.reviewer_ids:
            await self._notify_best_effort(
                CHANGE_APPLICATION_RECEIVED, guild_id, actor.player_id, actor.player_id, res.reviewer_ids
            )
        return self.protocol.message("guildpb.ApplyJoinGuildResponse")

    async def cancel_guild_application(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.CancelGuildApplicationResponse", error_message=tip)
        guild_id = int(request.guild_id)
        if guild_id == 0:
            return self.protocol.message(
                "guildpb.CancelGuildApplicationResponse",
                error_message=self._tip(ERR_GUILD_APPLICATION_NOT_FOUND, "guild id is zero"),
            )
        try:
            await self._with_budget(self.store.cancel_application(actor, guild_id, self._clock_ms()))
        except Exception as exc:
            return self.protocol.message(
                "guildpb.CancelGuildApplicationResponse", error_message=tip_for_error(self.protocol, exc)
            )
        return self.protocol.message("guildpb.CancelGuildApplicationResponse")

    async def list_my_guild_applications(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        zone_id, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.ListMyGuildApplicationsResponse", error_message=tip)
        try:
            if await self.store.get_player_guild_id(actor.player_id) > 0:
                return self.protocol.message("guildpb.ListMyGuildApplicationsResponse")
            rows = await self._with_budget(
                self.store.list_my_applications(actor.player_id, self._clock_ms(), MY_APPLICATIONS_LIMIT)
            )
            guilds: list[tuple[GuildData, int, int]] = []
            leader_ids: list[int] = []
            for row in rows:
                g = await self.store.get_guild(row.guild_id)
                if g is None or not self.store.merged_zones.same_zone(g.zone_id, zone_id):
                    continue
                guilds.append((g, row.apply_ms, row.expire_ms))
                leader_ids.append(g.leader_id)
            names = await self._player_names.batch_get_names(leader_ids)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.ListMyGuildApplicationsResponse", error_message=tip_for_error(self.protocol, exc)
            )
        resp = self.protocol.message("guildpb.ListMyGuildApplicationsResponse")
        for g, apply_ms, expire_ms in guilds:
            resp.applications.add(
                guild_id=g.guild_id,
                guild_name=g.name,
                level=g.level,
                member_count=len(g.members),
                max_members=g.max_members,
                leader_id=g.leader_id,
                leader_name=names.get(g.leader_id, ""),
                apply_ms=apply_ms,
                expire_ms=expire_ms,
            )
        return resp

    async def list_guild_applications(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        try:
            guild_id = await self.store.get_player_guild_id(actor.player_id)
            if guild_id == 0:
                return self.protocol.message(
                    "guildpb.ListGuildApplicationsResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            rows = await self._with_budget(
                self.store.list_guild_applications(
                    actor.player_id,
                    guild_id,
                    self._clock_ms(),
                    self.store.catalog.rule.max_pending_applications_per_guild,
                )
            )
            ids = [r.player_id for r in rows]
            online_map = await self._online.batch_online(ids, strict=False)
            names = await self._player_names.batch_get_names(ids)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.ListGuildApplicationsResponse", error_message=tip_for_error(self.protocol, exc)
            )
        resp = self.protocol.message("guildpb.ListGuildApplicationsResponse")
        for r in rows:
            resp.applicants.add(
                player_id=r.player_id,
                name=names.get(r.player_id, ""),
                online=bool(online_map.get(r.player_id, False)),
                apply_ms=r.apply_ms,
                expire_ms=r.expire_ms,
            )
        return resp

    async def review_guild_application(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        applicant_id = int(request.applicant_player_id)
        if applicant_id == 0:
            return self.protocol.message(
                "guildpb.ReviewGuildApplicationResponse",
                error_message=self._tip(ERR_GUILD_APPLICATION_NOT_FOUND, "applicant player id is zero"),
            )
        if applicant_id == actor.player_id:
            return self.protocol.message(
                "guildpb.ReviewGuildApplicationResponse",
                error_message=self._tip(ERR_GUILD_CANNOT_TARGET_SELF, "cannot review own application"),
            )
        _, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.ReviewGuildApplicationResponse", error_message=tip)

        approve = bool(request.approve)
        applicant_zone = 0
        if approve:
            a_zone, a_tip = await self._resolve_home_zone(applicant_id)
            if a_tip is not None or a_zone <= 0:
                return self.protocol.message(
                    "guildpb.ReviewGuildApplicationResponse",
                    error_message=self._tip(ERR_GUILD_APPLICATION_NOT_FOUND, "applicant home zone unknown"),
                )
            applicant_zone = a_zone

        try:
            guild_id = await self.store.get_player_guild_id(actor.player_id)
            if guild_id == 0:
                return self.protocol.message(
                    "guildpb.ReviewGuildApplicationResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            res = await self._with_budget(
                self.store.review_application(
                    actor,
                    guild_id=guild_id,
                    applicant_id=applicant_id,
                    approve=approve,
                    applicant_zone=applicant_zone,
                    now_ms=self._clock_ms(),
                )
            )
        except GuildZoneMismatchError:
            return self.protocol.message(
                "guildpb.ReviewGuildApplicationResponse",
                error_message=self._tip(ERR_GUILD_APPLICATION_NOT_FOUND, "applicant is in another zone"),
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.ReviewGuildApplicationResponse", error_message=tip_for_error(self.protocol, exc)
            )

        if res.approved:
            await self._notify_best_effort(
                CHANGE_MEMBER_JOINED,
                guild_id,
                actor.player_id,
                applicant_id,
                [m.player_id for m in res.guild.members if m.player_id != actor.player_id],
            )
        else:
            await self._notify_best_effort(
                CHANGE_APPLICATION_REJECTED, guild_id, actor.player_id, applicant_id, [applicant_id]
            )
        return self.protocol.message(
            "guildpb.ReviewGuildApplicationResponse", guild=await self._guild_info_for(res.guild, actor.player_id)
        )

    # -- 推送占位（1 个） -----------------------------------------------------------------------

    async def notify_guild_changed(self, request: Message, context, path: str) -> Message:
        await self._reject_session_metadata(context, "NotifyGuildChanged")
        return self.protocol.message("Empty")

    # -- 排行榜 RPC（3 个） ---------------------------------------------------------------------

    async def update_guild_score(self, request: Message, context, path: str) -> Message:
        await self._reject_session_metadata(context, "UpdateGuildScore")
        guild_id = int(request.guild_id)
        req_zone = int(request.zone_id)
        score = int(request.score)
        try:
            auth_zone, new_version = await self._with_budget(self.store.update_guild_score(guild_id, score))
            if req_zone > 0 and not self.store.merged_zones.same_zone(req_zone, auth_zone):
                logger.warning(
                    "UpdateGuildScore ignored caller zone=%d; using authoritative guild zone=%d guild=%d",
                    req_zone,
                    auth_zone,
                    guild_id,
                )
            if auth_zone > 0:
                await self.rank_store.upsert_score(guild_id, auth_zone, score, new_version)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.UpdateGuildScoreResponse", error_message=tip_for_error(self.protocol, exc)
            )
        return self.protocol.message("guildpb.UpdateGuildScoreResponse")

    async def get_guild_rank(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        zone_id, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.GetGuildRankResponse", error_message=tip)

        page = int(request.page) or 1
        page_size = min(int(request.page_size) or DEFAULT_RANK_PAGE_SIZE, MAX_RANK_PAGE_SIZE)
        try:
            entries, total = await self._with_budget(self.rank_store.get_page(zone_id, page, page_size))
            guilds_by_id: dict[int, GuildData] = {}
            leader_ids: list[int] = []
            for e in entries:
                g = await self.store.get_guild(e.guild_id)
                if g is not None:
                    guilds_by_id[e.guild_id] = g
                    leader_ids.append(g.leader_id)
            names = await self._player_names.batch_get_names(leader_ids)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.GetGuildRankResponse", error_message=tip_for_error(self.protocol, exc)
            )

        resp = self.protocol.message(
            "guildpb.GetGuildRankResponse", total_count=total, page=page, page_size=page_size
        )
        for e in entries:
            g = guilds_by_id.get(e.guild_id)
            resp.entries.add(
                guild_id=e.guild_id,
                name=g.name if g else "",
                leader_id=g.leader_id if g else 0,
                level=g.level if g else 0,
                member_count=len(g.members) if g else 0,
                score=e.score,
                rank=e.rank,
                leader_name=names.get(g.leader_id, "") if g else "",
            )
        return resp

    async def get_guild_rank_by_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        zone_id, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.GetGuildRankByGuildResponse", error_message=tip)

        guild_id = int(request.guild_id)
        try:
            entry = await self._with_budget(self.rank_store.get_rank(guild_id, zone_id))
            if entry.rank == 0:
                return self.protocol.message(
                    "guildpb.GetGuildRankByGuildResponse",
                    error_message=self._tip(ERR_GUILD_NOT_RANKED, "guild not ranked"),
                )
            g = await self.store.get_guild(guild_id)
            names = await self._player_names.batch_get_names([g.leader_id]) if g is not None else {}
        except Exception as exc:
            return self.protocol.message(
                "guildpb.GetGuildRankByGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )

        pb_entry = self.protocol.message(
            "guildpb.GuildRankEntry",
            guild_id=entry.guild_id,
            score=entry.score,
            rank=entry.rank,
            name=g.name if g else "",
            leader_id=g.leader_id if g else 0,
            level=g.level if g else 0,
            member_count=len(g.members) if g else 0,
            leader_name=names.get(g.leader_id, "") if g else "",
        )
        return self.protocol.message("guildpb.GetGuildRankByGuildResponse", entry=pb_entry)

    # -- 帮会经济 RPC（5 个） -------------------------------------------------------------------

    async def upgrade_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        try:
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None:
                return self.protocol.message(
                    "guildpb.UpgradeGuildResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            res = await self._with_budget(
                self.store.upgrade_guild(actor, guild.guild_id, int(request.expected_level))
            )
        except (GuildNotFoundError, GuildNotInGuildError) as exc:
            return self.protocol.message(
                "guildpb.UpgradeGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.UpgradeGuildResponse",
                error_message=tip_for_error(self.protocol, exc),
                guild=await self._current_guild_info(actor.player_id),
            )

        if res.changed:
            await self._notify_best_effort(
                CHANGE_LEVEL_UP,
                guild.guild_id,
                actor.player_id,
                0,
                [pid for pid in res.member_ids if pid != actor.player_id],
            )
        return self.protocol.message(
            "guildpb.UpgradeGuildResponse", guild=await self._current_guild_info(actor.player_id)
        )

    async def get_guild_donate_options(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        now_ms = self._clock_ms()
        try:
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None:
                return self.protocol.message(
                    "guildpb.GetGuildDonateOptionsResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            contrib = await self.store.get_member_contribution(guild.guild_id, actor.player_id)
            if contrib is None:
                return self.protocol.message(
                    "guildpb.GetGuildDonateOptionsResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not a member of the guild"),
                )
            total_c, balance_c = contrib
            d_key = day_key(now_ms)
            usage = await self.store.get_counter_usage(actor.player_id, COUNTER_KIND_DONATE, (d_key,))
            pending = await self.store.list_pending_ops(actor.player_id, ASSET_STREAM_GUILD_DEBIT, ASSET_OP_MAX_PENDING)
            recent = await self.store.list_recent_ops(actor.player_id, ASSET_STREAM_GUILD_DEBIT, RECENT_SCAN_LIMIT)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.GetGuildDonateOptionsResponse", error_message=tip_for_error(self.protocol, exc)
            )

        resp = self.protocol.message(
            "guildpb.GetGuildDonateOptionsResponse",
            contribution_total=total_c,
            contribution_balance=balance_c,
            next_daily_reset_ms=next_daily_reset_ms(now_ms),
        )
        for cfg in self.store.catalog.sorted_donates():
            resp.options.add(
                donate_id=cfg.donate_id,
                name=cfg.name,
                currency_type=cfg.currency_type,
                cost_amount=cfg.cost_amount,
                contribution_gain=cfg.contribution_gain,
                funds_gain=cfg.funds_gain,
                daily_limit=cfg.daily_limit,
                used_today=usage.get((cfg.donate_id, d_key), 0),
                min_guild_level=cfg.min_guild_level,
                unlocked=guild.level >= cfg.min_guild_level,
            )
        for op in pending:
            if op.kind == OP_KIND_DONATE and op.guild_id == guild.guild_id:
                resp.pending_donations.add().CopyFrom(self._donation_view(op))
        cutoff = max(0, now_ms - RECENT_RESULT_WINDOW_MS)
        recent_added = 0
        for op in recent:
            if recent_added >= RECENT_RESULT_LIMIT:
                break
            if (
                op.kind == OP_KIND_DONATE
                and op.guild_id == guild.guild_id
                and is_terminal_op_status(op.status)
                and op.updated_ms >= cutoff
            ):
                resp.recent_results.add().CopyFrom(self._donation_view(op))
                recent_added += 1
        return resp

    async def _replay_donation(self, player_id: int, request: Message) -> Message | None:
        key = int(request.client_request_id)
        if key == 0:
            return None
        found, _, op = await self.store.get_client_request(player_id, key)
        if not found:
            return None
        expected_gid = int(request.expected_guild_id)
        if (
            op is None
            or op.kind != OP_KIND_DONATE
            or op.ref_id != int(request.donate_id)
            or op.ref_count != 1
            or (expected_gid != 0 and op.guild_id != expected_gid)
        ):
            return self.protocol.message(
                "guildpb.DonateToGuildResponse",
                error_message=self._tip(ERR_INVALID_PARAMETER, "client_request_id reused with different parameters"),
            )
        view = self._donation_view(op)
        err_tip = None
        if int(view.status) == ORDER_STATUS_REJECTED:
            err_tip = (
                self._tip(ERR_GUILD_CURRENCY_INSUFFICIENT, "currency insufficient")
                if int(view.reason_tip_id) == ERR_ASSET_CURRENCY_INSUFFICIENT
                else self._tip(ERR_GUILD_ASSET_REJECTED, "asset op rejected")
            )
        guild_msg = await self._current_guild_info(player_id)
        return self.protocol.message(
            "guildpb.DonateToGuildResponse",
            error_message=err_tip,
            donation=view,
            guild=guild_msg if guild_msg is not None and int(guild_msg.guild_id) == op.guild_id else None,
        )

    async def donate_to_guild(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        now_ms = self._clock_ms()
        try:
            replayed = await self._replay_donation(actor.player_id, request)
            if replayed is not None:
                return replayed
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None:
                return self.protocol.message(
                    "guildpb.DonateToGuildResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            expected_gid = int(request.expected_guild_id)
            if expected_gid != 0 and expected_gid != guild.guild_id:
                return self.protocol.message(
                    "guildpb.DonateToGuildResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "guild changed"),
                )
            if not self._asset_op_enabled:
                return self.protocol.message(
                    "guildpb.DonateToGuildResponse",
                    error_message=self._tip(ERR_GUILD_ASSET_PENDING, "guild asset channel disabled"),
                )
            donate_cfg = self.store.catalog.donates.get(int(request.donate_id))
            if donate_cfg is None:
                return self.protocol.message(
                    "guildpb.DonateToGuildResponse",
                    error_message=self._tip(ERR_GUILD_ASSET_REJECTED, "donate option not found"),
                )
            if guild.level < donate_cfg.min_guild_level:
                return self.protocol.message(
                    "guildpb.DonateToGuildResponse",
                    error_message=self._tip(ERR_GUILD_SHOP_LEVEL_TOO_LOW, "guild level too low"),
                )
            bundle = self.protocol.message("AssetBundle")
            bundle.currencies.add(currency_type=donate_cfg.currency_type, amount=donate_cfg.cost_amount)
            payload = bundle.SerializeToString(deterministic=True)
            deadline_ms = now_ms + self.store.catalog.rule.asset_op_deadline_seconds * 1000
            op = await self._with_budget(
                self.store.reserve_donation(
                    actor,
                    guild_id=guild.guild_id,
                    donate_id=donate_cfg.donate_id,
                    min_guild_level=donate_cfg.min_guild_level,
                    contribution_gain=donate_cfg.contribution_gain,
                    funds_gain=donate_cfg.funds_gain,
                    daily_limit=donate_cfg.daily_limit,
                    period_key_val=day_key(now_ms),
                    deadline_ms=deadline_ms,
                    lease_until_ms=now_ms + self._asset_op_lease_ms,
                    payload=payload,
                    client_request_id=int(request.client_request_id),
                    now_ms=now_ms,
                )
            )
        except GuildClientRequestReplayedError:
            replayed = await self._replay_donation(actor.player_id, request)
            if replayed is not None:
                return replayed
            return self.protocol.message(
                "guildpb.DonateToGuildResponse",
                error_message=self._tip(ERR_GUILD_ASSET_PENDING, "client request replay pending"),
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.DonateToGuildResponse", error_message=tip_for_error(self.protocol, exc)
            )

        if self._asset_loop is not None:
            try:
                await self._asset_loop.process_one(self.store.op_to_pending(op))
            except Exception as exc:
                logger.warning("guild donate sync asset delivery failed op_id=%d err=%s", op.op_id, exc)
            refreshed = await self.store.get_asset_op(op.op_id)
            if refreshed is not None:
                op = refreshed

        view = self._donation_view(op)
        err_tip = None
        if int(view.status) == ORDER_STATUS_REJECTED:
            err_tip = (
                self._tip(ERR_GUILD_CURRENCY_INSUFFICIENT, "currency insufficient")
                if int(view.reason_tip_id) == ERR_ASSET_CURRENCY_INSUFFICIENT
                else self._tip(ERR_GUILD_ASSET_REJECTED, "asset op rejected")
            )
        return self.protocol.message(
            "guildpb.DonateToGuildResponse",
            error_message=err_tip,
            donation=view,
            guild=await self._current_guild_info(actor.player_id),
        )

    async def get_guild_shop(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        now_ms = self._clock_ms()
        try:
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None:
                return self.protocol.message(
                    "guildpb.GetGuildShopResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            contrib = await self.store.get_member_contribution(guild.guild_id, actor.player_id)
            if contrib is None:
                return self.protocol.message(
                    "guildpb.GetGuildShopResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not a member of the guild"),
                )
            _, balance_c = contrib
            d_key = day_key(now_ms)
            w_key = week_key(now_ms)
            usage = await self.store.get_counter_usage(actor.player_id, COUNTER_KIND_SHOP, (d_key, w_key))
            pending = await self.store.list_pending_ops(actor.player_id, ASSET_STREAM_GUILD_CREDIT, ASSET_OP_MAX_PENDING)
            recent = await self.store.list_recent_ops(actor.player_id, ASSET_STREAM_GUILD_CREDIT, RECENT_SCAN_LIMIT)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.GetGuildShopResponse", error_message=tip_for_error(self.protocol, exc)
            )

        resp = self.protocol.message(
            "guildpb.GetGuildShopResponse",
            contribution_balance=balance_c,
            next_daily_reset_ms=next_daily_reset_ms(now_ms),
            next_weekly_reset_ms=next_weekly_reset_ms(now_ms),
        )
        for cfg in self.store.catalog.sorted_shops():
            pk, ok = period_key(cfg.limit_period, now_ms)
            used = usage.get((cfg.goods_id, pk), 0) if ok and pk > 0 else 0
            max_stack = self.store.catalog.item_max_stacks.get(cfg.item_id, 0)
            resp.goods.add(
                goods_id=cfg.goods_id,
                name=cfg.name,
                category=cfg.category,
                item_id=cfg.item_id,
                item_count=cfg.item_count,
                cost_contribution=cfg.cost_contribution,
                required_guild_level=cfg.required_guild_level,
                unlocked=guild.level >= cfg.required_guild_level,
                limit_period=cfg.limit_period,
                limit_count=cfg.limit_count,
                used_count=used,
                max_buy_count=max_shop_buy_count(cfg, max_stack),
            )
        for op in pending:
            if op.kind == OP_KIND_SHOP:
                resp.pending_orders.add().CopyFrom(self._shop_order_view(op))
        cutoff = max(0, now_ms - RECENT_RESULT_WINDOW_MS)
        recent_added = 0
        for op in recent:
            if recent_added >= RECENT_RESULT_LIMIT:
                break
            if op.kind == OP_KIND_SHOP and is_terminal_op_status(op.status) and op.updated_ms >= cutoff:
                resp.recent_orders.add().CopyFrom(self._shop_order_view(op))
                recent_added += 1
        return resp

    async def _replay_shop_order(self, player_id: int, request: Message, count: int) -> Message | None:
        key = int(request.client_request_id)
        if key == 0:
            return None
        found, _, op = await self.store.get_client_request(player_id, key)
        if not found:
            return None
        expected_gid = int(request.expected_guild_id)
        if (
            op is None
            or op.kind != OP_KIND_SHOP
            or op.ref_id != int(request.goods_id)
            or op.ref_count != count
            or (expected_gid != 0 and op.guild_id != expected_gid)
        ):
            return self.protocol.message(
                "guildpb.BuyGuildShopGoodsResponse",
                error_message=self._tip(ERR_INVALID_PARAMETER, "client_request_id reused with different parameters"),
            )
        view = self._shop_order_view(op)
        contrib = await self.store.get_member_contribution(op.guild_id, player_id)
        balance = contrib[1] if contrib is not None else 0
        err_tip = (
            self._tip(ERR_GUILD_ASSET_REJECTED, "asset op rejected")
            if int(view.status) == ORDER_STATUS_REJECTED
            else None
        )
        return self.protocol.message(
            "guildpb.BuyGuildShopGoodsResponse",
            error_message=err_tip,
            order=view,
            contribution_balance=balance,
        )

    async def buy_guild_shop_goods(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        now_ms = self._clock_ms()
        count = int(request.count) or 1
        try:
            replayed = await self._replay_shop_order(actor.player_id, request, count)
            if replayed is not None:
                return replayed
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None:
                return self.protocol.message(
                    "guildpb.BuyGuildShopGoodsResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            expected_gid = int(request.expected_guild_id)
            if expected_gid != 0 and expected_gid != guild.guild_id:
                return self.protocol.message(
                    "guildpb.BuyGuildShopGoodsResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "guild changed"),
                )
            if not self._asset_op_enabled:
                return self.protocol.message(
                    "guildpb.BuyGuildShopGoodsResponse",
                    error_message=self._tip(ERR_GUILD_ASSET_PENDING, "guild asset channel disabled"),
                )
            goods = self.store.catalog.shops.get(int(request.goods_id))
            if goods is None:
                return self.protocol.message(
                    "guildpb.BuyGuildShopGoodsResponse",
                    error_message=self._tip(ERR_GUILD_SHOP_GOODS_NOT_FOUND, "shop goods not found"),
                )
            max_stack = self.store.catalog.item_max_stacks.get(goods.item_id, 0)
            if count > max_shop_buy_count(goods, max_stack):
                return self.protocol.message(
                    "guildpb.BuyGuildShopGoodsResponse",
                    error_message=self._tip(ERR_GUILD_SHOP_LIMIT, "count exceeds max buy count"),
                )
            if guild.level < goods.required_guild_level:
                return self.protocol.message(
                    "guildpb.BuyGuildShopGoodsResponse",
                    error_message=self._tip(ERR_GUILD_SHOP_LEVEL_TOO_LOW, "guild level too low"),
                )
            pk, ok = period_key(goods.limit_period, now_ms)
            if not ok:
                return self.protocol.message(
                    "guildpb.BuyGuildShopGoodsResponse",
                    error_message=self._tip(ERR_INVALID_PARAMETER, "invalid shop limit period"),
                )
            bundle = self.protocol.message("AssetBundle")
            bundle.items.add(config_id=goods.item_id, count=goods.item_count * count)
            payload = bundle.SerializeToString(deterministic=True)
            cost = goods.cost_contribution * count
            op, balance_after = await self._with_budget(
                self.store.reserve_shop_order(
                    actor,
                    guild_id=guild.guild_id,
                    goods_id=goods.goods_id,
                    count=count,
                    required_guild_level=goods.required_guild_level,
                    cost=cost,
                    limit_count=goods.limit_count if pk > 0 else 0,
                    period_key_val=pk,
                    lease_until_ms=now_ms + self._asset_op_lease_ms,
                    payload=payload,
                    client_request_id=int(request.client_request_id),
                    now_ms=now_ms,
                )
            )
        except GuildClientRequestReplayedError:
            replayed = await self._replay_shop_order(actor.player_id, request, count)
            if replayed is not None:
                return replayed
            return self.protocol.message(
                "guildpb.BuyGuildShopGoodsResponse",
                error_message=self._tip(ERR_GUILD_ASSET_PENDING, "client request replay pending"),
            )
        except Exception as exc:
            return self.protocol.message(
                "guildpb.BuyGuildShopGoodsResponse", error_message=tip_for_error(self.protocol, exc)
            )

        if self._asset_loop is not None:
            try:
                await self._asset_loop.process_one(self.store.op_to_pending(op))
            except Exception as exc:
                logger.warning("guild shop sync asset delivery failed op_id=%d err=%s", op.op_id, exc)
            refreshed = await self.store.get_asset_op(op.op_id)
            if refreshed is not None:
                op = refreshed
            contrib_after = await self.store.get_member_contribution(guild.guild_id, actor.player_id)
            if contrib_after is not None:
                balance_after = contrib_after[1]

        view = self._shop_order_view(op)
        err_tip = (
            self._tip(ERR_GUILD_ASSET_REJECTED, "asset op rejected")
            if int(view.status) == ORDER_STATUS_REJECTED
            else None
        )
        return self.protocol.message(
            "guildpb.BuyGuildShopGoodsResponse",
            error_message=err_tip,
            order=view,
            contribution_balance=balance_after,
        )

    # -- 帮会活动与同道历练 RPC（5 个） ---------------------------------------------------------

    def _build_activity_view(
        self,
        row: ActivityRuleRow,
        *,
        guild: GuildData,
        join_time_ms: int,
        used_count: int,
        progress: ActivityProgress,
        reunion_online: int,
        reward_status: ActivityRewardStatus,
        owed_count: int = 0,
        trial_lobby: Message | None = None,
        now_ms: int,
    ) -> Message:
        rule = self.store.catalog.rule
        state = activity_state_of(row, now_ms)
        threshold = row.guild_threshold
        prog_val = 0
        reached = False
        funds_granted = False
        if row.activity_type == ACTIVITY_TYPE_LANTERN:
            prog_val = progress.count
            reached, funds_granted = progress.latched, progress.funds_granted
        elif row.activity_type == ACTIVITY_TYPE_REUNION:
            threshold = reunion_threshold(row, rule.reunion_min_online_members)
            reached, funds_granted = progress.latched, progress.funds_granted
            prog_val = 0 if reached else reunion_online
        elif row.activity_type == ACTIVITY_TYPE_TRIAL:
            prog_val = progress.count

        blocked_tip_id, _ = activity_blocked(
            state=state,
            guild_level=guild.level,
            min_level=row.min_guild_level,
            joined=joined_long_enough(join_time_ms, now_ms, rule.activity_join_min_hours),
            join_min_hours=rule.activity_join_min_hours,
            used=used_count,
            daily_limit=row.daily_limit,
            activity_type=row.activity_type,
            threshold_reached=reached,
            progress=prog_val,
            threshold=threshold,
        )
        if row.activity_type == ACTIVITY_TYPE_TRIAL and not self._trials_enabled:
            state = ACTIVITY_STATE_DISABLED
            blocked_tip_id = ERR_GUILD_ACTIVITY_NOT_OPEN

        view = self.protocol.message(
            "guildpb.GuildActivityView",
            activity_id=row.activity_id,
            type=row.activity_type,
            name=row.name,
            state=state,
            start_at_ms=row.start_at_ms,
            end_at_ms=row.end_at_ms,
            min_guild_level=row.min_guild_level,
            period_key=day_key(now_ms),
            next_reset_ms=next_daily_reset_ms(now_ms),
            server_time_ms=now_ms,
            personal_contribution=row.personal_contribution,
            guild_funds=row.guild_funds,
            guild_threshold=threshold,
            daily_limit=row.daily_limit,
            my_used_count=used_count,
            progress=prog_val,
            threshold_reached=reached,
            funds_granted=funds_granted,
            blocked_tip_id=blocked_tip_id,
            dungeon_id=row.dungeon_id,
            team_size_min=row.team_size_min,
            team_size_max=row.team_size_max,
            my_pending_reward_count=reward_status.pending_count + owed_count,
            my_pending_reason_tip_id=reward_status.pending_reason_tip_id,
            my_last_reward_reject_tip_id=reward_status.last_reject_tip_id,
            join_min_hours=rule.activity_join_min_hours,
            guild_period_key=guild_period_key(row, now_ms),
        )
        if trial_lobby is not None:
            view.trial_lobby.CopyFrom(trial_lobby)
            if int(trial_lobby.battle_id) != 0:
                view.my_trial_battle_id = int(trial_lobby.battle_id)
        for item_id, count in self.store.catalog.reward_items(row.reward_id):
            view.reward_items.add(item_id=item_id, count=count)
        return view

    async def _count_reunion_online(self, guild: GuildData, now_ms: int, *, strict: bool) -> int:
        min_hours = self.store.catalog.rule.activity_join_min_hours
        eligible = [m.player_id for m in guild.members if joined_long_enough(m.join_time_ms, now_ms, min_hours)]
        if not eligible:
            return 0
        online_map = await self._online.batch_online(eligible, strict=strict)
        return sum(1 for pid in eligible if online_map.get(pid, False))

    async def get_guild_activities(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        zone_id, tip = await self._resolve_home_zone(actor.player_id)
        if tip is not None:
            return self.protocol.message("guildpb.GetGuildActivitiesResponse", error_message=tip)
        now_ms = self._clock_ms()
        try:
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None or not self.store.merged_zones.same_zone(guild.zone_id, zone_id):
                return self.protocol.message(
                    "guildpb.GetGuildActivitiesResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            me = guild.member(actor.player_id)
            if me is None:
                return self.protocol.message(
                    "guildpb.GetGuildActivitiesResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            rows = select_visible_activities(self.store.catalog.activities, now_ms)
            d_key = day_key(now_ms)
            usage = await self.store.get_counter_usage(actor.player_id, COUNTER_KIND_ACTIVITY, (d_key,))
            p_keys = [(r.activity_id, guild_period_key(r, now_ms)) for r in rows]
            progress_map = await self.store.get_activity_progress(guild.guild_id, p_keys)
            reward_map = await self.store.get_activity_reward_statuses(actor.player_id, now_ms)
            owed_map = await self.store.count_trial_rewards_owed_by_player(actor.player_id)
            reunion_online = 0
            for r in rows:
                if r.activity_type == ACTIVITY_TYPE_REUNION and activity_state_of(r, now_ms) == ACTIVITY_STATE_OPEN:
                    if not progress_map.get((r.activity_id, guild_period_key(r, now_ms)), ActivityProgress()).latched:
                        try:
                            reunion_online = await self._count_reunion_online(guild, now_ms, strict=False)
                        except Exception:
                            reunion_online = 0
                    break
        except Exception as exc:
            return self.protocol.message(
                "guildpb.GetGuildActivitiesResponse", error_message=tip_for_error(self.protocol, exc)
            )

        resp = self.protocol.message("guildpb.GetGuildActivitiesResponse")
        for r in rows:
            g_pk = guild_period_key(r, now_ms)
            lobby_view = None
            if r.activity_type == ACTIVITY_TYPE_TRIAL and self._trials_enabled and self._trials is not None:
                lobby_view = await self._trials.get_lobby_view_for_player(
                    actor.player_id, guild.guild_id, r.activity_id, now_ms
                )
            view = self._build_activity_view(
                r,
                guild=guild,
                join_time_ms=me.join_time_ms,
                used_count=usage.get((r.activity_id, d_key), 0),
                progress=progress_map.get((r.activity_id, g_pk), ActivityProgress()),
                reunion_online=reunion_online,
                reward_status=reward_map.get(r.activity_id, ActivityRewardStatus()),
                owed_count=owed_map.get(r.activity_id, 0),
                trial_lobby=lobby_view,
                now_ms=now_ms,
            )
            resp.activities.add().CopyFrom(view)
        return resp

    async def _participate(
        self, actor: RoleOriginal, activity_type: int, activity_id: int, resp_type: str
    ) -> Message:
        now_ms = self._clock_ms()
        try:
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None or (me := guild.member(actor.player_id)) is None:
                return self.protocol.message(
                    resp_type, error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild")
                )
            row = pick_activity_for_write(self.store.catalog.activities, activity_type, activity_id, now_ms)
            if row is None:
                return self.protocol.message(
                    resp_type, error_message=self._tip(ERR_GUILD_ACTIVITY_NOT_OPEN, "guild activity not open")
                )
            rule = self.store.catalog.rule
            d_key = day_key(now_ms)
            g_pk = guild_period_key(row, now_ms)
            usage = await self.store.get_counter_usage(actor.player_id, COUNTER_KIND_ACTIVITY, (d_key,))
            used = usage.get((row.activity_id, d_key), 0)
            blocked_tip_id, params = activity_blocked(
                state=ACTIVITY_STATE_OPEN,
                guild_level=guild.level,
                min_level=row.min_guild_level,
                joined=joined_long_enough(me.join_time_ms, now_ms, rule.activity_join_min_hours),
                join_min_hours=rule.activity_join_min_hours,
                used=used,
                daily_limit=row.daily_limit,
                activity_type=row.activity_type,
                threshold_reached=True,
                progress=0,
                threshold=0,
            )
            if blocked_tip_id != 0:
                return self.protocol.message(
                    resp_type, error_message=self._tip(blocked_tip_id, *(params or ("guild activity blocked",)))
                )

            observed = False
            online = 0
            threshold = 0
            if activity_type == ACTIVITY_TYPE_REUNION:
                threshold = reunion_threshold(row, rule.reunion_min_online_members)
                prog_map = await self.store.get_activity_progress(guild.guild_id, ((row.activity_id, g_pk),))
                latched = prog_map.get((row.activity_id, g_pk), ActivityProgress()).latched
                if not latched:
                    online = await self._count_reunion_online(guild, now_ms, strict=True)
                    observed = reunion_observed(online, threshold)
                    if not observed:
                        b_tip, b_params = activity_blocked(
                            state=ACTIVITY_STATE_OPEN,
                            guild_level=guild.level,
                            min_level=row.min_guild_level,
                            joined=True,
                            join_min_hours=rule.activity_join_min_hours,
                            used=used,
                            daily_limit=row.daily_limit,
                            activity_type=ACTIVITY_TYPE_REUNION,
                            threshold_reached=False,
                            progress=online,
                            threshold=threshold,
                        )
                        return self.protocol.message(resp_type, error_message=self._tip(b_tip, *b_params))

            reward_items = self.store.catalog.reward_items(row.reward_id)
            reward_payload = b""
            if reward_items:
                if not self._asset_op_enabled:
                    return self.protocol.message(
                        resp_type, error_message=self._tip(ERR_GUILD_ASSET_PENDING, "guild asset channel disabled")
                    )
                bundle = self.protocol.message("AssetBundle")
                for item_id, count in reward_items:
                    bundle.items.add(config_id=item_id, count=count)
                reward_payload = bundle.SerializeToString(deterministic=True)

            res = await self._with_budget(
                self.store.participate_activity(
                    actor,
                    guild_id=guild.guild_id,
                    row=row,
                    observed_reunion=observed,
                    online_for_error=online,
                    threshold_for_error=threshold,
                    reward_payload=reward_payload,
                    lease_until_ms=now_ms + self._asset_op_lease_ms,
                    now_ms=now_ms,
                )
            )
        except Exception as exc:
            return self.protocol.message(resp_type, error_message=tip_for_error(self.protocol, exc))

        if res.op is not None and self._asset_loop is not None:
            try:
                await self._asset_loop.process_one(self.store.op_to_pending(res.op))
            except Exception as exc:
                logger.warning("guild activity reward sync delivery failed op_id=%d err=%s", res.op.op_id, exc)

        if res.reached_now:
            await self._notify_best_effort(
                CHANGE_ACTIVITY_CHANGED,
                guild.guild_id,
                actor.player_id,
                0,
                [m.player_id for m in guild.members if m.player_id != actor.player_id],
            )
        usage_after = await self.store.get_counter_usage(actor.player_id, COUNTER_KIND_ACTIVITY, (res.day_key_val,))
        reward_after = await self.store.get_activity_reward_statuses(actor.player_id, now_ms)
        owed_after = await self.store.count_trial_rewards_owed_by_player(actor.player_id)
        view = self._build_activity_view(
            row,
            guild=guild,
            join_time_ms=me.join_time_ms,
            used_count=usage_after.get((row.activity_id, res.day_key_val), used + 1),
            progress=res.progress,
            reunion_online=online,
            reward_status=reward_after.get(row.activity_id, ActivityRewardStatus()),
            owed_count=owed_after.get(row.activity_id, 0),
            now_ms=now_ms,
        )
        return self.protocol.message(resp_type, activity=view)

    async def light_guild_lantern(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._participate(
            actor, ACTIVITY_TYPE_LANTERN, int(request.activity_id), "guildpb.LightGuildLanternResponse"
        )

    async def claim_guild_reunion(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._participate(
            actor, ACTIVITY_TYPE_REUNION, int(request.activity_id), "guildpb.ClaimGuildReunionResponse"
        )

    async def _build_trial_activity_view(
        self, actor: RoleOriginal, guild: GuildData, activity_id: int, now_ms: int
    ) -> Message | None:
        row = pick_activity_for_write(self.store.catalog.activities, ACTIVITY_TYPE_TRIAL, activity_id, now_ms)
        if row is None:
            row = next(
                (
                    r
                    for r in self.store.catalog.activities
                    if r.activity_id == activity_id and r.activity_type == ACTIVITY_TYPE_TRIAL
                ),
                None,
            )
        if row is None:
            return None
        me = guild.member(actor.player_id)
        if me is None:
            return None
        d_key = day_key(now_ms)
        g_pk = guild_period_key(row, now_ms)
        usage = await self.store.get_counter_usage(actor.player_id, COUNTER_KIND_ACTIVITY, (d_key,))
        progress_map = await self.store.get_activity_progress(guild.guild_id, ((row.activity_id, g_pk),))
        reward_map = await self.store.get_activity_reward_statuses(actor.player_id, now_ms)
        owed_map = await self.store.count_trial_rewards_owed_by_player(actor.player_id)
        lobby_view = (
            await self._trials.get_lobby_view_for_player(actor.player_id, guild.guild_id, row.activity_id, now_ms)
            if self._trials is not None
            else None
        )
        return self._build_activity_view(
            row,
            guild=guild,
            join_time_ms=me.join_time_ms,
            used_count=usage.get((row.activity_id, d_key), 0),
            progress=progress_map.get((row.activity_id, g_pk), ActivityProgress()),
            reunion_online=0,
            reward_status=reward_map.get(row.activity_id, ActivityRewardStatus()),
            owed_count=owed_map.get(row.activity_id, 0),
            trial_lobby=lobby_view,
            now_ms=now_ms,
        )

    async def start_guild_trial(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        if not self._trials_enabled or self._trials is None:
            return self.protocol.message(
                "guildpb.StartGuildTrialResponse",
                error_message=self._tip(ERR_GUILD_ACTIVITY_NOT_OPEN, "guild trial not open"),
            )
        now_ms = self._clock_ms()
        try:
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None or guild.member(actor.player_id) is None:
                return self.protocol.message(
                    "guildpb.StartGuildTrialResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            tip_id, tip_params = await self._trials.start_trial(
                actor,
                guild,
                int(request.activity_id),
                [int(p) for p in request.member_player_ids],
            )
            if tip_id != 0:
                return self.protocol.message(
                    "guildpb.StartGuildTrialResponse",
                    error_message=self._tip(tip_id, *(tip_params or ("guild trial failed",))),
                )
            view = await self._build_trial_activity_view(actor, guild, int(request.activity_id), now_ms)
            return self.protocol.message("guildpb.StartGuildTrialResponse", activity=view)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.StartGuildTrialResponse", error_message=tip_for_error(self.protocol, exc)
            )

    async def respond_guild_trial_invite(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        if not self._trials_enabled or self._trials is None:
            return self.protocol.message(
                "guildpb.RespondGuildTrialInviteResponse",
                error_message=self._tip(ERR_GUILD_ACTIVITY_NOT_OPEN, "guild trial not open"),
            )
        now_ms = self._clock_ms()
        lobby_id = int(request.lobby_id)
        try:
            guild = await self.store.get_player_guild(actor.player_id)
            if guild is None or guild.member(actor.player_id) is None:
                return self.protocol.message(
                    "guildpb.RespondGuildTrialInviteResponse",
                    error_message=self._tip(ERR_GUILD_NOT_IN_GUILD, "not in any guild"),
                )
            pre = await self.store.get_trial_lobby(lobby_id)
            activity_id = pre.activity_id if pre is not None else 0
            tip_id, tip_params = await self._trials.respond_invite(
                actor, guild, lobby_id, bool(request.accept)
            )
            if tip_id != 0:
                return self.protocol.message(
                    "guildpb.RespondGuildTrialInviteResponse",
                    error_message=self._tip(tip_id, *(tip_params or ("guild trial respond failed",))),
                )
            view = (
                await self._build_trial_activity_view(actor, guild, activity_id, now_ms)
                if activity_id != 0
                else None
            )
            return self.protocol.message("guildpb.RespondGuildTrialInviteResponse", activity=view)
        except Exception as exc:
            return self.protocol.message(
                "guildpb.RespondGuildTrialInviteResponse", error_message=tip_for_error(self.protocol, exc)
            )

    # -- 帮会全天帮战 V2 RPC（11 个，含 1 个下推桩） ---------------------------------------------

    async def get_guild_war_overview(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.get_overview(actor))

    async def set_guild_war_stance(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.set_stance(actor, request))

    async def register_guild_war_garrison(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.register_garrison(actor, request))

    async def start_guild_war_battle(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.start_battle(actor, request))

    async def claim_guild_war_milestone(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.claim_milestone(actor, request))

    async def claim_guild_war_dividend(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.claim_dividend(actor, request))

    async def post_guild_war_bounty(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.post_bounty(actor, request))

    async def track_guild_war_bounty(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.track_bounty(actor, request))

    async def get_guild_war_battle_result(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.get_battle_result(actor, request))

    async def notify_guild_war_alert(self, request: Message, context, path: str) -> Message:
        await self._reject_session_metadata(context, "NotifyGuildWarAlert")
        return self.protocol.message("Empty")

    async def inspect_field_war_targets(self, request: Message, context, path: str) -> Message:
        actor = await self._admit_client(context, path)
        return await self._with_budget(self.war.inspect_field_targets(actor, request))

    async def handle_battle_result_event(self, event: Message):
        """消费战斗结果事件：按 activity_context.kind 分派至全天帮战或同道历练结算。"""
        if event.HasField("activity_context") and int(event.activity_context.kind) == 3:
            return await self.war.settle_battle(
                int(event.battle_id),
                outcome=int(event.outcome),
                rounds=int(event.total_rounds),
                finished_at_ms=int(event.finished_at_ms) or self._clock_ms(),
            )
        if self._trials is None:
            return None
        return await self._trials.handle_battle_result_event(event)

    # -- GuildInternal.ListAppliedAssetOpsSince -------------------------------------------------

    async def list_applied_asset_ops_since(self, request: Message, context, path: str) -> Message:
        await self._reject_session_metadata(context, "GuildInternal.ListAppliedAssetOpsSince")
        player_ids = [int(pid) for pid in request.player_ids]
        if not player_ids:
            await context.abort(CODE.INVALID_ARGUMENT, "player_ids is required")
        if len(player_ids) > MAX_APPLIED_OPS_PLAYER_IDS:
            await context.abort(CODE.INVALID_ARGUMENT, f"player_ids has {len(player_ids)} entries, limit {MAX_APPLIED_OPS_PLAYER_IDS}")
        seen: set[int] = set()
        for pid in player_ids:
            if pid == 0:
                await context.abort(CODE.INVALID_ARGUMENT, "player_ids must not contain 0")
            if pid in seen:
                await context.abort(CODE.INVALID_ARGUMENT, "player_ids must not contain duplicates")
            seen.add(pid)
        since_ms = int(request.since_ms)
        if since_ms == 0:
            await context.abort(CODE.INVALID_ARGUMENT, "since_ms is required")
        raw_limit = int(request.limit)
        if raw_limit == 0:
            limit = MAX_APPLIED_OPS_PAGE_LIMIT
        elif raw_limit > MAX_APPLIED_OPS_PAGE_LIMIT:
            await context.abort(CODE.INVALID_ARGUMENT, f"limit {raw_limit} exceeds {MAX_APPLIED_OPS_PAGE_LIMIT}")
        else:
            limit = raw_limit

        if self._terminal_retention_days <= 0:
            await context.abort(CODE.UNAVAILABLE, "guild asset op retention not configured")
        try:
            watermark = await self.rank_store.terminal_cleanup_watermark_ms()
        except Exception as exc:
            logger.error("ListAppliedAssetOpsSince watermark unavailable err=%s", exc)
            await context.abort(CODE.UNAVAILABLE, "guild asset op cleanup watermark is not available")
        cutoff = provable_cutoff_ms(self._clock_ms(), self._terminal_retention_days, watermark)
        if since_ms < cutoff:
            await context.abort(CODE.FAILED_PRECONDITION, f"{RETENTION_REJECTED_PREFIX}{cutoff}")

        try:
            ops, next_after = await self._with_budget(
                self.store.list_applied_asset_ops_since(
                    zone_id=int(request.zone_id),
                    player_ids=player_ids,
                    since_ms=since_ms,
                    after_op_id=int(request.after_op_id),
                    limit=limit,
                )
            )
        except asyncio.TimeoutError:
            await context.abort(CODE.DEADLINE_EXCEEDED, "list applied asset ops timed out")
        except Exception as exc:
            logger.error("ListAppliedAssetOpsSince query failed players=%d err=%s", len(player_ids), exc)
            await context.abort(CODE.INTERNAL, "list applied asset ops failed")

        resp = self.protocol.message("guildpb.ListAppliedAssetOpsSinceResponse", next_after_op_id=next_after)
        for op in ops:
            resp.ops.add(
                op_id=op.op_id,
                player_id=op.player_id,
                guild_id=op.guild_id,
                stream=op.stream,
                kind=op.kind,
                status=op.status,
                funds_delta=op.funds_delta,
                contribution_delta=op.contribution_delta,
                updated_ms=op.updated_ms,
            )
        return resp

    # -- RoleFenceParticipant（3 个） -----------------------------------------------------------

    async def prepare_role_freeze(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            outcome = await self._with_budget(
                self.store.prepare_role_freeze(request.state, dry_run=bool(request.dry_run))
            )
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception as exc:
            logger.error("guild PrepareRoleFreeze failed player=%d err=%s", request.state.player_id, exc)
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", retry=True)
        return self.protocol.message(
            "rolefence.PrepareRoleFreezeResponse",
            clean=outcome.clean,
            blocker=outcome.blocker,
            stale=outcome.stale,
            retry=outcome.retry,
        )

    async def apply_role_fence(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            await self._with_budget(self.store.apply_role_fence(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception as exc:
            logger.error("guild ApplyRoleFence failed player=%d err=%s", request.state.player_id, exc)
            return self.protocol.message("rolefence.ApplyRoleFenceResponse", retry=True)
        return self.protocol.message("rolefence.ApplyRoleFenceResponse", retry=False)

    async def purge_removed_role(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            res = await self._with_budget(self.store.purge_removed_role(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception as exc:
            logger.error("guild PurgeRemovedRole failed player=%d err=%s", request.state.player_id, exc)
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)
        if res is None:
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)

        if res.guild_id != 0:
            if res.disbanded:
                try:
                    known_zones = await self.store.list_guild_zones()
                    await self.rank_store.bury_guild(res.guild_id, res.zone_id, known_zones)
                except Exception as exc:
                    logger.error(
                        "guild role purge bury rank failed guild=%d zone=%d err=%s", res.guild_id, res.zone_id, exc
                    )
            elif res.successor != 0:
                await self._notify_best_effort(
                    CHANGE_LEADER_TRANSFERRED, res.guild_id, 0, res.successor, res.members
                )
            else:
                await self._notify_best_effort(CHANGE_MEMBER_LEFT, res.guild_id, 0, res.player_id, res.members)
        return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=False)
