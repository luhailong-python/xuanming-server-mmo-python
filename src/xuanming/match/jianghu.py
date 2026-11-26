"""江湖恩怨与天墉生死状一期（548..554）权威业务逻辑与状态管理（对齐 Go jianghu_duel_logic.go）。

职责：
- 从 JianghuRule 表 id=1 加载配置规则（缺失表时使用首测基准默认值，enabled=0 时 fail-closed 拒收）；
- CreateJianghuDuelInvite / ConfirmJianghuDuelMember / RespondJianghuDuelInvite 全状态机：
  - 1V1 直接进入 PENDING_TARGET，目标应战即原子成局；
  - 2V2–5V5 先发起方 30s 全员确认，再进入 PENDING_TARGET，目标队长应战后启动目标队 30s 全员确认，全员确认后成局；
  - 同目标 5 分钟冷却、UTC+8 05:00 游戏日未成局上限（成局自动回补计数）、单目标同时仅挂 1 张有效战书；
  - 队伍名册变动自动作废（0 扣费）、拉黑拦截与跨队全员互为好友绕过闭关免战；
- ListJianghuGrudges / DismissJianghuGrudge 个人恩怨录（按 (player_id, owner_revision) 隔离过户换人，单人最多 50 宿敌 × 20 条战报，一笔勾销后再交手自动显现）；
- NotifyJianghuDuelInvite (553) / NotifyJianghuDuelStatus (554) 推送辅助与系统世界频道模板播报触发。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone

from google.protobuf.message import Message

from ..protocol import Protocol
from .errors import (
    ERR_JIANGHU_BLOCKED_BY_PEER,
    ERR_JIANGHU_DAILY_INVITE_LIMIT_REACHED,
    ERR_JIANGHU_DUEL_DISABLED,
    ERR_JIANGHU_GRUDGE_NOT_FOUND,
    ERR_JIANGHU_INSUFFICIENT_WRIT_OR_SILVER,
    ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED,
    ERR_JIANGHU_NOT_INVITE_PARTICIPANT,
    ERR_JIANGHU_SAME_TARGET_COOLDOWN,
    ERR_JIANGHU_SERVICE_BUSY,
    ERR_JIANGHU_TEAM_CHANGED_ABORTED,
    ERR_JIANGHU_TEAM_SIZE_MISMATCH,
    ERR_MATCH_CHALLENGE_SELF,
    ERR_MATCH_CHALLENGE_TARGET_BUSY,
    ERR_MATCH_CHALLENGE_TARGET_OFFLINE,
    set_tip,
)
from .gather import PresenceProvider
from .model import MAX_TEAM_SIZE, CallerIdentity
from .ranked import _default_config_tables
from .store import MatchStore

logger = logging.getLogger(__name__)

# -- 协议常量（与 proto/match/match_service.proto、message_id.txt 严格一致）--------------------------

MSG_NOTIFY_JIANGHU_DUEL_INVITE = 553
MSG_NOTIFY_JIANGHU_DUEL_STATUS = 554

WRIT_TIER_SILVER = 1
WRIT_TIER_GOLD_ITEM = 2

INVITE_STATE_AWAITING_CHALLENGER_TEAM = 1
INVITE_STATE_PENDING_TARGET = 2
INVITE_STATE_AWAITING_TARGET_TEAM = 3
INVITE_STATE_MATCHED = 4
INVITE_STATE_DECLINED = 5
INVITE_STATE_ABORTED = 6

# Battle outcome（proto/battle/battle.proto EBattleOutcome）
_OUTCOME_SIDE_A_WIN = 1
_OUTCOME_SIDE_B_WIN = 2
_OUTCOME_DRAW = 3
_OUTCOME_VOID = 4

# 系统世界频道模板 ID（proto/chat/chat.proto SystemWorldNoticeTemplate）
NOTICE_JIANGHU_DUEL_START_SILVER = 1
NOTICE_JIANGHU_DUEL_START_GOLD = 2
NOTICE_JIANGHU_DUEL_WIN_SILVER = 3
NOTICE_JIANGHU_DUEL_WIN_GOLD = 4
NOTICE_JIANGHU_DUEL_DRAW = 5
NOTICE_JIANGHU_DUEL_STREAK_MILESTONE = 6
NOTICE_JIANGHU_DUEL_STREAK_BROKEN = 7


# -- 规则配置（JianghuRule 表驱动）------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class JianghuRuleConfig:
    """JianghuRule 表 id=1 权威规则参数（对齐 Go loadJianghuRule）。"""

    enabled: bool = True
    silver_writ_cost: int = 500
    gold_writ_item_id: int = 3415
    clear_item_id: int = 3416
    silver_clear_cost: int = 1000
    invite_ttl_ms: int = 60_000
    team_confirm_ms: int = 30_000
    same_target_cooldown_ms: int = 300_000
    daily_unmatched_limit: int = 20
    duel_round_limit: int = 30
    streak_highlight_threshold: int = 3
    grudge_max_rivals: int = 50
    grudge_max_logs_per_rival: int = 20
    guild_wall_max_records: int = 20


def load_jianghu_rule(tables=None) -> JianghuRuleConfig:
    """读取 JianghuRule 表 id=1；表缺失时回落一期基准默认值，enabled=0 时严格关闭。"""
    source = tables if tables is not None else _default_config_tables()
    if source is None or "JianghuRule" not in source.tables:
        return JianghuRuleConfig()
    row = source["JianghuRule"].get(1)
    if row is None:
        return JianghuRuleConfig()
    return JianghuRuleConfig(
        enabled=int(getattr(row, "enabled", 1)) != 0,
        silver_writ_cost=int(getattr(row, "silver_writ_cost", 500)) or 500,
        gold_writ_item_id=int(getattr(row, "gold_writ_item_id", 3415)) or 3415,
        clear_item_id=int(getattr(row, "clear_item_id", 3416)) or 3416,
        silver_clear_cost=int(getattr(row, "silver_clear_cost", 1000)) or 1000,
        invite_ttl_ms=(int(getattr(row, "invite_ttl_sec", 60)) or 60) * 1000,
        team_confirm_ms=(int(getattr(row, "team_confirm_sec", 30)) or 30) * 1000,
        same_target_cooldown_ms=(int(getattr(row, "same_target_cooldown_sec", 300)) or 300) * 1000,
        daily_unmatched_limit=int(getattr(row, "daily_unmatched_invite_limit", 20)) or 20,
        duel_round_limit=int(getattr(row, "duel_round_limit", 30)) or 30,
        streak_highlight_threshold=int(getattr(row, "streak_highlight_threshold", 3)) or 3,
        grudge_max_rivals=int(getattr(row, "grudge_max_rivals", 50)) or 50,
        grudge_max_logs_per_rival=int(getattr(row, "grudge_max_logs_per_rival", 20)) or 20,
        guild_wall_max_records=int(getattr(row, "guild_wall_max_records", 20)) or 20,
    )


# -- 纯函数 -----------------------------------------------------------------------------------------


def jianghu_game_day_key(now_ms: int) -> int:
    """计算 UTC+8 每日 05:00 游戏日键（YYYYMMDD，与 Go JianghuGameDayKey 逐秒一致）。"""
    shifted_sec = (int(now_ms) // 1000) + 8 * 3600 - 5 * 3600
    dt = datetime.fromtimestamp(shifted_sec, tz=timezone.utc)
    return dt.year * 10000 + dt.month * 100 + dt.day


def normalize_jianghu_team_ids(captain_id: int, provided: Sequence[int]) -> tuple[list[int], bool]:
    """规范化一侧成员列表：确保队长位于首位、去重、不含 0 且人数在 1..5。"""
    cap = int(captain_id)
    if cap <= 0:
        return [], False
    seen: set[int] = {cap}
    out = [cap]
    for raw in provided:
        pid = int(raw)
        if pid <= 0:
            return [], False
        if pid in seen:
            continue
        seen.add(pid)
        out.append(pid)
    if not 1 <= len(out) <= MAX_TEAM_SIZE:
        return [], False
    return out, True


# -- 内部状态与结算摘要 -----------------------------------------------------------------------------


@dataclass(slots=True)
class _DuelMemberState:
    player_id: int
    name: str
    confirmed: bool = False


@dataclass(slots=True)
class _DuelInviteState:
    duel_invite_id: int
    writ_tier: int
    team_size: int
    state: int
    challenger_captain_id: int
    target_captain_id: int
    challenger_account: str
    challenger_revision: int
    target_account: str
    target_revision: int
    side_a_guild_id: int
    side_a_guild_name: str
    side_b_guild_id: int
    side_b_guild_name: str
    challenger_members: list[_DuelMemberState]
    target_members: list[_DuelMemberState]
    created_ms: int
    expires_ms: int
    confirm_deadline_ms: int
    battle_id: int = 0


@dataclass(slots=True)
class _GrudgeLogState:
    battle_id: int
    outcome_for_self: int
    total_rounds: int
    writ_tier: int
    team_size: int
    finished_at_ms: int


@dataclass(slots=True)
class _GrudgeRelationState:
    peer_player_id: int
    peer_name: str
    wins: int = 0
    losses: int = 0
    draws: int = 0
    win_streak: int = 0
    lose_streak: int = 0
    last_battle_ms: int = 0
    dismissed: bool = False
    logs: list[_GrudgeLogState] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class JianghuBattleGrudgeSummary:
    """单局生死状写入个人恩怨录后的连胜/雪耻摘要，供上层决定系统播报。"""

    winner_player_id: int = 0
    loser_player_id: int = 0
    winner_new_win_streak: int = 0
    loser_prev_win_streak: int = 0
    is_draw: bool = False


# -- 外部协作者回调类型 -----------------------------------------------------------------------------

SocialRelationChecker = Callable[[Sequence[int], Sequence[int]], Awaitable[tuple[bool, bool]]]
EligibilityChecker = Callable[
    [list[int], list[int], int, int, JianghuRuleConfig, bool], Awaitable[tuple[int, str]]
]
GuildLookup = Callable[[int], Awaitable[tuple[int, str]]]
BattleLauncher = Callable[[Message, list[int], list[int], Message], Awaitable[int]]
WorldNoticePublisher = Callable[[Message], Awaitable[None]]
DuelPushSender = Callable[[int, int, Message], Awaitable[None]]


class JianghuManager:
    """江湖恩怨与天墉生死状一期管理器（548..554 + 终局恩怨入账与系统播报）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        presence: PresenceProvider,
        *,
        rule: JianghuRuleConfig | None = None,
        social_checker: SocialRelationChecker | None = None,
        eligibility_checker: EligibilityChecker | None = None,
        guild_resolver: GuildLookup | None = None,
        battle_launcher: BattleLauncher | None = None,
        world_notice_publisher: WorldNoticePublisher | None = None,
        push_sender: DuelPushSender | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.presence = presence
        self.rule = rule if rule is not None else load_jianghu_rule()
        self.social_checker = social_checker
        self.eligibility_checker = eligibility_checker
        self.guild_resolver = guild_resolver
        self.battle_launcher = battle_launcher
        self.world_notice_publisher = world_notice_publisher
        self.push_sender = push_sender
        self._clock_ms = clock_ms

        self._lock = asyncio.Lock()
        self._next_invite_id = 880_000
        self._invites: dict[int, _DuelInviteState] = {}
        self._same_target_cooldown: dict[tuple[int, int], int] = {}
        self._daily_unmatched: dict[int, tuple[int, int]] = {}
        self._grudges: dict[tuple[int, int, int], _GrudgeRelationState] = {}
        self._grudge_seen: set[tuple[int, int, int, int]] = set()
        self.pushed_events: list[tuple[int, int, Message]] = []

    def _now(self) -> int:
        return max(1, int(self._clock_ms()))

    # -- 推送与世界播报辅助 -------------------------------------------------------------------------

    async def push_invite(self, to_player_id: int, view: Message) -> None:
        """向单个目标推送 553 NotifyJianghuDuelInvite。"""
        if to_player_id <= 0:
            return
        msg = self.protocol.message("match.JianghuDuelInviteS2C", invite=view)
        self.pushed_events.append((to_player_id, MSG_NOTIFY_JIANGHU_DUEL_INVITE, msg))
        if self.push_sender is not None:
            try:
                await self.push_sender(to_player_id, MSG_NOTIFY_JIANGHU_DUEL_INVITE, msg)
            except Exception as exc:
                logger.error("jianghu push invite failed player=%d invite=%d err=%s", to_player_id, int(view.duel_invite_id), exc)

    async def push_status_to_members(
        self, members: Sequence[_DuelMemberState], view: Message, reason_code: str
    ) -> None:
        """向指定一侧成员推送 554 NotifyJianghuDuelStatus。"""
        msg = self.protocol.message("match.JianghuDuelStatusS2C", invite=view, reason_code=reason_code)
        for m in members:
            if m.player_id <= 0:
                continue
            self.pushed_events.append((m.player_id, MSG_NOTIFY_JIANGHU_DUEL_STATUS, msg))
            if self.push_sender is not None:
                try:
                    await self.push_sender(m.player_id, MSG_NOTIFY_JIANGHU_DUEL_STATUS, msg)
                except Exception as exc:
                    logger.error(
                        "jianghu push status failed player=%d invite=%d reason=%s err=%s",
                        m.player_id,
                        int(view.duel_invite_id),
                        reason_code,
                        exc,
                    )

    async def push_status_to_all(
        self,
        challengers: Sequence[_DuelMemberState],
        targets: Sequence[_DuelMemberState],
        view: Message,
        reason_code: str,
    ) -> None:
        await self.push_status_to_members(challengers, view, reason_code)
        await self.push_status_to_members(targets, view, reason_code)

    async def publish_world_notice(self, req: Message) -> None:
        """通过系统世界播报通道发布白名单模板播报（尽力而为，不阻塞开局/结算）。"""
        if self.world_notice_publisher is None or req is None:
            return
        try:
            await self.world_notice_publisher(req)
        except Exception as exc:
            logger.error(
                "jianghu publish world notice failed notice_id=%s template=%d err=%s",
                req.notice_id,
                int(req.template_id),
                exc,
            )

    async def publish_duel_start_notice(self, battle_id: int, duel_ctx: Message) -> None:
        """成局建房后向全服播报天墉生死状开战信息（供 GatherPipeline 回调）。"""
        if battle_id <= 0 or duel_ctx is None:
            return
        tmpl = (
            NOTICE_JIANGHU_DUEL_START_GOLD
            if int(duel_ctx.writ_tier) == WRIT_TIER_GOLD_ITEM
            else NOTICE_JIANGHU_DUEL_START_SILVER
        )
        req = self.protocol.message(
            "chatpb.PublishSystemWorldNoticeRequest",
            notice_id=f"jh_duel:{battle_id}:start",
            template_id=tmpl,
            battle_id=battle_id,
            actor_a_name=str(duel_ctx.challenger_captain_name),
            actor_b_name=str(duel_ctx.target_captain_name),
            team_size=int(duel_ctx.team_size) or 1,
        )
        await self.publish_world_notice(req)

    # -- 视图构建与队伍核验 -------------------------------------------------------------------------

    def _build_view(self, inv: _DuelInviteState, now_ms: int) -> Message:
        ch_name = next((m.name for m in inv.challenger_members if m.player_id == inv.challenger_captain_id), "")
        tg_name = next((m.name for m in inv.target_members if m.player_id == inv.target_captain_id), "")
        view = self.protocol.message(
            "match.JianghuDuelInviteView",
            duel_invite_id=inv.duel_invite_id,
            writ_tier=inv.writ_tier,
            team_size=inv.team_size,
            state=inv.state,
            challenger_captain_id=inv.challenger_captain_id,
            challenger_captain_name=ch_name or f"道友{inv.challenger_captain_id}",
            target_captain_id=inv.target_captain_id,
            target_captain_name=tg_name or f"道友{inv.target_captain_id}",
            side_a_guild_id=inv.side_a_guild_id,
            side_a_guild_name=inv.side_a_guild_name,
            side_b_guild_id=inv.side_b_guild_id,
            side_b_guild_name=inv.side_b_guild_name,
            created_at_ms=inv.created_ms,
            expires_at_ms=inv.expires_ms,
            confirm_deadline_ms=inv.confirm_deadline_ms,
            battle_id=inv.battle_id,
            server_time_ms=now_ms,
        )
        for m in inv.challenger_members:
            view.challenger_members.add(player_id=m.player_id, name=m.name, confirmed=m.confirmed)
        for m in inv.target_members:
            view.target_members.add(player_id=m.player_id, name=m.name, confirmed=m.confirmed)
        return view

    async def _load_team_members(self, captain_id: int) -> tuple[int, int, list[int]]:
        """返回 (team_id, leader_id, member_ids)；无队时返回 (0, captain_id, [captain_id])。"""
        snap = await self.store.get_my_team(captain_id)
        if snap.team_id == 0 or snap.record is None:
            return 0, captain_id, [captain_id]
        leader_id = int(snap.record.leader_id)
        members = [int(m.player_id) for m in snap.record.members if int(m.player_id) > 0]
        return int(snap.team_id), leader_id, members or [captain_id]

    async def _resolve_authoritative_side(
        self, captain_id: int, requested: Sequence[int]
    ) -> tuple[list[int], int, str]:
        team_id, leader_id, auth_members = await self._load_team_members(captain_id)
        if team_id != 0 and leader_id != 0 and leader_id != captain_id:
            return [], ERR_JIANGHU_NOT_INVITE_PARTICIPANT, "小队生死状仅限队长发起或应战"
        norm_auth, ok = normalize_jianghu_team_ids(captain_id, auth_members)
        if not ok:
            return [], ERR_JIANGHU_TEAM_SIZE_MISMATCH, "小队人数不合法"
        req_ids = [int(x) for x in requested]
        if team_id == 0:
            if len(req_ids) > 1 or (len(req_ids) == 1 and req_ids[0] != captain_id):
                return [], ERR_JIANGHU_TEAM_SIZE_MISMATCH, "仅组队状态下可发起多人战书"
            return [captain_id], 0, ""
        if req_ids:
            norm_req, req_ok = normalize_jianghu_team_ids(captain_id, req_ids)
            if not req_ok or set(norm_auth) != set(norm_req):
                return [], ERR_JIANGHU_TEAM_CHANGED_ABORTED, "小队成员已变动，请刷新后重下战书"
        return norm_auth, 0, ""

    async def _verify_roster_unchanged(self, captain_id: int, expected: Sequence[int]) -> bool:
        team_id, leader_id, auth_members = await self._load_team_members(captain_id)
        exp_set = set(expected)
        if len(expected) == 1:
            return team_id == 0 or (set(auth_members) == exp_set and leader_id in (0, captain_id))
        return team_id != 0 and leader_id in (0, captain_id) and set(auth_members) == exp_set

    async def _check_players_available(self, side_a: Sequence[int], side_b: Sequence[int]) -> tuple[int, str]:
        for pid in (*side_a, *side_b):
            pres = await self.presence.get_presence(pid)
            if pres.in_battle:
                return ERR_MATCH_CHALLENGE_TARGET_BUSY, "有参战道友正在其它战斗中"
            if not pres.online:
                return ERR_MATCH_CHALLENGE_TARGET_OFFLINE, "有参战道友当前不在线"
        return 0, ""

    async def _resolve_names(self, player_ids: Sequence[int]) -> dict[int, str]:
        out: dict[int, str] = {}
        for pid in player_ids:
            pres = await self.presence.get_presence(pid)
            out[pid] = pres.name.strip() if pres.name and pres.name.strip() else f"道友{pid}"
        return out

    async def _resolve_pure_guild(self, members: Sequence[int]) -> tuple[int, str]:
        if self.guild_resolver is None or not members:
            return 0, ""
        common_id = 0
        common_name = ""
        for pid in members:
            try:
                gid, gname = await self.guild_resolver(pid)
            except Exception:
                return 0, ""
            if gid <= 0:
                return 0, ""
            if common_id == 0:
                common_id, common_name = gid, gname
            elif common_id != gid:
                return 0, ""
        return common_id, common_name

    def _target_has_pending_locked(self, target_captain_id: int, now_ms: int, *, exclude_id: int = 0) -> bool:
        for inv in self._invites.values():
            if inv.duel_invite_id == exclude_id:
                continue
            if (
                inv.target_captain_id == target_captain_id
                and inv.state in (INVITE_STATE_PENDING_TARGET, INVITE_STATE_AWAITING_TARGET_TEAM)
                and inv.expires_ms > now_ms
            ):
                return True
        return False

    # -- 548 CreateJianghuDuelInvite ----------------------------------------------------------------

    async def create_invite(self, caller: CallerIdentity, request: Message) -> Message:
        """548 发起天墉生死状战书（1V1 直接推送目标；2V2–5V5 先发起本队 30s 确认）。"""
        resp = self.protocol.message("match.CreateJianghuDuelInviteResponse")
        if not self.rule.enabled:
            return set_tip(resp, ERR_JIANGHU_DUEL_DISABLED, "天墉生死状暂未开放")
        target_id = int(request.target_player_id)
        if target_id <= 0 or target_id == caller.player_id:
            return set_tip(resp, ERR_MATCH_CHALLENGE_SELF, "不能向自己下达战书")
        writ_tier = int(request.writ_tier)
        if writ_tier not in (WRIT_TIER_SILVER, WRIT_TIER_GOLD_ITEM):
            return set_tip(resp, ERR_JIANGHU_INSUFFICIENT_WRIT_OR_SILVER, "战书档位无效")

        # 1. 按权威队伍核验双侧成员
        side_a, err_code, err_msg = await self._resolve_authoritative_side(
            caller.player_id, request.challenger_member_ids
        )
        if err_code != 0:
            return set_tip(resp, err_code, err_msg)
        side_b, err_code, err_msg = await self._resolve_authoritative_side(target_id, request.target_member_ids)
        if err_code != 0:
            return set_tip(resp, err_code, err_msg)
        if not 1 <= len(side_a) <= MAX_TEAM_SIZE or len(side_a) != len(side_b):
            return set_tip(resp, ERR_JIANGHU_TEAM_SIZE_MISMATCH, "双方出战人数须一致且在1至5人内")
        if set(side_a) & set(side_b):
            return set_tip(resp, ERR_JIANGHU_TEAM_SIZE_MISMATCH, "双方队员存在重复，无法立生死状")

        # 2. 全员在线与空闲核验
        code, msg = await self._check_players_available(side_a, side_b)
        if code != 0:
            return set_tip(resp, code, msg)

        # 3. 批量拉黑与好友关系核验
        all_are_friends = False
        if self.social_checker is not None:
            try:
                has_block, all_are_friends = await self.social_checker(side_a, side_b)
            except Exception as exc:
                logger.error("jianghu social check failed caller=%d target=%d err=%s", caller.player_id, target_id, exc)
                return set_tip(resp, ERR_JIANGHU_SERVICE_BUSY, "社交关系校验暂不可用")
            if has_block:
                return set_tip(resp, ERR_JIANGHU_BLOCKED_BY_PEER, "双方队伍存在拉黑关系，无法下达战书")

        # 4. 闭关免战开关与资费预检（只读不扣费）
        if self.eligibility_checker is not None:
            code, msg = await self.eligibility_checker(
                side_a, side_b, caller.player_id, writ_tier, self.rule, all_are_friends
            )
            if code != 0:
                return set_tip(resp, code, msg)

        # 5. 解析展示名与纯同帮小队归属
        names = await self._resolve_names((*side_a, *side_b))
        side_a_gid, side_a_gname = await self._resolve_pure_guild(side_a)
        side_b_gid, side_b_gname = await self._resolve_pure_guild(side_b)

        now_ms = self._now()
        day_key = jianghu_game_day_key(now_ms)
        async with self._lock:
            cd_until = self._same_target_cooldown.get((caller.player_id, target_id), 0)
            if cd_until > now_ms:
                return set_tip(resp, ERR_JIANGHU_SAME_TARGET_COOLDOWN, "对同一道友下达战书间隔不足5分钟")
            stored_day, daily_cnt = self._daily_unmatched.get(caller.player_id, (0, 0))
            if stored_day == day_key and self.rule.daily_unmatched_limit > 0 and daily_cnt >= self.rule.daily_unmatched_limit:
                return set_tip(resp, ERR_JIANGHU_DAILY_INVITE_LIMIT_REACHED, "今日未成局战书邀请已达上限")
            if self._target_has_pending_locked(target_id, now_ms):
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "对方已有待处理战书")

            self._next_invite_id += 1
            invite_id = self._next_invite_id
            if len(side_a) > 1:
                state = INVITE_STATE_AWAITING_CHALLENGER_TEAM
                confirm_deadline_ms = now_ms + self.rule.team_confirm_ms
                expires_ms = confirm_deadline_ms + self.rule.invite_ttl_ms
            else:
                state = INVITE_STATE_PENDING_TARGET
                confirm_deadline_ms = 0
                expires_ms = now_ms + self.rule.invite_ttl_ms

            inv = _DuelInviteState(
                duel_invite_id=invite_id,
                writ_tier=writ_tier,
                team_size=len(side_a),
                state=state,
                challenger_captain_id=caller.player_id,
                target_captain_id=target_id,
                challenger_account=caller.account,
                challenger_revision=caller.owner_revision,
                target_account="",
                target_revision=0,
                side_a_guild_id=side_a_gid,
                side_a_guild_name=side_a_gname,
                side_b_guild_id=side_b_gid,
                side_b_guild_name=side_b_gname,
                challenger_members=[
                    _DuelMemberState(player_id=pid, name=names[pid], confirmed=(pid == caller.player_id))
                    for pid in side_a
                ],
                target_members=[_DuelMemberState(player_id=pid, name=names[pid], confirmed=False) for pid in side_b],
                created_ms=now_ms,
                expires_ms=expires_ms,
                confirm_deadline_ms=confirm_deadline_ms,
            )
            self._invites[invite_id] = inv
            self._same_target_cooldown[(caller.player_id, target_id)] = now_ms + self.rule.same_target_cooldown_ms
            next_cnt = (daily_cnt + 1) if stored_day == day_key else 1
            self._daily_unmatched[caller.player_id] = (day_key, next_cnt)
            view = self._build_view(inv, now_ms)

        if state == INVITE_STATE_PENDING_TARGET:
            await self.push_invite(target_id, view)
        else:
            for pid in side_a:
                if pid != caller.player_id:
                    await self.push_invite(pid, view)

        resp.invite.CopyFrom(view)
        return resp

    # -- 549 ConfirmJianghuDuelMember ---------------------------------------------------------------

    async def confirm_member(self, caller: CallerIdentity, request: Message) -> Message:
        """549 小队队员确认或退出天墉生死状；全员同意后流转至目标应战或直接成局。"""
        resp = self.protocol.message("match.ConfirmJianghuDuelMemberResponse")
        if not self.rule.enabled:
            return set_tip(resp, ERR_JIANGHU_DUEL_DISABLED, "天墉生死状暂未开放")
        invite_id = int(request.duel_invite_id)
        agree = bool(request.agree)
        now_ms = self._now()

        # 先在锁内检查基本资格与超时，避免持锁期间做异步存储查询
        async with self._lock:
            inv = self._invites.get(invite_id)
            if inv is None:
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "战书不存在或已失效")
            if inv.state not in (INVITE_STATE_AWAITING_CHALLENGER_TEAM, INVITE_STATE_AWAITING_TARGET_TEAM):
                resp.invite.CopyFrom(self._build_view(inv, now_ms))
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "该战书已不在小队确认阶段")
            active_side = (
                inv.challenger_members
                if inv.state == INVITE_STATE_AWAITING_CHALLENGER_TEAM
                else inv.target_members
            )
            active_captain = (
                inv.challenger_captain_id
                if inv.state == INVITE_STATE_AWAITING_CHALLENGER_TEAM
                else inv.target_captain_id
            )
            active_ids = [m.player_id for m in active_side]
            challenger_ids = [m.player_id for m in inv.challenger_members]
            check_challenger_too = inv.state == INVITE_STATE_AWAITING_TARGET_TEAM

        roster_ok = True
        challenger_roster_ok = True
        if agree:
            roster_ok = await self._verify_roster_unchanged(active_captain, active_ids)
            if roster_ok and check_challenger_too:
                challenger_roster_ok = await self._verify_roster_unchanged(
                    inv.challenger_captain_id, challenger_ids
                )

        tip_code = 0
        tip_msg = ""
        push_reason = ""
        push_all = False
        push_target_cap = False
        push_side: list[_DuelMemberState] = []
        ready_to_finalize = False
        expected_state = 0

        async with self._lock:
            inv = self._invites.get(invite_id)
            if inv is None:
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "战书不存在或已失效")
            if inv.state not in (INVITE_STATE_AWAITING_CHALLENGER_TEAM, INVITE_STATE_AWAITING_TARGET_TEAM):
                resp.invite.CopyFrom(self._build_view(inv, now_ms))
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "该战书已不在小队确认阶段")
            challengers_snap = list(inv.challenger_members)
            targets_snap = list(inv.target_members)
            if inv.confirm_deadline_ms > 0 and now_ms > inv.confirm_deadline_ms:
                inv.state = INVITE_STATE_ABORTED
                tip_code, tip_msg = ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "小队确认已超时作废"
                push_all, push_reason = True, "expired"
            else:
                active_side = (
                    inv.challenger_members
                    if inv.state == INVITE_STATE_AWAITING_CHALLENGER_TEAM
                    else inv.target_members
                )
                member = next((m for m in active_side if m.player_id == caller.player_id), None)
                if member is None:
                    tip_code, tip_msg = ERR_JIANGHU_NOT_INVITE_PARTICIPANT, "您不是当前待确认小队的成员"
                elif not agree:
                    inv.state = INVITE_STATE_ABORTED
                    push_all, push_reason = True, "member_declined"
                elif not roster_ok:
                    inv.state = INVITE_STATE_ABORTED
                    tip_code, tip_msg = ERR_JIANGHU_TEAM_CHANGED_ABORTED, "小队成员已变动，战书已自动作废"
                    push_all, push_reason = True, "team_changed"
                else:
                    member.confirmed = True
                    if not all(m.confirmed for m in active_side):
                        push_side, push_reason = list(active_side), "member_confirmed"
                    elif inv.state == INVITE_STATE_AWAITING_CHALLENGER_TEAM:
                        if self._target_has_pending_locked(inv.target_captain_id, now_ms, exclude_id=invite_id):
                            inv.state = INVITE_STATE_ABORTED
                            tip_code, tip_msg = ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "对方当前已有待处理战书"
                            push_all, push_reason = True, "aborted"
                        else:
                            inv.state = INVITE_STATE_PENDING_TARGET
                            inv.confirm_deadline_ms = 0
                            inv.expires_ms = now_ms + self.rule.invite_ttl_ms
                            push_target_cap = True
                            push_side, push_reason = list(inv.challenger_members), "sent_to_target"
                    else:
                        if not challenger_roster_ok:
                            inv.state = INVITE_STATE_ABORTED
                            tip_code, tip_msg = ERR_JIANGHU_TEAM_CHANGED_ABORTED, "发起方小队成员已变动，战书已自动作废"
                            push_all, push_reason = True, "team_changed"
                        else:
                            ready_to_finalize = True
                            expected_state = inv.state
            view = self._build_view(inv, now_ms)
            target_cap_id = inv.target_captain_id

        if push_all:
            await self.push_status_to_all(challengers_snap, targets_snap, view, push_reason)
        else:
            if push_target_cap:
                await self.push_invite(target_cap_id, view)
            if push_side:
                await self.push_status_to_members(push_side, view, push_reason)

        if tip_code != 0:
            resp.invite.CopyFrom(view)
            return set_tip(resp, tip_code, tip_msg)
        if ready_to_finalize:
            return await self._finalize_match_from_invite(
                invite_id, expected_state, now_ms, "match.ConfirmJianghuDuelMemberResponse"
            )
        resp.invite.CopyFrom(view)
        return resp

    # -- 550 RespondJianghuDuelInvite ---------------------------------------------------------------

    async def respond_invite(self, caller: CallerIdentity, request: Message) -> Message:
        """550 目标队长应战或拒战（1V1 直接成局；2V2–5V5 触发目标队 30s 全员确认）。"""
        resp = self.protocol.message("match.RespondJianghuDuelInviteResponse")
        if not self.rule.enabled:
            return set_tip(resp, ERR_JIANGHU_DUEL_DISABLED, "天墉生死状暂未开放")
        invite_id = int(request.duel_invite_id)
        accept = bool(request.accept)
        now_ms = self._now()

        async with self._lock:
            inv = self._invites.get(invite_id)
            if inv is None:
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "战书不存在或已失效")
            if inv.target_captain_id != caller.player_id:
                resp.invite.CopyFrom(self._build_view(inv, now_ms))
                return set_tip(resp, ERR_JIANGHU_NOT_INVITE_PARTICIPANT, "该战书不是发给您的")
            ch_cap = inv.challenger_captain_id
            tg_cap = inv.target_captain_id
            ch_ids = [m.player_id for m in inv.challenger_members]
            tg_ids = [m.player_id for m in inv.target_members]

        rosters_ok = True
        if accept:
            rosters_ok = (await self._verify_roster_unchanged(ch_cap, ch_ids)) and (
                await self._verify_roster_unchanged(tg_cap, tg_ids)
            )

        tip_code = 0
        tip_msg = ""
        push_all = False
        push_reason = ""
        push_target_team = False
        ready_to_finalize = False

        async with self._lock:
            inv = self._invites.get(invite_id)
            if inv is None:
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "战书不存在或已失效")
            challengers_snap = list(inv.challenger_members)
            targets_snap = list(inv.target_members)
            if inv.target_captain_id != caller.player_id:
                tip_code, tip_msg = ERR_JIANGHU_NOT_INVITE_PARTICIPANT, "该战书不是发给您的"
            elif inv.state != INVITE_STATE_PENDING_TARGET:
                tip_code, tip_msg = ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "该战书已失效或已处理"
            elif inv.expires_ms > 0 and now_ms > inv.expires_ms:
                inv.state = INVITE_STATE_ABORTED
                tip_code, tip_msg = ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "该战书已过期"
                push_all, push_reason = True, "expired"
            elif not accept:
                inv.state = INVITE_STATE_DECLINED
                push_all, push_reason = True, "declined"
            elif not rosters_ok:
                inv.state = INVITE_STATE_ABORTED
                tip_code, tip_msg = ERR_JIANGHU_TEAM_CHANGED_ABORTED, "小队成员已变动，战书已自动作废"
                push_all, push_reason = True, "team_changed"
            else:
                inv.target_account = caller.account
                inv.target_revision = caller.owner_revision
                for m in inv.target_members:
                    if m.player_id == caller.player_id:
                        m.confirmed = True
                if inv.team_size > 1:
                    inv.state = INVITE_STATE_AWAITING_TARGET_TEAM
                    inv.confirm_deadline_ms = now_ms + self.rule.team_confirm_ms
                    inv.expires_ms = inv.confirm_deadline_ms
                    push_target_team = True
                else:
                    ready_to_finalize = True
            view = self._build_view(inv, now_ms)

        if push_all:
            await self.push_status_to_all(challengers_snap, targets_snap, view, push_reason)
        elif push_target_team:
            for m in targets_snap:
                if m.player_id != caller.player_id:
                    await self.push_invite(m.player_id, view)
            await self.push_status_to_members(challengers_snap, view, "awaiting_target_team")

        if tip_code != 0:
            resp.invite.CopyFrom(view)
            return set_tip(resp, tip_code, tip_msg)
        if ready_to_finalize:
            return await self._finalize_match_from_invite(
                invite_id, INVITE_STATE_PENDING_TARGET, now_ms, "match.RespondJianghuDuelInviteResponse"
            )
        resp.invite.CopyFrom(view)
        return resp

    async def _finalize_match_from_invite(
        self, invite_id: int, expected_state: int, now_ms: int, response_type: str
    ) -> Message:
        resp = self.protocol.message(response_type)
        async with self._lock:
            inv = self._invites.get(invite_id)
            if inv is None:
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "战书状态已变更")
            side_a = [m.player_id for m in inv.challenger_members]
            side_b = [m.player_id for m in inv.target_members]

        code, msg = await self._check_players_available(side_a, side_b)
        if code != 0:
            return set_tip(resp, code, msg)

        if self.social_checker is not None:
            try:
                has_block, _ = await self.social_checker(side_a, side_b)
            except Exception:
                return set_tip(resp, ERR_JIANGHU_SERVICE_BUSY, "社交关系校验暂不可用")
            if has_block:
                return set_tip(resp, ERR_JIANGHU_BLOCKED_BY_PEER, "双方存在拉黑关系，无法成局")

        # 原子抢占 MATCHED 终态
        async with self._lock:
            inv = self._invites.get(invite_id)
            if (
                inv is None
                or inv.state != expected_state
                or (inv.confirm_deadline_ms > 0 and now_ms > inv.confirm_deadline_ms)
                or (inv.expires_ms > 0 and now_ms > inv.expires_ms)
            ):
                return set_tip(resp, ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED, "战书状态已变更")
            inv.state = INVITE_STATE_MATCHED
            inv.confirm_deadline_ms = 0
            challengers_snap = list(inv.challenger_members)
            targets_snap = list(inv.target_members)
            ch_name = next((m.name for m in challengers_snap if m.player_id == inv.challenger_captain_id), f"道友{inv.challenger_captain_id}")
            tg_name = next((m.name for m in targets_snap if m.player_id == inv.target_captain_id), f"道友{inv.target_captain_id}")
            duel_ctx = self.protocol.message(
                "JianghuDuelBattleContext",
                duel_invite_id=inv.duel_invite_id,
                writ_tier=inv.writ_tier,
                challenger_captain_id=inv.challenger_captain_id,
                challenger_captain_name=ch_name,
                target_captain_id=inv.target_captain_id,
                target_captain_name=tg_name,
                side_a_guild_id=inv.side_a_guild_id,
                side_a_guild_name=inv.side_a_guild_name,
                side_b_guild_id=inv.side_b_guild_id,
                side_b_guild_name=inv.side_b_guild_name,
                round_limit_override=self.rule.duel_round_limit,
                team_size=inv.team_size,
            )
            inv_view_pre = self._build_view(inv, now_ms)

        try:
            if self.battle_launcher is not None:
                battle_id = int(await self.battle_launcher(inv_view_pre, side_a, side_b, duel_ctx))
            else:
                battle_id = now_ms
        except Exception as exc:
            logger.error("jianghu launch duel formation failed invite=%d err=%s", invite_id, exc)
            async with self._lock:
                inv = self._invites.get(invite_id)
                if inv is not None and inv.state == INVITE_STATE_MATCHED and inv.battle_id == 0:
                    inv.state = INVITE_STATE_ABORTED
                view_abort = self._build_view(inv, now_ms) if inv is not None else inv_view_pre
            await self.push_status_to_all(challengers_snap, targets_snap, view_abort, "aborted")
            resp.invite.CopyFrom(view_abort)
            return set_tip(resp, ERR_JIANGHU_SERVICE_BUSY, "成局启动失败，请稍后重试")

        day_key = jianghu_game_day_key(now_ms)
        async with self._lock:
            inv = self._invites[invite_id]
            inv.battle_id = battle_id
            # 成局不计入“每日未成局上限”，回补 1 次
            stored_day, daily_cnt = self._daily_unmatched.get(inv.challenger_captain_id, (0, 0))
            if stored_day == day_key and daily_cnt > 0:
                self._daily_unmatched[inv.challenger_captain_id] = (day_key, daily_cnt - 1)
            view_matched = self._build_view(inv, now_ms)

        await self.push_status_to_all(challengers_snap, targets_snap, view_matched, "matched")
        resp.invite.CopyFrom(view_matched)
        return resp

    # -- 551 ListJianghuGrudges / 552 DismissJianghuGrudge ------------------------------------------

    async def list_grudges(self, caller: CallerIdentity) -> Message:
        """551 返回本人当前持有人代次下的个人恩怨录（仅含布尔在线态）及在途生死状。"""
        now_ms = self._now()
        async with self._lock:
            rel_rows = [
                rel
                for (pid, rev, _), rel in self._grudges.items()
                if pid == caller.player_id and rev == caller.owner_revision and not rel.dismissed
            ]
            rel_rows.sort(key=lambda r: (r.last_battle_ms, r.peer_player_id), reverse=True)
            rel_rows = rel_rows[: self.rule.grudge_max_rivals]
            active_views = [
                self._build_view(inv, now_ms)
                for inv in sorted(self._invites.values(), key=lambda x: (x.created_ms, x.duel_invite_id), reverse=True)
                if inv.state in (
                    INVITE_STATE_AWAITING_CHALLENGER_TEAM,
                    INVITE_STATE_PENDING_TARGET,
                    INVITE_STATE_AWAITING_TARGET_TEAM,
                )
                and inv.expires_ms > now_ms
                and (
                    inv.challenger_captain_id == caller.player_id
                    or inv.target_captain_id == caller.player_id
                    or any(m.player_id == caller.player_id for m in inv.challenger_members)
                    or any(m.player_id == caller.player_id for m in inv.target_members)
                )
            ][:20]

        resp = self.protocol.message("match.ListJianghuGrudgesResponse", server_time_ms=now_ms)
        for rel in rel_rows:
            pres = await self.presence.get_presence(rel.peer_player_id)
            entry = resp.grudges.add(
                peer_player_id=rel.peer_player_id,
                peer_name=rel.peer_name,
                wins=rel.wins,
                losses=rel.losses,
                draws=rel.draws,
                win_streak=rel.win_streak,
                lose_streak=rel.lose_streak,
                last_battle_ms=rel.last_battle_ms,
                peer_online=bool(pres.online),
            )
            for log in rel.logs[: self.rule.grudge_max_logs_per_rival]:
                entry.recent_logs.add(
                    battle_id=log.battle_id,
                    outcome_for_self=log.outcome_for_self,
                    total_rounds=log.total_rounds,
                    writ_tier=log.writ_tier,
                    team_size=log.team_size,
                    finished_at_ms=log.finished_at_ms,
                )
        for av in active_views:
            resp.active_invites.add().CopyFrom(av)
        return resp

    async def dismiss_grudge(self, caller: CallerIdentity, request: Message) -> Message:
        """552 单向一笔勾销指定宿敌（若再交手则自动重新显现）。"""
        resp = self.protocol.message("match.DismissJianghuGrudgeResponse")
        peer_id = int(request.peer_player_id)
        if peer_id <= 0:
            return set_tip(resp, ERR_JIANGHU_GRUDGE_NOT_FOUND, "未找到该宿敌记录")
        async with self._lock:
            rel = self._grudges.get((caller.player_id, caller.owner_revision, peer_id))
            if rel is None or rel.dismissed:
                return set_tip(resp, ERR_JIANGHU_GRUDGE_NOT_FOUND, "未找到该宿敌记录")
            rel.dismissed = True
        return resp

    # -- 终局恩怨入账与连胜/终结世界播报 -------------------------------------------------------------

    async def apply_battle_result(
        self,
        event: Message,
        *,
        owner_revs: dict[int, int] | None = None,
    ) -> tuple[JianghuBattleGrudgeSummary, bool]:
        """幂等入账一场天墉生死状终局至个人恩怨录，并按规则发布战果与宿敌连胜/雪耻世界播报。"""
        if event is None or int(getattr(event, "battle_id", 0)) == 0:
            return JianghuBattleGrudgeSummary(), False
        if not event.HasField("jianghu_duel_context"):
            return JianghuBattleGrudgeSummary(), False
        duel = event.jianghu_duel_context
        cap_a = int(duel.challenger_captain_id)
        cap_b = int(duel.target_captain_id)
        if cap_a <= 0 or cap_b <= 0:
            return JianghuBattleGrudgeSummary(), False
        total_rounds = int(event.total_rounds)
        outcome = int(event.outcome)
        if total_rounds == 0 or outcome == _OUTCOME_VOID:
            return JianghuBattleGrudgeSummary(), False

        winner_id = 0
        loser_id = 0
        is_draw = False
        if outcome == _OUTCOME_SIDE_A_WIN:
            out_a, out_b = 1, 2
            winner_id, loser_id = cap_a, cap_b
        elif outcome == _OUTCOME_SIDE_B_WIN:
            out_a, out_b = 2, 1
            winner_id, loser_id = cap_b, cap_a
        elif outcome == _OUTCOME_DRAW:
            out_a, out_b = 3, 3
            is_draw = True
        else:
            return JianghuBattleGrudgeSummary(), False

        revs = owner_revs or {}
        rev_a = int(revs.get(cap_a, 1))
        rev_b = int(revs.get(cap_b, 1))
        battle_id = int(event.battle_id)
        finished_ms = int(event.finished_at_ms) or battle_id
        writ_tier = int(duel.writ_tier)
        team_size = int(duel.team_size) or 1

        winner_new_streak = 0
        loser_prev_streak = 0
        sides = (
            (cap_a, rev_a, cap_b, str(duel.target_captain_name) or f"道友{cap_b}", out_a),
            (cap_b, rev_b, cap_a, str(duel.challenger_captain_name) or f"道友{cap_a}", out_b),
        )
        async with self._lock:
            for pid, rev, peer_id, _, _ in sides:
                if (pid, rev, peer_id, battle_id) in self._grudge_seen:
                    return JianghuBattleGrudgeSummary(), False
            for pid, rev, peer_id, peer_name, out_self in sides:
                self._grudge_seen.add((pid, rev, peer_id, battle_id))
                key = (pid, rev, peer_id)
                rel = self._grudges.get(key)
                if rel is None:
                    rel = _GrudgeRelationState(peer_player_id=peer_id, peer_name=peer_name)
                    self._grudges[key] = rel
                else:
                    rel.peer_name = peer_name
                    rel.dismissed = False
                prev_win_streak = rel.win_streak
                if out_self == 1:
                    rel.wins += 1
                    rel.win_streak += 1
                    rel.lose_streak = 0
                    if pid == winner_id:
                        winner_new_streak = rel.win_streak
                elif out_self == 2:
                    rel.losses += 1
                    rel.win_streak = 0
                    rel.lose_streak += 1
                    if pid == loser_id:
                        loser_prev_streak = prev_win_streak
                else:
                    rel.draws += 1
                rel.last_battle_ms = max(rel.last_battle_ms, finished_ms)
                rel.logs.append(
                    _GrudgeLogState(
                        battle_id=battle_id,
                        outcome_for_self=out_self,
                        total_rounds=total_rounds,
                        writ_tier=writ_tier,
                        team_size=team_size,
                        finished_at_ms=finished_ms,
                    )
                )
                rel.logs.sort(key=lambda lg: (lg.finished_at_ms, lg.battle_id), reverse=True)
                if len(rel.logs) > self.rule.grudge_max_logs_per_rival:
                    del rel.logs[self.rule.grudge_max_logs_per_rival :]

                # 裁剪该角色当前代次宿敌总数至上限（50 位）
                owner_keys = [
                    (k, r)
                    for k, r in self._grudges.items()
                    if k[0] == pid and k[1] == rev
                ]
                if len(owner_keys) > self.rule.grudge_max_rivals:
                    owner_keys.sort(key=lambda kv: (kv[1].last_battle_ms, kv[1].peer_player_id), reverse=True)
                    for stale_key, _ in owner_keys[self.rule.grudge_max_rivals :]:
                        self._grudges.pop(stale_key, None)

        summary = JianghuBattleGrudgeSummary(
            winner_player_id=winner_id,
            loser_player_id=loser_id,
            winner_new_win_streak=winner_new_streak,
            loser_prev_win_streak=loser_prev_streak,
            is_draw=is_draw,
        )

        # 发布战果及宿敌连胜/雪耻系统播报
        name_a = str(duel.challenger_captain_name) or f"道友{cap_a}"
        name_b = str(duel.target_captain_name) or f"道友{cap_b}"
        if is_draw:
            await self.publish_world_notice(
                self.protocol.message(
                    "chatpb.PublishSystemWorldNoticeRequest",
                    notice_id=f"jh_duel:{battle_id}:end",
                    template_id=NOTICE_JIANGHU_DUEL_DRAW,
                    battle_id=battle_id,
                    actor_a_name=name_a,
                    actor_b_name=name_b,
                    total_rounds=total_rounds,
                    team_size=team_size,
                )
            )
            return summary, True

        winner_name, loser_name = (name_b, name_a) if outcome == _OUTCOME_SIDE_B_WIN else (name_a, name_b)
        end_tmpl = (
            NOTICE_JIANGHU_DUEL_WIN_GOLD if writ_tier == WRIT_TIER_GOLD_ITEM else NOTICE_JIANGHU_DUEL_WIN_SILVER
        )
        await self.publish_world_notice(
            self.protocol.message(
                "chatpb.PublishSystemWorldNoticeRequest",
                notice_id=f"jh_duel:{battle_id}:end",
                template_id=end_tmpl,
                battle_id=battle_id,
                actor_a_name=winner_name,
                actor_b_name=loser_name,
                total_rounds=total_rounds,
                team_size=team_size,
            )
        )
        if winner_new_streak >= self.rule.streak_highlight_threshold:
            await self.publish_world_notice(
                self.protocol.message(
                    "chatpb.PublishSystemWorldNoticeRequest",
                    notice_id=f"jh_duel:{battle_id}:streak",
                    template_id=NOTICE_JIANGHU_DUEL_STREAK_MILESTONE,
                    battle_id=battle_id,
                    actor_a_name=winner_name,
                    actor_b_name=loser_name,
                    streak_count=winner_new_streak,
                    team_size=team_size,
                )
            )
        if loser_prev_streak >= self.rule.streak_highlight_threshold:
            await self.publish_world_notice(
                self.protocol.message(
                    "chatpb.PublishSystemWorldNoticeRequest",
                    notice_id=f"jh_duel:{battle_id}:streak_break",
                    template_id=NOTICE_JIANGHU_DUEL_STREAK_BROKEN,
                    battle_id=battle_id,
                    actor_a_name=winner_name,
                    actor_b_name=loser_name,
                    streak_count=loser_prev_streak,
                    team_size=team_size,
                )
            )
        return summary, True
