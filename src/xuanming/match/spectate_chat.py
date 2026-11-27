"""局外人观战交流模式 V1（587 SendBattleSpectateChat / 588 PullBattleSpectateChat）。

对齐 C++/Go 仓 ``go/match/internal/spectatechat/hub.go``、``logic/spectate_chat_policy.go`` 与
``logic/spectate_chat_logic.go``：
- 开局冻结策略矩阵：显式覆盖全部 MatchMode，未知模式 fail-closed 禁止；关闭交流绝不影响正常观战可见性；
- 全房唯一公开室：按参战者集合由服务端权威标注 ``PARTICIPANT`` / ``SPECTATOR`` 角色；
- 自由文本强制经过 ContentGuard 审核（fail-closed：不可达拒发，敏感词拒发，辱骂打码后放行）；
- 单房有界 60 条环形队列、单人差异化冷却（参战者 2s / 观战者 4s）、单房每秒总流速上限（默认 8 条/秒）、
  终局关闭后保留消息供尾帧增量拉取并拒收新发言。
"""

from __future__ import annotations

import inspect
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from enum import IntEnum

from google.protobuf.message import DecodeError, Message

from ..content_guard.engine import Engine as ContentGuardEngine, SceneClass, Verdict as GuardVerdict
from ..protocol import Protocol
from .config import SpectateChatConfig
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_MATCH_BATTLE_NOT_WATCHABLE,
    ERR_MATCH_INTERNAL,
    ERR_MATCH_SPECTATE_CHAT_CHECK_UNAVAILABLE,
    ERR_MATCH_SPECTATE_CHAT_CONTENT_REJECTED,
    ERR_MATCH_SPECTATE_CHAT_COOLDOWN,
    ERR_MATCH_SPECTATE_CHAT_DISABLED,
    ERR_MATCH_SPECTATE_CHAT_ENDED,
    ERR_MATCH_SPECTATE_CHAT_ROOM_BUSY,
    set_tip,
)
from .model import (
    BATTLE_ACTIVITY_KIND_GUILD_WAR,
    MODE_1V1,
    MODE_3V3,
    MODE_5V5,
    MODE_GUILD_WAR_PVP,
    MODE_GUILD_WAR_SIEGE,
    MODE_JIANGHU_DUEL,
    MODE_POLARITY_5V5,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    MODE_PVP_CHALLENGE,
    MODE_PVP_TEAM,
    MODE_SEASON_TOURNAMENT,
    MODE_UNSPECIFIED,
    SPECTATE_VISIBILITY_PUBLIC,
    CallerIdentity,
    formation_outcome_decided,
)
from .store import MatchStore

logger = logging.getLogger(__name__)

# -- 常量与枚举（与 proto/match/match_service.proto & player_comp.proto 对齐）------------------------

RING_LIMIT = 60
DEFAULT_PULL_LIMIT = 20
MAX_SENDER_NAME_RUNES = 16
MEM_IDLE_TTL_MS = 2 * 3600 * 1000
MEM_COOLDOWN_PRUNE_THRESHOLD = 1024

# eSpectateChatPreference
SPECTATE_CHAT_PREF_UNSPECIFIED = 0
SPECTATE_CHAT_PREF_ALLOW = 1
SPECTATE_CHAT_PREF_FORBID = 2

# BattleSpectateChatRole
SPECTATE_CHAT_ROLE_UNSPECIFIED = 0
SPECTATE_CHAT_ROLE_PARTICIPANT = 1
SPECTATE_CHAT_ROLE_SPECTATOR = 2


class SpectateChatGuardVerdict(IntEnum):
    """内容安全审核裁决（对应 Go contentguard.Verdict）。"""

    PASS = 1
    MASKED = 2
    REJECT = 3
    UNAVAILABLE = 4


@dataclass(frozen=True, slots=True)
class SpectateChatGuardResult:
    """单次文本审核结果。"""

    verdict: SpectateChatGuardVerdict
    masked: str = ""


SpectateChatGuardFn = Callable[
    [int, str],
    SpectateChatGuardResult | Awaitable[SpectateChatGuardResult],
]
DisplayNameLookupFn = Callable[[int], Awaitable[str]]


def adapt_content_guard_engine(engine: ContentGuardEngine) -> SpectateChatGuardFn:
    """将内置 ``content_guard.Engine`` 适配为观战交流审核回调（CHAT 场景大类）。"""

    def _check(player_id: int, text: str) -> SpectateChatGuardResult:
        res = engine.check(SceneClass.CHAT, text)
        if res.verdict == GuardVerdict.PASS:
            return SpectateChatGuardResult(SpectateChatGuardVerdict.PASS)
        if res.verdict == GuardVerdict.MASKED:
            return SpectateChatGuardResult(SpectateChatGuardVerdict.MASKED, masked=res.masked)
        return SpectateChatGuardResult(SpectateChatGuardVerdict.REJECT)

    return _check


# -- 开局冻结策略矩阵（对齐 Go spectate_chat_policy.go）---------------------------------------------


class SpectateChatModeRule(IntEnum):
    """各 MatchMode 对应的观战交流裁决规则。"""

    FORBID = 0
    PVE_SOLO = 1
    PVE_TEAM = 2
    PVP_ALL_HUMANS = 3
    PVP_TEAM = 4
    TOURNAMENT = 5


_MODE_RULES: dict[int, SpectateChatModeRule] = {
    MODE_PVE_SOLO: SpectateChatModeRule.PVE_SOLO,
    MODE_PVE_TEAM: SpectateChatModeRule.PVE_TEAM,
    MODE_PVP_CHALLENGE: SpectateChatModeRule.PVP_ALL_HUMANS,
    MODE_JIANGHU_DUEL: SpectateChatModeRule.PVP_ALL_HUMANS,
    MODE_PVP_TEAM: SpectateChatModeRule.PVP_TEAM,
    MODE_GUILD_WAR_PVP: SpectateChatModeRule.PVP_TEAM,
    MODE_GUILD_WAR_SIEGE: SpectateChatModeRule.PVP_TEAM,
    MODE_SEASON_TOURNAMENT: SpectateChatModeRule.TOURNAMENT,
    MODE_1V1: SpectateChatModeRule.FORBID,
    MODE_3V3: SpectateChatModeRule.FORBID,
    MODE_5V5: SpectateChatModeRule.FORBID,
    MODE_POLARITY_5V5: SpectateChatModeRule.FORBID,
    MODE_UNSPECIFIED: SpectateChatModeRule.FORBID,
}


def spectate_chat_rule_of_mode(mode: int) -> tuple[SpectateChatModeRule, bool]:
    """为每个 MatchMode 显式表态；未登记模式返回 ``(FORBID, False)``（fail-closed）。"""
    rule = _MODE_RULES.get(int(mode))
    if rule is None:
        return SpectateChatModeRule.FORBID, False
    return rule, True


def resolve_player_spectate_chat_allow(pref: int, default_allow: bool) -> bool:
    """解析单个玩家的个人观战交流偏好。"""
    p = int(pref)
    if p == SPECTATE_CHAT_PREF_ALLOW:
        return True
    if p == SPECTATE_CHAT_PREF_FORBID:
        return False
    return bool(default_allow)


def _all_humans_allow(humans: Sequence[Message], default_allow: bool) -> bool:
    for snap in humans:
        if not resolve_player_spectate_chat_allow(int(getattr(snap, "spectate_chat_pref", 0)), default_allow):
            return False
    return True


def _team_captains_allow(humans: Sequence[Message], default_allow: bool) -> bool:
    """按队伍找显式队长；某队无显式队长标记时回退检查该队全部真人（fail-closed）。"""
    by_team: dict[int, list[Message]] = {}
    for snap in humans:
        by_team.setdefault(int(getattr(snap, "team_index", 0)), []).append(snap)
    for members in by_team.values():
        found_captain = False
        for snap in members:
            if bool(getattr(snap, "is_team_captain", False)):
                found_captain = True
                if not resolve_player_spectate_chat_allow(
                    int(getattr(snap, "spectate_chat_pref", 0)), default_allow
                ):
                    return False
        if not found_captain and not _all_humans_allow(members, default_allow):
            return False
    return True


def spectate_chat_enabled_for_battle(
    cfg: SpectateChatConfig,
    mode: int,
    visibility: int,
    activity: Message | None,
    players: Sequence[Message],
) -> bool:
    """在 Gather 建房时一次性裁决本场战斗是否开放「局外人观战交流」。"""
    if not cfg.is_enabled or int(visibility) != SPECTATE_VISIBILITY_PUBLIC:
        return False
    if activity is not None:
        kind = int(getattr(activity, "kind", 0))
        if kind != 0 and kind != BATTLE_ACTIVITY_KIND_GUILD_WAR:
            return False
    rule, decided = spectate_chat_rule_of_mode(mode)
    if not decided or rule == SpectateChatModeRule.FORBID:
        return False

    default_allow = cfg.default_allows_when_unset
    humans = [p for p in players if p is not None and not bool(getattr(p, "ai_controlled", False))]
    if not humans:
        return False

    if rule == SpectateChatModeRule.PVE_SOLO:
        return resolve_player_spectate_chat_allow(int(getattr(humans[0], "spectate_chat_pref", 0)), default_allow)
    if rule == SpectateChatModeRule.PVE_TEAM:
        for snap in humans:
            if bool(getattr(snap, "is_team_captain", False)):
                return resolve_player_spectate_chat_allow(
                    int(getattr(snap, "spectate_chat_pref", 0)), default_allow
                )
        return resolve_player_spectate_chat_allow(int(getattr(humans[0], "spectate_chat_pref", 0)), default_allow)
    if rule == SpectateChatModeRule.PVP_ALL_HUMANS:
        return _all_humans_allow(humans, default_allow)
    if rule == SpectateChatModeRule.PVP_TEAM:
        if cfg.team_pvp_captain_only:
            return _team_captains_allow(humans, default_allow)
        return _all_humans_allow(humans, default_allow)
    if rule == SpectateChatModeRule.TOURNAMENT:
        if not cfg.is_tournament_enabled:
            return False
        if cfg.team_pvp_captain_only:
            return _team_captains_allow(humans, default_allow)
        return _all_humans_allow(humans, default_allow)
    return False


# -- 有界环形缓冲与限流中枢（对齐 Go spectatechat/hub.go）-------------------------------------------


def sanitize_sender_name(raw: str, role: int) -> str:
    """截断并兜底发言者公开展示名（最多 16 个字符）。"""
    trimmed = (raw or "").strip()
    if not trimmed:
        return "参战道友" if int(role) == SPECTATE_CHAT_ROLE_PARTICIPANT else "观战道友"
    if len(trimmed) > MAX_SENDER_NAME_RUNES:
        return trimmed[:MAX_SENDER_NAME_RUNES]
    return trimmed


@dataclass(frozen=True, slots=True)
class SpectateChatMessageItem:
    """单条落定的观战交流不可变消息。"""

    seq: int
    sender_player_id: int
    sender_name: str
    role: int
    content: str
    created_ms: int


@dataclass(frozen=True, slots=True)
class SpectateChatSendResult:
    """单次发送尝试的原子裁决结果。"""

    message: SpectateChatMessageItem | None = None
    cooldown_until_ms: int = 0
    hit_cooldown: bool = False
    hit_room_busy: bool = False
    room_closed: bool = False


@dataclass(frozen=True, slots=True)
class SpectateChatPullResult:
    """增量拉取结果快照。"""

    messages: tuple[SpectateChatMessageItem, ...] = ()
    latest_seq: int = 0
    cooldown_until_ms: int = 0
    room_closed: bool = False


@dataclass(slots=True)
class _ChatRoom:
    next_seq: int = 0
    last_active_ms: int = 0
    closed: bool = False
    rate_sec: int = 0
    rate_count: int = 0
    messages: list[SpectateChatMessageItem] = field(default_factory=list)
    cooldowns: dict[int, int] = field(default_factory=dict)


class SpectateChatHub:
    """管理「局外人观战交流模式」的单房有界环形队列、个人冷却与单房每秒总流速上限。"""

    def __init__(self) -> None:
        self._rooms: dict[int, _ChatRoom] = {}

    def _evict_idle(self, now_ms: int) -> None:
        if len(self._rooms) < 64:
            return
        stale = [
            bid
            for bid, room in self._rooms.items()
            if now_ms > room.last_active_ms and now_ms - room.last_active_ms >= MEM_IDLE_TTL_MS
        ]
        for bid in stale:
            self._rooms.pop(bid, None)

    def _get_or_create(self, battle_id: int, now_ms: int) -> _ChatRoom:
        room = self._rooms.get(battle_id)
        if room is None:
            room = _ChatRoom(last_active_ms=now_ms)
            self._rooms[battle_id] = room
        return room

    def send(
        self,
        battle_id: int,
        player_id: int,
        sender_name: str,
        role: int,
        content: str,
        now_ms: int,
        cooldown_ms: int,
        room_rate_limit: int,
    ) -> SpectateChatSendResult:
        """原子校验封房状态、单人冷却与单房 1 秒流速并追加一条已通过审核的消息。"""
        limit = room_rate_limit if room_rate_limit > 0 else 8
        self._evict_idle(now_ms)
        room = self._get_or_create(int(battle_id), now_ms)
        if room.closed:
            return SpectateChatSendResult(room_closed=True)
        room.last_active_ms = now_ms

        prev_until = room.cooldowns.get(int(player_id), 0)
        if prev_until > now_ms:
            return SpectateChatSendResult(cooldown_until_ms=prev_until, hit_cooldown=True)

        now_sec = now_ms // 1000
        if room.rate_sec == now_sec:
            if room.rate_count >= limit:
                return SpectateChatSendResult(hit_room_busy=True)
            room.rate_count += 1
        else:
            room.rate_sec = now_sec
            room.rate_count = 1

        room.next_seq += 1
        item = SpectateChatMessageItem(
            seq=room.next_seq,
            sender_player_id=int(player_id),
            sender_name=sanitize_sender_name(sender_name, role),
            role=int(role),
            content=content,
            created_ms=now_ms,
        )
        room.messages.append(item)
        if len(room.messages) > RING_LIMIT:
            room.messages = room.messages[-RING_LIMIT:]

        next_until = now_ms + max(0, int(cooldown_ms))
        room.cooldowns[int(player_id)] = next_until
        if len(room.cooldowns) > MEM_COOLDOWN_PRUNE_THRESHOLD:
            room.cooldowns = {pid: u for pid, u in room.cooldowns.items() if u > now_ms}
        return SpectateChatSendResult(message=item, cooldown_until_ms=next_until)

    def pull(
        self,
        battle_id: int,
        player_id: int,
        after_seq: int,
        limit: int,
        now_ms: int,
    ) -> SpectateChatPullResult:
        """只读增量拉取指定战斗的观战交流消息（仅返回 ``seq > after_seq``）。"""
        capped = int(limit)
        if capped <= 0:
            capped = DEFAULT_PULL_LIMIT if after_seq == 0 else RING_LIMIT
        if capped > RING_LIMIT:
            capped = RING_LIMIT

        room = self._rooms.get(int(battle_id))
        if room is None:
            return SpectateChatPullResult()

        cd_until = room.cooldowns.get(int(player_id), 0)
        if cd_until <= now_ms:
            cd_until = 0
        filtered = [m for m in room.messages if m.seq > after_seq]
        if len(filtered) > capped:
            filtered = filtered[-capped:]
        return SpectateChatPullResult(
            messages=tuple(filtered),
            latest_seq=room.next_seq,
            cooldown_until_ms=cd_until,
            room_closed=room.closed,
        )

    def mark_ended(self, battle_id: int) -> None:
        """在战斗终局或回收时关闭该场观战交流室（拒收新发言，保留尾帧供拉取）。"""
        bid = int(battle_id)
        if bid <= 0:
            return
        room = self._rooms.get(bid)
        if room is not None:
            room.closed = True


# -- 房间事实加载与 587/588 业务逻辑（对齐 Go spectate_chat_logic.go）--------------------------------


@dataclass(frozen=True, slots=True)
class SpectateChatRoomInfo:
    """从权威成局原件提取的不可变观战交流室事实。"""

    battle_id: int
    visibility: int
    chat_enabled: bool
    ended: bool
    deadline_ms: int
    participants: frozenset[int]


RoomLoaderFn = Callable[[int, int], Awaitable[SpectateChatRoomInfo | None]]


def _is_valid_utf8_text(text: str) -> bool:
    try:
        text.encode("utf-8")
        return True
    except UnicodeEncodeError:
        return False


class SpectateChatManager:
    """承载 587 SendBattleSpectateChat 与 588 PullBattleSpectateChat 权威业务逻辑。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        config: SpectateChatConfig | None = None,
        *,
        hub: SpectateChatHub | None = None,
        content_guard: SpectateChatGuardFn | None = None,
        display_name_lookup: DisplayNameLookupFn | None = None,
        room_loader: RoomLoaderFn | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config if config is not None else SpectateChatConfig()
        self.hub = hub if hub is not None else SpectateChatHub()
        self.content_guard = content_guard
        self.display_name_lookup = display_name_lookup
        self.room_loader = room_loader
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    def _now_ms(self) -> int:
        return int(self._clock_ms())

    def mark_battle_ended(self, battle_id: int) -> None:
        """终局或回收时立即封房。"""
        self.hub.mark_ended(battle_id)

    async def _load_room(self, battle_id: int, now_ms: int) -> SpectateChatRoomInfo | None:
        if self.room_loader is not None:
            return await self.room_loader(battle_id, now_ms)
        formation = await self.store.get_formation(battle_id)
        if formation is None or not formation.create_request:
            return None
        try:
            create_req = self.protocol.parse("CreateBattleRequest", bytes(formation.create_request))
        except DecodeError:
            return None
        participants = frozenset(
            int(p.player_id)
            for p in create_req.players
            if not bool(getattr(p, "ai_controlled", False)) and int(p.player_id) != 0
        )
        deadline_ms = int(getattr(formation, "deadline_ms", 0))
        ended = formation_outcome_decided(formation) or (deadline_ms > 0 and now_ms > deadline_ms)
        return SpectateChatRoomInfo(
            battle_id=int(formation.battle_id),
            visibility=int(getattr(create_req, "spectate_visibility", 0)),
            chat_enabled=bool(getattr(create_req, "spectate_chat_enabled", False)),
            ended=ended,
            deadline_ms=deadline_ms,
            participants=participants,
        )

    async def _check_content(self, player_id: int, text: str) -> SpectateChatGuardResult:
        if self.content_guard is None:
            return SpectateChatGuardResult(SpectateChatGuardVerdict.PASS)
        try:
            res = self.content_guard(player_id, text)
            if inspect.isawaitable(res):
                res = await res
            if isinstance(res, SpectateChatGuardResult):
                return res
            return SpectateChatGuardResult(SpectateChatGuardVerdict.UNAVAILABLE)
        except Exception:
            logger.warning("match spectate_chat content_guard check failed player_id=%d", player_id, exc_info=True)
            return SpectateChatGuardResult(SpectateChatGuardVerdict.UNAVAILABLE)

    def _fill_message_proto(self, dst: Message, item: SpectateChatMessageItem) -> None:
        dst.seq = item.seq
        dst.sender_player_id = item.sender_player_id
        dst.sender_name = item.sender_name
        dst.role = item.role
        dst.content = item.content
        dst.created_ms = item.created_ms

    async def send_chat(self, caller: CallerIdentity, request: Message) -> Message:
        """587 SendBattleSpectateChat：校验开关/终局/文本审核/冷却/流速并落盘到有界环形队列。"""
        now_ms = self._now_ms()
        battle_id = int(getattr(request, "battle_id", 0))
        resp = self.protocol.message(
            "match.SendBattleSpectateChatResponse",
            battle_id=battle_id,
            server_time_ms=now_ms,
        )
        if caller.player_id <= 0:
            return set_tip(resp, ERR_MATCH_INTERNAL)

        cfg = self.config
        raw_content = str(getattr(request, "content", ""))
        trimmed = raw_content.strip()
        if (
            battle_id <= 0
            or not trimmed
            or not _is_valid_utf8_text(trimmed)
            or len(trimmed.encode("utf-8")) > cfg.effective_max_content_bytes
        ):
            return set_tip(resp, ERR_INVALID_PARAMETER)

        if not cfg.is_enabled:
            resp.enabled = False
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_DISABLED)

        try:
            room = await self._load_room(battle_id, now_ms)
        except Exception:
            logger.error("match spectate_chat load formation failed battle_id=%d", battle_id, exc_info=True)
            return set_tip(resp, ERR_MATCH_INTERNAL)

        if room is None:
            resp.battle_ended = True
            return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)

        if room.visibility != SPECTATE_VISIBILITY_PUBLIC or not room.chat_enabled:
            resp.enabled = False
            resp.battle_ended = room.ended
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_DISABLED)

        if room.ended:
            resp.enabled = True
            resp.battle_ended = True
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_ENDED)

        # 自由文本强制走 ContentGuard 审核（fail-closed）
        final_content = trimmed
        guard_res = await self._check_content(caller.player_id, trimmed)
        if guard_res.verdict == SpectateChatGuardVerdict.PASS:
            pass
        elif guard_res.verdict == SpectateChatGuardVerdict.MASKED:
            masked = guard_res.masked
            if (
                not masked.strip()
                or not _is_valid_utf8_text(masked)
                or len(masked.encode("utf-8")) > cfg.effective_max_content_bytes
            ):
                resp.enabled = True
                return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_CONTENT_REJECTED)
            final_content = masked
        elif guard_res.verdict == SpectateChatGuardVerdict.UNAVAILABLE:
            resp.enabled = True
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_CHECK_UNAVAILABLE)
        else:
            resp.enabled = True
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_CONTENT_REJECTED)

        if caller.player_id in room.participants:
            role = SPECTATE_CHAT_ROLE_PARTICIPANT
            cooldown_ms = cfg.effective_participant_cooldown_ms
        else:
            role = SPECTATE_CHAT_ROLE_SPECTATOR
            cooldown_ms = cfg.effective_spectator_cooldown_ms

        sender_name = ""
        if self.display_name_lookup is not None:
            try:
                sender_name = await self.display_name_lookup(caller.player_id)
            except Exception:
                logger.warning(
                    "match spectate_chat lookup sender name failed player_id=%d", caller.player_id, exc_info=True
                )

        send_res = self.hub.send(
            battle_id,
            caller.player_id,
            sender_name,
            role,
            final_content,
            now_ms,
            cooldown_ms,
            cfg.effective_room_rate_limit_per_sec,
        )
        resp.enabled = True
        if send_res.room_closed:
            resp.battle_ended = True
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_ENDED)
        if send_res.hit_cooldown:
            resp.cooldown_until_ms = send_res.cooldown_until_ms
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_COOLDOWN)
        if send_res.hit_room_busy:
            return set_tip(resp, ERR_MATCH_SPECTATE_CHAT_ROOM_BUSY)

        resp.battle_ended = False
        resp.cooldown_until_ms = send_res.cooldown_until_ms
        if send_res.message is not None:
            self._fill_message_proto(resp.message, send_res.message)
        return resp

    async def pull_chat(self, caller: CallerIdentity, request: Message) -> Message:
        """588 PullBattleSpectateChat：按 ``seq > after_seq`` 增量拉取观战交流消息。"""
        now_ms = self._now_ms()
        battle_id = int(getattr(request, "battle_id", 0))
        resp = self.protocol.message(
            "match.PullBattleSpectateChatResponse",
            battle_id=battle_id,
            server_time_ms=now_ms,
        )
        if battle_id <= 0:
            return set_tip(resp, ERR_INVALID_PARAMETER)

        cfg = self.config
        if not cfg.is_enabled:
            resp.enabled = False
            return resp

        try:
            room = await self._load_room(battle_id, now_ms)
        except Exception:
            logger.error("match spectate_chat pull load formation failed battle_id=%d", battle_id, exc_info=True)
            return set_tip(resp, ERR_MATCH_INTERNAL)

        if room is None:
            resp.battle_ended = True
            return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)

        if room.visibility != SPECTATE_VISIBILITY_PUBLIC or not room.chat_enabled:
            resp.enabled = False
            resp.battle_ended = room.ended
            return resp

        pull_res = self.hub.pull(
            battle_id,
            caller.player_id,
            int(getattr(request, "after_seq", 0)),
            int(getattr(request, "limit", 0)),
            now_ms,
        )
        resp.enabled = True
        resp.battle_ended = bool(room.ended or pull_res.room_closed)
        resp.cooldown_until_ms = pull_res.cooldown_until_ms
        resp.latest_seq = pull_res.latest_seq
        for item in pull_res.messages:
            self._fill_message_proto(resp.messages.add(), item)
        return resp
