"""全服赛季战队锦标赛《群仙问道赛》V1–V4：规则目录、四阶段推进、BO3 五行灵脉、观战喝彩、队徽、热身约战、神机竞猜、季军战与 MVP。"""

from __future__ import annotations

import logging
import time
import unicodedata
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from google.protobuf.message import Message

from ..protocol import Protocol
from .errors import (
    ERR_MATCH_INTERNAL,
    ERR_MATCH_TOURNAMENT_ALREADY_CHEERED,
    ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD,
    ERR_MATCH_TOURNAMENT_EMBLEM_INVALID,
    ERR_MATCH_TOURNAMENT_INVITE_NOT_FOUND,
    ERR_MATCH_TOURNAMENT_LINEUP_INVALID,
    ERR_MATCH_TOURNAMENT_MATCH_NOT_READY,
    ERR_MATCH_TOURNAMENT_MEMBER_NOT_ENOUGH,
    ERR_MATCH_TOURNAMENT_NOT_CAPTAIN,
    ERR_MATCH_TOURNAMENT_NOT_OPEN,
    ERR_MATCH_TOURNAMENT_PREDICTION_CLOSED,
    ERR_MATCH_TOURNAMENT_PREDICTION_TARGET_INVALID,
    ERR_MATCH_TOURNAMENT_QUALIFIER_LIMIT,
    ERR_MATCH_TOURNAMENT_REWARD_UNAVAILABLE,
    ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_COOLDOWN,
    ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_INVALID,
    ERR_MATCH_TOURNAMENT_SQUAD_FULL,
    ERR_MATCH_TOURNAMENT_SQUAD_LOCKED,
    ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID,
    ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND,
    ERR_MATCH_TOURNAMENT_STAGE_CLOSED,
    ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE,
    set_tip,
)
from .model import CallerIdentity

logger = logging.getLogger(__name__)

# -- 协议枚举常量（与 proto/match/match_service.proto 严格对齐）----------------------------------------

STAGE_REGISTRATION = 1
STAGE_QUALIFIER = 2
STAGE_KNOCKOUT = 3
STAGE_SETTLED = 4

SQUAD_DRAFT = 1
SQUAD_REGISTERED = 2
SQUAD_ELIMINATED = 3
SQUAD_CHAMPION = 4
SQUAD_DISBANDED = 5

ROLE_CAPTAIN = 1
ROLE_STARTER = 2
ROLE_SUBSTITUTE = 3

PHASE_QUALIFIER = 1
PHASE_KNOCKOUT = 2
PHASE_WARMUP = 3

MATCH_WAITING_OPPONENT = 1
MATCH_READY = 2
MATCH_IN_BATTLE = 3
MATCH_FINISHED = 4
MATCH_BYE = 5
MATCH_FORFEIT = 6
MATCH_INTERMISSION = 7

INVITE_PENDING = 1
INVITE_ACCEPTED = 2
INVITE_DECLINED = 3
INVITE_EXPIRED = 4

REWARD_TIER_CHAMPION = 1
REWARD_TIER_RUNNER_UP = 2
REWARD_TIER_TOP4 = 3
REWARD_TIER_TOP8 = 4
REWARD_TIER_TOP16 = 5
REWARD_TIER_PARTICIPANT = 6
REWARD_TIER_CHEER = 7
REWARD_TIER_THIRD_PLACE = 8
REWARD_TIER_PREDICTION = 9

DELIVERY_CLAIMABLE = 1
DELIVERY_CLAIMED = 2
DELIVERY_ABANDONED = 3

OUTCOME_SIDE_A_WIN = 1
OUTCOME_SIDE_B_WIN = 2
OUTCOME_DRAW = 3

LEYLINE_UNSPECIFIED = 0
LEYLINE_METAL = 1
LEYLINE_WOOD = 2
LEYLINE_WATER = 3
LEYLINE_FIRE = 4
LEYLINE_EARTH = 5

PREDICTION_PENDING = 0
PREDICTION_HIT = 1
PREDICTION_MISS = 2

# V4 模块 J：战队荣耀铭牌单调位图（与 Go internal/tournament/rules.go 逐位一致）
HONOR_BIT_DEBUT = 1 << 0
HONOR_BIT_STREAK = 1 << 1
HONOR_BIT_UNBEATEN = 1 << 2
HONOR_BIT_COMEBACK = 1 << 3
HONOR_BIT_SWEEP = 1 << 4
HONOR_BIT_CHAMPION = 1 << 5

MIN_SQUAD_NAME_CHARS = 2
MAX_SQUAD_NAME_CHARS = 12
MILLIS_PER_DAY = 86_400_000

MIN_SPECTATE_CHEER_PHRASE_ID = 1
MAX_SPECTATE_CHEER_PHRASE_ID = 6
SPECTATE_CHEER_COOLDOWN_MS = 5_000
SPECTATE_CHEER_RING_LIMIT = 32
SPECTATE_CHEER_INITIAL_RETURN = 12
SPECTATE_CHEER_ROOM_IDLE_TTL_MS = 2 * 3_600_000
MAX_CHEER_SENDER_NAME_CHARS = 16
DEFAULT_INTERMISSION_SEC = 60
MIN_SQUAD_EMBLEM_ID = 1
MAX_SQUAD_EMBLEM_ID = 8
DEFAULT_EMBLEM_ID = 1

_MASK64 = (1 << 64) - 1


# -- 纯规则辅助（与 Go internal/tournament/rules.go 一致）---------------------------------------------


def normalize_squad_name(raw: str) -> tuple[str, str, bool]:
    """校验并归一化战队名：去首尾空白后长度 2..12，禁控制字符与替换符，返回 (display, norm, ok)。"""
    trimmed = raw.strip()
    if not MIN_SQUAD_NAME_CHARS <= len(trimmed) <= MAX_SQUAD_NAME_CHARS:
        return "", "", False
    norm_chars: list[str] = []
    for ch in trimmed:
        cat = unicodedata.category(ch)
        if cat.startswith("C") or ch == "\ufffd":
            return "", "", False
        if ch.isspace():
            continue
        norm_chars.append(ch.lower())
    norm = "".join(norm_chars)
    if len(norm) < MIN_SQUAD_NAME_CHARS:
        return "", "", False
    return trimmed, norm, True


def knockout_bracket_size(qualified_count: int, max_teams: int) -> int:
    """按海选有效战队数动态计算单败淘汰赛规模（0 / 2 / 4 / 8 / 16）。"""
    if qualified_count < 2:
        return 0
    cap_limit = min(max(int(max_teams), 2), 16)
    effective = min(qualified_count, cap_limit)
    size = 2
    while size * 2 <= effective:
        size *= 2
    if effective > size and size * 2 <= cap_limit:
        size *= 2
    return size


def total_knockout_rounds(bracket_size: int) -> int:
    """返回淘汰赛总轮数（2->1, 4->2, 8->3, 16->4）。"""
    rounds = 0
    size = bracket_size
    while size >= 2:
        rounds += 1
        size //= 2
    return rounds


def seed_pairs(bracket_size: int) -> tuple[tuple[int, int], ...]:
    """标准单败淘汰赛首轮对阵种子对（1-based），确保 1/2 号种子分守上下半区。"""
    if bracket_size == 2:
        return ((1, 2),)
    if bracket_size == 4:
        return ((1, 4), (2, 3))
    if bracket_size == 8:
        return ((1, 8), (4, 5), (2, 7), (3, 6))
    if bracket_size == 16:
        return (
            (1, 16), (8, 9), (4, 13), (5, 12),
            (2, 15), (7, 10), (3, 14), (6, 11),
        )
    return ()


def rank_for_eliminated_round(bracket_size: int, round_index: int) -> int:
    """计算在淘汰赛第 round_index（1-based）轮落败的战队所获最终名次下界。"""
    total = total_knockout_rounds(bracket_size)
    if total == 0 or not 1 <= round_index <= total:
        return 0
    remaining = bracket_size
    for _ in range(1, round_index):
        remaining //= 2
    return remaining // 2 + 1


@dataclass(frozen=True, slots=True)
class KnockoutTieMetrics:
    """淘汰赛打满回合判平时的四级确定性决胜事实。"""

    squad_id: int
    seed_rank: int = 0
    surviving_units: int = 0
    remaining_hp_bps_sum: int = 0


def resolve_knockout_draw_winner(side_a: KnockoutTieMetrics, side_b: KnockoutTieMetrics) -> int:
    """淘汰赛平局确定性决胜：存活单位多 -> 剩余血量万分比和 -> 海选种子高 -> SquadID 小；0=A 胜，1=B 胜。"""
    if side_a.surviving_units != side_b.surviving_units:
        return 0 if side_a.surviving_units > side_b.surviving_units else 1
    if side_a.remaining_hp_bps_sum != side_b.remaining_hp_bps_sum:
        return 0 if side_a.remaining_hp_bps_sum > side_b.remaining_hp_bps_sum else 1
    if side_a.seed_rank != side_b.seed_rank and side_a.seed_rank > 0 and side_b.seed_rank > 0:
        return 0 if side_a.seed_rank < side_b.seed_rank else 1
    return 0 if side_a.squad_id <= side_b.squad_id else 1


def best_of_for_knockout(bracket_size: int, round_index: int, bo3_top4_enabled: bool) -> int:
    """判定淘汰赛某一轮是否为 BO3（半决赛与总决赛，即该轮参赛队数 remaining <= 4）。"""
    if not bo3_top4_enabled or bracket_size < 2 or round_index < 1:
        return 1
    remaining = bracket_size >> (round_index - 1)
    if 2 <= remaining <= 4:
        return 3
    return 1


def _splitmix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & _MASK64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & _MASK64
    return (x ^ (x >> 31)) & _MASK64


def is_valid_leyline_element(element: int) -> bool:
    """校验五行灵脉枚举是否为 1..5（金/木/水/火/土）。"""
    return LEYLINE_METAL <= int(element) <= LEYLINE_EARTH


def draw_leylines_with_bans(
    season_id: int,
    match_id: int,
    best_of: int,
    banned_a: int = 0,
    banned_b: int = 0,
) -> tuple[int, ...]:
    """V4 模块 H：从五行灵脉池剔除双方队长禁选后再以 SplitMix64 确定性无放回抽取 best_of 条灵脉。"""
    if best_of <= 1:
        return ()
    all_elems = [LEYLINE_METAL, LEYLINE_WOOD, LEYLINE_WATER, LEYLINE_FIRE, LEYLINE_EARTH]
    ba = int(banned_a)
    bb = int(banned_b)
    pool = [
        e for e in all_elems
        if not ((is_valid_leyline_element(ba) and e == ba) or (is_valid_leyline_element(bb) and e == bb))
    ]
    if not pool:
        pool = list(all_elems)
    count = min(int(best_of), len(pool))
    state = (((int(season_id) & _MASK64) << 32) ^ (int(match_id) & _MASK64)) & _MASK64
    out: list[int] = []
    for i in range(count):
        state = _splitmix64((state + i + 1) & _MASK64)
        rem = len(pool) - i
        pick = i + int(state % rem)
        pool[i], pool[pick] = pool[pick], pool[i]
        out.append(pool[i])
    return tuple(out)


def draw_leylines(season_id: int, match_id: int, best_of: int) -> tuple[int, ...]:
    """为一场 BO3 淘汰赛确定性无放回抽取五行灵脉顺序（与 Go/Java 逐字节同源）。"""
    return draw_leylines_with_bans(season_id, match_id, best_of, 0, 0)


def compute_qualifier_honor_bits(
    existing: int,
    played: int,
    wins: int,
    losses: int,
    min_participation: int,
) -> int:
    """V4 模块 J：海选每场结束后单调累加战队荣耀铭牌（问道初成/连胜破竹/不败金身）。"""
    bits = int(existing)
    req = min_participation if min_participation > 0 else 2
    if played >= req:
        bits |= HONOR_BIT_DEBUT
    if wins >= 3:
        bits |= HONOR_BIT_STREAK
    if played >= 3 and losses == 0:
        bits |= HONOR_BIT_UNBEATEN
    return bits


def compute_knockout_series_honor_bits(
    existing: int,
    best_of: int,
    winner_wins: int,
    loser_wins: int,
    is_champion: bool,
) -> int:
    """V4 模块 J：淘汰赛大场决胜后单调累加胜方战队荣耀铭牌（逆转乾坤/零封强敌/问鼎群仙）。"""
    bits = int(existing)
    if best_of >= 3:
        if winner_wins + loser_wins >= 3:
            bits |= HONOR_BIT_COMEBACK
        if winner_wins >= 2 and loser_wins == 0:
            bits |= HONOR_BIT_SWEEP
    if is_champion:
        bits |= HONOR_BIT_CHAMPION
    return bits


def classify_highlight_tag(
    is_third_place: bool,
    round_index: int,
    max_round: int,
    best_of: int,
    wins_a: int,
    wins_b: int,
) -> tuple[int, str, int]:
    """V4 模块 I：为已完赛的公开淘汰赛判定名局录标签与确定性排序优先级（0 最高）。"""
    if not is_third_place and max_round > 0 and round_index >= max_round:
        return 1, "总决赛巅峰局", 0
    if best_of >= 3 and wins_a + wins_b >= 3:
        return 2, "决胜三局激战", 1
    if is_third_place:
        return 3, "季军争夺战", 2
    return 4, "高光问鼎局", 3


def encode_leyline_sequence(seq: Iterable[int]) -> str:
    """将灵脉枚举序列编码为逗号分隔整数串（如 '1,4,2'）。"""
    return ",".join(str(int(e)) for e in seq if 1 <= int(e) <= 5)


def decode_leyline_sequence(raw: str) -> tuple[int, ...]:
    """解析逗号分隔的灵脉序列。"""
    trimmed = raw.strip()
    if not trimmed:
        return ()
    out: list[int] = []
    for part in trimmed.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            n = int(p)
        except ValueError:
            continue
        if 1 <= n <= 5:
            out.append(n)
    return tuple(out)


def leyline_for_game(seq: tuple[int, ...] | list[int], game_index: int) -> int:
    """返回 1-based 小局对应的灵脉；不存在返回 0（UNSPECIFIED）。"""
    if game_index <= 0 or game_index > len(seq):
        return LEYLINE_UNSPECIFIED
    return int(seq[game_index - 1])


def is_valid_spectate_cheer_phrase(phrase_id: int) -> bool:
    """校验观战预设喝彩白名单 ID（严格 1..6，零自由文本）。"""
    return MIN_SPECTATE_CHEER_PHRASE_ID <= int(phrase_id) <= MAX_SPECTATE_CHEER_PHRASE_ID


def is_valid_spectate_cheer_side(side: int) -> bool:
    """校验喝彩支持阵营（0=A 队，1=B 队）。"""
    return int(side) in (0, 1)


def is_valid_emblem_id(emblem_id: int) -> bool:
    """校验官方预设道家队徽白名单（1..8）。"""
    return MIN_SQUAD_EMBLEM_ID <= int(emblem_id) <= MAX_SQUAD_EMBLEM_ID


def normalize_emblem_id(emblem_id: int) -> tuple[int, bool]:
    """建队时将 0 归一化为默认 1 青剑；非 0 须落在 1..8。"""
    eid = int(emblem_id)
    if eid == 0:
        return DEFAULT_EMBLEM_ID, True
    if is_valid_emblem_id(eid):
        return eid, True
    return 0, False


def prediction_weight_for_round(bracket_size: int, round_index: int, is_third_place: bool) -> int:
    """返回一场淘汰赛命中神机预测对应的慧眼分权重：十六强=1, 八强=2, 半决赛=3, 季军战=3, 总决赛=5。"""
    if is_third_place:
        return 3
    total = total_knockout_rounds(bracket_size)
    if total == 0 or not 1 <= round_index <= total:
        return 1
    remaining = bracket_size >> (round_index - 1)
    if remaining == 2:
        return 5
    if remaining == 4:
        return 3
    if remaining == 8:
        return 2
    return 1


@dataclass(frozen=True, slots=True)
class MatchMvpResult:
    """单局权威问鼎之星（MVP）评定结果。"""

    player_id: int
    team_index: int
    score: int
    damage: int
    healing: int
    controls: int


def compute_match_mvp(settlements: Iterable[Message], winner_team_index: int) -> MatchMvpResult | None:
    """基于 BattleSettlementData 权威累计统计确定性评定本局 MVP（与 Go/Java 逐条对齐）。"""
    best: MatchMvpResult | None = None
    best_win = False
    best_alive = False
    best_contrib = 0
    for s in settlements:
        if s is None:
            continue
        pid = int(getattr(s, "player_id", 0))
        if pid == 0:
            continue
        dmg = int(getattr(s, "total_damage_dealt", 0))
        heal = int(getattr(s, "total_healing_done", 0))
        ctrl = int(getattr(s, "total_controls_landed", 0))
        alive = not bool(getattr(s, "is_dead", False)) and not bool(getattr(s, "fled", False))
        team_idx = int(getattr(s, "player_team_index", 0))
        on_win = winner_team_index >= 0 and team_idx == winner_team_index
        raw = dmg // 100 + heal // 80 + ctrl * 60
        if alive:
            raw += 120
        if on_win:
            raw += 180
        score = min(raw, 100_000)
        contrib = dmg + heal
        cand = MatchMvpResult(
            player_id=pid,
            team_index=team_idx,
            score=score,
            damage=dmg,
            healing=heal,
            controls=ctrl,
        )
        if (
            best is None
            or cand.score > best.score
            or (cand.score == best.score and contrib > best_contrib)
            or (cand.score == best.score and contrib == best_contrib and on_win and not best_win)
            or (cand.score == best.score and contrib == best_contrib and on_win == best_win and alive and not best_alive)
            or (
                cand.score == best.score
                and contrib == best_contrib
                and on_win == best_win
                and alive == best_alive
                and cand.player_id < best.player_id
            )
        ):
            best = cand
            best_win = on_win
            best_alive = alive
            best_contrib = contrib
    return best


# -- V2 观战预设喝彩聚合器（与 Go internal/tournament/spectate_cheer.go 严格对齐）------------------------


@dataclass(frozen=True, slots=True)
class SpectateCheerItem:
    seq: int
    phrase_id: int
    side: int
    sender_name: str
    created_ms: int


@dataclass(frozen=True, slots=True)
class SpectateCheerSnapshot:
    battle_id: int
    count_a: int = 0
    count_b: int = 0
    cooldown_until_ms: int = 0
    recent_cheers: tuple[SpectateCheerItem, ...] = ()
    server_time_ms: int = 0


@dataclass
class _SpectateCheerRoom:
    count_a: int = 0
    count_b: int = 0
    next_seq: int = 0
    last_active_ms: int = 0
    recent: list[SpectateCheerItem] = field(default_factory=list)
    cooldowns: dict[int, int] = field(default_factory=dict)


def _sanitize_cheer_sender_name(raw: str) -> str:
    trimmed = raw.strip()
    if not trimmed:
        return "观战道友"
    return trimmed[:MAX_CHEER_SENDER_NAME_CHARS]


class SpectateCheerHub:
    """管理锦标赛 L0/L1/L2 通用观战预设喝彩的权威双边计数、5 秒冷却与有界近期环形队列。"""

    # shortcut: 进程内聚合，多 match 副本场景下升级为 Redis HINCRBY + Stream 聚合。

    def __init__(self) -> None:
        self._rooms: dict[int, _SpectateCheerRoom] = {}

    def _evict_idle(self, now_ms: int) -> None:
        if len(self._rooms) < 64:
            return
        stale = [
            bid
            for bid, room in self._rooms.items()
            if now_ms > room.last_active_ms and now_ms - room.last_active_ms >= SPECTATE_CHEER_ROOM_IDLE_TTL_MS
        ]
        for bid in stale:
            del self._rooms[bid]

    @staticmethod
    def _collect_after(items: list[SpectateCheerItem], after_seq: int, limit: int) -> tuple[SpectateCheerItem, ...]:
        if not items:
            return ()
        out = [it for it in items if it.seq > after_seq]
        if limit > 0 and len(out) > limit:
            out = out[-limit:]
        return tuple(out)

    def send(
        self,
        battle_id: int,
        player_id: int,
        sender_name: str,
        phrase_id: int,
        side: int,
        now_ms: int,
    ) -> tuple[SpectateCheerSnapshot, bool, bool]:
        """校验白名单与 5 秒个人冷却并记录一条观战预设喝彩。返回 (snapshot, cooldown_hit, invalid_arg)。"""
        if (
            battle_id <= 0
            or player_id <= 0
            or not is_valid_spectate_cheer_phrase(phrase_id)
            or not is_valid_spectate_cheer_side(side)
        ):
            return SpectateCheerSnapshot(battle_id=battle_id, server_time_ms=now_ms), False, True
        self._evict_idle(now_ms)
        room = self._rooms.get(battle_id)
        if room is None:
            room = _SpectateCheerRoom(last_active_ms=now_ms)
            self._rooms[battle_id] = room
        room.last_active_ms = now_ms
        until = room.cooldowns.get(player_id, 0)
        if until > now_ms:
            return (
                SpectateCheerSnapshot(
                    battle_id=battle_id,
                    count_a=room.count_a,
                    count_b=room.count_b,
                    cooldown_until_ms=until,
                    recent_cheers=self._collect_after(room.recent, 0, SPECTATE_CHEER_INITIAL_RETURN),
                    server_time_ms=now_ms,
                ),
                True,
                False,
            )
        if side == 0:
            room.count_a += 1
        else:
            room.count_b += 1
        room.next_seq += 1
        item = SpectateCheerItem(
            seq=room.next_seq,
            phrase_id=int(phrase_id),
            side=int(side),
            sender_name=_sanitize_cheer_sender_name(sender_name),
            created_ms=now_ms,
        )
        room.recent.append(item)
        if len(room.recent) > SPECTATE_CHEER_RING_LIMIT:
            room.recent = room.recent[-SPECTATE_CHEER_RING_LIMIT:]
        next_until = now_ms + SPECTATE_CHEER_COOLDOWN_MS
        room.cooldowns[player_id] = next_until
        return (
            SpectateCheerSnapshot(
                battle_id=battle_id,
                count_a=room.count_a,
                count_b=room.count_b,
                cooldown_until_ms=next_until,
                recent_cheers=(item,),
                server_time_ms=now_ms,
            ),
            False,
            False,
        )

    def snapshot(self, battle_id: int, player_id: int, after_seq: int, now_ms: int) -> SpectateCheerSnapshot:
        """只读查询某场战斗当前观战人气与 seq > after_seq 的新增喝彩。"""
        if battle_id <= 0:
            return SpectateCheerSnapshot(battle_id=battle_id, server_time_ms=now_ms)
        room = self._rooms.get(battle_id)
        if room is None:
            return SpectateCheerSnapshot(battle_id=battle_id, server_time_ms=now_ms)
        until = room.cooldowns.get(player_id, 0)
        cooldown_until = until if until > now_ms else 0
        limit = SPECTATE_CHEER_INITIAL_RETURN if after_seq == 0 else SPECTATE_CHEER_RING_LIMIT
        return SpectateCheerSnapshot(
            battle_id=battle_id,
            count_a=room.count_a,
            count_b=room.count_b,
            cooldown_until_ms=cooldown_until,
            recent_cheers=self._collect_after(room.recent, after_seq, limit),
            server_time_ms=now_ms,
        )


# -- 配置目录（读取 ConfigTables.TournamentRule / TournamentReward，无表时回落默认首测目录）------------


@dataclass(frozen=True, slots=True)
class TournamentRuleSpec:
    id: int = 1
    rule_version: int = 3
    enabled: bool = True
    battle_config_id: int = 5
    battle_team_size: int = 3
    roster_max_size: int = 4
    min_level: int = 15
    registration_days: int = 42
    qualifier_days: int = 7
    knockout_days: int = 2
    settle_display_days: int = 5
    qualifier_max_matches: int = 6
    participation_min_matches: int = 2
    knockout_max_teams: int = 16
    knockout_round_timeout_seconds: int = 14400
    qualifier_win_points: int = 3
    qualifier_draw_points: int = 1
    qualifier_loss_points: int = 0
    invite_ttl_seconds: int = 259200
    desc: str = ""
    bo3_top4_enabled: bool = True
    intermission_seconds: int = 60
    public_knockout_replay: bool = True
    warmup_spar_enabled: bool = True
    prediction_enabled: bool = True
    prediction_reward_min_hits: int = 2
    third_place_match_enabled: bool = True
    leyline_ban_enabled: bool = True
    skill_ban_slots_per_team: int = 3


@dataclass(frozen=True, slots=True)
class TournamentRewardSpec:
    id: int
    tier_kind: int
    min_rank: int
    max_rank: int
    gold_amount: int
    items: tuple[tuple[int, int], ...]
    title_code: str = ""
    badge_code: str = ""
    mail_title: str = ""
    mail_body: str = ""
    desc: str = ""


_FALLBACK_PREDICTION_REWARD = TournamentRewardSpec(
    9,
    REWARD_TIER_PREDICTION,
    0,
    0,
    12000,
    ((3406, 6), (20, 10)),
    "tournament_oracle",
    "badge_tournament_oracle",
    "群仙问道赛·神机慧眼礼",
    "恭喜道友在群仙问道赛神机竞猜中料事如神达到领奖门槛！特赐绑定修炼物资与慧眼识仙称号。",
    "免费神机竞猜命中达标奖励(绑定自用)",
)


class TournamentCatalog:
    """不可变锦标赛规则与奖励目录，支持从 ConfigTables 加载并做边界校验。"""

    def __init__(self, rule: TournamentRuleSpec, rewards: tuple[TournamentRewardSpec, ...]) -> None:
        if rule.battle_config_id == 0:
            raise ValueError("tournament: battle_config_id must be > 0")
        if not 1 <= rule.battle_team_size <= 5:
            raise ValueError(f"tournament: battle_team_size={rule.battle_team_size} out of [1,5]")
        if not rule.battle_team_size <= rule.roster_max_size <= 7:
            raise ValueError(f"tournament: roster_max_size={rule.roster_max_size} invalid")
        if min(rule.registration_days, rule.qualifier_days, rule.knockout_days, rule.settle_display_days) <= 0:
            raise ValueError("tournament: all four stage durations must be > 0 days")
        if rule.qualifier_max_matches <= 0:
            raise ValueError("tournament: qualifier_max_matches must be > 0")
        if rule.knockout_max_teams not in (2, 4, 8, 16):
            raise ValueError(f"tournament: knockout_max_teams={rule.knockout_max_teams} invalid")
        if rule.knockout_round_timeout_seconds <= 0:
            raise ValueError("tournament: knockout_round_timeout_seconds must be > 0")
        if not 0 <= rule.skill_ban_slots_per_team <= 3:
            raise ValueError(f"tournament: skill_ban_slots_per_team={rule.skill_ban_slots_per_team} out of [0,3]")
        self.rule = rule
        seen: set[int] = set()
        ordered = sorted((r for r in rewards if r.id > 0), key=lambda r: r.id)
        for row in ordered:
            if row.id in seen:
                raise ValueError(f"tournament: duplicate TournamentReward id={row.id}")
            seen.add(row.id)
        self.ordered_rewards: tuple[TournamentRewardSpec, ...] = tuple(ordered)
        self.rewards_by_id: dict[int, TournamentRewardSpec] = {r.id: r for r in ordered}
        by_tier: dict[int, list[TournamentRewardSpec]] = {}
        for r in ordered:
            by_tier.setdefault(r.tier_kind, []).append(r)
        self.rewards_by_tier: dict[int, tuple[TournamentRewardSpec, ...]] = {
            k: tuple(v) for k, v in by_tier.items()
        }

    @classmethod
    def from_tables(cls, tables) -> TournamentCatalog:
        if (
            tables is None
            or "TournamentRule" not in tables.tables
            or "TournamentReward" not in tables.tables
        ):
            return cls.default_fallback()
        row = tables["TournamentRule"].get(1)
        if row is None:
            return cls.default_fallback()
        rule = TournamentRuleSpec(
            id=int(row.id),
            rule_version=int(row.rule_version),
            enabled=bool(row.enabled),
            battle_config_id=int(row.battle_config_id),
            battle_team_size=int(row.battle_team_size),
            roster_max_size=int(row.roster_max_size),
            min_level=int(row.min_level),
            registration_days=int(row.registration_days),
            qualifier_days=int(row.qualifier_days),
            knockout_days=int(row.knockout_days),
            settle_display_days=int(row.settle_display_days),
            qualifier_max_matches=int(row.qualifier_max_matches),
            participation_min_matches=int(row.participation_min_matches),
            knockout_max_teams=int(row.knockout_max_teams),
            knockout_round_timeout_seconds=int(row.knockout_round_timeout_seconds),
            qualifier_win_points=int(row.qualifier_win_points),
            qualifier_draw_points=int(row.qualifier_draw_points),
            qualifier_loss_points=int(row.qualifier_loss_points),
            invite_ttl_seconds=int(row.invite_ttl_seconds),
            desc=str(row.desc),
            bo3_top4_enabled=bool(getattr(row, "bo3_top4_enabled", True)),
            intermission_seconds=int(getattr(row, "intermission_seconds", DEFAULT_INTERMISSION_SEC)) or DEFAULT_INTERMISSION_SEC,
            public_knockout_replay=bool(getattr(row, "public_knockout_replay", True)),
            warmup_spar_enabled=bool(getattr(row, "warmup_spar_enabled", True)),
            prediction_enabled=bool(getattr(row, "prediction_enabled", True)),
            prediction_reward_min_hits=int(getattr(row, "prediction_reward_min_hits", 2)) or 2,
            third_place_match_enabled=bool(getattr(row, "third_place_match_enabled", True)),
            leyline_ban_enabled=bool(getattr(row, "leyline_ban_enabled", True)),
            skill_ban_slots_per_team=int(getattr(row, "skill_ban_slots_per_team", 3)),
        )
        rewards: list[TournamentRewardSpec] = []
        for r in tables["TournamentReward"].rows():
            pairs = tuple(
                (int(iid), int(cnt))
                for iid, cnt in zip(r.item_ids, r.item_counts)
                if int(iid) > 0 and int(cnt) > 0
            )
            rewards.append(
                TournamentRewardSpec(
                    id=int(r.id),
                    tier_kind=int(r.tier_kind),
                    min_rank=int(r.min_rank),
                    max_rank=int(r.max_rank),
                    gold_amount=int(r.gold_amount),
                    items=pairs,
                    title_code=str(r.title_code),
                    badge_code=str(r.badge_code),
                    mail_title=str(r.mail_title),
                    mail_body=str(r.mail_body),
                    desc=str(r.desc),
                )
            )
        return cls(rule, tuple(rewards))

    @classmethod
    def default_fallback(cls) -> TournamentCatalog:
        return cls(
            TournamentRuleSpec(),
            (
                TournamentRewardSpec(1, REWARD_TIER_CHAMPION, 1, 1, 120000, ((3403, 30), (3406, 30), (20, 60)), "tournament_champion", "badge_tournament_champion", "群仙问道赛·全服总冠军奖励", "恭喜你的战队夺得本赛季群仙问道赛全服总冠军！", "全服总冠军（第1名）"),
                TournamentRewardSpec(2, REWARD_TIER_RUNNER_UP, 2, 2, 80000, ((3403, 20), (3406, 20), (20, 40)), "tournament_runner_up", "badge_tournament_runner_up", "群仙问道赛·全服亚军奖励", "恭喜你的战队斩获本赛季群仙问道赛全服亚军！", "全服亚军（第2名）"),
                TournamentRewardSpec(3, REWARD_TIER_TOP4, 3, 4, 50000, ((3403, 14), (3406, 14), (20, 30)), "tournament_top4", "badge_tournament_top4", "群仙问道赛·全服四强奖励", "恭喜你的战队晋级本赛季群仙问道赛全服四强！", "全服四强（第3-4名）"),
                TournamentRewardSpec(4, REWARD_TIER_TOP8, 5, 8, 35000, ((3403, 10), (3406, 10), (20, 20)), "", "badge_tournament_top8", "群仙问道赛·全服八强奖励", "恭喜你的战队闯入本赛季群仙问道赛全服八强！", "全服八强（第5-8名）"),
                TournamentRewardSpec(5, REWARD_TIER_TOP16, 9, 16, 20000, ((3403, 6), (3406, 6), (20, 15)), "", "badge_tournament_top16", "群仙问道赛·全服十六强奖励", "恭喜你的战队入围本赛季群仙问道赛十六强！", "全服十六强（第9-16名）"),
                TournamentRewardSpec(6, REWARD_TIER_PARTICIPANT, 0, 0, 12000, ((3403, 4), (3406, 4), (20, 10)), "", "badge_tournament_participant", "群仙问道赛·赛季参赛保底奖", "感谢参与本赛季群仙问道赛并完成达标场次！", "参赛保底（完赛≥2场）"),
                TournamentRewardSpec(7, REWARD_TIER_CHEER, 0, 0, 5000, ((3403, 1), (20, 5)), "", "badge_tournament_cheer", "群仙问道赛·观赛助威纪念奖", "你免费助威的战队闯入四强或夺冠，奉上纪念礼！", "免费助威命中（进四强/夺冠）"),
                TournamentRewardSpec(8, REWARD_TIER_THIRD_PLACE, 3, 3, 65000, ((3403, 18), (3406, 18), (20, 35)), "tournament_third_place", "badge_tournament_third_place", "群仙问道赛·季军嘉奖", "恭喜阁下在群仙问道赛季军争夺战中力克群雄斩获季军！特赐绑定修炼物资与季军荣耀。", "季军争夺战第3名专属奖励(绑定自用)"),
                _FALLBACK_PREDICTION_REWARD,
            ),
        )

    def reward_for_placement(self, final_rank: int, qualifier_played: int) -> TournamentRewardSpec | None:
        if final_rank == 3 and self.rule.third_place_match_enabled:
            rows = self.rewards_by_tier.get(REWARD_TIER_THIRD_PLACE)
            if rows:
                return rows[0]
        if final_rank > 0:
            for row in self.ordered_rewards:
                if row.tier_kind == REWARD_TIER_THIRD_PLACE and not self.rule.third_place_match_enabled:
                    continue
                if row.min_rank > 0 and row.min_rank <= final_rank <= row.max_rank:
                    return row
        if qualifier_played >= self.rule.participation_min_matches:
            rows = self.rewards_by_tier.get(REWARD_TIER_PARTICIPANT)
            if rows:
                return rows[0]
        return None

    def cheer_reward(self) -> TournamentRewardSpec | None:
        rows = self.rewards_by_tier.get(REWARD_TIER_CHEER)
        return rows[0] if rows else None

    def prediction_reward(self) -> TournamentRewardSpec | None:
        rows = self.rewards_by_tier.get(REWARD_TIER_PREDICTION)
        if rows:
            return rows[0]
        return _FALLBACK_PREDICTION_REWARD if self.rule.prediction_enabled else None


# -- 内部数据记录 ------------------------------------------------------------------------------------


@dataclass
class _SeasonRecord:
    season_id: int
    season_key: str
    season_name: str
    rule_version: int
    stage: int
    registration_start_ms: int
    qualifier_start_ms: int
    knockout_start_ms: int
    settle_start_ms: int
    end_ms: int
    battle_team_size: int
    roster_max_size: int
    knockout_team_count: int = 0
    champion_squad_id: int = 0
    champion_squad_name: str = ""
    champion_member_summary: str = ""
    settled_ms: int = 0
    updated_ms: int = 0
    champion_members: tuple[_MemberRecord, ...] = ()
    champion_emblem_id: int = 0
    third_place_squad_id: int = 0
    third_place_squad_name: str = ""
    third_place_emblem_id: int = 0
    final_mvp_player_id: int = 0
    final_mvp_player_name: str = ""
    final_mvp_score: int = 0
    champion_honor_bits: int = 0


@dataclass
class _MemberRecord:
    season_id: int
    player_id: int
    squad_id: int
    role: int
    starter_slot: int
    player_name: str
    zone_id: int
    joined_ms: int
    class_id: int = 0
    gender: int = 0
    appearance_id: str = ""


@dataclass
class _SquadRecord:
    squad_id: int
    season_id: int
    name: str
    name_norm: str
    captain_player_id: int
    state: int
    member_count: int = 1
    qualifier_points: int = 0
    qualifier_played: int = 0
    qualifier_wins: int = 0
    qualifier_losses: int = 0
    qualifier_draws: int = 0
    seed_rank: int = 0
    final_rank: int = 0
    cheer_count: int = 0
    registered_ms: int = 0
    updated_ms: int = 0
    emblem_id: int = DEFAULT_EMBLEM_ID
    honor_bits: int = 0


@dataclass
class _InviteRecord:
    squad_id: int
    invitee_player_id: int
    season_id: int
    inviter_player_id: int
    inviter_name: str
    state: int
    created_ms: int
    expires_ms: int


@dataclass
class _MatchRecord:
    match_id: int
    season_id: int
    phase: int
    round_index: int
    slot_index: int
    squad_a_id: int
    squad_b_id: int = 0
    lineup_a: tuple[int, ...] = ()
    lineup_b: tuple[int, ...] = ()
    ready_a: bool = False
    ready_b: bool = False
    state: int = MATCH_WAITING_OPPONENT
    battle_id: int = 0
    winner_squad_id: int = 0
    outcome: int = 0
    featured_spectate: bool = False
    scheduled_start_ms: int = 0
    deadline_ms: int = 0
    finished_ms: int = 0
    updated_ms: int = 0
    best_of: int = 1
    current_game_index: int = 1
    wins_a: int = 0
    wins_b: int = 0
    current_leyline_element: int = LEYLINE_UNSPECIFIED
    leyline_sequence: tuple[int, ...] = ()
    intermission_deadline_ms: int = 0
    is_third_place: bool = False
    mvp_player_id: int = 0
    mvp_player_name: str = ""
    mvp_squad_id: int = 0
    mvp_score: int = 0
    mvp_damage: int = 0
    mvp_healing: int = 0
    mvp_controls: int = 0
    target_squad_id: int = 0
    banned_leyline_a: int = LEYLINE_UNSPECIFIED
    banned_leyline_b: int = LEYLINE_UNSPECIFIED


@dataclass
class _MatchGameRecord:
    match_id: int
    game_index: int
    season_id: int
    phase: int
    battle_id: int
    leyline_element: int = LEYLINE_UNSPECIFIED
    winner_squad_id: int = 0
    outcome: int = 0
    finished_ms: int = 0


@dataclass
class _CheerRecord:
    season_id: int
    player_id: int
    squad_id: int
    rewarded: bool = False
    created_ms: int = 0


@dataclass
class _PredictionRecord:
    season_id: int
    player_id: int
    match_id: int
    predicted_squad_id: int
    round_index: int
    is_third_place: bool
    player_name: str
    result_state: int = PREDICTION_PENDING
    earned_points: int = 0
    created_ms: int = 0
    updated_ms: int = 0


@dataclass
class _RewardRecord:
    reward_id: int
    season_id: int
    player_id: int
    tier_kind: int
    squad_id: int
    final_rank: int
    tier_id: int
    gold_amount: int
    items: tuple[tuple[int, int], ...] = field(default_factory=tuple)
    title_code: str = ""
    badge_code: str = ""
    delivery_state: int = DELIVERY_CLAIMABLE
    mail_id: int = 0
    claimed_ms: int = 0
    updated_ms: int = 0


# -- 锦标赛权威管理器 --------------------------------------------------------------------------------


class TournamentManager:
    """全服赛季战队锦标赛《群仙问道赛》权威服务（支持配置表目录、内存持久态与全部 V1/V2/V3 客户端 RPC）。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        tables=None,
        catalog: TournamentCatalog | None = None,
        clock_ms: Callable[[], int] | None = None,
        battle_id_alloc: Callable[[], int] | None = None,
        cheer_hub: SpectateCheerHub | None = None,
    ) -> None:
        self.protocol = protocol
        self.catalog = catalog if catalog is not None else TournamentCatalog.from_tables(tables)
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._next_id = 100_000
        self._battle_id_alloc = battle_id_alloc
        self.cheer_hub = cheer_hub if cheer_hub is not None else SpectateCheerHub()

        self._seasons: dict[int, _SeasonRecord] = {}
        self._squads: dict[int, _SquadRecord] = {}
        self._members: dict[tuple[int, int], _MemberRecord] = {}  # (season_id, player_id)
        self._invites: dict[tuple[int, int], _InviteRecord] = {}  # (squad_id, invitee_player_id)
        self._matches: dict[int, _MatchRecord] = {}
        self._match_games: dict[tuple[int, int], _MatchGameRecord] = {}  # (match_id, game_index)
        self._games_by_battle: dict[int, _MatchGameRecord] = {}  # battle_id -> game record
        self._battle_contexts: dict[int, Message] = {}  # battle_id -> TournamentBattleContext
        self._cheers: dict[tuple[int, int], _CheerRecord] = {}  # (season_id, player_id)
        self._predictions: dict[tuple[int, int, int], _PredictionRecord] = {}  # (season_id, player_id, match_id)
        self._rewards: dict[tuple[int, int, int], _RewardRecord] = {}  # (season_id, player_id, tier_kind)

    def _alloc_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def _alloc_battle_id(self) -> int:
        if self._battle_id_alloc is not None:
            return int(self._battle_id_alloc())
        return self._alloc_id()

    def _now_ms(self) -> int:
        return int(self._clock_ms())

    def _build_season(self, season_id: int, start_ms: int) -> _SeasonRecord:
        r = self.catalog.rule
        q_start = start_ms + r.registration_days * MILLIS_PER_DAY
        k_start = q_start + r.qualifier_days * MILLIS_PER_DAY
        s_start = k_start + r.knockout_days * MILLIS_PER_DAY
        end_ms = s_start + r.settle_display_days * MILLIS_PER_DAY
        return _SeasonRecord(
            season_id=season_id,
            season_key=f"ts{season_id}",
            season_name=f"第{season_id}赛季·群仙问道赛",
            rule_version=r.rule_version,
            stage=STAGE_REGISTRATION,
            registration_start_ms=start_ms,
            qualifier_start_ms=q_start,
            knockout_start_ms=k_start,
            settle_start_ms=s_start,
            end_ms=end_ms,
            battle_team_size=r.battle_team_size,
            roster_max_size=r.roster_max_size,
            updated_ms=start_ms,
        )

    def _ensure_season(self, stamp: int) -> _SeasonRecord:
        if not self._seasons:
            s1 = self._build_season(1, stamp)
            self._seasons[1] = s1
            return s1
        latest_id = max(self._seasons)
        return self._seasons[latest_id]

    def _squad_members(self, squad_id: int) -> list[_MemberRecord]:
        members = [m for m in self._members.values() if m.squad_id == squad_id]
        members.sort(key=lambda m: (m.role, m.starter_slot, m.joined_ms, m.player_id))
        return members

    def _registered_squad_count(self, season_id: int) -> int:
        return sum(
            1 for sq in self._squads.values()
            if sq.season_id == season_id and sq.state in (SQUAD_REGISTERED, SQUAD_ELIMINATED, SQUAD_CHAMPION)
        )

    def _game_battle_ids(self, match_id: int) -> list[int]:
        games = [g for g in self._match_games.values() if g.match_id == match_id and g.battle_id > 0]
        games.sort(key=lambda g: g.game_index)
        return [g.battle_id for g in games]

    def _prediction_counts(self, match: _MatchRecord) -> tuple[int, int]:
        count_a = 0
        count_b = 0
        for p in self._predictions.values():
            if p.season_id != match.season_id or p.match_id != match.match_id:
                continue
            if p.predicted_squad_id == match.squad_a_id and match.squad_a_id > 0:
                count_a += 1
            elif p.predicted_squad_id == match.squad_b_id and match.squad_b_id > 0:
                count_b += 1
        return count_a, count_b

    def _record_game_launch(self, match: _MatchRecord, battle_id: int) -> Message:
        """登记单局小局归档并冻结 TournamentBattleContext（供 Gather 建房与单测读取）。"""
        game_idx = match.current_game_index or 1
        game = _MatchGameRecord(
            match_id=match.match_id,
            game_index=game_idx,
            season_id=match.season_id,
            phase=match.phase,
            battle_id=battle_id,
            leyline_element=match.current_leyline_element,
        )
        self._match_games[(match.match_id, game_idx)] = game
        self._games_by_battle[battle_id] = game
        sq_a = self._squads.get(match.squad_a_id)
        sq_b = self._squads.get(match.squad_b_id)
        ctx = self.protocol.message(
            "TournamentBattleContext",
            season_id=match.season_id,
            match_id=match.match_id,
            best_of=match.best_of or 1,
            game_index=game_idx,
            wins_a=match.wins_a,
            wins_b=match.wins_b,
            leyline_element=match.current_leyline_element,
            squad_a_id=match.squad_a_id,
            squad_a_name=sq_a.name if sq_a is not None else "",
            squad_b_id=match.squad_b_id,
            squad_b_name=sq_b.name if sq_b is not None else "",
            squad_a_emblem_id=sq_a.emblem_id if sq_a is not None else DEFAULT_EMBLEM_ID,
            squad_b_emblem_id=sq_b.emblem_id if sq_b is not None else DEFAULT_EMBLEM_ID,
            is_warmup=(match.phase == PHASE_WARMUP),
            is_third_place=match.is_third_place,
            skill_ban_slots_per_team=(
                0 if match.phase == PHASE_WARMUP else self.catalog.rule.skill_ban_slots_per_team
            ),
        )
        self._battle_contexts[battle_id] = ctx
        return ctx

    def load_squad_captains(self, squad_a_id: int, squad_b_id: int) -> set[int]:
        """返回两支参战战队队长的 player_id 集合（供 Gather 标记 `is_team_captain`）。"""
        captains: set[int] = set()
        for sid in (int(squad_a_id), int(squad_b_id)):
            sq = self._squads.get(sid)
            if sq is not None and sq.captain_player_id > 0:
                captains.add(int(sq.captain_player_id))
        return captains

    def battle_context_for(self, battle_id: int) -> Message | None:
        """返回某场已开战锦标赛对局的冻结 TournamentBattleContext（无则返回 None）。"""
        ctx = self._battle_contexts.get(int(battle_id))
        if ctx is None:
            return None
        cloned = self.protocol.message("TournamentBattleContext")
        cloned.CopyFrom(ctx)
        return cloned

    def is_tournament_battle(self, battle_id: int) -> bool:
        """校验某场 battle_id 是否属于锦标赛对局（防观战喝彩接口伪造任意 battle_id）。"""
        bid = int(battle_id)
        if bid <= 0:
            return False
        if bid in self._games_by_battle:
            return True
        return any(m.battle_id == bid for m in self._matches.values())

    def is_public_knockout_replay_allowed(self, battle_id: int) -> bool:
        """校验某场 battle_id 是否属于允许全服公开回放的淘汰赛对局（绝不放行海选/热身/私密切磋）。"""
        bid = int(battle_id)
        if bid <= 0 or not self.catalog.rule.public_knockout_replay:
            return False
        game = self._games_by_battle.get(bid)
        if game is not None:
            return game.phase == PHASE_KNOCKOUT
        return any(m.battle_id == bid and m.phase == PHASE_KNOCKOUT for m in self._matches.values())

    def sync_member_profiles(self, profiles: Iterable[tuple[int, str, int, int, str]]) -> int:
        """V3 模块 F：建房阶段从权威 BattlePlayerSnapshot 回填本赛季战队成员真实外观快照。"""
        if not self._seasons:
            return 0
        latest_season_id = max(self._seasons)
        updated = 0
        for pid, name, class_id, gender, appearance_id in profiles:
            mem = self._members.get((latest_season_id, int(pid)))
            if mem is None:
                continue
            if name:
                mem.player_name = str(name)
            if int(class_id) > 0:
                mem.class_id = int(class_id)
            if int(gender) > 0:
                mem.gender = int(gender)
            if appearance_id:
                mem.appearance_id = str(appearance_id)
            updated += 1
        return updated

    # -- Proto 视图构造 ------------------------------------------------------------------------------

    def _fill_member_view(self, dst, m: _MemberRecord) -> None:
        dst.player_id = m.player_id
        dst.player_name = m.player_name
        dst.zone_id = m.zone_id
        dst.role = m.role
        dst.starter_slot = m.starter_slot
        dst.joined_ms = m.joined_ms
        dst.class_id = m.class_id
        dst.gender = m.gender
        dst.appearance_id = m.appearance_id

    def _season_view(self, season: _SeasonRecord, registered_count: int) -> Message:
        rule = self.catalog.rule
        view = self.protocol.message(
            "match.TournamentSeasonView",
            season_id=season.season_id,
            season_key=season.season_key,
            season_name=season.season_name,
            rule_version=season.rule_version,
            stage=season.stage,
            enabled=rule.enabled,
            battle_team_size=season.battle_team_size,
            roster_max_size=season.roster_max_size,
            min_level=rule.min_level,
            qualifier_max_matches=rule.qualifier_max_matches,
            participation_min_matches=rule.participation_min_matches,
            knockout_max_teams=rule.knockout_max_teams,
            knockout_team_count=season.knockout_team_count,
            registered_squad_count=registered_count,
            registration_start_ms=season.registration_start_ms,
            qualifier_start_ms=season.qualifier_start_ms,
            knockout_start_ms=season.knockout_start_ms,
            settle_start_ms=season.settle_start_ms,
            end_ms=season.end_ms,
            champion_squad_id=season.champion_squad_id,
            champion_squad_name=season.champion_squad_name,
            champion_member_summary=season.champion_member_summary,
            champion_emblem_id=season.champion_emblem_id,
            third_place_squad_id=season.third_place_squad_id,
            third_place_squad_name=season.third_place_squad_name,
            third_place_emblem_id=season.third_place_emblem_id,
            final_mvp_player_id=season.final_mvp_player_id,
            final_mvp_player_name=season.final_mvp_player_name,
            final_mvp_score=season.final_mvp_score,
            champion_honor_bits=season.champion_honor_bits,
        )
        for cm in season.champion_members:
            self._fill_member_view(view.champion_members.add(), cm)
        return view

    def _squad_view(self, squad: _SquadRecord) -> Message:
        emblem = squad.emblem_id if is_valid_emblem_id(squad.emblem_id) else DEFAULT_EMBLEM_ID
        view = self.protocol.message(
            "match.TournamentSquadView",
            squad_id=squad.squad_id,
            season_id=squad.season_id,
            name=squad.name,
            captain_player_id=squad.captain_player_id,
            state=squad.state,
            member_count=squad.member_count,
            qualifier_points=squad.qualifier_points,
            qualifier_played=squad.qualifier_played,
            qualifier_wins=squad.qualifier_wins,
            qualifier_losses=squad.qualifier_losses,
            qualifier_draws=squad.qualifier_draws,
            seed_rank=squad.seed_rank,
            final_rank=squad.final_rank,
            cheer_count=squad.cheer_count,
            registered_ms=squad.registered_ms,
            emblem_id=emblem,
            honor_bits=squad.honor_bits,
        )
        for m in self._squad_members(squad.squad_id):
            self._fill_member_view(view.members.add(), m)
        return view

    def _invite_view(self, inv: _InviteRecord, squad: _SquadRecord, roster_max_size: int) -> Message:
        return self.protocol.message(
            "match.TournamentInviteView",
            squad_id=inv.squad_id,
            season_id=inv.season_id,
            squad_name=squad.name,
            inviter_player_id=inv.inviter_player_id,
            inviter_name=inv.inviter_name,
            current_member_count=squad.member_count,
            roster_max_size=roster_max_size,
            created_ms=inv.created_ms,
            expires_ms=inv.expires_ms,
        )

    def _match_view(self, m: _MatchRecord, *, viewer_player_id: int = 0) -> Message:
        sq_a = self._squads.get(m.squad_a_id)
        sq_b = self._squads.get(m.squad_b_id)
        game_battle_ids = self._game_battle_ids(m.match_id)
        best_of = m.best_of or 1
        cur_game = m.current_game_index or 1
        public_replay = (
            self.catalog.rule.public_knockout_replay
            and m.phase == PHASE_KNOCKOUT
            and (m.battle_id > 0 or bool(game_battle_ids))
            and (m.state == MATCH_FINISHED or m.wins_a > 0 or m.wins_b > 0)
        )
        my_pred_squad = 0
        if viewer_player_id > 0 and m.phase == PHASE_KNOCKOUT:
            pred = self._predictions.get((m.season_id, viewer_player_id, m.match_id))
            if pred is not None:
                my_pred_squad = pred.predicted_squad_id
        pred_a, pred_b = self._prediction_counts(m) if m.phase == PHASE_KNOCKOUT else (0, 0)
        view = self.protocol.message(
            "match.TournamentMatchView",
            match_id=m.match_id,
            season_id=m.season_id,
            phase=m.phase,
            round_index=m.round_index,
            slot_index=m.slot_index,
            squad_a_id=m.squad_a_id,
            squad_a_name=sq_a.name if sq_a is not None else "",
            squad_a_seed=sq_a.seed_rank if sq_a is not None else 0,
            squad_b_id=m.squad_b_id,
            squad_b_name=sq_b.name if sq_b is not None else "",
            squad_b_seed=sq_b.seed_rank if sq_b is not None else 0,
            ready_a=m.ready_a,
            ready_b=m.ready_b,
            state=m.state,
            battle_id=m.battle_id,
            winner_squad_id=m.winner_squad_id,
            outcome=m.outcome,
            featured_spectate=m.featured_spectate,
            scheduled_start_ms=m.scheduled_start_ms,
            deadline_ms=m.deadline_ms,
            finished_ms=m.finished_ms,
            best_of=best_of,
            current_game_index=cur_game,
            wins_a=m.wins_a,
            wins_b=m.wins_b,
            current_leyline_element=m.current_leyline_element,
            intermission_deadline_ms=m.intermission_deadline_ms,
            public_replay_available=public_replay,
            squad_a_emblem_id=sq_a.emblem_id if sq_a is not None else 0,
            squad_b_emblem_id=sq_b.emblem_id if sq_b is not None else 0,
            is_third_place=m.is_third_place,
            mvp_player_id=m.mvp_player_id,
            mvp_player_name=m.mvp_player_name,
            mvp_squad_id=m.mvp_squad_id,
            mvp_score=m.mvp_score,
            mvp_damage=m.mvp_damage,
            mvp_healing=m.mvp_healing,
            mvp_controls=m.mvp_controls,
            my_predicted_squad_id=my_pred_squad,
            predict_count_a=pred_a,
            predict_count_b=pred_b,
            banned_leyline_a=m.banned_leyline_a,
            banned_leyline_b=m.banned_leyline_b,
        )
        view.leyline_sequence.extend(m.leyline_sequence)
        view.game_battle_ids.extend(game_battle_ids)
        return view

    def _reward_view(self, r: _RewardRecord) -> Message:
        view = self.protocol.message(
            "match.TournamentPlayerRewardView",
            reward_id=r.reward_id,
            season_id=r.season_id,
            tier_kind=r.tier_kind,
            tier_id=r.tier_id,
            final_rank=r.final_rank,
            gold_amount=r.gold_amount,
            title_code=r.title_code,
            badge_code=r.badge_code,
            delivery_state=r.delivery_state,
            mail_id=r.mail_id,
            claimed_ms=r.claimed_ms,
        )
        for item_id, count in r.items:
            view.items.add(item_id=item_id, count=count)
        return view

    def _fill_reward_tiers(self, out_repeated) -> None:
        for row in self.catalog.ordered_rewards:
            tier = out_repeated.add(
                tier_id=row.id,
                tier_kind=row.tier_kind,
                min_rank=row.min_rank,
                max_rank=row.max_rank,
                gold_amount=row.gold_amount,
                title_code=row.title_code,
                badge_code=row.badge_code,
                desc=row.desc,
            )
            for item_id, count in row.items:
                tier.items.add(item_id=item_id, count=count)

    @staticmethod
    def _standings_sort_key(sq: _SquadRecord) -> tuple:
        has_final = 0 if sq.final_rank > 0 else 1
        final_key = sq.final_rank if sq.final_rank > 0 else 0
        return (
            has_final,
            final_key,
            -sq.qualifier_points,
            -sq.qualifier_wins,
            -sq.qualifier_draws,
            sq.registered_ms,
            sq.squad_id,
        )

    @staticmethod
    def _caller_display_name(caller: CallerIdentity) -> str:
        return caller.account or f"P{caller.player_id}"

    # -- 18 条客户端 RPC（V1 11 条 + V2 3 条 + V3 4 条）----------------------------------------------

    async def get_tournament_overview(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.GetTournamentOverviewResponse")
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        reg_count = self._registered_squad_count(cur.season_id)
        resp.current_season.CopyFrom(self._season_view(cur, reg_count))
        resp.server_time_ms = stamp
        self._fill_reward_tiers(resp.reward_tiers)

        my_mem = self._members.get((cur.season_id, caller.player_id))
        if my_mem is not None:
            sq = self._squads.get(my_mem.squad_id)
            if sq is not None:
                resp.my_squad.CopyFrom(self._squad_view(sq))
                active = [
                    m for m in self._matches.values()
                    if m.season_id == cur.season_id
                    and (m.squad_a_id == sq.squad_id or m.squad_b_id == sq.squad_id)
                    and m.state in (MATCH_WAITING_OPPONENT, MATCH_READY, MATCH_IN_BATTLE, MATCH_INTERMISSION)
                ]
                if active:
                    # 正式赛（海选/淘汰）永远优先于热身约战，防热身抢占活跃对局展示。
                    active.sort(
                        key=lambda m: (1 if m.phase != PHASE_WARMUP else 0, m.phase, m.round_index, m.match_id),
                        reverse=True,
                    )
                    resp.my_active_match.CopyFrom(self._match_view(active[0], viewer_player_id=caller.player_id))

        pending_invites = [
            inv for inv in self._invites.values()
            if inv.invitee_player_id == caller.player_id
            and inv.season_id == cur.season_id
            and inv.state == INVITE_PENDING
            and inv.expires_ms > stamp
        ]
        pending_invites.sort(key=lambda i: i.created_ms, reverse=True)
        for inv in pending_invites[:20]:
            sq = self._squads.get(inv.squad_id)
            if sq is not None and sq.state == SQUAD_DRAFT:
                resp.pending_invites.add().CopyFrom(self._invite_view(inv, sq, cur.roster_max_size))

        featured = [
            m for m in self._matches.values()
            if m.season_id == cur.season_id and m.state == MATCH_IN_BATTLE and m.phase != PHASE_WARMUP
        ]
        featured.sort(key=lambda m: (1 if m.featured_spectate else 0, m.phase, m.round_index), reverse=True)
        for m in featured[:10]:
            resp.featured_matches.add().CopyFrom(self._match_view(m, viewer_player_id=caller.player_id))

        cheer = self._cheers.get((cur.season_id, caller.player_id))
        if cheer is not None:
            resp.my_cheered_squad_id = cheer.squad_id
            cheered_sq = self._squads.get(cheer.squad_id)
            if cheered_sq is not None:
                resp.my_cheered_squad_name = cheered_sq.name

        include_history = bool(request.include_history)
        my_rewards = [
            r for r in self._rewards.values()
            if r.player_id == caller.player_id and (include_history or r.season_id == cur.season_id)
        ]
        my_rewards.sort(key=lambda r: (-r.season_id, r.tier_kind))
        for r in my_rewards[:50]:
            resp.my_rewards.add().CopyFrom(self._reward_view(r))

        if include_history:
            hof = [
                s for s in self._seasons.values()
                if s.stage == STAGE_SETTLED and s.champion_squad_id > 0
            ]
            hof.sort(key=lambda s: s.season_id, reverse=True)
            for hs in hof[:20]:
                resp.hall_of_fame.add().CopyFrom(self._season_view(hs, 0))
        return resp

    async def create_tournament_squad(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.CreateTournamentSquadResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        display, norm, ok = normalize_squad_name(str(request.squad_name))
        if not ok:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID)
        emblem_id, emblem_ok = normalize_emblem_id(int(getattr(request, "emblem_id", 0)))
        if not emblem_ok:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_EMBLEM_INVALID)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage != STAGE_REGISTRATION:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        if (cur.season_id, caller.player_id) in self._members:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD)
        if any(
            sq.season_id == cur.season_id and sq.name_norm == norm and sq.state != SQUAD_DISBANDED
            for sq in self._squads.values()
        ):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID)

        squad_id = self._alloc_id()
        squad = _SquadRecord(
            squad_id=squad_id,
            season_id=cur.season_id,
            name=display,
            name_norm=norm,
            captain_player_id=caller.player_id,
            state=SQUAD_DRAFT,
            member_count=1,
            updated_ms=stamp,
            emblem_id=emblem_id,
        )
        self._squads[squad_id] = squad
        self._members[(cur.season_id, caller.player_id)] = _MemberRecord(
            season_id=cur.season_id,
            player_id=caller.player_id,
            squad_id=squad_id,
            role=ROLE_CAPTAIN,
            starter_slot=1,
            player_name=self._caller_display_name(caller),
            zone_id=int(getattr(caller, "zone_id", 0)),
            joined_ms=stamp,
        )
        logger.info("Tournament squad created season=%d squad_id=%d captain=%d", cur.season_id, squad_id, caller.player_id)
        resp.squad.CopyFrom(self._squad_view(squad))
        return resp

    async def invite_tournament_member(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.InviteTournamentMemberResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        target_id = int(request.target_player_id)
        if target_id == 0 or target_id == caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_LINEUP_INVALID)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage != STAGE_REGISTRATION:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        mem = self._members.get((cur.season_id, caller.player_id))
        if mem is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        squad = self._squads.get(mem.squad_id)
        if squad is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if squad.captain_player_id != caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)
        if squad.state != SQUAD_DRAFT:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_LOCKED)
        if squad.member_count >= cur.roster_max_size:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_FULL)
        if (cur.season_id, target_id) in self._members:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD)

        ttl_sec = self.catalog.rule.invite_ttl_seconds or 259200
        expires_ms = stamp + ttl_sec * 1000
        inv = _InviteRecord(
            squad_id=squad.squad_id,
            invitee_player_id=target_id,
            season_id=cur.season_id,
            inviter_player_id=caller.player_id,
            inviter_name=mem.player_name,
            state=INVITE_PENDING,
            created_ms=stamp,
            expires_ms=expires_ms,
        )
        self._invites[(squad.squad_id, target_id)] = inv
        resp.invite.CopyFrom(self._invite_view(inv, squad, cur.roster_max_size))
        return resp

    async def respond_tournament_invite(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.RespondTournamentInviteResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        squad_id = int(request.squad_id)
        accept = bool(request.accept)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        inv = self._invites.get((squad_id, caller.player_id))
        if inv is None or inv.season_id != cur.season_id or inv.state != INVITE_PENDING or inv.expires_ms <= stamp:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_INVITE_NOT_FOUND)
        if not accept:
            inv.state = INVITE_DECLINED
            return resp
        if cur.stage != STAGE_REGISTRATION:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        squad = self._squads.get(squad_id)
        if squad is None or squad.state == SQUAD_DISBANDED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if squad.state != SQUAD_DRAFT:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_LOCKED)
        if squad.member_count >= cur.roster_max_size:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_FULL)
        if (cur.season_id, caller.player_id) in self._members:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD)

        members = self._squad_members(squad_id)
        used_slots = {m.starter_slot for m in members if m.starter_slot > 0}
        role = ROLE_SUBSTITUTE
        slot = 0
        for cand in range(1, cur.battle_team_size + 1):
            if cand not in used_slots:
                role = ROLE_STARTER
                slot = cand
                break
        self._members[(cur.season_id, caller.player_id)] = _MemberRecord(
            season_id=cur.season_id,
            player_id=caller.player_id,
            squad_id=squad_id,
            role=role,
            starter_slot=slot,
            player_name=self._caller_display_name(caller),
            zone_id=int(getattr(caller, "zone_id", 0)),
            joined_ms=stamp,
        )
        squad.member_count += 1
        squad.updated_ms = stamp
        inv.state = INVITE_ACCEPTED
        for other in self._invites.values():
            if (
                other.invitee_player_id == caller.player_id
                and other.season_id == cur.season_id
                and other.state == INVITE_PENDING
                and other.squad_id != squad_id
            ):
                other.state = INVITE_EXPIRED
        resp.squad.CopyFrom(self._squad_view(squad))
        return resp

    async def leave_tournament_squad(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.LeaveTournamentSquadResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        target_id = int(request.target_player_id) or caller.player_id
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage != STAGE_REGISTRATION:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        actor_mem = self._members.get((cur.season_id, caller.player_id))
        if actor_mem is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        squad = self._squads.get(actor_mem.squad_id)
        if squad is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if squad.state != SQUAD_DRAFT:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_LOCKED)
        if target_id != caller.player_id and squad.captain_player_id != caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)
        target_mem = self._members.get((cur.season_id, target_id))
        if target_mem is None or target_mem.squad_id != squad.squad_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)

        del self._members[(cur.season_id, target_id)]
        remaining = self._squad_members(squad.squad_id)
        if not remaining:
            squad.state = SQUAD_DISBANDED
            squad.member_count = 0
            squad.name_norm = f"__disbanded_{squad.squad_id}_{squad.name_norm}"[:190]
            squad.updated_ms = stamp
            for inv in self._invites.values():
                if inv.squad_id == squad.squad_id and inv.state == INVITE_PENDING:
                    inv.state = INVITE_EXPIRED
            resp.squad_disbanded = True
            return resp

        if target_id == squad.captain_player_id:
            new_cap = remaining[0]
            squad.captain_player_id = new_cap.player_id
            new_cap.role = ROLE_CAPTAIN
            if new_cap.starter_slot == 0:
                new_cap.starter_slot = 1
        squad.member_count = len(remaining)
        squad.updated_ms = stamp
        if target_id != caller.player_id:
            resp.squad.CopyFrom(self._squad_view(squad))
        return resp

    async def update_tournament_lineup(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.UpdateTournamentLineupResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage == STAGE_SETTLED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        mem = self._members.get((cur.season_id, caller.player_id))
        if mem is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        squad = self._squads.get(mem.squad_id)
        if squad is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if squad.captain_player_id != caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)

        starter_ids = [int(pid) for pid in request.starter_player_ids]
        if len(starter_ids) != cur.battle_team_size:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_LINEUP_INVALID)
        members = self._squad_members(squad.squad_id)
        member_ids = {m.player_id for m in members}
        slot_by_pid: dict[int, int] = {}
        for idx, pid in enumerate(starter_ids):
            if pid == 0 or pid not in member_ids or pid in slot_by_pid:
                return set_tip(resp, ERR_MATCH_TOURNAMENT_LINEUP_INVALID)
            slot_by_pid[pid] = idx + 1

        for m in members:
            slot = slot_by_pid.get(m.player_id, 0)
            m.starter_slot = slot
            if m.player_id == squad.captain_player_id:
                m.role = ROLE_CAPTAIN
            elif slot > 0:
                m.role = ROLE_STARTER
            else:
                m.role = ROLE_SUBSTITUTE
        squad.updated_ms = stamp
        resp.squad.CopyFrom(self._squad_view(squad))
        return resp

    async def register_tournament_squad(self, caller: CallerIdentity, _request: Message) -> Message:
        resp = self.protocol.message("match.RegisterTournamentSquadResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage != STAGE_REGISTRATION:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        mem = self._members.get((cur.season_id, caller.player_id))
        if mem is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        squad = self._squads.get(mem.squad_id)
        if squad is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if squad.captain_player_id != caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)
        if squad.state == SQUAD_REGISTERED:
            resp.squad.CopyFrom(self._squad_view(squad))
            return resp
        if squad.state != SQUAD_DRAFT:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_LOCKED)
        if squad.member_count < cur.battle_team_size:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_MEMBER_NOT_ENOUGH)

        squad.state = SQUAD_REGISTERED
        squad.registered_ms = stamp
        squad.updated_ms = stamp
        for inv in self._invites.values():
            if inv.squad_id == squad.squad_id and inv.state == INVITE_PENDING:
                inv.state = INVITE_EXPIRED
        logger.info("Tournament squad registered season=%d squad_id=%d", cur.season_id, squad.squad_id)
        resp.squad.CopyFrom(self._squad_view(squad))
        return resp

    async def list_tournament_standings(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.ListTournamentStandingsResponse")
        page = max(int(request.page), 1)
        page_size = int(request.page_size) or 20
        page_size = min(page_size, 50)
        stamp = self._now_ms()
        req_season = int(request.season_id)
        if req_season == 0:
            season = self._ensure_season(stamp)
        else:
            season = self._seasons.get(req_season)
            if season is None:
                return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)

        all_squads = [
            sq for sq in self._squads.values()
            if sq.season_id == season.season_id and sq.state in (SQUAD_REGISTERED, SQUAD_ELIMINATED, SQUAD_CHAMPION)
        ]
        all_squads.sort(key=self._standings_sort_key)

        resp.season.CopyFrom(self._season_view(season, len(all_squads)))
        resp.total_squads = len(all_squads)
        resp.server_time_ms = stamp
        start = (page - 1) * page_size
        for sq in all_squads[start : start + page_size]:
            resp.standings.add().CopyFrom(self._squad_view(sq))

        my_mem = self._members.get((season.season_id, caller.player_id))
        if my_mem is not None:
            my_sq = self._squads.get(my_mem.squad_id)
            if my_sq is not None and my_sq.state in (SQUAD_REGISTERED, SQUAD_ELIMINATED, SQUAD_CHAMPION):
                resp.my_squad.CopyFrom(self._squad_view(my_sq))

        bracket = [
            m for m in self._matches.values()
            if m.season_id == season.season_id and m.phase == PHASE_KNOCKOUT
        ]
        bracket.sort(key=lambda m: (m.round_index, m.slot_index))
        for m in bracket:
            resp.knockout_bracket.add().CopyFrom(self._match_view(m, viewer_player_id=caller.player_id))
        return resp

    def _resolve_lineup(self, squad_id: int, requested: list[int], required: int) -> tuple[int, tuple[int, ...]]:
        members = self._squad_members(squad_id)
        if len(members) < required:
            return ERR_MATCH_TOURNAMENT_MEMBER_NOT_ENOUGH, ()
        if not requested:
            starters = [m.player_id for m in members if m.starter_slot > 0]
            if len(starters) < required:
                starters = [m.player_id for m in members[:required]]
            if len(starters) != required:
                return ERR_MATCH_TOURNAMENT_LINEUP_INVALID, ()
            return 0, tuple(starters[:required])
        if len(requested) != required:
            return ERR_MATCH_TOURNAMENT_LINEUP_INVALID, ()
        valid = {m.player_id for m in members}
        if len(set(requested)) != required or any(pid == 0 or pid not in valid for pid in requested):
            return ERR_MATCH_TOURNAMENT_LINEUP_INVALID, ()
        return 0, tuple(requested)

    async def start_tournament_match(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.StartTournamentMatchResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage not in (STAGE_QUALIFIER, STAGE_KNOCKOUT):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        mem = self._members.get((cur.season_id, caller.player_id))
        if mem is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        my_squad = self._squads.get(mem.squad_id)
        if my_squad is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if my_squad.captain_player_id != caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)
        if my_squad.state != SQUAD_REGISTERED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)

        err, lineup = self._resolve_lineup(
            my_squad.squad_id, [int(pid) for pid in request.lineup_player_ids], cur.battle_team_size,
        )
        if err:
            return set_tip(resp, err)

        req_match_id = int(request.match_id)

        # 淘汰赛分支：确认对阵就绪（含 BO3 局间换阵 INTERMISSION），双方都就绪立即分配 battle_id 进入激战并标全服精选观战。
        if cur.stage == STAGE_KNOCKOUT:
            target_match: _MatchRecord | None = None
            if req_match_id != 0:
                cand = self._matches.get(req_match_id)
                if cand is not None and cand.season_id == cur.season_id and cand.phase == PHASE_KNOCKOUT:
                    target_match = cand
            else:
                cands = [
                    m for m in self._matches.values()
                    if m.season_id == cur.season_id
                    and m.phase == PHASE_KNOCKOUT
                    and my_squad.squad_id in (m.squad_a_id, m.squad_b_id)
                    and m.state in (MATCH_WAITING_OPPONENT, MATCH_READY, MATCH_INTERMISSION)
                ]
                cands.sort(key=lambda m: (m.round_index, m.slot_index))
                if cands:
                    target_match = cands[0]
            if (
                target_match is None
                or target_match.state not in (MATCH_WAITING_OPPONENT, MATCH_READY, MATCH_INTERMISSION)
                or target_match.squad_a_id == 0
                or target_match.squad_b_id == 0
                or my_squad.squad_id not in (target_match.squad_a_id, target_match.squad_b_id)
            ):
                return set_tip(resp, ERR_MATCH_TOURNAMENT_MATCH_NOT_READY)

            if my_squad.squad_id == target_match.squad_a_id:
                target_match.lineup_a = lineup
                target_match.ready_a = True
            else:
                target_match.lineup_b = lineup
                target_match.ready_b = True
            target_match.updated_ms = stamp
            if target_match.ready_a and target_match.ready_b:
                target_match.battle_id = self._alloc_battle_id()
                target_match.state = MATCH_IN_BATTLE
                target_match.featured_spectate = True
                target_match.intermission_deadline_ms = 0
                self._record_game_launch(target_match, target_match.battle_id)
                resp.battle_id = target_match.battle_id
            resp.match.CopyFrom(self._match_view(target_match, viewer_player_id=caller.player_id))
            return resp

        # 海选积分赛分支
        if my_squad.qualifier_played >= self.catalog.rule.qualifier_max_matches:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_QUALIFIER_LIMIT)
        for existing in self._matches.values():
            if (
                existing.season_id == cur.season_id
                and existing.phase == PHASE_QUALIFIER
                and my_squad.squad_id in (existing.squad_a_id, existing.squad_b_id)
                and existing.state in (MATCH_WAITING_OPPONENT, MATCH_READY, MATCH_IN_BATTLE)
            ):
                if existing.state == MATCH_WAITING_OPPONENT and existing.squad_a_id == my_squad.squad_id:
                    existing.lineup_a = lineup
                    existing.updated_ms = stamp
                resp.match.CopyFrom(self._match_view(existing, viewer_player_id=caller.player_id))
                resp.battle_id = existing.battle_id
                return resp

        total_reg = sum(
            1 for sq in self._squads.values()
            if sq.season_id == cur.season_id and sq.state == SQUAD_REGISTERED
        )
        max_pair_meetings = 2 if total_reg <= 4 else 1

        waiting = [
            m for m in self._matches.values()
            if m.season_id == cur.season_id
            and m.phase == PHASE_QUALIFIER
            and m.state == MATCH_WAITING_OPPONENT
            and m.squad_a_id != my_squad.squad_id
            and m.squad_b_id == 0
        ]
        waiting.sort(key=lambda m: m.match_id)

        paired: _MatchRecord | None = None
        for cand in waiting:
            met = sum(
                1 for hist in self._matches.values()
                if hist.season_id == cur.season_id
                and hist.phase == PHASE_QUALIFIER
                and hist.state in (MATCH_READY, MATCH_IN_BATTLE, MATCH_FINISHED, MATCH_FORFEIT)
                and {hist.squad_a_id, hist.squad_b_id} == {my_squad.squad_id, cand.squad_a_id}
            )
            if met < max_pair_meetings:
                paired = cand
                break

        if paired is not None:
            paired.squad_b_id = my_squad.squad_id
            paired.lineup_b = lineup
            paired.ready_b = True
            paired.battle_id = self._alloc_battle_id()
            paired.state = MATCH_IN_BATTLE
            paired.updated_ms = stamp
            self._record_game_launch(paired, paired.battle_id)
            resp.match.CopyFrom(self._match_view(paired, viewer_player_id=caller.player_id))
            resp.battle_id = paired.battle_id
            return resp

        match_id = self._alloc_id()
        created = _MatchRecord(
            match_id=match_id,
            season_id=cur.season_id,
            phase=PHASE_QUALIFIER,
            round_index=my_squad.qualifier_played + 1,
            slot_index=0,
            squad_a_id=my_squad.squad_id,
            lineup_a=lineup,
            ready_a=True,
            state=MATCH_WAITING_OPPONENT,
            scheduled_start_ms=stamp,
            deadline_ms=cur.knockout_start_ms,
            updated_ms=stamp,
        )
        self._matches[match_id] = created
        resp.match.CopyFrom(self._match_view(created, viewer_player_id=caller.player_id))
        return resp

    async def cheer_tournament_squad(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.CheerTournamentSquadResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        squad_id = int(request.squad_id)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage == STAGE_SETTLED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        squad = self._squads.get(squad_id)
        if squad is None or squad.season_id != cur.season_id or squad.state != SQUAD_REGISTERED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if (cur.season_id, caller.player_id) in self._cheers:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_ALREADY_CHEERED)

        self._cheers[(cur.season_id, caller.player_id)] = _CheerRecord(
            season_id=cur.season_id,
            player_id=caller.player_id,
            squad_id=squad_id,
            rewarded=False,
            created_ms=stamp,
        )
        squad.cheer_count += 1
        squad.updated_ms = stamp
        resp.squad_id = squad_id
        resp.squad_cheer_count = squad.cheer_count
        return resp

    async def claim_tournament_reward(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.ClaimTournamentRewardResponse")
        req_season = int(request.season_id)
        stamp = self._now_ms()
        pending = [
            r for r in self._rewards.values()
            if r.player_id == caller.player_id
            and r.delivery_state == DELIVERY_CLAIMABLE
            and (req_season == 0 or r.season_id == req_season)
        ]
        if not pending:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_REWARD_UNAVAILABLE)
        pending.sort(key=lambda r: (-r.season_id, r.tier_kind))
        for r in pending:
            r.delivery_state = DELIVERY_CLAIMED
            r.claimed_ms = stamp
            r.updated_ms = stamp
            resp.claimed_rewards.add().CopyFrom(self._reward_view(r))
        return resp

    # -- V2 客户端 RPC（507..508，506 已停用）：观战预设喝彩 -------------------------------------------

    def _fill_cheer_snapshot(self, resp: Message, snap: SpectateCheerSnapshot) -> None:
        resp.battle_id = snap.battle_id
        resp.count_a = snap.count_a
        resp.count_b = snap.count_b
        resp.cooldown_until_ms = snap.cooldown_until_ms
        resp.server_time_ms = snap.server_time_ms
        for item in snap.recent_cheers:
            resp.recent_cheers.add(
                seq=item.seq,
                phrase_id=item.phrase_id,
                side=item.side,
                sender_name=item.sender_name,
                created_ms=item.created_ms,
            )

    async def send_tournament_spectate_cheer(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.SendTournamentSpectateCheerResponse")
        battle_id = int(request.battle_id)
        phrase_id = int(request.phrase_id)
        side = int(request.side)
        if (
            battle_id <= 0
            or not is_valid_spectate_cheer_phrase(phrase_id)
            or not is_valid_spectate_cheer_side(side)
            or not self.is_tournament_battle(battle_id)
        ):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_INVALID)
        stamp = self._now_ms()
        snap, cooldown_hit, invalid_arg = self.cheer_hub.send(
            battle_id,
            caller.player_id,
            self._caller_display_name(caller),
            phrase_id,
            side,
            stamp,
        )
        if invalid_arg:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_INVALID)
        if cooldown_hit:
            set_tip(resp, ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_COOLDOWN)
        self._fill_cheer_snapshot(resp, snap)
        return resp

    async def get_tournament_spectate_cheer(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.GetTournamentSpectateCheerResponse")
        battle_id = int(request.battle_id)
        if battle_id <= 0:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SPECTATE_CHEER_INVALID)
        stamp = self._now_ms()
        snap = self.cheer_hub.snapshot(battle_id, caller.player_id, int(request.after_seq), stamp)
        self._fill_cheer_snapshot(resp, snap)
        return resp

    # -- V3 客户端 RPC（511..514）：队徽更换 + 零积分热身约战 + 神机问道免费竞猜 ------------------------

    async def update_tournament_squad_emblem(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.UpdateTournamentSquadEmblemResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        emblem_id = int(request.emblem_id)
        if not is_valid_emblem_id(emblem_id):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_EMBLEM_INVALID)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage == STAGE_SETTLED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        mem = self._members.get((cur.season_id, caller.player_id))
        if mem is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        squad = self._squads.get(mem.squad_id)
        if squad is None or squad.state == SQUAD_DISBANDED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if squad.captain_player_id != caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)
        squad.emblem_id = emblem_id
        squad.updated_ms = stamp
        resp.squad.CopyFrom(self._squad_view(squad))
        return resp

    async def start_tournament_warmup(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.StartTournamentWarmupResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        if not self.catalog.rule.warmup_spar_enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage not in (STAGE_REGISTRATION, STAGE_QUALIFIER):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE)
        mem = self._members.get((cur.season_id, caller.player_id))
        if mem is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        my_squad = self._squads.get(mem.squad_id)
        if my_squad is None or my_squad.state == SQUAD_DISBANDED:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if my_squad.captain_player_id != caller.player_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)
        if my_squad.state not in (SQUAD_DRAFT, SQUAD_REGISTERED):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE)

        leyline = int(request.leyline_element)
        if not LEYLINE_UNSPECIFIED <= leyline <= LEYLINE_EARTH:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE)

        target_squad_id = int(request.target_squad_id)
        if target_squad_id == my_squad.squad_id:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE)
        if target_squad_id != 0:
            target_sq = self._squads.get(target_squad_id)
            if (
                target_sq is None
                or target_sq.season_id != cur.season_id
                or target_sq.state not in (SQUAD_DRAFT, SQUAD_REGISTERED)
                or target_sq.member_count < cur.battle_team_size
            ):
                return set_tip(resp, ERR_MATCH_TOURNAMENT_WARMUP_UNAVAILABLE)

        err, lineup = self._resolve_lineup(
            my_squad.squad_id,
            [int(pid) for pid in request.lineup_player_ids],
            cur.battle_team_size,
        )
        if err:
            return set_tip(resp, err)

        for existing in self._matches.values():
            if (
                existing.season_id == cur.season_id
                and existing.phase == PHASE_WARMUP
                and my_squad.squad_id in (existing.squad_a_id, existing.squad_b_id)
                and existing.state in (MATCH_WAITING_OPPONENT, MATCH_READY, MATCH_IN_BATTLE)
            ):
                if existing.state == MATCH_WAITING_OPPONENT and existing.squad_a_id == my_squad.squad_id:
                    existing.lineup_a = lineup
                    existing.current_leyline_element = leyline
                    existing.target_squad_id = target_squad_id
                    existing.updated_ms = stamp
                resp.match.CopyFrom(self._match_view(existing, viewer_player_id=caller.player_id))
                resp.battle_id = existing.battle_id
                return resp

        waiting = [
            m for m in self._matches.values()
            if m.season_id == cur.season_id
            and m.phase == PHASE_WARMUP
            and m.state == MATCH_WAITING_OPPONENT
            and m.squad_a_id != my_squad.squad_id
            and m.squad_b_id == 0
        ]
        compatible: list[_MatchRecord] = []
        for cand in waiting:
            if target_squad_id != 0:
                if cand.squad_a_id == target_squad_id and cand.target_squad_id in (0, my_squad.squad_id):
                    compatible.append(cand)
            else:
                if cand.target_squad_id in (0, my_squad.squad_id):
                    compatible.append(cand)

        if compatible:
            compatible.sort(key=lambda m: (0 if m.target_squad_id == my_squad.squad_id else 1, m.match_id))
            paired = compatible[0]
            paired.squad_b_id = my_squad.squad_id
            paired.lineup_b = lineup
            paired.ready_b = True
            if paired.current_leyline_element == LEYLINE_UNSPECIFIED and leyline != LEYLINE_UNSPECIFIED:
                paired.current_leyline_element = leyline
            paired.battle_id = self._alloc_battle_id()
            paired.state = MATCH_IN_BATTLE
            paired.featured_spectate = False
            paired.updated_ms = stamp
            self._record_game_launch(paired, paired.battle_id)
            resp.match.CopyFrom(self._match_view(paired, viewer_player_id=caller.player_id))
            resp.battle_id = paired.battle_id
            return resp

        match_id = self._alloc_id()
        created = _MatchRecord(
            match_id=match_id,
            season_id=cur.season_id,
            phase=PHASE_WARMUP,
            round_index=1,
            slot_index=0,
            squad_a_id=my_squad.squad_id,
            lineup_a=lineup,
            ready_a=True,
            state=MATCH_WAITING_OPPONENT,
            featured_spectate=False,
            scheduled_start_ms=stamp,
            deadline_ms=cur.knockout_start_ms,
            updated_ms=stamp,
            best_of=1,
            current_game_index=1,
            current_leyline_element=leyline,
            target_squad_id=target_squad_id,
        )
        self._matches[match_id] = created
        resp.match.CopyFrom(self._match_view(created, viewer_player_id=caller.player_id))
        return resp

    async def predict_tournament_match(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.PredictTournamentMatchResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        if not self.catalog.rule.prediction_enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_PREDICTION_CLOSED)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage != STAGE_KNOCKOUT:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_PREDICTION_CLOSED)
        match_id = int(request.match_id)
        predicted_squad_id = int(request.predicted_squad_id)
        match = self._matches.get(match_id)
        if (
            match is None
            or match.season_id != cur.season_id
            or match.phase != PHASE_KNOCKOUT
            or match.state not in (MATCH_WAITING_OPPONENT, MATCH_READY)
            or match.squad_a_id == 0
            or match.squad_b_id == 0
        ):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_PREDICTION_CLOSED)
        if predicted_squad_id not in (match.squad_a_id, match.squad_b_id):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_PREDICTION_TARGET_INVALID)

        key = (cur.season_id, caller.player_id, match_id)
        existing = self._predictions.get(key)
        created_ms = existing.created_ms if existing is not None else stamp
        self._predictions[key] = _PredictionRecord(
            season_id=cur.season_id,
            player_id=caller.player_id,
            match_id=match_id,
            predicted_squad_id=predicted_squad_id,
            round_index=match.round_index,
            is_third_place=match.is_third_place,
            player_name=self._caller_display_name(caller),
            result_state=PREDICTION_PENDING,
            earned_points=0,
            created_ms=created_ms,
            updated_ms=stamp,
        )
        count_a, count_b = self._prediction_counts(match)
        resp.match_id = match_id
        resp.predicted_squad_id = predicted_squad_id
        resp.predict_count_a = count_a
        resp.predict_count_b = count_b
        return resp

    async def get_tournament_prediction_board(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.GetTournamentPredictionBoardResponse")
        stamp = self._now_ms()
        req_season = int(request.season_id)
        if req_season == 0:
            season = self._ensure_season(stamp)
        else:
            season = self._seasons.get(req_season)
            if season is None:
                return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)

        resp.season_id = season.season_id
        resp.min_hits_for_reward = self.catalog.rule.prediction_reward_min_hits
        resp.server_time_ms = stamp

        # 聚合本季全服玩家神机榜统计：慧眼分降序 -> 命中数降序 -> 最新更新时间升序 -> player_id 升序。
        agg: dict[int, list] = {}  # pid -> [player_name, hit_count, total_preds, total_points, max_updated_ms]
        my_preds: list[_PredictionRecord] = []
        for pred in self._predictions.values():
            if pred.season_id != season.season_id:
                continue
            if pred.player_id == caller.player_id:
                my_preds.append(pred)
            row = agg.get(pred.player_id)
            if row is None:
                row = [pred.player_name, 0, 0, 0, 0]
                agg[pred.player_id] = row
            elif pred.player_name:
                row[0] = pred.player_name
            row[2] += 1
            if pred.result_state == PREDICTION_HIT:
                row[1] += 1
                row[3] += pred.earned_points
            if pred.updated_ms > row[4]:
                row[4] = pred.updated_ms

        ranked = sorted(
            agg.items(),
            key=lambda kv: (-kv[1][3], -kv[1][1], kv[1][4], kv[0]),
        )
        for rank_idx, (pid, row) in enumerate(ranked, start=1):
            if pid == caller.player_id:
                resp.my_rank = rank_idx
                resp.my_hit_count = row[1]
                resp.my_total_points = row[3]
            if rank_idx <= 50:
                resp.top_predictors.add(
                    rank=rank_idx,
                    player_id=pid,
                    player_name=row[0],
                    hit_count=row[1],
                    total_predictions=row[2],
                    total_points=row[3],
                )

        my_preds.sort(
            key=lambda p: (
                p.round_index,
                self._matches[p.match_id].slot_index if p.match_id in self._matches else 0,
                p.match_id,
            )
        )
        for pred in my_preds:
            m = self._matches.get(pred.match_id)
            sq_a = self._squads.get(m.squad_a_id) if m is not None else None
            sq_b = self._squads.get(m.squad_b_id) if m is not None else None
            resp.my_predictions.add(
                match_id=pred.match_id,
                round_index=pred.round_index,
                slot_index=m.slot_index if m is not None else 0,
                is_third_place=pred.is_third_place,
                squad_a_id=m.squad_a_id if m is not None else 0,
                squad_a_name=sq_a.name if sq_a is not None else "",
                squad_b_id=m.squad_b_id if m is not None else 0,
                squad_b_name=sq_b.name if sq_b is not None else "",
                predicted_squad_id=pred.predicted_squad_id,
                winner_squad_id=m.winner_squad_id if m is not None else 0,
                result_state=pred.result_state,
                earned_points=pred.earned_points,
                updated_ms=pred.updated_ms,
            )
        return resp

    # -- 赛果回流、BO3 局间自动开局与四阶段推进（与 Go authority/tournament.go 一致）---------------------

    def _settle_match_predictions(
        self,
        season: _SeasonRecord,
        match: _MatchRecord,
        winner_squad_id: int,
        stamp: int,
    ) -> None:
        if match.phase != PHASE_KNOCKOUT or winner_squad_id <= 0:
            return
        weight = prediction_weight_for_round(season.knockout_team_count, match.round_index, match.is_third_place)
        for pred in self._predictions.values():
            if pred.season_id != match.season_id or pred.match_id != match.match_id:
                continue
            if pred.predicted_squad_id == winner_squad_id:
                pred.result_state = PREDICTION_HIT
                pred.earned_points = weight
            else:
                pred.result_state = PREDICTION_MISS
                pred.earned_points = 0
            pred.updated_ms = stamp

    def apply_battle_result(
        self,
        battle_id: int,
        outcome: int,
        *,
        side_a_metrics: KnockoutTieMetrics | None = None,
        side_b_metrics: KnockoutTieMetrics | None = None,
        settlements: Iterable[Message] = (),
    ) -> bool:
        """幂等消费一场锦标赛终局结果：热身零污染结束、更新海选积分、推进 BO1/BO3 与 MVP/竞猜/季军战/冠军。"""
        if not battle_id:
            return False
        game = self._games_by_battle.get(battle_id)
        if game is not None and game.finished_ms > 0:
            return False
        if game is not None:
            match = self._matches.get(game.match_id)
        else:
            match = next((m for m in self._matches.values() if m.battle_id == battle_id), None)
        if match is None or match.state in (MATCH_FINISHED, MATCH_FORFEIT):
            return False
        stamp = self._now_ms()
        season = self._seasons[match.season_id]
        sq_a = self._squads[match.squad_a_id]
        sq_b = self._squads[match.squad_b_id]
        rule = self.catalog.rule
        game_idx = match.current_game_index or 1

        # V3 模块 D：热身约战零污染隔离，仅归档结束，绝不改动海选积分/淘汰树/奖励资格。
        if match.phase == PHASE_WARMUP:
            win_id = sq_a.squad_id if outcome == OUTCOME_SIDE_A_WIN else (sq_b.squad_id if outcome == OUTCOME_SIDE_B_WIN else 0)
            if game is not None:
                game.winner_squad_id = win_id
                game.outcome = outcome
                game.finished_ms = stamp
            match.state = MATCH_FINISHED
            match.winner_squad_id = win_id
            match.outcome = outcome
            match.finished_ms = stamp
            match.updated_ms = stamp
            return True

        game_winner_id = 0
        if match.phase == PHASE_QUALIFIER:
            sq_a.qualifier_played += 1
            sq_b.qualifier_played += 1
            if outcome == OUTCOME_SIDE_A_WIN:
                game_winner_id = sq_a.squad_id
                sq_a.qualifier_wins += 1
                sq_a.qualifier_points += rule.qualifier_win_points
                sq_b.qualifier_losses += 1
                sq_b.qualifier_points += rule.qualifier_loss_points
            elif outcome == OUTCOME_SIDE_B_WIN:
                game_winner_id = sq_b.squad_id
                sq_b.qualifier_wins += 1
                sq_b.qualifier_points += rule.qualifier_win_points
                sq_a.qualifier_losses += 1
                sq_a.qualifier_points += rule.qualifier_loss_points
            else:
                sq_a.qualifier_draws += 1
                sq_a.qualifier_points += rule.qualifier_draw_points
                sq_b.qualifier_draws += 1
                sq_b.qualifier_points += rule.qualifier_draw_points
            sq_a.honor_bits = compute_qualifier_honor_bits(
                sq_a.honor_bits, sq_a.qualifier_played, sq_a.qualifier_wins, sq_a.qualifier_losses,
                rule.participation_min_matches,
            )
            sq_b.honor_bits = compute_qualifier_honor_bits(
                sq_b.honor_bits, sq_b.qualifier_played, sq_b.qualifier_wins, sq_b.qualifier_losses,
                rule.participation_min_matches,
            )
            sq_a.updated_ms = stamp
            sq_b.updated_ms = stamp
            if game is None:
                game = _MatchGameRecord(match.match_id, game_idx, match.season_id, match.phase, battle_id, match.current_leyline_element)
                self._match_games[(match.match_id, game_idx)] = game
                self._games_by_battle[battle_id] = game
            game.winner_squad_id = game_winner_id
            game.outcome = outcome
            game.finished_ms = stamp
            match.state = MATCH_FINISHED
            match.winner_squad_id = game_winner_id
            match.outcome = outcome
            match.finished_ms = stamp
            match.updated_ms = stamp
            return True

        # 淘汰赛分支（BO1 / BO3 + 五行灵脉 + 问鼎之星 MVP + 季军战 + 神机竞猜结算）
        if outcome == OUTCOME_SIDE_A_WIN:
            game_winner_id = sq_a.squad_id
        elif outcome == OUTCOME_SIDE_B_WIN:
            game_winner_id = sq_b.squad_id
        else:
            ma = side_a_metrics or KnockoutTieMetrics(squad_id=sq_a.squad_id, seed_rank=sq_a.seed_rank)
            mb = side_b_metrics or KnockoutTieMetrics(squad_id=sq_b.squad_id, seed_rank=sq_b.seed_rank)
            game_winner_id = sq_a.squad_id if resolve_knockout_draw_winner(ma, mb) == 0 else sq_b.squad_id

        if game is None:
            game = _MatchGameRecord(match.match_id, game_idx, match.season_id, match.phase, battle_id, match.current_leyline_element)
            self._match_games[(match.match_id, game_idx)] = game
            self._games_by_battle[battle_id] = game
        game.winner_squad_id = game_winner_id
        game.outcome = outcome
        game.finished_ms = stamp

        # V3 模块 F：评定单局问鼎之星（MVP），BO3 保留最高表现分。
        winner_team_idx = 0 if game_winner_id == sq_a.squad_id else 1
        mvp = compute_match_mvp(settlements, winner_team_idx)
        if mvp is not None and (match.mvp_player_id == 0 or mvp.score > match.mvp_score):
            mem = self._members.get((match.season_id, mvp.player_id))
            match.mvp_player_id = mvp.player_id
            match.mvp_player_name = mem.player_name if mem is not None and mem.player_name else f"P{mvp.player_id}"
            match.mvp_squad_id = sq_a.squad_id if mvp.team_index == 0 else sq_b.squad_id
            match.mvp_score = mvp.score
            match.mvp_damage = mvp.damage
            match.mvp_healing = mvp.healing
            match.mvp_controls = mvp.controls

        if game_winner_id == sq_a.squad_id:
            match.wins_a += 1
        else:
            match.wins_b += 1

        best_of = match.best_of or 1
        wins_needed = best_of // 2 + 1
        if match.wins_a >= wins_needed or match.wins_b >= wins_needed:
            series_winner = sq_a.squad_id if match.wins_a >= wins_needed else sq_b.squad_id
            loser = sq_b if series_winner == sq_a.squad_id else sq_a
            winner_sq = self._squads[series_winner]
            win_games = match.wins_a if series_winner == sq_a.squad_id else match.wins_b
            lose_games = match.wins_b if series_winner == sq_a.squad_id else match.wins_a
            total_rounds = total_knockout_rounds(season.knockout_team_count)
            is_final_champion = (not match.is_third_place) and match.round_index >= total_rounds
            winner_sq.honor_bits = compute_knockout_series_honor_bits(
                winner_sq.honor_bits, best_of, win_games, lose_games, is_final_champion,
            )
            self._advance_knockout_winner(season, match, series_winner, loser, stamp)
            match.state = MATCH_FINISHED
            match.winner_squad_id = series_winner
            match.outcome = outcome
            match.intermission_deadline_ms = 0
            match.finished_ms = stamp
            match.updated_ms = stamp
        else:
            next_game = game_idx + 1
            next_leyline = leyline_for_game(match.leyline_sequence, next_game)
            inter_sec = rule.intermission_seconds or DEFAULT_INTERMISSION_SEC
            match.state = MATCH_INTERMISSION
            match.current_game_index = next_game
            match.current_leyline_element = next_leyline
            match.ready_a = False
            match.ready_b = False
            match.intermission_deadline_ms = stamp + inter_sec * 1000
            match.outcome = outcome
            match.updated_ms = stamp
        return True

    def take_due_intermission_launches(self, now_override_ms: int | None = None) -> list[tuple[int, int, Message]]:
        """扫描 BO3 局间换阵倒计时已到期的淘汰赛对局，沿用上局阵容自动开打下一局，返回 [(match_id, battle_id, TournamentBattleContext)]。"""
        stamp = self._now_ms() if now_override_ms is None else int(now_override_ms)
        due = [
            m for m in self._matches.values()
            if m.phase == PHASE_KNOCKOUT
            and m.state == MATCH_INTERMISSION
            and 0 < m.intermission_deadline_ms <= stamp
            and m.squad_a_id > 0
            and m.squad_b_id > 0
        ]
        due.sort(key=lambda m: m.match_id)
        launched: list[tuple[int, int, Message]] = []
        for m in due[:16]:
            season = self._seasons.get(m.season_id)
            if season is None:
                continue
            if len(m.lineup_a) != season.battle_team_size:
                err_a, lu_a = self._resolve_lineup(m.squad_a_id, [], season.battle_team_size)
                if err_a:
                    continue
                m.lineup_a = lu_a
            if len(m.lineup_b) != season.battle_team_size:
                err_b, lu_b = self._resolve_lineup(m.squad_b_id, [], season.battle_team_size)
                if err_b:
                    continue
                m.lineup_b = lu_b
            m.ready_a = True
            m.ready_b = True
            m.battle_id = self._alloc_battle_id()
            m.state = MATCH_IN_BATTLE
            m.featured_spectate = True
            m.intermission_deadline_ms = 0
            m.updated_ms = stamp
            ctx = self._record_game_launch(m, m.battle_id)
            launched.append((m.match_id, m.battle_id, ctx))
        return launched

    def _find_third_place_match(self, season_id: int) -> _MatchRecord | None:
        return next(
            (m for m in self._matches.values() if m.season_id == season_id and m.phase == PHASE_KNOCKOUT and m.is_third_place),
            None,
        )

    def _advance_knockout_winner(
        self,
        season: _SeasonRecord,
        match: _MatchRecord,
        winner_id: int,
        loser: _SquadRecord | None,
        stamp: int,
    ) -> None:
        total_rounds = total_knockout_rounds(season.knockout_team_count)
        self._settle_match_predictions(season, match, winner_id, stamp)

        # V3 模块 G：季军争夺战（铜牌战）独立结算第 3 / 第 4 名。
        if match.is_third_place:
            winner = self._squads[winner_id]
            winner.state = SQUAD_ELIMINATED
            winner.final_rank = 3
            winner.updated_ms = stamp
            if loser is not None and loser.squad_id:
                loser.state = SQUAD_ELIMINATED
                loser.final_rank = 4
                loser.updated_ms = stamp
            season.third_place_squad_id = winner_id
            season.third_place_squad_name = winner.name
            season.third_place_emblem_id = winner.emblem_id
            season.updated_ms = stamp
            return

        # 总决赛结束：固化冠军、亚军、成员外观快照、队徽与总决赛 MVP。
        if match.round_index >= total_rounds:
            if loser is not None and loser.squad_id:
                loser.state = SQUAD_ELIMINATED
                loser.final_rank = 2
                loser.updated_ms = stamp
            winner = self._squads[winner_id]
            winner.state = SQUAD_CHAMPION
            winner.final_rank = 1
            winner.updated_ms = stamp
            members = tuple(self._squad_members(winner_id))
            names = [m.player_name or f"#{m.player_id}" for m in members]
            season.champion_squad_id = winner_id
            season.champion_squad_name = winner.name
            season.champion_member_summary = " / ".join(names)
            season.champion_members = members
            season.champion_emblem_id = winner.emblem_id
            winner.honor_bits |= HONOR_BIT_CHAMPION
            season.champion_honor_bits = winner.honor_bits
            if match.mvp_player_id > 0:
                season.final_mvp_player_id = match.mvp_player_id
                season.final_mvp_player_name = match.mvp_player_name
                season.final_mvp_score = match.mvp_score
            season.updated_ms = stamp
            return

        # 半决赛：若存在季军争夺战，败者自动晋级季军争夺战而非直接判并列第 3。
        third_match = self._find_third_place_match(season.season_id) if match.round_index == total_rounds - 1 else None
        if third_match is not None and loser is not None and loser.squad_id:
            if match.slot_index % 2 == 0:
                third_match.squad_a_id = loser.squad_id
            else:
                third_match.squad_b_id = loser.squad_id
            third_match.updated_ms = stamp
        elif loser is not None and loser.squad_id:
            loser.state = SQUAD_ELIMINATED
            loser.final_rank = rank_for_eliminated_round(season.knockout_team_count, match.round_index)
            loser.updated_ms = stamp

        next_round = match.round_index + 1
        next_slot = match.slot_index // 2
        for cand in self._matches.values():
            if (
                cand.season_id == season.season_id
                and cand.phase == PHASE_KNOCKOUT
                and not cand.is_third_place
                and cand.round_index == next_round
                and cand.slot_index == next_slot
            ):
                if match.slot_index % 2 == 0:
                    cand.squad_a_id = winner_id
                else:
                    cand.squad_b_id = winner_id
                cand.updated_ms = stamp
                break

    def advance_season(self, now_override_ms: int | None = None) -> int:
        """可恢复推进锦标赛四阶段状态机（REGISTRATION -> QUALIFIER -> KNOCKOUT -> SETTLED -> 下届）。"""
        stamp = self._now_ms() if now_override_ms is None else int(now_override_ms)
        cur = self._ensure_season(stamp)
        if cur.stage == STAGE_REGISTRATION:
            if stamp < cur.qualifier_start_ms:
                return cur.stage
            for sq in self._squads.values():
                if sq.season_id == cur.season_id and sq.state == SQUAD_DRAFT:
                    if sq.member_count >= cur.battle_team_size:
                        sq.state = SQUAD_REGISTERED
                        sq.registered_ms = stamp
                    else:
                        sq.state = SQUAD_DISBANDED
                    sq.updated_ms = stamp
            for inv in self._invites.values():
                if inv.season_id == cur.season_id and inv.state == INVITE_PENDING:
                    inv.state = INVITE_EXPIRED
            cur.stage = STAGE_QUALIFIER
            cur.updated_ms = stamp
            return cur.stage

        if cur.stage == STAGE_QUALIFIER:
            if stamp < cur.knockout_start_ms:
                return cur.stage
            for m in self._matches.values():
                if m.season_id == cur.season_id and m.phase in (PHASE_QUALIFIER, PHASE_WARMUP) and m.state == MATCH_WAITING_OPPONENT:
                    m.state = MATCH_FORFEIT
                    m.updated_ms = stamp
            reg_squads = [
                sq for sq in self._squads.values()
                if sq.season_id == cur.season_id and sq.state == SQUAD_REGISTERED
            ]
            reg_squads.sort(key=self._standings_sort_key)
            rule = self.catalog.rule
            bracket_size = knockout_bracket_size(len(reg_squads), rule.knockout_max_teams)
            for idx, sq in enumerate(reg_squads, start=1):
                sq.seed_rank = idx
                if bracket_size == 0 or idx > bracket_size:
                    sq.state = SQUAD_ELIMINATED
                    sq.final_rank = idx
                sq.updated_ms = stamp

            cur.knockout_team_count = bracket_size
            if bracket_size > 0:
                total_rounds = total_knockout_rounds(bracket_size)
                round_window_ms = (rule.knockout_round_timeout_seconds or 14400) * 1000
                pairs = seed_pairs(bracket_size)
                round_matches: dict[int, list[_MatchRecord]] = {}
                for r in range(1, total_rounds + 1):
                    slots = bracket_size >> r
                    r_start = stamp + (r - 1) * round_window_ms
                    r_deadline = r_start + round_window_ms
                    round_matches[r] = []
                    best_of = best_of_for_knockout(bracket_size, r, rule.bo3_top4_enabled)
                    for slot in range(slots):
                        mid = self._alloc_id()
                        seq = draw_leylines(cur.season_id, mid, best_of)
                        rec = _MatchRecord(
                            match_id=mid,
                            season_id=cur.season_id,
                            phase=PHASE_KNOCKOUT,
                            round_index=r,
                            slot_index=slot,
                            squad_a_id=0,
                            featured_spectate=True,
                            scheduled_start_ms=r_start,
                            deadline_ms=r_deadline,
                            updated_ms=stamp,
                            best_of=best_of,
                            current_game_index=1,
                            current_leyline_element=leyline_for_game(seq, 1),
                            leyline_sequence=seq,
                        )
                        if r == 1:
                            sa, sb = pairs[slot]
                            if sa <= len(reg_squads):
                                rec.squad_a_id = reg_squads[sa - 1].squad_id
                            if sb <= len(reg_squads):
                                rec.squad_b_id = reg_squads[sb - 1].squad_id
                            if rec.squad_a_id and not rec.squad_b_id:
                                rec.state = MATCH_BYE
                                rec.winner_squad_id = rec.squad_a_id
                                rec.finished_ms = stamp
                            elif rec.squad_b_id and not rec.squad_a_id:
                                rec.state = MATCH_BYE
                                rec.winner_squad_id = rec.squad_b_id
                                rec.finished_ms = stamp
                        round_matches[r].append(rec)
                        self._matches[rec.match_id] = rec
                # V3 模块 G：4 强及以上且启用季军战时，末轮新增 slot=1 铜牌战（独立 BO3 + 灵脉）。
                if bracket_size >= 4 and rule.third_place_match_enabled:
                    third_mid = self._alloc_id()
                    r_start = stamp + (total_rounds - 1) * round_window_ms
                    r_deadline = r_start + round_window_ms
                    third_bo = best_of_for_knockout(bracket_size, total_rounds, rule.bo3_top4_enabled)
                    third_seq = draw_leylines(cur.season_id, third_mid, third_bo)
                    third_rec = _MatchRecord(
                        match_id=third_mid,
                        season_id=cur.season_id,
                        phase=PHASE_KNOCKOUT,
                        round_index=total_rounds,
                        slot_index=1,
                        squad_a_id=0,
                        featured_spectate=True,
                        scheduled_start_ms=r_start,
                        deadline_ms=r_deadline,
                        updated_ms=stamp,
                        best_of=third_bo,
                        current_game_index=1,
                        current_leyline_element=leyline_for_game(third_seq, 1),
                        leyline_sequence=third_seq,
                        is_third_place=True,
                    )
                    self._matches[third_mid] = third_rec
                if total_rounds >= 2:
                    for m in round_matches[1]:
                        if m.state == MATCH_BYE and m.winner_squad_id:
                            target = round_matches[2][m.slot_index // 2]
                            if m.slot_index % 2 == 0:
                                target.squad_a_id = m.winner_squad_id
                            else:
                                target.squad_b_id = m.winner_squad_id
            cur.stage = STAGE_KNOCKOUT
            cur.updated_ms = stamp
            return cur.stage

        if cur.stage == STAGE_KNOCKOUT:
            # 1. 到期未打淘汰赛弃权裁决（含季军争夺战）。
            overdue = [
                m for m in self._matches.values()
                if m.season_id == cur.season_id
                and m.phase == PHASE_KNOCKOUT
                and m.state in (MATCH_WAITING_OPPONENT, MATCH_READY)
                and 0 < m.deadline_ms <= stamp
            ]
            overdue.sort(key=lambda m: (m.round_index, m.slot_index))
            for m in overdue:
                if not m.squad_a_id and not m.squad_b_id:
                    continue
                loser_id = 0
                if m.squad_a_id and not m.squad_b_id:
                    winner_id = m.squad_a_id
                elif m.squad_b_id and not m.squad_a_id:
                    winner_id = m.squad_b_id
                elif m.ready_a and not m.ready_b:
                    winner_id, loser_id = m.squad_a_id, m.squad_b_id
                elif m.ready_b and not m.ready_a:
                    winner_id, loser_id = m.squad_b_id, m.squad_a_id
                else:
                    sa = self._squads[m.squad_a_id].seed_rank
                    sb = self._squads[m.squad_b_id].seed_rank
                    if sb == 0 or (sa > 0 and sa <= sb):
                        winner_id, loser_id = m.squad_a_id, m.squad_b_id
                    else:
                        winner_id, loser_id = m.squad_b_id, m.squad_a_id
                self._advance_knockout_winner(
                    cur, m, winner_id, self._squads.get(loser_id) if loser_id else None, stamp,
                )
                m.state = MATCH_FORFEIT
                m.winner_squad_id = winner_id
                m.finished_ms = stamp
                m.updated_ms = stamp

            # 2. 封榜门禁：须总冠军已出且季军战已完成（或到达 settle_start_ms 强制结算）。
            if cur.knockout_team_count > 0 and stamp < cur.settle_start_ms:
                if cur.champion_squad_id == 0:
                    return cur.stage
                third = self._find_third_place_match(cur.season_id)
                if (
                    third is not None
                    and third.squad_a_id > 0
                    and third.squad_b_id > 0
                    and third.state not in (MATCH_FINISHED, MATCH_BYE, MATCH_FORFEIT)
                ):
                    return cur.stage
            self._settle_season(cur, stamp)
            return cur.stage

        if cur.stage == STAGE_SETTLED and stamp >= cur.end_ms:
            next_id = cur.season_id + 1
            if next_id not in self._seasons:
                self._seasons[next_id] = self._build_season(next_id, stamp)
            return self._seasons[next_id].stage
        return cur.stage

    def _settle_season(self, season: _SeasonRecord, stamp: int) -> None:
        squads = [
            sq for sq in self._squads.values()
            if sq.season_id == season.season_id and sq.state in (SQUAD_REGISTERED, SQUAD_ELIMINATED, SQUAD_CHAMPION)
        ]
        top4_ids = {sq.squad_id for sq in squads if 1 <= sq.final_rank <= 4}
        for sq in squads:
            tier = self.catalog.reward_for_placement(sq.final_rank, sq.qualifier_played)
            if tier is None:
                continue
            for mem in self._squad_members(sq.squad_id):
                key = (season.season_id, mem.player_id, tier.tier_kind)
                if key not in self._rewards:
                    self._rewards[key] = _RewardRecord(
                        reward_id=self._alloc_id(),
                        season_id=season.season_id,
                        player_id=mem.player_id,
                        tier_kind=tier.tier_kind,
                        squad_id=sq.squad_id,
                        final_rank=sq.final_rank,
                        tier_id=tier.id,
                        gold_amount=tier.gold_amount,
                        items=tier.items,
                        title_code=tier.title_code,
                        badge_code=tier.badge_code,
                        delivery_state=DELIVERY_CLAIMABLE,
                        updated_ms=stamp,
                    )
        cheer_tier = self.catalog.cheer_reward()
        if cheer_tier is not None and top4_ids:
            for cheer in self._cheers.values():
                if cheer.season_id == season.season_id and not cheer.rewarded and cheer.squad_id in top4_ids:
                    key = (season.season_id, cheer.player_id, cheer_tier.tier_kind)
                    if key not in self._rewards:
                        self._rewards[key] = _RewardRecord(
                            reward_id=self._alloc_id(),
                            season_id=season.season_id,
                            player_id=cheer.player_id,
                            tier_kind=cheer_tier.tier_kind,
                            squad_id=cheer.squad_id,
                            final_rank=0,
                            tier_id=cheer_tier.id,
                            gold_amount=cheer_tier.gold_amount,
                            items=cheer_tier.items,
                            title_code=cheer_tier.title_code,
                            badge_code=cheer_tier.badge_code,
                            delivery_state=DELIVERY_CLAIMABLE,
                            updated_ms=stamp,
                        )
                    cheer.rewarded = True
        # V3 模块 E：神机竞猜命中达标（>= prediction_reward_min_hits）幂等落账绑定纪念礼。
        if self.catalog.rule.prediction_enabled:
            pred_tier = self.catalog.prediction_reward()
            min_hits = max(1, self.catalog.rule.prediction_reward_min_hits)
            if pred_tier is not None:
                hits_by_player: dict[int, int] = {}
                for pred in self._predictions.values():
                    if pred.season_id == season.season_id and pred.result_state == PREDICTION_HIT:
                        hits_by_player[pred.player_id] = hits_by_player.get(pred.player_id, 0) + 1
                for pid, hit_count in hits_by_player.items():
                    if hit_count < min_hits:
                        continue
                    key = (season.season_id, pid, pred_tier.tier_kind)
                    if key not in self._rewards:
                        self._rewards[key] = _RewardRecord(
                            reward_id=self._alloc_id(),
                            season_id=season.season_id,
                            player_id=pid,
                            tier_kind=pred_tier.tier_kind,
                            squad_id=0,
                            final_rank=0,
                            tier_id=pred_tier.id,
                            gold_amount=pred_tier.gold_amount,
                            items=pred_tier.items,
                            title_code=pred_tier.title_code,
                            badge_code=pred_tier.badge_code,
                            delivery_state=DELIVERY_CLAIMABLE,
                            updated_ms=stamp,
                        )
        season.stage = STAGE_SETTLED
        season.settled_ms = stamp
        season.updated_ms = stamp
        logger.info("Tournament season settled season=%d champion=%d", season.season_id, season.champion_squad_id)

    # -- V4 模块 H / I 客户端 RPC 实现 ----------------------------------------------------------------

    def ban_tournament_leyline(self, caller: CallerIdentity, request: Message) -> Message:
        """V4 模块 H（534）：BO3 淘汰赛首局开战前，参战两队队长各禁 1 条五行灵脉并立即重算灵脉序列。"""
        resp = self.protocol.message("match.BanTournamentLeylineResponse")
        if not self.catalog.rule.enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_OPEN)
        if not self.catalog.rule.leyline_ban_enabled:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        match_id = int(request.match_id)
        leyline = int(request.leyline_element)
        if caller.player_id <= 0 or match_id <= 0 or not is_valid_leyline_element(leyline):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_MATCH_NOT_READY)
        stamp = self._now_ms()
        cur = self._ensure_season(stamp)
        if cur.stage != STAGE_KNOCKOUT:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_STAGE_CLOSED)
        m = self._matches.get(match_id)
        if m is None or m.season_id != cur.season_id or m.phase != PHASE_KNOCKOUT:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_MATCH_NOT_READY)
        if (
            m.best_of < 3
            or m.wins_a > 0
            or m.wins_b > 0
            or m.squad_a_id == 0
            or m.squad_b_id == 0
            or m.state not in (MATCH_WAITING_OPPONENT, MATCH_READY)
        ):
            return set_tip(resp, ERR_MATCH_TOURNAMENT_MATCH_NOT_READY)
        sq_a = self._squads.get(m.squad_a_id)
        sq_b = self._squads.get(m.squad_b_id)
        if sq_a is None or sq_b is None:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        if caller.player_id == sq_a.captain_player_id:
            m.banned_leyline_a = leyline
        elif caller.player_id == sq_b.captain_player_id:
            m.banned_leyline_b = leyline
        else:
            return set_tip(resp, ERR_MATCH_TOURNAMENT_NOT_CAPTAIN)
        seq = draw_leylines_with_bans(m.season_id, m.match_id, m.best_of, m.banned_leyline_a, m.banned_leyline_b)
        m.leyline_sequence = seq
        m.current_leyline_element = leyline_for_game(seq, 1)
        m.updated_ms = stamp
        resp.match.CopyFrom(self._match_view(m, viewer_player_id=caller.player_id))
        return resp

    def get_tournament_highlights(self, caller: CallerIdentity, request: Message) -> Message:
        """V4 模块 I（535）：从指定赛季已完赛的公开淘汰赛中确定性策展 Top 10 本季名局录。"""
        resp = self.protocol.message("match.GetTournamentHighlightsResponse")
        stamp = self._now_ms()
        season_id = int(request.season_id)
        if season_id == 0:
            season = self._ensure_season(stamp)
        else:
            season = self._seasons.get(season_id)
            if season is None:
                return set_tip(resp, ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND)
        resp.season_id = season.season_id
        resp.server_time_ms = stamp
        if not self.catalog.rule.public_knockout_replay:
            return resp

        finished = [
            m for m in self._matches.values()
            if m.season_id == season.season_id and m.phase == PHASE_KNOCKOUT and m.state == MATCH_FINISHED
        ]
        max_round = max((m.round_index for m in finished), default=0)
        candidates: list[tuple[int, int, int, int, int, str, _MatchRecord]] = []
        for m in finished:
            game_ids = self._game_battle_ids(m.match_id)
            if m.battle_id == 0 and not game_ids:
                continue
            tag, label, prio = classify_highlight_tag(
                m.is_third_place, m.round_index, max_round, m.best_of, m.wins_a, m.wins_b,
            )
            candidates.append((prio, -m.mvp_score, -m.finished_ms, m.match_id, tag, label, m))
        candidates.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        for _, _, _, _, tag, label, m in candidates[:10]:
            entry = resp.highlights.add(tag=tag, tag_label=label)
            entry.match.CopyFrom(self._match_view(m, viewer_player_id=caller.player_id))
        return resp
