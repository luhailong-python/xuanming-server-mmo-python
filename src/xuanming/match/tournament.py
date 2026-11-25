"""全服赛季战队锦标赛《群仙问道赛》V1：规则目录、四阶段推进、战队/邀请/首发/海选/淘汰/助威与幂等发奖。"""

from __future__ import annotations

import logging
import time
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass, field

from google.protobuf.message import Message

from ..protocol import Protocol
from .errors import (
    ERR_MATCH_INTERNAL,
    ERR_MATCH_TOURNAMENT_ALREADY_CHEERED,
    ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD,
    ERR_MATCH_TOURNAMENT_INVITE_NOT_FOUND,
    ERR_MATCH_TOURNAMENT_LINEUP_INVALID,
    ERR_MATCH_TOURNAMENT_MATCH_NOT_READY,
    ERR_MATCH_TOURNAMENT_MEMBER_NOT_ENOUGH,
    ERR_MATCH_TOURNAMENT_NOT_CAPTAIN,
    ERR_MATCH_TOURNAMENT_NOT_OPEN,
    ERR_MATCH_TOURNAMENT_QUALIFIER_LIMIT,
    ERR_MATCH_TOURNAMENT_REWARD_UNAVAILABLE,
    ERR_MATCH_TOURNAMENT_SQUAD_FULL,
    ERR_MATCH_TOURNAMENT_SQUAD_LOCKED,
    ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID,
    ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND,
    ERR_MATCH_TOURNAMENT_STAGE_CLOSED,
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

MATCH_WAITING_OPPONENT = 1
MATCH_READY = 2
MATCH_IN_BATTLE = 3
MATCH_FINISHED = 4
MATCH_BYE = 5
MATCH_FORFEIT = 6

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

DELIVERY_CLAIMABLE = 1
DELIVERY_CLAIMED = 2
DELIVERY_ABANDONED = 3

OUTCOME_SIDE_A_WIN = 1
OUTCOME_SIDE_B_WIN = 2
OUTCOME_DRAW = 3

MIN_SQUAD_NAME_CHARS = 2
MAX_SQUAD_NAME_CHARS = 12
MILLIS_PER_DAY = 86_400_000


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


# -- 配置目录（读取 ConfigTables.TournamentRule / TournamentReward，无表时回落默认首测目录）------------


@dataclass(frozen=True, slots=True)
class TournamentRuleSpec:
    id: int = 1
    rule_version: int = 1
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
    knockout_round_timeout_seconds: int = 1800
    qualifier_win_points: int = 3
    qualifier_draw_points: int = 1
    qualifier_loss_points: int = 0
    invite_ttl_seconds: int = 259200
    desc: str = ""


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
            ),
        )

    def reward_for_placement(self, final_rank: int, qualifier_played: int) -> TournamentRewardSpec | None:
        if final_rank > 0:
            for row in self.ordered_rewards:
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


@dataclass
class _CheerRecord:
    season_id: int
    player_id: int
    squad_id: int
    rewarded: bool = False
    created_ms: int = 0


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
    """全服赛季战队锦标赛《群仙问道赛》权威服务（支持配置表目录、内存持久态与全部 11 条客户端 RPC）。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        tables=None,
        catalog: TournamentCatalog | None = None,
        clock_ms: Callable[[], int] | None = None,
        battle_id_alloc: Callable[[], int] | None = None,
    ) -> None:
        self.protocol = protocol
        self.catalog = catalog if catalog is not None else TournamentCatalog.from_tables(tables)
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._next_id = 100_000
        self._battle_id_alloc = battle_id_alloc

        self._seasons: dict[int, _SeasonRecord] = {}
        self._squads: dict[int, _SquadRecord] = {}
        self._members: dict[tuple[int, int], _MemberRecord] = {}  # (season_id, player_id)
        self._invites: dict[tuple[int, int], _InviteRecord] = {}  # (squad_id, invitee_player_id)
        self._matches: dict[int, _MatchRecord] = {}
        self._cheers: dict[tuple[int, int], _CheerRecord] = {}  # (season_id, player_id)
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

    # -- Proto 视图构造 ------------------------------------------------------------------------------

    def _season_view(self, season: _SeasonRecord, registered_count: int) -> Message:
        rule = self.catalog.rule
        return self.protocol.message(
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
        )

    def _squad_view(self, squad: _SquadRecord) -> Message:
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
        )
        for m in self._squad_members(squad.squad_id):
            view.members.add(
                player_id=m.player_id,
                player_name=m.player_name,
                zone_id=m.zone_id,
                role=m.role,
                starter_slot=m.starter_slot,
                joined_ms=m.joined_ms,
            )
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

    def _match_view(self, m: _MatchRecord) -> Message:
        sq_a = self._squads.get(m.squad_a_id)
        sq_b = self._squads.get(m.squad_b_id)
        return self.protocol.message(
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
        )

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

    # -- 11 条客户端 RPC -----------------------------------------------------------------------------

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
                    and m.state in (MATCH_WAITING_OPPONENT, MATCH_READY, MATCH_IN_BATTLE)
                ]
                if active:
                    active.sort(key=lambda m: (m.phase, m.round_index, m.match_id), reverse=True)
                    resp.my_active_match.CopyFrom(self._match_view(active[0]))

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
            if m.season_id == cur.season_id and m.state == MATCH_IN_BATTLE
        ]
        featured.sort(key=lambda m: (1 if m.featured_spectate else 0, m.phase, m.round_index), reverse=True)
        for m in featured[:10]:
            resp.featured_matches.add().CopyFrom(self._match_view(m))

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
            resp.knockout_bracket.add().CopyFrom(self._match_view(m))
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

        # 淘汰赛分支：确认对阵就绪，双方都就绪立即分配 battle_id 进入激战并标全服精选观战。
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
                    and m.state in (MATCH_WAITING_OPPONENT, MATCH_READY)
                ]
                cands.sort(key=lambda m: (m.round_index, m.slot_index))
                if cands:
                    target_match = cands[0]
            if (
                target_match is None
                or target_match.state not in (MATCH_WAITING_OPPONENT, MATCH_READY)
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
                resp.battle_id = target_match.battle_id
            resp.match.CopyFrom(self._match_view(target_match))
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
                resp.match.CopyFrom(self._match_view(existing))
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
            resp.match.CopyFrom(self._match_view(paired))
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
        resp.match.CopyFrom(self._match_view(created))
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

    # -- 赛果回流与四阶段推进（与 Go authority/tournament.go 一致）--------------------------------------

    def apply_battle_result(
        self,
        battle_id: int,
        outcome: int,
        *,
        side_a_metrics: KnockoutTieMetrics | None = None,
        side_b_metrics: KnockoutTieMetrics | None = None,
    ) -> bool:
        """幂等消费一场锦标赛终局结果：更新海选积分或推进淘汰赛晋级与冠军。"""
        if not battle_id:
            return False
        match = next((m for m in self._matches.values() if m.battle_id == battle_id), None)
        if match is None or match.state in (MATCH_FINISHED, MATCH_FORFEIT):
            return False
        stamp = self._now_ms()
        season = self._seasons[match.season_id]
        sq_a = self._squads[match.squad_a_id]
        sq_b = self._squads[match.squad_b_id]
        rule = self.catalog.rule
        winner_id = 0

        if match.phase == PHASE_QUALIFIER:
            sq_a.qualifier_played += 1
            sq_b.qualifier_played += 1
            if outcome == OUTCOME_SIDE_A_WIN:
                winner_id = sq_a.squad_id
                sq_a.qualifier_wins += 1
                sq_a.qualifier_points += rule.qualifier_win_points
                sq_b.qualifier_losses += 1
                sq_b.qualifier_points += rule.qualifier_loss_points
            elif outcome == OUTCOME_SIDE_B_WIN:
                winner_id = sq_b.squad_id
                sq_b.qualifier_wins += 1
                sq_b.qualifier_points += rule.qualifier_win_points
                sq_a.qualifier_losses += 1
                sq_a.qualifier_points += rule.qualifier_loss_points
            else:
                sq_a.qualifier_draws += 1
                sq_a.qualifier_points += rule.qualifier_draw_points
                sq_b.qualifier_draws += 1
                sq_b.qualifier_points += rule.qualifier_draw_points
            sq_a.updated_ms = stamp
            sq_b.updated_ms = stamp
        else:
            if outcome == OUTCOME_SIDE_A_WIN:
                winner_id = sq_a.squad_id
            elif outcome == OUTCOME_SIDE_B_WIN:
                winner_id = sq_b.squad_id
            else:
                ma = side_a_metrics or KnockoutTieMetrics(squad_id=sq_a.squad_id, seed_rank=sq_a.seed_rank)
                mb = side_b_metrics or KnockoutTieMetrics(squad_id=sq_b.squad_id, seed_rank=sq_b.seed_rank)
                winner_id = sq_a.squad_id if resolve_knockout_draw_winner(ma, mb) == 0 else sq_b.squad_id
            loser = sq_b if winner_id == sq_a.squad_id else sq_a
            self._advance_knockout_winner(season, match, winner_id, loser, stamp)

        match.state = MATCH_FINISHED
        match.winner_squad_id = winner_id
        match.outcome = outcome
        match.finished_ms = stamp
        match.updated_ms = stamp
        return True

    def _advance_knockout_winner(
        self,
        season: _SeasonRecord,
        match: _MatchRecord,
        winner_id: int,
        loser: _SquadRecord | None,
        stamp: int,
    ) -> None:
        total_rounds = total_knockout_rounds(season.knockout_team_count)
        elim_rank = rank_for_eliminated_round(season.knockout_team_count, match.round_index)
        if loser is not None and loser.squad_id:
            loser.state = SQUAD_ELIMINATED
            loser.final_rank = elim_rank
            loser.updated_ms = stamp
        if match.round_index >= total_rounds:
            winner = self._squads[winner_id]
            winner.state = SQUAD_CHAMPION
            winner.final_rank = 1
            winner.updated_ms = stamp
            names = [m.player_name or f"#{m.player_id}" for m in self._squad_members(winner_id)]
            season.champion_squad_id = winner_id
            season.champion_squad_name = winner.name
            season.champion_member_summary = " / ".join(names)
            season.updated_ms = stamp
            return
        next_round = match.round_index + 1
        next_slot = match.slot_index // 2
        for cand in self._matches.values():
            if (
                cand.season_id == season.season_id
                and cand.phase == PHASE_KNOCKOUT
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
                if m.season_id == cur.season_id and m.phase == PHASE_QUALIFIER and m.state == MATCH_WAITING_OPPONENT:
                    m.state = MATCH_FORFEIT
                    m.updated_ms = stamp
            reg_squads = [
                sq for sq in self._squads.values()
                if sq.season_id == cur.season_id and sq.state == SQUAD_REGISTERED
            ]
            reg_squads.sort(key=self._standings_sort_key)
            bracket_size = knockout_bracket_size(len(reg_squads), self.catalog.rule.knockout_max_teams)
            for idx, sq in enumerate(reg_squads, start=1):
                sq.seed_rank = idx
                if bracket_size == 0 or idx > bracket_size:
                    sq.state = SQUAD_ELIMINATED
                    sq.final_rank = idx
                sq.updated_ms = stamp

            cur.knockout_team_count = bracket_size
            if bracket_size > 0:
                total_rounds = total_knockout_rounds(bracket_size)
                round_window_ms = (self.catalog.rule.knockout_round_timeout_seconds or 1800) * 1000
                pairs = seed_pairs(bracket_size)
                round_matches: dict[int, list[_MatchRecord]] = {}
                for r in range(1, total_rounds + 1):
                    slots = bracket_size >> r
                    r_start = stamp + (r - 1) * round_window_ms
                    r_deadline = r_start + round_window_ms
                    round_matches[r] = []
                    for slot in range(slots):
                        rec = _MatchRecord(
                            match_id=self._alloc_id(),
                            season_id=cur.season_id,
                            phase=PHASE_KNOCKOUT,
                            round_index=r,
                            slot_index=slot,
                            squad_a_id=0,
                            featured_spectate=True,
                            scheduled_start_ms=r_start,
                            deadline_ms=r_deadline,
                            updated_ms=stamp,
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
            # 1. 到期未打淘汰赛弃权裁决。
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

            # 2. 已产生总冠军、无淘汰席位或到达结算时刻 -> 幂等发奖结算。
            if cur.knockout_team_count > 0 and cur.champion_squad_id == 0 and stamp < cur.settle_start_ms:
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
        season.stage = STAGE_SETTLED
        season.settled_ms = stamp
        season.updated_ms = stamp
        logger.info("Tournament season settled season=%d champion=%d", season.season_id, season.champion_squad_id)
