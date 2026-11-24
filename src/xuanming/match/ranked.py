"""论剑排位首发 V1 领域规则、预设目录与客户端服务逻辑。

对应规格 ``docs/design/ranked-play-season-ui-candidate.md`` 与 ``proto/match/match_service.proto``、
``proto/match/match_authority.proto``、``proto/team/team.proto``。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache

from google.protobuf.message import DecodeError, Message

from ..data.config_tables import ConfigTableError, ConfigTables
from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_INTERNAL,
    ERR_MATCH_RANKED_PRESET_INVALID,
    ERR_MATCH_RANKED_RULE_VERSION_MISMATCH,
    ERR_MATCH_RANKED_SEASON_SETTLING,
    ERR_TEAM_RANKED_PRESET_INVALID,
    TeamBusinessError,
    set_tip,
)
from .model import MODE_PVP_TEAM, CallerIdentity
from .rating import DEFAULT_RATING_CENTI

logger = logging.getLogger(__name__)

# -- 枚举常量（与 proto/match/match_service.proto 对齐）----------------------------------------------

RANKED_TIER_UNSPECIFIED = 0
RANKED_TIER_CHUSHI = 1
RANKED_TIER_QINGFENG = 2
RANKED_TIER_YUNZHANG = 3
RANKED_TIER_TIANSHU = 4
RANKED_TIER_WUSHUANG = 5

RANKED_SEASON_UNSPECIFIED = 0
RANKED_SEASON_ACTIVE = 1
RANKED_SEASON_SETTLING = 2
RANKED_SEASON_CLOSED = 3

RANKED_REWARD_UNSPECIFIED = 0
RANKED_REWARD_AVATAR_PATTERN = 1
RANKED_REWARD_CARD_BACKGROUND = 2
RANKED_REWARD_TALISMAN = 3
RANKED_REWARD_TIER_BADGE = 4
RANKED_REWARD_TIER_TITLE = 5
RANKED_REWARD_CHAMPION_INSCRIPTION = 6

RANKED_MATCH_RATING_UNSPECIFIED = 0
RANKED_MATCH_RATING_PENDING = 1
RANKED_MATCH_RATING_CONFIRMED = 2
RANKED_MATCH_RATING_VOID = 3
RANKED_MATCH_RATING_ZERO_ROUND = 4

ROLE_KIND_CHARACTER = 1
ROLE_KIND_PET = 2

# 里程碑与荣誉位图（与 match_authority.proto 注释一致，纯布尔状态用 bit 存储）
MILESTONE_BIT_AVATAR = 1 << 0
MILESTONE_BIT_CARD = 1 << 1
MILESTONE_BIT_TALISMAN = 1 << 2

HONOR_BIT_TIER_BADGE = 1 << 0
HONOR_BIT_TIER_TITLE = 1 << 1
HONOR_BIT_CHAMPION = 1 << 2

DEFAULT_CHARACTER_PRESET_ID = 101
DEFAULT_PET_PRESET_ID = 203
DEFAULT_RULE_VERSION = 1
DEFAULT_BATTLE_CONFIG_ID = 5
DEFAULT_SEASON_DURATION_DAYS = 56
DAY_MS = 24 * 3_600_000


# -- 不可变配置与存储行模型 ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _default_config_tables() -> ConfigTables | None:
    """按需懒加载仓库内置配置表快照；加载异常时返回 None 交由调用方回退内置默认值。"""
    try:
        return ConfigTables()
    except ConfigTableError:
        logger.warning("Failed to load default ConfigTables for ranked rules; falling back to embedded defaults",
                       exc_info=True)
        return None


@dataclass(frozen=True, slots=True)
class RankedPresetSpec:
    """单套官方排位预设规格（与 RankedPreset.xlsx / rankedpreset.json 逐项对齐）。"""

    preset_id: int
    role_kind: int
    code: str
    name: str
    role_tag: str
    pet_table_id: int
    weights: tuple[int, int, int, int]  # [体质, 灵力, 力量, 敏捷]
    skill_ids: tuple[int, ...]
    pet_entry_ids: tuple[int, ...] = ()
    pet_entry_points: tuple[int, ...] = ()
    sort: int = 0
    desc: str = ""

    @classmethod
    def from_row(cls, row: Message) -> RankedPresetSpec:
        """从 ``RankedPreset`` 配置表行构造不可变预设规格。"""
        return cls(
            preset_id=int(row.id),
            role_kind=int(row.role_kind),
            code=str(row.code),
            name=str(row.name),
            role_tag=str(row.role_tag),
            pet_table_id=int(row.pet_table_id),
            weights=(int(row.weight_con), int(row.weight_spi), int(row.weight_str), int(row.weight_agi)),
            skill_ids=tuple(int(x) for x in row.skill_ids),
            pet_entry_ids=tuple(int(x) for x in row.pet_entry_ids),
            pet_entry_points=tuple(int(x) for x in row.pet_entry_points),
            sort=int(row.sort),
            desc=str(row.desc),
        )

    @classmethod
    def load_from_config_tables(cls, tables: ConfigTables | None = None) -> tuple[RankedPresetSpec, ...]:
        """从 ``ConfigTables`` 的 ``RankedPreset`` 表加载预设列表；缺失或空表时回退内置默认预设。"""
        snapshot = _default_config_tables() if tables is None else tables
        if snapshot is None or "RankedPreset" not in snapshot.tables:
            return OFFICIAL_RANKED_PRESETS
        rows = snapshot["RankedPreset"].rows()
        if not rows:
            return OFFICIAL_RANKED_PRESETS
        ordered = sorted(rows, key=lambda r: (int(r.role_kind), int(r.sort), int(r.id)))
        return tuple(cls.from_row(row) for row in ordered)


@dataclass(frozen=True, slots=True)
class RankedRuleConfig:
    """排位全局规则参数（与 RankedRule.xlsx / rankedrule.json 逐项对齐）。"""

    rule_version: int = DEFAULT_RULE_VERSION
    battle_config_id: int = DEFAULT_BATTLE_CONFIG_ID
    reference_class_id: int = 1
    competitive_level: int = 30
    default_character_preset_id: int = DEFAULT_CHARACTER_PRESET_ID
    default_pet_preset_id: int = DEFAULT_PET_PRESET_ID
    season_duration_days: int = DEFAULT_SEASON_DURATION_DAYS
    wait_prompt_seconds: int = 120
    spread_tier1_seconds: int = 30
    spread_tier1_points: int = 100
    spread_tier2_seconds: int = 90
    spread_tier2_points: int = 200
    spread_tier3_points: int = 300
    milestone_avatar_matches: int = 4
    milestone_card_matches: int = 8
    milestone_talisman_matches: int = 12
    tier_qingfeng_centi: int = 140_000
    tier_yunzhang_centi: int = 155_000
    tier_tianshu_centi: int = 170_000
    tier_wushuang_centi: int = 185_000

    @classmethod
    def from_row(cls, row: Message) -> RankedRuleConfig:
        """从 ``RankedRule`` 配置表行构造全局规则参数；零值字段回退默认值。"""
        defaults = cls()
        return cls(
            rule_version=int(row.rule_version) or defaults.rule_version,
            battle_config_id=int(row.battle_config_id) or defaults.battle_config_id,
            reference_class_id=int(row.reference_class_id) or defaults.reference_class_id,
            competitive_level=int(row.competitive_level) or defaults.competitive_level,
            default_character_preset_id=(
                int(row.default_character_preset_id) or defaults.default_character_preset_id
            ),
            default_pet_preset_id=int(row.default_pet_preset_id) or defaults.default_pet_preset_id,
            season_duration_days=int(row.season_duration_days) or defaults.season_duration_days,
            wait_prompt_seconds=int(row.wait_prompt_seconds) or defaults.wait_prompt_seconds,
            spread_tier1_seconds=int(row.spread_tier1_seconds) or defaults.spread_tier1_seconds,
            spread_tier1_points=int(row.spread_tier1_points) or defaults.spread_tier1_points,
            spread_tier2_seconds=int(row.spread_tier2_seconds) or defaults.spread_tier2_seconds,
            spread_tier2_points=int(row.spread_tier2_points) or defaults.spread_tier2_points,
            spread_tier3_points=int(row.spread_tier3_points) or defaults.spread_tier3_points,
            milestone_avatar_matches=int(row.milestone_avatar_matches) or defaults.milestone_avatar_matches,
            milestone_card_matches=int(row.milestone_card_matches) or defaults.milestone_card_matches,
            milestone_talisman_matches=int(row.milestone_talisman_matches) or defaults.milestone_talisman_matches,
            tier_qingfeng_centi=int(row.tier_qingfeng_centi) or defaults.tier_qingfeng_centi,
            tier_yunzhang_centi=int(row.tier_yunzhang_centi) or defaults.tier_yunzhang_centi,
            tier_tianshu_centi=int(row.tier_tianshu_centi) or defaults.tier_tianshu_centi,
            tier_wushuang_centi=int(row.tier_wushuang_centi) or defaults.tier_wushuang_centi,
        )

    @classmethod
    def from_config_tables(cls, tables: ConfigTables | None = None, *, rule_id: int = 1) -> RankedRuleConfig:
        """从 ``ConfigTables`` 的 ``RankedRule`` 表加载指定行；缺失时回退内置默认值。"""
        snapshot = _default_config_tables() if tables is None else tables
        if snapshot is None or "RankedRule" not in snapshot.tables:
            return cls()
        row = snapshot["RankedRule"].get(rule_id)
        if row is None:
            return cls()
        return cls.from_row(row)


def load_ranked_presets(tables: ConfigTables | None = None) -> tuple[RankedPresetSpec, ...]:
    """从配置表加载论剑排位预设目录，缺失时回退内置 ``OFFICIAL_RANKED_PRESETS``。"""
    return RankedPresetSpec.load_from_config_tables(tables)


OFFICIAL_RANKED_PRESETS: tuple[RankedPresetSpec, ...] = (
    RankedPresetSpec(
        preset_id=101,
        role_kind=ROLE_KIND_CHARACTER,
        code="pozhen",
        name="破阵",
        role_tag="爆发输出",
        pet_table_id=0,
        weights=(1, 0, 3, 1),
        skill_ids=(10002, 10039, 10025),
        sort=1,
        desc="偏物理群体与爆发输出，适合破防后收割",
    ),
    RankedPresetSpec(
        preset_id=102,
        role_kind=ROLE_KIND_CHARACTER,
        code="kongchang",
        name="控场",
        role_tag="先手封控",
        pet_table_id=0,
        weights=(1, 2, 0, 2),
        skill_ids=(10006, 10021, 10034),
        sort=2,
        desc="偏速度与法术控场，先手封法并驱散增益",
    ),
    RankedPresetSpec(
        preset_id=103,
        role_kind=ROLE_KIND_CHARACTER,
        code="shouxin",
        name="守心",
        role_tag="治疗护盾",
        pet_table_id=0,
        weights=(2, 2, 0, 1),
        skill_ids=(10010, 10033, 10040),
        sort=3,
        desc="偏生存、治疗、净化与独立护盾，稳定团队血线",
    ),
    RankedPresetSpec(
        preset_id=201,
        role_kind=ROLE_KIND_PET,
        code="pojia",
        name="破甲",
        role_tag="破防追击",
        pet_table_id=3,
        weights=(1, 0, 3, 1),
        skill_ids=(10002, 10018, 13001),
        pet_entry_ids=(501,),
        pet_entry_points=(20,),
        sort=1,
        desc="金猊破防物理宝宝，碎甲击降防并触发乘胜追击",
    ),
    RankedPresetSpec(
        preset_id=202,
        role_kind=ROLE_KIND_PET,
        code="lingzhou",
        name="灵咒",
        role_tag="群法施毒",
        pet_table_id=1,
        weights=(1, 2, 0, 2),
        skill_ids=(10006, 10019, 10034),
        pet_entry_ids=(502, 531),
        pet_entry_points=(10, 10),
        sort=2,
        desc="灵狐群法施毒宝宝，持续压血并驱散敌方增益",
    ),
    RankedPresetSpec(
        preset_id=203,
        role_kind=ROLE_KIND_PET,
        code="huzhu",
        name="护主",
        role_tag="减伤护主",
        pet_table_id=2,
        weights=(2, 2, 0, 1),
        skill_ids=(10014, 10010, 11006, 12004),
        pet_entry_ids=(503, 504),
        pet_entry_points=(10, 10),
        sort=3,
        desc="石灵护主治疗宝宝，强化双防、治疗与物理护盾",
    ),
)


@dataclass(frozen=True, slots=True)
class RankedSeasonRow:
    """``match_ranked_season`` 表行模型。"""

    season_id: int
    season_key: str
    season_name: str
    rank_generation: int
    source_epoch: int
    rule_version: int
    state: int
    rollover_stage: int
    start_ms: int
    end_ms: int
    rank_board_id: int = 0
    expected_rank_cursor: int = 0
    champion_player_id: int = 0
    champion_name: str = ""
    champion_rating_centi: int = 0
    updated_ms: int = 0


@dataclass(frozen=True, slots=True)
class RankedPlayerPrefsRow:
    """``match_ranked_player_prefs`` 表行模型。"""

    player_id: int
    character_preset_id: int
    pet_preset_id: int
    rule_version: int
    cosmetic_loadout: bytes
    updated_ms: int


@dataclass(frozen=True, slots=True)
class RankedPlayerSeasonRow:
    """``match_ranked_player_season`` 表行模型。"""

    season_id: int
    player_id: int
    qualified: bool
    qualified_at_ms: int
    rating_centi: int
    peak_rating_centi: int
    peak_tier: int
    valid_matches: int
    wins: int
    losses: int
    draws: int
    milestone_bits: int
    honor_bits: int
    final_rank: int
    updated_ms: int


@dataclass(frozen=True, slots=True)
class RankedRewardRow:
    """``match_ranked_reward`` 表行模型。"""

    player_id: int
    season_id: int
    reward_kind: int
    tier_value: int
    item_code: str
    granted_at_ms: int


# -- 纯规则函数 ---------------------------------------------------------------------------------------


def tier_for_rating(rating_centi: int, *, qualified: bool = True, rule: RankedRuleConfig | None = None) -> int:
    """按服务端原精度计算论剑五段位；未亮段返回 ``RANKED_TIER_UNSPECIFIED``。"""
    if not qualified:
        return RANKED_TIER_UNSPECIFIED
    cfg = rule or RankedRuleConfig()
    if rating_centi >= cfg.tier_wushuang_centi:
        return RANKED_TIER_WUSHUANG
    if rating_centi >= cfg.tier_tianshu_centi:
        return RANKED_TIER_TIANSHU
    if rating_centi >= cfg.tier_yunzhang_centi:
        return RANKED_TIER_YUNZHANG
    if rating_centi >= cfg.tier_qingfeng_centi:
        return RANKED_TIER_QINGFENG
    return RANKED_TIER_CHUSHI


def ranked_spread_points(wait_seconds: int, rule: RankedRuleConfig | None = None) -> int:
    """排位硬上限分差窗口（<30s: 100 / 30..<90s: 200 / >=90s: 300，永不退化为 +Inf）。"""
    cfg = rule or RankedRuleConfig()
    if wait_seconds < cfg.spread_tier1_seconds:
        return cfg.spread_tier1_points
    if wait_seconds < cfg.spread_tier2_seconds:
        return cfg.spread_tier2_points
    return cfg.spread_tier3_points


def milestone_specs(season_key: str, rule: RankedRuleConfig | None = None) -> tuple[tuple[int, int, int, str], ...]:
    """返回本季里程碑奖励定义：``(threshold, bit_mask, reward_kind, item_code)``。"""
    cfg = rule or RankedRuleConfig()
    return (
        (cfg.milestone_avatar_matches, MILESTONE_BIT_AVATAR, RANKED_REWARD_AVATAR_PATTERN, f"{season_key}_avatar_pattern"),
        (cfg.milestone_card_matches, MILESTONE_BIT_CARD, RANKED_REWARD_CARD_BACKGROUND, f"{season_key}_card_bg"),
        (cfg.milestone_talisman_matches, MILESTONE_BIT_TALISMAN, RANKED_REWARD_TALISMAN, f"{season_key}_qingyun_talisman"),
    )


# -- 论剑排位业务服务 ---------------------------------------------------------------------------------


class RankedManager:
    """承载论剑排位 V1 客户端 RPC 与组队预设确认校验。"""

    def __init__(
        self,
        protocol: Protocol,
        store,
        *,
        rule: RankedRuleConfig | None = None,
        presets: tuple[RankedPresetSpec, ...] | None = None,
        tables: ConfigTables | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.rule = rule if rule is not None else RankedRuleConfig.from_config_tables(tables)
        loaded_presets = (
            tuple(presets) if presets is not None else RankedPresetSpec.load_from_config_tables(tables)
        )
        self.presets = loaded_presets
        self._char_preset_ids = frozenset(p.preset_id for p in self.presets if p.role_kind == ROLE_KIND_CHARACTER)
        self._pet_preset_ids = frozenset(p.preset_id for p in self.presets if p.role_kind == ROLE_KIND_PET)
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    def is_valid_preset_pair(self, char_preset_id: int, pet_preset_id: int) -> bool:
        return int(char_preset_id) in self._char_preset_ids and int(pet_preset_id) in self._pet_preset_ids

    async def ensure_current_season(self) -> RankedSeasonRow:
        """获取当前赛季；若库内尚未初始化则幂等创建默认 S1 活动赛季。"""
        season = await self.store.get_active_ranked_season()
        if season is not None:
            return season
        seasons = await self.store.list_ranked_seasons()
        if seasons:
            return seasons[0]
        now_ms = await self.store.now_ms()
        default_season = RankedSeasonRow(
            season_id=1,
            season_key="s1",
            season_name="论剑第一赛季",
            rank_generation=1,
            source_epoch=1,
            rule_version=self.rule.rule_version,
            state=RANKED_SEASON_ACTIVE,
            rollover_stage=0,
            start_ms=now_ms,
            end_ms=now_ms + self.rule.season_duration_days * DAY_MS,
            updated_ms=now_ms,
        )
        return await self.store.upsert_ranked_season(default_season)

    def _season_info_proto(self, season: RankedSeasonRow) -> Message:
        return self.protocol.message(
            "match.RankedSeasonInfo",
            season_id=season.season_id,
            season_key=season.season_key,
            season_name=season.season_name,
            rank_generation=season.rank_generation,
            rule_version=season.rule_version,
            state=season.state,
            start_ms=season.start_ms,
            end_ms=season.end_ms,
            battle_config_id=self.rule.battle_config_id,
            wait_prompt_seconds=self.rule.wait_prompt_seconds,
            spread_tier1_seconds=self.rule.spread_tier1_seconds,
            spread_tier1_points=self.rule.spread_tier1_points,
            spread_tier2_seconds=self.rule.spread_tier2_seconds,
            spread_tier2_points=self.rule.spread_tier2_points,
            spread_tier3_points=self.rule.spread_tier3_points,
            champion_player_id=season.champion_player_id,
            champion_name=season.champion_name,
            champion_rating_centi=season.champion_rating_centi,
            rank_board_sealed=(season.state == RANKED_SEASON_CLOSED or season.rollover_stage >= 6),
        )

    def _player_season_proto(
        self,
        season: RankedSeasonRow,
        row: RankedPlayerSeasonRow | None,
        fallback_rating_centi: int,
    ) -> Message:
        if row is None:
            return self.protocol.message(
                "match.RankedPlayerSeasonSummary",
                season_id=season.season_id,
                season_key=season.season_key,
                qualified=False,
                rating_centi=fallback_rating_centi,
                current_tier=RANKED_TIER_UNSPECIFIED,
                peak_rating_centi=0,
                peak_tier=RANKED_TIER_UNSPECIFIED,
            )
        return self.protocol.message(
            "match.RankedPlayerSeasonSummary",
            season_id=row.season_id,
            season_key=season.season_key,
            qualified=row.qualified,
            rating_centi=row.rating_centi,
            current_tier=tier_for_rating(row.rating_centi, qualified=row.qualified, rule=self.rule),
            peak_rating_centi=row.peak_rating_centi,
            peak_tier=row.peak_tier,
            valid_matches=row.valid_matches,
            wins=row.wins,
            losses=row.losses,
            draws=row.draws,
            qualified_at_ms=row.qualified_at_ms,
            milestone_bits=row.milestone_bits,
            final_rank=row.final_rank,
            champion_awarded=bool(row.honor_bits & HONOR_BIT_CHAMPION),
        )

    def _fill_presets(self, repeated_field) -> None:
        for spec in self.presets:
            repeated_field.add(
                preset_id=spec.preset_id,
                role_kind=spec.role_kind,
                code=spec.code,
                name=spec.name,
                role_tag=spec.role_tag,
                pet_table_id=spec.pet_table_id,
                weights=list(spec.weights),
                skill_ids=list(spec.skill_ids),
                pet_entry_ids=list(spec.pet_entry_ids),
                pet_entry_points=list(spec.pet_entry_points),
                sort=spec.sort,
                desc=spec.desc,
            )

    def _decode_cosmetic(self, raw: bytes) -> Message:
        if not raw:
            return self.protocol.message("match.RankedCosmeticLoadout")
        try:
            return self.protocol.parse("match.RankedCosmeticLoadout", raw)
        except DecodeError:
            return self.protocol.message("match.RankedCosmeticLoadout")

    # -- 组队预设确认校验（供 TeamGrpcService.confirm_ranked_preset 调用）-------------------------------

    async def validate_team_preset_confirm(
        self,
        char_preset_id: int,
        pet_preset_id: int,
        rule_version: int,
    ) -> RankedSeasonRow:
        """核验排位赛季状态、预设组合与规则版本；不合法抛 ``TeamBusinessError``。"""
        season = await self.ensure_current_season()
        if season.state != RANKED_SEASON_ACTIVE:
            raise TeamBusinessError(ERR_MATCH_RANKED_SEASON_SETTLING)
        if not self.is_valid_preset_pair(char_preset_id, pet_preset_id):
            raise TeamBusinessError(ERR_TEAM_RANKED_PRESET_INVALID)
        if rule_version != season.rule_version:
            raise TeamBusinessError(ERR_TEAM_RANKED_PRESET_INVALID)
        return season

    # -- MatchService 客户端 RPC 实现 -----------------------------------------------------------------

    async def get_ranked_profile(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.GetRankedProfileResponse")
        try:
            now_ms = await self.store.now_ms()
            season = await self.ensure_current_season()
            rating_centi, _, _ = await self.store.get_rating(caller.player_id, MODE_PVP_TEAM)
            cur_prog = await self.store.get_ranked_player_season(season.season_id, caller.player_id)
            prefs = await self.store.get_ranked_player_prefs(caller.player_id)
            entitlements = await self.store.list_ranked_rewards(caller.player_id)

            resp.current_season.CopyFrom(self._season_info_proto(season))
            resp.current_progress.CopyFrom(self._player_season_proto(season, cur_prog, rating_centi))
            if prefs is not None and self.is_valid_preset_pair(prefs.character_preset_id, prefs.pet_preset_id):
                resp.selected_presets.character_preset_id = prefs.character_preset_id
                resp.selected_presets.pet_preset_id = prefs.pet_preset_id
                resp.cosmetic_loadout.CopyFrom(self._decode_cosmetic(prefs.cosmetic_loadout))
            else:
                resp.selected_presets.character_preset_id = self.rule.default_character_preset_id
                resp.selected_presets.pet_preset_id = self.rule.default_pet_preset_id

            self._fill_presets(resp.presets)
            for ent in entitlements:
                resp.entitlements.add(
                    season_id=ent.season_id,
                    kind=ent.reward_kind,
                    tier_value=ent.tier_value,
                    item_code=ent.item_code,
                    granted_at_ms=ent.granted_at_ms,
                )

            if request.include_history:
                all_seasons = {s.season_id: s for s in await self.store.list_ranked_seasons()}
                for s in sorted(all_seasons.values(), key=lambda x: x.season_id, reverse=True):
                    if s.season_id != season.season_id:
                        resp.past_seasons.add().CopyFrom(self._season_info_proto(s))
                for hist in await self.store.list_ranked_player_seasons(caller.player_id):
                    if hist.season_id == season.season_id:
                        continue
                    s_meta = all_seasons.get(hist.season_id)
                    if s_meta is None:
                        s_meta = RankedSeasonRow(
                            season_id=hist.season_id,
                            season_key=f"s{hist.season_id}",
                            season_name=f"S{hist.season_id}",
                            rank_generation=hist.season_id,
                            source_epoch=hist.season_id,
                            rule_version=self.rule.rule_version,
                            state=RANKED_SEASON_CLOSED,
                            rollover_stage=8,
                            start_ms=0,
                            end_ms=0,
                        )
                    resp.history.add().CopyFrom(self._player_season_proto(s_meta, hist, hist.rating_centi))

            resp.server_time_ms = now_ms
            return resp
        except Exception:
            logger.error("ranked GetRankedProfile failed player=%d", caller.player_id, exc_info=True)
            return set_tip(self.protocol.message("match.GetRankedProfileResponse"), ERR_MATCH_INTERNAL)

    async def save_ranked_preset(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.SaveRankedPresetResponse")
        try:
            season = await self.ensure_current_season()
            if season.state != RANKED_SEASON_ACTIVE:
                return set_tip(resp, ERR_MATCH_RANKED_SEASON_SETTLING)
            req_rule_ver = int(request.rule_version)
            if req_rule_ver != 0 and req_rule_ver != season.rule_version:
                return set_tip(resp, ERR_MATCH_RANKED_RULE_VERSION_MISMATCH)
            char_id = int(request.character_preset_id)
            pet_id = int(request.pet_preset_id)
            if not self.is_valid_preset_pair(char_id, pet_id):
                return set_tip(resp, ERR_MATCH_RANKED_PRESET_INVALID)

            existing = await self.store.get_ranked_player_prefs(caller.player_id)
            cosmetic_bytes = existing.cosmetic_loadout if existing is not None else b""
            saved = await self.store.save_ranked_player_prefs(
                caller,
                char_preset_id=char_id,
                pet_preset_id=pet_id,
                rule_version=season.rule_version,
                cosmetic_loadout=cosmetic_bytes,
            )
            resp.selected_presets.character_preset_id = saved.character_preset_id
            resp.selected_presets.pet_preset_id = saved.pet_preset_id
            resp.rule_version = saved.rule_version
            return resp
        except Exception:
            logger.error("ranked SaveRankedPreset failed player=%d", caller.player_id, exc_info=True)
            return set_tip(resp, ERR_MATCH_INTERNAL)

    async def update_ranked_cosmetic(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.UpdateRankedCosmeticResponse")
        if not request.HasField("loadout"):
            return set_tip(resp, ERR_INVALID_PARAMETER)
        loadout = request.loadout
        try:
            entitlements = await self.store.list_ranked_rewards(caller.player_id)
            owned_by_kind: dict[int, set[str]] = {}
            for ent in entitlements:
                owned_by_kind.setdefault(ent.reward_kind, set()).add(ent.item_code)

            # 校验每项佩戴代号确实归属本人且类型匹配
            if loadout.equipped_talisman_code and loadout.equipped_talisman_code not in owned_by_kind.get(
                RANKED_REWARD_TALISMAN, set()
            ):
                return set_tip(resp, ERR_INVALID_PARAMETER)
            if loadout.equipped_avatar_pattern_code and loadout.equipped_avatar_pattern_code not in owned_by_kind.get(
                RANKED_REWARD_AVATAR_PATTERN, set()
            ):
                return set_tip(resp, ERR_INVALID_PARAMETER)
            if loadout.equipped_card_bg_code and loadout.equipped_card_bg_code not in owned_by_kind.get(
                RANKED_REWARD_CARD_BACKGROUND, set()
            ):
                return set_tip(resp, ERR_INVALID_PARAMETER)
            title_owned = owned_by_kind.get(RANKED_REWARD_TIER_TITLE, set()) | owned_by_kind.get(
                RANKED_REWARD_CHAMPION_INSCRIPTION, set()
            )
            if loadout.equipped_title_code and loadout.equipped_title_code not in title_owned:
                return set_tip(resp, ERR_INVALID_PARAMETER)

            normalized = self.protocol.message(
                "match.RankedCosmeticLoadout",
                equipped_talisman_code="" if loadout.hide_talisman else loadout.equipped_talisman_code,
                equipped_avatar_pattern_code=loadout.equipped_avatar_pattern_code,
                equipped_card_bg_code=loadout.equipped_card_bg_code,
                equipped_title_code=loadout.equipped_title_code,
                hide_talisman=bool(loadout.hide_talisman),
            )
            existing = await self.store.get_ranked_player_prefs(caller.player_id)
            season = await self.ensure_current_season()
            char_id = existing.character_preset_id if existing else self.rule.default_character_preset_id
            pet_id = existing.pet_preset_id if existing else self.rule.default_pet_preset_id
            rule_ver = existing.rule_version if existing else season.rule_version
            await self.store.save_ranked_player_prefs(
                caller,
                char_preset_id=char_id,
                pet_preset_id=pet_id,
                rule_version=rule_ver,
                cosmetic_loadout=normalized.SerializeToString(deterministic=True),
            )
            resp.loadout.CopyFrom(normalized)
            return resp
        except Exception:
            logger.error("ranked UpdateRankedCosmetic failed player=%d", caller.player_id, exc_info=True)
            return set_tip(resp, ERR_MATCH_INTERNAL)

    async def get_ranked_battle_result(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.GetRankedBattleResultResponse")
        battle_id = int(request.battle_id)
        try:
            raw = await self.store.get_ranked_battle_result(caller.player_id, battle_id)
            if raw is not None:
                resp.result.CopyFrom(self.protocol.parse("match.RankedBattleSettlementView", raw))
                return resp
            # 若指定了 battle_id 且该玩家仍在对应排位成局中尚未落账，返回 PENDING 态
            if battle_id != 0:
                formation = await self.store.get_formation(battle_id)
                if (
                    formation is not None
                    and int(formation.ranked_season_id) > 0
                    and any(int(m.player_id) == caller.player_id for m in formation.members)
                ):
                    resp.result.battle_id = battle_id
                    resp.result.season_id = int(formation.ranked_season_id)
                    resp.result.status = RANKED_MATCH_RATING_PENDING
                    return resp
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND)
        except Exception:
            logger.error(
                "ranked GetRankedBattleResult failed player=%d battle_id=%d",
                caller.player_id,
                battle_id,
                exc_info=True,
            )
            return set_tip(resp, ERR_MATCH_INTERNAL)

    async def record_battle_settlement(
        self,
        *,
        battle_id: int,
        player_id: int,
        season_id: int,
        team_size: int,
        status: int,
        outcome: int,
        player_team_index: int,
        total_rounds: int,
        participated: bool,
        fled: bool,
        rating_before_centi: int,
        rating_after_centi: int,
        character_preset_id: int = DEFAULT_CHARACTER_PRESET_ID,
        pet_preset_id: int = DEFAULT_PET_PRESET_ID,
        settled_at_ms: int | None = None,
    ) -> Message:
        """幂等落账单名玩家单局排位进度、里程碑展示权益与结算摘要视图。"""
        now_ms = int(settled_at_ms if settled_at_ms is not None else await self.store.now_ms())
        season = await self.ensure_current_season()
        season_key = season.season_key if season.season_id == season_id else f"s{season_id}"
        milestones = milestone_specs(season_key, self.rule)

        # outcome_kind: 1 win / 2 loss / 3 draw / 0 none
        outcome_kind = 0
        if status == RANKED_MATCH_RATING_CONFIRMED and participated and total_rounds >= 1:
            if outcome == 3:
                outcome_kind = 3
            elif (outcome == 1 and player_team_index == 0) or (outcome == 2 and player_team_index == 1):
                outcome_kind = 1
            elif outcome in (1, 2):
                outcome_kind = 2

        prev = await self.store.get_ranked_player_season(season_id, player_id)
        was_qualified = bool(prev and prev.qualified)
        tier_before = tier_for_rating(rating_before_centi, qualified=was_qualified, rule=self.rule)
        will_qualify = was_qualified or (status == RANKED_MATCH_RATING_CONFIRMED and participated and total_rounds >= 1)
        tier_after = tier_for_rating(rating_after_centi, qualified=will_qualify, rule=self.rule)
        newly_qualified = (not was_qualified) and will_qualify

        summary = self.protocol.message(
            "match.RankedBattleSettlementView",
            battle_id=battle_id,
            season_id=season_id,
            team_size=team_size,
            status=status,
            outcome=outcome,
            player_team_index=player_team_index,
            total_rounds=total_rounds,
            participated=participated,
            fled=fled,
            rating_before_centi=rating_before_centi,
            rating_after_centi=rating_after_centi,
            rating_delta_centi=rating_after_centi - rating_before_centi,
            tier_before=tier_before,
            tier_after=tier_after,
            newly_qualified=newly_qualified,
            settled_at_ms=now_ms,
            character_preset_id=character_preset_id,
            pet_preset_id=pet_preset_id,
        )

        updated_row, unlocked_rewards, final_bytes = await self.store.save_ranked_battle_settlement(
            season_id=season_id,
            battle_id=battle_id,
            player_id=player_id,
            summary_template=summary,
            rating_centi=rating_after_centi,
            tier_after=tier_after,
            outcome_kind=outcome_kind,
            participated=(status == RANKED_MATCH_RATING_CONFIRMED and participated and total_rounds >= 1),
            settled_at_ms=now_ms,
            milestones=milestones,
        )
        return self.protocol.parse("match.RankedBattleSettlementView", final_bytes)
