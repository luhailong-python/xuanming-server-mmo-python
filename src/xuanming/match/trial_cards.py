"""五行试炼志 V3：好友约战帖（570..572）与试炼誓约目录辅助（对齐 Go trial_challenge_card.go / catalog.go）。

职责：
- 根据 Dungeon 与 TrialJournalEntry 配表映射常驻首领试炼上下文（journal_entry_id / trial_rule_version）与誓约支持判定；
- CreateTrialChallengeCard (570)：核验本人有效通关的试炼志 PVE 战报，幂等复用未过期约战帖，每人最多 10 张 FIFO 淘汰，
  14 天有效期，并与录像 ref_count 联动钉住/释放回放正文；
- GetTrialChallengeCard (571)：凭未过期 card_id 只读查询约战帖详情；
- GetSharedTrialReplay (572)：仅凭有效未过期 card_id 受控开放试炼回放播放，不破坏既有 GetBattleReport 隐私边界。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from google.protobuf.message import DecodeError, Message

from ..protocol import Protocol
from .errors import ERR_MATCH_BATTLE_REPORT_NOT_FOUND, MatchBusinessError, set_tip
from .model import (
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    OUTCOME_DECISION_COMPLETE,
    CallerIdentity,
    is_permanent_boss_trial_kind,
)
from .ranked import _default_config_tables
from .store import MatchStore

logger = logging.getLogger(__name__)

MAX_TRIAL_CHALLENGE_CARDS_PER_PLAYER = 10
TRIAL_CHALLENGE_CARD_TTL_MS = 14 * 24 * 60 * 60 * 1000

_OUTCOME_SIDE_A_WIN = 1
_OUTCOME_SIDE_B_WIN = 2

_BOSS_TRIAL_VARIANTS: dict[int, tuple[int, str]] = {
    1: (1, "rejuvenation"),
    2: (1, "bulwark"),
    3: (1, "barrier"),
    4: (1, "venom"),
    5: (2, "rescue"),
    6: (3, "polarity_5v5"),
    8: (1, "dual_guard"),
    12: (1, "wild_wind"),
    13: (1, "wild_bell"),
    14: (1, "wild_tide"),
}


@dataclass(frozen=True, slots=True)
class TrialCatalogEntry:
    """单个 Dungeon 对应的试炼志条目与规则版本。"""

    journal_entry_id: int
    trial_rule_version: int
    season_id: int
    boss_trial_kind: int


def lookup_trial_catalog_entry(config_id: int, tables=None) -> TrialCatalogEntry | None:
    """按 Dungeon.id 查对应的 TrialJournalEntry（对齐 Go catalog.buildTrialContexts）。"""
    source = tables if tables is not None else _default_config_tables()
    if source is None or "Dungeon" not in source.tables or "TrialJournalEntry" not in source.tables:
        return None
    d_row = source["Dungeon"].get(int(config_id))
    if d_row is None:
        return None
    kind = int(getattr(d_row, "boss_trial_kind", 0))
    variant = _BOSS_TRIAL_VARIANTS.get(kind)
    if variant is None:
        return None
    source_kind, variant_key = variant
    difficulty = int(getattr(d_row, "difficulty", 0))
    for entry in source["TrialJournalEntry"].rows():
        if not getattr(entry, "enabled", True):
            continue
        if (
            int(getattr(entry, "source_kind", 0)) == source_kind
            and int(getattr(entry, "difficulty", 0)) == difficulty
            and str(getattr(entry, "variant_key", "")) == variant_key
        ):
            return TrialCatalogEntry(
                journal_entry_id=int(getattr(entry, "id", 0)),
                trial_rule_version=int(getattr(entry, "rule_version", 1)) or 1,
                season_id=int(getattr(entry, "season_id", 0)),
                boss_trial_kind=kind,
            )
    return None


def supports_trial_vows(config_id: int, tables=None) -> bool:
    """报告该副本模板是否支持挂载试炼誓约（仅常驻卷一/卷二/卷四首领试炼，且非赛季限时项）。"""
    entry = lookup_trial_catalog_entry(config_id, tables=tables)
    if entry is None:
        return False
    return (
        entry.journal_entry_id > 0
        and entry.season_id == 0
        and is_permanent_boss_trial_kind(entry.boss_trial_kind)
    )


def build_trial_battle_context(
    protocol: Protocol,
    mode: int,
    config_id: int,
    *,
    vow_mask: int = 0,
    has_activity_context: bool = False,
    tables=None,
) -> Message | None:
    """为试炼志 PVE 副本构造 TrialBattleContext（含 challenge_vow_mask）。"""
    if has_activity_context or mode not in (MODE_PVE_SOLO, MODE_PVE_TEAM):
        return None
    entry = lookup_trial_catalog_entry(config_id, tables=tables)
    if entry is None or entry.journal_entry_id <= 0:
        if vow_mask:
            return protocol.message("TrialBattleContext", challenge_vow_mask=int(vow_mask))
        return None
    return protocol.message(
        "TrialBattleContext",
        journal_entry_id=entry.journal_entry_id,
        trial_rule_version=entry.trial_rule_version,
        season_id=entry.season_id,
        challenge_vow_mask=int(vow_mask),
    )


def _is_trial_victory_for_player(event: Message, settlement: Message | None) -> bool:
    if event is None or settlement is None or bool(settlement.fled) or int(event.total_rounds) == 0:
        return False
    outcome = int(event.outcome)
    team_index = int(settlement.player_team_index)
    return (outcome == _OUTCOME_SIDE_A_WIN and team_index == 0) or (
        outcome == _OUTCOME_SIDE_B_WIN and team_index == 1
    )


@dataclass(slots=True)
class _StoredCard:
    card_id: int
    owner_player_id: int
    battle_id: int
    created_ms: int
    expires_ms: int
    has_replay: bool
    view_bytes: bytes


class TrialChallengeCardManager:
    """五行试炼志 V3 好友约战帖与受控录像分享管理器（570..572）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._clock_ms = clock_ms
        self._lock = asyncio.Lock()
        self._next_card_id = 770_000
        self._cards: dict[int, _StoredCard] = {}

    def _now(self) -> int:
        return max(1, int(self._clock_ms()))

    def _adjust_report_ref_count(self, battle_id: int, delta: int) -> bool:
        """若底层存储为 MemoryMatchStore，则同步增减战报 ref_count 钉住/释放回放正文。"""
        state = getattr(self.store, "_state", None)
        if state is None:
            return False
        rep = state.reports.get(battle_id)
        if rep is None or not rep.envelope:
            return False
        if delta > 0:
            rep.ref_count += delta
        elif delta < 0:
            rep.ref_count = max(0, rep.ref_count + delta)
            if rep.ref_count == 0:
                state.reports.pop(battle_id, None)
        return True

    async def _has_replay(self, owner_player_id: int, battle_id: int) -> bool:
        try:
            await self.store.get_battle_report(owner_player_id, battle_id, allow_public=True)
            return True
        except MatchBusinessError:
            return False

    async def create_card(self, caller: CallerIdentity, request: Message) -> Message:
        """570 CreateTrialChallengeCard：将本人有效通关的试炼志 PVE 战报签发为可分享约战帖。"""
        resp = self.protocol.message("match.CreateTrialChallengeCardResponse")
        battle_id = int(request.battle_id)
        if battle_id <= 0:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "该试炼战报不可用于签发约战帖")

        now_ms = self._now()

        # 1. 核验本人参战列表包含该局
        summaries = await self.store.list_battle_reports(caller.player_id)
        if not any(int(s.battle_id) == battle_id for s in summaries):
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "仅本人近期有效通关的试炼志战报可签发约战帖")

        # 2. 幂等复用未过期约战帖
        async with self._lock:
            for card in self._cards.values():
                if card.owner_player_id == caller.player_id and card.battle_id == battle_id and card.expires_ms > now_ms:
                    resp.card.CopyFrom(self.protocol.parse("match.TrialChallengeCardView", card.view_bytes))
                    return resp

        # 3. 核验终局原件与建房原件为本人有效通关的试炼志 PVE 战报
        decision, raw_completion = await self.store.get_battle_completion(battle_id)
        if decision != OUTCOME_DECISION_COMPLETE or not raw_completion:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "仅本人近期有效通关的试炼志战报可签发约战帖")
        try:
            event = self.protocol.parse("contracts.kafka.BattleResultEvent", raw_completion)
        except DecodeError:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "试炼战报数据异常")
        if int(event.match_mode) not in (MODE_PVE_SOLO, MODE_PVE_TEAM):
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "仅试炼志 PVE 战报可签发约战帖")
        my_settlement = next((s for s in event.settlements if int(s.player_id) == caller.player_id), None)
        if (
            my_settlement is None
            or not _is_trial_victory_for_player(event, my_settlement)
            or not my_settlement.HasField("trial_facts")
            or int(my_settlement.trial_facts.journal_entry_id) == 0
        ):
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "仅本人近期有效通关的试炼志战报可签发约战帖")

        formation = await self.store.get_formation(battle_id)
        if formation is None or not formation.create_request:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "建房原件缺失")
        try:
            create_req = self.protocol.parse("CreateBattleRequest", bytes(formation.create_request))
        except DecodeError:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "建房原件损坏")
        owner_name = ""
        for p in create_req.players:
            if int(p.player_id) == caller.player_id:
                owner_name = str(p.player_name)
                break

        has_replay = await self._has_replay(caller.player_id, battle_id)
        facts = my_settlement.trial_facts
        artifacts = sorted(int(aid) for aid in facts.used_artifact_effect_ids)

        async with self._lock:
            # 锁内二次检查幂等复用
            for card in self._cards.values():
                if card.owner_player_id == caller.player_id and card.battle_id == battle_id and card.expires_ms > now_ms:
                    resp.card.CopyFrom(self.protocol.parse("match.TrialChallengeCardView", card.view_bytes))
                    return resp

            owner_cards = sorted(
                (c for c in self._cards.values() if c.owner_player_id == caller.player_id),
                key=lambda c: (c.created_ms, c.card_id),
            )
            evict: list[_StoredCard] = []
            live: list[_StoredCard] = []
            for c in owner_cards:
                if c.expires_ms <= now_ms:
                    evict.append(c)
                else:
                    live.append(c)
            while len(live) >= MAX_TRIAL_CHALLENGE_CARDS_PER_PLAYER:
                evict.append(live.pop(0))

            for victim in evict:
                self._cards.pop(victim.card_id, None)
                if victim.has_replay:
                    self._adjust_report_ref_count(victim.battle_id, -1)

            self._next_card_id += 1
            card_id = self._next_card_id
            view = self.protocol.message(
                "match.TrialChallengeCardView",
                card_id=card_id,
                owner_player_id=caller.player_id,
                owner_name=owner_name,
                battle_id=battle_id,
                journal_entry_id=int(facts.journal_entry_id),
                trial_rule_version=int(facts.trial_rule_version),
                vow_mask=int(facts.challenge_vow_mask),
                total_rounds=int(event.total_rounds),
                party_size=int(facts.initial_human_count),
                pet_table_id=int(facts.used_pet_table_id),
                artifact_effect_ids=artifacts,
                season_seal_id=int(facts.used_season_seal_id),
                has_replay=has_replay,
                created_ms=now_ms,
                expires_ms=now_ms + TRIAL_CHALLENGE_CARD_TTL_MS,
            )
            view_bytes = view.SerializeToString(deterministic=True)
            self._cards[card_id] = _StoredCard(
                card_id=card_id,
                owner_player_id=caller.player_id,
                battle_id=battle_id,
                created_ms=now_ms,
                expires_ms=now_ms + TRIAL_CHALLENGE_CARD_TTL_MS,
                has_replay=has_replay,
                view_bytes=view_bytes,
            )
            if has_replay:
                self._adjust_report_ref_count(battle_id, +1)

        resp.card.CopyFrom(view)
        return resp

    async def get_card(self, request: Message) -> Message:
        """571 GetTrialChallengeCard：按约战帖 ID 只读查询未过期约战帖详情。"""
        resp = self.protocol.message("match.GetTrialChallengeCardResponse")
        card_id = int(request.card_id)
        if card_id <= 0:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "约战帖不存在或已过期")
        now_ms = self._now()
        async with self._lock:
            card = self._cards.get(card_id)
            if card is None or card.expires_ms <= now_ms:
                return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "约战帖不存在或已过期")
            view_bytes = card.view_bytes
        resp.card.CopyFrom(self.protocol.parse("match.TrialChallengeCardView", view_bytes))
        return resp

    async def get_shared_replay(self, caller: CallerIdentity, request: Message) -> Message:
        """572 GetSharedTrialReplay：仅凭有效未过期约战帖 card_id 读取对应试炼回放正文。"""
        resp = self.protocol.message("match.GetSharedTrialReplayResponse")
        card_id = int(request.card_id)
        if card_id <= 0:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "约战帖回放不存在或已过期")
        now_ms = self._now()
        async with self._lock:
            card = self._cards.get(card_id)
            if card is None or card.expires_ms <= now_ms:
                return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "约战帖回放不存在或已过期")
            battle_id = card.battle_id
        try:
            env = await self.store.get_battle_report(caller.player_id, battle_id, allow_public=True)
        except MatchBusinessError:
            return set_tip(resp, ERR_MATCH_BATTLE_REPORT_NOT_FOUND, "约战帖回放不存在或已过期")
        resp.replay.CopyFrom(env)
        return resp
