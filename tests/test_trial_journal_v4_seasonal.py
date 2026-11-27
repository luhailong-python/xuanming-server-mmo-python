"""五行试炼志 V4：卷三季游 S2–S10 全十季自动翻篇、技印共鸣条件 15/16 与 Match 多季索引单测。"""

from __future__ import annotations

from types import SimpleNamespace

from xuanming.data import ConfigTables
from xuanming.match.model import MODE_POLARITY_5V5, MODE_PVE_SOLO
from xuanming.match.trial_cards import (
    build_trial_battle_context,
    lookup_ranked_trial_catalog_entry,
    lookup_trial_catalog_entry,
)
from xuanming.nodes.battle import TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    SealConditionType,
    TrialContext,
    evaluate_seal_condition as evaluate_battle_seal_condition,
)
from xuanming.nodes.battle_tables import BattleTables
from xuanming.nodes.scene_trial_journal import (
    CHAPTER_SEASONAL,
    SceneTrialJournal,
)
from xuanming.protocol import Protocol


def test_trial_journal_v4_tables_json_and_pb_load_s2_to_s10():
    """校验 JSON 与 PB 双格式均完整载入 S2..S10 共 27 格 / 54 印 / 18 里程碑且共鸣掩码对齐 SeasonSeal。"""
    tables_json = ConfigTables(fmt="json")
    tables_pb = ConfigTables(fmt="pb")
    assert len(tables_json["TrialJournalEntry"].rows()) == len(tables_pb["TrialJournalEntry"].rows())
    assert len(tables_json["TrialJournalSeal"].rows()) == len(tables_pb["TrialJournalSeal"].rows())
    assert len(tables_json["TrialJournalMilestone"].rows()) == len(tables_pb["TrialJournalMilestone"].rows())

    expected_masks: dict[int, int] = {}
    for seal_row in tables_json["SeasonSeal"].rows():
        rk = int(getattr(seal_row, "resonance_kind", 0))
        sid = int(getattr(seal_row, "season_id", 0))
        if sid >= 2 and 1 <= rk <= 8:
            expected_masks[sid] = expected_masks.get(sid, 0) | (1 << (rk - 1))

    for sid in range(2, 11):
        base = 3000 + sid * 10
        for offset in (1, 2, 3):
            entry = tables_json["TrialJournalEntry"].get(base + offset)
            assert entry is not None
            assert entry.season_id == sid
            assert entry.chapter_id == CHAPTER_SEASONAL
        s_cast = tables_json["TrialJournalSeal"].get((base + 3) * 10 + 1)
        s_res = tables_json["TrialJournalSeal"].get((base + 3) * 10 + 2)
        assert s_cast is not None and s_cast.condition_type == SealConditionType.SEASON_SKILL_CAST
        assert s_res is not None and s_res.condition_type == SealConditionType.SEASON_RESONANCE
        assert s_res.condition_param == expected_masks[sid]
        m2 = tables_json["TrialJournalMilestone"].get(base + 2)
        assert m2 is not None
        assert m2.badge_key == f"badge_season_s{sid}_trial"
        assert m2.commentary_key == f"commentary_season_s{sid}_trial"


def test_trial_journal_v4_battle_engine_tracks_season_skill_and_resonance_mask():
    """S2 玄脉之季：施放免费赛季技 15103 触发 R6 首盾回蓝共鸣，权威写入 fields 27..29 并按掩码判定。"""
    protocol = Protocol()
    tables = ConfigTables(fmt="json")
    battle_tables = BattleTables(protocol, tables)

    p_a = protocol.message(
        "BattlePlayerSnapshot",
        player_id=4001,
        player_name="玄脉试剑",
        team_index=0,
        active_season_id=2,
        level=30,
        class_id=1,
        max_health=10000,
        max_mana=2000,
        physical_attack=800,
        magic_attack=800,
        defense=200,
        base_attributes=protocol.message(
            "BaseAttributesComp", health=10000, mana=1000, speed=400, strength=10, armor=50
        ),
        skill_table_ids=[15103],
        skill_ranks={15103: 1},
    )
    p_a.equipment_effects.season_seal.CopyFrom(
        protocol.message(
            "SeasonSealEffect",
            season_id=2,
            seal_id=203,
            effect_id=5,
            bonus_bps=1200,
            per_round_limit=1,
            battle_limit=3,
            resonance_skill_id=15103,
            resonance_kind=6,
            resonance_param=250,
        )
    )
    p_b = protocol.message(
        "BattlePlayerSnapshot",
        player_id=4002,
        player_name="对手",
        team_index=1,
        level=30,
        class_id=1,
        max_health=10000,
        max_mana=2000,
        physical_attack=100,
        magic_attack=100,
        defense=200,
        base_attributes=protocol.message(
            "BaseAttributesComp", health=10000, mana=2000, speed=100, strength=10, armor=50
        ),
    )
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=95001,
        match_mode=3,
        seed=29,
        players=[p_a, p_b],
    )
    engine = TurnEngine(
        protocol,
        req,
        skills=battle_tables.skills,
        items=battle_tables.items,
        buffs=battle_tables.buffs,
        silence_permissions=battle_tables.silence_permissions,
        max_rounds=20,
        trial_context=TrialContext(journal_entry_id=3023, trial_rule_version=1, season_id=2),
    )
    cast_action = protocol.message("BattleAction", action_type=2, target_id=4001, skill_table_id=15103)
    defend_action = protocol.message("BattleAction", action_type=3)
    assert engine.validate(engine.actors[4001], cast_action) == 0
    assert engine.validate(engine.actors[4002], defend_action) == 0
    engine.pending[4001] = cast_action
    engine.pending[4002] = defend_action
    engine.resolve()

    settlement = protocol.message(
        "BattleSettlementData",
        battle_id=95001,
        player_id=4001,
        player_team_index=0,
        outcome=1,
        total_rounds=1,
        participated=True,
    )
    engine.trial_tracker.populate_settlement(settlement, 4001)
    facts = settlement.trial_facts
    assert facts.self_cast_season_skill is True
    assert facts.self_triggered_season_resonance is True
    assert facts.self_season_resonance_mask == (1 << 5)
    assert evaluate_battle_seal_condition(SealConditionType.SEASON_SKILL_CAST, 0, settlement) is True
    assert evaluate_battle_seal_condition(SealConditionType.SEASON_RESONANCE, 0, settlement) is True
    assert evaluate_battle_seal_condition(SealConditionType.SEASON_RESONANCE, 51, settlement) is True
    assert evaluate_battle_seal_condition(SealConditionType.SEASON_RESONANCE, 21, settlement) is False


def test_trial_journal_v4_match_multi_season_and_scene_rotation():
    """验证默认常开 S1 与开启 28 天十季无限循环后的 Match 多季解析和 Scene 卷三翻篇隔离。"""
    protocol = Protocol()
    base_tables = ConfigTables(fmt="json")

    # 0. 默认表（cycle_duration_days=0，首测常开 S1）命中 S1 301
    assert lookup_trial_catalog_entry(109, tables=base_tables, now_ms=1_760_000_005_000).journal_entry_id == 301

    # 构造开启 28 天 10 季无限循环的 SeasonRule 视图
    anchor_ms = 1_760_000_000_000
    period_ms = 28 * 24 * 3600 * 1000
    cycle_rows = []
    for row in base_tables["SeasonRule"].rows():
        cloned = type(row).FromString(row.SerializeToString())
        cloned.enabled = True
        cloned.starts_at_ms = anchor_ms
        cloned.ends_at_ms = 0
        cloned.cycle_duration_days = 28
        cycle_rows.append(cloned)

    class _MockRuleTable:
        def __init__(self, rows):
            self._rows = tuple(rows)
            self._by_id = {int(r.id): r for r in rows}

        def rows(self):
            return self._rows

        def get(self, key):
            return self._by_id.get(int(key))

    class _MockTables:
        def __init__(self, base, rules_table):
            self._base = base
            self._rules = rules_table
            self.tables = dict(base.tables)
            self.tables["SeasonRule"] = rules_table

        def __getitem__(self, key):
            return self._rules if key == "SeasonRule" else self._base[key]

    tables = _MockTables(base_tables, _MockRuleTable(cycle_rows))

    # 1. Match 多季上下文解析（S1 / S2 / S10 / 第 2 轮 S1）
    t_s1 = anchor_ms + 5_000
    t_s2 = anchor_ms + period_ms + 5_000
    t_s10 = anchor_ms + period_ms * 9 + 5_000
    t_cycle2_s1 = anchor_ms + period_ms * 10 + 5_000

    assert lookup_trial_catalog_entry(109, tables=tables, now_ms=t_s1).journal_entry_id == 301
    assert lookup_trial_catalog_entry(109, tables=tables, now_ms=t_s2).journal_entry_id == 3021
    assert lookup_trial_catalog_entry(110, tables=tables, now_ms=t_s10).journal_entry_id == 3102
    assert lookup_trial_catalog_entry(109, tables=tables, now_ms=t_cycle2_s1).journal_entry_id == 301
    assert lookup_ranked_trial_catalog_entry(2, tables=tables).journal_entry_id == 3023
    assert lookup_ranked_trial_catalog_entry(10, tables=tables).journal_entry_id == 3103

    ctx_pol_s2 = build_trial_battle_context(protocol, MODE_POLARITY_5V5, 110, tables=tables, now_ms=t_s2)
    assert ctx_pol_s2 is not None and ctx_pol_s2.journal_entry_id == 3022 and ctx_pol_s2.season_id == 2
    ctx_ranked_s10 = build_trial_battle_context(protocol, 3, 5, ranked_season_id=10, tables=tables, now_ms=t_s10)
    assert ctx_ranked_s10 is not None and ctx_ranked_s10.journal_entry_id == 3103 and ctx_ranked_s10.season_id == 10
    ctx_boss = build_trial_battle_context(protocol, MODE_PVE_SOLO, 101, vow_mask=1, tables=tables, now_ms=t_s2)
    assert ctx_boss is not None and ctx_boss.journal_entry_id == 101 and ctx_boss.challenge_vow_mask == 1

    # 2. Scene 卷三按钟拨入 S2 -> S3 自动翻篇且永久分母恒为 36、S2 与 S3 存档完全隔离
    now_box = [t_s2]
    fake_scene = SimpleNamespace(protocol=protocol, mutable=lambda _p: True, battle=None, assets=None)
    svc = SceneTrialJournal(fake_scene, tables, clock=lambda: now_box[0])
    data = protocol.message("player_database")
    svc.initialize_on_load(data)
    fake_player = SimpleNamespace(player_id=4001, in_battle=False, frozen=False)

    snap_s2 = protocol.message("TrialJournalSnapshot")
    svc._fill_snapshot(fake_player, data, t_s2, snap_s2)
    assert snap_s2.total_permanent_seals == 36
    ch3_s2 = next(c for c in snap_s2.chapters if c.chapter_id == CHAPTER_SEASONAL)
    assert ch3_s2.season_id == 2
    assert [e.entry_id for e in ch3_s2.entries] == [3021, 3022, 3023]

    # 错配共鸣（R4=bit3，不在 S2 并集掩码 51 内）只亮 30231
    d_wrong = svc.record_trial_progress(
        data,
        3023,
        won=False,
        participated=True,
        total_rounds=5,
        self_cast_season_skill=True,
        self_triggered_season_resonance=True,
        self_season_resonance_mask=1 << 3,
        achieved_at_ms=t_s2,
    )
    assert d_wrong["newly_lit_seal_ids"] == [30231]

    # 命中 S2 共鸣（R6=bit5）点亮 30232，且永久位图仍为空
    d_hit = svc.record_trial_progress(
        data,
        3023,
        won=False,
        participated=True,
        total_rounds=5,
        self_triggered_season_resonance=True,
        self_season_resonance_mask=1 << 5,
        achieved_at_ms=t_s2,
    )
    assert d_hit["newly_lit_seal_ids"] == [30232]
    assert data.trial_journal_component.lit_seal_bits == b""

    # 拨入 S3，卷三自动切到 S3（3031..3033），进度 0/6
    t_s3 = anchor_ms + period_ms * 2 + 5_000
    now_box[0] = t_s3
    snap_s3 = protocol.message("TrialJournalSnapshot")
    svc._fill_snapshot(fake_player, data, t_s3, snap_s3)
    ch3_s3 = next(c for c in snap_s3.chapters if c.chapter_id == CHAPTER_SEASONAL)
    assert ch3_s3.season_id == 3
    assert ch3_s3.lit_seal_count == 0
    assert [e.entry_id for e in ch3_s3.entries] == [3031, 3032, 3033]
