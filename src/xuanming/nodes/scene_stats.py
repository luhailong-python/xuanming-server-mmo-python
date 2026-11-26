"""配表驱动的角色、宝宝二级属性和技能快照。"""

import math
from collections import Counter

from .scene_mission import effective_level
from .scene_player import _fill_item

STATS = ("max_health", "max_mana", "physical_attack", "magic_attack", "speed", "defense")
PASSIVE_NAMES = {
    "maxHp": "max_health", "maxMp": "max_mana", "physicalAttack": "physical_attack",
    "magicAttack": "magic_attack", "speed": "speed", "defense": "defense",
}
UINT64_MAX = (1 << 64) - 1
UINT32_MAX = (1 << 32) - 1

# 人物战法构筑装配常量（与 C++ skill_loadout_rules.h 对齐：免费 3 套，仙途轻享卡生效扩展至 6 套）。
LOADOUT_BASE_MAX_SCHEMES = 3
LOADOUT_MAX_SCHEMES = 6
LOADOUT_MAX_ROLE_TAG = 3
LOADOUT_MAX_PASSIVE_SLOTS = 3
LOADOUT_MAX_PURSUIT_SLOTS = 1
LOADOUT_MAX_OPENING_SLOTS = 1


def unlocked_skill_schemes(light_card_active: bool) -> int:
    """返回当前可用的战法构筑方案槽位数（免费 3 套；仙途轻享卡生效 6 套）。"""
    return LOADOUT_MAX_SCHEMES if light_card_active else LOADOUT_BASE_MAX_SCHEMES


def rank_limit(level, row) -> int:
    return min(row.max_rank, 1 + (level - row.unlock_level) // 2) if level >= row.unlock_level else 0


def class_id(data) -> int:
    return getattr(data.uint32_pb_component, "class")


def is_class_skill(row) -> bool:
    """需要等级与现代规则判定的技能（含 legacy 职业技与 battle_model=1 自由战法）。"""
    return bool(row.class_id or getattr(row, "battle_model", 0) == 1)


def is_free_skill_for(row, caster_mask: int) -> bool:
    """判断是否为指定施法者掩码（1=人物, 2=宝宝）可用的自由战法。"""
    return getattr(row, "battle_model", 0) == 1 and bool(getattr(row, "caster_mask", 0) & caster_mask)


def is_passive_skill(row) -> bool:
    """判断技能是否为非主动/被动技能。"""
    if getattr(row, "battle_model", 0) == 1:
        return getattr(row, "ability_kind", 0) != 1
    return bool(row.passive_stat)


def is_turn_battle_castable(row) -> bool:
    """判断技能是否可作为回合制战斗中的主动施放动作。"""
    battle_model = getattr(row, "battle_model", 0)
    if battle_model != 0:
        return battle_model == 1 and getattr(row, "ability_kind", 0) == 1
    return not is_passive_skill(row) and not {0, 2, 3}.intersection(row.skill_type)


def is_skill_active_for(row, cls: int, class_has_class_skills: bool) -> bool:
    """判断技能行对当前角色是否生效（与 C++ IsActiveFor 一致）。"""
    if getattr(row, "battle_model", 0) != 0:
        return is_free_skill_for(row, 1)
    if is_class_skill(row):
        return row.class_id == cls
    return not class_has_class_skills


def is_skill_usable(row, entry, cls: int, class_has_class_skills: bool, level: int) -> bool:
    """判断角色已持有的技能条目当前是否可用（与 C++ IsUsable 一致）。"""
    if not is_skill_active_for(row, cls, class_has_class_skills):
        return False
    return not is_class_skill(row) or min(entry.rank, rank_limit(level, row)) > 0


def collect_equipped_non_active_skills(
    tables,
    skill_list_comp,
    cls: int,
    class_has_class_skills: bool,
    level: int,
) -> set[int]:
    """计算当前生效的非主动战法集合（被动/追击/开场），对齐 C++ CollectEquippedNonActiveSkills。"""
    equipped: set[int] = set()
    occupied_groups: set[int] = set()
    by_id = {}
    for entry in skill_list_comp.skill_list:
        by_id.setdefault(entry.skill_table_id, entry)

    def can_use(skill_id: int):
        row = tables["Skill"].get(skill_id)
        if (
            row is None
            or getattr(row, "battle_model", 0) != 1
            or getattr(row, "season_id", 0) != 0
            or getattr(row, "ability_kind", 0) <= 1
        ):
            return None
        entry = by_id.get(skill_id)
        if entry is None or not is_skill_usable(row, entry, cls, class_has_class_skills, level):
            return None
        return row

    active_scheme_id = int(getattr(skill_list_comp, "active_loadout_scheme_id", 0))
    if 1 <= active_scheme_id <= LOADOUT_MAX_SCHEMES:
        scheme = next(
            (item for item in skill_list_comp.loadout_schemes if item.scheme_id == active_scheme_id),
            None,
        )
        if scheme is None:
            return equipped

        def add_from(ids, expected_kind: int, limit: int) -> None:
            count = 0
            for skill_id in ids:
                if count >= limit:
                    break
                row = can_use(skill_id)
                if row is None or getattr(row, "ability_kind", 0) != expected_kind:
                    continue
                group = int(getattr(row, "exclusive_group", 0))
                if group and group in occupied_groups:
                    continue
                if skill_id not in equipped:
                    if group:
                        occupied_groups.add(group)
                    equipped.add(skill_id)
                    count += 1

        add_from(scheme.passive_skill_ids, 2, LOADOUT_MAX_PASSIVE_SLOTS)
        add_from(scheme.pursuit_skill_ids, 3, LOADOUT_MAX_PURSUIT_SLOTS)
        add_from(scheme.opening_skill_ids, 4, LOADOUT_MAX_OPENING_SLOTS)
        return equipped

    # 未启用自定义方案时的兼容态：按 ID 升序全部生效，仅拦截同 exclusive_group 互斥冲突。
    learned_ids = sorted(
        entry.skill_table_id
        for entry in by_id.values()
        if can_use(entry.skill_table_id) is not None
    )
    for skill_id in learned_ids:
        row = tables["Skill"].get(skill_id)
        if row is None:
            continue
        group = int(getattr(row, "exclusive_group", 0))
        if group and group in occupied_groups:
            continue
        if group:
            occupied_groups.add(group)
        equipped.add(skill_id)
    return equipped


def try_auto_slot_newly_learned(tables, skill_list_comp, row) -> None:
    """新学非主动战法时，若当前启用了自定义方案且对应分类尚有空位且无互斥冲突，则自动装配。"""
    active_scheme_id = int(getattr(skill_list_comp, "active_loadout_scheme_id", 0))
    if (
        getattr(row, "battle_model", 0) != 1
        or getattr(row, "season_id", 0) != 0
        or getattr(row, "ability_kind", 0) <= 1
        or not 1 <= active_scheme_id <= LOADOUT_MAX_SCHEMES
    ):
        return
    scheme = next(
        (item for item in skill_list_comp.loadout_schemes if item.scheme_id == active_scheme_id),
        None,
    )
    if scheme is None:
        scheme = skill_list_comp.loadout_schemes.add(scheme_id=active_scheme_id, role_tag=0)
    kind = int(getattr(row, "ability_kind", 0))
    if kind == 2:
        slot_list, limit = scheme.passive_skill_ids, LOADOUT_MAX_PASSIVE_SLOTS
    elif kind == 3:
        slot_list, limit = scheme.pursuit_skill_ids, LOADOUT_MAX_PURSUIT_SLOTS
    else:
        slot_list, limit = scheme.opening_skill_ids, LOADOUT_MAX_OPENING_SLOTS
    if len(slot_list) >= limit or row.id in slot_list:
        return
    group = int(getattr(row, "exclusive_group", 0))
    if group:
        all_existing = (*scheme.passive_skill_ids, *scheme.pursuit_skill_ids, *scheme.opening_skill_ids)
        for existing_id in all_existing:
            existing = tables["Skill"].get(existing_id)
            if existing is not None and int(getattr(existing, "exclusive_group", 0)) == group:
                return
    updated = sorted((*slot_list, row.id))
    del slot_list[:]
    slot_list.extend(updated)


def battle_skills(tables, data, snapshot, *, active_season_id: int = 0) -> None:
    """向 BattlePlayerSnapshot 填充生效技能与等级（过滤未装配的非主动战法）。"""
    level, cls = effective_level(data), class_id(data)
    has_class = any(row.class_id == cls for row in tables["Skill"].rows()) if cls else False
    equipped_non_active = collect_equipped_non_active_skills(tables, data.skill_list, cls, has_class, level)
    if hasattr(snapshot, "active_season_id"):
        snapshot.active_season_id = active_season_id
    for item in data.skill_list.skill_list:
        row = tables["Skill"].get(item.skill_table_id)
        if row is None or getattr(row, "season_id", 0) != 0:
            continue
        if not is_skill_usable(row, item, cls, has_class, level):
            continue
        if (
            getattr(row, "battle_model", 0) == 1
            and getattr(row, "ability_kind", 0) > 1
            and row.id not in equipped_non_active
        ):
            continue
        if is_class_skill(row):
            snapshot.skill_ranks[row.id] = min(item.rank, rank_limit(level, row))
        if is_turn_battle_castable(row):
            snapshot.skill_table_ids.append(row.id)
    if active_season_id:
        for row in sorted(tables["Skill"].rows(), key=lambda r: r.id):
            if getattr(row, "season_id", 0) != active_season_id or not is_free_skill_for(row, 1):
                continue
            eff_rank = rank_limit(level, row)
            if not eff_rank:
                continue
            snapshot.skill_ranks[row.id] = eff_rank
            if is_turn_battle_castable(row):
                snapshot.skill_table_ids.append(row.id)


def _class(tables, data):
    return tables["Class"].get(class_id(data)) or next(iter(tables["Class"].rows()), None)


def _baseline(row):
    return dict(zip(STATS, (row.init_health, row.init_mana, 0., 0., row.init_speed, 0.)))


def _total(pool, level, extra=0) -> int:
    return min(UINT32_MAX, pool.base_points + pool.points_per_level * (level - pool.unlock_level + 1) + extra
               ) if 0 < pool.unlock_level <= level else 0


def _effective(pool, dims, allocated, level, extra=0) -> bool:
    used = [allocated.get(dim.id, 0) for dim in dims]
    return sum(used) <= _total(pool, level, extra) and (not pool.dimension_cap or max(used, default=0) <= pool.dimension_cap)


def _linear(values, row, points) -> None:
    for key in STATS:
        values[key] += getattr(row, key) * points


def _equipment(protocol, tables, data):
    primary, flat, combat = Counter(), Counter(), Counter()
    all_points = 0
    for entry in data.bag_component.items:
        if entry.bag_type != 2:
            continue
        instance = entry.instance if entry.HasField("instance") else None
        config = instance.config_id if instance is not None else entry.config_id
        equip = (instance.equip if instance.HasField("equip") else None) if instance is not None else (
            entry.equip if entry.HasField("equip") else None
        )
        info = protocol.message("BagItemInfo", config_id=config)
        _fill_item(tables, info, equip)
        lines = [*info.base_attrs, *info.affixes, *(socket.attr for socket in info.gem_sockets if socket.HasField("attr"))]
        for line in lines:
            row = tables["EquipAttribute"].get(line.attr_id)
            if row.effect == 1:
                primary[row.effect_param] += line.value
            elif row.effect == 2:
                all_points += line.value
            elif row.effect == 3:
                flat[STATS[row.effect_param - 1]] += line.value
            elif row.effect == 4:
                flat["physical_attack"] += line.value
                flat["magic_attack"] += line.value
            elif row.effect == 5:
                combat[row.effect_param] += line.value
    return primary, all_points, flat, combat


def player_stats(protocol, tables, data):
    row = _class(tables, data)
    if row is None:
        raise ValueError("缺少职业初始属性")
    values, standard = _baseline(row), _baseline(row)
    level = effective_level(data)
    comp = data.attribute_component
    scheme = next((item for item in comp.schemes if item.scheme_id == comp.active_scheme_id), None)
    allocated = scheme.allocated if scheme is not None else {}
    dimensions = tuple(tables["AttributeDimension"].rows())
    pools = [pool for pool in tables["AttributePool"].rows() if pool.owner_type == 0]
    primary, all_points, flat, combat = _equipment(protocol, tables, data)
    by_pool = {pool.id: [dim for dim in dimensions if dim.pool_id == pool.id] for pool in pools}
    for dims in by_pool.values():
        for dim in dims:
            _linear(standard, dim, dim.base_per_level * 85)
    rule = tables["AttributeRule"].get(1)
    bonus = rule.alloc_efficiency_bonus if rule is not None else 0.20
    if not math.isfinite(bonus) or bonus < 0:
        bonus = 0.20
    ratios = {(item.dimension_id, item.class_id): item for item in tables["AttributeAllocRatio"].rows()}
    for pool in pools:
        dims = by_pool[pool.id]
        active = _effective(pool, dims, allocated, level, comp.bonus_points.get(pool.id, 0))
        scale = _total(pool, 85)
        if pool.dimension_cap:
            scale = min(scale, pool.dimension_cap)
        for dim in dims:
            extra = comp.bonus_values.get(dim.id, 0) + primary[dim.id] + all_points
            _linear(values, dim, dim.base_per_level * level + min(UINT64_MAX, extra))
            points = allocated.get(dim.id, 0) if active else 0
            if not points:
                continue
            ratio = ratios.get((dim.id, class_id(data))) or ratios.get((dim.id, 0))
            if ratio is None:
                _linear(values, dim, points)
            elif scale:
                factor = points * (1 + bonus * points / scale) / (scale * (1 + bonus))
                for key in STATS:
                    values[key] += standard[key] * max(0, getattr(ratio, key)) * factor
    passives = Counter()
    cls = class_id(data)
    has_class = any(r.class_id == cls for r in tables["Skill"].rows()) if cls else False
    equipped_non_active = collect_equipped_non_active_skills(tables, data.skill_list, cls, has_class, level)
    for item in data.skill_list.skill_list:
        skill = tables["Skill"].get(item.skill_table_id)
        if skill is None or getattr(skill, "season_id", 0) != 0 or not skill.passive_stat:
            continue
        if not is_skill_usable(skill, item, cls, has_class, level):
            continue
        if (
            getattr(skill, "battle_model", 0) == 1
            and getattr(skill, "ability_kind", 0) > 1
            and skill.id not in equipped_non_active
        ):
            continue
        rank = min(item.rank, rank_limit(level, skill))
        if rank:
            passives[skill.passive_stat] += skill.passive_percent * (1 + .05 * (rank - 1))
    for passive, key in PASSIVE_NAMES.items():
        values[key] = (values[key] + min(UINT64_MAX, flat[key])) * (1 + passives[passive])
    crit = max(0, math.floor(passives["crit"] * 100 + 1e-9))
    combat[1] += crit
    combat[2] += crit
    result = protocol.message("DerivedAttributesComp", **_floor(values))
    result.max_health = max(1, result.max_health)
    for number, value in combat.items():
        setattr(result.combat, result.combat.DESCRIPTOR.fields_by_number[number].name, min(UINT64_MAX, value))
    return result


def _floor(values):
    return {key: min(UINT64_MAX, max(0, math.floor(value))) for key, value in values.items()}


def initialize_base(protocol, tables, data) -> None:
    row = _class(tables, data)
    if row is None:
        raise ValueError("缺少职业初始属性")
    base = data.derived_attributes_component
    if not any((base.health, base.strength, base.speed)):
        base.strength, base.critchance, base.resistance = row.init_strength, row.init_critchance, row.init_resistance
    stats = player_stats(protocol, tables, data)
    base.speed, base.armor = stats.speed, row.init_armor
    if not base.health:
        base.health, base.mana = stats.max_health, stats.max_mana or row.init_mana
    else:
        base.health, base.mana = min(base.health, stats.max_health), min(base.mana, stats.max_mana)
    data.level_component.SetInParent()


def pet_pool(tables):
    return next((item for item in tables["AttributePool"].rows() if item.owner_type == 1), None)


def pet_dimensions(tables, pool):
    return sorted((item for item in tables["AttributeDimension"].rows() if item.pool_id == pool.id),
                  key=lambda item: item.id)


def pet_level(level, row):
    level = max(1, level)
    return min(level, row.level_cap) if row is not None and row.level_cap else level


def pet_skills(pet, row):
    skills = [skill for skill in row.skill if skill]
    for skill in pet.skill_table_ids:
        if skill and skill not in skills:
            skills.append(skill)
    return skills


def pet_stats(tables, pet):
    row = tables["Pet"].get(pet.pet_table_id)
    pool = pet_pool(tables)
    if row is None or pool is None:
        return None
    level = pet_level(pet.level, row)
    values = _baseline(row)
    dims = pet_dimensions(tables, pool)
    active = _effective(pool, dims, pet.allocated, level)
    for dim in dims:
        points = dim.base_per_level * level + (pet.allocated.get(dim.id, 0) if active else 0)
        _linear(values, dim, points * ((pet.aptitude.get(dim.id, 0) or 10_000) / 10_000.0))
    result = _floor(values)
    result["max_health"] = max(1, result["max_health"])
    return result
