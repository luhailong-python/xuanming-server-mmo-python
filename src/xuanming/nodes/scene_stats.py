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


def rank_limit(level, row) -> int:
    return min(row.max_rank, 1 + (level - row.unlock_level) // 2) if level >= row.unlock_level else 0


def class_id(data) -> int:
    return getattr(data.uint32_pb_component, "class")


def battle_skills(tables, data, snapshot) -> None:
    level, cls = effective_level(data), class_id(data)
    has_class = any(row.class_id == cls for row in tables["Skill"].rows()) if cls else False
    for item in data.skill_list.skill_list:
        row = tables["Skill"].get(item.skill_table_id)
        if row is None:
            continue
        if row.class_id:
            rank = min(item.rank, rank_limit(level, row))
            if cls != row.class_id or not rank:
                continue
            snapshot.skill_ranks[row.id] = rank
        elif has_class:
            continue
        if not row.passive_stat and not {0, 2, 3}.intersection(row.skill_type):
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
    for item in data.skill_list.skill_list:
        skill = tables["Skill"].get(item.skill_table_id)
        if skill is not None and skill.class_id and skill.class_id == class_id(data) and skill.passive_stat:
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
