"""从当前玩家的持久组件构造 Scene 只读响应。"""

from ..data import ConfigTables, PlayerRowKind

_INVALID_TABLE_DATA = 1002
_INVALID_PARAMETER = 1005
_BAG_CAPACITY = (100, 200, 10, 200)
_EQUIPMENT_BAG = 2
_MAX_BATTLE_RESULTS = 10
_MAX_GEM_SOCKETS = 8
_MAX_UINT64 = (1 << 64) - 1


def register_player_handlers(scene) -> None:
    tables = ConfigTables()
    scene.register(
        "SceneCurrencyClientPlayer", "GetCurrencyList",
        lambda world, player, route, request: _currency(scene, player),
    )
    scene.register(
        "SceneBagClientPlayer", "GetBag",
        lambda world, player, route, request: _bag(scene, tables, player, request.bag_type),
    )
    scene.register(
        "SceneBattleResultClientPlayer", "GetRecentBattleResults",
        lambda world, player, route, request: _recent_battles(scene, player),
    )


def actor_profile_fields(scene, player) -> dict:
    """同 C++ AOI 出生通知，从角色存档读外观、性别和职业。"""
    data = _read_row(scene, player, PlayerRowKind.PROFILE)
    return {
        "appearance_id": data.profile_component.appearance_id,
        "gender": data.profile_component.gender,
        "class_id": getattr(data.uint32_pb_component, "class"),
    }


def _read_row(scene, player, kind: PlayerRowKind):
    return scene.protocol.parse("player_database", player.snapshot.rows.get(kind, b""))


def _currency(scene, player):
    wallet = _read_row(scene, player, PlayerRowKind.WALLET)
    return scene.protocol.message("GetCurrencyListResponse", currency=wallet.currency)


def _bag(scene, tables: ConfigTables, player, bag_type: int):
    response = scene.protocol.message("GetBagResponse")
    if bag_type >= len(_BAG_CAPACITY):
        response.error_message.id = _INVALID_PARAMETER
        return response

    saved = _read_row(scene, player, PlayerRowKind.BAG).bag_component
    capacity = saved.capacities[bag_type] if bag_type < len(saved.capacities) else _BAG_CAPACITY[bag_type]
    slots = {}
    items = {}
    for entry in saved.items:
        if entry.bag_type != bag_type:
            continue
        if entry.HasField("instance"):
            item = entry.instance
            item_id, config_id, count = item.item_id, item.config_id, item.size
        else:
            item = entry
            item_id, config_id, count = entry.item_uuid, entry.config_id, entry.stack_size
        if (
            not all((item_id, config_id, count)) or item_id in items
            or entry.pos in slots or entry.pos >= capacity
        ):
            response.error_message.id = _INVALID_TABLE_DATA
            return response
        slots[entry.pos] = item_id
        items[item_id] = (config_id, count, item.equip if item.HasField("equip") else None)

    for item_id, (config_id, count, equip) in sorted(items.items()):
        info = response.bag.items.add(item_id=item_id, config_id=config_id, count=count)
        _fill_item(tables, info, equip)
    layout = response.bag.layout
    layout.bag_type = bag_type
    layout.capacity = capacity
    layout.can_sort = bag_type in (0, 1)
    for slot, item_id in sorted(slots.items()):
        layout.slots.add(slot=slot, item_id=item_id, width=1, height=1)
    if bag_type == _EQUIPMENT_BAG:
        equips = tables["EquipSlot"]
        keys = sorted({row.id for row in equips.rows() if row.id < capacity})
        for slot in keys:
            row = equips.get(slot)
            if row.name or slot in slots:
                layout.equip_slots.add(slot=slot, equip_kind=row.equip_kind, name=row.name)
    wallet = _read_row(scene, player, PlayerRowKind.WALLET)
    response.bag.currency.CopyFrom(wallet.currency)
    return response


def _fill_item(tables: ConfigTables, info, equip) -> None:
    row = tables["Item"].get(info.config_id)
    if row is None:
        return
    info.max_stack, info.equip_kind = row.max_stack_size, row.equip_kind
    info.name, info.description, info.icon_key = row.name, row.description, row.icon_key
    if not row.equip_kind:
        gem = tables["EquipGem"].get(info.config_id)
        if gem is not None:
            info.gem_type = gem.gem_type
            _gem_attribute(tables, gem, info.gem_attr)
        return

    info.equip_level, info.equip_class = row.equip_level, row.equip_class
    enhancement = tables["EquipEnhance"]
    levels = 0
    while levels < len(enhancement) and enhancement.get(levels + 1) is not None:
        levels += 1
    level = min(equip.enhance_level, levels) if equip is not None else 0
    current = enhancement.get(level) if level else None
    bonus = current.bonus_permyriad if current is not None else 0
    info.enhance_level = level
    info.enhance_max_level = levels
    info.enhance_bonus_permyriad = bonus
    preview = enhancement.get(level + 1) if equip is not None and level < levels else None
    if preview is not None and not preview.success_rate:
        preview = None
    if preview is not None:
        _fill_enhance_preview(tables, info.enhance_next, preview)

    for base in row.base_attr:
        attr = _accepted_attribute(tables, base.base_attr_id)
        if attr is None:
            continue
        value = _scaled(base.base_attr_value, bonus)
        line = info.base_attrs.add(
            seq=len(info.base_attrs), enhance_bonus=value - base.base_attr_value,
        )
        _fill_line(line, attr, value)
        if preview is not None:
            line.enhance_next_value = _scaled(base.base_attr_value, preview.bonus_permyriad)
    if equip is None:
        return
    for affix in sorted(equip.affixes, key=lambda line: (line.tier, line.seq)):
        attr = _accepted_attribute(tables, affix.attr_id)
        if attr is None or not 1 <= affix.tier <= 4:
            continue
        levels = tables["EquipAttributeCap"].find_all(affix.attr_id, field="attr_id")
        applicable = max(
            (cap for cap in levels if cap.level <= row.equip_level),
            key=lambda cap: cap.level, default=None,
        )
        cap = applicable.cap if applicable is not None else 0
        line = info.affixes.add(cap=cap, tier=affix.tier, seq=affix.seq)
        _fill_line(line, attr, min(affix.value, cap))
    _fill_sockets(tables, info, equip, row.equip_kind)


def _fill_enhance_preview(tables: ConfigTables, target, row) -> None:
    target.next_level = row.id
    target.success_rate = row.success_rate
    target.cost_gold = row.cost_gold
    target.bonus_permyriad = row.bonus_permyriad
    target.fail_level_loss = row.fail_level_loss
    if row.material_id and row.material_count:
        target.material_config_id = row.material_id
        target.material_count = row.material_count
        material = tables["Item"].get(row.material_id)
        if material is not None:
            target.material_name = material.name
            target.material_icon_key = material.icon_key


def _fill_sockets(tables: ConfigTables, info, equip, equip_kind: int) -> None:
    socket_definitions = {}
    for row in tables["EquipGemSocket"].find_all(equip_kind, field="equip_kind"):
        socket_definitions.setdefault(row.socket, row)
    occupied = {}
    for index, gem in enumerate(equip.gems):
        if gem.gem_config_id:
            occupied.setdefault(gem.socket, (index, gem.gem_config_id))
    for index, definition in sorted(socket_definitions.items()):
        socket = info.gem_sockets.add(socket=index, gem_type=definition.gem_type, name=definition.name)
        if index not in occupied:
            continue
        record_index, config_id = occupied[index]
        socket.gem_config_id = config_id
        gem_item = tables["Item"].get(config_id)
        if gem_item is not None:
            socket.gem_name, socket.gem_icon_key = gem_item.name, gem_item.icon_key
        gem = tables["EquipGem"].get(config_id)
        if gem is not None and record_index < _MAX_GEM_SOCKETS:
            _gem_attribute(tables, gem, socket.attr)


def _accepted_attribute(tables: ConfigTables, attr_id: int):
    attr = tables["EquipAttribute"].get(attr_id)
    if attr is None:
        return None
    kind, param = attr.effect, attr.effect_param
    if (
        kind in (2, 4) or kind == 1 and param > 0
        or kind == 3 and 1 <= param <= 6 or kind == 5 and 1 <= param <= 15
    ):
        return attr
    return None


def _gem_attribute(tables: ConfigTables, gem, line) -> None:
    attr = _accepted_attribute(tables, gem.attr_id)
    if attr is not None:
        _fill_line(line, attr, gem.value)


def _fill_line(line, attr, value: int) -> None:
    line.attr_id, line.name, line.value = attr.id, attr.name, value
    line.percent = bool(attr.percent)


def _scaled(value: int, permyriad: int) -> int:
    return min(_MAX_UINT64, value + value * permyriad // 10_000)


def _recent_battles(scene, player):
    profile = _read_row(scene, player, PlayerRowKind.PROFILE)
    # 本模块只从确认的存档取结果，等同于 C++ 先排除在途结算再展示。
    entries = sorted(profile.battle_results.entries, key=lambda item: item.result_seq, reverse=True)
    for entry in entries:
        if entry.battle_id in player.seen_battles:
            entry.seen = True
    return scene.protocol.message("GetRecentBattleResultsResponse", results=entries[:_MAX_BATTLE_RESULTS])
