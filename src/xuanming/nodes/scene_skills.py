"""Scene 技能书、修习升级、7 套战法·加点一体化流派方案（含按月租用）及实时施放入口。"""

import logging
import time
from inspect import isawaitable

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import balance, clone
from .battle_tables import rounds_from_seconds
from .scene_skill_casts import SceneSkillCasts
from .scene_stats import (
    LOADOUT_BASE_FREE_SCHEMES,
    LOADOUT_BASE_MAX_SCHEMES,
    LOADOUT_LIGHT_CARD_BONUS_SCHEMES,
    LOADOUT_MAX_OPENING_SLOTS,
    LOADOUT_MAX_PASSIVE_SLOTS,
    LOADOUT_MAX_PURSUIT_SLOTS,
    LOADOUT_MAX_ROLE_TAG,
    LOADOUT_MAX_SCHEMES,
    LOADOUT_RENT_MAX_REMAINING_DAYS,
    LOADOUT_RENT_PERIOD_DAYS,
    LOADOUT_RENT_UNIT_PRICE_DIAMOND,
    UINT64_MAX,
    LoadoutRentError,
    active_rented_loadout_slots,
    class_id,
    collect_equipped_non_active_skills,
    default_loadout_scheme_name,
    effective_level,
    free_loadout_schemes_for_level,
    is_class_skill,
    is_free_skill_for,
    is_passive_skill,
    is_skill_active_for,
    is_skill_usable,
    plan_skill_loadout_rent,
    player_stats,
    rank_limit,
    try_auto_slot_newly_learned,
    unlocked_skill_schemes,
    validate_loadout_scheme_name,
)

logger = logging.getLogger(__name__)

_SERVICE = "SceneSkillClientPlayer"
_GOLD = 0
_DIAMOND = 1
_MAX_INT64 = (1 << 63) - 1
_CLIENT_REQUEST_KIND_RENT_SKILL_LOADOUT_SLOTS = 30
_INVALID_TABLE, _UNAVAILABLE, _INVALID_PARAM = 1001, 1003, 1005
_NOT_IN_CLASS, _RANK_CHANGED, _COUNT_INVALID = 7007, 7008, 7009
_LEVEL_NOT_ENOUGH, _MAX_RANK, _GOLD_NOT_ENOUGH, _BUSY = 7010, 7011, 7012, 7013
_CRAFT_BOOK_REQUIRED = 7019
_LOADOUT_SLOT_LIMIT = 7023
_LOADOUT_CONFLICT = 7024
_LOADOUT_NOT_LEARNED = 7025
_LOADOUT_SCHEME_INVALID = 7026
_LOADOUT_VERSION_CHANGED = 7027
_LOADOUT_NAME_INVALID = 7029
_LOADOUT_RENT_PRICE_CHANGED = 7030
_LOADOUT_RENT_DIAMOND_NOT_ENOUGH = 7031
_LOADOUT_RENT_STACK_LIMIT = 7032
_LOADOUT_RENT_SLOTS_INVALID = 7033
_LOADOUT_ATTRIBUTE_INVALID = 7034
_LOADOUT_ATTRIBUTE_RESET_REQUIRED = 7035
_SEASON_NO_UPGRADE = 34010
_REQUEST_ID_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")


def _map_rent_error(err: LoadoutRentError) -> int:
    if err == LoadoutRentError.NONE:
        return 0
    if err == LoadoutRentError.SLOTS_INVALID:
        return _LOADOUT_RENT_SLOTS_INVALID
    if err == LoadoutRentError.STACK_LIMIT:
        return _LOADOUT_RENT_STACK_LIMIT
    return _INVALID_PARAM


def _valid_loadout_scheme_id(scheme_id: int, max_allowed: int = LOADOUT_MAX_SCHEMES) -> bool:
    return 1 <= scheme_id <= max_allowed


def _valid_loadout_role_tag(role_tag: int) -> bool:
    return 0 <= role_tag <= LOADOUT_MAX_ROLE_TAG


def _valid_loadout_request_id(request_id: str) -> bool:
    return 16 <= len(request_id) <= 64 and all(ch in _REQUEST_ID_CHARS for ch in request_id)


def _loadout_version(stored: int) -> int:
    return stored or 1


def _find_loadout_scheme(skill_list_comp, scheme_id: int):
    return next((scheme for scheme in skill_list_comp.loadout_schemes if scheme.scheme_id == scheme_id), None)


def _find_or_create_loadout_scheme(skill_list_comp, scheme_id: int):
    existing = _find_loadout_scheme(skill_list_comp, scheme_id)
    if existing is not None:
        return existing
    return skill_list_comp.loadout_schemes.add(scheme_id=scheme_id, role_tag=0)


def initialize_skill_list(tables, data) -> None:
    """C++ InitializeOnLoad: keep unknown entries and seed only the current class."""
    data.skill_list.SetInParent()
    skills = data.skill_list.skill_list
    seen = {}
    for index in range(len(skills)):
        entry = skills[index]
        if entry.skill_table_id:
            seen.setdefault(entry.skill_table_id, index)
    for index in range(len(skills) - 1, -1, -1):
        if seen.get(skills[index].skill_table_id) != index:
            del skills[index]
    if not data.HasField("uint32_pb_component") or not data.HasField("level_component"):
        return
    cls, level = class_id(data), effective_level(data)
    class_row = tables["Class"].get(cls)
    if class_row is None:
        return
    by_id = {entry.skill_table_id: entry for entry in skills}
    for skill_id in class_row.skill:
        row = tables["Skill"].get(skill_id) if skill_id else None
        if row is None or row.class_id and (row.class_id != cls or not rank_limit(level, row)):
            continue
        entry = by_id.get(skill_id)
        if entry is None:
            entry = skills.add(skill_table_id=skill_id)
            by_id[skill_id] = entry
        if row.class_id and not entry.rank:
            entry.rank = 1


def _upgrade_cost(next_rank: int, unlock_level: int) -> int:
    return 100 * next_rank * next_rank + 10 * unlock_level


def _blocked(row, level: int, rank: int, gold: int) -> int:
    if rank >= row.max_rank:
        return _MAX_RANK
    if rank >= rank_limit(level, row):
        return _LEVEL_NOT_ENOUGH
    if _upgrade_cost(rank + 1, row.unlock_level) > gold:
        return _GOLD_NOT_ENOUGH
    return 0


def _rescale(current: int, old_max: int, new_max: int) -> int:
    if not current or not new_max:
        return 0
    if not old_max or old_max == new_max:
        return min(current, new_max)
    return min(new_max, max(1, int(float(current) * float(new_max) / float(old_max))))


class SceneSkills:
    def __init__(self, scene, tables, *, now_s=None):
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self._now_s = now_s
        transaction_types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self.debit_tx = transaction_types.values_by_name["TX_CURRENCY_DEDUCT"].number
        rent_enum = transaction_types.values_by_name.get("TX_SKILL_LOADOUT_RENT")
        self.rent_tx = rent_enum.number if rent_enum is not None else 48
        self.skills = {}
        self.catalogs = {}
        for row in tables["Skill"].rows():
            self.skills.setdefault(row.id, row)
            if row.class_id:
                self.catalogs.setdefault(row.class_id, []).append(row)
        for rows in self.catalogs.values():
            rows.sort(key=lambda row: row.id)
        self.buff_duration = {
            row.id: rounds_from_seconds(row.duration)
            for row in tables["Buff"].rows() if row.duration > 0 and not row.infinite_duration
        }
        self.casts = SceneSkillCasts(self)

    def register(self) -> None:
        self.scene.skills = self
        self.scene.register(_SERVICE, "ListSkills", self.list_skills)
        self.scene.register(_SERVICE, "UpgradeSkill", self.upgrade, mutating=True)
        self.scene.register(_SERVICE, "ReleaseSkill", self.casts.release, mutating=True)
        self.scene.register(_SERVICE, "SaveSkillLoadout", self.save_loadout, mutating=True)
        self.scene.register(_SERVICE, "SwitchSkillLoadout", self.switch_loadout, mutating=True)
        self.scene.register(_SERVICE, "RentSkillLoadoutSlots", self.rent_loadout_slots, mutating=True)

    async def close(self) -> None:
        await self.casts.close()

    def _now_sec(self) -> int:
        if self._now_s is not None:
            return int(self._now_s())
        afk_pass = getattr(self.scene, "afk_pass", None)
        if afk_pass is not None and hasattr(afk_pass, "now_s"):
            return int(afk_pass.now_s())
        return int(time.time())

    def _busy(self, player) -> bool:
        return not self.scene.mutable(player) or bool(self.scene.battle and self.scene.battle.frozen(player.player_id))

    @staticmethod
    def _present(data) -> bool:
        return all(data.HasField(field) for field in ("skill_list", "level_component", "uint32_pb_component"))

    def _active_season_id(self) -> int:
        season = getattr(self.scene, "season", None)
        return int(season.active_season_id()) if season is not None else 0

    def _craft_gate_enabled(self) -> bool:
        skill_craft = getattr(self.scene, "skill_craft", None)
        return bool(getattr(skill_craft, "_craft_unlock_gate", False)) if skill_craft is not None else False

    def _is_light_card_active(self, data, now_unix_sec: int) -> bool:
        afk_pass = getattr(self.scene, "afk_pass", None)
        if afk_pass is not None and hasattr(afk_pass, "is_active_data"):
            return bool(afk_pass.is_active_data(data, now_sec=now_unix_sec))
        expires = int(getattr(data.uint64_pb_component, "afk_pass_expires_at", 0)) if data.HasField("uint64_pb_component") else 0
        return expires > now_unix_sec

    def unlocked_loadout_scheme_count(self, data, now_unix_sec: int | None = None) -> int:
        """返回玩家当前生效的战法·加点一体化方案槽位数（2 免费 + 85 级+1 + 轻享卡+2 + 租用 0..5，封顶 7 套）。"""
        now_sec = self._now_sec() if now_unix_sec is None else int(now_unix_sec)
        free_schemes = free_loadout_schemes_for_level(effective_level(data))
        light_active = self._is_light_card_active(data, now_sec)
        rented = int(data.skill_list.rented_extra_slots) if data.HasField("skill_list") else 0
        expires = int(data.skill_list.rent_expires_at_sec) if data.HasField("skill_list") else 0
        return unlocked_skill_schemes(light_active, rented, expires, now_sec, free_schemes=free_schemes)

    def try_switch_linked_loadout(self, data, scheme_id: int) -> tuple[bool, str]:
        """供装备流派一键联动切换战法方案（原地修改 data，不改写独立 request_id 幂等回执）。"""
        if not self._present(data):
            return False, "技能数据尚未就绪"
        unlocked = self.unlocked_loadout_scheme_count(data)
        if not _valid_loadout_scheme_id(scheme_id, unlocked):
            return False, f"战法方案 {scheme_id} 未解锁或编号无效"
        skill_list = data.skill_list
        current_ver = _loadout_version(int(skill_list.loadout_state_version))
        if current_ver >= UINT64_MAX:
            return False, "战法方案版本号已饱和"
        if int(skill_list.active_loadout_scheme_id) == scheme_id:
            return True, ""
        old_stats = player_stats(self.protocol, self.tables, data)
        _find_or_create_loadout_scheme(skill_list, scheme_id)
        skill_list.active_loadout_scheme_id = scheme_id
        skill_list.loadout_state_version = current_ver + 1
        stats = player_stats(self.protocol, self.tables, data)
        attrs = data.derived_attributes_component
        attrs.health = _rescale(attrs.health, old_stats.max_health, stats.max_health)
        attrs.mana = _rescale(attrs.mana, old_stats.max_mana, stats.max_mana)
        attrs.speed = stats.speed
        return True, ""

    def _usable(self, data, row) -> bool:
        cls = class_id(data)
        entry = next((item for item in data.skill_list.skill_list if item.skill_table_id == row.id), None)
        if entry is None:
            return False
        return is_skill_usable(row, entry, cls, bool(self.catalogs.get(cls)), effective_level(data))

    def is_skill_equipped(self, player, skill_table_id: int) -> bool:
        """判断指定技能在玩家当前战法构筑方案中是否处于已装配生效态。"""
        data = self.scene.rows.assemble(player.snapshot)
        if not self._present(data):
            return False
        row = self.skills.get(skill_table_id)
        if row is None:
            return False
        cls, level = class_id(data), effective_level(data)
        season_id = int(getattr(row, "season_id", 0))
        if season_id:
            active_season = self._active_season_id()
            return (
                bool(active_season and season_id == active_season)
                and is_free_skill_for(row, 1)
                and rank_limit(level, row) > 0
            )
        has_class = bool(self.catalogs.get(cls))
        entry = next((item for item in data.skill_list.skill_list if item.skill_table_id == skill_table_id), None)
        if entry is None or not is_skill_usable(row, entry, cls, has_class, level):
            return False
        if getattr(row, "battle_model", 0) != 1 or getattr(row, "ability_kind", 0) <= 1:
            return True
        equipped = collect_equipped_non_active_skills(self.tables, data.skill_list, cls, has_class, level)
        return skill_table_id in equipped

    def _fill_loadout_snapshot(
        self, data, cls: int, has_class: bool, level: int, now_unix_sec: int, out,
    ) -> None:
        """填充 7 套战法·加点一体化流派方案快照（到期只锁不删）；未自定义前为方案 1 生成建议初始装配。"""
        out.Clear()
        skill_list_comp = data.skill_list
        free_schemes = free_loadout_schemes_for_level(level)
        light_active = self._is_light_card_active(data, now_unix_sec)
        stored_rented = int(skill_list_comp.rented_extra_slots)
        stored_expires = int(skill_list_comp.rent_expires_at_sec)
        active_rented = active_rented_loadout_slots(stored_rented, stored_expires, now_unix_sec)
        unlocked_schemes = unlocked_skill_schemes(
            light_active, stored_rented, stored_expires, now_unix_sec, free_schemes=free_schemes,
        )
        active_id = int(skill_list_comp.active_loadout_scheme_id)
        custom_enabled = _valid_loadout_scheme_id(active_id, LOADOUT_MAX_SCHEMES)
        out.custom_enabled = custom_enabled
        out.active_scheme_id = active_id if custom_enabled else 1
        out.state_version = _loadout_version(int(skill_list_comp.loadout_state_version))
        out.max_passive_slots = LOADOUT_MAX_PASSIVE_SLOTS
        out.max_pursuit_slots = LOADOUT_MAX_PURSUIT_SLOTS
        out.max_opening_slots = LOADOUT_MAX_OPENING_SLOTS
        out.last_request_id = skill_list_comp.loadout_last_request_id
        out.unlocked_schemes = unlocked_schemes
        out.max_schemes = LOADOUT_MAX_SCHEMES
        out.free_schemes = free_schemes
        out.light_card_bonus_schemes = LOADOUT_LIGHT_CARD_BONUS_SCHEMES if light_active else 0
        out.rented_extra_slots = active_rented
        out.rent_expires_at_sec = stored_expires if active_rented > 0 else 0
        out.server_now_sec = now_unix_sec
        out.rent_unit_price_diamond = LOADOUT_RENT_UNIT_PRICE_DIAMOND
        out.rent_period_days = LOADOUT_RENT_PERIOD_DAYS
        out.rent_max_remaining_days = LOADOUT_RENT_MAX_REMAINING_DAYS
        out.diamond_balance = balance(data, _DIAMOND)

        attributes = getattr(self.scene, "attributes", None)
        if attributes is not None:
            if hasattr(attributes, "player_pool_reset_cost_gold"):
                out.attribute_reset_cost_gold = attributes.player_pool_reset_cost_gold(data)
            if hasattr(attributes, "switch_cooldown_until"):
                out.attribute_switch_cooldown_until = attributes.switch_cooldown_until(data)
            if hasattr(attributes, "active_scheme_id"):
                out.active_attribute_scheme_id = attributes.active_scheme_id(data)

        for scheme_id in range(1, LOADOUT_MAX_SCHEMES + 1):
            stored = _find_loadout_scheme(skill_list_comp, scheme_id)
            if scheme_id > unlocked_schemes and stored is None:
                continue
            info = out.schemes.add(scheme_id=scheme_id)
            info.name = (
                stored.name if (stored is not None and stored.name) else default_loadout_scheme_name(scheme_id)
            )
            if attributes is not None and hasattr(attributes, "fill_scheme_allocation_summary"):
                summary = attributes.fill_scheme_allocation_summary(data, scheme_id)
                if summary is not None:
                    for dim_id, pts in summary.allocated.items():
                        info.attribute_allocated[dim_id] = pts
                    info.attribute_remaining = summary.remaining
                    info.attribute_total_points = summary.total_points
            if stored is not None:
                info.role_tag = stored.role_tag
                info.passive_skill_ids.extend(stored.passive_skill_ids)
                info.pursuit_skill_ids.extend(stored.pursuit_skill_ids)
                info.opening_skill_ids.extend(stored.opening_skill_ids)
            elif not custom_enabled and scheme_id == 1:
                learned_ids = sorted(
                    {
                        entry.skill_table_id
                        for entry in skill_list_comp.skill_list
                        if (row := self.skills.get(entry.skill_table_id)) is not None
                        and getattr(row, "battle_model", 0) == 1
                        and getattr(row, "season_id", 0) == 0
                        and getattr(row, "ability_kind", 0) > 1
                        and is_skill_usable(row, entry, cls, has_class, level)
                    }
                )
                groups: set[int] = set()
                for skill_id in learned_ids:
                    row = self.skills.get(skill_id)
                    if row is None:
                        continue
                    group = int(getattr(row, "exclusive_group", 0))
                    if group and group in groups:
                        continue
                    kind = int(getattr(row, "ability_kind", 0))
                    added = False
                    if kind == 2 and len(info.passive_skill_ids) < LOADOUT_MAX_PASSIVE_SLOTS:
                        info.passive_skill_ids.append(skill_id)
                        added = True
                    elif kind == 3 and len(info.pursuit_skill_ids) < LOADOUT_MAX_PURSUIT_SLOTS:
                        info.pursuit_skill_ids.append(skill_id)
                        added = True
                    elif kind == 4 and len(info.opening_skill_ids) < LOADOUT_MAX_OPENING_SLOTS:
                        info.opening_skill_ids.append(skill_id)
                        added = True
                    if added and group:
                        groups.add(group)

    def _spendable_gold(self, data) -> int:
        assets = getattr(self.scene, "assets", None)
        if assets is not None and hasattr(assets, "spendable_gold_with_bind"):
            return assets.spendable_gold_with_bind(data)
        return min(UINT64_MAX, balance(data, _GOLD) + balance(data, 3))

    def _book(self, player, data, *, now_unix_sec: int | None = None):
        response = self.protocol.message("ListSkillsResponse")
        if not self._present(data):
            response.error_message.id = _UNAVAILABLE
            return response
        now_sec = self._now_sec() if now_unix_sec is None else int(now_unix_sec)
        cls, level, gold = class_id(data), effective_level(data), self._spendable_gold(data)
        has_class = bool(self.catalogs.get(cls))
        active_season = self._active_season_id()
        response.class_id, response.player_level, response.currency_balance = cls, level, gold
        response.skill_list.SetInParent()
        ranks = {}
        for entry in data.skill_list.skill_list:
            if entry.skill_table_id in ranks:
                continue
            ranks[entry.skill_table_id] = entry.rank
            row = self.skills.get(entry.skill_table_id)
            if row is None or getattr(row, "season_id", 0) != 0:
                continue
            if not is_skill_usable(row, entry, cls, has_class, level):
                continue
            response.skill_list.skill_list.add(
                skill_table_id=row.id, rank=entry.rank if is_class_skill(row) else 0,
            )

        def _active_in_season(row) -> bool:
            sid = int(getattr(row, "season_id", 0))
            return sid == 0 or (active_season != 0 and sid == active_season)

        free_rows = [
            row for row in self.skills.values()
            if is_free_skill_for(row, 1) and _active_in_season(row)
        ]
        if free_rows:
            rows = sorted(free_rows, key=lambda r: r.id)
        else:
            rows = list(self.catalogs.get(cls, ()))

        # 当季免费赛季技能自动并入已拥有列表（按 skill_table_id 升序），不写入永久存档。
        for row in rows:
            sid = int(getattr(row, "season_id", 0))
            if not sid or sid != active_season:
                continue
            eff_rank = rank_limit(level, row)
            if eff_rank > 0:
                response.skill_list.skill_list.add(skill_table_id=row.id, rank=eff_rank)

        busy = self._busy(player)
        craft_gate = self._craft_gate_enabled()
        unlocked_books = (
            set(data.craft_component.book_unlocked_skill_ids)
            if data.HasField("craft_component") else set()
        )
        equipped_non_active = collect_equipped_non_active_skills(
            self.tables, data.skill_list, cls, has_class, level,
        )
        self._fill_loadout_snapshot(
            data, cls, has_class, level, now_sec, response.loadout,
        )

        for row in rows:
            is_seasonal = int(getattr(row, "season_id", 0)) != 0
            rank = rank_limit(level, row) if is_seasonal else ranks.get(row.id, 0)
            scroll_id = int(getattr(row, "scroll_id", 0))
            craft_required = craft_gate and not is_seasonal and scroll_id > 0
            unlocked_by_book = row.id in unlocked_books
            battle_model = int(getattr(row, "battle_model", 0))
            ability_kind = int(getattr(row, "ability_kind", 0))
            equipped = rank > 0 and (
                is_seasonal or battle_model != 1 or ability_kind == 1 or row.id in equipped_non_active
            )
            blocked_by_book = craft_required and not unlocked_by_book and rank == 0
            if is_seasonal:
                block_tip = _SEASON_NO_UPGRADE
            elif busy:
                block_tip = _BUSY
            elif blocked_by_book:
                block_tip = _CRAFT_BOOK_REQUIRED
            else:
                block_tip = _blocked(row, level, rank, gold)
            duration = next((self.buff_duration[buff_id] for buff_id in row.effect if buff_id in self.buff_duration), 0)
            response.catalog.add(
                skill_table_id=row.id, name=row.name, description=row.description, icon_key=row.icon_key,
                class_id=row.class_id, category=row.category, passive=is_passive_skill(row),
                target_team=row.target_team, max_targets=row.max_targets, unlock_level=row.unlock_level,
                max_rank=row.max_rank, rank=rank, rank_limit=rank_limit(level, row),
                mana_percent=row.mana_percent, cooldown_rounds=row.cooldown_rounds, duration_rounds=duration,
                next_rank_cost=(_upgrade_cost(rank + 1, row.unlock_level) if (not is_seasonal and rank < row.max_rank) else 0),
                upgrade_block_tip_id=block_tip,
                allow_dead_target=bool(getattr(row, "allow_dead_target", 0)),
                ability_kind=ability_kind,
                caster_mask=int(getattr(row, "caster_mask", 0)),
                rarity_tier=int(getattr(row, "rarity_tier", 0)),
                craft_unlock_required=craft_required,
                unlocked_by_book=unlocked_by_book,
                secondary_target_policy=int(getattr(row, "secondary_target_policy", 0)),
                exclusive_group=int(getattr(row, "exclusive_group", 0)),
                equipped=equipped,
            )
        return response

    def list_skills(self, world, player, route, request):
        return self._book(player, self.scene.rows.assemble(player.snapshot))

    async def upgrade(self, world, player, route, request):
        response = self.protocol.message("UpgradeSkillResponse")
        data = self.scene.rows.assemble(player.snapshot)

        def finish(error: int = 0):
            book = self._book(player, self.scene.rows.assemble(player.snapshot))
            if not book.error_message.id:
                response.state.CopyFrom(book)
            if error or book.error_message.id:
                response.error_message.id = error or book.error_message.id
            return response

        if not self._present(data):
            return finish(_UNAVAILABLE)
        if request.count not in (1, 10):
            return finish(_COUNT_INVALID)
        row = self.skills.get(request.skill_table_id)
        if not request.skill_table_id or row is None:
            return finish(_INVALID_TABLE)
        if getattr(row, "season_id", 0) != 0:
            return finish(_SEASON_NO_UPGRADE)
        cls = class_id(data)
        if not is_class_skill(row) or not is_skill_active_for(row, cls, False):
            return finish(_NOT_IN_CLASS)
        if self._busy(player):
            return finish(_BUSY)
        skills = data.skill_list.skill_list
        entry = next((item for item in skills if item.skill_table_id == row.id), None)
        rank = entry.rank if entry is not None else 0
        if (
            rank == 0
            and self._craft_gate_enabled()
            and int(getattr(row, "scroll_id", 0)) > 0
            and (not data.HasField("craft_component") or row.id not in data.craft_component.book_unlocked_skill_ids)
        ):
            return finish(_CRAFT_BOOK_REQUIRED)
        if rank != request.expected_rank:
            return finish(_RANK_CHANGED)
        level, gold = effective_level(data), self._spendable_gold(data)
        total_cost, final_rank, stop = 0, rank, 0
        for _ in range(request.count):
            stop = _blocked(row, level, final_rank, gold - total_cost)
            if stop:
                break
            final_rank += 1
            total_cost += _upgrade_cost(final_rank, row.unlock_level)
        if final_rank == rank:
            return finish(stop)
        assets = getattr(self.scene, "assets", None)
        if assets is None:
            return finish(_UNAVAILABLE)
        before = clone(data) if assets.audit_enabled else None
        error = (
            assets.deduct_gold_prefer_bind(data, total_cost)
            if hasattr(assets, "deduct_gold_prefer_bind")
            else assets.deduct_currency(data, _GOLD, total_cost)
        )
        if error:
            return finish(error)
        recalc_stats = is_passive_skill(row)
        old_stats = player_stats(self.protocol, self.tables, data) if recalc_stats else None
        if entry is None:
            # 持久化条目按 skill_table_id 索引，不铸造额外 GUID。
            entry = skills.add(skill_table_id=row.id)
        entry.rank = final_rank
        if rank == 0:
            try_auto_slot_newly_learned(self.tables, data.skill_list, row)
        if old_stats is not None:
            stats = player_stats(self.protocol, self.tables, data)
            attrs = data.derived_attributes_component
            attrs.health = _rescale(attrs.health, old_stats.max_health, stats.max_health)
            attrs.mana = _rescale(attrs.mana, old_stats.max_mana, stats.max_mana)
            attrs.speed = stats.speed
        audit = assets.make_audit_callback(player, before, data, self.debit_tx, correlation=row.id)
        attributes = getattr(self.scene, "attributes", None) if old_stats is not None else None

        async def committed(snapshot):
            if audit is not None:
                result = audit(snapshot)
                if isawaitable(result):
                    await result
            if attributes is not None:
                attributes.notify(player)

        try:
            await self.scene.commit_player_data(
                player, data, on_committed=committed if attributes is not None else audit,
            )
        except (StoreUnavailable, StoreRejected, OSError):
            return finish(_UNAVAILABLE)
        response.upgraded_count = final_rank - rank
        response.stop_tip_id = stop
        return finish()

    def _validate_loadout_scheme(
        self,
        skill_list_comp,
        cls: int,
        has_class: bool,
        level: int,
        scheme_id: int,
        role_tag: int,
        passive_ids: list[int],
        pursuit_ids: list[int],
        opening_ids: list[int],
        *,
        unlocked_schemes: int = LOADOUT_BASE_MAX_SCHEMES,
    ) -> int:
        """校验单套战法构筑装配方案，返回 0 或权威错误 tip 码（对齐 skill_loadout_rules.h）。"""
        if not _valid_loadout_scheme_id(scheme_id, unlocked_schemes) or not _valid_loadout_role_tag(role_tag):
            return _LOADOUT_SCHEME_INVALID
        if (
            len(passive_ids) > LOADOUT_MAX_PASSIVE_SLOTS
            or len(pursuit_ids) > LOADOUT_MAX_PURSUIT_SLOTS
            or len(opening_ids) > LOADOUT_MAX_OPENING_SLOTS
        ):
            return _LOADOUT_SLOT_LIMIT

        by_id = {entry.skill_table_id: entry for entry in skill_list_comp.skill_list}
        seen_skills: set[int] = set()
        occupied_groups: set[int] = set()

        def check_list(ids: list[int], expected_kind: int) -> int:
            for sid in ids:
                if not sid or sid in seen_skills:
                    return _INVALID_PARAM
                seen_skills.add(sid)
                row = self.skills.get(sid)
                if (
                    row is None
                    or getattr(row, "battle_model", 0) != 1
                    or getattr(row, "season_id", 0) != 0
                    or not is_free_skill_for(row, 1)
                    or int(getattr(row, "ability_kind", 0)) != expected_kind
                ):
                    return _INVALID_PARAM
                entry = by_id.get(sid)
                if entry is None or not is_skill_usable(row, entry, cls, has_class, level):
                    return _LOADOUT_NOT_LEARNED
                group = int(getattr(row, "exclusive_group", 0))
                if group:
                    if group in occupied_groups:
                        return _LOADOUT_CONFLICT
                    occupied_groups.add(group)
            return 0

        for ids, expected_kind in ((passive_ids, 2), (pursuit_ids, 3), (opening_ids, 4)):
            if err := check_list(ids, expected_kind):
                return err
        return 0

    async def save_loadout(self, world, player, route, request):
        """保存并激活 7 套战法·加点一体化流派方案（名称、3 被动 + 1 追击 + 1 开场 + 四维加点联动）。"""
        response = self.protocol.message("SaveSkillLoadoutResponse", request_id=request.request_id)
        data = self.scene.rows.assemble(player.snapshot)

        def finish(error: int = 0):
            if error:
                response.error_message.id = error
            book = self._book(player, self.scene.rows.assemble(player.snapshot))
            if not book.error_message.id:
                response.state.CopyFrom(book)
            elif not error:
                response.error_message.id = book.error_message.id
            return response

        if not self._present(data):
            return finish(_UNAVAILABLE)
        if not _valid_loadout_request_id(request.request_id):
            return finish(_INVALID_PARAM)

        skill_list = data.skill_list
        if skill_list.loadout_last_request_id == request.request_id:
            same = (
                skill_list.loadout_last_base_version == request.state_version
                and skill_list.loadout_last_scheme_id == request.scheme_id
            )
            if not same:
                return finish(_INVALID_PARAM)
            response.replayed = True
            return finish(0)

        if self._busy(player):
            return finish(_BUSY)
        current_ver = _loadout_version(int(skill_list.loadout_state_version))
        if request.state_version != current_ver or skill_list.loadout_state_version >= UINT64_MAX:
            return finish(_LOADOUT_VERSION_CHANGED)

        if request.name and not validate_loadout_scheme_name(request.name):
            return finish(_LOADOUT_NAME_INVALID)

        now_sec = self._now_sec()
        cls, level = class_id(data), effective_level(data)
        has_class = bool(self.catalogs.get(cls))
        passive_ids = list(request.passive_skill_ids)
        pursuit_ids = list(request.pursuit_skill_ids)
        opening_ids = list(request.opening_skill_ids)
        unlocked = self.unlocked_loadout_scheme_count(data, now_sec)
        if err := self._validate_loadout_scheme(
            skill_list, cls, has_class, level,
            request.scheme_id, request.role_tag, passive_ids, pursuit_ids, opening_ids,
            unlocked_schemes=unlocked,
        ):
            return finish(err)

        attributes = getattr(self.scene, "attributes", None)
        prepared_attr = None
        if request.update_attribute_allocation:
            if attributes is None or not hasattr(attributes, "prepare_scheme_allocation"):
                return finish(_UNAVAILABLE)
            attr_err, prepared_attr = attributes.prepare_scheme_allocation(
                player,
                data,
                int(request.scheme_id),
                {int(k): int(v) for k, v in request.attribute_allocated.items()},
                bool(request.reset_attribute_if_needed),
            )
            if attr_err:
                return finish(attr_err)

        existing_scheme = _find_loadout_scheme(skill_list, request.scheme_id)
        final_name = (
            request.name
            or (existing_scheme.name if existing_scheme is not None and existing_scheme.name else default_loadout_scheme_name(request.scheme_id))
        )

        old_stats = player_stats(self.protocol, self.tables, data)
        assets = getattr(self.scene, "assets", None)
        need_audit = bool(
            prepared_attr is not None
            and prepared_attr.needs_reset
            and prepared_attr.reset_cost_gold > 0
            and assets is not None
            and assets.audit_enabled
        )
        before = clone(data) if need_audit else None

        attr_notice = ""
        if attributes is not None and hasattr(attributes, "commit_scheme_allocation_and_switch") and (
            request.update_attribute_allocation or data.HasField("attribute_component")
        ):
            commit_err, attr_notice = attributes.commit_scheme_allocation_and_switch(
                data,
                int(request.scheme_id),
                final_name,
                bool(request.update_attribute_allocation),
                prepared_attr,
            )
            if commit_err:
                return finish(commit_err)

        passive_ids.sort()
        pursuit_ids.sort()
        opening_ids.sort()

        scheme = _find_or_create_loadout_scheme(skill_list, request.scheme_id)
        scheme.name = final_name
        scheme.role_tag = request.role_tag
        del scheme.passive_skill_ids[:]
        del scheme.pursuit_skill_ids[:]
        del scheme.opening_skill_ids[:]
        scheme.passive_skill_ids.extend(passive_ids)
        scheme.pursuit_skill_ids.extend(pursuit_ids)
        scheme.opening_skill_ids.extend(opening_ids)

        skill_list.active_loadout_scheme_id = request.scheme_id
        skill_list.loadout_state_version = current_ver + 1
        skill_list.loadout_last_request_id = request.request_id
        skill_list.loadout_last_base_version = request.state_version
        skill_list.loadout_last_scheme_id = request.scheme_id

        stats = player_stats(self.protocol, self.tables, data)
        attrs = data.derived_attributes_component
        attrs.health = _rescale(attrs.health, old_stats.max_health, stats.max_health)
        attrs.mana = _rescale(attrs.mana, old_stats.max_mana, stats.max_mana)
        attrs.speed = stats.speed

        audit = (
            assets.make_audit_callback(player, before, data, self.debit_tx)
            if before is not None and assets is not None
            else None
        )

        async def committed(snapshot):
            if audit is not None:
                res = audit(snapshot)
                if isawaitable(res):
                    await res
            if attributes is not None:
                attributes.notify(player)

        try:
            await self.scene.commit_player_data(
                player, data, on_committed=committed if (audit is not None or attributes is not None) else None,
            )
        except (StoreUnavailable, StoreRejected, OSError):
            return finish(_UNAVAILABLE)
        if attr_notice:
            response.attribute_sync_notice = attr_notice
        logger.info(
            "Skill loadout saved player_id=%s scheme_id=%s role_tag=%s passives=%s pursuits=%s openings=%s attr_updated=%s version=%s",
            player.player_id, request.scheme_id, request.role_tag,
            len(passive_ids), len(pursuit_ids), len(opening_ids),
            bool(request.update_attribute_allocation), skill_list.loadout_state_version,
        )
        return finish(0)

    async def switch_loadout(self, world, player, route, request):
        """一键切换 7 套战法·加点一体化流派方案（1..7），带乐观锁、属性同步与 request_id 幂等。"""
        response = self.protocol.message("SwitchSkillLoadoutResponse", request_id=request.request_id)
        data = self.scene.rows.assemble(player.snapshot)

        def finish(error: int = 0):
            if error:
                response.error_message.id = error
            book = self._book(player, self.scene.rows.assemble(player.snapshot))
            if not book.error_message.id:
                response.state.CopyFrom(book)
            elif not error:
                response.error_message.id = book.error_message.id
            return response

        if not self._present(data):
            return finish(_UNAVAILABLE)
        if not _valid_loadout_request_id(request.request_id):
            return finish(_INVALID_PARAM)
        if not _valid_loadout_scheme_id(request.scheme_id, LOADOUT_MAX_SCHEMES):
            return finish(_LOADOUT_SCHEME_INVALID)

        skill_list = data.skill_list
        if skill_list.loadout_last_request_id == request.request_id:
            same = (
                skill_list.loadout_last_base_version == request.state_version
                and skill_list.loadout_last_scheme_id == request.scheme_id
            )
            if not same:
                return finish(_INVALID_PARAM)
            response.replayed = True
            return finish(0)

        now_sec = self._now_sec()
        if not _valid_loadout_scheme_id(request.scheme_id, self.unlocked_loadout_scheme_count(data, now_sec)):
            return finish(_LOADOUT_SCHEME_INVALID)
        if self._busy(player):
            return finish(_BUSY)
        current_ver = _loadout_version(int(skill_list.loadout_state_version))
        if request.state_version != current_ver or skill_list.loadout_state_version >= UINT64_MAX:
            return finish(_LOADOUT_VERSION_CHANGED)

        old_stats = player_stats(self.protocol, self.tables, data)
        scheme = _find_or_create_loadout_scheme(skill_list, request.scheme_id)
        scheme_name = scheme.name or default_loadout_scheme_name(request.scheme_id)
        skill_list.active_loadout_scheme_id = request.scheme_id
        skill_list.loadout_state_version = current_ver + 1
        skill_list.loadout_last_request_id = request.request_id
        skill_list.loadout_last_base_version = request.state_version
        skill_list.loadout_last_scheme_id = request.scheme_id

        attributes = getattr(self.scene, "attributes", None)
        attr_notice = ""
        if not request.skip_attribute_sync and attributes is not None and hasattr(attributes, "try_sync_linked_scheme_for_skill"):
            attr_notice = attributes.try_sync_linked_scheme_for_skill(data, int(request.scheme_id), scheme_name)

        stats = player_stats(self.protocol, self.tables, data)
        attrs = data.derived_attributes_component
        attrs.health = _rescale(attrs.health, old_stats.max_health, stats.max_health)
        attrs.mana = _rescale(attrs.mana, old_stats.max_mana, stats.max_mana)
        attrs.speed = stats.speed

        on_committed = (lambda _snapshot: attributes.notify(player)) if attributes is not None else None
        try:
            await self.scene.commit_player_data(player, data, on_committed=on_committed)
        except (StoreUnavailable, StoreRejected, OSError):
            return finish(_UNAVAILABLE)
        if attr_notice:
            response.attribute_sync_notice = attr_notice
        logger.info(
            "Skill loadout switched player_id=%s scheme_id=%s skip_attr_sync=%s version=%s",
            player.player_id, request.scheme_id, bool(request.skip_attribute_sync), skill_list.loadout_state_version,
        )
        return finish(0)

    async def rent_loadout_slots(self, world, player, route, request):
        """565 RentSkillLoadoutSlots：7 套非永久流派方案槽位按月真实灵石租用/扩容/续期。"""
        return await self.rent_loadout_slots_at(player, request, self._now_sec())

    async def rent_loadout_slots_at(self, player, request, now_unix_sec: int):
        """指定时间戳执行流派方案槽位租用（便于确定性测试到期/补差/重放）。"""
        now_sec = int(now_unix_sec)
        client_request_id = int(request.client_request_id)
        response = self.protocol.message("RentSkillLoadoutSlotsResponse", client_request_id=client_request_id)
        data = self.scene.rows.assemble(player.snapshot)

        def finish(error: int = 0):
            if error:
                response.error_message.id = error
            book = self._book(player, self.scene.rows.assemble(player.snapshot), now_unix_sec=now_sec)
            if not book.error_message.id:
                response.state.CopyFrom(book)
            elif not error:
                response.error_message.id = book.error_message.id
            return response

        if not self._present(data):
            return finish(_UNAVAILABLE)
        if not client_request_id:
            return finish(_INVALID_PARAM)

        # 1. 持久化幂等键查账排在一切状态预检之前（避免首次成交后重放因价格/租期已变而被拒）。
        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(
            ledger, request, _CLIENT_REQUEST_KIND_RENT_SKILL_LOADOUT_SLOTS,
        )
        if err:
            return finish(err)
        if prior is not None:
            response.replayed = True
            response.rented_extra_slots = int(prior.skill_loadout_rented_slots)
            response.rent_expires_at_sec = int(prior.skill_loadout_rent_expires_at_sec)
            response.spent_diamond = int(prior.skill_loadout_rent_spent_diamond)
            return finish(0)

        # 2. 可写闸（战斗中/冻结/交接/退出拒绝）。
        if self._busy(player):
            return finish(_BUSY)

        skill_list = data.skill_list
        free_schemes = free_loadout_schemes_for_level(effective_level(data))
        light_active = self._is_light_card_active(data, now_sec)
        plan = plan_skill_loadout_rent(
            light_active,
            int(skill_list.rented_extra_slots),
            int(skill_list.rent_expires_at_sec),
            now_sec,
            int(request.target_rented_slots),
            int(request.add_periods),
            free_schemes,
        )
        if plan.error != LoadoutRentError.NONE:
            return finish(_map_rent_error(plan.error))
        if int(request.expected_cost_diamond) != plan.total_cost_diamond:
            return finish(_LOADOUT_RENT_PRICE_CHANGED)
        # 严格仅消耗真实非绑灵石（CURRENCY_TYPE_DIAMOND = 1），禁止混扣绑定灵石。
        if plan.total_cost_diamond <= 0 or plan.total_cost_diamond > _MAX_INT64:
            return finish(_INVALID_PARAM)
        assets = getattr(self.scene, "assets", None)
        if assets is None:
            return finish(_UNAVAILABLE)
        if balance(data, _DIAMOND) < plan.total_cost_diamond:
            return finish(_LOADOUT_RENT_DIAMOND_NOT_ENOUGH)

        staged = clone(data)
        before = clone(data) if assets.audit_enabled else None
        deduct_err = assets.deduct_currency(staged, _DIAMOND, plan.total_cost_diamond)
        if deduct_err:
            logger.error(
                "Skill loadout slot rent charge failed after precheck player_id=%s cost_diamond=%s err=%s client_request_id=%s",
                player.player_id, plan.total_cost_diamond, deduct_err, client_request_id,
            )
            return finish(_LOADOUT_RENT_DIAMOND_NOT_ENOUGH if deduct_err == 1004 else deduct_err)

        sl = staged.skill_list
        sl.rented_extra_slots = plan.new_rented_slots
        sl.rent_expires_at_sec = plan.new_expires_at_sec
        if sl.loadout_state_version < UINT64_MAX:
            sl.loadout_state_version = _loadout_version(int(sl.loadout_state_version)) + 1

        client_request.remember(
            staged.client_request_ledger,
            client_request_id,
            _CLIENT_REQUEST_KIND_RENT_SKILL_LOADOUT_SLOTS,
            fingerprint,
            skill_loadout_rented_slots=plan.new_rented_slots,
            skill_loadout_rent_expires_at_sec=plan.new_expires_at_sec,
            skill_loadout_rent_spent_diamond=plan.total_cost_diamond,
        )

        audit = assets.make_audit_callback(
            player, before, staged, self.rent_tx, correlation=client_request_id,
        )
        attributes = getattr(self.scene, "attributes", None)

        async def committed(snapshot):
            if audit is not None:
                res = audit(snapshot)
                if isawaitable(res):
                    await res
            if attributes is not None and staged.HasField("attribute_component"):
                attributes.notify(player)

        try:
            await self.scene.commit_player_data(
                player, staged, on_committed=committed if (audit is not None or attributes is not None) else None,
            )
        except (StoreUnavailable, StoreRejected, OSError):
            return finish(_UNAVAILABLE)

        response.rented_extra_slots = plan.new_rented_slots
        response.rent_expires_at_sec = plan.new_expires_at_sec
        response.spent_diamond = plan.total_cost_diamond
        logger.info(
            "metric=skill_loadout_slots_rented player_id=%s rented_slots=%s add_periods=%s spent_diamond=%s prorate_diamond=%s renew_diamond=%s expires_at=%s client_request_id=%s",
            player.player_id, plan.new_rented_slots, request.add_periods,
            plan.total_cost_diamond, plan.prorate_cost_diamond, plan.renew_cost_diamond,
            plan.new_expires_at_sec, client_request_id,
        )
        return finish(0)
