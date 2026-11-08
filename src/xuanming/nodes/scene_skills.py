"""Scene class skill book, initial grants, and atomic paid upgrades."""

from ..data import StoreRejected, StoreUnavailable
from ..data.asset_inventory import balance, clone
from .battle_tables import rounds_from_seconds
from .scene_skill_casts import SceneSkillCasts
from .scene_stats import class_id, effective_level, player_stats, rank_limit


_SERVICE = "SceneSkillClientPlayer"
_GOLD = 0
_INVALID_TABLE, _UNAVAILABLE = 1001, 1003
_NOT_IN_CLASS, _RANK_CHANGED, _COUNT_INVALID = 7007, 7008, 7009
_LEVEL_NOT_ENOUGH, _MAX_RANK, _GOLD_NOT_ENOUGH, _BUSY = 7010, 7011, 7012, 7013


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
    def __init__(self, scene, tables):
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        transaction_types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self.debit_tx = transaction_types.values_by_name["TX_CURRENCY_DEDUCT"].number
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
        self.scene.register(_SERVICE, "ListSkills", self.list_skills)
        self.scene.register(_SERVICE, "UpgradeSkill", self.upgrade, mutating=True)
        self.scene.register(_SERVICE, "ReleaseSkill", self.casts.release, mutating=True)

    async def close(self) -> None:
        await self.casts.close()

    def _busy(self, player) -> bool:
        return not self.scene.mutable(player) or bool(self.scene.battle and self.scene.battle.frozen(player.player_id))

    @staticmethod
    def _present(data) -> bool:
        return all(data.HasField(field) for field in ("skill_list", "level_component", "uint32_pb_component"))

    def _usable(self, data, row) -> bool:
        cls = class_id(data)
        entry = next((item for item in data.skill_list.skill_list if item.skill_table_id == row.id), None)
        if entry is None:
            return False
        if row.class_id:
            return row.class_id == cls and bool(min(entry.rank, rank_limit(effective_level(data), row)))
        return not self.catalogs.get(cls)

    def _book(self, player, data):
        response = self.protocol.message("ListSkillsResponse")
        if not self._present(data):
            response.error_message.id = _UNAVAILABLE
            return response
        cls, level, gold = class_id(data), effective_level(data), balance(data, _GOLD)
        catalog = self.catalogs.get(cls, ())
        response.class_id, response.player_level, response.currency_balance = cls, level, gold
        response.skill_list.SetInParent()
        ranks = {}
        for entry in data.skill_list.skill_list:
            if entry.skill_table_id in ranks:
                continue
            ranks[entry.skill_table_id] = entry.rank
            row = self.skills.get(entry.skill_table_id)
            if row is None or (row.class_id and (row.class_id != cls or not min(entry.rank, rank_limit(level, row)))):
                continue
            if not row.class_id and catalog:
                continue
            response.skill_list.skill_list.add(skill_table_id=row.id, rank=entry.rank if row.class_id else 0)
        busy = self._busy(player)
        for row in catalog:
            rank = ranks.get(row.id, 0)
            duration = next((self.buff_duration[buff_id] for buff_id in row.effect if buff_id in self.buff_duration), 0)
            response.catalog.add(
                skill_table_id=row.id, name=row.name, description=row.description, icon_key=row.icon_key,
                class_id=cls, category=row.category, passive=bool(row.passive_stat), target_team=row.target_team,
                max_targets=row.max_targets, unlock_level=row.unlock_level, max_rank=row.max_rank,
                rank=rank, rank_limit=rank_limit(level, row), mana_percent=row.mana_percent,
                cooldown_rounds=row.cooldown_rounds, duration_rounds=duration,
                next_rank_cost=_upgrade_cost(rank + 1, row.unlock_level) if rank < row.max_rank else 0,
                upgrade_block_tip_id=_BUSY if busy else _blocked(row, level, rank, gold),
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
        if not row.class_id or row.class_id != class_id(data):
            return finish(_NOT_IN_CLASS)
        if self._busy(player):
            return finish(_BUSY)
        skills = data.skill_list.skill_list
        entry = next((item for item in skills if item.skill_table_id == row.id), None)
        rank = entry.rank if entry is not None else 0
        if rank != request.expected_rank:
            return finish(_RANK_CHANGED)
        level, gold = effective_level(data), balance(data, _GOLD)
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
        error = assets.deduct_currency(data, _GOLD, total_cost)
        if error:
            return finish(error)
        old_stats = player_stats(self.protocol, self.tables, data) if row.passive_stat else None
        if entry is None:
            # The C++ persistence entry is keyed by skill_table_id, without minting a GUID.
            entry = skills.add(skill_table_id=row.id)
        entry.rank = final_rank
        if old_stats is not None:
            stats = player_stats(self.protocol, self.tables, data)
            attrs = data.derived_attributes_component
            attrs.health = _rescale(attrs.health, old_stats.max_health, stats.max_health)
            attrs.mana = _rescale(attrs.mana, old_stats.max_mana, stats.max_mana)
            attrs.speed = stats.speed
        audit = assets.make_audit_callback(player, before, data, self.debit_tx, correlation=row.id)
        attributes = getattr(self.scene, "attributes", None) if old_stats is not None else None

        def committed(snapshot):
            if audit is not None:
                audit(snapshot)
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
