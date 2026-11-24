"""战斗剧本校验、官方目录、客户端 CRUD 与运营审核逻辑。

与 Go ``go/match/internal/battlescript``、``internal/logic/battlescriptlogic.go``
及 ``internal/logic/battlescriptreviewlogic.go`` 保持口径逐项一致。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Protocol as TypingProtocol, runtime_checkable

from google.protobuf import json_format
from google.protobuf.message import DecodeError, Message

from ..data.config_tables import ConfigTables
from ..protocol import Protocol
from .errors import (
    ERR_MATCH_BATTLE_SCRIPT_INVALID,
    ERR_MATCH_BATTLE_SCRIPT_LIMIT_REACHED,
    ERR_MATCH_BATTLE_SCRIPT_LOADOUT_FULL,
    ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID,
    ERR_MATCH_BATTLE_SCRIPT_NOT_APPROVED,
    ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND,
    ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT,
    ERR_MATCH_BATTLE_SCRIPT_TOO_LARGE,
    ERR_MATCH_INTERNAL,
    MatchBusinessError,
    MatchError,
    set_tip,
)
from .model import (
    MODE_1V1,
    MODE_3V3,
    MODE_5V5,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    MODE_PVP_CHALLENGE,
    MODE_PVP_TEAM,
    CallerIdentity,
    valid_operator_text,
)

logger = logging.getLogger(__name__)

# -- 结构与容量上限（与 C++ battle_script.h 及 Go battlescript/validate.go 逐项一致）----------------

SCHEMA_VERSION = 1
MAX_ROLES = 8
MAX_RULES_PER_ROLE = 16
MAX_CONDITIONS_PER_RULE = 2
MAX_FILTERS_PER_RULE = 3
MAX_ROLE_MEMBERS = 10
MAX_ROUND = 1000
MAX_UNIT_COUNT = 20
MAX_CLASS_ID = 1000
PERMYRIAD_FULL = 10000
MAX_NAME_BYTES = 64
MAX_DESCRIPTION_BYTES = 512
MAX_NOTE_BYTES = 256

MAX_OFFICIAL_SCRIPTS = 256
MAX_OFFICIAL_BINDINGS = 1024

MAX_BATTLE_SCRIPTS_PER_PLAYER = 10
MAX_BATTLE_SCRIPT_LOADOUT_ENTRIES = 32
MAX_PLAYER_BATTLE_SCRIPT_BYTES = 900

MAX_PENDING_BATTLE_SCRIPTS_PAGE = 50
MAX_REVIEWER_BYTES = 64
MAX_REVIEW_REASON_BYTES = 191

# -- 枚举常量 ----------------------------------------------------------------------------------------

SCRIPT_SIDE_PLAYER = 1
SCRIPT_SIDE_MONSTER = 2

SCRIPT_ORIGIN_UNSPECIFIED = 0
SCRIPT_ORIGIN_OFFICIAL = 1
SCRIPT_ORIGIN_PLAYER = 2

REVIEW_STATE_UNSPECIFIED = 0
REVIEW_STATE_PENDING = 1
REVIEW_STATE_APPROVED = 2
REVIEW_STATE_REJECTED = 3

REVIEW_REJECT_NONE = 0
REVIEW_REJECT_DENIED = 1
REVIEW_REJECT_INVALID = 2
REVIEW_REJECT_STALE = 3
REVIEW_REJECT_INTERNAL = 4

# eBattleActorType
_ACTOR_TYPE_NONE = 0
_ACTOR_TYPE_PLAYER = 1
_ACTOR_TYPE_MONSTER = 2
_ACTOR_TYPE_PET = 3
_KNOWN_ACTOR_TYPES = frozenset((_ACTOR_TYPE_NONE, _ACTOR_TYPE_PLAYER, _ACTOR_TYPE_MONSTER, _ACTOR_TYPE_PET))

# eBattleActionType
_ACTION_ATTACK = 1
_ACTION_SKILL = 2
_ACTION_DEFEND = 3

# eBattleScriptCondition
_COND_SELF_HP_BELOW = 1
_COND_ROUND_AT_LEAST = 2
_COND_ROUND_EVERY = 3
_COND_ENEMY_COUNT_AT_LEAST = 4

# eBattleScriptTargetSide
_TARGET_SELF = 1
_TARGET_ENEMY = 2
_TARGET_ALLY = 3
_TARGET_ENGINE_PICK = 4

# eBattleScriptTargetFilter
_FILTER_HP_BELOW = 1
_FILTER_HAS_BUFF_TYPE = 2
_FILTER_HAS_BUFF = 3
_FILTER_IS_MONSTER = 4
_FILTER_IS_CLASS = 5
_FILTER_IS_ACTOR_TYPE = 6
_FILTER_CLAIMED_BY_ROLE = 7

# eBattleScriptTargetOrder (0..4)
_KNOWN_TARGET_ORDERS = frozenset((0, 1, 2, 3, 4))

# 持续状态 Buff 类型集合（与 Go statusBuffTypes 一致）
STATUS_BUFF_TYPES = frozenset((10, 11, 13, 30, 31, 32, 34, 40, 41, 42, 50, 51, 52))

# 技能类型禁止位与法力消耗/目标阵营常量
_SKILL_TYPE_BIT_PASSIVE = 0
_SKILL_TYPE_BIT_CHANNEL = 2
_SKILL_TYPE_BIT_TOGGLE = 3
_SKILL_COST_RESOURCE_MANA = 1
_TARGET_TEAM_ENEMY = 1
_TARGET_TEAM_ALLY = 2
_TARGET_TEAM_SELF = 3

_KNOWN_MATCH_MODES = frozenset(
    (0, MODE_5V5, MODE_3V3, MODE_1V1, MODE_PVE_SOLO, MODE_PVE_TEAM, MODE_PVP_CHALLENGE, MODE_PVP_TEAM)
)
_RATED_MODES = frozenset((MODE_1V1, MODE_5V5, MODE_PVP_TEAM))

# 内置官方剧本配置（与 Go go/match/internal/battlescript/official_scripts.json 一致）
DEFAULT_OFFICIAL_SCRIPTS_CONFIG: dict = {
    "scripts": [
        {
            "schema_version": 1,
            "script_id": 1,
            "name": "四职业·集火控速",
            "description": "丹心保命,破军点名集火,玄霄跟着破军的目标打,第四职业先给最快的敌人减速;宝宝跟集火。",
            "roles": [
                {
                    "role_id": 1,
                    "order": 10,
                    "name": "丹心·治疗",
                    "actor_type": "BATTLE_ACTOR_TYPE_PLAYER",
                    "class_id": 3,
                    "rules": [
                        {
                            "priority": 10,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1305,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ALLY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_RATIO_LOWEST",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_HP_BELOW", "p1": 3000}],
                            },
                            "note": "队友气血低于三成:先急救最危险的",
                        },
                        {
                            "priority": 20,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1302,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ALLY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_RATIO_LOWEST",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_HP_BELOW", "p1": 7000}],
                            },
                            "note": "有队友气血低于七成:群疗",
                        },
                        {
                            "priority": 30,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_CLAIMED_BY_ROLE", "p1": 2}],
                            },
                            "note": "没人要治:跟着破军集火",
                        },
                        {
                            "priority": 40,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {"side": "BATTLE_SCRIPT_TARGET_ENGINE_PICK"},
                        },
                    ],
                },
                {
                    "role_id": 2,
                    "order": 20,
                    "name": "破军·点名",
                    "actor_type": "BATTLE_ACTOR_TYPE_PLAYER",
                    "class_id": 1,
                    "rules": [
                        {
                            "priority": 10,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1104,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_HIGHEST",
                                "filters": [
                                    {"kind": "BATTLE_SCRIPT_FILTER_IS_ACTOR_TYPE", "p1": 1},
                                    {"kind": "BATTLE_SCRIPT_FILTER_HAS_BUFF", "p1": 11104, "negate": True},
                                ],
                            },
                            "note": "PVP:给还没被削的敌方玩家减物攻(怪物物攻为 0,不放)",
                        },
                        {
                            "priority": 20,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1102,
                            "conditions": [{"kind": "BATTLE_SCRIPT_COND_ENEMY_COUNT_AT_LEAST", "p1": 3}],
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_LOWEST",
                            },
                            "note": "敌人三个以上:群攻,主目标取血最少的",
                        },
                        {
                            "priority": 30,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1101,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_LOWEST",
                            },
                            "note": "点杀血最少的",
                        },
                        {
                            "priority": 40,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_LOWEST",
                            },
                        },
                    ],
                },
                {
                    "role_id": 3,
                    "order": 30,
                    "name": "玄霄·跟打",
                    "actor_type": "BATTLE_ACTOR_TYPE_PLAYER",
                    "class_id": 2,
                    "rules": [
                        {
                            "priority": 10,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1202,
                            "conditions": [{"kind": "BATTLE_SCRIPT_COND_ENEMY_COUNT_AT_LEAST", "p1": 3}],
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_CLAIMED_BY_ROLE", "p1": 2}],
                            },
                            "note": "敌人三个以上:以破军的目标为主目标放雷阵",
                        },
                        {
                            "priority": 20,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1201,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_CLAIMED_BY_ROLE", "p1": 2}],
                            },
                            "note": "组合:跟破军打同一个",
                        },
                        {
                            "priority": 30,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1201,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_LOWEST",
                            },
                        },
                        {
                            "priority": 40,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_LOWEST",
                            },
                        },
                    ],
                },
                {
                    "role_id": 4,
                    "order": 40,
                    "name": "控速",
                    "actor_type": "BATTLE_ACTOR_TYPE_PLAYER",
                    "class_id": 4,
                    "rules": [
                        {
                            "priority": 10,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1404,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_SPEED_HIGHEST",
                                "filters": [
                                    {"kind": "BATTLE_SCRIPT_FILTER_HAS_BUFF", "p1": 11404, "negate": True}
                                ],
                            },
                            "note": "先给最快、还没被减速的敌人减速",
                        },
                        {
                            "priority": 20,
                            "action": "BATTLE_ACTION_SKILL",
                            "skill_id": 1401,
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_CLAIMED_BY_ROLE", "p1": 2}],
                            },
                        },
                        {
                            "priority": 30,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_LOWEST",
                            },
                        },
                    ],
                },
                {
                    "role_id": 5,
                    "order": 50,
                    "name": "宝宝",
                    "actor_type": "BATTLE_ACTOR_TYPE_PET",
                    "rules": [
                        {
                            "priority": 10,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_CLAIMED_BY_ROLE", "p1": 2}],
                            },
                        },
                        {
                            "priority": 20,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {"side": "BATTLE_SCRIPT_TARGET_ENGINE_PICK"},
                        },
                    ],
                },
            ],
        },
        {
            "schema_version": 1,
            "script_id": 2,
            "name": "首领·先打治疗",
            "description": "首领盯住气血比例最低的丹心,小怪跟着首领打同一个人。",
            "roles": [
                {
                    "role_id": 1,
                    "order": 10,
                    "name": "首领",
                    "actor_type": "BATTLE_ACTOR_TYPE_MONSTER",
                    "monster_id": 16,
                    "rules": [
                        {
                            "priority": 10,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_RATIO_LOWEST",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_IS_CLASS", "p1": 3}],
                            },
                        },
                        {
                            "priority": 20,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "order": "BATTLE_SCRIPT_ORDER_HP_LOWEST",
                            },
                        },
                    ],
                },
                {
                    "role_id": 2,
                    "order": 20,
                    "name": "小怪",
                    "actor_type": "BATTLE_ACTOR_TYPE_MONSTER",
                    "rules": [
                        {
                            "priority": 10,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {
                                "side": "BATTLE_SCRIPT_TARGET_ENEMY",
                                "filters": [{"kind": "BATTLE_SCRIPT_FILTER_CLAIMED_BY_ROLE", "p1": 1}],
                            },
                        },
                        {
                            "priority": 20,
                            "action": "BATTLE_ACTION_ATTACK",
                            "target": {"side": "BATTLE_SCRIPT_TARGET_ENGINE_PICK"},
                        },
                    ],
                },
            ],
        },
    ],
    "bindings": [
        {"match_mode": 0, "battle_config_id": 3, "side": "BATTLE_SCRIPT_SIDE_MONSTER", "script_id": 2},
    ],
}


# -- 异常与配置表端口 ---------------------------------------------------------------------------------


class BattleScriptValidationError(MatchError):
    """战斗剧本结构、参数或配表校验失败。"""


class BattleScriptReviewError(MatchError):
    """运营审核带内拒绝异常，携带 ``BattleScriptReviewReject`` 枚举码。"""

    def __init__(self, reject: int, detail: str = "") -> None:
        super().__init__(detail or f"battle script review rejected code={reject}")
        self.reject = reject


@dataclass(frozen=True, slots=True)
class SkillInfo:
    """剧本校验所需的技能配置摘要（不可变值对象）。"""

    skill_id: int
    class_id: int = 0
    target_team: int = 0
    castable: bool = True
    legacy_mana_cost: int = 0


@runtime_checkable
class BattleScriptTables(TypingProtocol):
    """剧本校验查询配置表的抽象端口。"""

    def skill(self, skill_id: int) -> SkillInfo | None: ...
    def has_buff(self, buff_id: int) -> bool: ...
    def has_monster(self, monster_id: int) -> bool: ...
    def has_dungeon(self, dungeon_id: int) -> bool: ...


class ConfigTableScriptLookup:
    """基于仓库内置 ``ConfigTables`` 的剧本配置表查询实现。"""

    def __init__(self, tables: ConfigTables | None = None) -> None:
        self._tables = tables if tables is not None else ConfigTables()

    def skill(self, skill_id: int) -> SkillInfo | None:
        row = self._tables["Skill"].get(int(skill_id))
        if row is None:
            return None
        passive = bool(getattr(row, "passive_stat", ""))
        skill_types = tuple(int(bit) for bit in getattr(row, "skill_type", ()))
        is_castable = not passive and not any(
            bit in (_SKILL_TYPE_BIT_PASSIVE, _SKILL_TYPE_BIT_TOGGLE, _SKILL_TYPE_BIT_CHANNEL)
            for bit in skill_types
        )
        mana_cost = 0
        for entry in getattr(row, "cost_resource", ()):
            if int(getattr(entry, "cost_resource_id", 0)) == _SKILL_COST_RESOURCE_MANA:
                mana_cost += int(getattr(entry, "cost_resource_cost", 0))
        return SkillInfo(
            skill_id=int(skill_id),
            class_id=int(getattr(row, "class_id", 0)),
            target_team=int(getattr(row, "target_team", 0)),
            castable=is_castable,
            legacy_mana_cost=mana_cost,
        )

    def has_buff(self, buff_id: int) -> bool:
        return self._tables["Buff"].get(int(buff_id)) is not None

    def has_monster(self, monster_id: int) -> bool:
        return self._tables["Monster"].get(int(monster_id)) is not None

    def has_dungeon(self, dungeon_id: int) -> bool:
        return self._tables["Dungeon"].get(int(dungeon_id)) is not None


# -- 纯校验与规范化函数 -------------------------------------------------------------------------------


def is_known_match_mode(match_mode: int) -> bool:
    return int(match_mode) in _KNOWN_MATCH_MODES


def is_pve_match_mode(match_mode: int) -> bool:
    return int(match_mode) in (MODE_PVE_SOLO, MODE_PVE_TEAM)


def _in_range(value: int, low: int, high: int) -> bool:
    return low <= value <= high


def _check_condition(cond: Message, where: str) -> None:
    kind = int(cond.kind)
    p1, p2 = int(cond.p1), int(cond.p2)
    if kind == _COND_SELF_HP_BELOW:
        if not _in_range(p1, 1, PERMYRIAD_FULL) or p2 != 0:
            raise BattleScriptValidationError(f"{where}: SELF_HP_BELOW needs p1 in 1..10000 and p2 = 0")
    elif kind == _COND_ROUND_AT_LEAST:
        if not _in_range(p1, 1, MAX_ROUND) or p2 != 0:
            raise BattleScriptValidationError(f"{where}: ROUND_AT_LEAST needs p1 in 1..{MAX_ROUND} and p2 = 0")
    elif kind == _COND_ROUND_EVERY:
        if not _in_range(p1, 1, MAX_ROUND) or not _in_range(p2, 1, MAX_ROUND):
            raise BattleScriptValidationError(
                f"{where}: ROUND_EVERY needs period p1 and first round p2 in 1..{MAX_ROUND}"
            )
    elif kind == _COND_ENEMY_COUNT_AT_LEAST:
        if not _in_range(p1, 1, MAX_UNIT_COUNT) or p2 != 0:
            raise BattleScriptValidationError(
                f"{where}: ENEMY_COUNT_AT_LEAST needs p1 in 1..{MAX_UNIT_COUNT} and p2 = 0"
            )
    else:
        raise BattleScriptValidationError(f"{where}: unknown condition kind {kind}")


def _check_filter(flt: Message, role_ids: set[int], tables: BattleScriptTables, where: str) -> None:
    kind = int(flt.kind)
    p1 = int(flt.p1)
    if kind == _FILTER_HP_BELOW:
        if not _in_range(p1, 1, PERMYRIAD_FULL):
            raise BattleScriptValidationError(f"{where}: HP_BELOW needs p1 in 1..10000")
    elif kind == _FILTER_HAS_BUFF_TYPE:
        if p1 not in STATUS_BUFF_TYPES:
            raise BattleScriptValidationError(f"{where}: HAS_BUFF_TYPE {p1} is not a status type")
    elif kind == _FILTER_HAS_BUFF:
        if p1 == 0 or not tables.has_buff(p1):
            raise BattleScriptValidationError(f"{where}: unknown buff {p1}")
    elif kind == _FILTER_IS_MONSTER:
        if p1 == 0 or not tables.has_monster(p1):
            raise BattleScriptValidationError(f"{where}: unknown monster {p1}")
    elif kind == _FILTER_IS_CLASS:
        if not _in_range(p1, 1, MAX_CLASS_ID):
            raise BattleScriptValidationError(f"{where}: IS_CLASS needs a class id")
    elif kind == _FILTER_IS_ACTOR_TYPE:
        if p1 not in (_ACTOR_TYPE_PLAYER, _ACTOR_TYPE_MONSTER, _ACTOR_TYPE_PET):
            raise BattleScriptValidationError(f"{where}: IS_ACTOR_TYPE needs 1 (player), 2 (monster) or 3 (pet)")
    elif kind == _FILTER_CLAIMED_BY_ROLE:
        if p1 != 0 and p1 not in role_ids:
            raise BattleScriptValidationError(f"{where}: CLAIMED_BY_ROLE refers to unknown role {p1}")
    else:
        raise BattleScriptValidationError(f"{where}: unknown filter kind {kind}")


def _check_target(target: Message, role_ids: set[int], tables: BattleScriptTables, where: str) -> None:
    side = int(target.side)
    if side not in (_TARGET_SELF, _TARGET_ENEMY, _TARGET_ALLY, _TARGET_ENGINE_PICK):
        raise BattleScriptValidationError(f"{where}: target side must be SELF, ENEMY, ALLY or ENGINE_PICK")
    order = int(target.order)
    if order not in _KNOWN_TARGET_ORDERS:
        raise BattleScriptValidationError(f"{where}: unknown target order {order}")
    if len(target.filters) > MAX_FILTERS_PER_RULE:
        raise BattleScriptValidationError(f"{where}: at most {MAX_FILTERS_PER_RULE} target filters")
    pickable = side in (_TARGET_ENEMY, _TARGET_ALLY)
    if not pickable and (len(target.filters) != 0 or order != 0):
        raise BattleScriptValidationError(f"{where}: SELF / ENGINE_PICK targets take no filters and no order")
    for flt in target.filters:
        _check_filter(flt, role_ids, tables, where)


def _check_skill_action(role: Message, rule: Message, tables: BattleScriptTables, where: str) -> None:
    side = int(rule.target.side)
    skill_id = int(rule.skill_id)
    if skill_id == 0:
        raise BattleScriptValidationError(f"{where}: SKILL needs skill_id")
    info = tables.skill(skill_id)
    if info is None:
        raise BattleScriptValidationError(f"{where}: unknown skill {skill_id}")
    if not info.castable:
        raise BattleScriptValidationError(
            f"{where}: skill {skill_id} cannot be cast in turn battles (passive, toggle or channel)"
        )
    if info.class_id == 0:
        if side != _TARGET_ENEMY:
            raise BattleScriptValidationError(f"{where}: legacy skill {skill_id} can only target ENEMY")
        return
    actor_type = int(role.actor_type)
    if actor_type not in (_ACTOR_TYPE_NONE, _ACTOR_TYPE_PLAYER):
        raise BattleScriptValidationError(f"{where}: class skill {skill_id} needs a role that accepts players")
    role_class = int(role.class_id)
    if role_class != 0 and role_class != info.class_id:
        raise BattleScriptValidationError(
            f"{where}: class skill {skill_id} belongs to class {info.class_id} "
            f"but the role only accepts class {role_class}"
        )
    if side == _TARGET_ENGINE_PICK:
        return
    matches = (
        (info.target_team == _TARGET_TEAM_ENEMY and side == _TARGET_ENEMY)
        or (info.target_team == _TARGET_TEAM_ALLY and side == _TARGET_ALLY)
        or (info.target_team == _TARGET_TEAM_SELF and side == _TARGET_SELF)
    )
    if not matches:
        raise BattleScriptValidationError(
            f"{where}: class skill {skill_id} target side does not match its target_team {info.target_team}"
        )


def _check_action(role: Message, rule: Message, tables: BattleScriptTables, where: str) -> None:
    action = int(rule.action)
    side = int(rule.target.side)
    if action == _ACTION_ATTACK:
        if int(rule.skill_id) != 0:
            raise BattleScriptValidationError(f"{where}: ATTACK carries no skill_id")
        if side not in (_TARGET_ENEMY, _TARGET_ENGINE_PICK):
            raise BattleScriptValidationError(f"{where}: ATTACK targets ENEMY or ENGINE_PICK")
    elif action == _ACTION_DEFEND:
        if int(rule.skill_id) != 0:
            raise BattleScriptValidationError(f"{where}: DEFEND carries no skill_id")
        if side != _TARGET_SELF:
            raise BattleScriptValidationError(f"{where}: DEFEND targets SELF")
    elif action == _ACTION_SKILL:
        _check_skill_action(role, rule, tables, where)
    else:
        raise BattleScriptValidationError(f"{where}: action must be ATTACK, SKILL or DEFEND")


def _check_role_header(role: Message, tables: BattleScriptTables, where: str) -> None:
    if role.name and len(role.name.encode("utf-8")) > MAX_NAME_BYTES:
        raise BattleScriptValidationError(f"{where}: name too long")
    actor_type = int(role.actor_type)
    if actor_type not in _KNOWN_ACTOR_TYPES:
        raise BattleScriptValidationError(f"{where}: unknown actor_type")
    class_id = int(role.class_id)
    if class_id > MAX_CLASS_ID:
        raise BattleScriptValidationError(f"{where}: class_id out of range")
    if class_id != 0 and actor_type not in (_ACTOR_TYPE_NONE, _ACTOR_TYPE_PLAYER):
        raise BattleScriptValidationError(f"{where}: class_id only applies to players")
    monster_id = int(role.monster_id)
    if monster_id != 0:
        if actor_type not in (_ACTOR_TYPE_NONE, _ACTOR_TYPE_MONSTER):
            raise BattleScriptValidationError(f"{where}: monster_id only applies to monsters")
        if class_id != 0:
            raise BattleScriptValidationError(f"{where}: a role cannot require both a class and a monster")
        if not tables.has_monster(monster_id):
            raise BattleScriptValidationError(f"{where}: unknown monster {monster_id}")
    if int(role.max_members) > MAX_ROLE_MEMBERS:
        raise BattleScriptValidationError(f"{where}: max_members above {MAX_ROLE_MEMBERS}")
    if not 1 <= len(role.rules) <= MAX_RULES_PER_ROLE:
        raise BattleScriptValidationError(f"{where}: needs 1..{MAX_RULES_PER_ROLE} rules")


def normalize_battle_script(script: Message, tables: BattleScriptTables) -> Message:
    """校验剧本结构与引用并返回按 order / priority 排好序的新副本（不修改入参）。"""
    where = f"script {int(script.script_id)}"
    if int(script.schema_version) != SCHEMA_VERSION:
        raise BattleScriptValidationError(f"{where}: unsupported schema_version {int(script.schema_version)}")
    if (
        len(script.name.encode("utf-8")) > MAX_NAME_BYTES
        or len(script.description.encode("utf-8")) > MAX_DESCRIPTION_BYTES
    ):
        raise BattleScriptValidationError(f"{where}: name or description too long")
    if not 1 <= len(script.roles) <= MAX_ROLES:
        raise BattleScriptValidationError(f"{where}: needs 1..{MAX_ROLES} roles")

    role_ids: set[int] = set()
    role_orders: set[int] = set()
    for role in script.roles:
        rid = int(role.role_id)
        role_where = f"{where} role {rid}"
        if rid == 0 or rid in role_ids:
            raise BattleScriptValidationError(f"{role_where}: role_id must be non-zero and unique")
        role_ids.add(rid)
        order = int(role.order)
        if order in role_orders:
            raise BattleScriptValidationError(f"{role_where}: duplicate role order {order}")
        role_orders.add(order)
        _check_role_header(role, tables, role_where)

    for role in script.roles:
        role_where = f"{where} role {int(role.role_id)}"
        priorities: set[int] = set()
        for rule in role.rules:
            prio = int(rule.priority)
            rule_where = f"{role_where} rule {prio}"
            if prio in priorities:
                raise BattleScriptValidationError(f"{rule_where}: duplicate rule priority")
            priorities.add(prio)
            if len(rule.note.encode("utf-8")) > MAX_NOTE_BYTES:
                raise BattleScriptValidationError(f"{rule_where}: note too long")
            if len(rule.conditions) > MAX_CONDITIONS_PER_RULE:
                raise BattleScriptValidationError(f"{rule_where}: at most {MAX_CONDITIONS_PER_RULE} conditions")
            for cond in rule.conditions:
                _check_condition(cond, rule_where)
            _check_target(rule.target, role_ids, tables, rule_where)
            _check_action(role, rule, tables, rule_where)

    normalized = type(script)()
    normalized.CopyFrom(script)
    sorted_roles = sorted(normalized.roles, key=lambda r: int(r.order))
    del normalized.roles[:]
    for role in sorted_roles:
        sorted_rules = sorted(role.rules, key=lambda ru: int(ru.priority))
        del role.rules[:]
        role.rules.extend(sorted_rules)
        normalized.roles.add().CopyFrom(role)
    return normalized


def check_battle_script_side(script: Message, side: int, tables: BattleScriptTables) -> None:
    """校验已规范化剧本是否允许用于指定一方（玩家方禁怪物；怪物方只允许零耗蓝老技能）。"""
    where = f"script {int(script.script_id)}"
    if side not in (SCRIPT_SIDE_PLAYER, SCRIPT_SIDE_MONSTER):
        raise BattleScriptValidationError(f"{where}: side must be PLAYER or MONSTER")
    for role in script.roles:
        role_where = f"{where} role {int(role.role_id)}"
        actor_type = int(role.actor_type)
        if side == SCRIPT_SIDE_PLAYER:
            if actor_type == _ACTOR_TYPE_MONSTER or int(role.monster_id) != 0:
                raise BattleScriptValidationError(f"{role_where}: player side roles cannot accept monsters")
            continue
        if actor_type not in (_ACTOR_TYPE_NONE, _ACTOR_TYPE_MONSTER):
            raise BattleScriptValidationError(f"{role_where}: monster side roles can only accept monsters")
        if int(role.class_id) != 0:
            raise BattleScriptValidationError(f"{role_where}: monster side roles cannot require a class")
        for rule in role.rules:
            if int(rule.action) != _ACTION_SKILL:
                continue
            rule_where = f"{role_where} rule {int(rule.priority)}"
            info = tables.skill(int(rule.skill_id))
            if info is None or info.class_id != 0:
                raise BattleScriptValidationError(f"{rule_where}: monsters can only cast legacy skills")
            if info.legacy_mana_cost != 0:
                raise BattleScriptValidationError(
                    f"{rule_where}: monsters have no mana but skill {int(rule.skill_id)} costs mana"
                )


# -- 官方剧本目录 -------------------------------------------------------------------------------------


class BattleScriptCatalog:
    """只读官方剧本目录（整份配置校验通过后构造，支持并发读）。"""

    def __init__(
        self,
        scripts: dict[int, Message],
        bindings: dict[tuple[int, int, int], Message],
    ) -> None:
        self._scripts = dict(scripts)
        self._ids = tuple(sorted(scripts))
        self._bindings = dict(bindings)

    @classmethod
    def from_config(cls, config_msg: Message, tables: BattleScriptTables) -> "BattleScriptCatalog":
        if len(config_msg.scripts) > MAX_OFFICIAL_SCRIPTS or len(config_msg.bindings) > MAX_OFFICIAL_BINDINGS:
            raise BattleScriptValidationError(
                f"battle script config exceeds {MAX_OFFICIAL_SCRIPTS} scripts or {MAX_OFFICIAL_BINDINGS} bindings"
            )
        scripts: dict[int, Message] = {}
        for script in config_msg.scripts:
            sid = int(script.script_id)
            if sid == 0 or sid in scripts:
                raise BattleScriptValidationError(f"script {sid}: script_id must be non-zero and unique")
            scripts[sid] = normalize_battle_script(script, tables)

        bindings: dict[tuple[int, int, int], Message] = {}
        for binding in config_msg.bindings:
            mode = int(binding.match_mode)
            cfg_id = int(binding.battle_config_id)
            side = int(binding.side)
            sid = int(binding.script_id)
            where = f"binding (match_mode {mode}, battle_config_id {cfg_id}, side {side})"
            script = scripts.get(sid)
            if script is None:
                raise BattleScriptValidationError(f"{where}: unknown script {sid}")
            if not is_known_match_mode(mode):
                raise BattleScriptValidationError(f"{where}: unknown match_mode")
            if cfg_id != 0 and not tables.has_dungeon(cfg_id):
                raise BattleScriptValidationError(f"{where}: unknown dungeon")
            if side == SCRIPT_SIDE_MONSTER and mode != 0 and not is_pve_match_mode(mode):
                raise BattleScriptValidationError(f"{where}: the monster side only exists in PVE modes")
            check_battle_script_side(script, side, tables)
            key = (mode, cfg_id, side)
            if key in bindings:
                raise BattleScriptValidationError(f"{where}: duplicate binding")
            bindings[key] = script
        return cls(scripts, bindings)

    @classmethod
    def load_default(cls, protocol: Protocol, tables: BattleScriptTables) -> "BattleScriptCatalog":
        cfg_msg = protocol.message("BattleScriptConfig")
        json_format.Parse(json.dumps(DEFAULT_OFFICIAL_SCRIPTS_CONFIG), cfg_msg)
        return cls.from_config(cfg_msg, tables)

    def official(self, script_id: int) -> Message | None:
        return self._scripts.get(int(script_id))

    def official_scripts(self) -> tuple[Message, ...]:
        return tuple(self._scripts[sid] for sid in self._ids)

    def bound(self, match_mode: int, battle_config_id: int, side: int) -> Message | None:
        for mode, cfg_id in (
            (match_mode, battle_config_id),
            (match_mode, 0),
            (0, battle_config_id),
            (0, 0),
        ):
            found = self._bindings.get((mode, cfg_id, side))
            if found is not None:
                return found
        return None


# -- 战斗剧本业务服务（客户端 CRUD + 运营审核）---------------------------------------------------------


class BattleScriptManager:
    """承载战斗剧本的客户端 CRUD 与运营审核逻辑，保持与 MatchGrpcService 职责解耦。"""

    def __init__(
        self,
        protocol: Protocol,
        store,
        *,
        tables: BattleScriptTables | None = None,
        catalog: BattleScriptCatalog | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.tables = tables if tables is not None else ConfigTableScriptLookup()
        self.catalog = catalog if catalog is not None else BattleScriptCatalog.load_default(protocol, self.tables)

    def _script_view_of(self, record: Message) -> Message:
        content = self.protocol.parse("BattleScript", bytes(record.content))
        view = self.protocol.message(
            "match.PlayerBattleScriptView",
            script_id=int(record.script_id),
            revision=int(record.revision),
            review_state=int(record.review_state),
            approved_revision=int(record.approved_revision),
            reject_reason=record.reject_reason,
            submitted_ms=int(record.submitted_ms),
        )
        view.content.CopyFrom(content)
        return view

    def _fill_loadout(self, target_repeated, records: list[Message]) -> None:
        for rec in records:
            target_repeated.add(
                match_mode=int(rec.match_mode),
                battle_config_id=int(rec.battle_config_id),
                origin=int(rec.origin),
                official_script_id=int(rec.official_script_id),
                player_script_id=int(rec.player_script_id),
            )

    def _validate_approved_bytes(self, content: bytes) -> None:
        try:
            script = self.protocol.parse("BattleScript", content)
        except DecodeError as exc:
            raise BattleScriptValidationError("battle script content undecodable") from exc
        normalized = normalize_battle_script(script, self.tables)
        check_battle_script_side(normalized, SCRIPT_SIDE_PLAYER, self.tables)

    # -- 客户端 RPC 实现 ------------------------------------------------------------------------------

    async def list_battle_scripts(self, caller: CallerIdentity) -> Message:
        resp = self.protocol.message(
            "match.ListBattleScriptsResponse",
            max_scripts=MAX_BATTLE_SCRIPTS_PER_PLAYER,
            max_script_bytes=MAX_PLAYER_BATTLE_SCRIPT_BYTES,
        )
        try:
            records = await self.store.list_battle_scripts(caller.player_id)
            loadout = await self.store.list_battle_script_loadout(caller.player_id)
            for official in self.catalog.official_scripts():
                resp.official_scripts.add().CopyFrom(official)
            for rec in records:
                resp.my_scripts.add().CopyFrom(self._script_view_of(rec))
            self._fill_loadout(resp.loadout, loadout)
            return resp
        except Exception:
            logger.error("battle_script ListBattleScripts failed player=%d", caller.player_id, exc_info=True)
            return set_tip(self.protocol.message("match.ListBattleScriptsResponse"), ERR_MATCH_INTERNAL)

    async def submit_battle_script(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.SubmitBattleScriptResponse")
        if not request.HasField("script"):
            resp.invalid_detail = "script is missing"
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_INVALID)
        script_id = int(request.script_id)
        expected_rev = int(request.expected_revision)
        if script_id == 0 and expected_rev != 0:
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT)

        candidate = self.protocol.message("BattleScript")
        candidate.CopyFrom(request.script)
        candidate.script_id = 0
        if candidate.ByteSize() > MAX_PLAYER_BATTLE_SCRIPT_BYTES:
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_TOO_LARGE)
        try:
            normalized = normalize_battle_script(candidate, self.tables)
            check_battle_script_side(normalized, SCRIPT_SIDE_PLAYER, self.tables)
        except BattleScriptValidationError as exc:
            resp.invalid_detail = str(exc)
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_INVALID)

        content = normalized.SerializeToString(deterministic=True)
        try:
            record = await self.store.submit_battle_script(
                caller,
                script_id=script_id,
                expected_revision=expected_rev,
                content=content,
                max_scripts=MAX_BATTLE_SCRIPTS_PER_PLAYER,
            )
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)
        except Exception:
            logger.error(
                "battle_script SubmitBattleScript failed player=%d script_id=%d",
                caller.player_id,
                script_id,
                exc_info=True,
            )
            return set_tip(resp, ERR_MATCH_INTERNAL)

        resp.script.CopyFrom(self._script_view_of(record))
        logger.info(
            "battle_script submitted player=%d script_id=%d revision=%d bytes=%d",
            caller.player_id,
            int(record.script_id),
            int(record.revision),
            len(content),
        )
        return resp

    async def delete_battle_script(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.DeleteBattleScriptResponse")
        script_id = int(request.script_id)
        if script_id == 0:
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND)
        try:
            await self.store.delete_battle_script(caller, script_id, int(request.expected_revision))
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)
        except Exception:
            logger.error(
                "battle_script DeleteBattleScript failed player=%d script_id=%d",
                caller.player_id,
                script_id,
                exc_info=True,
            )
            return set_tip(resp, ERR_MATCH_INTERNAL)
        logger.info("battle_script deleted player=%d script_id=%d", caller.player_id, script_id)
        return resp

    async def set_battle_script_loadout(self, caller: CallerIdentity, request: Message) -> Message:
        resp = self.protocol.message("match.SetBattleScriptLoadoutResponse")
        if not request.HasField("entry"):
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID)
        entry = request.entry
        mode = int(entry.match_mode)
        cfg_id = int(entry.battle_config_id)
        origin = int(entry.origin)
        official_id = 0
        player_script_id = 0

        if not is_known_match_mode(mode) or (cfg_id != 0 and not self.tables.has_dungeon(cfg_id)):
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID)

        if origin == SCRIPT_ORIGIN_UNSPECIFIED:
            pass
        elif origin == SCRIPT_ORIGIN_OFFICIAL:
            official_id = int(entry.official_script_id)
            script = self.catalog.official(official_id)
            if script is None:
                return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID)
            try:
                check_battle_script_side(script, SCRIPT_SIDE_PLAYER, self.tables)
            except BattleScriptValidationError:
                return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID)
        elif origin == SCRIPT_ORIGIN_PLAYER:
            player_script_id = int(entry.player_script_id)
            if player_script_id == 0:
                return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND)
            # 评分对局只能选用官方剧本；任意模式（0）允许保存，建房时在评分局跳过
            if mode != 0 and mode in _RATED_MODES:
                return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID)
        else:
            return set_tip(resp, ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID)

        try:
            loadout = await self.store.set_battle_script_loadout(
                caller,
                match_mode=mode,
                battle_config_id=cfg_id,
                origin=origin,
                official_script_id=official_id,
                player_script_id=player_script_id,
                max_entries=MAX_BATTLE_SCRIPT_LOADOUT_ENTRIES,
            )
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)
        except Exception:
            logger.error(
                "battle_script SetBattleScriptLoadout failed player=%d mode=%d config=%d",
                caller.player_id,
                mode,
                cfg_id,
                exc_info=True,
            )
            return set_tip(resp, ERR_MATCH_INTERNAL)

        self._fill_loadout(resp.loadout, loadout)
        return resp

    # -- 内部运营审核 RPC 实现 ------------------------------------------------------------------------

    async def list_pending_battle_scripts(self, request: Message) -> Message:
        resp = self.protocol.message("match.ListPendingBattleScriptsResponse")
        limit = int(request.limit)
        if not 1 <= limit <= MAX_PENDING_BATTLE_SCRIPTS_PAGE:
            resp.reject = REVIEW_REJECT_INVALID
            return resp
        try:
            records = await self.store.list_pending_battle_scripts(limit)
            for rec in records:
                content = self.protocol.parse("BattleScript", bytes(rec.content))
                item = resp.scripts.add(
                    script_id=int(rec.script_id),
                    owner_player_id=int(rec.owner_player_id),
                    revision=int(rec.revision),
                    submitted_ms=int(rec.submitted_ms),
                    approved_revision=int(rec.approved_revision),
                )
                item.content.CopyFrom(content)
            return resp
        except Exception:
            logger.error("battle_script ListPendingBattleScripts failed", exc_info=True)
            resp = self.protocol.message("match.ListPendingBattleScriptsResponse", reject=REVIEW_REJECT_INTERNAL)
            return resp

    async def review_battle_script(self, request: Message) -> Message:
        resp = self.protocol.message("match.ReviewBattleScriptResponse")
        script_id = int(request.script_id)
        revision = int(request.revision)
        approve = bool(request.approve)
        reviewer = request.reviewer
        reason = request.reason
        if (
            script_id == 0
            or revision == 0
            or not valid_operator_text(reviewer, MAX_REVIEWER_BYTES, required=True)
            or not valid_operator_text(reason, MAX_REVIEW_REASON_BYTES, required=not approve)
        ):
            resp.reject = REVIEW_REJECT_INVALID
            return resp

        try:
            await self.store.review_battle_script(
                script_id=script_id,
                revision=revision,
                approve=approve,
                reviewer=reviewer,
                reason=reason,
                validate=self._validate_approved_bytes if approve else None,
            )
        except BattleScriptReviewError as exc:
            resp.reject = exc.reject
            return resp
        except Exception:
            logger.error(
                "battle_script ReviewBattleScript failed script_id=%d revision=%d",
                script_id,
                revision,
                exc_info=True,
            )
            resp.reject = REVIEW_REJECT_INTERNAL
            return resp

        logger.info(
            "battle_script reviewed script_id=%d revision=%d approved=%s reviewer=%r",
            script_id,
            revision,
            approve,
            reviewer,
        )
        return resp
