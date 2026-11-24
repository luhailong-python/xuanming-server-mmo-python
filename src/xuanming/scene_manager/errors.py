"""scene_manager 业务错误码、场景类型常量、Kafka 事件号与内部异常定义。"""

from __future__ import annotations

from enum import IntEnum

# ── 业务错误码（与 Go scene_manager/internal/constants/errors.go 逐值对齐） ───────

ERR_OK = 0
ERR_NO_AVAILABLE_NODE = 1
ERR_SCENE_LOOKUP_FAILED = 2
ERR_UPDATE_LOCATION = 3
ERR_INVALID_NODE_ID = 4
ERR_INVALID_GATE_ID = 5
ERR_ENCODE_EVENT = 6
ERR_KAFKA_ROUTE = 7
ERR_REDIS = 8
ERR_DUPLICATE_SCENE = 9
ERR_INVALID_SCENE_TYPE = 10
ERR_NO_SCENE_CONF_ID = 11
ERR_NO_NODE_FOR_PURPOSE = 12
ERR_SOURCE_SCENE_GONE = 13
ERR_UNSAFE_CROSS_NODE_HANDOFF = 14
ERR_ENTER_SCENE_IN_PROGRESS = 15
ERR_ENTER_SCENE_IDEMPOTENCY_CONFLICT = 16
ERR_SCENE_REENTRY_BARRIER = 17
ERR_HANDOFF_PENDING = 18
ERR_OWNER_EPOCH_CONFLICT = 19
ERR_HOME_ZONE_UNAVAILABLE = 20
# 21 已退役（曾是 ErrHomeZoneMerging），永不复用
ERR_CHANNEL_UNAVAILABLE = 22
ERR_CHANNEL_FULL = 23
ERR_CHANNEL_SWITCH_COOLDOWN = 24
ERR_PLACEMENT_SESSION_SUPERSEDED = 25

# ── 场景类型与节点用途（与 proto/scene_manager & proto/common/base/node.proto 对齐） ──

SCENE_TYPE_UNSPECIFIED = 0
SCENE_TYPE_MAIN_WORLD = 1
SCENE_TYPE_INSTANCE = 2

SCENE_NODE_TYPE_MAIN_WORLD = 0        # kMainSceneNode
SCENE_NODE_TYPE_INSTANCE = 1          # kSceneNode
SCENE_NODE_TYPE_MAIN_WORLD_CROSS = 2  # kMainSceneCrossNode
SCENE_NODE_TYPE_INSTANCE_CROSS = 3    # kSceneSceneCrossNode


class NodePurpose(IntEnum):
    """节点承载用途：区分大世界频道池与按需副本池。"""

    WORLD = 1
    INSTANCE = 2


def is_world_hosting_type(scene_node_type: int) -> bool:
    return scene_node_type in (SCENE_NODE_TYPE_MAIN_WORLD, SCENE_NODE_TYPE_MAIN_WORLD_CROSS)


def is_instance_hosting_type(scene_node_type: int) -> bool:
    return scene_node_type in (SCENE_NODE_TYPE_INSTANCE, SCENE_NODE_TYPE_INSTANCE_CROSS)


def matches_purpose(scene_node_type: int, purpose: NodePurpose) -> bool:
    if purpose == NodePurpose.WORLD:
        return is_world_hosting_type(scene_node_type)
    if purpose == NodePurpose.INSTANCE:
        return is_instance_hosting_type(scene_node_type)
    return False


# ── Kafka 命令事件号（与 Go generated/pb/game/event_id.go 逐值对齐） ──────────────

EVENT_ID_ROUTE_PLAYER = 11
EVENT_ID_REDIRECT_TO_GATE = 37
EVENT_ID_PLAYER_HOSTED_PLACEMENT = 53


# ── 内部控制流异常 ─────────────────────────────────────────────────────────────

class SceneManagerError(RuntimeError):
    """scene_manager 内部基础异常。"""


class OwnerEpochConflictError(SceneManagerError):
    """落点 CAS 失败：owner_epoch 或 location 原文/修订已被并发修改。"""


class HandoffWithdrawnError(SceneManagerError):
    """预检时的 handoff 标记在落点提交时已被源节点撤回或替换。"""


class ReentryBarrierPendingError(SceneManagerError):
    """死节点再入屏障尚未走完，需调用方退避重试。"""


class LocationCorruptError(SceneManagerError):
    """player:{<pid>}:location 原始 protobuf 字节损坏。"""


class LocationRevisionCorruptError(SceneManagerError):
    """player:{<pid>}:location_revision 侧车标签损坏或与正文不匹配。"""


class WorldChannelNotDestroyableError(SceneManagerError):
    """DestroyScene 拒绝销毁大世界频道。"""


class HolderReleasePendingError(SceneManagerError):
    """LeaveScene 无法证明持有者已释放，须返回 FAILED_PRECONDITION 保留位置。"""


class RoleDirectoryUnavailableError(SceneManagerError):
    """DataService.LookupAccountRole 不可用或强读失败。"""


class RoleOwnershipInvalidError(SceneManagerError):
    """角色当前归属无效、修订不匹配或处于寄售交易状态。"""


class CrossDomainRedisKeyError(ValueError):
    """企图将控制域键与玩家域键混在同一段 Redis Lua 脚本中执行。"""
