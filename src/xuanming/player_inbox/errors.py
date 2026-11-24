"""player_inbox 服务的 Tip 错误码常量与内部异常分层。"""

from __future__ import annotations

# Tip.xlsx 码段：CommonError 与 player_inbox_error (base=29000)，与 Go constants.go 对齐。
ERR_INVALID_PARAMETER = 1005

ERR_DISABLED = 29000
ERR_UNKNOWN_TYPE = 29001
ERR_PLAYER_NOT_FOUND = 29002
ERR_PAYLOAD_INVALID = 29003
ERR_PAYLOAD_TOO_LARGE = 29004
ERR_TEXT_TOO_LONG = 29005
ERR_ISSUED_OUT_OF_WINDOW = 29006
ERR_FULL = 29007
ERR_IDEMPOTENCY_CONFLICT = 29008
ERR_STALE_OWNER = 29009
ERR_EPOCH_REQUIRED = 29010
ERR_EPOCH_AHEAD = 29011
ERR_ITEM_NOT_FOUND = 29012
ERR_ITEM_STATE_CONFLICT = 29013
ERR_STORE_UNAVAILABLE = 29014


class InboxError(Exception):
    """player_inbox 内部基础异常。"""


class OriginalRoleError(InboxError):
    """原角色身份缺失、已换主、冻结或入场登记失效（非故障，映射为 ERR_STALE_OWNER）。"""


class RoleNotFoundError(InboxError):
    """全局目录中不存在该角色。"""


class RoleFenceUnavailableError(InboxError):
    """角色护栏权威状态或入场校验暂不可达（事务未提交，非故障，映射为 ERR_STALE_OWNER）。"""


class RoleFenceStaleError(InboxError):
    """本库角色护栏副本缺行、修订号落后或处于冻结态，需先回滚事务并向目录自愈后重试一次。"""

    def __init__(self, players: tuple[int, ...], message: str = "role fence replica stale") -> None:
        super().__init__(message)
        self.players = players


class BattleSettlementOriginalError(InboxError):
    """Battle 永久原件的确定性冲突（对应 gRPC FAILED_PRECONDITION）。"""


class StoreUnavailableError(InboxError):
    """收件箱库或外部依赖故障（对应 ERR_STORE_UNAVAILABLE 或 Battle 的 UNAVAILABLE）。"""


class InvalidRoleFenceStateError(InboxError):
    """RoleFenceState 字段非法（对应 gRPC INVALID_ARGUMENT）。"""
