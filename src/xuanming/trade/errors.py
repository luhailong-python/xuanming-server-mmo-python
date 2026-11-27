"""trade 服务错误码、领域异常与带内 Tip 映射（与 Go 侧 constants / commerce 完全对齐）。"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 通用 / 交易 / 资产错误码（源自 Tip 表）
ERR_OK = 0
ERR_SERVICE_UNAVAILABLE = 1003
ERR_INVALID_PARAMETER = 1005
ERR_LOGIN_ACCOUNT_PLAYER_FULL = 2001

ERR_TRADE_LISTING_NOT_FOUND = 20000
ERR_TRADE_HOME_ZONE_UNKNOWN = 20001
ERR_TRADE_FAVORITE_LIMIT_REACHED = 20002
ERR_TRADE_FEATURE_DISABLED = 20003
ERR_TRADE_ORDER_NOT_FOUND = 20004
ERR_TRADE_ORDER_STATE_CONFLICT = 20005
ERR_TRADE_REQUEST_CONFLICT = 20006
ERR_TRADE_ORDER_LIMIT = 20007
ERR_TRADE_CANNOT_TRADE_SELF = 20008
ERR_TRADE_COUNTERPARTY_UNKNOWN = 20009
ERR_TRADE_INSUFFICIENT_ASSET = 20010
ERR_TRADE_BID_TOO_LOW = 20011
ERR_TRADE_AUCTION_ENDED = 20012
ERR_TRADE_ASSET_DELIVERY_PENDING = 20013
ERR_TRADE_MANUAL_REVIEW = 20014
ERR_TRADE_ASSET_NOT_TRADABLE = 20015
ERR_TRADE_TITLE_SENSITIVE = 20016
ERR_TRADE_TEXT_CHECK_UNAVAILABLE = 20017
ERR_TRADE_WANTED_ORDER_LIMIT_REACHED = 20018
ERR_TRADE_WANTED_BOOK_MISMATCH = 20019

ERR_ASSET_PARTIAL_APPLIED = 27007


class TradeError(Exception):
    """trade 领域基础异常。"""


class TradeRejection(TradeError):
    """客户端可见的确定性业务拒绝（直接映射为带内 Tip 码）。"""

    def __init__(self, tip_id: int, message: str = "trade request rejected") -> None:
        super().__init__(message)
        self.tip_id = int(tip_id)


class TradeCorruptContractError(TradeError):
    """合同自身持久数据损坏（确定性失败，Tick 隔离为 Manual）。"""


class TradeRoleConflictError(TradeRejection):
    """参与角色已换主、已删除或被冻结（长期状态，客户端回 20005）。"""

    def __init__(self, message: str = "trade role conflict") -> None:
        super().__init__(ERR_TRADE_ORDER_STATE_CONFLICT, message)


class TradeStaleObservationError(TradeRejection):
    """锁内复核发现预读观察已过期（乐观并发冲突，下一轮立即重试）。"""

    def __init__(self, message: str = "trade observation stale") -> None:
        super().__init__(ERR_TRADE_ORDER_STATE_CONFLICT, message)


class ListingNotFoundError(TradeError):
    """P1 聚宝斋商品不存在。"""


class StoreUnavailableError(TradeError):
    """独占库或下游依赖暂不可用。"""


class TradeTooManyPendingError(TradeError):
    """玩家资产通道未决操作数或序号跨度达到上限。"""


class RoleFenceUnavailableError(TradeError):
    """角色护栏权威状态暂不可读（事务已回滚，无结局未知）。"""


class RoleNotFoundError(TradeError):
    """账号目录中不存在该角色。"""


class RoleFenceStaleError(TradeError):
    """本库角色护栏副本缺行、落后或冻结，需要回滚后自愈重试。"""

    def __init__(
        self,
        player_ids: int | tuple[int, ...],
        *,
        reject: Exception | None = None,
    ) -> None:
        if isinstance(player_ids, int):
            ids = (player_ids,)
        else:
            ids = tuple(sorted({int(p) for p in player_ids if int(p) != 0}))
        super().__init__(f"role fence stale players={ids}")
        self.player_ids = ids
        self.reject = reject


class InvalidRoleFenceStateError(TradeError):
    """协调者传入的角色护栏状态字段不合法。"""


class AssetOpLeaseLostError(TradeError):
    """资产指令重排时租约已被其他副本接管。"""


class AssetOpPoisonRowError(TradeError):
    """资产指令载荷无法反序列化，已推迟等待人工介入。"""


class AssetOpOutcomeFlipError(TradeError):
    """同一 seq 两次查询给出的终结结局不一致（违反不变量 I2）。"""


class ManualNotFoundError(TradeError):
    """人工处置目标合同不存在。"""


class ManualNotAllowedError(TradeError):
    """人工处置前置条件不满足。"""


class ManualVersionChangedError(TradeError):
    """人工处置时合同（或关联出价）版本已变化。"""


class RoleDirectoryUnavailableError(TradeError):
    """全局账号角色目录暂不可用（结局未知）。"""


class RoleDirectoryRetryLaterError(RoleDirectoryUnavailableError):
    """全局账号角色目录返回可重试（结局已知，同参数重放）。"""


class RoleDirectoryMissingError(TradeError):
    """账号或角色目录项不存在。"""


class RoleDirectoryConflictError(TradeError):
    """账号角色归属或交易状态冲突。"""


class RoleDirectoryFullError(TradeError):
    """目标账号存活角色数已达全局上限。"""


class RoleDirectoryInvalidError(TradeError):
    """账号角色目录请求参数非法。"""


class RolePreparingError(TradeError):
    """买家角色仍在冻结投票中，下一轮再判。"""


class TextCheckUnavailableError(TradeError):
    """内容安全服务暂不可用（fail-closed 拒绝上架）。"""


def commerce_tip_id(error: BaseException | None) -> int:
    """将交易/护栏领域异常映射为客户端带内 Tip 错误码（与 Go logic.commerceCode 对齐）。"""
    if error is None:
        return ERR_OK
    if isinstance(error, TradeRejection):
        return error.tip_id
    if isinstance(error, RoleNotFoundError):
        return ERR_TRADE_COUNTERPARTY_UNKNOWN
    if isinstance(error, (RoleFenceUnavailableError, RoleDirectoryConflictError, RoleDirectoryRetryLaterError)):
        logger.info("trade role fence or directory state conflict err=%s", error)
        return ERR_TRADE_ORDER_STATE_CONFLICT
    if isinstance(error, RoleDirectoryFullError):
        return ERR_LOGIN_ACCOUNT_PLAYER_FULL
    if isinstance(error, RoleDirectoryInvalidError):
        return ERR_INVALID_PARAMETER
    if isinstance(error, RoleDirectoryMissingError):
        return ERR_TRADE_ORDER_NOT_FOUND
    if isinstance(error, ListingNotFoundError):
        return ERR_TRADE_LISTING_NOT_FOUND
    logger.error("trade authoritative request failed err=%s", error)
    return ERR_SERVICE_UNAVAILABLE
