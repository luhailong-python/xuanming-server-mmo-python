"""guild 服务错误码、业务异常与 Tip 映射（与 Go table.GuildError / CommonError 完全一致）。"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 通用错误码（CommonError）
ERR_OK = 0
ERR_SERVICE_UNAVAILABLE = 1003
ERR_INVALID_PARAMETER = 1005
ERR_FEATURE_UNAVAILABLE = 1006
ERR_RATE_LIMIT_EXCEEDED = 1008

# 帮会段错误码（GuildError，base=14000，与 Tip.xlsx 及 Go constants.go 逐项一致）
ERR_GUILD_ALREADY_IN_GUILD = 14000
ERR_GUILD_NOT_FOUND = 14001
ERR_GUILD_NOT_IN_GUILD = 14002
ERR_GUILD_FULL = 14003
ERR_GUILD_LEADER_CANT_LEAVE = 14004
ERR_GUILD_NOT_LEADER = 14005
ERR_GUILD_NO_PERMISSION = 14006
ERR_GUILD_NOT_RANKED = 14007
ERR_GUILD_ID_GEN_UNAVAILABLE = 14008
ERR_GUILD_NAME_INVALID = 14009
ERR_GUILD_NAME_TAKEN = 14010
ERR_GUILD_ANNOUNCEMENT_TOO_LONG = 14011
ERR_GUILD_HOME_ZONE_UNKNOWN = 14012
ERR_GUILD_ZONE_MERGING = 14013
ERR_GUILD_TARGET_NOT_MEMBER = 14014
ERR_GUILD_CANNOT_TARGET_SELF = 14015
ERR_GUILD_RANK_TOO_LOW = 14016
ERR_GUILD_OFFICER_LIMIT = 14017
ERR_GUILD_APPLICATION_NOT_FOUND = 14018
ERR_GUILD_APPLICATION_LIMIT = 14019
ERR_GUILD_APPLICATION_QUEUE_FULL = 14020
ERR_GUILD_BUSY_RETRY = 14021
ERR_GUILD_FUNDS_INSUFFICIENT = 14022
ERR_GUILD_MAX_LEVEL = 14023
ERR_GUILD_DONATE_LIMIT = 14024
ERR_GUILD_CURRENCY_INSUFFICIENT = 14025
ERR_GUILD_ASSET_PENDING = 14026
ERR_GUILD_ASSET_REJECTED = 14027
ERR_GUILD_SHOP_GOODS_NOT_FOUND = 14028
ERR_GUILD_SHOP_LEVEL_TOO_LOW = 14029
ERR_GUILD_SHOP_LIMIT = 14030
ERR_GUILD_CONTRIBUTION_INSUFFICIENT = 14031
ERR_GUILD_ACTIVITY_NOT_OPEN = 14032
ERR_GUILD_ACTIVITY_ALREADY_CLAIMED = 14033
ERR_GUILD_ACTIVITY_THRESHOLD_NOT_REACHED = 14034
ERR_GUILD_TRIAL_TEAM_INVALID = 14035
ERR_GUILD_ACTIVITY_LEVEL_TOO_LOW = 14036
ERR_GUILD_ACTIVITY_JOIN_TOO_RECENT = 14037
ERR_GUILD_TRIAL_INVITE_EXPIRED = 14038
ERR_GUILD_TRIAL_INVITE_DECLINED = 14039
ERR_GUILD_TRIAL_INVITE_COOLDOWN = 14040
ERR_GUILD_TRIAL_SERVICE_BUSY = 14041
ERR_GUILD_TRIAL_BATTLE_ABORTED = 14042
ERR_GUILD_NAME_SENSITIVE = 14043
ERR_GUILD_ANNOUNCEMENT_SENSITIVE = 14044
ERR_GUILD_TEXT_CHECK_UNAVAILABLE = 14045
ERR_GUILD_RENAME_COOLDOWN = 14046
ERR_GUILD_RENAME_SAME_NAME = 14047

# 资产错误码（AssetError）
ERR_ASSET_CURRENCY_INSUFFICIENT = 27001


class GuildError(Exception):
    """guild 领域基础异常。"""

    tip_id: int = ERR_SERVICE_UNAVAILABLE
    parameters: tuple[str, ...] = ()

    def __init__(self, message: str = "", *, tip_id: int | None = None, parameters: tuple[str, ...] = ()) -> None:
        super().__init__(message or self.__class__.__name__)
        if tip_id is not None:
            self.tip_id = tip_id
        self.parameters = tuple(parameters)


class GuildAlreadyInGuildError(GuildError):
    tip_id = ERR_GUILD_ALREADY_IN_GUILD


class GuildNotFoundError(GuildError):
    tip_id = ERR_GUILD_NOT_FOUND


class GuildZoneMismatchError(GuildError):
    """跨区帮会（对客户端统一表现为 kGuildNotFound，审批通过时转为 kGuildApplicationNotFound）。"""

    tip_id = ERR_GUILD_NOT_FOUND


class GuildNotInGuildError(GuildError):
    tip_id = ERR_GUILD_NOT_IN_GUILD


class GuildFullError(GuildError):
    tip_id = ERR_GUILD_FULL


class GuildLeaderCantLeaveError(GuildError):
    tip_id = ERR_GUILD_LEADER_CANT_LEAVE


class GuildNotLeaderError(GuildError):
    tip_id = ERR_GUILD_NOT_LEADER


class GuildNoPermissionError(GuildError):
    tip_id = ERR_GUILD_NO_PERMISSION


class GuildNotRankedError(GuildError):
    tip_id = ERR_GUILD_NOT_RANKED


class GuildIdGenUnavailableError(GuildError):
    tip_id = ERR_GUILD_ID_GEN_UNAVAILABLE


class GuildNameInvalidError(GuildError):
    tip_id = ERR_GUILD_NAME_INVALID


class GuildNameTakenError(GuildError):
    tip_id = ERR_GUILD_NAME_TAKEN


class GuildAnnouncementTooLongError(GuildError):
    tip_id = ERR_GUILD_ANNOUNCEMENT_TOO_LONG


class GuildNameSensitiveError(GuildError):
    tip_id = ERR_GUILD_NAME_SENSITIVE


class GuildAnnouncementSensitiveError(GuildError):
    tip_id = ERR_GUILD_ANNOUNCEMENT_SENSITIVE


class GuildTextCheckUnavailableError(GuildError):
    tip_id = ERR_GUILD_TEXT_CHECK_UNAVAILABLE


class GuildRenameCooldownError(GuildError):
    tip_id = ERR_GUILD_RENAME_COOLDOWN

    def __init__(self, next_rename_ms: int) -> None:
        super().__init__(f"guild rename cooldown until {next_rename_ms}")
        self.next_rename_ms = next_rename_ms


class GuildRenameSameNameError(GuildError):
    tip_id = ERR_GUILD_RENAME_SAME_NAME


class GuildHomeZoneUnknownError(GuildError):
    tip_id = ERR_GUILD_HOME_ZONE_UNKNOWN


class GuildTargetNotMemberError(GuildError):
    tip_id = ERR_GUILD_TARGET_NOT_MEMBER


class GuildCannotTargetSelfError(GuildError):
    tip_id = ERR_GUILD_CANNOT_TARGET_SELF


class GuildRankTooLowError(GuildError):
    tip_id = ERR_GUILD_RANK_TOO_LOW


class GuildOfficerLimitError(GuildError):
    tip_id = ERR_GUILD_OFFICER_LIMIT


class GuildApplicationNotFoundError(GuildError):
    tip_id = ERR_GUILD_APPLICATION_NOT_FOUND


class GuildApplicationLimitError(GuildError):
    tip_id = ERR_GUILD_APPLICATION_LIMIT


class GuildApplicationQueueFullError(GuildError):
    tip_id = ERR_GUILD_APPLICATION_QUEUE_FULL


class GuildWriteConflictError(GuildError):
    """死锁重试耗尽、锁等待超时、护栏冻结或提交结果不明，统一映射为 kGuildBusyRetry。"""

    tip_id = ERR_GUILD_BUSY_RETRY


class GuildFundsInsufficientError(GuildError):
    tip_id = ERR_GUILD_FUNDS_INSUFFICIENT


class GuildMaxLevelError(GuildError):
    tip_id = ERR_GUILD_MAX_LEVEL


class GuildDonateLimitError(GuildError):
    tip_id = ERR_GUILD_DONATE_LIMIT


class GuildCurrencyInsufficientError(GuildError):
    tip_id = ERR_GUILD_CURRENCY_INSUFFICIENT


class GuildAssetPendingError(GuildError):
    tip_id = ERR_GUILD_ASSET_PENDING


class GuildAssetRejectedError(GuildError):
    tip_id = ERR_GUILD_ASSET_REJECTED


class GuildShopGoodsNotFoundError(GuildError):
    tip_id = ERR_GUILD_SHOP_GOODS_NOT_FOUND


class GuildShopLevelTooLowError(GuildError):
    tip_id = ERR_GUILD_SHOP_LEVEL_TOO_LOW


class GuildShopLimitError(GuildError):
    tip_id = ERR_GUILD_SHOP_LIMIT


class GuildContributionInsufficientError(GuildError):
    tip_id = ERR_GUILD_CONTRIBUTION_INSUFFICIENT


class GuildActivityNotOpenError(GuildError):
    tip_id = ERR_GUILD_ACTIVITY_NOT_OPEN


class GuildActivityAlreadyClaimedError(GuildError):
    tip_id = ERR_GUILD_ACTIVITY_ALREADY_CLAIMED


class GuildActivityThresholdNotReachedError(GuildError):
    tip_id = ERR_GUILD_ACTIVITY_THRESHOLD_NOT_REACHED

    def __init__(self, online: int, threshold: int) -> None:
        super().__init__(
            f"reunion online {online} < threshold {threshold}",
            parameters=(str(online), str(threshold)),
        )


class GuildActivityLevelTooLowError(GuildError):
    tip_id = ERR_GUILD_ACTIVITY_LEVEL_TOO_LOW

    def __init__(self, min_level: int) -> None:
        super().__init__(
            f"guild level below activity requirement {min_level}",
            parameters=(str(min_level),),
        )


class GuildActivityJoinTooRecentError(GuildError):
    tip_id = ERR_GUILD_ACTIVITY_JOIN_TOO_RECENT

    def __init__(self, min_hours: int) -> None:
        super().__init__(
            f"joined guild less than {min_hours} hours ago",
            parameters=(str(min_hours),),
        )


class GuildRateLimitError(GuildError):
    tip_id = ERR_RATE_LIMIT_EXCEEDED


class GuildInvalidParameterError(GuildError):
    tip_id = ERR_INVALID_PARAMETER


class GuildClientRequestReplayedError(GuildError):
    """事务内复核发现同一 (player_id, client_request_id) 已被并发提交。"""

    tip_id = ERR_GUILD_BUSY_RETRY

    def __init__(self, op_id: int) -> None:
        super().__init__(f"client request replayed op_id={op_id}")
        self.op_id = op_id


class GuildConfigMissingError(GuildError):
    tip_id = ERR_SERVICE_UNAVAILABLE


class GuildLeaderMismatchError(GuildError):
    tip_id = ERR_SERVICE_UNAVAILABLE


class GuildActivityOverflowError(GuildError):
    tip_id = ERR_SERVICE_UNAVAILABLE


class StoreUnavailableError(GuildError):
    tip_id = ERR_SERVICE_UNAVAILABLE


class RoleFenceUnavailableError(GuildError):
    tip_id = ERR_GUILD_BUSY_RETRY


class RoleNotFoundError(GuildError):
    tip_id = ERR_GUILD_BUSY_RETRY


class RoleFenceStaleError(GuildError):
    """本库角色护栏副本缺行、落后或冻结，需要回滚后自愈重试。"""

    tip_id = ERR_GUILD_BUSY_RETRY

    def __init__(self, player_id: int) -> None:
        super().__init__(f"role fence stale player={player_id}")
        self.player_id = player_id


class InvalidRoleFenceStateError(GuildError):
    """协调者传入的角色护栏状态字段不合法。"""

    tip_id = ERR_INVALID_PARAMETER


def tip_for_error(protocol, error: BaseException | None):
    """将领域异常转换为带内 TipInfoMessage（GuildService 永不将业务/写冲突异常转为 gRPC 错误）。"""
    if error is None:
        return None
    if isinstance(error, GuildError):
        params = list(error.parameters) if error.parameters else [str(error)]
        return protocol.message("TipInfoMessage", id=error.tip_id, parameters=params)
    logger.error("guild unexpected internal error err=%s", error, exc_info=True)
    return protocol.message("TipInfoMessage", id=ERR_SERVICE_UNAVAILABLE, parameters=["guild service unavailable"])
