"""mail 服务错误码、业务异常与 Tip 映射（与 Go 侧 table.* 常量完全一致）。"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 通用 / 邮件 / 帮会 / 资产错误码（源自 Tip 表）
ERR_OK = 0
ERR_SERVICE_UNAVAILABLE = 1003
ERR_INVALID_PARAMETER = 1005
ERR_FEATURE_UNAVAILABLE = 1006
ERR_GUILD_NOT_IN_GUILD = 14002

ERR_MAIL_NOT_FOUND = 17000
ERR_MAIL_EXPIRED = 17001
ERR_MAIL_RECIPIENT_BOX_FULL = 17002
ERR_MAIL_ATTACHMENT_INVALID = 17003
ERR_MAIL_REQUEST_CONFLICT = 17004
ERR_MAIL_SYSTEM_LIMIT = 17005
ERR_MAIL_CLAIM_PENDING = 17006
ERR_MAIL_CLAIM_MANUAL = 17007
ERR_MAIL_ATTACHMENT_PROTECTED = 17008
ERR_MAIL_RECIPIENT_DELETED = 17011

ERR_ASSET_PARTIAL_APPLIED = 27007


class MailError(Exception):
    """mail 领域基础异常。"""


class MailInvalidError(MailError):
    """邮件请求参数非法。"""


class MailDeniedError(MailError):
    """发件方或角色无权执行该邮件操作。"""


class MailNotFoundError(MailError):
    """邮件或活动记录不存在。"""


class MailExpiredError(MailError):
    """邮件已过期。"""


class MailBoxFullError(MailError):
    """个人收件箱或共享邮件容量已满。"""


class MailRecipientDeletedError(MailError):
    """个人邮件收件人角色已删除（护栏墓碑）。"""


class MailAttachmentError(MailError):
    """邮件附件格式、配置或额度非法。"""


class MailConflictError(MailError):
    """幂等冲突、活动名单冲突或状态版本冲突。"""


class MailProtectedError(MailError):
    """邮件附件处于待到账/人工保护状态，不可执行该操作。"""


class MailTooManyPendingError(MailError):
    """玩家资产通道未决操作数或序号跨度达到上限。"""


class StoreUnavailableError(MailError):
    """独占库或下游依赖暂不可用。"""


class RoleFenceUnavailableError(MailError):
    """角色护栏权威状态暂不可读（事务已回滚，无结局未知）。"""


class RoleNotFoundError(MailError):
    """账号目录中不存在该角色。"""


class RoleFenceStaleError(MailError):
    """本库角色护栏副本缺行、落后或冻结，需要回滚后自愈重试。"""

    def __init__(self, player_id: int) -> None:
        super().__init__(f"role fence stale player={player_id}")
        self.player_id = player_id


class InvalidRoleFenceStateError(MailError):
    """协调者传入的角色护栏状态字段不合法。"""


class AssetOpLeaseLostError(MailError):
    """资产指令重排时租约已被其他副本接管。"""


class AssetOpPoisonRowError(MailError):
    """资产指令载荷无法反序列化，已推迟等待人工介入。"""


class AssetOpOutcomeFlipError(MailError):
    """同一 seq 两次查询给出的终结结局不一致（违反不变量 I2）。"""


def rejection_tip_id(error: BaseException | None) -> int:
    """将领域异常映射为客户端/管理接口的 Tip 错误码（与 Go server.rejection 对齐）。"""
    if error is None:
        return ERR_OK
    if isinstance(error, (MailInvalidError, MailDeniedError, RoleNotFoundError)):
        return ERR_INVALID_PARAMETER
    if isinstance(error, MailNotFoundError):
        return ERR_MAIL_NOT_FOUND
    if isinstance(error, MailExpiredError):
        return ERR_MAIL_EXPIRED
    if isinstance(error, MailBoxFullError):
        return ERR_MAIL_RECIPIENT_BOX_FULL
    if isinstance(error, MailRecipientDeletedError):
        return ERR_MAIL_RECIPIENT_DELETED
    if isinstance(error, MailAttachmentError):
        return ERR_MAIL_ATTACHMENT_INVALID
    if isinstance(error, MailConflictError):
        return ERR_MAIL_REQUEST_CONFLICT
    if isinstance(error, MailProtectedError):
        return ERR_MAIL_ATTACHMENT_PROTECTED
    if isinstance(error, MailTooManyPendingError):
        return ERR_MAIL_CLAIM_PENDING
    if isinstance(error, RoleFenceUnavailableError):
        logger.info("mail role fence authority unavailable err=%s", error)
        return ERR_FEATURE_UNAVAILABLE
    logger.error("mail authoritative request failed err=%s", error)
    return ERR_SERVICE_UNAVAILABLE
