"""chat 服务业务提示码（TipInfoMessage.id）与领域异常定义。"""

from __future__ import annotations

# 公共错误码（common_error_tip）
ERR_STORAGE = 1003  # CommonError_kServiceUnavailable（仅用于真实存储/依赖故障）
ERR_INVALID_PARAMETER = 1005  # CommonError_kInvalidParameter
ERR_CHANNEL_UNAVAILABLE = 1006  # CommonError_kFeatureUnavailable
ERR_RATE_LIMITED = 1008  # CommonError_kRateLimitExceeded
ERR_MESSAGE_TOO_LONG = 1010  # CommonError_kMessageSizeExceeded

# 聊天专属错误码（chat_error_tip, base=18000）
ERR_CONTENT_REJECTED = 18000  # ChatError_kChatContentRejected
ERR_CONTENT_CHECK_UNAVAILABLE = 18001  # ChatError_kChatContentCheckUnavailable
ERR_LEVEL_TOO_LOW = 18002  # ChatError_kChatLevelTooLow
ERR_DUPLICATE_SPAM = 18003  # ChatError_kChatDuplicateSpam
ERR_PRIVATE_TARGET_LIMIT = 18004  # ChatError_kChatPrivateTargetLimit
ERR_CONTACT_REJECTED = 18005  # ChatError_kChatContactRejected
ERR_MESSAGE_NOT_FOUND = 18006  # ChatError_kChatMessageNotFound


class ChatError(Exception):
    """chat 领域异常基类。"""


class OriginalRoleError(ChatError):
    """原会话角色归属已变化、不存在或已被最终冻结（对应 Go ErrOriginal）。"""


class RequestConflictError(ChatError):
    """同一 (player_id, request_id) 已用于不同请求内容（对应 Go ErrConflict）。"""


class InvalidChatError(ChatError):
    """请求参数或状态非法（对应 Go ErrInvalid）。"""


class PolicyMismatchError(ChatError):
    """当前配置生成的策略摘要与库内 chat_policy 不一致（对应 Go ErrPolicy）。"""


class StoreUnavailableError(ChatError):
    """SQL 存储、目录依赖失败或持久原件损坏。"""
