"""friend 服务的 Tip 业务码与领域异常（与 Go friend/internal/constants 及 data 哨兵完全对齐）。"""

from __future__ import annotations

# CommonError 段（Tip.xlsx）
ERR_STORAGE = 1003  # CommonError_kServiceUnavailable（本域唯一 fault 码）
ERR_INVALID_PARAMETER = 1005  # CommonError_kInvalidParameter
ERR_ROLE_NOT_VERIFIED = 1006  # CommonError_kFeatureUnavailable
ERR_RATE_LIMITED = 1008  # CommonError_kRateLimitExceeded

# FriendError 段（Tip.xlsx base=15000，全部为规则拒绝而非故障）
ERR_CANNOT_ADD_SELF = 15000  # FriendError_kFriendCannotAddSelf
ERR_ALREADY_FRIENDS = 15001  # FriendError_kFriendAlreadyFriends
ERR_FRIEND_LIST_FULL = 15002  # FriendError_kFriendListFull
ERR_REQUEST_ALREADY_SENT = 15003  # FriendError_kFriendRequestAlreadySent
ERR_TARGET_FRIEND_LIST_FULL = 15004  # FriendError_kFriendTargetListFull
ERR_NO_PENDING_REQUEST = 15005  # FriendError_kFriendNoPendingRequest
ERR_TOO_MANY_PENDING = 15006  # FriendError_kFriendTooManyPending
ERR_BLOCKED = 15007  # FriendError_kFriendBlocked（双向中性码，不泄露拉黑方向）
ERR_BLOCK_LIST_FULL = 15008  # FriendError_kFriendBlockListFull
ERR_TARGET_INBOX_FULL = 15009  # FriendError_kFriendTargetInboxFull


class FriendError(Exception):
    """friend 领域基础异常。"""


class BlockedError(FriendError):
    """两人之间存在任一方向拉黑（刻意不区分方向）。"""


class AlreadyFriendsError(FriendError):
    """两人已存在好友边。"""


class RequestAlreadySentError(FriendError):
    """本方向好友申请已处于待处理状态。"""


class TooManyPendingError(FriendError):
    """申请发起方出站待处理申请数达到上限。"""


class TargetInboxFullError(FriendError):
    """接收方入站待处理申请数达到上限。"""


class SenderFriendsFullError(FriendError):
    """申请发起方（Sender）好友数达到上限（按 RPC 映射为不同 Tip 码）。"""


class AcceptorFriendsFullError(FriendError):
    """申请接收方（Acceptor）好友数达到上限（按 RPC 映射为不同 Tip 码）。"""


class RequestNotFoundError(FriendError):
    """不存在匹配的待处理好友申请。"""


class BlockListFullError(FriendError):
    """操作者黑名单条数达到上限。"""


class OriginalRoleError(FriendError):
    """原会话角色归属已失效、已删除或被冻结。"""


class RoleFenceUnavailableError(FriendError):
    """角色护栏权威状态当前不可读取，调用方应带内回 1006。"""


class InvalidRoleFenceStateError(FriendError):
    """RoleFenceParticipant 入参中的 RoleFenceState 非法（应回 gRPC INVALID_ARGUMENT）。"""


class InvalidDirectoryInputError(FriendError):
    """在线目录的 cursor 或 query 参数非法。"""


class StoreUnavailableError(FriendError):
    """底层 MySQL / Redis / 发号存储暂不可用（应带内回 1003）。"""
