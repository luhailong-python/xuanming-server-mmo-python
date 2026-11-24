"""rank 服务业务提示码（Tip）常量与领域异常体系。

错误码取值与 Go ``shared/generated/pb/table`` 完全对齐：
- 通用码：``1000``（成功）、``1003``（服务不可用）、``1005``（参数非法/拒绝）
- 排行码：``21000..21003``
- 邮件码：``17011``（收件人角色已注销）
"""

from __future__ import annotations

ERR_OK = 0
ERR_SUCCESS = 1000
ERR_SERVICE_UNAVAILABLE = 1003
ERR_INVALID_PARAMETER = 1005

ERR_BOARD_NOT_FOUND = 21000
ERR_GENERATION_CLOSED = 21001
ERR_REQUEST_CONFLICT = 21002
ERR_SETTLEMENT_PENDING = 21003

ERR_MAIL_RECIPIENT_DELETED = 17011


class RankError(Exception):
    """排行榜领域业务异常基类，携带带内 Tip 提示码。"""

    tip_code: int = ERR_SERVICE_UNAVAILABLE

    def __init__(self, message: str = "", *, tip_code: int | None = None) -> None:
        super().__init__(message)
        if tip_code is not None:
            self.tip_code = tip_code


class RankInvalidError(RankError):
    """参数非法（1005）。"""

    tip_code = ERR_INVALID_PARAMETER


class RankDeniedError(RankError):
    """调用方无权操作该榜或该范围（1005）。"""

    tip_code = ERR_INVALID_PARAMETER


class RankNotFoundError(RankError):
    """排行榜不存在（21000）。"""

    tip_code = ERR_BOARD_NOT_FOUND


class RankClosedError(RankError):
    """排行榜本代已关闭/封榜（21001）。"""

    tip_code = ERR_GENERATION_CLOSED


class RankConflictError(RankError):
    """排行榜请求冲突（21002）。"""

    tip_code = ERR_REQUEST_CONFLICT


class RankCursorError(RankError):
    """排行榜输入游标未齐或跳号（21003）。"""

    tip_code = ERR_SETTLEMENT_PENDING


class RankUnavailableError(RankError):
    """排行榜存储或上游依赖不可用（1003）。"""

    tip_code = ERR_SERVICE_UNAVAILABLE


class EntriesDriftError(RankUnavailableError):
    """排行榜分片计数与分数表数据不一致（fail-closed 回滚，1003）。"""


class ScoreInsertRaceError(Exception):
    """不同来源片并发首次插入同一实体撞唯一键，触发整笔事务回滚重试。"""


def tip_code_for_error(exc: BaseException | None) -> int:
    """将领域异常或未知异常映射为带内 Tip 提示码。"""
    if exc is None:
        return ERR_OK
    if isinstance(exc, RankError):
        return exc.tip_code
    return ERR_SERVICE_UNAVAILABLE
