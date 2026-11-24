"""Business outcomes of the store layer (Go ``shared/accountdirectory`` sentinels and ``logic`` name errors)."""


class DirectoryError(Exception):
    """Base for outcomes that map to a gRPC status; anything else is an unknown/SQL failure (UNAVAILABLE)."""


class Missing(DirectoryError):
    pass


class Conflict(DirectoryError):
    pass


class Full(DirectoryError):
    pass


class Invalid(DirectoryError):
    pass


class NotCommitted(DirectoryError):
    """Create failed before the commit point; login releases the name when it sees ABORTED."""


class RetryLater(DirectoryError):
    pass


class ZoneConflict(DirectoryError):
    def __init__(self, existing: int) -> None:
        super().__init__(f"home zone already mapped to {existing}")
        self.existing = existing


class NameConflict(DirectoryError):
    """The player id already holds a different name (id reuse); never delete the old row."""


class NameInUse(DirectoryError):
    """The name belongs to a committed role (bound, or reserved with a committed role row)."""


class NameOutsideWindow(DirectoryError):
    pass


class TxNeverBegan(RuntimeError):
    """事务在任何业务语句发出前失败（对应 Go ``txNeverBegan``）。"""


class RecallBatchExists(DirectoryError):
    """同 request_id 的回收批次已存在。"""


class RecallSourceConsumed(DirectoryError):
    """回收来源流水已被其它批次或人工登记消费。"""


class RecallBatchNotFound(DirectoryError):
    """指定 request_id 的回收批次不存在。"""


class RecallOpNotFound(DirectoryError):
    """指定 op_id 的回收指令不存在。"""
