"""payment 服务的错误码（TipInfoMessage.id）与结构化异常定义。

码值与 Go ``go/payment/internal/constants/constants.go`` 及 Tip.xlsx（//payment_error base=31000）逐字一致。
"""

from __future__ import annotations

import asyncio

# CommonError（common_error_tip.pb.go）
ERR_SERVICE_UNAVAILABLE = 1003
ERR_INVALID_PARAMETER = 1005

# PaymentError（payment_error_tip.pb.go，base=31000）
ERR_CHANNEL_UNAVAILABLE = 31000
ERR_ORDER_NOT_FOUND = 31001
ERR_ORDER_NOT_PAYABLE = 31002
ERR_ORDER_EXPIRED = 31003
ERR_CHANNEL_MISMATCH = 31004
ERR_CURRENCY_UNSUPPORTED = 31005
ERR_BUSY = 31006
ERR_LOGIN_REQUIRED = 31007
ERR_REAL_NAME_REQUIRED = 31008
ERR_MINOR_NOT_ALLOWED = 31009
ERR_MINOR_SINGLE_LIMIT = 31010
ERR_MINOR_DAILY_LIMIT = 31011
ERR_MINOR_MONTHLY_LIMIT = 31012


class PaymentError(Exception):
    """支付业务异常基类，携带写入 TipInfoMessage.id 的 tip 码与可选已有意图视图。"""

    tip_id: int = ERR_SERVICE_UNAVAILABLE

    def __init__(self, message: str = "", *, tip_id: int | None = None, intent: object | None = None) -> None:
        super().__init__(message)
        if tip_id is not None:
            self.tip_id = tip_id
        self.intent = intent


class InvalidRequestError(PaymentError):
    tip_id = ERR_INVALID_PARAMETER


class ChannelUnavailableError(PaymentError):
    tip_id = ERR_CHANNEL_UNAVAILABLE


class OrderNotFoundError(PaymentError):
    tip_id = ERR_ORDER_NOT_FOUND


class OrderNotPayableError(PaymentError):
    tip_id = ERR_ORDER_NOT_PAYABLE


class OrderExpiredError(PaymentError):
    tip_id = ERR_ORDER_EXPIRED


class ChannelMismatchError(PaymentError):
    tip_id = ERR_CHANNEL_MISMATCH


class CurrencyUnsupportedError(PaymentError):
    tip_id = ERR_CURRENCY_UNSUPPORTED


class PaymentBusyError(PaymentError):
    tip_id = ERR_BUSY


class LoginRequiredError(PaymentError):
    tip_id = ERR_LOGIN_REQUIRED


class RealNameRequiredError(PaymentError):
    tip_id = ERR_REAL_NAME_REQUIRED


class MinorNotAllowedError(PaymentError):
    tip_id = ERR_MINOR_NOT_ALLOWED


class MinorSingleLimitError(PaymentError):
    tip_id = ERR_MINOR_SINGLE_LIMIT


class MinorDailyLimitError(PaymentError):
    tip_id = ERR_MINOR_DAILY_LIMIT


class MinorMonthlyLimitError(PaymentError):
    tip_id = ERR_MINOR_MONTHLY_LIMIT


class StoreUnavailableError(PaymentError):
    """底层 SQL 存储不可用或写入结局未知，映射为 kServiceUnavailable(1003)。"""

    tip_id = ERR_SERVICE_UNAVAILABLE


class FactConflictError(Exception):
    """同一渠道交易号再次报来，但金额、币种或订单绑定与已记事实冲突。"""


# 存储层内部错误（由 core 转换为业务或故障错误）
class RecordNotFoundError(Exception):
    """要查的意图或交易行不存在。"""


class InvalidKeyError(Exception):
    """订单号、商户单号或交易号格式不符合约定。"""


class MonthlyLimitError(Exception):
    """未成年人当月累计额度超限。"""


class DailyLimitError(Exception):
    """未成年人当日累计额度超限。"""


# 商城 HTTP 客户端错误（与 Go internal/mall/client.go 对齐）
class MallError(Exception):
    """调用商城 HTTP 内部接口失败基类。"""


class MallInvalidError(MallError):
    """商城返回 400（参数非法，不可重试）。"""


class MallNotFoundError(MallError):
    """商城返回 404（订单不存在）。"""


class MallConflictError(MallError):
    """商城返回 409（与商城已记录事实冲突，终局）。"""


class MallStatusError(MallError):
    """商城返回其它非 200 HTTP 状态（可重试）。"""

    def __init__(self, status: int) -> None:
        super().__init__(f"mall: request failed with HTTP {status}")
        self.status = status


# 渠道层错误（与 Go internal/channel/channel.go 对齐）
class ChannelError(Exception):
    """支付渠道协议异常基类。"""


class BadSignatureError(ChannelError):
    """回调验签失败、信封非法或商户不匹配。"""


class TempUnavailableError(ChannelError):
    """回调验签依赖的远端临时不可达（应回 Retry 让渠道重投）。"""


class OrderPaidError(ChannelError):
    """下单时渠道返回订单已付（如微信 ORDERPAID），应转查单记账。"""


class QueryUnsupportedError(ChannelError):
    """渠道不支持按 out_trade_no 主动查单。"""


class ChannelLoginRequiredError(ChannelError):
    """小游戏渠道需要刷新登录凭证。"""


class ChannelCallError(ChannelError):
    """渠道明确返回业务或 HTTP 失败（仅记录安全错误码，不含密钥或敏感信息）。"""

    def __init__(self, channel: str, op: str, status: int, code: str) -> None:
        super().__init__(f"payment channel: {channel} {op} failed (http {status}, code {code})")
        self.channel = channel
        self.op = op
        self.status = status
        self.code = code


def tip_id_for(exc: BaseException | None) -> int:
    """将任意异常映射为 TipInfoMessage.id；未知异常一律按服务不可用（1003）处理。"""
    if exc is None:
        return 0
    if isinstance(exc, PaymentError):
        return exc.tip_id
    return ERR_SERVICE_UNAVAILABLE


def error_class(exc: BaseException | None) -> str:
    """将异常归纳为写入 *_last_error 的脱敏错误类别（不含请求体、密钥与个人信息）。"""
    if exc is None:
        return ""
    if isinstance(exc, MallConflictError):
        return "mall_conflict"
    if isinstance(exc, MallInvalidError):
        return "mall_invalid"
    if isinstance(exc, MallNotFoundError):
        return "mall_not_found"
    if isinstance(exc, MallStatusError):
        return f"mall_http_{exc.status}"
    if isinstance(exc, ChannelCallError):
        return f"{exc.channel}_{exc.op}_{exc.status}_{exc.code}"
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return "timeout"
    return "unreachable"
