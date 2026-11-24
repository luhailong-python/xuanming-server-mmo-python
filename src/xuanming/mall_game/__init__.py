"""纯 Python 版商城微服务（mall_game），与原 Java ``mall_game`` 协议与状态机逐字对齐。"""

from .app import create_mall_app
from .auth import INTERNAL_ROLES, build_hmac_middleware, sign_v1, verify_v1
from .config import MallGameConfig
from .models import (
    STORAGE_CONTRACT,
    AbnormalPayment,
    CommerceError,
    DeliveryIntent,
    Order,
    OrderGrant,
    PaymentReceipt,
    PaymentRevision,
    PendingScanCursor,
    RefundIntent,
    ReversalIntent,
    RmbConsignment,
    RmbIncome,
    RmbOrder,
    RmbWithdrawal,
    Sku,
    SkuTerms,
)
from .node import run
from .sql_store import SchemaVerifier, SqlMallStore, open_pool, session_init_sql
from .store import MallStore, MemoryMallStore

__all__ = [
    "AbnormalPayment",
    "CommerceError",
    "DeliveryIntent",
    "INTERNAL_ROLES",
    "MallGameConfig",
    "MallStore",
    "MemoryMallStore",
    "Order",
    "OrderGrant",
    "PaymentReceipt",
    "PaymentRevision",
    "PendingScanCursor",
    "RefundIntent",
    "ReversalIntent",
    "RmbConsignment",
    "RmbIncome",
    "RmbOrder",
    "RmbWithdrawal",
    "STORAGE_CONTRACT",
    "SchemaVerifier",
    "Sku",
    "SkuTerms",
    "SqlMallStore",
    "build_hmac_middleware",
    "create_mall_app",
    "open_pool",
    "run",
    "session_init_sql",
    "sign_v1",
    "verify_v1",
]
