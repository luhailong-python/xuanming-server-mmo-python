"""mall_game HTTP v1 HMAC 签名、角色路由映射与防重放中间件。

严格对齐 Java ``HmacAuthentication``、Go ``payment/trade`` 客户端及 Python ``payment/mall_client.py``、
``trade/mall_adapter.py``、``portal_mall_admin.py``：
- 规范串：``v1\\n{role}\\n{subject}\\n{timestamp}\\n{nonce}\\n{METHOD}\\n{path}\\n{sha256_hex(body)}``；
- 角色由路由路径静态绑定，不依赖客户端自报；若调用方额外传入 ``X-Commerce-Role`` 则必须与路由角色一致；
- 请求头：``X-Commerce-Subject``、``X-Commerce-Timestamp``、``X-Commerce-Nonce``、``X-Commerce-Signature``；
- 所有校验通过后才按 ``(role, nonce)`` 原子占用防重放记录。
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass
from typing import Protocol

from aiohttp import web

from .config import MIN_SECRET_BYTES, MallGameConfig
from .models import CommerceError, validate_uint64_str

MAX_BODY_BYTES = 16_384
MAX_HEADER_LEN = 256

HEADER_ROLE = "X-Commerce-Role"
HEADER_SUBJECT = "X-Commerce-Subject"
HEADER_TIMESTAMP = "X-Commerce-Timestamp"
HEADER_NONCE = "X-Commerce-Nonce"
HEADER_SIGNATURE = "X-Commerce-Signature"

_TIMESTAMP_RE = re.compile(r"^[0-9]{1,15}$")
_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_SIGNATURE_RE = re.compile(r"^[0-9a-f]{64}$")

# 健康探针路径（仅无正文 GET 免鉴权）
HEALTH_PROBE_PATHS = frozenset((
    "/actuator/health",
    "/actuator/health/readiness",
    "/actuator/health/liveness",
))

# 全部内部接口到角色的唯一绑定（与 Java HmacAuthentication.INTERNAL_ROLES 逐条一致）
INTERNAL_ROLES: dict[str, str] = {
    "/internal/payments": "payment",
    "/internal/payments/order-view": "payment",
    "/internal/payments/skus": "payment",
    "/internal/payments/revision": "payment",
    "/internal/mock/pay": "payment",
    "/internal/mock/reconcile": "payment",
    "/internal/mock/withdraw": "payment",
    "/internal/mock/withdrawals/reconcile": "payment",
    "/internal/refunds/poll": "payment",
    "/internal/refunds/receipt": "payment",
    "/internal/refunds/close": "payment",
    "/internal/deliveries/poll": "delivery",
    "/internal/deliveries/receipt": "delivery",
    "/internal/reversals/poll": "delivery",
    "/internal/reversals/receipt": "delivery",
    "/internal/consignments/register": "delivery",
    "/internal/consignments/list": "delivery",
    "/internal/consignments/get": "delivery",
    "/internal/consignments/cancel": "delivery",
    "/internal/consignments/buy": "delivery",
    "/internal/consignments/orders/get": "delivery",
    "/internal/consignments/orders/list": "delivery",
    "/internal/consignments/orders/cancel": "delivery",
    "/internal/consignments/deliveries/poll": "delivery",
    "/internal/consignments/returns/poll": "delivery",
    "/internal/consignments/returns/receipt": "delivery",
    "/internal/consignments/incomes": "delivery",
    "/internal/consignments/withdrawals/create": "delivery",
    "/internal/consignments/withdrawals/get": "delivery",
    "/internal/admin/skus/list": "admin",
    "/internal/admin/skus/create": "admin",
    "/internal/admin/skus/update": "admin",
    "/internal/admin/skus/stock": "admin",
    "/internal/admin/order-view": "admin",
    "/internal/admin/manual/list": "admin",
    "/internal/admin/manual/orders/resolve": "admin",
    "/internal/admin/manual/refunds/resolve": "admin",
    "/internal/admin/manual/consignments/resolve": "admin",
}

REQ_SUBJECT_KEY = web.RequestKey("commerce_subject", str)
REQ_ROLE_KEY = web.RequestKey("commerce_role", str)
REQ_BODY_KEY = web.RequestKey("commerce_body", bytes)


class NonceClaimer(Protocol):
    """Nonce 防重放存储接口（由 MemoryMallStore / SqlMallStore 实现）。"""

    async def claim_nonce(self, role: str, nonce: str, sent_ms: int) -> None: ...


@dataclass(frozen=True, slots=True)
class AuthContext:
    """已通过 HMAC 认证的调用方上下文。"""

    role: str
    subject: str
    body: bytes


def sign_v1(
    secret: bytes | str,
    role: str,
    subject: str,
    timestamp: str | int,
    nonce: str,
    method: str,
    path: str,
    body: bytes = b"",
) -> str:
    """计算 v1 HMAC-SHA256 十六进制小写签名。"""
    key = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
    digest = hashlib.sha256(body).hexdigest()
    canonical = "\n".join((
        "v1",
        str(role),
        str(subject),
        str(timestamp),
        str(nonce),
        str(method),
        str(path),
        digest,
    )).encode("utf-8")
    return hmac.new(key, canonical, hashlib.sha256).hexdigest()


def verify_v1(
    secret: bytes | str,
    role: str,
    subject: str,
    timestamp: str,
    nonce: str,
    method: str,
    path: str,
    body: bytes,
    signature_hex: str,
) -> bool:
    """常量时间比对 v1 HMAC-SHA256 签名。"""
    if not _SIGNATURE_RE.fullmatch(signature_hex):
        return False
    expected = sign_v1(secret, role, subject, timestamp, nonce, method, path, body)
    return hmac.compare_digest(expected, signature_hex)


def _single_header(request: web.Request, name: str) -> str:
    values = request.headers.getall(name, [])
    if len(values) != 1 or values[0] is None or len(values[0]) > MAX_HEADER_LEN:
        raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")
    return values[0]


def _secret_for_role(config: MallGameConfig, role: str) -> str:
    if role == "payment":
        return config.payment_secret
    if role == "delivery":
        return config.delivery_secret
    if role == "admin":
        return config.admin_secret
    return config.gateway_secret


def _expected_subject(config: MallGameConfig, role: str) -> str:
    if role == "delivery":
        return "trade"
    if role == "admin":
        return config.admin_subject
    return config.payment_subject


def resolve_route_role(raw_path: str) -> str:
    """根据原始路径判定所需角色；未登记的 ``/internal/*`` 路径直接返回 404。"""
    role = INTERNAL_ROLES.get(raw_path)
    if role is not None:
        return role
    if raw_path.startswith("/internal/"):
        raise CommerceError(404, "NOT_FOUND", "接口不存在")
    return "gateway"


async def read_bounded_body(request: web.Request) -> bytes:
    """有界读取请求体（上限 16 KiB），超限返回 413。"""
    if request.content_length is not None and request.content_length > MAX_BODY_BYTES:
        raise CommerceError(413, "PAYLOAD_TOO_LARGE", "请求体过大")
    buf = bytearray()
    async for chunk in request.content.iter_chunked(8192):
        buf.extend(chunk)
        if len(buf) > MAX_BODY_BYTES:
            raise CommerceError(413, "PAYLOAD_TOO_LARGE", "请求体过大")
    return bytes(buf)


def build_hmac_middleware(config: MallGameConfig, store: NonceClaimer):
    """构造 aiohttp HMAC v1 鉴权中间件。"""

    @web.middleware
    async def hmac_middleware(request: web.Request, handler):
        try:
            body = await read_bounded_body(request)
            raw_path = request.raw_path
            if (
                "?" in raw_path
                or request.query_string
                or "%" in raw_path
                or ";" in raw_path
                or "\\" in raw_path
                or "//" in raw_path
                or ".." in raw_path
            ):
                raise CommerceError(400, "INVALID_PATH", "请求路径非法")

            if request.method == "GET" and len(body) == 0 and raw_path in HEALTH_PROBE_PATHS:
                return await handler(request)

            role = resolve_route_role(raw_path)

            # 若调用方显式附带 X-Commerce-Role，必须与路由绑定角色一致
            role_headers = request.headers.getall(HEADER_ROLE, [])
            if role_headers:
                if len(role_headers) != 1 or role_headers[0] != role:
                    raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")

            subject = _single_header(request, HEADER_SUBJECT)
            if role == "gateway":
                validate_uint64_str(subject, status=401, code="AUTH_FAILED", message="调用方认证失败")
            else:
                if subject != _expected_subject(config, role):
                    raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")

            timestamp = _single_header(request, HEADER_TIMESTAMP)
            nonce = _single_header(request, HEADER_NONCE)
            signature = _single_header(request, HEADER_SIGNATURE)
            if (
                not _TIMESTAMP_RE.fullmatch(timestamp)
                or not _NONCE_RE.fullmatch(nonce)
                or not _SIGNATURE_RE.fullmatch(signature)
            ):
                raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")

            secret = _secret_for_role(config, role)
            if not secret or len(secret.encode("utf-8")) < MIN_SECRET_BYTES:
                raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")

            if not verify_v1(secret, role, subject, timestamp, nonce, request.method, raw_path, body, signature):
                raise CommerceError(401, "AUTH_FAILED", "调用方认证失败")

            await store.claim_nonce(role, nonce, int(timestamp))
            request[REQ_ROLE_KEY] = role
            request[REQ_SUBJECT_KEY] = subject
            request[REQ_BODY_KEY] = body
        except CommerceError as err:
            return web.json_response({"error": err.message}, status=err.status)

        try:
            return await handler(request)
        except CommerceError as err:
            return web.json_response({"error": err.message}, status=err.status)

    return hmac_middleware
