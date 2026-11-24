"""支付渠道共享密码学、JOSE/JWS、整数金额解析与受限 HTTP 客户端工具。

全部能力均基于 ``cryptography``、``aiohttp`` 与 Python 标准库实现：
- 不经浮点做元/分换算（与 Go ``FormatYuan`` / ``ParseYuan`` 一致）；
- JWS 仅显式接受调用方指定的 ES256 或 RS256，绝不按令牌头挑选算法；
- 渠道 HTTP 请求不跟随重定向、应答上限 1 MiB、连接失败错误脱敏（不回显查询串里的密钥或令牌）。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit

import aiohttp
from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature, encode_dss_signature

from .errors import ChannelError
from .model import BEIJING_TZ

# 渠道金额上限（分），与商城 MAX_AMOUNT_MINOR 同量级，防整数溢出
MAX_YUAN_FEN = 9_000_000_000_000_000

# 渠道 HTTP 应答正文上限（1 MiB）
MAX_CHANNEL_RESPONSE_BYTES = 1 << 20

# 支付宝时间格式（北京时间）
ALIPAY_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# 商城回执允许的退款流水号字符集
REFUND_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

# 匹配 PEM 块
_PEM_BLOCK_RE = re.compile(
    rb"-----BEGIN ([A-Z0-9 ]+)-----\s*([A-Za-z0-9+/=\r\n\t ]+?)\s*-----END \1-----"
)


# ---------------------------------------------------------------------------
# 通用工具（全数字判断、十六进制常量时间比对、随机串、时间格式）
# ---------------------------------------------------------------------------


def all_digits(text: str) -> bool:
    """仅当非空且全为 ASCII 数字 0-9 时返回 True。"""
    return bool(text) and text.isascii() and text.isdigit()


def equal_hex(a: str, b: str) -> bool:
    """大小写无关、常量时间的十六进制签名比对。"""
    if len(a) != len(b):
        return False
    return hmac.compare_digest(a.lower().encode("ascii", errors="surrogateescape"), b.lower().encode("ascii", errors="surrogateescape"))


def random_nonce_hex() -> str:
    """生成 32 字符小写十六进制随机串（微信 nonce_str 上限 32 字符）。"""
    return secrets.token_hex(16)


def hmac_sha256_hex(key: bytes, message: str | bytes) -> str:
    """计算 HMAC-SHA256 小写十六进制摘要。"""
    data = message.encode("utf-8") if isinstance(message, str) else message
    return hmac.new(key, data, hashlib.sha256).hexdigest()


def header_value(headers: Mapping[str, str], target: str) -> str:
    """大小写无关读取 HTTP 请求头/应答头。"""
    lower = target.lower()
    for key, value in headers.items():
        if key.lower() == lower:
            return value
    return ""


def format_rfc3339_cst(ms: int) -> str:
    """将 Unix 毫秒格式化为北京时间 RFC3339（如 ``2026-11-24T12:00:00+08:00``）。"""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=BEIJING_TZ).replace(microsecond=0)
    return dt.isoformat()


def parse_rfc3339_ms(text: str) -> int:
    """解析 RFC3339 时间戳为 Unix 毫秒；非法抛 ValueError。"""
    if not text or ("T" not in text and "t" not in text):
        raise ValueError("timestamp is not RFC3339")
    normalized = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        raise ValueError("RFC3339 timestamp lacks timezone offset")
    return int(dt.timestamp() * 1000)


def format_cst_datetime(ms: int) -> str:
    """将 Unix 毫秒格式化为支付宝北京时间 ``YYYY-MM-DD HH:MM:SS``。"""
    return datetime.fromtimestamp(ms / 1000.0, tz=BEIJING_TZ).strftime(ALIPAY_TIME_FORMAT)


def parse_cst_datetime_ms(text: str) -> int:
    """解析支付宝北京时间 ``YYYY-MM-DD HH:MM:SS`` 为 Unix 毫秒；非法抛 ValueError。"""
    dt = datetime.strptime(text, ALIPAY_TIME_FORMAT).replace(tzinfo=BEIJING_TZ)
    return int(dt.timestamp() * 1000)


def time_from_unix_text_ms(text: str) -> int:
    """将十进制秒或毫秒时间戳文本转为 Unix 毫秒；非法返回 0（由调用方补当前时刻）。"""
    try:
        value = int(text)
    except ValueError:
        return 0
    if value <= 0:
        return 0
    return value if value > 10**12 else value * 1000


# ---------------------------------------------------------------------------
# 元 / 分 纯整数转换（禁止浮点）
# ---------------------------------------------------------------------------


def format_yuan(fen: int) -> str:
    """将非负整数分格式化为支付宝需要的「元，两位小数」文本。"""
    if isinstance(fen, bool) or not isinstance(fen, int) or fen < 0:
        raise ValueError("payment channel: fen must be a non-negative integer")
    return f"{fen // 100}.{fen % 100:02d}"


def parse_yuan(text: str) -> int:
    """将「元」十进制文本（至多两位小数）纯整数换算为分，严禁经过浮点。"""
    if not isinstance(text, str) or not text:
        raise ValueError(f"payment channel: amount {text!r} is not a yuan amount with at most two decimals")
    if "." in text:
        whole, frac = text.split(".", 1)
        if not whole or len(whole) > 16 or not all_digits(whole):
            raise ValueError(f"payment channel: amount {text!r} is not a yuan amount with at most two decimals")
        if not frac or len(frac) > 2 or not all_digits(frac):
            raise ValueError(f"payment channel: amount {text!r} is not a yuan amount with at most two decimals")
    else:
        whole, frac = text, ""
        if not whole or len(whole) > 16 or not all_digits(whole):
            raise ValueError(f"payment channel: amount {text!r} is not a yuan amount with at most two decimals")
    yuan = int(whole)
    cents = int(frac.ljust(2, "0"))
    if yuan > (MAX_YUAN_FEN - cents) // 100:
        raise ValueError(f"payment channel: amount {text!r} is out of range")
    return yuan * 100 + cents


# ---------------------------------------------------------------------------
# PEM / 裸 Base64 密钥与证书解析（报错绝不回显密钥内容）
# ---------------------------------------------------------------------------


def _key_der(text: str, *pem_types: str) -> bytes:
    trimmed = text.strip()
    if not trimmed:
        raise ValueError("payment channel: key is empty")
    raw = trimmed.encode("ascii", errors="strict")
    if trimmed.startswith("-----BEGIN "):
        match = _PEM_BLOCK_RE.search(raw)
        if match is None:
            raise ValueError("payment channel: key PEM cannot be decoded")
        block_type = match.group(1).decode("ascii")
        if block_type not in pem_types:
            raise ValueError(f"payment channel: unexpected key PEM type {block_type!r}")
        try:
            return base64.b64decode(match.group(2), validate=False)
        except ValueError as exc:
            raise ValueError("payment channel: key PEM body is not valid base64") from exc
    compact = re.sub(r"[\r\n \t]+", "", trimmed)
    try:
        return base64.b64decode(compact, validate=True)
    except ValueError as exc:
        raise ValueError("payment channel: key is neither PEM nor base64") from exc


def parse_rsa_private_key(text: str) -> rsa.RSAPrivateKey:
    """解析 RSA 私钥：支持 PEM（PKCS#8/PKCS#1）或裸 base64 DER。"""
    der = _key_der(text, "PRIVATE KEY", "RSA PRIVATE KEY")
    try:
        key = serialization.load_der_private_key(der, password=None)
    except Exception as exc:
        raise ValueError("payment channel: private key is neither PKCS#8 nor PKCS#1 RSA") from exc
    if not isinstance(key, rsa.RSAPrivateKey):
        raise ValueError("payment channel: private key is not RSA")
    return key


def parse_rsa_public_key(text: str) -> rsa.RSAPublicKey:
    """解析 RSA 公钥：支持 PEM（SubjectPublicKeyInfo / PKCS#1）或裸 base64 DER。"""
    der = _key_der(text, "PUBLIC KEY", "RSA PUBLIC KEY")
    try:
        key = serialization.load_der_public_key(der)
    except Exception as exc:
        raise ValueError("payment channel: public key is neither SubjectPublicKeyInfo nor PKCS#1 RSA") from exc
    if not isinstance(key, rsa.RSAPublicKey):
        raise ValueError("payment channel: public key is not RSA")
    return key


def parse_ec_private_key(text: str) -> ec.EllipticCurvePrivateKey:
    """解析 ECDSA 私钥（如 App Store Connect .p8 PKCS#8 或 SEC1 PEM / 裸 base64）。"""
    der = _key_der(text, "PRIVATE KEY", "EC PRIVATE KEY")
    try:
        key = serialization.load_der_private_key(der, password=None)
    except Exception as exc:
        raise ValueError("payment channel: private key is neither PKCS#8 nor SEC1 ECDSA") from exc
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise ValueError("payment channel: private key is not ECDSA")
    return key


def parse_x509_certificates(text: str) -> tuple[x509.Certificate, ...]:
    """解析一段包含一张或多张 PEM X.509 证书的文本。"""
    raw = text.strip().encode("ascii", errors="strict")
    certs: list[x509.Certificate] = []
    for match in _PEM_BLOCK_RE.finditer(raw):
        if match.group(1) != b"CERTIFICATE":
            continue
        try:
            der = base64.b64decode(match.group(2), validate=False)
            certs.append(x509.load_der_x509_certificate(der))
        except Exception as exc:
            raise ValueError("payment channel: certificate PEM cannot be parsed") from exc
    if not certs:
        raise ValueError("payment channel: no certificate in PEM")
    return tuple(certs)


# ---------------------------------------------------------------------------
# RSA PKCS#1 v1.5 SHA-256 与 JOSE/JWS（ES256 / RS256）
# ---------------------------------------------------------------------------


def rsa_sign_sha256_b64(key: rsa.RSAPrivateKey, message: bytes) -> str:
    """使用 RSA PKCS#1 v1.5 + SHA-256 签名并返回标准 Base64 文本。"""
    sig = key.sign(message, padding.PKCS1v15(), hashes.SHA256())
    return base64.b64encode(sig).decode("ascii")


def rsa_verify_sha256_b64(key: rsa.RSAPublicKey, message: bytes, signature_b64: str) -> None:
    """验证标准 Base64 编码的 RSA PKCS#1 v1.5 + SHA-256 签名，失败抛 ValueError。"""
    try:
        sig = base64.b64decode(signature_b64, validate=True)
    except ValueError as exc:
        raise ValueError("payment channel: signature is not valid base64") from exc
    try:
        key.verify(sig, message, padding.PKCS1v15(), hashes.SHA256())
    except InvalidSignature as exc:
        raise ValueError("payment channel: RSA-SHA256 signature rejected") from exc


def b64url_encode(data: bytes) -> str:
    """无填充 URL 安全 Base64 编码。"""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(text: str) -> bytes:
    """严格解码无填充 URL 安全 Base64 文本（禁止 '=' 与非 base64url 字符）。"""
    if not text or "=" in text or not text.isascii():
        raise ValueError("payment channel: invalid raw base64url segment")
    pad_len = (-len(text)) % 4
    if pad_len == 3:
        raise ValueError("payment channel: invalid raw base64url length")
    try:
        raw = base64.urlsafe_b64decode(text + ("=" * pad_len))
    except ValueError as exc:
        raise ValueError("payment channel: invalid base64url encoding") from exc
    if b64url_encode(raw) != text:
        raise ValueError("payment channel: non-canonical base64url segment")
    return raw


@dataclass(frozen=True, slots=True)
class JwsParts:
    """JWS 紧凑序列化拆解后的四部分。"""

    header: bytes
    payload: bytes
    signing_input: bytes
    signature: bytes


def split_jws(token: str) -> JwsParts:
    """按三段紧凑 JWS 格式拆解 ``header.payload.signature``。"""
    segments = token.split(".")
    if len(segments) != 3:
        raise ValueError("payment channel: token is not a compact JWS")
    header = b64url_decode(segments[0])
    payload = b64url_decode(segments[1])
    signature = b64url_decode(segments[2])
    signing_input = f"{segments[0]}.{segments[1]}".encode("ascii")
    return JwsParts(header=header, payload=payload, signing_input=signing_input, signature=signature)


def jwt_sign(header: dict, claims: dict, signer: Callable[[bytes], bytes]) -> str:
    """使用指定签名回调签发紧凑 JWT 字符串。"""
    header_b64 = b64url_encode(json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    claims_b64 = b64url_encode(json.dumps(claims, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    signing_input = f"{header_b64}.{claims_b64}"
    sig = signer(signing_input.encode("ascii"))
    return f"{signing_input}.{b64url_encode(sig)}"


def es256_sign_raw(key: ec.EllipticCurvePrivateKey, signing_input: bytes) -> bytes:
    """签出 JWS ES256 要求的定长 64 字节 ``R || S`` 原始签名。"""
    der_sig = key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der_sig)
    return r.to_bytes(32, "big") + s.to_bytes(32, "big")


def es256_verify_raw(key: ec.EllipticCurvePublicKey, signing_input: bytes, raw_sig: bytes) -> None:
    """将 64 字节 ``R || S`` 转为 ASN.1 DER 并校验 P-256 ECDSA-SHA256 签名。"""
    if len(raw_sig) != 64:
        raise ValueError("payment channel: ES256 signature is not 64 bytes")
    r = int.from_bytes(raw_sig[:32], "big")
    s = int.from_bytes(raw_sig[32:], "big")
    der_sig = encode_dss_signature(r, s)
    try:
        key.verify(der_sig, signing_input, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature as exc:
        raise ValueError("payment channel: ES256 signature rejected") from exc


def rs256_sign_raw(key: rsa.RSAPrivateKey, signing_input: bytes) -> bytes:
    """签出 JWS RS256（PKCS#1 v1.5 SHA-256）原始签名字节。"""
    return key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())


def rs256_verify_raw(key: rsa.RSAPublicKey, signing_input: bytes, raw_sig: bytes) -> None:
    """校验 JWS RS256（PKCS#1 v1.5 SHA-256）原始签名字节。"""
    try:
        key.verify(raw_sig, signing_input, padding.PKCS1v15(), hashes.SHA256())
    except InvalidSignature as exc:
        raise ValueError("payment channel: RS256 signature rejected") from exc


def cert_valid_at(cert: x509.Certificate, at: datetime) -> bool:
    """判断 X.509 证书在 UTC 时刻 ``at`` 是否处于有效期内。"""
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    return cert.not_valid_before_utc <= at <= cert.not_valid_after_utc


# ---------------------------------------------------------------------------
# 渠道受限 HTTP 客户端（禁重定向、1 MiB 上限、错误脱敏、支持单测注入 transport）
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HttpResponseData:
    """渠道 HTTP 应答值对象。"""

    status: int
    headers: Mapping[str, str]
    body: bytes


HttpTransport = Callable[[str, str, Mapping[str, str], bytes | None], Awaitable[HttpResponseData]]


class ChannelHttpClient:
    """支付渠道共享 aiohttp 封装：不跟随重定向、限制 1 MiB 应答并脱敏传输错误。"""

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession | None = None,
        transport: HttpTransport | None = None,
        timeout_sec: float = 10.0,
    ) -> None:
        self._session = session
        self._external_session = session is not None
        self._transport = transport
        self._timeout_sec = timeout_sec

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=64, limit_per_host=16)
            self._session = aiohttp.ClientSession(connector=connector)
            self._external_session = False
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._external_session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes | None = None,
    ) -> HttpResponseData:
        req_headers = dict(headers) if headers else {}
        if self._transport is not None:
            try:
                resp = await self._transport(method, url, req_headers, body)
            except ChannelError:
                raise
            except Exception as exc:
                host = urlsplit(url).netloc or "unknown-host"
                raise ChannelError(f"payment channel: {method} {host} failed: {type(exc).__name__}") from exc
            if len(resp.body) > MAX_CHANNEL_RESPONSE_BYTES:
                raise ChannelError("payment channel: answer is too large")
            return resp

        session = self._get_session()
        timeout = aiohttp.ClientTimeout(total=self._timeout_sec, sock_connect=3.0, sock_read=6.0)
        try:
            async with session.request(
                method,
                url,
                headers=req_headers,
                data=body,
                allow_redirects=False,
                timeout=timeout,
            ) as resp:
                raw = await resp.content.read(MAX_CHANNEL_RESPONSE_BYTES + 1)
                if len(raw) > MAX_CHANNEL_RESPONSE_BYTES:
                    raise ChannelError("payment channel: answer is too large")
                return HttpResponseData(status=resp.status, headers=dict(resp.headers), body=raw)
        except ChannelError:
            raise
        except Exception as exc:
            # 脱敏：不把完整 URL（可能含 secret / access_token / purchaseToken）写入异常文本或日志
            host = urlsplit(url).netloc or "unknown-host"
            raise ChannelError(f"payment channel: {method} {host} failed: {type(exc).__name__}") from exc
