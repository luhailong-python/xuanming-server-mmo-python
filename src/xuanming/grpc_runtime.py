import base64
import hashlib
import hmac
import inspect
import os
import re
import secrets
import time

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from google.protobuf.message import Message

from .protocol import Protocol

_SIGNATURE_WINDOW_MS = 300_000

# gRPC 服务端连接轮换与客户端 keepalive 缺省参数（对齐 Go shared/grpckeepalive）
DEFAULT_GRPC_MAX_CONNECTION_AGE_MS = 15 * 60 * 1000
DEFAULT_GRPC_MAX_CONNECTION_AGE_GRACE_MS = 30 * 1000
DEFAULT_GRPC_KEEPALIVE_TIME_MS = 5 * 60 * 1000
DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS = 20 * 1000
DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS = 0


def _resolve_int_option(explicit: int | None, env_key: str, default: int, environ=None) -> int:
    if explicit is not None:
        return int(explicit)
    env = os.environ if environ is None else environ
    raw = env.get(env_key, "").strip()
    if raw:
        return int(raw)
    return default


def grpc_client_options(
    *,
    receive_limit: int = 4 << 20,
    enable_retries: bool | None = None,
    keepalive_time_ms: int | None = None,
    keepalive_timeout_ms: int | None = None,
    keepalive_permit_without_calls: int | bool | None = None,
    environ=None,
) -> tuple[tuple[str, int], ...]:
    """构造带 keepalive 防半开连接参数的 gRPC 客户端 channel options。"""
    ka_time = _resolve_int_option(
        keepalive_time_ms, "MMORPG_GRPC_KEEPALIVE_TIME_MS", DEFAULT_GRPC_KEEPALIVE_TIME_MS, environ
    )
    ka_timeout = _resolve_int_option(
        keepalive_timeout_ms,
        "MMORPG_GRPC_KEEPALIVE_TIMEOUT_MS",
        DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
        environ,
    )
    permit = _resolve_int_option(
        None if keepalive_permit_without_calls is None else int(keepalive_permit_without_calls),
        "MMORPG_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS",
        DEFAULT_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS,
        environ,
    )
    options: list[tuple[str, int]] = [
        ("grpc.max_receive_message_length", int(receive_limit)),
        ("grpc.keepalive_time_ms", ka_time),
        ("grpc.keepalive_timeout_ms", ka_timeout),
        ("grpc.keepalive_permit_without_calls", permit),
    ]
    if enable_retries is not None:
        options.append(("grpc.enable_retries", int(enable_retries)))
    return tuple(options)


def grpc_server_options(
    *,
    max_receive_message_length: int | None = None,
    max_connection_age_ms: int | None = None,
    max_connection_age_grace_ms: int | None = None,
    keepalive_time_ms: int | None = None,
    keepalive_timeout_ms: int | None = None,
    environ=None,
) -> tuple[tuple[str, int], ...]:
    """构造带 MaxConnectionAge 优雅连接轮换与 keepalive 的 gRPC 服务端 options。"""
    max_age = _resolve_int_option(
        max_connection_age_ms,
        "MMORPG_GRPC_MAX_CONNECTION_AGE_MS",
        DEFAULT_GRPC_MAX_CONNECTION_AGE_MS,
        environ,
    )
    max_age_grace = _resolve_int_option(
        max_connection_age_grace_ms,
        "MMORPG_GRPC_MAX_CONNECTION_AGE_GRACE_MS",
        DEFAULT_GRPC_MAX_CONNECTION_AGE_GRACE_MS,
        environ,
    )
    ka_time = _resolve_int_option(
        keepalive_time_ms, "MMORPG_GRPC_KEEPALIVE_TIME_MS", DEFAULT_GRPC_KEEPALIVE_TIME_MS, environ
    )
    ka_timeout = _resolve_int_option(
        keepalive_timeout_ms,
        "MMORPG_GRPC_KEEPALIVE_TIMEOUT_MS",
        DEFAULT_GRPC_KEEPALIVE_TIMEOUT_MS,
        environ,
    )
    options: list[tuple[str, int]] = [
        ("grpc.so_reuseport", 0),
        ("grpc.max_connection_age_ms", max_age),
        ("grpc.max_connection_age_grace_ms", max_age_grace),
        ("grpc.keepalive_time_ms", ka_time),
        ("grpc.keepalive_timeout_ms", ka_timeout),
    ]
    if max_receive_message_length is not None:
        options.append(("grpc.max_receive_message_length", int(max_receive_message_length)))
    return tuple(options)


def service_digest(message: Message) -> str:
    canonical = type(message)()
    canonical.CopyFrom(message)
    canonical.ClearField("auth")
    return hashlib.sha256(canonical.SerializeToString(deterministic=True)).hexdigest()


def _service_signature(path: str, caller: str, timestamp: int, digest: str, secret: bytes) -> str:
    canonical = "\n".join(("v1", path, caller, str(timestamp), digest)).encode("ascii")
    return hmac.digest(secret, canonical, "sha256").hex()


def sign_service_call(path: str, message: Message, caller: str, timestamp: int, secret: bytes) -> None:
    signature = _service_signature(path, caller, timestamp, service_digest(message), secret)
    message.auth.caller = caller
    message.auth.timestamp_ms = timestamp
    message.auth.signature_hex = signature


def verify_service_call(path: str, message: Message, caller: str, timestamp: int, secret: bytes) -> bool:
    auth = message.auth
    sent = auth.timestamp_ms
    if (auth.caller != caller or not 0 < sent < (1 << 63) or abs(timestamp - sent) > _SIGNATURE_WINDOW_MS
            or len(auth.signature_hex) != 64 or not auth.signature_hex.isascii()):
        return False
    expected = _service_signature(path, caller, sent, service_digest(message), secret)
    return hmac.compare_digest(expected, auth.signature_hex)


class GrpcUnavailable(RuntimeError):
    """The RPC was rejected before business handling began."""


def gate_signing_key(environ=None) -> Ed25519PrivateKey:
    """Load the native Gate seed and require its public key in the verifier set."""
    environ = os.environ if environ is None else environ
    seed = environ.get("MMORPG_INTERNAL_AUTH_SIGNING_KEY", "")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", seed):
        raise ValueError("MMORPG_INTERNAL_AUTH_SIGNING_KEY 需要 64 位十六进制种子")
    public_keys = environ.get("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS", "").split(",")
    if not public_keys or any(not re.fullmatch(r"[0-9a-fA-F]{64}", item) for item in public_keys):
        raise ValueError("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS 格式无效")
    keys = [bytes.fromhex(item) for item in public_keys]
    if len(keys) != len(set(keys)):
        raise ValueError("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS 不得包含重复公钥")
    key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed))
    public_key = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    if public_key not in keys:
        raise ValueError("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS 与 Gate 签名私钥不匹配")
    return key


def session_metadata(subject: bytes, method: str, signer: Ed25519PrivateKey | bytes) -> tuple:
    if not method.startswith("/"):
        raise ValueError("调用方认证需要完整 gRPC 方法名")
    if isinstance(signer, bytes):
        signer = Ed25519PrivateKey.from_private_bytes(signer)
    timestamp = str(time.time_ns() // 1_000_000)
    nonce = secrets.token_hex(16)
    canonical = "\n".join((
        "mmorpg-gate-identity/v2", "gate", method,
        hashlib.sha256(subject).hexdigest(), timestamp, nonce,
    )).encode()
    return (
        ("x-session-detail-bin", base64.b64encode(subject)),
        ("x-caller-id", "gate"),
        ("x-caller-ts", timestamp),
        ("x-caller-nonce", nonce),
        ("x-caller-sig", signer.sign(canonical).hex()),
    )


class GrpcClient:
    def __init__(
        self,
        endpoint: str,
        *,
        timeout: float = 5,
        receive_limit: int = 4 << 20,
        enable_retries: bool | None = None,
        keepalive_time_ms: int | None = None,
        keepalive_timeout_ms: int | None = None,
        keepalive_permit_without_calls: int | bool | None = None,
        environ=None,
    ) -> None:
        self.options = grpc_client_options(
            receive_limit=receive_limit,
            enable_retries=enable_retries,
            keepalive_time_ms=keepalive_time_ms,
            keepalive_timeout_ms=keepalive_timeout_ms,
            keepalive_permit_without_calls=keepalive_permit_without_calls,
            environ=environ,
        )
        self.channel = grpc.aio.insecure_channel(endpoint, options=self.options)
        self.timeout = timeout

    def request(self, path: str, data: bytes, *, metadata: tuple = (), timeout: float | None = None):
        return self.channel.unary_unary(path)(data, metadata=metadata, timeout=self.timeout if timeout is None else timeout)

    async def call(self, path: str, data: bytes, *, metadata: tuple = (), timeout: float | None = None) -> bytes:
        return await self.request(path, data, metadata=metadata, timeout=timeout)

    async def close(self) -> None:
        await self.channel.close()


async def serve_grpc(
    protocol: Protocol,
    address: str,
    services: dict,
    *,
    kill_switch=None,
    pass_context=False,
    max_receive_message_length: int | None = None,
    max_connection_age_ms: int | None = None,
    max_connection_age_grace_ms: int | None = None,
    keepalive_time_ms: int | None = None,
    keepalive_timeout_ms: int | None = None,
    environ=None,
):
    """Serve unary handlers; with ``pass_context`` they are called as ``handler(request, context, path)``."""
    options = grpc_server_options(
        max_receive_message_length=max_receive_message_length,
        max_connection_age_ms=max_connection_age_ms,
        max_connection_age_grace_ms=max_connection_age_grace_ms,
        keepalive_time_ms=keepalive_time_ms,
        keepalive_timeout_ms=keepalive_timeout_ms,
        environ=environ,
    )
    server = grpc.aio.server(options=options)
    for service_name, handlers in services.items():
        descriptor = protocol.pool.FindServiceByName(service_name)
        methods = {}
        for name, handler in handlers.items():
            method = descriptor.methods_by_name[name]
            methods[name] = grpc.unary_unary_rpc_method_handler(
                _grpc_handler(handler, kill_switch=kill_switch, service=service_name, method=name,
                              pass_context=pass_context),
                request_deserializer=protocol.message_class(method.input_type.full_name).FromString,
                response_serializer=lambda message: message.SerializeToString(),
            )
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(service_name, methods),))
    port = server.add_insecure_port(address)
    if not port:
        raise OSError(f"无法监听 gRPC 地址 {address}")
    await server.start()
    return server, port


def _grpc_handler(handler, *, kill_switch=None, service="", method="", pass_context=False):
    async def invoke(request, context):
        try:
            rule = kill_switch.check(service, method) if kill_switch is not None else None
            if rule is not None:
                code = next((item for item in grpc.StatusCode if item.value[0] == rule.grpc_code),
                            grpc.StatusCode.UNAVAILABLE)
                reason = f", reason: {rule.reason}" if rule.reason else ""
                await context.abort(code, f"killswitch: method /{service}/{method} is disabled{reason}")
            response = handler(request, context, f"/{service}/{method}") if pass_context else handler(request)
            if inspect.isawaitable(response):
                response = await response
            return response
        except GrpcUnavailable as error:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(error))
        except (ValueError, PermissionError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))

    return invoke
