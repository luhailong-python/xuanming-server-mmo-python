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
    def __init__(self, endpoint: str, *, timeout: float = 5, receive_limit: int = 4 << 20,
                 enable_retries: bool | None = None) -> None:
        options = [("grpc.max_receive_message_length", receive_limit)]
        if enable_retries is not None:
            options.append(("grpc.enable_retries", int(enable_retries)))
        self.channel = grpc.aio.insecure_channel(endpoint, options=options)
        self.timeout = timeout

    def request(self, path: str, data: bytes, *, metadata: tuple = (), timeout: float | None = None):
        return self.channel.unary_unary(path)(data, metadata=metadata, timeout=self.timeout if timeout is None else timeout)

    async def call(self, path: str, data: bytes, *, metadata: tuple = (), timeout: float | None = None) -> bytes:
        return await self.request(path, data, metadata=metadata, timeout=timeout)

    async def close(self) -> None:
        await self.channel.close()


async def serve_grpc(protocol: Protocol, address: str, services: dict, *, kill_switch=None, pass_context=False):
    """Serve unary handlers; with ``pass_context`` they are called as ``handler(request, context, path)``."""
    server = grpc.aio.server(options=(("grpc.so_reuseport", 0),))
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
