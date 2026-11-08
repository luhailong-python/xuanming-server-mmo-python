import base64
import hashlib
import hmac
import inspect
import secrets
import time

import grpc

from .protocol import Protocol


class GrpcUnavailable(RuntimeError):
    """The RPC was rejected before business handling began."""


def session_metadata(subject: bytes, method: str, secret: bytes) -> tuple:
    timestamp = str(time.time_ns() // 1_000_000)
    nonce = secrets.token_hex(16)
    canonical = "\n".join((
        "mmorpg-login-caller-auth/v1", "gate", method,
        hashlib.sha256(subject).hexdigest(), timestamp, nonce,
    )).encode()
    return (
        ("x-session-detail-bin", base64.b64encode(subject)),
        ("x-caller-id", "gate"),
        ("x-caller-ts", timestamp),
        ("x-caller-nonce", nonce),
        ("x-caller-sig", hmac.new(secret, canonical, "sha256").hexdigest()),
    )


class GrpcClient:
    def __init__(self, endpoint: str, *, timeout: float = 5, receive_limit: int = 4 << 20,
                 enable_retries: bool | None = None) -> None:
        options = [("grpc.max_receive_message_length", receive_limit)]
        if enable_retries is not None:
            options.append(("grpc.enable_retries", int(enable_retries)))
        self.channel = grpc.aio.insecure_channel(endpoint, options=options)
        self.timeout = timeout

    def request(self, path: str, data: bytes, *, metadata: tuple = ()):
        return self.channel.unary_unary(path)(data, metadata=metadata, timeout=self.timeout)

    async def call(self, path: str, data: bytes, *, metadata: tuple = ()) -> bytes:
        return await self.request(path, data, metadata=metadata)

    async def close(self) -> None:
        await self.channel.close()


async def serve_grpc(protocol: Protocol, address: str, services: dict):
    server = grpc.aio.server(options=(("grpc.so_reuseport", 0),))
    for service_name, handlers in services.items():
        descriptor = protocol.pool.FindServiceByName(service_name)
        methods = {}
        for name, handler in handlers.items():
            method = descriptor.methods_by_name[name]
            methods[name] = grpc.unary_unary_rpc_method_handler(
                _grpc_handler(handler),
                request_deserializer=protocol.message_class(method.input_type.full_name).FromString,
                response_serializer=lambda message: message.SerializeToString(),
            )
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(service_name, methods),))
    port = server.add_insecure_port(address)
    if not port:
        raise OSError(f"无法监听 gRPC 地址 {address}")
    await server.start()
    return server, port


def _grpc_handler(handler):
    async def invoke(request, context):
        try:
            response = handler(request)
            if inspect.isawaitable(response):
                response = await response
            return response
        except GrpcUnavailable as error:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(error))
        except (ValueError, PermissionError) as error:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))

    return invoke
