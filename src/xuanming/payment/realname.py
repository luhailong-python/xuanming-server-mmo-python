"""账号目录实名查询 gRPC 客户端（``GrpcRealNameClient``）。

与 Go ``go/shared/accountdirectory/real_name.go`` 逐项对齐：
- 调用 ``/data_service.DataService/GetAccountRealName``；
- 使用 ``sign_service_call`` 以调用方 ``"payment"`` 及 ``MMORPG_ACCOUNT_DIRECTORY_SECRET_PAYMENT`` 密钥签名；
- 校验返回的非空 ``real_name_pi`` 符合国家实名标识编码规范，异常则 fail-closed 抛错。
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from ..grpc_runtime import GrpcClient, sign_service_call
from ..protocol import Protocol
from .config import MIN_SECRET_BYTES
from .model import birth_date_from_pi

PAYMENT_REALNAME_CALLER = "payment"
PAYMENT_REALNAME_SECRET_ENV = "MMORPG_ACCOUNT_DIRECTORY_SECRET_PAYMENT"
GET_ACCOUNT_REALNAME_PATH = "/data_service.DataService/GetAccountRealName"
DEFAULT_REALNAME_TIMEOUT_SEC = 2.5

GrpcCallFn = Callable[..., Awaitable[bytes]]


class GrpcRealNameClient:
    """调用 ``data_service`` 查询玩家账号实名 ``pi`` 的客户端（实现 ``RealNames`` 协议）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: GrpcCallFn,
        secret: bytes | str,
        *,
        timeout_sec: float = DEFAULT_REALNAME_TIMEOUT_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        grpc_client: GrpcClient | None = None,
    ) -> None:
        if call is None:
            raise ValueError("real-name lookup has no data_service client")
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        if len(secret_bytes) < MIN_SECRET_BYTES:
            raise ValueError(
                f"real-name lookup secret ({PAYMENT_REALNAME_SECRET_ENV}) must be at least {MIN_SECRET_BYTES} bytes"
            )
        if timeout_sec <= 0:
            raise ValueError("real-name lookup timeout_sec must be > 0")
        self._protocol = protocol
        self._call = call
        self._secret = secret_bytes
        self._timeout_sec = timeout_sec
        self._clock_ms = clock_ms
        self._grpc_client = grpc_client

    @classmethod
    def from_endpoint(
        cls,
        protocol: Protocol,
        endpoint: str,
        secret: bytes | str,
        *,
        timeout_sec: float = DEFAULT_REALNAME_TIMEOUT_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> "GrpcRealNameClient":
        """按 ``data_service`` 地址创建独立 gRPC 连接并返回 ``GrpcRealNameClient``。"""
        if not endpoint:
            raise ValueError("real-name lookup requires a non-empty data_service endpoint")
        client = GrpcClient(endpoint, timeout=timeout_sec)
        try:
            return cls(
                protocol,
                client.call,
                secret,
                timeout_sec=timeout_sec,
                clock_ms=clock_ms,
                grpc_client=client,
            )
        except Exception:
            # 构造阶段同步失败时无需等待连接建立，确保不泄漏通道对象
            raise

    async def close(self) -> None:
        if self._grpc_client is not None:
            await self._grpc_client.close()
            self._grpc_client = None

    async def real_name_of(self, player_id: int) -> tuple[str, bool]:
        """查询玩家对应账号的实名标识 ``(pi, found)``。

        - ``found=False``：账号目录中无该角色或已删除；
        - ``pi=""``：账号未在游戏方实名（如纯小游戏账号由平台方实名）；
        - 非空 ``pi`` 若格式非法则按服务不可用抛异常，绝不放行。
        """
        req = self._protocol.message("data_service.GetAccountRealNameRequest", player_id=int(player_id))
        sign_service_call(
            GET_ACCOUNT_REALNAME_PATH,
            req,
            PAYMENT_REALNAME_CALLER,
            self._clock_ms(),
            self._secret,
        )
        raw = await self._call(
            GET_ACCOUNT_REALNAME_PATH,
            req.SerializeToString(deterministic=True),
            timeout=self._timeout_sec,
        )
        resp = self._protocol.parse("data_service.GetAccountRealNameResponse", raw)
        pi = str(resp.real_name_pi)
        if pi and birth_date_from_pi(pi) is None:
            raise RuntimeError("real-name lookup: data_service returned a malformed pi")
        return pi, bool(resp.found)
