"""玩家存储条件工厂：按运行模式装配本地 ProtoJSON 或集群远程 PlayerStore。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path

from ..grpc_runtime import GrpcClient
from ..settings import secret as resolve_env_secret
from .player_store import DataServicePlayerStore, JsonPlayerStore

_PLAYER_STORE_SECRET_ENV = "MMORPG_PLAYER_STORE_SECRET_SCENE"
_PLAYER_STORE_RPC_TIMEOUT_SECONDS = 5
_PLAYER_STORE_MAX_RECEIVE_BYTES = 8 << 20
_MIN_SECRET_BYTES = 32


def create_player_store(
    settings,
    protocol,
    *,
    local: bool,
    grpc_client_factory: Callable[..., object] | None = None,
    secret_resolver: Callable[[str], bytes | str] | None = None,
) -> tuple[JsonPlayerStore | DataServicePlayerStore, Callable[[], Awaitable[None]] | None]:
    """根据 ``local`` 标志条件创建玩家存储与可选关闭回调。

    - ``local=True``：返回启用 ProtoJSON 直转的本地 ``JsonPlayerStore``，关闭回调为 ``None``。
    - ``local=False``：集群模式严格 fail-fast 校验端点与签名密钥，严禁静默降级到本地 JSON。
    """
    if local:
        legacy_sqlite = getattr(settings, "database", None)
        path = getattr(settings, "local_player_store", None)
        if path is None:
            if legacy_sqlite is None:
                raise ValueError("本地模式需要配置 local_player_store 或 database 路径")
            path = Path(legacy_sqlite).with_name("players.json")
        return (
            JsonPlayerStore(path, legacy_sqlite=legacy_sqlite, protocol=protocol),
            None,
        )

    endpoint = str(getattr(settings, "player_store", "") or "").strip()
    if not endpoint:
        raise ValueError("集群模式缺少 PlayerStore 的 gRPC 地址（严禁回退本地 JSON 存档）")

    raw_secret = (
        secret_resolver(_PLAYER_STORE_SECRET_ENV)
        if secret_resolver is not None
        else resolve_env_secret(_PLAYER_STORE_SECRET_ENV)
    )
    if raw_secret is None:
        store_secret = b""
    elif isinstance(raw_secret, str):
        store_secret = raw_secret.encode("utf-8").strip()
    else:
        store_secret = bytes(raw_secret).strip()
    if len(store_secret) < _MIN_SECRET_BYTES:
        raise ValueError(f"{_PLAYER_STORE_SECRET_ENV} 至少需要 {_MIN_SECRET_BYTES} 字节（严禁回退本地 JSON 存档）")

    client_factory = grpc_client_factory or GrpcClient
    client = client_factory(
        endpoint,
        timeout=_PLAYER_STORE_RPC_TIMEOUT_SECONDS,
        receive_limit=_PLAYER_STORE_MAX_RECEIVE_BYTES,
    )
    store = DataServicePlayerStore(protocol, client.call, store_secret)
    return store, client.close
