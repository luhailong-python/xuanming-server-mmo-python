"""db（player_store）gRPC 服务适配层与 data_service 落点客户端。

注册两个 gRPC 服务：
- ``playerstore.PlayerStore``（``LoadAndClaim``、``SavePlayer``，强制校验 ``caller="scene"`` 的 HMAC-SHA256 签名）
- ``db``（``Test``，无鉴权，返回空响应）
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

import grpc
from google.protobuf.message import DecodeError

from ..grpc_runtime import sign_service_call, verify_service_call
from .model import (
    MAX_INSTANCE_BYTES,
    ClaimResult,
    SaveResult,
    StorageUnavailableError,
    StoreBackendError,
    validate_claim_request,
)
from .store import PlayerStoreBackend, RouteResolver

logger = logging.getLogger(__name__)

PLAYER_STORE_SERVICE = "playerstore.PlayerStore"
DB_SERVICE = "db"
GET_PLAYER_ROUTE_PATH = "/data_service.DataService/GetPlayerRoute"

SCENE_CALLER = "scene"
ROUTE_CALLER = "db"
DEFAULT_ROUTE_TIMEOUT_SEC = 2.0


def _normalize_secret(secret: str | bytes, name: str) -> bytes:
    raw = (secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)).strip()
    if len(raw) < 32:
        raise ValueError(f"{name} 至少需要 32 字节")
    return raw


class DataServiceRouteResolver:
    """经 ``data_service.DataService/GetPlayerRoute`` 查询玩家落点（``caller="db"``，2s 超时）。"""

    def __init__(
        self,
        protocol,
        call: Callable[[str, bytes], Awaitable[bytes]],
        secret: str | bytes,
        *,
        timeout_sec: float = DEFAULT_ROUTE_TIMEOUT_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        if timeout_sec <= 0:
            raise ValueError("route timeout_sec 必须大于 0")
        self._protocol = protocol
        self._call = call
        self._secret = _normalize_secret(secret, "MMORPG_ROLE_FENCE_SECRET_DB")
        self._timeout_sec = timeout_sec
        self._clock_ms = clock_ms

    async def resolve_storage(self, player_id: int) -> int:
        request = self._protocol.message("data_service.GetPlayerRouteRequest", player_id=player_id)
        sign_service_call(
            GET_PLAYER_ROUTE_PATH,
            request,
            ROUTE_CALLER,
            int(self._clock_ms()),
            self._secret,
        )
        payload = request.SerializeToString(deterministic=True)
        try:
            async with asyncio.timeout(self._timeout_sec):
                raw = await self._call(GET_PLAYER_ROUTE_PATH, payload)
            response = self._protocol.parse("data_service.GetPlayerRouteResponse", raw)
        except asyncio.CancelledError:
            raise
        except (TimeoutError, OSError, DecodeError, grpc.aio.AioRpcError) as exc:
            logger.error("player_store route lookup rpc failed player_id=%d", player_id, exc_info=exc)
            raise StorageUnavailableError("route lookup failed") from exc
        except Exception as exc:
            logger.error("player_store route lookup unexpected error player_id=%d", player_id, exc_info=exc)
            raise StorageUnavailableError("route lookup failed") from exc

        if response.retry:
            logger.warning("player_store route lookup asked retry player_id=%d", player_id)
            raise StorageUnavailableError("route lookup retry")
        if not response.found:
            logger.warning("player_store route not found player_id=%d", player_id)
            raise StorageUnavailableError("player route missing")
        storage_id = int(response.storage_id)
        if storage_id == 0:
            logger.error("player_store route returned storage_id=0 player_id=%d", player_id)
            raise StorageUnavailableError("player route storage_id invalid")
        return storage_id


class DbService:
    """``db`` 节点 gRPC 处理器集合：鉴权、选库与结果拼装，事务细节委托给 ``PlayerStoreBackend``。"""

    def __init__(
        self,
        protocol,
        store: PlayerStoreBackend,
        route_resolver: RouteResolver,
        scene_secret: str | bytes,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._store = store
        self._route_resolver = route_resolver
        self._scene_secret = _normalize_secret(scene_secret, "MMORPG_PLAYER_STORE_SECRET_SCENE")
        self._clock_ms = clock_ms

    def services(self) -> dict:
        return {
            PLAYER_STORE_SERVICE: {
                "LoadAndClaim": self.load_and_claim,
                "SavePlayer": self.save_player,
            },
            DB_SERVICE: {
                "Test": self.test,
            },
        }

    async def _admit_scene(self, context, request, path: str) -> None:
        # 严格 fail-closed：直接 abort PERMISSION_DENIED，绝不落入 serve_grpc 的 FAILED_PRECONDITION 映射。
        if not verify_service_call(path, request, SCENE_CALLER, int(self._clock_ms()), self._scene_secret):
            logger.warning(
                "player_store auth denied path=%s caller=%s",
                path,
                request.auth.caller,
            )
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "player store: caller denied")

    async def test(self, request, context, path):
        """健康探活接口 ``/db/Test``，无需鉴权，返回空 ``TestResponse``。"""
        return self._protocol.message("TestResponse")

    async def load_and_claim(self, request, context, path):
        await self._admit_scene(context, request, path)

        player_id = int(request.player_id)
        owner_epoch = int(request.owner_epoch)
        instance_id = bytes(request.instance_id)
        binary_schema_gen = int(request.binary_schema_gen)

        invalid = validate_claim_request(player_id, owner_epoch, instance_id)
        if invalid != ClaimResult.UNSPECIFIED:
            return self._protocol.message("playerstore.LoadAndClaimResponse", result=int(invalid))

        try:
            storage_id = await self._route_resolver.resolve_storage(player_id)
        except Exception as exc:
            logger.error("player_store resolve storage failed op=claim player_id=%d", player_id, exc_info=exc)
            await context.abort(grpc.StatusCode.UNAVAILABLE, "player store: storage not resolved")

        try:
            outcome = await self._store.load_and_claim(
                storage_id, player_id, owner_epoch, instance_id, binary_schema_gen
            )
        except StorageUnavailableError as exc:
            logger.error(
                "player_store storage unavailable op=claim storage_id=%d player_id=%d",
                storage_id,
                player_id,
                exc_info=exc,
            )
            await context.abort(grpc.StatusCode.UNAVAILABLE, "player store: storage unavailable")
        except (StoreBackendError, Exception) as exc:
            logger.error(
                "player_store backend error op=claim storage_id=%d player_id=%d",
                storage_id,
                player_id,
                exc_info=exc,
            )
            await context.abort(grpc.StatusCode.UNAVAILABLE, "player store: outcome unknown")

        response = self._protocol.message(
            "playerstore.LoadAndClaimResponse",
            result=int(outcome.result),
            storage_id=storage_id,
            owner_epoch=outcome.owner_epoch,
            save_seq=outcome.save_seq,
            schema_gen=outcome.schema_gen,
        )
        for kind, payload in outcome.rows:
            response.rows.add(kind=kind, payload=payload)
        return response

    async def save_player(self, request, context, path):
        await self._admit_scene(context, request, path)

        player_id = int(request.player_id)
        storage_id = int(request.storage_id)
        owner_epoch = int(request.owner_epoch)
        save_seq = int(request.save_seq)
        schema_gen = int(request.schema_gen)
        instance_id = bytes(request.instance_id)
        release = bool(request.release)
        rows = [(int(row.kind), bytes(row.payload)) for row in request.rows]

        if storage_id == 0:
            # storage_id=0 仅允许实体已释放后的空行释放（release_detached），带行或非释放一律直接拒绝。
            if not release or rows:
                return self._protocol.message("playerstore.SavePlayerResponse", result=int(SaveResult.INVALID))
            if (
                player_id == 0
                or owner_epoch == 0
                or save_seq == 0
                or not 1 <= len(instance_id) <= MAX_INSTANCE_BYTES
            ):
                return self._protocol.message("playerstore.SavePlayerResponse", result=int(SaveResult.INVALID))
            try:
                storage_id = await self._route_resolver.resolve_storage(player_id)
            except Exception as exc:
                logger.error("player_store resolve storage failed op=save player_id=%d", player_id, exc_info=exc)
                await context.abort(grpc.StatusCode.UNAVAILABLE, "player store: storage not resolved")

        try:
            outcome = await self._store.save_player(
                storage_id,
                player_id,
                owner_epoch,
                save_seq,
                schema_gen,
                instance_id,
                rows,
                release,
                transaction_logs=list(request.transaction_logs),
                audit_snapshots=list(request.audit_snapshots),
            )
        except StorageUnavailableError as exc:
            logger.error(
                "player_store storage unavailable op=save storage_id=%d player_id=%d save_seq=%d",
                storage_id,
                player_id,
                save_seq,
                exc_info=exc,
            )
            await context.abort(grpc.StatusCode.UNAVAILABLE, "player store: storage unavailable")
        except (StoreBackendError, Exception) as exc:
            logger.error(
                "player_store backend error op=save storage_id=%d player_id=%d save_seq=%d",
                storage_id,
                player_id,
                save_seq,
                exc_info=exc,
            )
            await context.abort(grpc.StatusCode.UNAVAILABLE, "player store: outcome unknown")

        return self._protocol.message(
            "playerstore.SavePlayerResponse",
            result=int(outcome.result),
            row_owner_epoch=outcome.row_owner_epoch,
            row_save_seq=outcome.row_save_seq,
            audit_outbox_written=outcome.audit_outbox_written,
        )
