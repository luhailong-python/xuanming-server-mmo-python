"""死持有者存档写权复核与释放（严格对齐 Go dead_holder_release.go 三步协议）。

三步顺序不可颠倒：
1. 调用 DataService.ReadPlayerStoreHolder 读出 (owner_epoch, holder_instance, claim_seq)；
   若无行或 owner_epoch > location_epoch，说明无需释放，直接放行。
2. 若 holder_instance 非空，对注册表执行新鲜读取复核其不在册；
   仍在册则拒绝释放（FAILED_PRECONDITION），读注册表失败则 fail-closed（UNAVAILABLE）。
3. 调用 DataService.ReleaseDeadPlayerStoreHolder 携带 seen 三列 CAS 释放写权行；
   若期间被重新认领（FAILED_PRECONDITION），保留位置待下次重试。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Protocol as TypingProtocol

import grpc

from .errors import SceneManagerError

logger = logging.getLogger(__name__)

_HOLDER_RELEASE_PENDING_PREFIX = "holder_release_pending"


@dataclass(frozen=True, slots=True)
class PlayerStoreHolderInfo:
    """DataService.ReadPlayerStoreHolder 返回的写权行三列快照。"""

    owner_epoch: int
    holder_instance: bytes
    claim_seq: int


class PlayerStoreHolderClient(TypingProtocol):
    """DataService 存档写权判死读取与释放最小客户端接口。"""

    async def read_player_store_holder(self, player_id: int) -> PlayerStoreHolderInfo | None: ...

    async def release_dead_player_store_holder(
        self,
        player_id: int,
        owner_epoch: int,
        *,
        seen: PlayerStoreHolderInfo | None = None,
    ) -> None: ...


class DeadHolderReleaseOutcome(str, Enum):
    """单次死持有者写权释放的结果分类。"""

    NO_ROW = "no_row"
    NEWER_OWNER = "newer_owner"
    RELEASED = "released"
    HOLDER_REGISTERED = "holder_registered"
    ROW_CHANGED = "row_changed"
    REGISTRY_UNAVAILABLE = "registry_unavailable"
    ERROR = "error"


class DeadHolderReleaseBlockedError(SceneManagerError):
    """死持有者写权当前不能释放（在册、行已变更或依赖不可用），调用方须保留 location 稍后重试。"""

    def __init__(
        self,
        outcome: DeadHolderReleaseOutcome,
        grpc_code: grpc.StatusCode,
        detail: str,
    ) -> None:
        super().__init__(detail)
        self.outcome = outcome
        self.grpc_code = grpc_code
        self.detail = detail

    @property
    def keeps_location(self) -> bool:
        """FAILED_PRECONDITION 为正常等待（持有实例在册或写权行已变），其余为故障。"""
        return self.grpc_code == grpc.StatusCode.FAILED_PRECONDITION


class DeadHolderReleaser:
    """死持有者存档写权释放器：先读行、再复核注册表、最后带 seen 三列 CAS 释放。"""

    def __init__(self, holder_client: PlayerStoreHolderClient, node_registry, *, zone_id: int) -> None:
        self.holder_client = holder_client
        self.node_registry = node_registry
        self.zone_id = zone_id

    async def _fresh_registered_instances(self, zone_id: int) -> set[str]:
        fresh = getattr(self.node_registry, "fresh_scene_instance_uuids", None)
        if callable(fresh):
            return set(await fresh(zone_id))
        nodes = await self.node_registry.list_scene_nodes(zone_id)
        return {str(n.node_uuid) for n in nodes if getattr(n, "node_uuid", "")}

    async def release_dead_holder(
        self,
        player_id: int,
        location_epoch: int,
        *,
        zone_id: int | None = None,
    ) -> DeadHolderReleaseOutcome:
        if location_epoch <= 0:
            return DeadHolderReleaseOutcome.NO_ROW
        target_zone = self.zone_id if zone_id is None or zone_id <= 0 else zone_id

        # 1. 读取写权行三列 (owner_epoch, holder_instance, claim_seq)
        try:
            seen = await self.holder_client.read_player_store_holder(player_id)
        except Exception as exc:
            logger.error(
                "read player store holder failed player=%d location_epoch=%d error=%s",
                player_id, location_epoch, exc,
            )
            raise DeadHolderReleaseBlockedError(
                DeadHolderReleaseOutcome.ERROR,
                grpc.StatusCode.UNAVAILABLE,
                f"read player store holder failed for player {player_id}: {exc}",
            ) from exc

        if seen is None:
            return DeadHolderReleaseOutcome.NO_ROW
        if seen.owner_epoch > location_epoch:
            return DeadHolderReleaseOutcome.NEWER_OWNER

        # 2. 若写权行仍登记了持有实例 UUID，做一次新鲜注册表复核（防镜像滞后误踢重注册活进程）
        if seen.holder_instance:
            holder_uuid = seen.holder_instance.decode("utf-8", errors="replace")
            try:
                live_instances = await self._fresh_registered_instances(target_zone)
            except Exception as exc:
                logger.error(
                    "fresh registry check failed player=%d holder=%s zone=%d error=%s",
                    player_id, holder_uuid, target_zone, exc,
                )
                raise DeadHolderReleaseBlockedError(
                    DeadHolderReleaseOutcome.REGISTRY_UNAVAILABLE,
                    grpc.StatusCode.UNAVAILABLE,
                    f"re-checking holder instance {holder_uuid} of player {player_id} failed: {exc}",
                ) from exc
            if holder_uuid in live_instances:
                detail = (
                    f"{_HOLDER_RELEASE_PENDING_PREFIX}: player={player_id} holder instance {holder_uuid} "
                    f"is registered in etcd; not releasing its player store claim "
                    f"(owner_epoch={seen.owner_epoch} location_epoch={location_epoch})"
                )
                logger.info("dead holder release kept location: %s", detail)
                raise DeadHolderReleaseBlockedError(
                    DeadHolderReleaseOutcome.HOLDER_REGISTERED,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    detail,
                )

        # 3. 带 seen 三列条件释放写权行（期间若被重新认领则返回 FAILED_PRECONDITION）
        try:
            await self.holder_client.release_dead_player_store_holder(player_id, location_epoch, seen=seen)
        except grpc.aio.AioRpcError as exc:
            if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                detail = exc.details() or f"player {player_id} store holder row changed before release"
                logger.info("dead holder release row changed player=%d epoch=%d detail=%s", player_id, location_epoch, detail)
                raise DeadHolderReleaseBlockedError(
                    DeadHolderReleaseOutcome.ROW_CHANGED,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    detail,
                ) from exc
            logger.error(
                "release dead player store holder rpc failed player=%d epoch=%d code=%s error=%s",
                player_id, location_epoch, exc.code().name, exc.details(),
            )
            raise DeadHolderReleaseBlockedError(
                DeadHolderReleaseOutcome.ERROR,
                grpc.StatusCode.UNAVAILABLE,
                f"release dead player store holder failed for player {player_id}: {exc.details() or exc}",
            ) from exc
        except Exception as exc:
            logger.error(
                "release dead player store holder failed player=%d epoch=%d error=%s",
                player_id, location_epoch, exc,
            )
            raise DeadHolderReleaseBlockedError(
                DeadHolderReleaseOutcome.ERROR,
                grpc.StatusCode.UNAVAILABLE,
                f"release dead player store holder failed for player {player_id}: {exc}",
            ) from exc

        logger.info(
            "dead player store holder released player=%d location_epoch=%d prev_holder=%s",
            player_id, location_epoch, seen.holder_instance.decode("utf-8", errors="replace"),
        )
        return DeadHolderReleaseOutcome.RELEASED
