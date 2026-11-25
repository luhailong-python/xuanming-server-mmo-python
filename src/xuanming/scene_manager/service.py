"""scene_manager 核心服务与出站适配器（DataService / SceneNodeGrpc / Kafka / Etcd）。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import random
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol as TypingProtocol

import grpc
from google.protobuf.message import DecodeError

from ..cluster import command_route
from ..grpc_runtime import GrpcClient, sign_service_call
from ..protocol import Protocol
from . import config, errors, keys
from .dead_holder import (
    DeadHolderReleaseBlockedError,
    DeadHolderReleaseOutcome,
    DeadHolderReleaser,
    PlayerStoreHolderInfo,
)
from .owner_authority import OwnerAuthorityBridge, OwnerAuthorityStore
from .store import (
    DeadNodeTaskFence,
    PlayerPlacementStore,
    RegistrationOutcome,
    SceneControlStore,
    SceneIdGenerator,
    SceneTeardownJob,
    WorldChannelCandidate,
)

logger = logging.getLogger(__name__)

SERVICE_NAME = "scene_manager.SceneManager"
_LOOKUP_ROLE_PATH = "/data_service.DataService/LookupAccountRole"
_GET_HOME_ZONE_PATH = "/data_service.DataService/GetPlayerHomeZone"
_READ_PLAYER_STORE_HOLDER_PATH = "/data_service.DataService/ReadPlayerStoreHolder"
_RELEASE_DEAD_PLAYER_STORE_HOLDER_PATH = "/data_service.DataService/ReleaseDeadPlayerStoreHolder"
_SCENE_CREATE_PATH = "/scene_node.SceneNodeGrpc/CreateScene"
_SCENE_DESTROY_PATH = "/scene_node.SceneNodeGrpc/DestroyScene"
_SCENE_RELEASE_PATH = "/scene_node.SceneNodeGrpc/ReleasePlayer"

_DEDUPE_DONE_PREFIX = "done:"
_DEDUPE_PENDING_PREFIX = "pending:"
_UNBOUND_SESSION_ID = 0xFFFFFFFF
_HOME_ZONE_UNMAPPED_ERR_TEXT = "no home zone mapping"
_HOLDER_RELEASE_PENDING_PREFIX = "holder_release_pending"


# ==============================================================================
# 一、出站依赖 Protocol 与生产适配器
# ==============================================================================

@dataclass(frozen=True, slots=True)
class AccountRoleInfo:
    """DataService.LookupAccountRole 投影，只保留调度关心的字段。"""

    player_id: int
    account: str
    owner_revision: int
    trade_listing_id: int = 0


class HomeZoneNotFoundError(LookupError):
    """玩家尚未在 player_route 中登记归属 zone。"""


class DataServiceClient(TypingProtocol):
    async def lookup_account_role(self, player_id: int) -> AccountRoleInfo: ...
    async def get_player_home_zone(self, player_id: int) -> int: ...


class GrpcDataServiceClient:
    """DataService gRPC 客户端：LookupAccountRole 强制带 scene_manager HMAC 签名，GetPlayerHomeZone 不签名。"""

    def __init__(
        self,
        protocol: Protocol,
        client: GrpcClient,
        secret: bytes,
        *,
        role_timeout_sec: float = config.DEFAULT_ROLE_DIRECTORY_TIMEOUT_SEC,
        home_zone_timeout_sec: float = config.DEFAULT_HOME_ZONE_TIMEOUT_SEC,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if len(secret) < 32:
            raise ValueError(f"{config.ROLE_DIRECTORY_SECRET_ENV} 至少需要 32 字节")
        self.protocol = protocol
        self.client = client
        self.secret = secret
        self.role_timeout_sec = role_timeout_sec
        self.home_zone_timeout_sec = home_zone_timeout_sec
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    async def lookup_account_role(self, player_id: int) -> AccountRoleInfo:
        req = self.protocol.message("data_service.LookupAccountRoleRequest", player_id=player_id)
        sign_service_call(_LOOKUP_ROLE_PATH, req, "scene_manager", self._clock_ms(), self.secret)
        raw = await self.client.call(_LOOKUP_ROLE_PATH, req.SerializeToString(), timeout=self.role_timeout_sec)
        resp = self.protocol.parse("data_service.AccountRoleOwnership", raw)
        role_pid = resp.role.player_id if resp.HasField("role") else 0
        return AccountRoleInfo(
            player_id=role_pid,
            account=resp.account,
            owner_revision=resp.owner_revision,
            trade_listing_id=resp.trade_listing_id,
        )

    async def get_player_home_zone(self, player_id: int) -> int:
        req = self.protocol.message("data_service.GetPlayerHomeZoneRequest", player_id=player_id)
        try:
            raw = await self.client.call(_GET_HOME_ZONE_PATH, req.SerializeToString(), timeout=self.home_zone_timeout_sec)
        except grpc.aio.AioRpcError as exc:
            detail = exc.details() or ""
            if exc.code() == grpc.StatusCode.NOT_FOUND or _HOME_ZONE_UNMAPPED_ERR_TEXT in detail:
                raise HomeZoneNotFoundError(detail or f"no home zone mapping for player {player_id}") from exc
            raise
        resp = self.protocol.parse("data_service.GetPlayerHomeZoneResponse", raw)
        return int(resp.home_zone_id)

    async def read_player_store_holder(self, player_id: int) -> PlayerStoreHolderInfo | None:
        req = self.protocol.message("data_service.ReadPlayerStoreHolderRequest", player_id=player_id)
        sign_service_call(_READ_PLAYER_STORE_HOLDER_PATH, req, "scene_manager", self._clock_ms(), self.secret)
        raw = await self.client.call(_READ_PLAYER_STORE_HOLDER_PATH, req.SerializeToString(), timeout=self.role_timeout_sec)
        resp = self.protocol.parse("data_service.ReadPlayerStoreHolderResponse", raw)
        if not resp.found or not resp.HasField("holder"):
            return None
        return PlayerStoreHolderInfo(
            owner_epoch=int(resp.holder.owner_epoch),
            holder_instance=bytes(resp.holder.holder_instance),
            claim_seq=int(resp.holder.claim_seq),
        )

    async def release_dead_player_store_holder(
        self,
        player_id: int,
        owner_epoch: int,
        *,
        seen: PlayerStoreHolderInfo | None = None,
    ) -> None:
        req = self.protocol.message(
            "data_service.ReleaseDeadPlayerStoreHolderRequest",
            player_id=player_id,
            owner_epoch=owner_epoch,
        )
        if seen is not None:
            req.seen.owner_epoch = int(seen.owner_epoch)
            req.seen.holder_instance = bytes(seen.holder_instance)
            req.seen.claim_seq = int(seen.claim_seq)
        sign_service_call(_RELEASE_DEAD_PLAYER_STORE_HOLDER_PATH, req, "scene_manager", self._clock_ms(), self.secret)
        await self.client.call(
            _RELEASE_DEAD_PLAYER_STORE_HOLDER_PATH, req.SerializeToString(), timeout=self.role_timeout_sec,
        )


class NodeRegistry(TypingProtocol):
    async def list_scene_nodes(self, zone_id: int) -> list: ...
    async def list_gate_nodes(self, zone_id: int) -> list: ...
    async def resolve_scene_grpc_endpoint(self, zone_id: int, node_id: str) -> str: ...
    async def get_unique_scene_instance_uuid(self, zone_id: int, node_id: str) -> str | None: ...
    def is_node_registered(self, zone_id: int, node_id: str) -> bool: ...
    def is_node_identity_ambiguous(self, zone_id: int, node_id: str) -> bool: ...
    def is_zone_gone(self, zone_id: int) -> bool: ...
    def is_node_draining(self, zone_id: int, node_id: str) -> bool: ...
    def is_synced(self) -> bool: ...
    def get_gate_ws_url(self, zone_id: int, gate_node_id: int) -> str: ...


def _is_admission_closed(info) -> bool:
    admission = getattr(info, "admission", 0)
    return admission in (2, "CLOSED", "NODE_ADMISSION_CLOSED")


class EtcdNodeRegistry:
    """基于 EtcdDirectory 的 Scene/Gate 节点发现缓存；仅拨号 grpc_endpoint，永不回退 TCP endpoint。"""

    def __init__(self, directory, *, own_zone_id: int) -> None:
        self.directory = directory
        self.own_zone_id = own_zone_id
        self._scene_nodes: dict[int, list] = {}
        self._synced = False
        self._ws_urls: dict[tuple[int, int], str] = {}

    async def sync_zone(self, zone_id: int | None = None) -> list:
        target = self.own_zone_id if zone_id is None else zone_id
        nodes = await self.directory.candidates(config.SCENE_NODE_TYPE, target)
        self._scene_nodes[target] = list(nodes)
        if target == self.own_zone_id:
            self._synced = True
        return self._scene_nodes[target]

    async def list_scene_nodes(self, zone_id: int) -> list:
        if zone_id == self.own_zone_id and self._synced:
            return list(self._scene_nodes.get(zone_id, ()))
        return list(await self.directory.candidates(config.SCENE_NODE_TYPE, zone_id))

    async def fresh_scene_instance_uuids(self, zone_id: int) -> set[str]:
        """对 etcd 执行一次新鲜读取，返回目标 zone 当前在册 Scene 节点的 node_uuid 集合。"""
        nodes = await self.directory.candidates(config.SCENE_NODE_TYPE, zone_id)
        return {str(n.node_uuid) for n in nodes if getattr(n, "node_uuid", "")}

    async def list_gate_nodes(self, zone_id: int) -> list:
        return list(await self.directory.candidates(config.GATE_NODE_TYPE, zone_id))

    async def resolve_scene_grpc_endpoint(self, zone_id: int, node_id: str) -> str:
        nodes = await self.list_scene_nodes(zone_id)
        matches = [n for n in nodes if str(n.node_id) == node_id]
        if not matches and zone_id == self.own_zone_id:
            nodes = await self.sync_zone(zone_id)
            matches = [n for n in nodes if str(n.node_id) == node_id]
        if not matches:
            raise LookupError(f"scene node {node_id} not found in zone {zone_id}")
        if len(matches) > 1:
            raise RuntimeError(f"ambiguous scene node identity zone={zone_id} node={node_id} count={len(matches)}")
        info = matches[0]
        # 严禁回退到 TCP endpoint（HTTP/2 preface 会破坏原始 protobuf 协议）
        if not info.HasField("grpc_endpoint") or not info.grpc_endpoint.ip or info.grpc_endpoint.port <= 0:
            raise RuntimeError(f"scene node {node_id} has no grpc_endpoint")
        return f"{info.grpc_endpoint.ip}:{info.grpc_endpoint.port}"

    async def get_unique_scene_instance_uuid(self, zone_id: int, node_id: str) -> str | None:
        nodes = await self.list_scene_nodes(zone_id)
        matches = [n for n in nodes if str(n.node_id) == node_id]
        if len(matches) != 1:
            return None
        return matches[0].node_uuid or None

    def is_node_registered(self, zone_id: int, node_id: str) -> bool:
        return any(str(n.node_id) == node_id for n in self._scene_nodes.get(zone_id, ()))

    def is_node_identity_ambiguous(self, zone_id: int, node_id: str) -> bool:
        return sum(1 for n in self._scene_nodes.get(zone_id, ()) if str(n.node_id) == node_id) > 1

    def is_zone_gone(self, zone_id: int) -> bool:
        return self._synced and zone_id in self._scene_nodes and len(self._scene_nodes[zone_id]) == 0

    def is_node_draining(self, zone_id: int, node_id: str) -> bool:
        return any(str(n.node_id) == node_id and _is_admission_closed(n) for n in self._scene_nodes.get(zone_id, ()))

    def is_synced(self) -> bool:
        return self._synced

    def get_gate_ws_url(self, zone_id: int, gate_node_id: int) -> str:
        return self._ws_urls.get((zone_id, gate_node_id), "")


class SceneNodeClient(TypingProtocol):
    async def create_scene(
        self, zone_id: int, node_id: str, *, config_id: int, scene_id: int,
        mirror_config_id: int = 0, creator_ids: Sequence[int] = (), placement_generation: int = 0,
    ) -> None: ...
    async def destroy_scene(
        self, zone_id: int, node_id: str, *, scene_id: int, placement_generation: int = 0,
    ) -> None: ...
    async def release_player(
        self, zone_id: int, node_id: str, *, player_id: int,
        target_scene_id: int = 0, target_node_id: str = "", expected_owner_epoch: int = 0,
        session_ended: bool = False, covered_session_id: int = 0,
        covered_login_generation: int = 0, keep_held: bool = False,
    ) -> None: ...


class GrpcSceneNodeClient:
    """SceneNodeGrpc 控制面客户端（仅拨号 grpc_endpoint，缓存按 (zone, node_id, endpoint) 连接）。"""

    def __init__(
        self,
        protocol: Protocol,
        registry: NodeRegistry,
        *,
        own_zone_id: int,
        timeout_sec: float = config.DEFAULT_SCENE_NODE_RPC_TIMEOUT_SEC,
    ) -> None:
        self.protocol = protocol
        self.registry = registry
        self.own_zone_id = own_zone_id
        self.timeout_sec = timeout_sec
        self._clients: dict[tuple[int, str], tuple[str, GrpcClient]] = {}

    async def _client_for(self, zone_id: int, node_id: str) -> GrpcClient:
        endpoint = await self.registry.resolve_scene_grpc_endpoint(zone_id, node_id)
        key = (zone_id, node_id)
        cached = self._clients.get(key)
        if cached is not None:
            cached_ep, client = cached
            if cached_ep == endpoint:
                return client
            await client.close()
        client = GrpcClient(endpoint, timeout=self.timeout_sec)
        self._clients[key] = (endpoint, client)
        return client

    async def create_scene(
        self, zone_id: int, node_id: str, *, config_id: int, scene_id: int,
        mirror_config_id: int = 0, creator_ids: Sequence[int] = (), placement_generation: int = 0,
    ) -> None:
        if scene_id <= 0:
            raise ValueError("CreateScene refused: scene_id must be nonzero")
        client = await self._client_for(zone_id, node_id)
        req = self.protocol.message(
            "CreateSceneRequest",
            config_id=config_id,
            scene_id=scene_id,
            mirror_config_id=mirror_config_id,
            creator_ids=list(creator_ids),
            placement_generation=placement_generation,
            zone_id=self.own_zone_id,
        )
        raw = await client.call(_SCENE_CREATE_PATH, req.SerializeToString(), timeout=self.timeout_sec)
        resp = self.protocol.parse("CreateSceneResponse", raw)
        if not resp.HasField("scene_info") or resp.scene_info.scene_id != scene_id:
            got = resp.scene_info.scene_id if resp.HasField("scene_info") else 0
            raise RuntimeError(f"CreateScene on node {node_id} did not confirm scene {scene_id} (got {got})")

    async def destroy_scene(
        self, zone_id: int, node_id: str, *, scene_id: int, placement_generation: int = 0,
    ) -> None:
        client = await self._client_for(zone_id, node_id)
        req = self.protocol.message(
            "DestroySceneRequest",
            scene_id=scene_id,
            placement_generation=placement_generation,
        )
        await client.call(_SCENE_DESTROY_PATH, req.SerializeToString(), timeout=self.timeout_sec)

    async def release_player(
        self, zone_id: int, node_id: str, *, player_id: int,
        target_scene_id: int = 0, target_node_id: str = "", expected_owner_epoch: int = 0,
        session_ended: bool = False, covered_session_id: int = 0,
        covered_login_generation: int = 0, keep_held: bool = False,
    ) -> None:
        client = await self._client_for(zone_id, node_id)
        req = self.protocol.message(
            "scene_node.ReleasePlayerRequest",
            player_id=player_id,
            target_scene_id=target_scene_id,
            target_node_id=target_node_id,
            expected_owner_epoch=expected_owner_epoch,
            session_ended=session_ended,
            covered_session_id=covered_session_id,
            covered_login_generation=covered_login_generation,
            keep_held=keep_held,
        )
        await client.call(_SCENE_RELEASE_PATH, req.SerializeToString(), timeout=min(1.0, self.timeout_sec))

    async def close(self) -> None:
        for _, client in self._clients.values():
            await client.close()
        self._clients.clear()


class CommandPublisher(TypingProtocol):
    async def publish_gate_command(self, gate_node_id: int, player_id: int, command) -> None: ...
    async def publish_scene_command(self, scene_node_id: int, player_id: int, command) -> None: ...


class KafkaCommandPublisher:
    """同步 acks=all、显式分区 ``node_id % partitions`` 的 Kafka 命令发布器。"""

    def __init__(
        self,
        brokers: str,
        *,
        partitions: int = 256,
        generation: int = 1,
        write_timeout_sec: float = config.DEFAULT_KAFKA_WRITE_TIMEOUT_SEC,
        producer=None,
    ) -> None:
        self.brokers = brokers
        self.partitions = partitions
        self.generation = generation
        self.write_timeout_sec = write_timeout_sec
        self.producer = producer
        self._owns_producer = producer is None

    async def start(self) -> None:
        if self.producer is not None:
            return
        from aiokafka import AIOKafkaProducer

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.brokers,
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=max(1000, int(self.write_timeout_sec * 1000)),
        )
        await self.producer.start()

    async def close(self) -> None:
        if self._owns_producer and self.producer is not None:
            await self.producer.stop()
            self.producer = None

    async def publish_gate_command(self, gate_node_id: int, player_id: int, command) -> None:
        if self.producer is None:
            raise RuntimeError("KafkaCommandPublisher is not started")
        topic, partition = command_route("gate", gate_node_id, generation=self.generation, partitions=self.partitions)
        await self.producer.send_and_wait(
            topic,
            command.SerializeToString(),
            key=str(player_id).encode("ascii"),
            partition=partition,
        )

    async def publish_scene_command(self, scene_node_id: int, player_id: int, command) -> None:
        if self.producer is None:
            raise RuntimeError("KafkaCommandPublisher is not started")
        topic, partition = command_route("scene", scene_node_id, generation=self.generation, partitions=self.partitions)
        await self.producer.send_and_wait(
            topic,
            command.SerializeToString(),
            key=str(player_id).encode("ascii"),
            partition=partition,
        )


# ==============================================================================
# 二、纯函数状态机判定表（与 Go placement_identity / leave_release / owner_epoch / gate_redirect 对齐）
# ==============================================================================

def valid_session(session_id: int) -> bool:
    return session_id != 0 and session_id != _UNBOUND_SESSION_ID


@dataclass(frozen=True, slots=True)
class PlacementIdentity:
    session_id: int = 0
    login_generation: int = 0

    def absent(self) -> bool:
        return self.session_id == 0 and self.login_generation == 0


class PlacementVerdict(str, Enum):
    FRESH = "fresh"
    SAME_SESSION = "same_session"
    LEGACY = "legacy"
    NEWER = "newer"
    STALE = "stale"
    CONFLICT = "conflict"

    @property
    def admits(self) -> bool:
        return self not in (PlacementVerdict.STALE, PlacementVerdict.CONFLICT)


def decide_placement_identity(
    cur: PlacementIdentity, cur_exists: bool, req: PlacementIdentity,
) -> tuple[PlacementVerdict, PlacementIdentity]:
    """EnterScene 入口落点会话身份裁决（与 Go decidePlacementIdentity 逐行一致）。"""
    if not cur_exists or cur.absent():
        return PlacementVerdict.FRESH, req
    if valid_session(req.session_id) and req.session_id == cur.session_id:
        gen = max(cur.login_generation, req.login_generation)
        return PlacementVerdict.SAME_SESSION, PlacementIdentity(cur.session_id, gen)
    if req.login_generation == 0:
        if cur.login_generation != 0:
            return PlacementVerdict.LEGACY, cur
        return PlacementVerdict.LEGACY, PlacementIdentity(req.session_id, 0)
    if cur.login_generation == 0 or req.login_generation > cur.login_generation:
        return PlacementVerdict.NEWER, req
    if req.login_generation < cur.login_generation:
        return PlacementVerdict.STALE, PlacementIdentity()
    if valid_session(cur.session_id):
        return PlacementVerdict.CONFLICT, PlacementIdentity()
    return PlacementVerdict.SAME_SESSION, PlacementIdentity(req.session_id, req.login_generation)


class LeaveIdentityVerdict(str, Enum):
    LEGACY = "legacy"
    KEEP_NEWER = "keep_newer"
    KEEP_CONFLICT = "keep_conflict"
    DELETED = "deleted"

    @property
    def deletes(self) -> bool:
        return self in (LeaveIdentityVerdict.LEGACY, LeaveIdentityVerdict.DELETED)


def decide_leave_identity(loc: PlacementIdentity, req: PlacementIdentity) -> LeaveIdentityVerdict:
    """LeaveScene 离场会话身份裁决（与 Go decideLeaveIdentity 逐行一致）。"""
    if req.login_generation == 0 or loc.login_generation == 0:
        return LeaveIdentityVerdict.LEGACY
    if loc.login_generation > req.login_generation:
        return LeaveIdentityVerdict.KEEP_NEWER
    if (
        loc.login_generation == req.login_generation
        and valid_session(loc.session_id)
        and valid_session(req.session_id)
        and loc.session_id != req.session_id
    ):
        return LeaveIdentityVerdict.KEEP_CONFLICT
    return LeaveIdentityVerdict.DELETED


class HolderReleaseVerdict(str, Enum):
    NO_HOLDER = "no_holder"
    OWNER_DEAD = "owner_dead"
    RELEASED = "released"
    PENDING = "pending"
    PENDING_UNPROVEN = "pending_unproven"

    @property
    def deletes(self) -> bool:
        return self in (
            HolderReleaseVerdict.NO_HOLDER,
            HolderReleaseVerdict.OWNER_DEAD,
            HolderReleaseVerdict.RELEASED,
        )


def decide_holder_release(
    *, no_holder: bool, owner_dead: bool, marker_released: bool, epoch: int, identity_proven: bool,
) -> HolderReleaseVerdict:
    """LeaveScene 持有者释放裁决（与 Go decideHolderRelease 逐行一致）。"""
    if no_holder:
        return HolderReleaseVerdict.NO_HOLDER
    if owner_dead:
        return HolderReleaseVerdict.OWNER_DEAD
    if marker_released and epoch != 0:
        return HolderReleaseVerdict.RELEASED
    if epoch == 0:
        return HolderReleaseVerdict.PENDING_UNPROVEN
    if identity_proven:
        return HolderReleaseVerdict.PENDING
    return HolderReleaseVerdict.PENDING_UNPROVEN


def mint_target_epoch(observed: int, now_ms: int) -> int:
    """G3a 墙钟地板：target = max(observed + 1, now_ms)，拒绝越过 2^53 - 2。"""
    if observed >= keys.MAX_MINTED_OWNER_EPOCH:
        raise ValueError(f"observed owner_epoch {observed} reached max {keys.MAX_MINTED_OWNER_EPOCH}")
    target = max(observed + 1, now_ms)
    if target > keys.MAX_MINTED_OWNER_EPOCH:
        raise ValueError(f"wall clock {now_ms}ms exceeds max owner_epoch {keys.MAX_MINTED_OWNER_EPOCH}")
    return target


def enter_scene_request_fingerprint(request) -> str:
    """计算去重指纹：清零 correlation_id 与 login_generation，保留 owner_revision 及业务语义字段。"""
    target = request
    if request.correlation_id != 0 or request.login_generation != 0:
        clone = type(request)()
        clone.CopyFrom(request)
        clone.correlation_id = 0
        clone.login_generation = 0
        target = clone
    payload = target.SerializeToString(deterministic=True)
    return hashlib.sha256(payload).hexdigest()


def _fnv1a_64(data: bytes) -> int:
    h = 0xCBF29CE484222325
    for byte in data:
        h ^= byte
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def assign_node_by_hash(seed: int, sorted_nodes: Sequence[str]) -> str:
    """与 Go world_init.go assignNodeByHash 一致的 FNV-1a 稳定哈希选点。"""
    idx = _fnv1a_64(str(seed).encode("ascii")) % len(sorted_nodes)
    return sorted_nodes[idx]


@dataclass(frozen=True, slots=True)
class PlacedLocation:
    epoch: int
    minted: bool
    raw: bytes
    revision: int
    tag: str
    previous_tag: str
    observed_epoch: int
    observed_raw: bytes
    observed_known: bool


@dataclass(frozen=True, slots=True)
class RouteRollbackPlan:
    mode: str
    restored_raw: bytes
    epoch_after: int
    required_marker: str
    forward_marker: str


def plan_route_rollback(
    *,
    protocol: Protocol,
    old_loc,
    old_raw: bytes,
    old_zone_id: int,
    placed: PlacedLocation,
    required_marker: str,
    keep_minted_epoch: bool,
) -> RouteRollbackPlan:
    """构造推路由/重定向失败后的回滚计划（bump 单调抬 epoch + 写 rollback_receipt + 转写 handoff 标记）。"""
    if (old_loc is None) != (old_raw == b""):
        raise ValueError("old_loc and old_raw must both be empty or both be present")
    if not placed.minted or keep_minted_epoch:
        if required_marker:
            raise ValueError("keep rollback cannot rely on handoff marker")
        return RouteRollbackPlan(
            mode="keep",
            restored_raw=old_raw,
            epoch_after=placed.epoch,
            required_marker="",
            forward_marker="",
        )
    if placed.epoch == 0:
        raise ValueError("minted placement epoch cannot be 0")
    epoch_after = placed.epoch + 1
    forward_marker = ""
    if required_marker:
        if old_loc is None:
            raise ValueError("marker-backed rollback requires old location")
        keys.parse_handoff(required_marker)
        _, saved_at_text = required_marker.split(":", 1)
        forward_marker = f"{epoch_after}:{saved_at_text}"
    if old_loc is None:
        return RouteRollbackPlan(
            mode="bump",
            restored_raw=b"",
            epoch_after=epoch_after,
            required_marker=required_marker,
            forward_marker=forward_marker,
        )
    restored = protocol.message("storage.PlayerLocation")
    restored.CopyFrom(old_loc)
    restored.owner_epoch = epoch_after
    restored.rollback_receipt = required_marker
    if restored.zone_id == 0:
        restored.zone_id = old_zone_id
    return RouteRollbackPlan(
        mode="bump",
        restored_raw=restored.SerializeToString(),
        epoch_after=epoch_after,
        required_marker=required_marker,
        forward_marker=forward_marker,
    )


def restore_revision_tag(previous_tag: str, restored_raw: bytes) -> str:
    if not restored_raw or not previous_tag or ":" not in previous_tag:
        return ""
    rev_text, _ = previous_tag.split(":", 1)
    if not rev_text.isascii() or not rev_text.isdigit():
        return ""
    rev = int(rev_text)
    return keys.location_revision_tag(rev, restored_raw) if rev > 0 else ""


def sign_gate_token(secret: bytes, payload_bytes: bytes) -> bytes:
    """签发 GateTokenPayload 签名：HMAC-SHA256 hex 小写 ASCII 字节（与 nodes/gate.py 一致）。"""
    return hmac.new(secret, payload_bytes, "sha256").hexdigest().encode("ascii")


def _select_gate_address(node, require_client_endpoint: bool) -> tuple[str, int] | None:
    if node.HasField("client_endpoint") and node.client_endpoint.ip and 1 <= node.client_endpoint.port <= 65535:
        return node.client_endpoint.ip, int(node.client_endpoint.port)
    if require_client_endpoint:
        return None
    if node.HasField("endpoint") and node.endpoint.ip and 1 <= node.endpoint.port <= 65535:
        return node.endpoint.ip, int(node.endpoint.port)
    return None


def select_gate_and_sign_redirect(
    *,
    protocol: Protocol,
    gate_nodes: Sequence,
    draining_ids: set[int],
    stale_ids: set[int],
    require_client_endpoint: bool,
    secret: bytes,
    target_zone_id: int,
    player_id: int,
    now_unix_sec: int,
    ws_url_lookup: Callable[[int], str] | None = None,
    rng: random.Random | None = None,
):
    """挑目标 zone Gate（地址去重 -> 健康分档 -> 随机二选一）并签发 RedirectToGateInfo。"""
    if not secret:
        raise RuntimeError("GateTokenSecret not configured, cannot sign redirect token")
    if not gate_nodes:
        raise LookupError(f"no gate nodes available for zone {target_zone_id}")

    # 1. 选客户端可达地址并按 host:port 去重（保留 launch_time 最新者）
    by_addr: dict[tuple[str, int], tuple[object, str, int]] = {}
    for node in gate_nodes:
        addr = _select_gate_address(node, require_client_endpoint)
        if addr is None:
            continue
        existing = by_addr.get(addr)
        if existing is None or int(node.launch_time) > int(existing[0].launch_time):
            by_addr[addr] = (node, addr[0], addr[1])
    targets = list(by_addr.values())
    if not targets:
        raise LookupError(f"no gate with client-reachable address for zone {target_zone_id}")

    # 2. 三档筛选：完全合格 > 非停写可达 > 全部
    def _is_full(node) -> bool:
        return node.max_connections > 0 and node.player_count * 10 >= node.max_connections * 9

    tier1 = [
        item for item in targets
        if item[0].node_id not in stale_ids and item[0].node_id not in draining_ids and not _is_full(item[0])
    ]
    tier2 = [item for item in targets if item[0].node_id not in stale_ids]
    pool = tier1 or tier2 or targets

    # 3. 随机二选一（Power of Two Choices）按 player_count 选负载更低者
    if len(pool) == 1:
        best_node, host, port = pool[0]
    else:
        chooser = rng or random
        i = chooser.randrange(len(pool))
        j = chooser.randrange(len(pool) - 1)
        if j >= i:
            j += 1
        a, b = pool[i], pool[j]
        best_node, host, port = a if a[0].player_count <= b[0].player_count else b

    expire_ts = now_unix_sec + config.REDIRECT_TOKEN_TTL_SEC
    payload_msg = protocol.message(
        "GateTokenPayload",
        gate_node_id=best_node.node_id,
        zone_id=best_node.zone_id,
        expire_timestamp=expire_ts,
        player_id=player_id,
        target_zone_id=target_zone_id,
    )
    payload_bytes = payload_msg.SerializeToString()
    signature = sign_gate_token(secret, payload_bytes)
    ws_url = getattr(best_node, "client_ws_url", "") or (ws_url_lookup(best_node.node_id) if ws_url_lookup else "")

    return protocol.message(
        "scene_manager.RedirectToGateInfo",
        target_gate_ip=host,
        target_gate_port=port,
        target_gate_ws_url=ws_url,
        token_payload=payload_bytes,
        token_signature=signature,
        token_deadline=expire_ts,
    )


# ==============================================================================
# 三、SceneManagerService（四条 Unary RPC 与大世界自动初始化）
# ==============================================================================

class SceneManagerService:
    """scene_manager.SceneManager gRPC 服务实现，严格对齐 Go 版状态机与两域隔离。"""

    def __init__(
        self,
        protocol: Protocol,
        cfg: config.SceneManagerConfig,
        *,
        control_store: SceneControlStore,
        player_store: PlayerPlacementStore,
        data_service: DataServiceClient | None,
        scene_nodes: SceneNodeClient,
        node_registry: NodeRegistry,
        publisher: CommandPublisher | None,
        id_gen: SceneIdGenerator | None = None,
        clock_ms: Callable[[], int] | None = None,
        rng: random.Random | None = None,
        dead_holder_releaser: DeadHolderReleaser | None = None,
        owner_authority: OwnerAuthorityStore | None = None,
        owner_authority_bridge: OwnerAuthorityBridge | None = None,
    ) -> None:
        self.protocol = protocol
        self.config = cfg
        self.control_store = control_store
        self.player_store = player_store
        self.data_service = data_service
        self.scene_nodes = scene_nodes
        self.node_registry = node_registry
        self.publisher = publisher
        self.dead_holder_releaser = dead_holder_releaser
        if owner_authority_bridge is not None:
            self.owner_authority_bridge: OwnerAuthorityBridge | None = owner_authority_bridge
            self.owner_authority: OwnerAuthorityStore | None = owner_authority or owner_authority_bridge.store
        elif owner_authority is not None:
            self.owner_authority = owner_authority
            self.owner_authority_bridge = OwnerAuthorityBridge(
                owner_authority, mode=cfg.owner_authority.mode or "shadow",
            )
        else:
            self.owner_authority = None
            self.owner_authority_bridge = None
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.id_gen = id_gen or SceneIdGenerator(cfg.node_id, clock_ms=self._clock_ms)
        self._rng = rng
        self._world_conf_ids: tuple[int, ...] = cfg.effective_world_conf_ids()
        self._world_conf_set: frozenset[int] = frozenset(self._world_conf_ids)
        self._bg_tasks: set[asyncio.Task] = set()
        self._known_node_uuids: dict[tuple[int, str], str] = {}

    def services(self) -> dict:
        return {
            SERVICE_NAME: {
                "CreateScene": self.create_scene,
                "DestroyScene": self.destroy_scene,
                "EnterScene": self.enter_scene,
                "LeaveScene": self.leave_scene,
            }
        }

    def _owner_authority_mode(self) -> str:
        if self.owner_authority_bridge is None:
            return ""
        return self.owner_authority_bridge.mode

    def _now_ms(self) -> int:
        return int(self._clock_ms())

    def _now_sec(self) -> int:
        return self._now_ms() // 1000

    def _spawn_bg(self, coro, *, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    async def drain_background_tasks(self) -> None:
        """等待所有在途后台任务（如异步 ReleasePlayer）结束，供优雅关闭与测试使用。"""
        if self._bg_tasks:
            await asyncio.gather(*tuple(self._bg_tasks), return_exceptions=True)

    async def close(self) -> None:
        await self.drain_background_tasks()

    async def _abort(self, context, code: grpc.StatusCode, detail: str):
        if context is not None:
            await context.abort(code, detail)
        raise RuntimeError(f"{code.name}: {detail}")

    def _err_resp(self, code: int, message: str):
        return self.protocol.message(
            "scene_manager.EnterSceneResponse",
            error_code=code,
            error_message=message,
        )

    def _empty(self):
        return self.protocol.message("Empty")

    # ── 节点同步与选点 ────────────────────────────────────────────────────────

    async def sync_nodes_to_control_store(self, zone_id: int | None = None) -> list:
        """将当前注册的 Scene 节点镜像写入控制域负载集与 scene_node_type 键，并登记死节点收尾任务。"""
        target_zone = self.config.zone_id if zone_id is None else zone_id
        nodes = await self.node_registry.list_scene_nodes(target_zone)
        live_ids: set[str] = set()
        for info in nodes:
            node_id = str(info.node_id)
            node_uuid = str(getattr(info, "node_uuid", "") or "")
            live_ids.add(node_id)
            # 若为判死同一代进程回归则撤销收尾任务并清 death_at；若为新化身复用 node_id 则保留老一代 death_at
            verdict = await self.control_store.withdraw_dead_node_for_returning_instance(
                target_zone, node_id, node_uuid,
            )
            scene_count = await self.control_store.get_node_scene_count(target_zone, node_id) or 0
            score = float(scene_count) + 0.01 * float(info.player_count)
            await self.control_store.upsert_scene_node(
                target_zone,
                node_id,
                score=score,
                scene_node_type=int(info.scene_node_type),
                preserve_death_at=(verdict == 0),
            )
            if node_uuid:
                self._known_node_uuids[(target_zone, node_id)] = node_uuid
        existing = await self.control_store.get_node_load_pairs(target_zone)
        now_ms = self._now_ms()
        now_sec = now_ms // 1000
        for node_id, _ in existing:
            if node_id not in live_ids:
                last_uuid = self._known_node_uuids.get((target_zone, node_id), "")
                await self.control_store.remove_scene_node(target_zone, node_id, death_at_unix_sec=now_sec)
                await self.control_store.enqueue_dead_node_reconcile(
                    target_zone,
                    node_id,
                    queued_at_ms=now_ms,
                    detach=True,
                    node_uuid=last_uuid,
                )
        return nodes

    async def _get_nodes_for_purpose(self, zone_id: int, purpose: errors.NodePurpose) -> list[str]:
        pairs = await self.control_store.get_node_load_pairs(zone_id)
        if not pairs:
            return []
        all_nodes: list[str] = []
        preferred: list[str] = []
        for node_id, _ in pairs:
            if self.node_registry.is_node_identity_ambiguous(zone_id, node_id):
                continue
            if self.node_registry.is_node_draining(zone_id, node_id):
                continue
            all_nodes.append(node_id)
            node_type = await self.control_store.get_node_scene_type(zone_id, node_id)
            if node_type is None or errors.matches_purpose(node_type, purpose):
                preferred.append(node_id)
        if preferred:
            return preferred
        if self.config.strict_node_type_separation:
            return []
        return all_nodes

    async def _is_node_alive(self, zone_id: int, node_id: str) -> bool:
        if not node_id or self.node_registry.is_node_identity_ambiguous(zone_id, node_id):
            return False
        return await self.control_store.is_node_in_load_set(zone_id, node_id)

    # ── 世界频道自动初始化与选线 ──────────────────────────────────────────────

    async def init_world_scenes(
        self,
        zone_id: int | None = None,
        conf_ids: Sequence[int] | None = None,
        *,
        wait_for_lock: bool = False,
    ) -> int:
        """按 World.json 为本 zone 自动创建并确保大世界频道在线，返回新登记频道数。"""
        target_zone = self.config.zone_id if zone_id is None else zone_id
        targets = tuple(conf_ids) if conf_ids is not None else self._world_conf_ids
        if not targets:
            return 0
        token = str(uuid.uuid4())
        locked = False
        deadline = time.monotonic() + (10.0 if wait_for_lock else 0.0)
        while True:
            locked = await self.control_store.acquire_world_init_lock(
                target_zone, token, config.WORLD_INIT_LOCK_TTL_SEC,
            )
            if locked or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.05)
        if not locked:
            logger.info("world init lock busy zone=%d skipping", target_zone)
            return 0

        created = 0
        try:
            nodes = sorted(await self._get_nodes_for_purpose(target_zone, errors.NodePurpose.WORLD))
            if not nodes:
                logger.error("world init skipped: no world-hosting nodes zone=%d", target_zone)
                return 0
            for conf_id in targets:
                desired = await self.control_store.get_or_seed_desired_world_channels(
                    target_zone, conf_id, self.config.channel_count_for(conf_id),
                )
                existing = await self.control_store.get_world_channels(target_zone, conf_id)
                for idx in range(len(existing), desired):
                    scene_id = self.id_gen.next_id()
                    target_node = assign_node_by_hash(conf_id * 1000 + idx, nodes)
                    outcome = await self.control_store.register_world_channel(
                        zone_id=target_zone,
                        scene_id=scene_id,
                        node_id=target_node,
                        conf_id=conf_id,
                        desired_channels=desired,
                    )
                    if outcome == RegistrationOutcome.REGISTERED:
                        created += 1
                    elif outcome == RegistrationOutcome.OVER_DESIRED:
                        break
                    else:
                        logger.error(
                            "world channel register failed zone=%d conf=%d scene=%d node=%s outcome=%s",
                            target_zone, conf_id, scene_id, target_node, outcome.value,
                        )

                all_members = await self.control_store.get_world_channels(target_zone, conf_id)
                for idx, sid_text in enumerate(all_members):
                    sid = int(sid_text)
                    owner = await self.control_store.get_scene_node(target_zone, sid)
                    if not owner:
                        continue
                    # 属主节点已确认下线且再入屏障已过时，先在存活大世界节点上建实体再原子 CAS 改派属主
                    if (
                        not await self._is_node_alive(target_zone, owner)
                        and self.node_registry.is_synced()
                        and not self.node_registry.is_node_registered(target_zone, owner)
                    ):
                        death_at = await self.control_store.get_node_death_at(target_zone, owner)
                        if death_at is None or (self._now_sec() - death_at) >= self.config.reentry_barrier_sec:
                            new_node = assign_node_by_hash(conf_id * 1000 + idx, nodes)
                            new_gen = await self.control_store.allocate_placement_generation(target_zone)
                            try:
                                await self.scene_nodes.create_scene(
                                    target_zone, new_node, config_id=conf_id, scene_id=sid, placement_generation=new_gen,
                                )
                                await self.control_store.reassign_scene_node(
                                    zone_id=target_zone,
                                    scene_id=sid,
                                    old_node_id=owner,
                                    new_node_id=new_node,
                                    placement_generation=new_gen,
                                )
                            except Exception as exc:
                                logger.error(
                                    "world channel reassign failed zone=%d conf=%d scene=%d old=%s new=%s error=%s",
                                    target_zone, conf_id, sid, owner, new_node, exc,
                                )
                        continue
                    gen = await self.control_store.get_scene_placement_gen(target_zone, sid)
                    try:
                        await self.scene_nodes.create_scene(
                            target_zone, owner, config_id=conf_id, scene_id=sid, placement_generation=gen,
                        )
                    except Exception as exc:
                        logger.error(
                            "world init CreateScene RPC failed zone=%d conf=%d scene=%d node=%s error=%s",
                            target_zone, conf_id, sid, owner, exc,
                        )
            return created
        finally:
            await self.control_store.release_world_init_lock(target_zone, token)

    async def _collect_live_world_candidates(
        self, zone_id: int, conf_id: int, *, exclude_draining: bool = False,
    ) -> list[WorldChannelCandidate]:
        members = await self.control_store.get_world_channels(zone_id, conf_id)
        candidates: list[WorldChannelCandidate] = []
        for sid_text in members:
            try:
                sid = int(sid_text)
            except ValueError:
                continue
            owner = await self.control_store.get_scene_node(zone_id, sid)
            if not owner:
                continue
            if not await self._is_node_alive(zone_id, owner):
                continue
            if exclude_draining and self.node_registry.is_node_draining(zone_id, owner):
                continue
            candidates.append(WorldChannelCandidate(scene_id=sid, node_id=owner, zone_id=zone_id))
        return candidates

    async def _select_best_world_channel(self, zone_id: int, conf_id: int) -> tuple[int, str]:
        candidates = await self._collect_live_world_candidates(zone_id, conf_id)
        if not candidates:
            return 0, ""
        best_scene, best_node, best_count = 0, "", None
        for cand in candidates:
            cnt = await self.control_store.get_scene_member_count(zone_id, cand.scene_id)
            if best_count is None or cnt < best_count:
                best_scene, best_node, best_count = cand.scene_id, cand.node_id, cnt
        return best_scene, best_node

    async def _reserve_world_channel(
        self, zone_id: int, conf_id: int, player_id: int, *, create_on_demand: bool = True, exclude_draining: bool = False,
    ) -> tuple[int, str] | None:
        candidates = await self._collect_live_world_candidates(zone_id, conf_id, exclude_draining=exclude_draining)
        if candidates:
            picked = await self.control_store.reserve_best_world_channel(candidates, player_id)
            if picked is not None:
                return picked[0], picked[1]
        if not create_on_demand:
            return None
        await self.init_world_scenes(zone_id, (conf_id,), wait_for_lock=True)
        candidates = await self._collect_live_world_candidates(zone_id, conf_id, exclude_draining=exclude_draining)
        if not candidates:
            return None
        picked = await self.control_store.reserve_best_world_channel(candidates, player_id)
        return None if picked is None else (picked[0], picked[1])

    async def _locate_world_channel(self, zone_id: int, scene_id: int, hint_conf_id: int = 0) -> tuple[int, str]:
        """判定 scene_id 是否为大世界分线：先查在役再查回收中，回收中优先（与 Go locateWorldChannel 一致）。"""
        order: list[int] = []
        if hint_conf_id > 0:
            order.append(hint_conf_id)
        for cid in self._world_conf_ids:
            if cid != hint_conf_id:
                order.append(cid)
        for conf_id in order:
            active = await self.control_store.is_world_channel_active(zone_id, conf_id, scene_id)
            draining = await self.control_store.is_world_channel_draining(zone_id, conf_id, scene_id)
            if draining:
                return conf_id, "draining"
            if active:
                return conf_id, "active"
        return 0, "not_member"

    # ── RPC 1: CreateScene ───────────────────────────────────────────────────

    async def create_scene(self, request, context=None, path: str = ""):
        zone_id = int(request.zone_id) or self.config.zone_id
        if zone_id != self.config.zone_id:
            return self.protocol.message(
                "scene_manager.CreateSceneResponse",
                error_code=errors.ERR_NO_AVAILABLE_NODE,
                error_message=f"zone {zone_id} is served by its own scene_manager (this process serves zone {self.config.zone_id})",
            )

        scene_type = int(request.scene_type)
        if scene_type == errors.SCENE_TYPE_UNSPECIFIED:
            scene_type = (
                errors.SCENE_TYPE_MAIN_WORLD
                if int(request.scene_conf_id) in self._world_conf_set
                else errors.SCENE_TYPE_INSTANCE
            )

        if scene_type == errors.SCENE_TYPE_MAIN_WORLD:
            return await self._create_main_world_scene(request, zone_id)
        if scene_type == errors.SCENE_TYPE_INSTANCE:
            return await self._create_instance_scene(request, zone_id)
        return self.protocol.message(
            "scene_manager.CreateSceneResponse",
            error_code=errors.ERR_INVALID_SCENE_TYPE,
            error_message="unknown scene type",
        )

    async def _create_main_world_scene(self, request, zone_id: int):
        conf_id = int(request.scene_conf_id)
        scene_id, node_id = await self._select_best_world_channel(zone_id, conf_id)
        if scene_id > 0:
            return self.protocol.message("scene_manager.CreateSceneResponse", scene_id=scene_id, node_id=node_id)
        await self.init_world_scenes(zone_id, (conf_id,), wait_for_lock=True)
        scene_id, node_id = await self._select_best_world_channel(zone_id, conf_id)
        if scene_id > 0:
            return self.protocol.message("scene_manager.CreateSceneResponse", scene_id=scene_id, node_id=node_id)
        return self.protocol.message(
            "scene_manager.CreateSceneResponse",
            error_code=errors.ERR_NO_AVAILABLE_NODE,
            error_message="failed to create main world channels",
        )

    async def _resolve_mirror_source_node(self, source_scene_id: int, zone_id: int) -> str:
        owner = await self.control_store.get_scene_node(zone_id, source_scene_id)
        if not owner:
            return ""
        if not await self._is_node_alive(zone_id, owner):
            return ""
        if self.config.mirror_source_node_load_cap > 0:
            cnt = await self.control_store.get_node_scene_count(zone_id, owner)
            if cnt is not None and cnt >= self.config.mirror_source_node_load_cap:
                return ""
        return owner

    async def _create_instance_scene(self, request, zone_id: int):
        source_scene_id = int(request.source_scene_id)
        mirror_config_id = int(request.mirror_config_id)
        creator_ids = list(request.creator_ids)

        if source_scene_id > 0:
            if mirror_config_id == 0:
                if not await self.control_store.scene_node_exists(zone_id, source_scene_id):
                    return self.protocol.message(
                        "scene_manager.CreateSceneResponse",
                        error_code=errors.ERR_SOURCE_SCENE_GONE,
                        error_message=f"source scene {source_scene_id} no longer exists",
                    )
            if self.config.mirror_dedup_by_source:
                mirrors = await self.control_store.get_scene_mirrors(zone_id, source_scene_id)
                for mid_str in mirrors:
                    mid = int(mid_str)
                    owner = await self.control_store.get_scene_node(zone_id, mid)
                    if owner:
                        return self.protocol.message(
                            "scene_manager.CreateSceneResponse",
                            scene_id=mid,
                            node_id=owner,
                            creator_ids=creator_ids,
                        )
                    await self.control_store.remove_scene_mirror(zone_id, source_scene_id, mid)

        # 选节点：显式指定 > 镜像源共置（跨用途放行） > 实例节点池最低负载
        target_node = request.target_node_id
        if not target_node and source_scene_id > 0:
            target_node = await self._resolve_mirror_source_node(source_scene_id, zone_id)
        if not target_node:
            candidates = await self._get_nodes_for_purpose(zone_id, errors.NodePurpose.INSTANCE)
            if not candidates:
                code = errors.ERR_NO_NODE_FOR_PURPOSE if self.config.strict_node_type_separation else errors.ERR_NO_AVAILABLE_NODE
                return self.protocol.message(
                    "scene_manager.CreateSceneResponse",
                    error_code=code,
                    error_message=f"no instance-hosting node in zone {zone_id}",
                )
            target_node = candidates[0]

        scene_id = self.id_gen.next_id()
        is_mirror = mirror_config_id > 0 or source_scene_id > 0
        outcome = await self.control_store.register_instance(
            zone_id=zone_id,
            scene_id=scene_id,
            node_id=target_node,
            created_at=self._now_sec(),
            mirror=is_mirror,
            source_scene_id=source_scene_id,
        )
        if outcome != RegistrationOutcome.REGISTERED:
            code = errors.ERR_DUPLICATE_SCENE if outcome == RegistrationOutcome.CONFLICT else errors.ERR_REDIS
            return self.protocol.message(
                "scene_manager.CreateSceneResponse",
                error_code=code,
                error_message=f"registering instance {scene_id} failed: {outcome.value}",
            )

        try:
            await self.scene_nodes.create_scene(
                zone_id,
                target_node,
                config_id=int(request.scene_conf_id),
                scene_id=scene_id,
                mirror_config_id=mirror_config_id,
                creator_ids=creator_ids,
            )
        except Exception as exc:
            logger.error("CreateScene RPC failed zone=%d scene=%d node=%s error=%s", zone_id, scene_id, target_node, exc)
            await self._rollback_instance_allocation(zone_id, scene_id, target_node)
            return self.protocol.message(
                "scene_manager.CreateSceneResponse",
                error_code=errors.ERR_NO_AVAILABLE_NODE,
                error_message=f"scene node {target_node} rejected CreateScene: {exc}",
            )

        return self.protocol.message(
            "scene_manager.CreateSceneResponse",
            scene_id=scene_id,
            node_id=target_node,
            creator_ids=creator_ids,
        )

    async def _rollback_instance_allocation(self, zone_id: int, scene_id: int, target_node: str) -> None:
        claim = await self.control_store.atomic_destroy_instance_force(
            zone_id=zone_id,
            scene_id=scene_id,
            expected_owner=target_node,
            kind="rollback",
            reason="create_rpc_failed",
            count_owner=True,
            now_unix_sec=self._now_sec(),
        )
        if not claim.claimed:
            return
        job = SceneTeardownJob(
            zone_id=zone_id, scene_id=scene_id, kind="rollback", reason="create_rpc_failed", claim=claim,
        )
        await self.complete_scene_teardown(job)

    # ── RPC 2: DestroyScene 与幂等拆除副作用执行 ──────────────────────────────

    async def complete_scene_teardown(
        self, job: SceneTeardownJob, *, pre_mirror_children: Sequence[str] | None = None,
    ) -> bool:
        """执行场景认领后的幂等拆除副作用，全部成功时调用 finish_scene_teardown 清理待办任务。"""
        zone_id, scene_id, claim = job.zone_id, job.scene_id, job.claim
        all_done = True

        # 1. 副本 / 缩容大世界频道认领后级联销毁子镜像
        if job.kind in ("instance", "world_drain"):
            try:
                children = await self.control_store.get_scene_mirrors(zone_id, scene_id)
                keep_index = False
            except Exception:
                children = list(pre_mirror_children or ())
                keep_index = True
            for child_str in children:
                if not child_str.isascii() or not child_str.isdigit():
                    continue
                child_id = int(child_str)
                if child_id == scene_id:
                    continue
                try:
                    await self._destroy_instance_force(zone_id, child_id, reason="cascade")
                except Exception as exc:
                    logger.error(
                        "cascade destroy mirror failed source=%d mirror=%d error=%s", scene_id, child_id, exc,
                    )
                    keep_index = True
            if keep_index:
                all_done = False
            else:
                try:
                    await self.control_store.delete_scene_mirrors_index(zone_id, scene_id)
                except Exception as exc:
                    logger.error("delete scene mirrors index failed scene=%d error=%s", scene_id, exc)
                    all_done = False

        # 2. 若属主节点仍存活，通知 SceneNodeGrpc.DestroyScene（节点已死时进程随实体一起消亡）
        if claim.node_id and await self._is_node_alive(zone_id, claim.node_id):
            try:
                await self.scene_nodes.destroy_scene(zone_id, claim.node_id, scene_id=scene_id)
            except Exception as exc:
                logger.error("DestroyScene RPC failed scene=%d node=%s error=%s", scene_id, claim.node_id, exc)
                all_done = False

        # 3. 若本身是镜像，从源场景子镜像集合摘除自身
        if claim.source and claim.source.isascii() and claim.source.isdigit():
            try:
                await self.control_store.remove_scene_mirror(zone_id, int(claim.source), scene_id)
            except Exception as exc:
                logger.error("unlink mirror from source failed scene=%d source=%s error=%s", scene_id, claim.source, exc)
                all_done = False

        # 4. 清理残留节点成员计数（回滚路径无成员）
        if claim.node_id and job.kind != "rollback":
            for member_str in claim.members:
                if member_str.isascii() and member_str.isdigit():
                    await self._remove_residual_node_member(int(member_str), scene_id, zone_id, claim.node_id)

        if not all_done:
            return False
        return await self.control_store.finish_scene_teardown(zone_id, scene_id, claim.token)

    async def destroy_idle_instance(self, zone_id: int, scene_id: int) -> bool:
        """空闲副本/镜像原子销毁：使用 LUA_ATOMIC_DESTROY_INSTANCE，若期间有人进入则回 busy 不销毁。"""
        pre_mirrors = await self.control_store.get_scene_mirrors(zone_id, scene_id)
        owner = await self.control_store.get_scene_node(zone_id, scene_id)
        claim = await self.control_store.atomic_destroy_instance(
            zone_id=zone_id,
            scene_id=scene_id,
            expected_owner=owner,
            now_unix_sec=self._now_sec(),
        )
        if not claim.claimed:
            return False
        job = SceneTeardownJob(zone_id=zone_id, scene_id=scene_id, kind="instance", reason="idle", claim=claim)
        await self.complete_scene_teardown(job, pre_mirror_children=pre_mirrors)
        return True

    async def destroy_orphan_of_dead_node(
        self, zone_id: int, scene_id: int, dead_node: str, task_fence: DeadNodeTaskFence,
    ) -> tuple[bool, bool]:
        """死节点孤儿副本原子销毁，返回 (claimed, withdrawn)。"""
        pre_mirrors = await self.control_store.get_scene_mirrors(zone_id, scene_id)
        claim = await self.control_store.atomic_destroy_orphan_of_dead_node(
            zone_id=zone_id,
            scene_id=scene_id,
            dead_node=dead_node,
            task_fence=task_fence,
            now_unix_sec=self._now_sec(),
        )
        if claim.withdrawn:
            return False, True
        if not claim.claimed:
            return False, False
        job = SceneTeardownJob(zone_id=zone_id, scene_id=scene_id, kind="instance", reason="node_death", claim=claim)
        await self.complete_scene_teardown(job, pre_mirror_children=pre_mirrors)
        return True, False

    async def destroy_scene(self, request, context=None, path: str = ""):
        zone_id = int(request.zone_id) or self.config.zone_id
        scene_id = int(request.scene_id)
        if zone_id != self.config.zone_id:
            await self._abort(
                context,
                grpc.StatusCode.FAILED_PRECONDITION,
                f"destroy scene {scene_id}: zone {zone_id} is not served by this scene_manager (zone {self.config.zone_id})",
            )

        conf_id, membership = await self._locate_world_channel(zone_id, scene_id)
        if membership != "not_member":
            await self._abort(
                context,
                grpc.StatusCode.FAILED_PRECONDITION,
                f"destroy scene {scene_id} (world map {conf_id} channel): world channels cannot be destroyed via DestroyScene",
            )

        await self._destroy_instance_force(zone_id, scene_id, reason="explicit")
        return self._empty()

    async def _destroy_instance_force(self, zone_id: int, scene_id: int, *, reason: str) -> bool:
        pre_mirrors = await self.control_store.get_scene_mirrors(zone_id, scene_id)
        claim = None
        for _ in range(3):
            owner = await self.control_store.get_scene_node(zone_id, scene_id)
            if not owner:
                return False
            claim = await self.control_store.atomic_destroy_instance_force(
                zone_id=zone_id,
                scene_id=scene_id,
                expected_owner=owner,
                kind="instance",
                reason=reason,
                count_owner=True,
                now_unix_sec=self._now_sec(),
            )
            if claim.claimed or claim.pending:
                break
        if claim is None or not claim.claimed:
            return False

        job = SceneTeardownJob(zone_id=zone_id, scene_id=scene_id, kind="instance", reason=reason, claim=claim)
        await self.complete_scene_teardown(job, pre_mirror_children=pre_mirrors)
        return True

    # ── 两域成员集合收敛辅助（绝不跨两域写同一段 Lua） ────────────────────────

    async def _read_location_snapshot(self, player_id: int):
        raw, tag = await self.player_store.read_location_snapshot(player_id)
        if not raw:
            return None, b"", tag
        loc = self.protocol.message("storage.PlayerLocation")
        try:
            loc.ParseFromString(raw)
        except DecodeError as exc:
            raise errors.LocationCorruptError(str(exc)) from exc
        try:
            keys.parse_tagged_location_revision(tag, raw)
        except ValueError as exc:
            raise errors.LocationRevisionCorruptError(str(exc)) from exc
        return loc, raw, tag

    async def _location_for_revision(self, player_id: int, revision: int):
        loc, raw, tag = await self._read_location_snapshot(player_id)
        if loc is None:
            return None, b"", ""
        previous = keys.parse_tagged_location_revision(tag, raw)
        if previous == 0 or previous == revision:
            return loc, raw, tag
        if previous > revision:
            raise errors.OwnerEpochConflictError("location revision ahead of request")
        outcome = await self._delete_location_guarded(
            player_id=player_id,
            expected_raw=raw,
            zone_id=int(loc.zone_id),
            scene_id=int(loc.scene_id),
            mode="any",
            epoch=0,
            expected_tag=tag,
            owner_revision=revision,
        )
        if outcome not in ("deleted", "already_gone"):
            raise errors.OwnerEpochConflictError("stale revision cleanup conflict")
        return None, b"", ""

    @staticmethod
    def _location_overlap(loc, scene_id: int, zone_id: int, node_id: str) -> tuple[int, str]:
        if loc is None:
            return 0, ""
        keep_scene = scene_id if int(loc.scene_id) == scene_id else 0
        keep_node = node_id if (loc.node_id == node_id and int(loc.zone_id) in (zone_id, 0)) else ""
        return keep_scene, keep_node

    async def _commit_scene_member(self, player_id: int, scene_id: int, zone_id: int, node_id: str) -> None:
        if zone_id != self.config.zone_id:
            return
        try:
            await self.control_store.add_scene_member(zone_id, scene_id, node_id, player_id)
        except Exception as exc:
            logger.error("commit scene member failed player=%d scene=%d node=%s error=%s", player_id, scene_id, node_id, exc)

    async def _remove_scene_member(self, player_id: int, scene_id: int, zone_id: int, node_id: str) -> None:
        """两域幂等摘成员：先读 PlayerRedis location 保留重叠部分，SREM 后再复读防并发进场漏加。"""
        if zone_id != self.config.zone_id:
            return
        loc, _, _ = await self._read_location_snapshot(player_id)
        keep_scene, keep_node = self._location_overlap(loc, scene_id, zone_id, node_id)
        rm_scene = 0 if keep_scene else scene_id
        rm_node = "" if keep_node else node_id
        if rm_scene == 0 and not rm_node:
            return
        await self.control_store.remove_scene_member_keys(zone_id, rm_scene, rm_node, player_id)
        loc_after, _, _ = await self._read_location_snapshot(player_id)
        add_scene, add_node = self._location_overlap(loc_after, rm_scene, zone_id, rm_node)
        if add_scene or add_node:
            await self.control_store.add_scene_member(zone_id, add_scene, add_node, player_id)

    async def _release_scene_member_best_effort(self, player_id: int, scene_id: int, zone_id: int, node_id: str) -> None:
        try:
            await self._remove_scene_member(player_id, scene_id, zone_id, node_id)
        except Exception as exc:
            logger.error("release scene member failed player=%d scene=%d node=%s error=%s", player_id, scene_id, node_id, exc)

    async def _remove_scene_player(self, player_id: int, scene_id: int, zone_id: int) -> None:
        if scene_id == 0:
            return
        if zone_id == 0:
            owner = await self.control_store.get_scene_node(self.config.zone_id, scene_id)
            if not owner:
                return
            await self._remove_scene_member(player_id, scene_id, self.config.zone_id, owner)
            return
        if zone_id != self.config.zone_id:
            return
        owner = await self.control_store.get_scene_node(zone_id, scene_id)
        await self._remove_scene_member(player_id, scene_id, zone_id, owner)

    async def _remove_residual_node_member(self, player_id: int, scene_id: int, zone_id: int, node_id: str) -> None:
        if zone_id != self.config.zone_id or not node_id:
            return
        try:
            loc, _, _ = await self._read_location_snapshot(player_id)
            _, keep_node = self._location_overlap(loc, 0, zone_id, node_id)
            if keep_node and int(loc.scene_id) != scene_id:
                return
            await self.control_store.remove_scene_member_keys(zone_id, 0, node_id, player_id)
            loc2, _, _ = await self._read_location_snapshot(player_id)
            _, keep_node2 = self._location_overlap(loc2, 0, zone_id, node_id)
            if keep_node2 and int(loc2.scene_id) != scene_id:
                await self.control_store.add_scene_member(zone_id, 0, node_id, player_id)
        except Exception as exc:
            logger.error("remove residual node member failed player=%d node=%s error=%s", player_id, node_id, exc)

    # ── 角色目录与归属区校验 ──────────────────────────────────────────────────

    async def _lookup_current_role(self, player_id: int, context) -> AccountRoleInfo:
        if self.data_service is None:
            await self._abort(context, grpc.StatusCode.UNAVAILABLE, "role directory dependency not configured")
        try:
            role = await self.data_service.lookup_account_role(player_id)
        except Exception as exc:
            logger.error("LookupAccountRole failed player=%d error=%s", player_id, exc)
            await self._abort(context, grpc.StatusCode.UNAVAILABLE, "account role lookup failed")
        if (
            role is None
            or role.player_id != player_id
            or not role.account
            or role.owner_revision == 0
            or role.trade_listing_id != 0
        ):
            await self._abort(context, grpc.StatusCode.FAILED_PRECONDITION, "role ownership unavailable for scene routing")
        return role

    async def _resolve_home_zone(
        self, request, *, reject_unmapped_travel: bool,
    ) -> tuple[int, object | None]:
        gate_zone = int(request.gate_zone_id) or self.config.zone_id
        if self.data_service is None:
            if not self.config.allow_gate_zone_as_home_zone:
                return 0, self._err_resp(errors.ERR_HOME_ZONE_UNAVAILABLE, "DataService is not configured")
            return gate_zone, None
        try:
            home = await self.data_service.get_player_home_zone(int(request.player_id))
        except HomeZoneNotFoundError:
            if reject_unmapped_travel:
                return 0, self._err_resp(
                    errors.ERR_HOME_ZONE_UNAVAILABLE,
                    f"player {request.player_id} has no home zone mapping; cross-zone travel rejected",
                )
            return gate_zone, None
        except Exception as exc:
            logger.error("GetPlayerHomeZone failed player=%d error=%s", request.player_id, exc)
            return 0, self._err_resp(errors.ERR_HOME_ZONE_UNAVAILABLE, f"home zone lookup failed: {exc}")
        if home == 0:
            if reject_unmapped_travel:
                return 0, self._err_resp(
                    errors.ERR_HOME_ZONE_UNAVAILABLE,
                    f"player {request.player_id} home zone mapping is 0; cross-zone travel rejected",
                )
            return gate_zone, None
        return home, None

    # ── 死节点与交接门禁 ──────────────────────────────────────────────────────

    async def _is_owner_dead(self, loc, zone_id: int) -> bool:
        if loc is None or zone_id == 0 or not loc.node_id:
            return False
        node_id = loc.node_id
        if zone_id != self.config.zone_id:
            return False
        if not self.node_registry.is_synced():
            return False
        if self.node_registry.is_node_identity_ambiguous(zone_id, node_id):
            return False
        if self.node_registry.is_node_registered(zone_id, node_id):
            return False
        if await self.control_store.is_node_in_load_set(zone_id, node_id):
            return False
        if self.config.reentry_barrier_sec > 0:
            death_at = await self.control_store.get_node_death_at(zone_id, node_id)
            if death_at is not None and (self._now_sec() - death_at) < self.config.reentry_barrier_sec:
                return False
        return True

    async def _check_handoff_gate(
        self, request, current_loc, observed_epoch: int, *, site: str,
    ) -> tuple[object | None, bool, str]:
        """CZ-4 换手门：返回 (err_resp, mint, required_marker)。"""
        player_id = int(request.player_id)
        try:
            marker_raw = await self.player_store.get_handoff_marker(player_id)
        except Exception as exc:
            return self._err_resp(errors.ERR_REDIS, f"read handoff marker failed: {exc}"), False, ""
        if keys.marker_releases_epoch(marker_raw, observed_epoch):
            return None, True, marker_raw
        if self.config.allow_unsafe_cross_node_handoff:
            logger.info("unsafe cross-node handoff bypass site=%s player=%d epoch=%d", site, player_id, observed_epoch)
            return None, False, ""
        logger.info(
            "handoff pending site=%s player=%d owner_epoch=%d marker=%r old_node=%s",
            site, player_id, observed_epoch, marker_raw, current_loc.node_id,
        )
        return (
            self._err_resp(
                errors.ERR_HANDOFF_PENDING,
                f"handoff marker not ready for owner_epoch={observed_epoch}; retry after source persists",
            ),
            False,
            "",
        )

    # ── 落点写入与单调回滚 ────────────────────────────────────────────────────

    async def _place_player_location(
        self,
        *,
        player_id: int,
        scene_id: int,
        node_id: str,
        zone_id: int,
        observed_epoch: int,
        mint: bool,
        required_marker: str,
        pending_scene_conf_id: int,
        identity: PlacementIdentity,
        observed_raw: bytes,
        owner_revision: int,
        observed_tag: str,
    ) -> PlacedLocation:
        placed_epoch = mint_target_epoch(observed_epoch, self._now_ms()) if mint else observed_epoch
        loc = self.protocol.message(
            "storage.PlayerLocation",
            scene_id=scene_id,
            node_id=node_id,
            update_time=self._now_sec(),
            zone_id=zone_id,
            owner_epoch=placed_epoch,
            pending_scene_conf_id=pending_scene_conf_id,
            session_id=identity.session_id,
            login_generation=identity.login_generation,
        )
        raw = loc.SerializeToString()
        new_tag = keys.location_revision_tag(owner_revision, raw)
        expect_mode = "missing" if not observed_raw else "present"

        if mint:
            signed = await self.player_store.mint_epoch_and_set_location(
                player_id=player_id,
                observed_epoch=observed_epoch,
                location_bytes=raw,
                required_marker=required_marker,
                expect_mode=expect_mode,
                expect_raw=observed_raw,
                owner_revision=owner_revision,
                new_tag=new_tag,
                observed_tag=observed_tag,
                target_epoch=placed_epoch,
            )
            # 重放识别命中返回 -new_epoch（<= -2）
            if signed < -1:
                signed = -signed
            if signed == -1:
                raise errors.HandoffWithdrawnError("handoff marker withdrawn")
            if signed <= 0:
                raise errors.OwnerEpochConflictError("owner epoch or location advanced concurrently")
            if signed != placed_epoch:
                raise RuntimeError(f"minted epoch {signed} != expected {placed_epoch}")
        else:
            ok = await self.player_store.set_location_if_epoch(
                player_id=player_id,
                observed_epoch=observed_epoch,
                location_bytes=raw,
                expect_mode=expect_mode,
                expect_raw=observed_raw,
                owner_revision=owner_revision,
                new_tag=new_tag,
                observed_tag=observed_tag,
            )
            if ok <= 0:
                raise errors.OwnerEpochConflictError("owner epoch or location advanced concurrently")

        return PlacedLocation(
            epoch=placed_epoch,
            minted=mint,
            raw=raw,
            revision=owner_revision,
            tag=new_tag,
            previous_tag=observed_tag,
            observed_epoch=observed_epoch,
            observed_raw=observed_raw,
            observed_known=True,
        )

    async def _rollback_placement(
        self,
        *,
        player_id: int,
        old_loc,
        old_raw: bytes,
        old_zone_id: int,
        placed: PlacedLocation,
        required_marker: str,
        keep_minted_epoch: bool,
    ) -> tuple[bool, int]:
        try:
            plan = plan_route_rollback(
                protocol=self.protocol,
                old_loc=old_loc,
                old_raw=old_raw,
                old_zone_id=old_zone_id,
                placed=placed,
                required_marker=required_marker,
                keep_minted_epoch=keep_minted_epoch,
            )
        except Exception as exc:
            logger.error("route rollback plan failed player=%d error=%s", player_id, exc)
            return False, 0

        restored_tag = restore_revision_tag(placed.previous_tag, plan.restored_raw)
        try:
            reply = await self.player_store.rollback_player_placement(
                player_id=player_id,
                placed_raw=placed.raw,
                restored_raw=plan.restored_raw,
                placed_epoch=placed.epoch,
                mode=plan.mode,
                required_marker=plan.required_marker,
                forward_marker=plan.forward_marker,
                handoff_ttl_sec=config.HANDOFF_TTL_SEC,
                owner_revision=placed.revision,
                placed_tag=placed.tag,
                restored_tag=restored_tag,
            )
        except Exception as exc:
            logger.error("route rollback redis error player=%d error=%s", player_id, exc)
            return False, 0

        if reply <= -2:
            echo = plan.epoch_after if (plan.mode == "bump" and -reply == plan.epoch_after) else 0
            logger.info(
                "route rollback applied player=%d mode=%s epoch=%d->%d receipt=%r forwarded=%r",
                player_id, plan.mode, placed.epoch, plan.epoch_after, plan.required_marker, plan.forward_marker,
            )
            return True, echo
        if reply == 1:
            return True, 0
        if reply == 2 and plan.mode == "bump":
            return True, plan.epoch_after
        logger.warning("route rollback skipped player=%d reply=%d mode=%s", player_id, reply, plan.mode)
        return False, 0

    # ── Kafka 路由推送（RoutePlayerEvent=11 / RedirectToGateEvent=37 / PlayerHostedPlacementEvent=53） ──

    async def _push_route_player(
        self, request, *, node_id: str, scene_id: int, home_zone_id: int, owner_epoch: int,
    ) -> None:
        if self.publisher is None:
            raise RuntimeError("Kafka command publisher is unavailable")
        target_node_id = int(node_id)
        target_gate_id = int(request.gate_id)
        event = self.protocol.message(
            "contracts.kafka.RoutePlayerEvent",
            session_id=int(request.session_id),
            target_node_id=target_node_id,
            scene_id=scene_id,
            player_id=int(request.player_id),
            home_zone_id=home_zone_id,
            owner_epoch=owner_epoch,
            login_generation=int(request.login_generation),
            owner_revision=int(request.owner_revision),
        )
        cmd = self.protocol.message(
            "contracts.kafka.GateCommand",
            player_id=int(request.player_id),
            target_node_id=target_node_id,
            session_id=int(request.session_id),
            payload=event.SerializeToString(),
            target_gate_id=target_gate_id,
            target_instance_id=request.gate_instance_id,
            event_id=errors.EVENT_ID_ROUTE_PLAYER,
        )
        await self.publisher.publish_gate_command(target_gate_id, int(request.player_id), cmd)

    async def _push_redirect_to_gate(self, request, redirect) -> None:
        if self.publisher is None:
            raise RuntimeError("Kafka command publisher is unavailable")
        target_gate_id = int(request.gate_id)
        event_fields = {
            "player_id": int(request.player_id),
            "session_id": int(request.session_id),
            "target_gate_ip": redirect.target_gate_ip,
            "target_gate_port": int(redirect.target_gate_port),
            "token_payload": redirect.token_payload,
            "token_signature": redirect.token_signature,
            "token_deadline": int(redirect.token_deadline),
        }
        event_desc = self.protocol.pool.FindMessageTypeByName("contracts.kafka.RedirectToGateEvent")
        if "target_gate_ws_url" in event_desc.fields_by_name:
            event_fields["target_gate_ws_url"] = redirect.target_gate_ws_url
        event = self.protocol.message("contracts.kafka.RedirectToGateEvent", **event_fields)
        cmd = self.protocol.message(
            "contracts.kafka.GateCommand",
            player_id=int(request.player_id),
            session_id=int(request.session_id),
            target_gate_id=target_gate_id,
            target_instance_id=request.gate_instance_id,
            payload=event.SerializeToString(),
            event_id=errors.EVENT_ID_REDIRECT_TO_GATE,
        )
        await self.publisher.publish_gate_command(target_gate_id, int(request.player_id), cmd)

    async def _push_hosted_placement(
        self, request, *, node_id: str, instance_id: str, scene_id: int, home_zone_id: int, owner_epoch: int,
    ) -> None:
        if self.publisher is None:
            raise RuntimeError("Kafka command publisher is unavailable")
        if not instance_id:
            raise RuntimeError(f"hosted placement missing target_instance_id for node {node_id}")
        target_node_id = int(node_id)
        event = self.protocol.message(
            "PlayerHostedPlacementEvent",
            player_id=int(request.player_id),
            scene_id=scene_id,
            home_zone_id=home_zone_id,
            owner_epoch=owner_epoch,
            session_id=int(request.session_id),
            login_generation=int(request.login_generation),
            owner_revision=int(request.owner_revision),
        )
        cmd = self.protocol.message(
            "contracts.kafka.SceneCommand",
            command_type=0,  # DispatchEvent
            player_id=int(request.player_id),
            target_scene_id=target_node_id,
            payload=event.SerializeToString(),
            target_instance_id=instance_id,
            event_id=errors.EVENT_ID_PLAYER_HOSTED_PLACEMENT,
        )
        await self.publisher.publish_scene_command(target_node_id, int(request.player_id), cmd)

    # ── 异步 ReleasePlayer 派发 ───────────────────────────────────────────────

    def _dispatch_release_player(
        self, *, old_zone_id: int, old_node_id: str, player_id: int,
        target_scene_id: int, target_node_id: str, expected_owner_epoch: int,
    ) -> None:
        async def _run() -> None:
            try:
                await self.scene_nodes.release_player(
                    old_zone_id,
                    old_node_id,
                    player_id=player_id,
                    target_scene_id=target_scene_id,
                    target_node_id=target_node_id,
                    expected_owner_epoch=expected_owner_epoch,
                    session_ended=False,
                )
            except Exception as exc:
                logger.warning(
                    "async ReleasePlayer failed player=%d old_node=%s epoch=%d error=%s",
                    player_id, old_node_id, expected_owner_epoch, exc,
                )

        self._spawn_bg(_run(), name=f"sm-release-{player_id}")

    def _dispatch_session_ended_release(
        self, *, zone_id: int, node_id: str, player_id: int,
        expected_owner_epoch: int, covered: PlacementIdentity,
    ) -> None:
        async def _run() -> None:
            try:
                await self.scene_nodes.release_player(
                    zone_id,
                    node_id,
                    player_id=player_id,
                    expected_owner_epoch=expected_owner_epoch,
                    session_ended=True,
                    covered_session_id=covered.session_id,
                    covered_login_generation=covered.login_generation,
                    keep_held=False,
                )
            except Exception as exc:
                logger.warning(
                    "async session_ended ReleasePlayer failed player=%d node=%s epoch=%d error=%s",
                    player_id, node_id, expected_owner_epoch, exc,
                )

        self._spawn_bg(_run(), name=f"sm-leave-release-{player_id}")

    # ── RPC 3: EnterScene ────────────────────────────────────────────────────

    async def enter_scene(self, request, context=None, path: str = ""):
        player_id = int(request.player_id)
        owner_revision = int(request.owner_revision)
        if owner_revision == 0:
            await self._abort(context, grpc.StatusCode.INVALID_ARGUMENT, "missing owner_revision")

        current_role = await self._lookup_current_role(player_id, context)
        if current_role.owner_revision != owner_revision:
            await self._abort(context, grpc.StatusCode.FAILED_PRECONDITION, "owner_revision mismatch")
        try:
            obs = await self.player_store.observe_location_revision(player_id, owner_revision)
        except Exception as exc:
            logger.error("observe_location_revision failed player=%d error=%s", player_id, exc)
            await self._abort(context, grpc.StatusCode.UNAVAILABLE, "failed to observe location revision")
        if obs != 1:
            await self._abort(context, grpc.StatusCode.UNAVAILABLE, "location revision floor rejected")

        response = None
        dedupe_owned = False
        dedupe_owner = ""
        dedupe_fingerprint = ""
        charge_channel_cooldown = False

        try:
            # 预检：gate_id / gate_instance_id / hosted_placement / gate_zone_id
            if request.gate_id:
                if self.publisher is None:
                    response = self._err_resp(errors.ERR_KAFKA_ROUTE, "Kafka route writer is unavailable")
                    return response
                if not request.gate_id.isdigit() or not (0 <= int(request.gate_id) <= 0xFFFFFFFF):
                    response = self._err_resp(errors.ERR_INVALID_GATE_ID, f"invalid gate_id {request.gate_id!r}")
                    return response
                if not request.gate_instance_id:
                    response = self._err_resp(errors.ERR_INVALID_GATE_ID, "missing gate_instance_id")
                    return response

            if request.hosted_placement:
                if request.gate_id or request.gate_instance_id:
                    response = self._err_resp(errors.ERR_INVALID_GATE_ID, "hosted placement must not carry a gate")
                    return response
                if request.session_id == 0 or request.login_generation == 0:
                    response = self._err_resp(errors.ERR_INVALID_GATE_ID, "hosted placement requires session_id and login_generation")
                    return response
                if self.publisher is None:
                    response = self._err_resp(errors.ERR_KAFKA_ROUTE, "Kafka route writer is unavailable")
                    return response

            if request.gate_zone_id != 0 and int(request.gate_zone_id) != self.config.zone_id:
                response = self._err_resp(
                    errors.ERR_INVALID_GATE_ID,
                    f"gate zone {request.gate_zone_id} is served by its own scene_manager (this serves zone {self.config.zone_id})",
                )
                return response

            # 0. 请求级幂等去重（enter_scene:dedup:{<pid>}:<reqId>）
            if request.request_id:
                dedupe_fingerprint = enter_scene_request_fingerprint(request)
                dedupe_owner = f"{_DEDUPE_PENDING_PREFIX}{dedupe_fingerprint}:{uuid.uuid4()}"
                for _ in range(2):
                    ok = await self.player_store.setnx_enter_dedupe(
                        player_id, request.request_id, dedupe_owner, config.ENTER_SCENE_DEDUPE_TTL_SEC,
                    )
                    if ok:
                        dedupe_owned = True
                        break
                    existing = await self.player_store.get_enter_dedupe(player_id, request.request_id)
                    if not existing:
                        continue
                    if existing.startswith(_DEDUPE_PENDING_PREFIX):
                        body = existing[len(_DEDUPE_PENDING_PREFIX):]
                        parts = body.split(":", 1)
                        if len(parts) == 2 and parts[0] != dedupe_fingerprint:
                            response = self._err_resp(
                                errors.ERR_ENTER_SCENE_IDEMPOTENCY_CONFLICT,
                                "request_id reused with different EnterScene payload",
                            )
                            return response
                        response = self._err_resp(
                            errors.ERR_ENTER_SCENE_IN_PROGRESS,
                            "EnterScene with same request_id is still in progress",
                        )
                        return response
                    if existing.startswith(_DEDUPE_DONE_PREFIX):
                        parts = existing[len(_DEDUPE_DONE_PREFIX):].split(":", 3)
                        if (
                            len(parts) == 4
                            and parts[0] == "v2"
                            and (parts[1] != keys.revision_text(owner_revision) or parts[2] != dedupe_fingerprint)
                        ):
                            response = self._err_resp(
                                errors.ERR_ENTER_SCENE_IDEMPOTENCY_CONFLICT,
                                "request_id reused with different EnterScene payload or owner_revision",
                            )
                            return response
                        if len(parts) == 4 and parts[0] == "v2" and parts[1] == keys.revision_text(owner_revision):
                            try:
                                cached = self.protocol.parse("scene_manager.EnterSceneResponse", base64.b64decode(parts[3]))
                                if cached.error_code == 0:
                                    response = cached
                                    return response
                            except Exception:
                                pass
                    await self.player_store.delete_enter_dedupe_if_value(player_id, request.request_id, existing)
                if not dedupe_owned:
                    response = self._err_resp(errors.ERR_ENTER_SCENE_IN_PROGRESS, "EnterScene dedupe state changing; retry")
                    return response

            # 1. 读当前位置快照、修订对齐、owner_epoch 与落点会话身份裁决
            target_zone_id = int(request.zone_id)
            if target_zone_id == 0 and int(request.scene_id) != 0:
                if await self.control_store.scene_node_exists(self.config.zone_id, int(request.scene_id)):
                    target_zone_id = self.config.zone_id

            try:
                current_loc, current_loc_raw, current_loc_tag = await self._location_for_revision(player_id, owner_revision)
                observed_epoch = await self.player_store.get_owner_epoch(player_id)
            except Exception as exc:
                response = self._err_resp(errors.ERR_REDIS, f"failed to read player location/epoch: {exc}")
                return response

            observed_loc_raw = current_loc_raw
            cur_identity = (
                PlacementIdentity(int(current_loc.session_id), int(current_loc.login_generation))
                if current_loc is not None
                else PlacementIdentity()
            )
            req_identity = PlacementIdentity(int(request.session_id), int(request.login_generation))
            identity_verdict, identity_record = decide_placement_identity(cur_identity, current_loc is not None, req_identity)
            if not identity_verdict.admits:
                response = self._err_resp(
                    errors.ERR_PLACEMENT_SESSION_SUPERSEDED,
                    f"placement session superseded ({identity_verdict.value})",
                )
                return response

            current_zone_id = 0
            if current_loc is not None:
                current_zone_id = int(current_loc.zone_id)
                if current_zone_id == 0 and int(current_loc.scene_id) != 0:
                    if await self.control_store.scene_node_exists(self.config.zone_id, int(current_loc.scene_id)):
                        current_zone_id = self.config.zone_id

            # 死节点接管检测（持有进程无存活租约且节点已确认死亡并过再入屏障；配置死持有者释放时先核查并围栏写权行）
            taken_over_loc = None
            taken_over_zone = 0
            if current_loc is not None and await self._is_owner_dead(current_loc, current_zone_id):
                holder_lease = await self.player_store.get_holder_lease(player_id)
                if not holder_lease:
                    if self.dead_holder_releaser is not None and int(current_loc.owner_epoch) != 0:
                        try:
                            await self.dead_holder_releaser.release_dead_holder(
                                player_id, int(current_loc.owner_epoch), zone_id=current_zone_id,
                            )
                        except DeadHolderReleaseBlockedError as exc:
                            code = (
                                errors.ERR_HANDOFF_PENDING
                                if exc.outcome == DeadHolderReleaseOutcome.HOLDER_REGISTERED
                                else errors.ERR_SCENE_REENTRY_BARRIER
                            )
                            response = self._err_resp(code, exc.detail)
                            return response
                    taken_over_loc, taken_over_zone = current_loc, current_zone_id
                    current_loc, current_loc_raw, current_zone_id = None, b"", 0

            awaiting_placement = current_loc is not None and not current_loc.node_id and int(current_loc.owner_epoch) != 0
            awaiting_expired = (
                awaiting_placement
                and (self._now_sec() - int(current_loc.update_time)) > config.REDIRECT_TOKEN_TTL_SEC
            )

            if target_zone_id == 0:
                if current_loc is not None and current_loc.node_id and int(current_loc.zone_id) != 0:
                    target_zone_id = int(current_loc.zone_id)
            if target_zone_id == 0:
                target_zone_id = int(request.gate_zone_id) or self.config.zone_id

            # 2. 跨区重定向分支（必须在场景解析之前决定）
            cross_zone = int(request.gate_zone_id) != 0 and target_zone_id != 0 and int(request.gate_zone_id) != target_zone_id
            if cross_zone and request.hosted_placement:
                response = self._err_resp(
                    errors.ERR_INVALID_GATE_ID,
                    f"hosted placement cannot leave zone {request.gate_zone_id}",
                )
                return response
            if cross_zone:
                response = await self._handle_cross_zone_redirect(
                    request=request,
                    target_zone_id=target_zone_id,
                    current_loc=current_loc,
                    current_loc_raw=current_loc_raw,
                    current_zone_id=current_zone_id,
                    observed_loc_raw=observed_loc_raw,
                    observed_epoch=observed_epoch,
                    observed_tag=current_loc_tag,
                    awaiting_placement=awaiting_placement,
                    identity_record=identity_record,
                )
                return response

            # 2b. 玩家主动选线预检（client_channel_pick）
            if request.client_channel_pick and int(request.scene_id) != 0:
                pick_err, charge_channel_cooldown = await self._check_client_channel_pick(
                    request, current_loc, target_zone_id,
                )
                if pick_err is not None:
                    response = pick_err
                    return response

            # 3. 目标场景解析（evacuation / awaiting pending_scene_conf_id / resume_scene / direct / world channel）
            scene_conf_id = int(request.scene_conf_id)
            conf_from_pending = False
            if (
                awaiting_placement
                and not awaiting_expired
                and current_zone_id == target_zone_id
                and int(request.scene_id) == 0
                and scene_conf_id == 0
            ):
                scene_conf_id = int(current_loc.pending_scene_conf_id)
                conf_from_pending = scene_conf_id != 0

            resume_scene_id = 0
            if (
                int(request.zone_id) == 0
                and int(request.scene_id) == 0
                and scene_conf_id == 0
                and current_loc is not None
                and not awaiting_placement
                and current_loc.node_id
                and int(current_loc.scene_id) != 0
                and current_zone_id == target_zone_id
            ):
                if not await self.control_store.is_scene_draining(target_zone_id, int(current_loc.scene_id)):
                    resume_scene_id = int(current_loc.scene_id)

            requested_scene_id = resume_scene_id if resume_scene_id != 0 else int(request.scene_id)
            scene_id, node_id, reserved, resolve_err = await self._resolve_target_scene(
                request=request,
                requested_scene_id=requested_scene_id,
                scene_conf_id=scene_conf_id,
                target_zone_id=target_zone_id,
                current_loc=current_loc,
                current_zone_id=current_zone_id,
            )
            if resolve_err is not None and resume_scene_id != 0 and not isinstance(resolve_err, errors.ReentryBarrierPendingError):
                scene_id, node_id, reserved, resolve_err = await self._resolve_scene_for_enter(
                    0, 0, target_zone_id, player_id,
                )
            if resolve_err is not None and conf_from_pending:
                scene_id, node_id, reserved, resolve_err = await self._resolve_scene_for_enter(
                    0, 0, target_zone_id, player_id,
                )
            if resolve_err is not None:
                code = (
                    errors.ERR_SCENE_REENTRY_BARRIER
                    if isinstance(resolve_err, errors.ReentryBarrierPendingError)
                    else errors.ERR_NO_AVAILABLE_NODE
                )
                response = self._err_resp(code, str(resolve_err))
                return response

            if request.gate_id and (not node_id.isdigit() or int(node_id) > 0xFFFFFFFF):
                if reserved:
                    await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                response = self._err_resp(errors.ERR_INVALID_NODE_ID, f"invalid scene node id {node_id!r}")
                return response

            hosted_instance = ""
            if request.hosted_placement:
                if not node_id.isdigit() or int(node_id) > 0xFFFFFFFF:
                    if reserved:
                        await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                    response = self._err_resp(errors.ERR_INVALID_NODE_ID, f"invalid scene node id {node_id!r}")
                    return response
                inst = await self.node_registry.get_unique_scene_instance_uuid(target_zone_id, node_id)
                if not inst:
                    if reserved:
                        await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                    response = self._err_resp(
                        errors.ERR_INVALID_NODE_ID,
                        f"scene node {node_id} has no unique registered instance",
                    )
                    return response
                hosted_instance = inst

            # 3a. 跨节点换手门检查
            same_placement = (
                current_loc is not None
                and int(current_loc.scene_id) == scene_id
                and current_loc.node_id == node_id
                and current_zone_id != 0
                and current_zone_id == target_zone_id
            )
            same_physical_node = (
                current_loc is not None
                and bool(current_loc.node_id)
                and current_loc.node_id == node_id
                and current_zone_id != 0
                and current_zone_id == target_zone_id
            )
            cross_node_handoff = current_loc is not None and not awaiting_placement and not same_placement and not same_physical_node

            handoff_mint = not same_physical_node
            required_marker = ""
            if cross_node_handoff:
                gate_err, handoff_mint, required_marker = await self._check_handoff_gate(
                    request, current_loc, observed_epoch, site="scene handoff",
                )
                if gate_err is not None:
                    if reserved:
                        await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                    response = gate_err
                    return response

            # 3b. 查询玩家 home_zone（仅当需发路由/托管落点时）
            home_zone_id = 0
            if request.gate_id or request.hosted_placement:
                home_zone_id, hz_err = await self._resolve_home_zone(
                    request, reject_unmapped_travel=awaiting_placement,
                )
                if hz_err is not None:
                    if reserved:
                        await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                    response = hz_err
                    return response

            # 3c. 同节点 owner_epoch 键丢失补种（SET NX）与同节点 epoch=0 例外铸造
            if same_physical_node and observed_epoch == 0 and int(current_loc.owner_epoch) != 0:
                recorded_epoch = int(current_loc.owner_epoch)
                reseeded = await self.player_store.setnx_owner_epoch(player_id, recorded_epoch)
                if not reseeded:
                    if reserved:
                        await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                    response = self._err_resp(errors.ERR_OWNER_EPOCH_CONFLICT, "owner epoch changed while reseeding")
                    return response
                observed_epoch = recorded_epoch

            same_node_zero_mint = same_physical_node and int(current_loc.owner_epoch) == 0
            should_mint = handoff_mint if cross_node_handoff else (not same_physical_node or same_node_zero_mint)

            # 4. 同落点重连快路径：永不铸造 epoch，仅更新身份且 Kafka 失败不回滚身份
            if same_placement:
                await self._commit_scene_member(player_id, scene_id, target_zone_id, node_id)
                if identity_record != cur_identity:
                    cloned = self.protocol.message("storage.PlayerLocation")
                    cloned.CopyFrom(current_loc)
                    cloned.session_id = identity_record.session_id
                    cloned.login_generation = identity_record.login_generation
                    updated_raw = cloned.SerializeToString()
                    updated_tag = keys.location_revision_tag(owner_revision, updated_raw)
                    ok = await self.player_store.set_location_if_epoch(
                        player_id=player_id,
                        observed_epoch=observed_epoch,
                        location_bytes=updated_raw,
                        expect_mode="present",
                        expect_raw=current_loc_raw,
                        owner_revision=owner_revision,
                        new_tag=updated_tag,
                        observed_tag=current_loc_tag,
                    )
                    if ok <= 0:
                        response = self._err_resp(
                            errors.ERR_OWNER_EPOCH_CONFLICT,
                            "epoch or location advanced concurrently during same-placement identity update",
                        )
                        return response

                routed_epoch = 0 if int(current_loc.owner_epoch) == 0 else observed_epoch
                if request.gate_id:
                    try:
                        await self._push_route_player(
                            request, node_id=node_id, scene_id=scene_id, home_zone_id=home_zone_id, owner_epoch=routed_epoch,
                        )
                    except Exception as exc:
                        logger.error("same-placement route to gate failed player=%d error=%s", player_id, exc)
                        response = self._err_resp(errors.ERR_KAFKA_ROUTE, f"route player to gate failed: {exc}")
                        return response
                elif request.hosted_placement:
                    try:
                        await self._push_hosted_placement(
                            request,
                            node_id=node_id,
                            instance_id=hosted_instance,
                            scene_id=scene_id,
                            home_zone_id=home_zone_id,
                            owner_epoch=routed_epoch,
                        )
                    except Exception as exc:
                        logger.error("same-placement hosted route failed player=%d error=%s", player_id, exc)
                        response = self._err_resp(errors.ERR_KAFKA_ROUTE, f"route hosted player failed: {exc}")
                        return response
                response = self.protocol.message("scene_manager.EnterSceneResponse", error_code=errors.ERR_OK)
                return response

            # 5. 非频道自动选线时原子预占成员（防止 destroy-while-entering）
            if not reserved:
                count = await self.control_store.atomic_add_scene_member(target_zone_id, scene_id, node_id, player_id)
                if count < 0:
                    response = self._err_resp(errors.ERR_NO_AVAILABLE_NODE, f"scene {scene_id} disappeared during enter")
                    return response

            # 6. 原子写入 PlayerLocation（铸造或维持 epoch）
            try:
                placed = await self._place_player_location(
                    player_id=player_id,
                    scene_id=scene_id,
                    node_id=node_id,
                    zone_id=target_zone_id,
                    observed_epoch=observed_epoch,
                    mint=should_mint,
                    required_marker=required_marker,
                    pending_scene_conf_id=0,
                    identity=identity_record,
                    observed_raw=observed_loc_raw,
                    owner_revision=owner_revision,
                    observed_tag=current_loc_tag,
                )
            except errors.HandoffWithdrawnError:
                await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                response = self._err_resp(errors.ERR_HANDOFF_PENDING, "handoff marker withdrawn by source scene; retry")
                return response
            except errors.OwnerEpochConflictError:
                await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                response = self._err_resp(errors.ERR_OWNER_EPOCH_CONFLICT, "owner epoch or location advanced concurrently")
                return response
            except Exception as exc:
                await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                response = self._err_resp(errors.ERR_UPDATE_LOCATION, f"failed to update location: {exc}")
                return response

            # 6a & 6b. 确认新成员并移除旧场景成员
            await self._commit_scene_member(player_id, scene_id, target_zone_id, node_id)
            if current_loc is not None and int(current_loc.scene_id) != 0:
                await self._release_scene_member_best_effort(
                    player_id, int(current_loc.scene_id), current_zone_id, current_loc.node_id,
                )

            # 6d. 可选线性一致 SQL OwnerAuthority 强门禁（必须在 Kafka 路由提交前完成）
            if self._owner_authority_mode() == "enforce":
                assert self.owner_authority_bridge is not None
                try:
                    await self.owner_authority_bridge.sync_enter(
                        player_id=player_id,
                        scene_id=scene_id,
                        node_id=node_id,
                        zone_id=target_zone_id,
                        redis_epoch=placed.epoch,
                        owner_revision=owner_revision,
                        enforce=True,
                    )
                except Exception as exc:
                    restored, echo = await self._rollback_placement(
                        player_id=player_id,
                        old_loc=current_loc,
                        old_raw=current_loc_raw,
                        old_zone_id=current_zone_id,
                        placed=placed,
                        required_marker=required_marker,
                        keep_minted_epoch=same_node_zero_mint,
                    )
                    if restored:
                        await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                        if current_loc is not None and int(current_loc.scene_id) != 0:
                            await self._commit_scene_member(
                                player_id, int(current_loc.scene_id), current_zone_id, current_loc.node_id,
                            )
                    response = self._err_resp(errors.ERR_OWNER_EPOCH_CONFLICT, f"owner authority rejected enter: {exc}")
                    response.owner_epoch_after_rollback = echo
                    return response

            # 7. Kafka 提交点（broker ACK 之前失败须单调回滚）
            if request.gate_id or request.hosted_placement:
                try:
                    if request.hosted_placement:
                        await self._push_hosted_placement(
                            request,
                            node_id=node_id,
                            instance_id=hosted_instance,
                            scene_id=scene_id,
                            home_zone_id=home_zone_id,
                            owner_epoch=placed.epoch,
                        )
                    else:
                        await self._push_route_player(
                            request, node_id=node_id, scene_id=scene_id, home_zone_id=home_zone_id, owner_epoch=placed.epoch,
                        )
                except Exception as exc:
                    logger.error("Kafka route publish failed; rolling back placement player=%d error=%s", player_id, exc)
                    restored, echo = await self._rollback_placement(
                        player_id=player_id,
                        old_loc=current_loc,
                        old_raw=current_loc_raw,
                        old_zone_id=current_zone_id,
                        placed=placed,
                        required_marker=required_marker,
                        keep_minted_epoch=same_node_zero_mint,
                    )
                    if restored:
                        await self._release_scene_member_best_effort(player_id, scene_id, target_zone_id, node_id)
                        if current_loc is not None and int(current_loc.scene_id) != 0:
                            await self._commit_scene_member(
                                player_id, int(current_loc.scene_id), current_zone_id, current_loc.node_id,
                            )
                    response = self._err_resp(errors.ERR_KAFKA_ROUTE, f"route player failed: {exc}")
                    response.owner_epoch_after_rollback = echo
                    return response

            # 7b. Kafka ACK 之后异步通知旧场景节点 ReleasePlayer
            if current_loc is not None and current_loc.node_id and not same_physical_node:
                self._dispatch_release_player(
                    old_zone_id=current_zone_id,
                    old_node_id=current_loc.node_id,
                    player_id=player_id,
                    target_scene_id=scene_id,
                    target_node_id=node_id,
                    expected_owner_epoch=observed_epoch,
                )

            if taken_over_loc is not None:
                await self._release_scene_member_best_effort(
                    player_id, int(taken_over_loc.scene_id), taken_over_zone, taken_over_loc.node_id,
                )

            # 7c. 可选线性一致 SQL OwnerAuthority shadow 双写（非阻塞审计）
            if self._owner_authority_mode() == "shadow":
                assert self.owner_authority_bridge is not None
                await self.owner_authority_bridge.sync_enter(
                    player_id=player_id,
                    scene_id=scene_id,
                    node_id=node_id,
                    zone_id=target_zone_id,
                    redis_epoch=placed.epoch,
                    owner_revision=owner_revision,
                    enforce=False,
                )

            response = self.protocol.message("scene_manager.EnterSceneResponse", error_code=errors.ERR_OK)
            return response
        finally:
            # 1. 主动选线冷却（0 或 18 起算）
            if charge_channel_cooldown and response is not None and response.error_code in (errors.ERR_OK, errors.ERR_HANDOFF_PENDING):
                cooldown_sec = self.config.effective_channel_switch_cooldown_sec()
                if cooldown_sec > 0:
                    try:
                        await self.player_store.set_channel_switch_cooldown(player_id, cooldown_sec)
                    except Exception as exc:
                        logger.warning("write channel switch cooldown failed player=%d error=%s", player_id, exc)

            # 2. 去重占位收尾（在回显 correlation_id 之前写缓存，确保重放回显的是当次 correlation_id）
            if dedupe_owned:
                if response is None or response.error_code != errors.ERR_OK:
                    try:
                        await self.player_store.delete_enter_dedupe_if_value(player_id, request.request_id, dedupe_owner)
                    except Exception as exc:
                        logger.warning("release enter dedupe failed player=%d error=%s", player_id, exc)
                else:
                    cached_bytes = response.SerializeToString()
                    cached_val = (
                        f"{_DEDUPE_DONE_PREFIX}v2:{keys.revision_text(owner_revision)}:"
                        f"{dedupe_fingerprint}:{base64.b64encode(cached_bytes).decode('ascii')}"
                    )
                    try:
                        await self.player_store.complete_enter_dedupe(
                            player_id=player_id,
                            request_id=request.request_id,
                            expected_owner=dedupe_owner,
                            cached_value=cached_val,
                            ttl_sec=config.ENTER_SCENE_DEDUPE_TTL_SEC,
                            owner_revision=owner_revision,
                        )
                    except Exception as exc:
                        logger.warning("complete enter dedupe failed player=%d error=%s", player_id, exc)

            # 3. 永远回显 player_id 与本次请求的 correlation_id（覆盖去重重放与所有错误）
            if response is not None:
                response.player_id = player_id
                response.correlation_id = int(request.correlation_id)

    async def _handle_cross_zone_redirect(
        self,
        *,
        request,
        target_zone_id: int,
        current_loc,
        current_loc_raw: bytes,
        current_zone_id: int,
        observed_loc_raw: bytes,
        observed_epoch: int,
        observed_tag: str,
        awaiting_placement: bool,
        identity_record: PlacementIdentity,
    ):
        player_id = int(request.player_id)
        owner_revision = int(request.owner_revision)
        leaving_zone = current_loc is not None and current_zone_id != target_zone_id

        # 离区前只读核对：目标地图是否在 World 表且目标 zone 有大世界节点 + 玩家已登记 home_zone
        if leaving_zone:
            conf_id = int(request.scene_conf_id) or (self._world_conf_ids[0] if self._world_conf_ids else 0)
            if conf_id == 0 or conf_id not in self._world_conf_set:
                return self._err_resp(
                    errors.ERR_NO_AVAILABLE_NODE,
                    f"no world map {conf_id} configured for target zone {target_zone_id}",
                )
            try:
                target_scenes = await self.node_registry.list_scene_nodes(target_zone_id)
            except Exception as exc:
                logger.warning("target zone scene registry check failed zone=%d error=%s", target_zone_id, exc)
                target_scenes = None
            if target_scenes is not None:
                has_world = any(
                    not _is_admission_closed(n)
                    and (not self.config.strict_node_type_separation or errors.is_world_hosting_type(int(n.scene_node_type)))
                    for n in target_scenes
                )
                if not has_world:
                    return self._err_resp(
                        errors.ERR_NO_AVAILABLE_NODE,
                        f"target zone {target_zone_id} has no available world scene node",
                    )

            _, hz_err = await self._resolve_home_zone(request, reject_unmapped_travel=True)
            if hz_err is not None:
                return hz_err

        mint = True
        required_marker = ""
        if leaving_zone and not awaiting_placement:
            gate_err, mint, required_marker = await self._check_handoff_gate(
                request, current_loc, observed_epoch, site="cross-zone redirect",
            )
            if gate_err is not None:
                return gate_err

        # 先选目标 zone Gate 并签发令牌（纯读无副作用）
        try:
            gate_nodes = await self.node_registry.list_gate_nodes(target_zone_id)
            try:
                draining_ids, stale_ids = await self.control_store.get_gate_marks(
                    target_zone_id, [int(g.node_id) for g in gate_nodes],
                )
            except Exception:
                draining_ids, stale_ids = set(), set()
            redirect = select_gate_and_sign_redirect(
                protocol=self.protocol,
                gate_nodes=gate_nodes,
                draining_ids=draining_ids,
                stale_ids=stale_ids,
                require_client_endpoint=self.config.require_client_endpoint,
                secret=self.config.gate_token_secret,
                target_zone_id=target_zone_id,
                player_id=player_id,
                now_unix_sec=self._now_sec(),
                ws_url_lookup=lambda gid: self.node_registry.get_gate_ws_url(target_zone_id, gid),
                rng=self._rng,
            )
        except Exception as exc:
            logger.error("cross-zone gate assignment failed player=%d target_zone=%d error=%s", player_id, target_zone_id, exc)
            return self._err_resp(errors.ERR_NO_AVAILABLE_NODE, f"no gate available in target zone {target_zone_id}")

        placed: PlacedLocation | None = None
        if leaving_zone:
            try:
                placed = await self._place_player_location(
                    player_id=player_id,
                    scene_id=0,
                    node_id="",
                    zone_id=target_zone_id,
                    observed_epoch=observed_epoch,
                    mint=mint,
                    required_marker=required_marker,
                    pending_scene_conf_id=int(request.scene_conf_id),
                    identity=identity_record,
                    observed_raw=observed_loc_raw,
                    owner_revision=owner_revision,
                    observed_tag=observed_tag,
                )
            except errors.HandoffWithdrawnError:
                return self._err_resp(errors.ERR_HANDOFF_PENDING, "handoff marker withdrawn during cross-zone redirect")
            except errors.OwnerEpochConflictError:
                return self._err_resp(errors.ERR_OWNER_EPOCH_CONFLICT, "owner epoch or location advanced concurrently")
            except Exception as exc:
                return self._err_resp(errors.ERR_UPDATE_LOCATION, f"failed to write awaiting placement: {exc}")

            if current_loc is not None and int(current_loc.scene_id) != 0:
                await self._release_scene_member_best_effort(
                    player_id, int(current_loc.scene_id), current_zone_id, current_loc.node_id,
                )

        if request.gate_id:
            try:
                await self._push_redirect_to_gate(request, redirect)
            except Exception as exc:
                logger.error("Kafka RedirectToGate publish failed player=%d error=%s", player_id, exc)
                resp = self._err_resp(errors.ERR_KAFKA_ROUTE, f"redirect to gate failed: {exc}")
                if leaving_zone and placed is not None:
                    restored, echo = await self._rollback_placement(
                        player_id=player_id,
                        old_loc=current_loc,
                        old_raw=current_loc_raw,
                        old_zone_id=current_zone_id,
                        placed=placed,
                        required_marker=required_marker,
                        keep_minted_epoch=False,
                    )
                    if restored and current_loc is not None and int(current_loc.scene_id) != 0:
                        await self._commit_scene_member(
                            player_id, int(current_loc.scene_id), current_zone_id, current_loc.node_id,
                        )
                    resp.owner_epoch_after_rollback = echo
                return resp

        return self.protocol.message(
            "scene_manager.EnterSceneResponse",
            error_code=errors.ERR_OK,
            redirect=redirect,
        )

    async def _check_client_channel_pick(self, request, current_loc, target_zone_id: int) -> tuple[object | None, bool]:
        scene_id = int(request.scene_id)
        req_conf = int(request.scene_conf_id)
        try:
            owner_conf_id, membership = await self._locate_world_channel(target_zone_id, scene_id, req_conf)
        except Exception as exc:
            return self._err_resp(errors.ERR_REDIS, f"failed to read world channel membership: {exc}"), False
        if membership == "not_member":
            return None, False
        if membership == "draining":
            return self._err_resp(errors.ERR_CHANNEL_UNAVAILABLE, f"channel {scene_id} is closing"), False
        if req_conf != 0 and req_conf != owner_conf_id:
            return self._err_resp(
                errors.ERR_CHANNEL_UNAVAILABLE,
                f"channel {scene_id} belongs to map {owner_conf_id}, not {req_conf}",
            ), False
        if self.config.channel_switch_disabled:
            return self._err_resp(errors.ERR_CHANNEL_UNAVAILABLE, "channel switch is disabled"), False

        max_players = self.config.effective_channel_max_players()
        players = await self.control_store.get_scene_member_count(target_zone_id, scene_id)
        if players >= max_players:
            return self._err_resp(errors.ERR_CHANNEL_FULL, f"channel {scene_id} is full ({players}/{max_players})"), False

        is_switch = (
            current_loc is not None
            and bool(current_loc.node_id)
            and int(current_loc.scene_id) != 0
            and int(current_loc.scene_id) != scene_id
        )
        if not is_switch:
            return None, False
        cooldown_sec = self.config.effective_channel_switch_cooldown_sec()
        if cooldown_sec <= 0:
            return None, False
        if await self.player_store.has_channel_switch_cooldown(int(request.player_id)):
            return self._err_resp(
                errors.ERR_CHANNEL_SWITCH_COOLDOWN,
                f"channel switch cooldown active ({cooldown_sec}s)",
            ), False
        return None, True

    async def _resolve_target_scene(
        self, *, request, requested_scene_id: int, scene_conf_id: int,
        target_zone_id: int, current_loc, current_zone_id: int,
    ) -> tuple[int, str, bool, Exception | None]:
        if request.evacuation:
            source_node = (
                current_loc.node_id
                if current_loc is not None and current_zone_id != 0 and current_zone_id == target_zone_id
                else ""
            )
            return await self._resolve_evacuation_target(request, source_node, target_zone_id)
        return await self._resolve_scene_for_enter(requested_scene_id, scene_conf_id, target_zone_id, int(request.player_id))

    async def _resolve_evacuation_target(
        self, request, source_node: str, zone_id: int,
    ) -> tuple[int, str, bool, Exception | None]:
        player_id = int(request.player_id)
        orig_scene_id = int(request.scene_id)
        if orig_scene_id != 0:
            owner = await self.control_store.get_scene_node(zone_id, orig_scene_id)
            if (
                owner
                and owner != source_node
                and not self.node_registry.is_node_identity_ambiguous(zone_id, owner)
                and not self.node_registry.is_node_draining(zone_id, owner)
                and await self._is_node_alive(zone_id, owner)
            ):
                return orig_scene_id, owner, False, None

        conf_id = int(request.scene_conf_id)
        if conf_id != 0 and conf_id in self._world_conf_set:
            picked = await self._reserve_world_channel(
                zone_id, conf_id, player_id, create_on_demand=False, exclude_draining=True,
            )
            if picked is not None:
                return picked[0], picked[1], True, None

        default_conf = self._world_conf_ids[0] if self._world_conf_ids else 0
        if default_conf != 0 and default_conf != conf_id:
            picked = await self._reserve_world_channel(
                zone_id, default_conf, player_id, create_on_demand=False, exclude_draining=True,
            )
            if picked is not None:
                return picked[0], picked[1], True, None

        return 0, "", False, RuntimeError(f"no evacuation destination in zone {zone_id}")

    async def _resolve_scene_for_enter(
        self, scene_id: int, scene_conf_id: int, zone_id: int, player_id: int,
    ) -> tuple[int, str, bool, Exception | None]:
        if scene_id != 0:
            owner = await self.control_store.get_scene_node(zone_id, scene_id)
            if not owner:
                return 0, "", False, LookupError(f"scene {scene_id} does not exist in zone {zone_id}")
            if self.node_registry.is_node_identity_ambiguous(zone_id, owner):
                return 0, "", False, RuntimeError(f"scene {scene_id} maps to ambiguous node {owner}")
            if not await self._is_node_alive(zone_id, owner):
                death_at = await self.control_store.get_node_death_at(zone_id, owner)
                if death_at is not None and (self._now_sec() - death_at) < self.config.reentry_barrier_sec:
                    return 0, "", False, errors.ReentryBarrierPendingError(
                        f"scene {scene_id} owner node {owner} reentry barrier pending",
                    )
                return 0, "", False, RuntimeError(f"scene {scene_id} owner node {owner} is not alive")
            return scene_id, owner, False, None

        conf_id = scene_conf_id or (self._world_conf_ids[0] if self._world_conf_ids else 0)
        if conf_id == 0:
            return 0, "", False, RuntimeError("no scene_conf_id provided and World table is empty")
        picked = await self._reserve_world_channel(zone_id, conf_id, player_id, create_on_demand=True)
        if picked is None:
            return 0, "", False, RuntimeError(f"no available channel for conf {conf_id} in zone {zone_id}")
        return picked[0], picked[1], True, None

    # ── RPC 4: LeaveScene ────────────────────────────────────────────────────

    async def _delete_location_guarded(
        self, *, player_id: int, expected_raw: bytes, zone_id: int, scene_id: int,
        mode: str, epoch: int, expected_tag: str, owner_revision: int,
    ) -> str:
        res = await self.player_store.delete_location_if_unchanged(
            player_id=player_id,
            expected_raw=expected_raw,
            mode=mode,
            epoch=epoch,
            expected_tag=expected_tag,
            owner_revision=owner_revision,
        )
        if res == -2:
            return "not_released"
        if res == 1:
            outcome = "deleted"
        elif res == 0:
            outcome = "already_gone"
        elif res == -1:
            outcome = "superseded"
        else:
            raise RuntimeError(f"unexpected delete_location_if_unchanged reply: {res}")
        await self._remove_scene_player(player_id, scene_id, zone_id)
        return outcome

    async def leave_scene(self, request, context=None, path: str = ""):
        player_id = int(request.player_id)
        scene_id = int(request.scene_id)
        try:
            current_loc, raw, tag = await self._read_location_snapshot(player_id)
        except errors.LocationCorruptError:
            return self._empty()
        except Exception as exc:
            await self._abort(context, grpc.StatusCode.UNAVAILABLE, f"read location of player {player_id}: {exc}")

        if not raw:
            await self._remove_scene_player(player_id, scene_id, 0)
            return self._empty()

        current_role = await self._lookup_current_role(player_id, context)
        obs = await self.player_store.observe_location_revision(player_id, current_role.owner_revision)
        if obs != 1:
            await self._abort(context, grpc.StatusCode.FAILED_PRECONDITION, "location revision floor rejected")

        tag_revision = keys.parse_tagged_location_revision(tag, raw)
        if tag_revision != 0 and tag_revision != current_role.owner_revision:
            await self._location_for_revision(player_id, current_role.owner_revision)
            return self._empty()
        if tag_revision != 0 and (int(request.session_id) == 0 or int(request.login_generation) == 0):
            return self._empty()

        if int(current_loc.scene_id) != scene_id:
            await self._remove_scene_player(player_id, scene_id, 0)
            return self._empty()

        loc_identity = PlacementIdentity(int(current_loc.session_id), int(current_loc.login_generation))
        req_identity = PlacementIdentity(int(request.session_id), int(request.login_generation))
        identity_verdict = decide_leave_identity(loc_identity, req_identity)
        if not identity_verdict.deletes:
            return self._empty()

        # 持有者释放证明判定
        observed_epoch = await self.player_store.get_owner_epoch(player_id)
        epoch = observed_epoch if observed_epoch != 0 else int(current_loc.owner_epoch)
        no_holder = not current_loc.node_id
        loc_zone = int(current_loc.zone_id)
        if loc_zone == 0 and int(current_loc.scene_id) != 0:
            if await self.control_store.scene_node_exists(self.config.zone_id, int(current_loc.scene_id)):
                loc_zone = self.config.zone_id

        owner_dead = False
        marker_released = False
        if epoch != 0 and not no_holder:
            owner_dead = await self._is_owner_dead(current_loc, loc_zone)
            marker_raw = await self.player_store.get_handoff_marker(player_id)
            marker_released = keys.marker_releases_epoch(marker_raw, epoch)
            if owner_dead and not marker_released:
                if await self.player_store.get_holder_lease(player_id):
                    owner_dead = False

        release_verdict = decide_holder_release(
            no_holder=no_holder,
            owner_dead=owner_dead,
            marker_released=marker_released,
            epoch=epoch,
            identity_proven=(identity_verdict == LeaveIdentityVerdict.DELETED),
        )

        if not release_verdict.deletes:
            await self._keep_location_until_released(
                context=context,
                player_id=player_id,
                scene_id=scene_id,
                loc=current_loc,
                loc_zone=loc_zone,
                epoch=epoch,
                verdict=release_verdict,
                req_identity=req_identity,
            )

        if (
            release_verdict == HolderReleaseVerdict.OWNER_DEAD
            and self.dead_holder_releaser is not None
            and int(current_loc.owner_epoch) != 0
        ):
            try:
                await self.dead_holder_releaser.release_dead_holder(
                    player_id,
                    int(current_loc.owner_epoch),
                    zone_id=loc_zone or self.config.zone_id,
                )
            except DeadHolderReleaseBlockedError as exc:
                if exc.keeps_location:
                    logger.info(
                        "leave scene dead holder release deferred player=%d scene=%d detail=%s",
                        player_id, scene_id, exc.detail,
                    )
                else:
                    logger.error(
                        "leave scene dead holder release failed player=%d scene=%d detail=%s",
                        player_id, scene_id, exc.detail,
                    )
                await self._abort(context, exc.grpc_code, exc.detail)

        mode = "released" if release_verdict == HolderReleaseVerdict.RELEASED else "any"
        outcome = await self._delete_location_guarded(
            player_id=player_id,
            expected_raw=raw,
            zone_id=int(current_loc.zone_id),
            scene_id=scene_id,
            mode=mode,
            epoch=epoch,
            expected_tag=tag,
            owner_revision=current_role.owner_revision,
        )
        if outcome == "not_released":
            pending_verdict = (
                HolderReleaseVerdict.PENDING
                if identity_verdict == LeaveIdentityVerdict.DELETED
                else HolderReleaseVerdict.PENDING_UNPROVEN
            )
            await self._keep_location_until_released(
                context=context,
                player_id=player_id,
                scene_id=scene_id,
                loc=current_loc,
                loc_zone=loc_zone,
                epoch=epoch,
                verdict=pending_verdict,
                req_identity=req_identity,
            )

        if outcome != "deleted":
            logger.info(
                "player leave scene location not deleted player=%d scene=%d outcome=%s holder_release=%s",
                player_id, scene_id, outcome, release_verdict.value,
            )
            return self._empty()

        if self.owner_authority_bridge is not None:
            try:
                await self.owner_authority_bridge.sync_leave(
                    player_id=player_id,
                    scene_id=scene_id,
                    node_id=current_loc.node_id,
                    zone_id=int(current_loc.zone_id) or loc_zone,
                )
            except Exception as exc:
                await self._abort(
                    context,
                    grpc.StatusCode.UNAVAILABLE,
                    f"release owner authority for player {player_id}: {exc}",
                )

        logger.info(
            "player left scene player=%d scene=%d node=%s outcome=%s holder_release=%s owner_epoch=%d",
            player_id, scene_id, current_loc.node_id, outcome, release_verdict.value, epoch,
        )
        return self._empty()

    async def _keep_location_until_released(
        self, *, context, player_id: int, scene_id: int, loc, loc_zone: int,
        epoch: int, verdict: HolderReleaseVerdict, req_identity: PlacementIdentity,
    ) -> None:
        scope = f"{epoch}:{loc.node_id}:{loc.session_id}:{loc.login_generation}"
        wait_ttl = self.config.effective_leave_release_wait_sec() + 600
        waited_ms = await self.player_store.note_leave_release_wait(player_id, scope, wait_ttl)
        if verdict == HolderReleaseVerdict.PENDING and loc_zone > 0 and loc.node_id:
            self._dispatch_session_ended_release(
                zone_id=loc_zone,
                node_id=loc.node_id,
                player_id=player_id,
                expected_owner_epoch=epoch,
                covered=req_identity,
            )
        detail = (
            f"{_HOLDER_RELEASE_PENDING_PREFIX}: player={player_id} scene={scene_id} "
            f"node={loc.node_id} owner_epoch={epoch} verdict={verdict.value} waited={waited_ms // 1000}s"
        )
        logger.info("leave scene keeping location: %s", detail)
        await self._abort(context, grpc.StatusCode.FAILED_PRECONDITION, detail)
