"""断线租约监视器、离场清理通知（SceneManager.LeaveScene）与 Gate Kafka 过期事件发布。"""

from __future__ import annotations

import asyncio
import bisect
import logging
import math
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol as TypingProtocol

import grpc
from google.protobuf.message import DecodeError

from ..grpc_runtime import GrpcClient
from ..protocol import Protocol
from . import keys
from .config import (
    AFK_PASS_LEASE_TTL_SEC,
    AFK_PASS_PROBE_RETRY_SEC,
    SCENE_MANAGER_NODE_TYPE,
    PlayerLocatorConfig,
    parse_scene_manager_endpoints,
)
from .hosted_resume import resume_hosted_if_lost
from .store import (
    AfkPassState,
    CommitLeaseResult,
    LeaseClaim,
    RearmLeaseResult,
    SessionStore,
)

logger = logging.getLogger(__name__)

_WAKE_QUEUE_CAPACITY = 1024
_LEAVE_SCENE_PATH = "/scene_manager.SceneManager/LeaveScene"
_ENTER_SCENE_PATH = "/scene_manager.SceneManager/EnterScene"
_LEASE_EXPIRED_EVENT_NAME = "ContractsKafkaPlayerLeaseExpiredEvent"


class HolderReleasePendingError(RuntimeError):
    """SceneManager.LeaveScene 返回 FAILED_PRECONDITION，表示持有节点尚未释放该玩家。"""


class SceneManagerClient(TypingProtocol):
    async def leave_scene(
        self, *, player_id: int, scene_id: int, session_id: int, login_generation: int,
    ) -> None: ...

    async def enter_scene(
        self,
        *,
        player_id: int,
        session_id: int,
        login_generation: int,
        owner_revision: int,
        gate_zone_id: int,
        hosted_placement: bool = True,
    ): ...


class GateCommandPublisher(TypingProtocol):
    async def publish_gate_command(self, gate_node_id: int, player_id: int, command) -> None: ...


class GrpcSceneManagerClient:
    """调用 SceneManager.LeaveScene / EnterScene 的 gRPC 适配器。"""

    def __init__(self, protocol: Protocol, client: GrpcClient, *, timeout_sec: float = 5.0) -> None:
        self.protocol = protocol
        self.client = client
        self.timeout_sec = timeout_sec

    async def leave_scene(
        self, *, player_id: int, scene_id: int, session_id: int, login_generation: int,
    ) -> None:
        request = self.protocol.message(
            "scene_manager.LeaveSceneRequest",
            player_id=player_id,
            scene_id=scene_id,
            session_id=session_id,
            login_generation=login_generation,
        )
        await self.client.call(_LEAVE_SCENE_PATH, request.SerializeToString(), timeout=self.timeout_sec)

    async def enter_scene(
        self,
        *,
        player_id: int,
        session_id: int,
        login_generation: int,
        owner_revision: int,
        gate_zone_id: int,
        hosted_placement: bool = True,
    ):
        request = self.protocol.message(
            "scene_manager.EnterSceneRequest",
            player_id=player_id,
            session_id=session_id,
            login_generation=login_generation,
            owner_revision=owner_revision,
            gate_zone_id=gate_zone_id,
            hosted_placement=hosted_placement,
        )
        raw = await self.client.call(_ENTER_SCENE_PATH, request.SerializeToString(), timeout=self.timeout_sec)
        return self.protocol.parse("scene_manager.EnterSceneResponse", raw)


def _format_node_grpc_address(info) -> str:
    """从 etcd ``NodeInfo`` 提取 SceneManager 的 gRPC 通信地址。"""
    endpoint = info.grpc_endpoint if info.HasField("grpc_endpoint") and info.grpc_endpoint.ip else info.endpoint
    ip = str(endpoint.ip).strip()
    port = int(endpoint.port)
    if not ip or port <= 0:
        return ""
    return f"[{ip}]:{port}" if ":" in ip else f"{ip}:{port}"


class RoutingSceneManagerClient:
    """按区号路由的多区 SceneManager gRPC 客户端（静态映射 -> etcd 服务发现 -> 默认回退）。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        default_endpoint: str = "",
        zone_endpoints: Mapping[int, str] | Sequence[tuple[int, str]] | str | None = None,
        directory=None,
        timeout_sec: float = 5.0,
        client_factory: Callable[[str, float], object] | None = None,
    ) -> None:
        self.protocol = protocol
        self.default_endpoint = default_endpoint.strip()
        self._zone_endpoints: dict[int, str] = dict(parse_scene_manager_endpoints(zone_endpoints))
        self.directory = directory
        self.timeout_sec = timeout_sec
        self._client_factory = client_factory or (lambda address, timeout: GrpcClient(address, timeout=timeout))
        self._clients: dict[str, object] = {}

    async def resolve_endpoint(self, zone_id: int) -> str:
        """按区号解析目标 SceneManager 地址：静态覆盖 > etcd 发现 > 默认地址。"""
        if zone_id > 0:
            override = self._zone_endpoints.get(zone_id)
            if override:
                return override
            if self.directory is not None:
                try:
                    candidates = await self.directory.candidates(SCENE_MANAGER_NODE_TYPE, zone_id)
                except Exception as exc:
                    logger.warning(
                        "SceneManager directory lookup failed zone=%d error=%s; falling back to default",
                        zone_id, exc,
                    )
                    candidates = ()
                valid = [
                    (int(getattr(info, "launch_time", 0)), addr)
                    for info in candidates
                    if (addr := _format_node_grpc_address(info))
                ]
                if valid:
                    valid.sort(key=lambda item: item[0], reverse=True)
                    return valid[0][1]
        if self.default_endpoint:
            return self.default_endpoint
        raise RuntimeError(f"no SceneManager endpoint configured for zone={zone_id}")

    def _get_client(self, address: str):
        client = self._clients.get(address)
        if client is None:
            client = self._client_factory(address, self.timeout_sec)
            self._clients[address] = client
        return client

    async def leave_scene_for_zone(
        self,
        *,
        zone_id: int,
        player_id: int,
        scene_id: int,
        session_id: int,
        login_generation: int,
    ) -> None:
        address = await self.resolve_endpoint(zone_id)
        request = self.protocol.message(
            "scene_manager.LeaveSceneRequest",
            player_id=player_id,
            scene_id=scene_id,
            session_id=session_id,
            login_generation=login_generation,
        )
        await self._get_client(address).call(_LEAVE_SCENE_PATH, request.SerializeToString(), timeout=self.timeout_sec)

    async def leave_scene(
        self,
        *,
        player_id: int,
        scene_id: int,
        session_id: int,
        login_generation: int,
        zone_id: int = 0,
    ) -> None:
        await self.leave_scene_for_zone(
            zone_id=zone_id,
            player_id=player_id,
            scene_id=scene_id,
            session_id=session_id,
            login_generation=login_generation,
        )

    async def enter_scene(
        self,
        *,
        player_id: int,
        session_id: int,
        login_generation: int,
        owner_revision: int,
        gate_zone_id: int,
        hosted_placement: bool = True,
    ):
        address = await self.resolve_endpoint(int(gate_zone_id))
        request = self.protocol.message(
            "scene_manager.EnterSceneRequest",
            player_id=player_id,
            session_id=session_id,
            login_generation=login_generation,
            owner_revision=owner_revision,
            gate_zone_id=gate_zone_id,
            hosted_placement=hosted_placement,
        )
        raw = await self._get_client(address).call(
            _ENTER_SCENE_PATH, request.SerializeToString(), timeout=self.timeout_sec,
        )
        return self.protocol.parse("scene_manager.EnterSceneResponse", raw)

    async def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            close = getattr(client, "close", None)
            if close is None:
                continue
            try:
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.warning("close cached SceneManager client failed error=%s", exc)


class KafkaGatePublisher:
    """经既有 KafkaCommands 向 gate-cmd_gN 指定分区投递 GateCommand（acks=all 幂等生产者）。"""

    def __init__(self, kafka_commands) -> None:
        self.kafka = kafka_commands

    async def publish_gate_command(self, gate_node_id: int, player_id: int, command) -> None:
        await self.kafka.publish("gate", gate_node_id, command, key=str(player_id).encode("utf-8"))


def _grpc_status_code(error: Exception):
    code_fn = getattr(error, "code", None)
    return code_fn() if callable(code_fn) else None


async def resolve_scene_manager_location(protocol: Protocol, store: SessionStore, player_id: int) -> tuple[int, int]:
    """读取 SceneManager 权威位置键 ``player:{<pid>}:location``，返回 ``(scene_id, zone_id)``。"""
    raw = await store.get_location_bytes(player_id)
    if not raw:
        return 0, 0
    location = protocol.parse("storage.PlayerLocation", raw)
    return int(location.scene_id), int(location.zone_id)


async def resolve_scene_manager_scene_id(protocol: Protocol, store: SessionStore, player_id: int) -> int:
    """当会话中 scene_id 为 0 时，回退读取 SceneManager 权威位置键 player:{<pid>}:location。"""
    scene_id, _ = await resolve_scene_manager_location(protocol, store, player_id)
    return scene_id


async def notify_scene_manager_leave(
    protocol: Protocol,
    store: SessionStore,
    scene_manager: SceneManagerClient | None,
    session,
) -> None:
    """通知 SceneManager.LeaveScene 清理位置与人数；失败时保留清理凭证重试。"""
    scene_id = int(session.scene_id)
    zone_id = int(getattr(session, "gate_zone_id", 0) or 0)
    leave_for_zone = getattr(scene_manager, "leave_scene_for_zone", None) if scene_manager is not None else None
    if scene_id == 0 or leave_for_zone is not None:
        loc_scene_id, loc_zone_id = await resolve_scene_manager_location(protocol, store, int(session.player_id))
        if scene_id == 0:
            scene_id = loc_scene_id
        if loc_zone_id:
            zone_id = loc_zone_id
    if scene_id == 0:
        return
    if scene_manager is None:
        raise RuntimeError(f"SceneManager client is not configured for player={session.player_id} scene={scene_id}")
    try:
        if leave_for_zone is not None:
            await leave_for_zone(
                zone_id=zone_id,
                player_id=int(session.player_id),
                scene_id=scene_id,
                session_id=int(session.session_id),
                login_generation=int(session.login_generation),
            )
        else:
            await scene_manager.leave_scene(
                player_id=int(session.player_id),
                scene_id=scene_id,
                session_id=int(session.session_id),
                login_generation=int(session.login_generation),
            )
    except HolderReleasePendingError:
        raise
    except Exception as exc:
        if _grpc_status_code(exc) == grpc.StatusCode.FAILED_PRECONDITION:
            raise HolderReleasePendingError(
                f"location holder release pending: player={session.player_id} scene={scene_id} "
                f"session={session.session_id} login_generation={session.login_generation}: {exc}"
            ) from exc
        raise RuntimeError(
            f"SceneManager.LeaveScene failed player={session.player_id} scene={scene_id} "
            f"session={session.session_id} login_generation={session.login_generation}: {exc}"
        ) from exc
    logger.info(
        "notified SceneManager.LeaveScene player=%d zone=%d scene=%d session=%d login_generation=%d",
        session.player_id, zone_id, scene_id, session.session_id, session.login_generation,
    )


def _parse_gate_node_id(gate_id: str) -> int:
    if not gate_id or not gate_id.isascii() or not gate_id.isdigit():
        raise ValueError(f"invalid gate_id {gate_id!r}")
    node_id = int(gate_id)
    if not 0 < node_id < (1 << 32):
        raise ValueError(f"gate_id out of uint32 range: {gate_id!r}")
    return node_id


async def send_lease_expired_to_gate(
    protocol: Protocol,
    gate_publisher: GateCommandPublisher | None,
    session,
) -> None:
    """构造并发送 event_id=34 的 PlayerLeaseExpiredEvent；gate_id 为空时跳过（正常登出清理不踢连接）。"""
    if not session.gate_id:
        return
    if gate_publisher is None:
        raise RuntimeError("Kafka gate publisher is not configured")
    gate_node_id = _parse_gate_node_id(session.gate_id)
    event = protocol.message(
        "contracts.kafka.PlayerLeaseExpiredEvent",
        player_id=session.player_id,
        session_id=session.session_id,
        scene_node_id=session.scene_node_id,
        scene_id=session.scene_id,
        gate_id=session.gate_id,
        gate_instance_id=session.gate_instance_id,
    )
    command = protocol.message(
        "contracts.kafka.GateCommand",
        player_id=session.player_id,
        session_id=session.session_id,
        payload=event.SerializeToString(),
        target_gate_id=gate_node_id,
        target_instance_id=session.gate_instance_id,
        event_id=protocol.event_ids[_LEASE_EXPIRED_EVENT_NAME],
    )
    await gate_publisher.publish_gate_command(gate_node_id, int(session.player_id), command)


async def check_afk_pass_state(store: SessionStore, player_id: int, now_unix_sec: int) -> AfkPassState:
    """检查玩家挂机月卡投影 player:afk_pass:{<pid>}，读失败 fail-closed 返回 UNKNOWN。"""
    try:
        raw = await store.get_afk_pass_bytes(player_id)
    except Exception as exc:
        logger.error("afk pass probe error player=%d error=%s", player_id, exc)
        return AfkPassState.UNKNOWN
    if raw is None:
        return AfkPassState.INACTIVE
    try:
        expiry_unix_sec = int(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        logger.error("malformed afk pass value player=%d raw=%r", player_id, raw)
        return AfkPassState.INACTIVE
    return AfkPassState.ACTIVE if now_unix_sec < expiry_unix_sec else AfkPassState.INACTIVE


class LeaseMonitor:
    """按 slot 轮转探测并清理到期断线租约及 MarkOffline 异步重试条目。"""

    def __init__(
        self,
        protocol: Protocol,
        store: SessionStore,
        config: PlayerLocatorConfig,
        *,
        scene_manager: SceneManagerClient | None = None,
        gate_publisher: GateCommandPublisher | None = None,
        slot_owner: Callable[[], Sequence[int]] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config
        self.scene_manager = scene_manager
        self.gate_publisher = gate_publisher
        self._all_slots = tuple(range(keys.SLOT_COUNT))
        self._slot_owner = slot_owner
        self.clock = clock
        self._next_slot = 0
        self._wake_queue: asyncio.Queue[int] = asyncio.Queue(maxsize=_WAKE_QUEUE_CAPACITY)
        self._wake_handles: set[asyncio.TimerHandle] = set()

    def owned_slots(self) -> Sequence[int]:
        return self._all_slots if self._slot_owner is None else self._slot_owner()

    def wake(self, slot: int, ready_at: float | None = None) -> None:
        """安排在 ready_at 时刻立即探测指定 slot；队列满时静默降级由常规轮转兜底。"""
        delay = 0.0 if ready_at is None else max(0.0, ready_at - self.clock())

        def enqueue() -> None:
            try:
                self._wake_queue.put_nowait(slot)
            except asyncio.QueueFull:
                pass

        if delay <= 0.0:
            enqueue()
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            enqueue()
            return

        def fire() -> None:
            self._wake_handles.discard(handle)
            enqueue()

        handle = loop.call_later(delay, fire)
        self._wake_handles.add(handle)

    def next_slots(self, owned: Sequence[int]) -> list[int]:
        """按 scan_round_sec 计算本拍探测的 slot 切片，保证一轮内覆盖全部持有 slot。"""
        if not owned:
            return []
        count = min(max(math.ceil(len(owned) * self.config.poll_interval_sec / self.config.scan_round_sec), 1),
                    len(owned))
        start = bisect.bisect_left(owned, self._next_slot)
        slots = [owned[(start + idx) % len(owned)] for idx in range(count)]
        self._next_slot = slots[-1] + 1
        return slots

    async def tick(self) -> None:
        slots = self.next_slots(self.owned_slots())
        resume_at, budget_used = await self.process_slots(slots)
        if budget_used:
            self._next_slot = resume_at

    async def process_slots(self, slots: Sequence[int]) -> tuple[int, bool]:
        if not slots:
            return 0, False
        now_unix_sec = int(self.clock())
        claim_deadline_sec = now_unix_sec + int(math.ceil(self.config.claim_ttl_sec))
        due = await self.store.probe_due_lease_slots(slots, now_unix_sec)

        budget = self.config.batch_size
        claims: list[LeaseClaim] = []
        resume_at = 0
        budget_used = False
        for slot in due:
            try:
                got = await self.store.claim_expired_leases(slot, now_unix_sec, claim_deadline_sec, budget)
            except Exception as exc:
                logger.error("claim expired leases failed slot=%d error=%s", slot, exc)
                continue
            claims.extend(got)
            budget -= len(got)
            if budget <= 0:
                resume_at = slot
                budget_used = True
                break

        if not claims:
            return resume_at, budget_used

        heartbeat = asyncio.create_task(self._heartbeat_claims(claims), name="lease-claim-heartbeat")
        try:
            for claim in claims:
                try:
                    await self.handle_lease_expiry(claim)
                except Exception:
                    logger.exception("handle lease expiry unexpected error player=%d", claim.player_id)
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
        return resume_at, budget_used

    async def _heartbeat_claims(self, claims: Sequence[LeaseClaim]) -> None:
        interval = max(0.05, self.config.claim_heartbeat_sec)
        ttl_sec = max(self.config.claim_ttl_sec, interval * 3.0)
        while True:
            await asyncio.sleep(interval)
            try:
                deadline = int(self.clock() + ttl_sec)
                await self.store.renew_lease_claims(claims, deadline)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("renew processing claims failed error=%s", exc)

    async def handle_lease_expiry(self, claim: LeaseClaim) -> None:
        session = None
        if claim.payload:
            try:
                session = self.protocol.parse("playerlocator.PlayerSession", claim.payload)
            except DecodeError:
                session = None
        if session is None:
            result = await self.store.commit_lease_expiry(claim)
            if result == CommitLeaseResult.COMMITTED:
                logger.error("malformed or empty session payload player=%d; notifications unavailable", claim.player_id)
            await self.store.ack_lease_claim(claim)
            return

        if int(session.player_id) != claim.player_id:
            # 载荷身份不符时保留 processing 凭证不 ack，防止 SetSession 在修复前绕过清理屏障
            logger.error(
                "claim player_id mismatch member=%d payload=%d; claim retained for retry",
                claim.player_id, session.player_id,
            )
            return

        if int(session.state) != keys.SESSION_STATE_DISCONNECTING:
            logger.info("stale lease claim player=%d state=%d; acked", claim.player_id, session.state)
            await self.store.ack_lease_claim(claim)
            return

        # 仅当活会话键仍然存在时才探测挂机月卡：MarkOffline 入队的登出清理无活会话键，不适用月卡保护
        session_exists = True
        try:
            live_session = await self.store.get_session_bytes(claim.player_id)
            session_exists = live_session is not None
        except Exception as exc:
            logger.error("check live session failed player=%d error=%s", claim.player_id, exc)
            session_exists = True

        if session_exists:
            now_unix_sec = int(self.clock())
            afk_state = await check_afk_pass_state(self.store, claim.player_id, now_unix_sec)
            if afk_state == AfkPassState.ACTIVE:
                rearm_res = await self.store.rearm_lease_claim(claim, now_unix_sec + AFK_PASS_LEASE_TTL_SEC)
                if rearm_res == RearmLeaseResult.CLAIM_LOST:
                    logger.error("rearm afk lease failed player=%d reason=claim_lost", claim.player_id)
                    return
                if rearm_res != RearmLeaseResult.REARMED:
                    return
                logger.info("player has active afk pass; lease extended player=%d seconds=%d",
                            claim.player_id, AFK_PASS_LEASE_TTL_SEC)
                await resume_hosted_if_lost(
                    self.protocol, self.store, self.scene_manager, session, self.clock(),
                )
                return
            if afk_state == AfkPassState.UNKNOWN:
                rearm_res = await self.store.rearm_lease_claim(claim, now_unix_sec + AFK_PASS_PROBE_RETRY_SEC)
                if rearm_res == RearmLeaseResult.CLAIM_LOST:
                    logger.error("defer afk probe failed player=%d reason=claim_lost", claim.player_id)
                    return
                logger.error("afk pass probe failed player=%d defer_sec=%d",
                             claim.player_id, AFK_PASS_PROBE_RETRY_SEC)
                return

        commit_res = await self.store.commit_lease_expiry(claim)
        if commit_res != CommitLeaseResult.COMMITTED:
            return

        gate_err: Exception | None = None
        scene_err: Exception | None = None
        try:
            await send_lease_expired_to_gate(self.protocol, self.gate_publisher, session)
        except Exception as exc:
            gate_err = exc
        try:
            await notify_scene_manager_leave(self.protocol, self.store, self.scene_manager, session)
        except Exception as exc:
            scene_err = exc

        if gate_err is not None or scene_err is not None:
            if gate_err is None and isinstance(scene_err, HolderReleasePendingError):
                logger.info("waiting for location holder release player=%d detail=%s", claim.player_id, scene_err)
            else:
                logger.error(
                    "cleanup side effects failed player=%d gate_err=%s scene_err=%s; claim retained for retry",
                    claim.player_id, gate_err, scene_err,
                )
            return

        await self.store.ack_lease_claim(claim)
        logger.info(
            "lease expired player=%d session=%d version=%d gate=%s scene=%d",
            claim.player_id, session.session_id, session.session_version, session.gate_id, session.scene_id,
        )

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        logger.info(
            "lease monitor started poll_sec=%.3f batch=%d scan_round_sec=%.1f",
            self.config.poll_interval_sec, self.config.batch_size, self.config.scan_round_sec,
        )
        loop = asyncio.get_running_loop()
        next_tick_at = loop.time() + self.config.poll_interval_sec
        try:
            while stop_event is None or not stop_event.is_set():
                wait_sec = max(0.0, next_tick_at - loop.time())
                try:
                    slot = await asyncio.wait_for(self._wake_queue.get(), timeout=wait_sec)
                except TimeoutError:
                    next_tick_at = loop.time() + self.config.poll_interval_sec
                    try:
                        await self.tick()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("lease monitor tick failed")
                    continue
                try:
                    await self.process_slots([slot])
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("lease monitor wake processing failed slot=%d", slot)
        finally:
            for handle in list(self._wake_handles):
                handle.cancel()
            self._wake_handles.clear()
            logger.info("lease monitor stopped")
