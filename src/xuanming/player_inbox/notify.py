"""提交后尽力而为的在线 Kafka 提醒（与 Go inbox/notify.go 对齐）。"""

from __future__ import annotations

import logging
from enum import IntEnum
from typing import Protocol as TypingProtocol

from google.protobuf.message import DecodeError

from ..cluster import SCENE
from ..protocol import Protocol
from .model import PLAYER_INBOX_NOTIFY_EVENT_ID

logger = logging.getLogger(__name__)


class NotifyResult(IntEnum):
    """单次在线提醒结局。"""

    ERROR = 0
    SENT = 1
    OFFLINE = 2
    AWAITING_PLACEMENT = 3
    NO_NODE = 4
    EMPTY_UUID = 5
    NODES_UNSYNCED = 6
    NODE_AMBIGUOUS = 7
    NODE_MISSING = 8

    @property
    def settled(self) -> bool:
        """可立即清零 notify_due_ms 的终局集合。"""
        return self in (
            NotifyResult.SENT,
            NotifyResult.OFFLINE,
            NotifyResult.AWAITING_PLACEMENT,
            NotifyResult.NO_NODE,
        )


class InboxNotifier(TypingProtocol):
    """可注入的提交后提醒协议（实现不得向外抛异常）。"""

    async def notify(self, player_id: int, domain: int) -> NotifyResult: ...


class LocationReader(TypingProtocol):
    """读取玩家域 Redis 中 player:{id}:location 原始字节的最小协议。"""

    async def get_location(self, player_id: int) -> bytes | None: ...


class SceneNodeLookup(TypingProtocol):
    """查询目标 Scene 节点（返回 NodeInfo 或 None）的最小协议。"""

    async def lookup(self, node_type: int, zone: int, node_id: int): ...


class KafkaCommandPublisher(TypingProtocol):
    """发布 SceneCommand 至 scene-cmd_g<N> 的最小协议（与 cluster.KafkaCommands.publish 签名一致）。"""

    async def publish(self, role: str, node_id: int, message, *, key: bytes | None = None) -> None: ...


class RedisLocationReader:
    """使用 redis.asyncio 客户端读取玩家位置键 ``player:{player_id}:location``。"""

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    async def get_location(self, player_id: int) -> bytes | None:
        key = f"player:{{{player_id}}}:location"
        raw = await self._redis.get(key)
        if not raw:
            return None
        return raw if isinstance(raw, bytes) else bytes(raw)


class NullInboxNotifier:
    """空提醒器：未装配 Kafka/Redis 时不发提醒，由补发循环或玩家落地拉取兜底。"""

    async def notify(self, player_id: int, domain: int) -> NotifyResult:
        return NotifyResult.ERROR


class KafkaInboxNotifier:
    """生产提醒器：读位置 -> 查 Scene 节点 UUID -> 发送 SceneCommand(event_id=49) 到 scene-cmd_g<N>。"""

    def __init__(
        self,
        protocol: Protocol,
        locations: LocationReader,
        directory: SceneNodeLookup,
        publisher: KafkaCommandPublisher,
        *,
        event_id: int = PLAYER_INBOX_NOTIFY_EVENT_ID,
    ) -> None:
        self._protocol = protocol
        self._locations = locations
        self._directory = directory
        self._publisher = publisher
        self._event_id = event_id

    async def notify(self, player_id: int, domain: int) -> NotifyResult:
        try:
            return await self._notify(player_id, domain)
        except Exception:
            logger.error(
                "player_inbox post-commit notify error player=%d domain=%d",
                player_id,
                domain,
                exc_info=True,
            )
            return NotifyResult.ERROR

    async def _notify(self, player_id: int, domain: int) -> NotifyResult:
        if self._event_id <= 0:
            return NotifyResult.ERROR
        raw = await self._locations.get_location(player_id)
        if not raw:
            return NotifyResult.OFFLINE
        try:
            loc = self._protocol.parse("storage.PlayerLocation", raw)
        except DecodeError:
            logger.error("player_inbox corrupt location payload player=%d domain=%d", player_id, domain)
            return NotifyResult.ERROR
        if not loc.node_id:
            if loc.owner_epoch != 0:
                return NotifyResult.AWAITING_PLACEMENT
            logger.error("player_inbox incomplete location without node_id or owner_epoch player=%d", player_id)
            return NotifyResult.NO_NODE
        try:
            target_node_id = int(loc.node_id)
        except ValueError:
            logger.error("player_inbox non-integer scene node_id=%r player=%d", loc.node_id, player_id)
            return NotifyResult.NODE_MISSING
        info = await self._directory.lookup(SCENE, int(loc.zone_id), target_node_id)
        if info is None:
            return NotifyResult.NODE_MISSING
        if not info.node_uuid:
            logger.error("player_inbox scene node missing uuid zone=%d node_id=%d", loc.zone_id, target_node_id)
            return NotifyResult.EMPTY_UUID

        payload = self._protocol.message(
            "PlayerInboxNotifyEvent",
            player_id=player_id,
            domain=domain,
        ).SerializeToString()
        command = self._protocol.message(
            "contracts.kafka.SceneCommand",
            command_type=0,  # DispatchEvent
            player_id=player_id,
            target_scene_id=int(info.node_id),
            payload=payload,
            target_instance_id=info.node_uuid,
            event_id=self._event_id,
        )
        await self._publisher.publish("scene", int(info.node_id), command, key=str(player_id).encode("ascii"))
        return NotifyResult.SENT
