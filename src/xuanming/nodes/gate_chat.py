"""Gate 世界频道广播与每节点的推送预算。"""

import logging
from time import monotonic

from google.protobuf.message import DecodeError

logger = logging.getLogger(__name__)


class WorldChatFanout:
    def __init__(self, protocol, deliver, *, clock=monotonic):
        self._protocol = protocol
        self._allowed_ids = frozenset(
            protocol.message_ids[name]
            for name in ("ClientPlayerChatNotifyWorldChat", "ClientPlayerChatNotifyWorldChatRetracted")
            if name in protocol.message_ids
        )
        self._deliver = deliver
        self._clock = clock
        self._tokens = 20.0
        self._last = None
        self._dropped = 0

    async def __call__(self, payload: bytes) -> None:
        try:
            event = self._protocol.parse("contracts.kafka.BroadcastToAllEvent", payload)
        except DecodeError:
            logger.warning("无法解析世界频道广播")
            return
        content = event.message_content
        if content.message_id not in self._allowed_ids:
            logger.error("世界频道广播拒绝非聊天推送 message_id=%d", content.message_id)
            return

        now = self._clock()
        if self._last is not None:
            self._tokens = min(20.0, self._tokens + 10.0 * (now - self._last))
        self._last = now
        if self._tokens < 1:
            self._dropped += 1
            if self._dropped % 1024 == 1:
                logger.warning("世界频道广播超过节点推送预算 dropped=%d", self._dropped)
            return

        self._tokens -= 1
        self._deliver(content)
