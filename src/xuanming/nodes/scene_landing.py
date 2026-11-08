"""落地后的域版本摘要与资产待办唤醒。"""

import asyncio
import logging

from ..data.asset_ledger import UINT64_MAX

logger = logging.getLogger(__name__)
WAKE_CHANNEL = "assetop:wake"
WAKE_COALESCE_SECONDS = 5
IO_TIMEOUT_SECONDS = 2
MAX_PENDING = 1024


def _decimal(value: bytes) -> int:
    if not value.isdigit():
        raise ValueError("expected an unsigned decimal")
    digits = value.lstrip(b"0") or b"0"
    if len(digits) > 20 or (number := int(digits)) > UINT64_MAX:
        raise ValueError("unsigned decimal exceeds uint64")
    return number


class SceneLanding:
    def __init__(self, scene, *, player_redis=None, control_redis=None) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self._player_redis = player_redis
        self._control_redis = control_redis
        domains = self.protocol.pool.FindEnumTypeByName("SyncDomain")
        self._domains = {str(number).encode(): number for number in domains.values_by_number if number > 0}
        self._pending: set[asyncio.Task] = set()
        self._closed = False

    def register(self) -> None:
        self.scene.register("SceneSceneClientPlayer", "GetSyncDigest", self.request_digest)

    def _schedule(self, work, *args) -> bool:
        if self._closed or len(self._pending) >= MAX_PENDING:
            return False
        task = asyncio.create_task(work(*args))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return True

    def request_digest(self, _world, player, _route, _request) -> None:
        if self._player_redis is None or not self._schedule(self._read_digest, player, self._identity(player)):
            self._push_digest(player, self.protocol.message("SyncDigestS2C", status=1))

    @staticmethod
    def _identity(player):
        return player.gate_uuid, player.session_id, player.generation, player.snapshot.owner_epoch

    async def _read_digest(self, player, identity) -> None:
        try:
            async with asyncio.timeout(IO_TIMEOUT_SECONDS):
                fields = await self._player_redis.hgetall(f"player:{{{player.player_id}}}:sync_digest")
            digest = self._parse(fields)
        except Exception as error:
            logger.warning("Sync digest unavailable for player %d (%s)", player.player_id, type(error).__name__)
            digest = self.protocol.message("SyncDigestS2C", status=1)
        if (not self._closed and player.disconnect_at is None and self._identity(player) == identity
                and self.scene.sessions.get(identity[:2]) is player):
            self._push_digest(player, digest)

    def _parse(self, fields):
        digest = self.protocol.message("SyncDigestS2C")
        if not isinstance(fields, dict) or not all(
            isinstance(key, bytes) and isinstance(value, bytes) for key, value in fields.items()
        ):
            raise ValueError("invalid Redis hash")
        if not fields:
            return digest
        generation = _decimal(fields.get(b"g", b""))
        if not generation:
            raise ValueError("sync digest generation is missing or zero")
        digest.generation = generation
        for key, value in fields.items():
            if key in self._domains:
                digest.entries.add(domain=self._domains[key], version=_decimal(value))
        return digest

    def _push_digest(self, player, digest) -> None:
        self.scene._push(player, "SceneSceneClientPlayerNotifySyncDigest", digest)

    def wake(self, player, *, coalesce: bool = True) -> None:
        if self._control_redis is None or not self.scene.mutable(player):
            return
        now = asyncio.get_running_loop().time()
        if coalesce and now < player.asset_wake_after:
            return
        if self._schedule(self._publish_wake, player.player_id) and coalesce:
            player.asset_wake_after = now + WAKE_COALESCE_SECONDS

    async def _publish_wake(self, player_id) -> None:
        try:
            async with asyncio.timeout(IO_TIMEOUT_SECONDS):
                await self._control_redis.publish(WAKE_CHANNEL, str(player_id))
        except Exception as error:
            logger.warning("Asset wake failed for player %d (%s)", player_id, type(error).__name__)

    async def close(self) -> None:
        self._closed = True
        pending = tuple(self._pending)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
