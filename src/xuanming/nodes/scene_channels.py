"""Scene 从场景控制域读取并转发原版分线目录。"""

import asyncio
import logging

from google.protobuf.message import DecodeError

logger = logging.getLogger(__name__)
IO_TIMEOUT_SECONDS = 3
MAX_PENDING = 1024


def directory_key(zone_id: int, scene_config_id: int) -> str:
    return f"world_channel_directory:{{ctl:{zone_id}}}:{scene_config_id}"


class SceneChannels:
    def __init__(self, scene, *, control_redis=None) -> None:
        self.scene, self.protocol = scene, scene.protocol
        self.control_redis = control_redis
        self._pending: set[asyncio.Task] = set()
        self._closed = False

    def query(self, world, player, request) -> None:
        if self._closed:
            return
        info = world.info
        is_world_channel = bool(info.scene_config_id and not info.mirror_config_id and not info.dungeon_config_id)
        if (not request.with_channel_directory or not is_world_channel or not player.player_id
                or self.control_redis is None or len(self._pending) >= MAX_PENDING):
            self._push(player, info)
            return
        copied = type(info).FromString(info.SerializeToString())
        task = asyncio.create_task(self._read(player, copied), name=f"scene-channels-{player.player_id}")
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    def _parse(self, raw, scene_config_id: int):
        if raw is None:
            return None
        if not isinstance(raw, bytes) or len(raw) > (1 << 31) - 1:
            raise ValueError("invalid channel directory Redis response")
        directory = self.protocol.parse("SceneChannelDirectory", raw)
        if directory.scene_config_id != scene_config_id:
            raise ValueError("channel directory map does not match the requested map")
        return directory

    async def _read(self, player, info) -> None:
        try:
            async with asyncio.timeout(IO_TIMEOUT_SECONDS):
                raw = await self.control_redis.get(directory_key(self.scene.info.zone_id, info.scene_config_id))
            directory = self._parse(raw, info.scene_config_id)
        except (DecodeError, ValueError, TypeError) as error:
            logger.error("Invalid Scene channel directory player_id=%d scene_config_id=%d error=%s",
                         player.player_id, info.scene_config_id, type(error).__name__)
            directory = None
        except Exception as error:
            logger.warning("Scene channel directory unavailable player_id=%d scene_config_id=%d error=%s",
                           player.player_id, info.scene_config_id, type(error).__name__)
            directory = None
        world = self.scene.locations.get(player.player_id)
        if (not self._closed and world is not None and world.players.get(player.player_id) is player
                and world.info.scene_id == info.scene_id):
            self._push(player, info, directory)

    def _push(self, player, info, directory=None) -> None:
        response = self.protocol.message("SceneInfoS2C", scene_info=[info])
        if directory is not None:
            response.channel_directory.CopyFrom(directory)
        self.scene._push(player, "SceneSceneClientPlayerNotifySceneInfo", response)

    async def close(self) -> None:
        self._closed = True
        pending = tuple(self._pending)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
