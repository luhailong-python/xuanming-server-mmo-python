"""大世界分线目录周期发布器（对齐 Go world_channel_directory.go 与 nodes/scene_channels.py 消费者）。

职责边界：
- 对既有场景与节点状态全程只读（不触发懒改派或 CreateScene）；
- 仅写两把自有控制域键：
  1. ``world_channel_lineno:{ctl:<z>}:<confId>``：确定性 1-based 线号映射表（无 TTL）；
  2. ``world_channel_directory:{ctl:<z>}:<confId>``：序列化 ``SceneChannelDirectory`` protobuf（带 TTL，默认 8s）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence

from ..protocol import Protocol
from .config import SceneManagerConfig
from .store import SceneControlStore

logger = logging.getLogger(__name__)

# 与 proto SceneChannelState 枚举值逐一对应
SCENE_CHANNEL_STATE_UNKNOWN = 0
SCENE_CHANNEL_STATE_SMOOTH = 1
SCENE_CHANNEL_STATE_BUSY = 2
SCENE_CHANNEL_STATE_FULL = 3
SCENE_CHANNEL_STATE_CLOSING = 4
SCENE_CHANNEL_STATE_UNAVAILABLE = 5

_MAX_UINT32 = 0xFFFFFFFF


def world_channel_state(
    *,
    closing: bool,
    node_usable: bool,
    players: int,
    max_players: int,
    busy_percent: int,
) -> int:
    """判定单条大世界分线的客户端展示状态（纯函数，与 Go worldChannelState 逐行一致）。"""
    if closing:
        return SCENE_CHANNEL_STATE_CLOSING
    if not node_usable:
        return SCENE_CHANNEL_STATE_UNAVAILABLE
    if max_players > 0:
        if players >= max_players:
            return SCENE_CHANNEL_STATE_FULL
        if players * 100 >= max_players * busy_percent:
            return SCENE_CHANNEL_STATE_BUSY
    return SCENE_CHANNEL_STATE_SMOOTH


def _parse_channel_ids(members: Sequence[str]) -> list[int]:
    ids: list[int] = []
    for raw in members:
        if raw.isascii() and raw.isdigit():
            val = int(raw)
            if val > 0:
                ids.append(val)
    return ids


def _clamp_uint32(count: int) -> int:
    if count <= 0:
        return 0
    return min(count, _MAX_UINT32)


class WorldChannelDirectoryPublisher:
    """大世界分线目录发布器：分配稳定 1-based 线号并将 SceneChannelDirectory 写入控制域 Redis。"""

    def __init__(
        self,
        protocol: Protocol,
        cfg: SceneManagerConfig,
        *,
        control_store: SceneControlStore,
        node_registry,
        clock_ms: Callable[[], int] | None = None,
        world_conf_ids: Sequence[int] | None = None,
    ) -> None:
        self.protocol = protocol
        self.config = cfg
        self.control_store = control_store
        self.node_registry = node_registry
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._world_conf_ids: tuple[int, ...] = (
            tuple(world_conf_ids) if world_conf_ids is not None else cfg.effective_world_conf_ids()
        )

    async def _node_usable(self, zone_id: int, node_id: str) -> bool:
        if not node_id:
            return False
        if self.node_registry.is_node_identity_ambiguous(zone_id, node_id):
            return False
        return await self.control_store.is_node_in_load_set(zone_id, node_id)

    async def publish_conf(self, zone_id: int, conf_id: int):
        """发布单张大世界地图的分线目录，返回写入的 SceneChannelDirectory 或 None（空集/跳过）。"""
        # 严格先读在役再读回收中：防止分线单向转入排空时两头落空导致线号被误回收
        active_members = await self.control_store.get_world_channels(zone_id, conf_id)
        draining_members = await self.control_store.get_world_draining_channels(zone_id, conf_id)
        active_ids = _parse_channel_ids(active_members)
        draining_ids = _parse_channel_ids(draining_members)
        if not active_ids and not draining_ids:
            return None

        closing_set = set(draining_ids)
        all_ids = active_ids + draining_ids
        line_nos = await self.control_store.allocate_world_channel_line_numbers(zone_id, conf_id, all_ids)
        if not line_nos:
            return None

        max_players = self.config.effective_channel_max_players()
        busy_percent = self.config.effective_channel_busy_percent()
        ttl_sec = self.config.effective_channel_directory_ttl_sec()
        if ttl_sec <= 0:
            ttl_sec = 8

        channels = []
        for scene_id, line_no in sorted(line_nos.items(), key=lambda item: item[1]):
            is_closing = scene_id in closing_set
            node_usable = False
            if not is_closing:
                owner = await self.control_store.get_scene_node(zone_id, scene_id)
                node_usable = await self._node_usable(zone_id, owner)
            try:
                players = await self.control_store.get_scene_member_count(zone_id, scene_id)
            except Exception:
                players = 0
            channels.append(
                self.protocol.message(
                    "SceneChannelInfo",
                    scene_id=scene_id,
                    channel_no=line_no,
                    player_count=_clamp_uint32(players),
                    state=world_channel_state(
                        closing=is_closing,
                        node_usable=node_usable,
                        players=players,
                        max_players=max_players,
                        busy_percent=busy_percent,
                    ),
                )
            )

        directory = self.protocol.message(
            "SceneChannelDirectory",
            zone_id=zone_id,
            scene_config_id=conf_id,
            channels=channels,
            updated_at_ms=int(self._clock_ms()),
            switch_cooldown_seconds=self.config.effective_channel_switch_cooldown_sec(),
            max_players_per_channel=max_players,
            switch_enabled=not self.config.channel_switch_disabled,
        )
        await self.control_store.set_world_channel_directory(
            zone_id, conf_id, directory.SerializeToString(), ttl_sec,
        )
        return directory

    async def publish_zone(self, zone_id: int | None = None) -> int:
        """发布本 zone 全部大世界地图的分线目录，返回成功发布地图数。"""
        target_zone = self.config.zone_id if zone_id is None else zone_id
        published = 0
        for conf_id in self._world_conf_ids:
            try:
                directory = await self.publish_conf(target_zone, conf_id)
                if directory is not None:
                    published += 1
            except Exception as exc:
                logger.error(
                    "world channel directory publish failed zone=%d conf=%d error=%s",
                    target_zone, conf_id, exc,
                )
        return published

    async def run(self, stop: asyncio.Event) -> None:
        """按配置周期持续发布分线目录，直到 stop 触发。"""
        interval = self.config.effective_channel_directory_refresh_sec()
        if interval <= 0:
            logger.info("world channel directory publisher disabled by configuration")
            return
        logger.info(
            "world channel directory publisher started zone=%d interval=%.1fs ttl=%ds",
            self.config.zone_id, interval, self.config.effective_channel_directory_ttl_sec(),
        )
        while not stop.is_set():
            try:
                await self.publish_zone()
            except Exception as exc:
                logger.error("world channel directory publish tick failed zone=%d error=%s", self.config.zone_id, exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
