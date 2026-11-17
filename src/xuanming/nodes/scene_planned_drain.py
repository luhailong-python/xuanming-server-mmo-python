"""Bounded planned Scene shutdown while its transports and world loop stay live."""

import asyncio
import logging
import os

from ..cluster import SCENE

logger = logging.getLogger(__name__)


def _unsigned(value):
    return int(value) if value and len(value) <= 9 and value.isascii() and value.isdecimal() else None


class RelocationPacer:
    """Node-wide token bucket matching Scene's planned evacuation policy."""

    def __init__(self, rate):
        self.rate = rate
        self.tokens = 0.0
        self.last = None

    @classmethod
    def from_env(cls):
        rate = _unsigned(os.getenv("SCENE_PLANNED_DRAIN_RELOCATIONS_PER_SECOND", ""))
        return cls(200 if rate is None else rate)

    def allow(self, now):
        if not self.rate:
            return True
        if self.last is not None and now > self.last:
            self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens < 1:
            return False
        self.tokens -= 1
        return True


def budget_from_env():
    raw = os.getenv("MMORPG_PLANNED_DRAIN_BUDGET_SECONDS", "")
    if not raw:
        return 35
    value = _unsigned(raw)
    if value is None or value > 86400:
        raise ValueError("MMORPG_PLANNED_DRAIN_BUDGET_SECONDS must be an integer in 0..86400")
    return value


async def planned_drain(scene, directory, budget):
    scene.begin_planned_drain()
    scene.info.admission = 2
    if not budget:
        try:
            async with asyncio.timeout(1):
                await directory.update(scene.info)
        except Exception:
            logger.warning("Scene 停机时更新节点准入失败", exc_info=True)
        return
    loop = asyncio.get_running_loop()
    start = loop.time()
    deadline = start + budget
    configured = _unsigned(os.getenv("SCENE_PLANNED_DRAIN_SELF_EVACUATE_AFTER_SECONDS", ""))
    evacuate_after = min(configured, budget) if configured is not None else budget // 2
    no_target_since = None
    while True:
        now = loop.time()
        if now >= deadline:
            logger.warning("Scene 计划排空预算耗尽，剩余角色将存档退出")
            return
        try:
            async with asyncio.timeout_at(deadline):
                await directory.update(scene.info)
                peers = await directory.candidates(SCENE, scene.info.zone_id)
        except Exception:
            # Failure to read the registry is not evidence that there is no target.
            no_target_since = None
            logger.warning("Scene 排空期间更新或读取节点登记失败", exc_info=True)
        else:
            has_target = any(peer.node_id != scene.info.node_id and peer.admission != 2
                             and peer.scene_node_type in (0, 2) for peer in peers)
            if has_target:
                no_target_since = None
            elif no_target_since is None:
                no_target_since = loop.time()
            elif loop.time() - no_target_since >= 3:
                logger.warning("Scene 排空无可用目标，剩余角色将存档退出")
                return
        if (not scene.locations and not scene._pending_sessions
                and not (scene.travel and scene.travel._trips)):
            return
        if loop.time() - start >= evacuate_after:
            scene.evacuate_remaining()
        await asyncio.sleep(min(1, max(0, deadline - loop.time())))
