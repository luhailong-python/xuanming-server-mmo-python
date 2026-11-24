"""scene_manager 后台清扫器：空闲副本回收、拆除任务重试与死节点孤儿副本清理。

严格对齐 Go 参考实现：
- ``IdleInstanceSweeper`` -> ``instance_lifecycle.go``（mirror 30s vs instance 300s，Lua CAS 绝不误拆有人副本）；
- ``TeardownJobSweeper`` -> ``scene_teardown_job.go``（按租期接手 pending 拆除任务并幂等重试至 finish）；
- ``DeadNodeSweeper`` -> ``reentry_barrier.go`` + ``load_reporter.go``（20s 再入屏障后清理死节点孤儿副本，保留大世界频道）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

from .config import SceneManagerConfig
from .store import DeadNodeReconcileTask, SceneControlStore

logger = logging.getLogger(__name__)


def _death_at_elapsed_sec(death_at: int, now_ms: int) -> float:
    """兼容 Unix 秒与 Unix 毫秒两种 death_at 精度，返回已过去秒数（负值表示未来时钟偏差）。"""
    if death_at >= 1_000_000_000_000:
        return (now_ms - death_at) / 1000.0
    return (now_ms // 1000) - death_at


class IdleInstanceSweeper:
    """空闲副本/镜像清扫器：扫描 instances:{ctl:<z>}:active 并通过 LUA_ATOMIC_DESTROY_INSTANCE 回收超时空实例。"""

    def __init__(
        self,
        service,
        *,
        control_store: SceneControlStore | None = None,
        cfg: SceneManagerConfig | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.service = service
        self.control_store = control_store or service.control_store
        self.config = cfg or service.config
        self._clock_ms = clock_ms or service._clock_ms

    async def sweep_once(self, zone_id: int | None = None) -> int:
        target_zone = self.config.zone_id if zone_id is None else zone_id
        instance_timeout = self.config.instance_idle_timeout_sec
        mirror_timeout = self.config.effective_mirror_idle_timeout_sec()
        if instance_timeout <= 0 and mirror_timeout <= 0:
            return 0

        pairs = await self.control_store.list_active_instances_with_scores(target_zone)
        if not pairs:
            return 0

        now_sec = int(self._clock_ms()) // 1000
        destroyed = 0
        for scene_str, last_active_score in pairs:
            if not scene_str.isascii() or not scene_str.isdigit():
                continue
            scene_id = int(scene_str)
            try:
                player_count = await self.control_store.get_scene_member_count(target_zone, scene_id)
            except Exception:
                player_count = 0

            if player_count > 0:
                await self.control_store.touch_active_instance(target_zone, scene_id, now_sec)
                continue

            is_mirror = await self.control_store.is_scene_mirror(target_zone, scene_id)
            timeout_sec = mirror_timeout if is_mirror else instance_timeout
            kind = "mirror" if is_mirror else "instance"
            if timeout_sec <= 0:
                continue

            idle_sec = now_sec - int(last_active_score)
            if idle_sec < timeout_sec:
                continue

            logger.info(
                "destroying idle %s zone=%d scene=%d idle=%ds timeout=%ds",
                kind, target_zone, scene_id, idle_sec, timeout_sec,
            )
            if await self.service.destroy_idle_instance(target_zone, scene_id):
                destroyed += 1

        if destroyed > 0:
            logger.info("idle instance sweep finished zone=%d destroyed=%d", target_zone, destroyed)
        return destroyed

    async def run(self, stop: asyncio.Event) -> None:
        instance_timeout = self.config.instance_idle_timeout_sec
        mirror_timeout = self.config.effective_mirror_idle_timeout_sec()
        if instance_timeout <= 0 and mirror_timeout <= 0:
            logger.info("idle instance sweeper disabled by configuration")
            return
        interval = self.config.instance_check_interval_sec if self.config.instance_check_interval_sec > 0 else 30.0
        logger.info(
            "idle instance sweeper started zone=%d interval=%.1fs instance_timeout=%ds mirror_timeout=%ds",
            self.config.zone_id, interval, instance_timeout, mirror_timeout,
        )
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.sweep_once()
            except Exception as exc:
                logger.error("idle instance sweep failed zone=%d error=%s", self.config.zone_id, exc)


class TeardownJobSweeper:
    """场景拆除任务清扫器：接手 scene_teardown:{ctl:<z>}:pending 中超租期未完成的拆除任务并幂等重试。"""

    def __init__(
        self,
        service,
        *,
        control_store: SceneControlStore | None = None,
        cfg: SceneManagerConfig | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.service = service
        self.control_store = control_store or service.control_store
        self.config = cfg or service.config
        self._clock_ms = clock_ms or service._clock_ms

    async def sweep_once(self, zone_id: int | None = None) -> int:
        target_zone = self.config.zone_id if zone_id is None else zone_id
        now_sec = int(self._clock_ms()) // 1000
        lease_sec = max(0, int(self.config.teardown_job_lease_sec))
        batch = max(1, int(self.config.teardown_sweep_batch))

        due = await self.control_store.list_due_teardown_jobs(target_zone, now_sec - lease_sec, batch)
        finished = 0
        for scene_str, claimed_at in due:
            if not scene_str.isascii() or not scene_str.isdigit():
                logger.error("malformed pending teardown member zone=%d member=%r", target_zone, scene_str)
                continue
            scene_id = int(scene_str)
            try:
                job = await self.control_store.load_scene_teardown_job(target_zone, scene_id)
            except Exception as exc:
                logger.error("load teardown job failed zone=%d scene=%d error=%s", target_zone, scene_id, exc)
                continue
            if job is None:
                await self.control_store.drop_dangling_teardown(target_zone, scene_id)
                continue

            logger.info(
                "recovering pending teardown job zone=%d scene=%d kind=%s reason=%s age=%ds",
                target_zone, scene_id, job.kind, job.reason, max(0, now_sec - int(claimed_at)),
            )
            if await self.service.complete_scene_teardown(job):
                finished += 1
        return finished

    async def run(self, stop: asyncio.Event) -> None:
        interval = self.config.teardown_sweep_interval_sec
        if interval <= 0:
            logger.info("teardown job sweeper disabled by configuration")
            return
        logger.info(
            "teardown job sweeper started zone=%d interval=%.1fs lease=%ds batch=%d",
            self.config.zone_id, interval, self.config.teardown_job_lease_sec, self.config.teardown_sweep_batch,
        )
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.sweep_once()
            except Exception as exc:
                logger.error("teardown job sweep failed zone=%d error=%s", self.config.zone_id, exc)


class DeadNodeSweeper:
    """死节点孤儿副本清扫器：遵循 20s 再入屏障与返回实例撤销语义，仅清理孤儿副本，绝不销毁大世界频道。"""

    def __init__(
        self,
        service,
        *,
        control_store: SceneControlStore | None = None,
        node_registry=None,
        cfg: SceneManagerConfig | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.service = service
        self.control_store = control_store or service.control_store
        self.node_registry = node_registry or service.node_registry
        self.config = cfg or service.config
        self._clock_ms = clock_ms or service._clock_ms
        self._retry_since_ms: dict[str, int] = {}

    async def _withdraw_if_instance_back(self, task: DeadNodeReconcileTask) -> str:
        """检查判死实例是否已重新注册；返回 'cancelled' / 'unknown' / 'not_back'。"""
        try:
            nodes = await self.node_registry.list_scene_nodes(task.zone_id)
        except Exception as exc:
            logger.error(
                "dead node cleanup paused: registry read failed zone=%d node=%s error=%s",
                task.zone_id, task.node_id, exc,
            )
            return "unknown"
        for info in nodes:
            if str(info.node_id) != task.node_id or not getattr(info, "node_uuid", ""):
                continue
            try:
                verdict = await self.control_store.withdraw_dead_node_for_returning_instance(
                    task.zone_id, task.node_id, str(info.node_uuid),
                )
            except Exception as exc:
                logger.error(
                    "dead node cleanup paused: withdraw check failed zone=%d node=%s uuid=%s error=%s",
                    task.zone_id, task.node_id, info.node_uuid, exc,
                )
                return "unknown"
            if verdict == 1:
                self._retry_since_ms.pop(task.member, None)
                logger.info(
                    "dead node cleanup cancelled: returning instance re-registered zone=%d node=%s uuid=%s",
                    task.zone_id, task.node_id, info.node_uuid,
                )
                return "cancelled"
        return "not_back"

    async def _reentry_barrier_blocks(self, zone_id: int, node_id: str) -> bool:
        barrier_sec = self.config.reentry_barrier_sec
        if barrier_sec <= 0:
            return False
        try:
            death_at = await self.control_store.get_node_death_at(zone_id, node_id)
        except Exception as exc:
            logger.error("failed to read death_at; blocking dead node cleanup zone=%d node=%s error=%s", zone_id, node_id, exc)
            return True
        if death_at is None:
            return False
        elapsed = _death_at_elapsed_sec(death_at, int(self._clock_ms()))
        if elapsed < 0 or elapsed < barrier_sec:
            return True
        return False

    async def _reconcile_task(self, task: DeadNodeReconcileTask) -> bool:
        members = await self.control_store.get_node_reconcile_scenes(task.zone_id, task.node_id)
        fence = task.fence()
        resolved: list[str] = []
        failed: list[str] = []
        destroyed = 0
        skipped_world = 0
        stale = 0

        for scene_str in members:
            if not scene_str.isascii() or not scene_str.isdigit():
                resolved.append(scene_str)
                continue
            scene_id = int(scene_str)

            current_owner = await self.control_store.get_scene_node(task.zone_id, scene_id)
            if current_owner and current_owner != task.node_id:
                stale += 1
                resolved.append(scene_str)
                continue

            # 大世界频道不在 instances:{ctl:<z>}:active 集合内，交由世界频道自愈改派，绝不销毁
            try:
                is_inst = await self.control_store.is_active_instance(task.zone_id, scene_id)
            except Exception as exc:
                logger.error("check active instance failed zone=%d scene=%d error=%s", task.zone_id, scene_id, exc)
                failed.append(scene_str)
                continue
            if not is_inst:
                skipped_world += 1
                resolved.append(scene_str)
                continue

            try:
                claimed, withdrawn = await self.service.destroy_orphan_of_dead_node(
                    task.zone_id, scene_id, task.node_id, fence,
                )
            except Exception as exc:
                logger.error(
                    "destroy orphan instance failed zone=%d node=%s scene=%d error=%s",
                    task.zone_id, task.node_id, scene_id, exc,
                )
                failed.append(scene_str)
                continue

            if withdrawn:
                self._retry_since_ms.pop(task.member, None)
                logger.info(
                    "dead node cleanup stopped mid-pass: task withdrawn zone=%d node=%s scene=%d",
                    task.zone_id, task.node_id, scene_id,
                )
                return True
            if claimed:
                destroyed += 1
            else:
                stale += 1
            resolved.append(scene_str)

        if resolved:
            try:
                await self.control_store.remove_node_scenes(task.zone_id, task.node_id, resolved)
                await self.control_store.remove_node_reconcile_scenes(task.zone_id, task.node_id, resolved)
            except Exception as exc:
                logger.error("unlink reconciled scenes failed zone=%d node=%s error=%s", task.zone_id, task.node_id, exc)
                failed.extend(resolved)

        if failed:
            now_ms = int(self._clock_ms())
            started_ms = self._retry_since_ms.setdefault(task.member, now_ms)
            give_up_ms = max(0, int(self.config.dead_node_give_up_sec)) * 1000
            if give_up_ms > 0 and (now_ms - started_ms) >= give_up_ms:
                finished = await self.control_store.finish_dead_node_reconcile(
                    task.zone_id, task.node_id, task.member, task.queued_at_ms, abandon=True,
                )
                if finished:
                    self._retry_since_ms.pop(task.member, None)
                    logger.error(
                        "dead node cleanup abandoned after give-up deadline zone=%d node=%s failed=%d",
                        task.zone_id, task.node_id, len(failed),
                    )
                return finished
            return False

        finished = await self.control_store.finish_dead_node_reconcile(
            task.zone_id, task.node_id, task.member, task.queued_at_ms, abandon=False,
        )
        if finished:
            self._retry_since_ms.pop(task.member, None)
            logger.info(
                "dead node cleanup finished zone=%d node=%s destroyed=%d world_skipped=%d stale=%d",
                task.zone_id, task.node_id, destroyed, skipped_world, stale,
            )
        return finished

    async def sweep_once(self, zone_id: int | None = None) -> int:
        """处理本 zone 已过再入屏障的死节点收尾任务，返回本轮出队任务数。"""
        target_zone = self.config.zone_id if zone_id is None else zone_id
        tasks, malformed = await self.control_store.list_dead_node_reconcile_tasks(target_zone)
        for bad in malformed:
            logger.error("dropping malformed dead node reconcile member zone=%d member=%r", target_zone, bad)
            await self.control_store.remove_dead_node_reconcile_member(target_zone, bad)

        active_members = {t.member for t in tasks}
        for stale_member in list(self._retry_since_ms.keys()):
            if stale_member not in active_members:
                self._retry_since_ms.pop(stale_member, None)

        done = 0
        for task in tasks:
            # 1. 先检查判死那一代是否回归（先于屏障判断，防刚回归就清 death_at 后被拆）
            status = await self._withdraw_if_instance_back(task)
            if status == "cancelled":
                done += 1
                continue
            if status == "unknown":
                continue

            # 2. 检查 20s 再入屏障
            if await self._reentry_barrier_blocks(task.zone_id, task.node_id):
                continue

            # 3. 动手销毁孤儿副本并清理死节点计数
            if await self._reconcile_task(task):
                done += 1
        return done

    async def run(self, stop: asyncio.Event) -> None:
        interval = self.config.dead_node_sweep_interval_sec
        if interval <= 0:
            logger.info("dead node sweeper disabled by configuration")
            return
        logger.info(
            "dead node sweeper started zone=%d interval=%.1fs barrier=%ds",
            self.config.zone_id, interval, self.config.reentry_barrier_sec,
        )
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.sweep_once()
            except Exception as exc:
                logger.error("dead node sweep failed zone=%d error=%s", self.config.zone_id, exc)
