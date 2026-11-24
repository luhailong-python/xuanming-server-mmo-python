"""死 Gate 会话对账与清扫器（逐项对齐 Go ``gate_sweep.go`` 与 ``shared/liveview``）。

兜底「gate 整机崩溃 -> SetDisconnecting 永不到达 -> 会话永久停留在 ONLINE」缺口：
1. 每拍先读持有 slot 的 gate 登记集，再读 etcd/目录存活的 GateNodeService 实例集合（顺序不可颠倒）；
2. 经过存活集合理性闸门（空集永不放行、4+ gate 时骤降超半数压住等待稳定）与 2 拍连续缺席去抖；
3. 对确认死亡的 gate 逐页扫描 ``player:gate_index:{<slot>}:sessions:<dead_gate_uuid>``：
   - 仍在死 gate 上 ``ONLINE`` 的会话经整字节 CAS（``transition_to_disconnecting``，与 SetDisconnecting 同提交点）
     转为 ``DISCONNECTING`` 并挂断线租约；
   - 已迁移至存活 gate、已非 ONLINE、缺失或畸形的陈旧成员仅按字节守卫摘除索引，绝不触碰存活会话；
   - 索引清空后原子释放索引键并移出本 slot 的 gate 登记集。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Protocol as TypingProtocol

from google.protobuf.message import DecodeError

from ..cluster import GATE
from ..protocol import Protocol
from . import keys
from .config import DEFAULT_DISCONNECT_LEASE_SEC, DEFAULT_LIVE_VIEW_HOLD_SEC, PlayerLocatorConfig
from .store import SessionStore

logger = logging.getLogger(__name__)

# 基线 gate 数达到该门槛时才启用「消失超过一半」骤降压住（与 Go liveview.MinKnown 一致）
LIVE_VIEW_MIN_KNOWN = 4
DEBOUNCE_TICKS = 2


class LiveGateView(TypingProtocol):
    """查询当前存活 GateNodeService 实例 ``node_uuid`` 集合的外部视图接口。"""

    async def live_gate_instances(self) -> set[str]: ...


class DirectoryLiveGateView:
    """复用 ``EtcdDirectory`` / ``LocalDirectory`` 读取全区存活 Gate 实例 UUID。"""

    def __init__(self, directory) -> None:
        self.directory = directory

    async def live_gate_instances(self) -> set[str]:
        nodes = await self.directory.candidates(GATE, None)
        return {str(info.node_uuid) for info in nodes if getattr(info, "node_uuid", "")}


class LiveViewVerdict(str, Enum):
    ACCEPT = "accept"
    HOLD_EMPTY = "held_empty"
    HOLD_DROP = "held_drop"
    RELEASE = "released"

    @property
    def held(self) -> bool:
        return self in (LiveViewVerdict.HOLD_EMPTY, LiveViewVerdict.HOLD_DROP)


class LiveViewGate:
    """存活集合理性闸门（RecoverBelowThreshold，对应 Go ``shared/liveview.Gate``）。"""

    def __init__(self, hold_sec: float = DEFAULT_LIVE_VIEW_HOLD_SEC, *, min_known: int = LIVE_VIEW_MIN_KNOWN) -> None:
        self.hold_sec = hold_sec
        self.min_known = min_known
        self._holding = False
        self._last_missing = 0
        self._calm_since: float | None = None
        self._released_at: float | None = None

    def _reset(self) -> None:
        self._holding = False
        self._last_missing = 0
        self._calm_since = None
        self._released_at = None

    def _restart(self, missing: int, now: float) -> None:
        self._holding = True
        self._last_missing = missing
        self._calm_since = now

    def observe(self, *, known: int, missing: int, empty: bool, now: float) -> LiveViewVerdict:
        if known <= 0:
            return LiveViewVerdict.ACCEPT
        if missing <= 0:
            self._reset()
            return LiveViewVerdict.ACCEPT
        if self._released_at is not None:
            if not empty and (now - self._released_at) < self.hold_sec:
                return LiveViewVerdict.ACCEPT
            self._reset()
        if empty:
            self._restart(missing, now)
            return LiveViewVerdict.HOLD_EMPTY
        dropped = known >= self.min_known and missing * 2 > known
        if not self._holding:
            if not dropped:
                return LiveViewVerdict.ACCEPT
            self._restart(missing, now)
            return LiveViewVerdict.HOLD_DROP
        if not dropped:
            self._reset()
            return LiveViewVerdict.ACCEPT
        if missing != self._last_missing or self._calm_since is None:
            self._restart(missing, now)
            return LiveViewVerdict.HOLD_DROP
        if (now - self._calm_since) < self.hold_sec:
            return LiveViewVerdict.HOLD_DROP
        self._holding = False
        self._released_at = now
        return LiveViewVerdict.RELEASE


class IndexedMemberAction(IntEnum):
    ONLINE_HERE = 0
    STALE = 1
    MISSING = 2
    MALFORMED = 3
    BAD_MEMBER = 4


def classify_indexed_member(
    protocol: Protocol,
    member: str,
    raw: bytes | None,
    gate_uuid: str,
) -> tuple[int, object | None, IndexedMemberAction]:
    """纯函数判定 gate 索引中的单个成员应当转换还是按守卫摘除。"""
    player_id = keys.parse_index_member(member)
    if player_id is None:
        return 0, None, IndexedMemberAction.BAD_MEMBER
    if raw is None:
        return player_id, None, IndexedMemberAction.MISSING
    try:
        session = protocol.parse("playerlocator.PlayerSession", raw)
    except DecodeError:
        return player_id, None, IndexedMemberAction.MALFORMED
    if int(session.player_id) != player_id:
        return player_id, None, IndexedMemberAction.MALFORMED
    if keys.indexed_gate(session) == gate_uuid:
        return player_id, session, IndexedMemberAction.ONLINE_HERE
    return player_id, session, IndexedMemberAction.STALE


@dataclass(slots=True)
class SweepRoundReport:
    """单拍死 Gate 清扫统计，供日志与单测断言。"""

    stage: str = "ok"
    held: bool = False
    error: str | None = None
    dead_gates: list[str] = field(default_factory=list)
    released: list[tuple[int, str]] = field(default_factory=list)
    dead_index_keys: int = 0
    live_index_keys: int = 0
    actions: dict[str, int] = field(default_factory=dict)


class GateSweeper:
    """按拍追踪缺席 Gate（2 拍去抖）并将确认死亡 Gate 上的孤儿 ONLINE 会话转为 DISCONNECTING。"""

    def __init__(
        self,
        protocol: Protocol,
        store: SessionStore,
        live_view: LiveGateView,
        config: PlayerLocatorConfig | None = None,
        *,
        slot_owner: Callable[[], Sequence[int]] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.live_view = live_view
        self.config = config or PlayerLocatorConfig()
        self._all_slots = tuple(range(keys.SLOT_COUNT))
        self._slot_owner = slot_owner
        self.clock = clock
        self._suspects: dict[str, int] = {}
        self._cursors: dict[tuple[int, str], int] = {}
        self._live_view_gate = LiveViewGate(self.config.live_view_hold_sec)

    def owned_slots(self) -> Sequence[int]:
        return self._all_slots if self._slot_owner is None else self._slot_owner()

    def _check_live_view(self, pairs: Sequence[tuple[int, str]], live: set[str]) -> LiveViewVerdict:
        registered = {gate for _, gate in pairs}
        missing = sum(1 for gate in registered if gate not in live)
        verdict = self._live_view_gate.observe(
            known=len(registered),
            missing=missing,
            empty=(len(live) == 0),
            now=self.clock(),
        )
        if verdict == LiveViewVerdict.RELEASE:
            logger.error(
                "GateSweeper: live view gate released after hold; treating missing gates as dead "
                "registered_gates=%d missing=%d live=%d hold_sec=%.1f",
                len(registered), missing, len(live), self.config.live_view_hold_sec,
            )
        return verdict

    def _advance_suspects(
        self,
        pairs: Sequence[tuple[int, str]],
        live: set[str],
    ) -> tuple[list[str], list[tuple[int, str]], int]:
        in_registry = set(pairs)
        gates = {gate for _, gate in pairs}
        dead_set: set[str] = set()
        for gate in gates:
            if gate in live:
                self._suspects.pop(gate, None)
                continue
            misses = min(self._suspects.get(gate, 0) + 1, DEBOUNCE_TICKS)
            self._suspects[gate] = misses
            if misses >= DEBOUNCE_TICKS:
                dead_set.add(gate)

        for gate in list(self._suspects):
            if gate not in gates:
                self._suspects.pop(gate, None)

        for pair in list(self._cursors):
            if pair not in in_registry or pair[1] not in dead_set:
                self._cursors.pop(pair, None)

        dead_pairs = sorted((pair for pair in pairs if pair[1] in dead_set), key=lambda item: (item[1], item[0]))
        live_pairs = sum(1 for _, gate in pairs if gate in live)
        return sorted(dead_set), dead_pairs, live_pairs

    async def _transition_online(self, player_id: int, expected: bytes, session) -> bool:
        updated_session = type(session)()
        updated_session.CopyFrom(session)
        updated_session.state = keys.SESSION_STATE_DISCONNECTING
        updated_session.session_version += 1
        updated = updated_session.SerializeToString()

        ttl_sec = self.config.default_disconnect_lease_sec or DEFAULT_DISCONNECT_LEASE_SEC
        deadline_unix_sec = int(self.clock() + ttl_sec)
        swapped = await self.store.transition_to_disconnecting(
            player_id, expected, updated, deadline_unix_sec, str(session.gate_instance_id),
        )
        if swapped:
            logger.info(
                "GateSweeper: orphaned ONLINE session -> DISCONNECTING player=%d session=%d dead_gate_instance=%s",
                player_id, session.session_id, session.gate_instance_id,
            )
        return swapped

    async def _sweep_index_page(
        self,
        slot: int,
        gate_uuid: str,
        members: Sequence[str],
        actions: dict[str, int],
    ) -> None:
        valid_members: list[str] = []
        player_ids: list[int] = []
        for member in members:
            player_id = keys.parse_index_member(member)
            if player_id is None or keys.player_slot(player_id) != slot:
                await self.store.remove_bad_gate_index_member(slot, gate_uuid, member)
                actions["pruned_bad_member"] = actions.get("pruned_bad_member", 0) + 1
                logger.error(
                    "GateSweeper: pruned bad member=%r from gate_instance=%s slot=%d",
                    member, gate_uuid, slot,
                )
                continue
            valid_members.append(member)
            player_ids.append(player_id)

        if not player_ids:
            return

        raws = await self.store.mget_sessions_bytes(player_ids)
        for member, player_id, raw in zip(valid_members, player_ids, raws):
            parsed_pid, session, action = classify_indexed_member(self.protocol, member, raw, gate_uuid)
            if action == IndexedMemberAction.ONLINE_HERE:
                assert raw is not None and session is not None
                swapped = await self._transition_online(parsed_pid, raw, session)
                key_name = "to_disconnecting" if swapped else "cas_conflict"
                actions[key_name] = actions.get(key_name, 0) + 1
                continue

            if action == IndexedMemberAction.MISSING:
                metric = "pruned_missing"
            elif action == IndexedMemberAction.MALFORMED:
                metric = "pruned_malformed"
                logger.error(
                    "GateSweeper: malformed session behind index member gate_instance=%s player=%d; pruning member",
                    gate_uuid, parsed_pid,
                )
            else:
                metric = "pruned_stale"

            removed = await self.store.prune_gate_index_member(parsed_pid, gate_uuid, member, raw)
            if removed:
                actions[metric] = actions.get(metric, 0) + 1

    async def _process_dead_gates(
        self,
        dead_pairs: Sequence[tuple[int, str]],
        report: SweepRoundReport,
    ) -> None:
        budget = self.config.dead_gate_members_per_round
        for slot, gate_uuid in dead_pairs:
            if budget <= 0:
                return
            pair = (slot, gate_uuid)
            while budget > 0:
                cursor = self._cursors.get(pair, 0)
                next_cursor, members = await self.store.scan_gate_index_members(
                    slot, gate_uuid, cursor=cursor, count=self.config.gate_scan_count,
                )
                await self._sweep_index_page(slot, gate_uuid, members, report.actions)
                budget -= len(members)
                if next_cursor != 0:
                    self._cursors[pair] = next_cursor
                    continue
                self._cursors.pop(pair, None)
                if await self.store.release_gate_index_if_empty(slot, gate_uuid):
                    report.released.append(pair)
                break

    async def sweep_round(self) -> SweepRoundReport:
        """执行一拍清扫：读登记集 -> 读存活集 -> 合理性闸门 -> 2 拍去抖 -> 预算内清扫死 Gate。"""
        report = SweepRoundReport()
        slots = list(self.owned_slots())
        if not slots:
            return report

        # a) 必须先读登记集再读 etcd 存活集，防止并发新注册的 gate 被误判缺席
        try:
            pairs = await self.store.list_registered_gates(slots)
        except Exception as exc:
            report.stage = "redis"
            report.error = str(exc)
            logger.error("GateSweeper: read gate registries failed error=%s", exc)
            return report

        # b) fail-closed：读不到 etcd 存活集或被合理性闸门压住时，不推进去抖、不转换会话
        try:
            live = await self.live_view.live_gate_instances()
        except Exception as exc:
            report.stage = "etcd"
            report.error = str(exc)
            logger.error("GateSweeper: list live gates failed error=%s", exc)
            return report

        verdict = self._check_live_view(pairs, live)
        if verdict.held:
            report.stage = "view_held"
            report.held = True
            report.error = f"live view held: {verdict.value}"
            logger.error(
                "GateSweeper: round held by live view gate verdict=%s pairs=%d live=%d",
                verdict.value, len(pairs), len(live),
            )
            return report

        # c) 推进 gate 级 2 拍去抖
        dead_gates, dead_pairs, live_pairs = self._advance_suspects(pairs, live)
        report.dead_gates = dead_gates

        # d) 在预算内处理已确认死亡的 gate 索引
        try:
            await self._process_dead_gates(dead_pairs, report)
        except Exception as exc:
            report.stage = "redis"
            report.error = str(exc)
            logger.error("GateSweeper: process dead gates failed error=%s", exc)
            return report

        released_set = set(report.released)
        report.dead_index_keys = sum(1 for pair in dead_pairs if pair not in released_set)
        report.live_index_keys = live_pairs
        if report.dead_gates or report.actions:
            logger.info(
                "GateSweeper: round dead_gates=%d released_keys=%d dead_index_keys_left=%d "
                "live_index_keys=%d actions=%s",
                len(report.dead_gates), len(report.released),
                report.dead_index_keys, report.live_index_keys, report.actions,
            )
        return report

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        interval = self.config.reconcile_interval_sec
        if interval <= 0:
            logger.info("GateSweeper disabled by config reconcile_interval_sec=%.1f", interval)
            return
        logger.info(
            "GateSweeper started interval_sec=%.1f members_per_round=%d live_view_hold_sec=%.1f",
            interval, self.config.dead_gate_members_per_round, self.config.live_view_hold_sec,
        )
        try:
            while stop_event is None or not stop_event.is_set():
                try:
                    await self.sweep_round()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("GateSweeper: unexpected round failure")
                if stop_event is None:
                    await asyncio.sleep(interval)
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=interval)
                        break
                    except TimeoutError:
                        continue
        finally:
            logger.info("GateSweeper stopped")
