"""Fail-open etcd RPC kill switch, scoped to one node instance."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from dataclasses import dataclass

import grpc

from .grpc_runtime import GrpcClient

_LOG = logging.getLogger(__name__)
_PREFIX = b"/mmorpg/killswitch/"
_RANGE_END = _PREFIX[:-1] + bytes((_PREFIX[-1] + 1,))
_STALE = 60.0
_PROTECTED = frozenset({
    "Gate/NodeHandshake", "Scene/NodeHandshake", "Gate/GmGracefulShutdown",
    "SceneScene/GmGracefulShutdown", "Scene/ProcessClientPlayerMessage", "ScenePlayer/ExitGame",
    "SceneNodeGrpc/ReleasePlayer", "ClientPlayerLogin/LeaveGame", "BattleClientPlayer/StopWatchBattle",
    "GateClientSpectate/UnsubscribeSpectate", "BattleNode/RemoveObserver", "Scene/DestroyScene",
    "SceneNodeGrpc/DestroyScene", "BattleNode/DestroyBattle", "Scene/CancelBattlePrepare",
    "SceneNodeGrpc/CancelBattlePrepare", "SceneNodeGrpc/AssetAbortDebit", "SceneNodeGrpc/AssetAckReceipt",
})


@dataclass(frozen=True, slots=True)
class Rule:
    deny: bool = False
    reason: str = ""
    code: int = 0

    @property
    def grpc_code(self) -> int:
        return self.code if 1 <= self.code <= 16 else 14


def parse_rule(value: bytes | str) -> Rule | None:
    try:
        value = value.decode("utf-8") if isinstance(value, bytes) else value
        value = value.strip()
        if not value:
            return Rule()
        if value.startswith("{"):
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                return None
            deny, reason, code = (parsed.get(key) for key in ("deny", "reason", "code"))
            if ((deny is not None and type(deny) is not bool)
                    or (reason is not None and not isinstance(reason, str))
                    or (code is not None and (type(code) not in (int, float) or not math.isfinite(code)
                                              or code < 0 or int(code) != code or code > 0xffffffff))):
                return None
            return Rule(bool(deny), reason or "", int(code or 0))
        word = value.lower()
        if word in ("1", "true", "on", "yes", "deny"):
            return Rule(True)
        if word in ("0", "false", "off", "no", "allow"):
            return Rule()
    except (UnicodeDecodeError, ValueError, TypeError, OverflowError):
        pass
    return None


def parse_key(key: str) -> tuple[str, str, str] | None:
    key = key.strip().lstrip("/")
    if not key:
        return None
    for scope in ("instance", "type"):
        if key.startswith(scope + "/"):
            name, sep, target = key[len(scope) + 1:].partition("/")
            return (scope, name, target) if name and sep and target else None
    return "all", "", key


class KillSwitch:
    def __init__(self, protocol, info, *, clock=time.monotonic) -> None:
        self.protocol = protocol
        self.node_type = protocol.pool.FindEnumTypeByName("common.base.eNodeType").values_by_number[info.node_type].name
        self.node_uuid = info.node_uuid
        self.clock = clock
        self.synced_at: float | None = None
        self.revision = 0
        self._raw: dict[bytes, bytes] = {}
        self.rules: dict[tuple[str, str, str], Rule] = {}

    def replace(self, revision: int, entries) -> None:
        self.synced_at = self.clock()
        if revision < self.revision:
            return
        self._raw = dict(entries)
        self.revision = revision
        self._rebuild()

    def _rebuild(self) -> None:
        rules = {}
        for key, value in sorted(self._raw.items()):
            try:
                if not key.startswith(_PREFIX):
                    continue
                parsed = parse_key(key[len(_PREFIX):].decode("utf-8"))
            except UnicodeDecodeError:
                continue
            rule = parse_rule(value)
            if parsed is not None and rule is not None:
                if parsed not in rules or rule.deny:
                    rules[parsed] = rule
        self.rules = rules

    def apply_events(self, events, *, header_revision: int = 0) -> None:
        # One watch response can contain several keys at the same revision.
        baseline = self.revision
        updated = False
        for event in events:
            key, revision = event.kv.key, event.kv.mod_revision or header_revision
            if revision < baseline or not key.startswith(_PREFIX):
                continue
            if event.type == 0:
                self._raw[key] = event.kv.value
            else:
                self._raw.pop(key, None)
            self.revision = max(self.revision, revision)
            updated = True
        if updated:
            self.synced_at = self.clock()
            self._rebuild()

    def check(self, service: str, method: str, *, protected: bool | None = None) -> Rule | None:
        if self.synced_at is None or self.clock() - self.synced_at > _STALE:
            return None
        short = service.rsplit(".", 1)[-1]
        if protected is None:
            protected = f"{short}/{method}" in _PROTECTED
        targets = [f"{service}/{method}"]
        if short != service:
            targets.append(f"{short}/{method}")
        if not protected:
            targets.append(f"{service}/*")
            if short != service:
                targets.append(f"{short}/*")
            targets.append("*")
        for scope, identity in (("instance", self.node_uuid), ("type", self.node_type), ("all", "")):
            for target in targets:
                rule = self.rules.get((scope, identity, target))
                if rule is not None:
                    return rule if rule.deny else None
        return None

    def check_id(self, message_id: int) -> Rule | None:
        route = self.protocol.routes.get(message_id)
        return self.check(route.service, route.method) if route is not None else None


class EtcdKillSwitch:
    """Continuously reconcile a watch with linearizable prefix reads."""

    def __init__(self, protocol, endpoint: str, target: KillSwitch) -> None:
        self.protocol, self.target = protocol, target
        self.client = GrpcClient(endpoint, timeout=4)
        self._tasks: tuple[asyncio.Task, ...] = ()
        self._ready = asyncio.Event()
        self._closed = False

    def start(self) -> None:
        if not self._tasks:
            self._tasks = (asyncio.create_task(self._range_loop(), name="killswitch-range"),
                           asyncio.create_task(self._watch_loop(), name="killswitch-watch"))

    async def _range_loop(self) -> None:
        while not self._closed:
            try:
                request = self.protocol.message("etcdserverpb.RangeRequest", key=_PREFIX, range_end=_RANGE_END)
                raw = await self.client.call("/etcdserverpb.KV/Range", request.SerializeToString())
                response = self.protocol.parse("etcdserverpb.RangeResponse", raw)
                self.target.replace(response.header.revision, ((kv.key, kv.value) for kv in response.kvs))
                self._ready.set()
                delay = random.uniform(16, 24)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOG.warning("Kill switch etcd range unavailable", exc_info=True)
                delay = 1 if not self._ready.is_set() else 20
            await asyncio.sleep(delay)

    async def _watch_loop(self) -> None:
        await self._ready.wait()
        backoff = 1
        while not self._closed:
            call = None
            compacted = False
            try:
                call = self.client.channel.stream_stream("/etcdserverpb.Watch/Watch")()
                request = self.protocol.message("etcdserverpb.WatchRequest", create_request={
                    "key": _PREFIX, "range_end": _RANGE_END, "start_revision": self.target.revision + 1,
                })
                await call.write(request.SerializeToString())
                while not self._closed:
                    raw = await call.read()
                    if raw is grpc.aio.EOF:
                        raise ConnectionError("etcd watch closed")
                    response = self.protocol.parse("etcdserverpb.WatchResponse", raw)
                    if response.created and not response.canceled:
                        backoff = 1
                    if response.canceled:
                        compacted = bool(response.compact_revision)
                        break
                    if response.events:
                        self.target.apply_events(response.events, header_revision=response.header.revision)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOG.warning("Kill switch etcd watch unavailable", exc_info=True)
            finally:
                if call is not None:
                    call.cancel()
            # Resynchronize before watching again, including after compaction.
            ranged = False
            try:
                request = self.protocol.message("etcdserverpb.RangeRequest", key=_PREFIX, range_end=_RANGE_END)
                raw = await self.client.call("/etcdserverpb.KV/Range", request.SerializeToString())
                response = self.protocol.parse("etcdserverpb.RangeResponse", raw)
                self.target.replace(response.header.revision, ((kv.key, kv.value) for kv in response.kvs))
                ranged = True
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            if not compacted or not ranged:
                await asyncio.sleep(backoff)
                backoff = min(30, backoff * 2)

    async def close(self) -> None:
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.client.close()
