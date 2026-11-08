"""Agones sidecar lifecycle and room-allocation permits for Battle."""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from enum import Enum, auto

logger = logging.getLogger(__name__)

_DRAIN_LABEL = "mmorpg.io/drain"
_ENDPOINT_BUDGET = 60
_READY_INITIAL_BACKOFF = 0.2
_READY_MAX_BACKOFF = 2
_ALLOCATE_WAIT = 3
_HEALTH_INTERVAL = 2
_DRAIN_INTERVAL = 5
_STALE_HEARTBEAT = 10


class _Phase(Enum):
    STARTING = auto()
    READY = auto()
    ALLOCATING = auto()
    ALLOCATED = auto()
    RETURNING = auto()
    STOPPED = auto()


class BattleAgones:
    def __init__(self, host: str, port: int) -> None:
        from aiohttp import ClientError, ClientSession, ClientTimeout

        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        self._url = f"http://{host}:{port}"
        self._http = ClientSession(timeout=ClientTimeout(total=2, connect=0.5), trust_env=False)
        self._http_errors = (ClientError, TimeoutError, ValueError)
        self._phase = _Phase.STARTING
        self._run_task = None
        self._transition = None
        self._changed = asyncio.Event()
        self._draining = False
        self._rooms = self._permits = self._waiters = self._allocation_failures = 0
        self._heartbeat = asyncio.get_running_loop().time()

    @classmethod
    def from_env(cls):
        if os.getenv("AGONES_ENABLED") in ("0", "false"):
            return None
        port_text = os.getenv("AGONES_SDK_HTTP_PORT")
        if not port_text:
            return None
        try:
            port = int(port_text)
            if not 1 <= port <= 65535:
                raise ValueError
        except ValueError:
            logger.warning("invalid AGONES_SDK_HTTP_PORT; Battle Agones lifecycle disabled")
            return None
        return cls(os.getenv("AGONES_SDK_HOST") or "127.0.0.1", port)

    async def _request(self, path: str):
        method, options = ("GET", {}) if path == "gameserver" else ("POST", {"json": {}})
        async with self._http.request(method, f"{self._url}/{path}", allow_redirects=False, **options) as response:
            if not 200 <= response.status < 300:
                raise ValueError(f"Agones {path} returned HTTP {response.status}")
            if method == "GET":
                return json.loads(await response.read())
            await response.read()

    async def client_endpoint(self) -> tuple[str, int]:
        backoff = _READY_INITIAL_BACKOFF
        try:
            async with asyncio.timeout(_ENDPOINT_BUDGET):
                for attempt in range(30):
                    try:
                        return _endpoint(await self._request("gameserver"))
                    except self._http_errors as exc:
                        logger.warning("Agones client endpoint attempt %s failed: %s", attempt + 1, exc)
                    if attempt < 29:
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, _READY_MAX_BACKOFF)
        except TimeoutError as exc:
            raise RuntimeError("Agones client endpoint lookup exhausted its time budget") from exc
        raise RuntimeError("Agones client endpoint lookup exhausted its attempts")

    def start(self) -> None:
        if self._phase == _Phase.STARTING and self._run_task is None:
            self._run_task = asyncio.create_task(self._run(), name="battle-agones")

    @property
    def admitting(self) -> bool:
        return not self._draining and self._phase not in (_Phase.STARTING, _Phase.STOPPED)

    def touch_heartbeat(self) -> None:
        self._heartbeat = asyncio.get_running_loop().time()

    def _notify(self) -> None:
        self._changed.set()
        self._changed = asyncio.Event()

    def _set_phase(self, phase: _Phase) -> None:
        self._phase = phase
        self._notify()

    async def _run(self) -> None:
        backoff = _READY_INITIAL_BACKOFF
        attempts = 0
        while True:
            try:
                await self._request("ready")
                break
            except self._http_errors as exc:
                attempts += 1
                if attempts <= 30 or attempts % 30 == 0:
                    logger.warning("Agones initial readiness attempt %s failed: %s", attempts, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _READY_MAX_BACKOFF)
        await self._poll_drain()
        self._set_phase(_Phase.READY)
        async with asyncio.TaskGroup() as group:
            group.create_task(self._health_loop(), name="battle-agones-health")
            group.create_task(self._drain_loop(), name="battle-agones-drain")

    async def _health_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(_HEALTH_INTERVAL)
            if loop.time() - self._heartbeat < _STALE_HEARTBEAT:
                try:
                    await self._request("health")
                except self._http_errors as exc:
                    logger.warning("Agones health request failed: %s", exc)

    async def _drain_loop(self) -> None:
        while True:
            await asyncio.sleep(_DRAIN_INTERVAL)
            await self._poll_drain()

    async def _poll_drain(self) -> None:
        try:
            draining = _DRAIN_LABEL in _labels(await self._request("gameserver"))
        except self._http_errors as exc:
            logger.warning("Agones drain poll failed; keeping draining=%s: %s", self._draining, exc)
            return
        if draining != self._draining:
            self._draining = draining
            self._notify()
        if draining:
            self._return_if_idle()

    @asynccontextmanager
    async def permit(self):
        granted = await self._acquire()
        try:
            yield granted
        finally:
            if granted:
                self._permits -= 1
                self._return_if_idle()

    async def _acquire(self) -> bool:
        generation = self._allocation_failures
        self._waiters += 1
        try:
            async with asyncio.timeout(_ALLOCATE_WAIT):
                while self.admitting and generation == self._allocation_failures:
                    if self._phase == _Phase.ALLOCATED:
                        self._permits += 1
                        return True
                    if self._phase == _Phase.READY:
                        self._set_phase(_Phase.ALLOCATING)
                        self._transition = asyncio.create_task(self._allocate(), name="battle-agones-allocate")
                    await self._changed.wait()
        except TimeoutError:
            logger.warning("Agones Battle allocation permit timed out")
        finally:
            self._waiters -= 1
            self._return_if_idle()
        return False

    async def _allocate(self) -> None:
        allocated = False
        for attempt in range(3):
            try:
                await self._request("allocate")
                allocated = True
                break
            except self._http_errors as exc:
                logger.warning("Agones allocate attempt %s failed: %s", attempt + 1, exc)
            if self._draining:
                break
            if attempt < 2:
                await asyncio.sleep(_READY_INITIAL_BACKOFF)
        self._transition = None
        if not allocated:
            self._allocation_failures += 1
        self._set_phase(_Phase.ALLOCATED if allocated else _Phase.READY)
        self._return_if_idle()

    def rooms_changed(self, count: int) -> None:
        self._rooms = count
        self._return_if_idle()

    def _return_if_idle(self) -> None:
        if (self._phase != _Phase.ALLOCATED or self._transition is not None
                or self._rooms or self._permits or self._waiters):
            return
        self._transition = asyncio.create_task(self._back_to_ready(), name="battle-agones-return-ready")

    async def _back_to_ready(self) -> None:
        if self._rooms or self._permits or self._waiters:
            self._transition = None
            return
        self._set_phase(_Phase.RETURNING)
        phase = _Phase.READY
        try:
            await self._request("ready")
        except self._http_errors as exc:
            logger.warning("Agones return to Ready failed: %s", exc)
            phase = _Phase.ALLOCATED
        self._transition = None
        self._set_phase(phase)

    async def close(self) -> None:
        if self._phase == _Phase.STOPPED:
            return
        self._set_phase(_Phase.STOPPED)
        tasks = tuple(task for task in (self._transition, self._run_task) if task is not None)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._http.close()


def _endpoint(data) -> tuple[str, int]:
    status = data.get("status") if isinstance(data, dict) else None
    if isinstance(status, dict) and isinstance(status.get("ports"), list):
        address = status.get("address")
        for item in status["ports"]:
            if isinstance(item, dict) and item.get("name") == "client":
                port = item.get("port")
                if isinstance(port, str) and port.isascii() and port.isdecimal():
                    port = int(port)
                if isinstance(address, str) and address and type(port) is int and 1 <= port <= 65535:
                    return address, port
    raise ValueError("Agones GameServer has no assigned client address and port")


def _labels(data) -> dict[str, str]:
    metadata = data.get("object_meta", data.get("objectMeta")) if isinstance(data, dict) else None
    if isinstance(metadata, dict):
        labels = metadata.get("labels")
        if labels is None:
            return {}
        if isinstance(labels, dict) and all(isinstance(value, str) for value in labels.values()):
            return labels
    raise ValueError("Agones GameServer has invalid metadata or labels")
