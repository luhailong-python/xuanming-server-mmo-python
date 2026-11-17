"""Zone list from bounded SQL and an atomic, expiring etcd health snapshot."""

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import grpc
from aiohttp import web
from google.protobuf.json_format import ParseError

from .portal_db import as_bool
from .portal_sql import connection

_LOG = logging.getLogger(__name__)
_HEALTH_PREFIX = b"/mmorpg/zone_health/zone/"
_HEALTH_KEY = re.compile(rb"/mmorpg/zone_health/zone/([1-9][0-9]{0,18})/publisher/[A-Za-z0-9._-]+\Z")
_LOGIN_PREFIX = b"LoginNodeService.rpc/zone/"
_STATUS_TTL = 15.0
_POLL_INTERVAL = 5.0
_MAX_MIRROR_AGE = 300.0


@dataclass(frozen=True, slots=True)
class Health:
    status: str
    online: int
    capacity: int

    @property
    def ratio(self):
        return self.online / self.capacity if self.capacity else 0.0


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    by_zone: dict[int, Health]
    login_zones: frozenset[int]
    captured_at: float


def _decode_health(key, value):
    match = _HEALTH_KEY.fullmatch(key)
    if match is None:
        raise ValueError("invalid health key")
    zone = int(match.group(1))
    if zone >= 2**63:
        raise ValueError("invalid health zone")
    data = json.loads(value)
    if not isinstance(data, dict):
        raise ValueError("invalid health value")
    status, online, capacity = data.get("status"), data.get("onlineCount"), data.get("capacity")
    if (type(data.get("zoneId")) is not int or data["zoneId"] != zone
            or status not in ("HEALTHY", "DEGRADED", "DOWN")
            or type(online) is not int or not 0 <= online < 2**63
            or type(capacity) is not int or not 0 <= capacity < 2**63):
        raise ValueError("invalid health fields")
    return zone, Health(status, online, capacity)


class _PrefixMirror:
    """A single-owner list/watch mirror; bad records poison complete reads."""

    def __init__(self, directory, prefix, decode):
        self.directory, self.prefix, self.decode = directory, prefix, decode
        self.end = prefix[:-1] + bytes((prefix[-1] + 1,))
        self.entries = {}
        self.bad_keys = set()
        self.synced = False

    def _apply(self, kv, *, deleted=False):
        key = bytes(kv.key)
        if not key.startswith(self.prefix):
            raise ValueError("watch event outside requested prefix")
        if deleted:
            self.entries.pop(key, None)
            self.bad_keys.discard(key)
            return
        try:
            value = self.decode(key, bytes(kv.value))
        except (ValueError, TypeError, UnicodeError, ParseError):
            self.entries.pop(key, None)
            self.bad_keys.add(key)
        else:
            self.entries[key] = (value, kv.mod_revision)
            self.bad_keys.discard(key)

    async def run(self):
        backoff = 1
        while True:
            call = None
            immediate = False
            try:
                response = await self.directory._rpc("KV", "Range", key=self.prefix, range_end=self.end)
                self.entries = {}
                self.bad_keys = set()
                for kv in response.kvs:
                    self._apply(kv)
                call = self.directory.client.channel.stream_stream("/etcdserverpb.Watch/Watch")()
                request = self.directory.protocol.message("etcdserverpb.WatchRequest", create_request={
                    "key": self.prefix, "range_end": self.end, "start_revision": response.header.revision + 1,
                })
                # Neither a half-open connection nor a hung creation may make
                # an unconfirmed mirror look healthy indefinitely.
                async with asyncio.timeout(4):
                    await call.write(request.SerializeToString())
                    first = await call.read()
                if first is grpc.aio.EOF:
                    raise ConnectionError("etcd watch closed during creation")
                created = self.directory.protocol.parse("etcdserverpb.WatchResponse", first)
                if created.canceled:
                    immediate = bool(created.compact_revision)
                    raise ConnectionError("etcd watch was cancelled")
                if not created.created:
                    raise ConnectionError("etcd watch creation was not acknowledged")
                for event in created.events:
                    if event.type not in (0, 1):
                        raise ValueError("unknown etcd watch event")
                    self._apply(event.kv, deleted=event.type == 1)
                self.synced = True
                backoff = 1
                deadline = asyncio.get_running_loop().time() + _MAX_MIRROR_AGE
                while True:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        immediate = True
                        break
                    try:
                        async with asyncio.timeout(remaining):
                            raw = await call.read()
                    except TimeoutError:
                        immediate = True
                        break
                    if raw is grpc.aio.EOF:
                        raise ConnectionError("etcd watch closed")
                    update = self.directory.protocol.parse("etcdserverpb.WatchResponse", raw)
                    if update.canceled:
                        immediate = bool(update.compact_revision)
                        raise ConnectionError("etcd watch was cancelled")
                    # All events in a response apply before the event loop can
                    # publish a combined snapshot.
                    for event in update.events:
                        if event.type not in (0, 1):
                            raise ValueError("unknown etcd watch event")
                        self._apply(event.kv, deleted=event.type == 1)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOG.warning("portal etcd watch failed for %s; re-listing", self.prefix.decode(), exc_info=True)
            finally:
                self.synced = False
                if call is not None:
                    call.cancel()
            if not immediate:
                await asyncio.sleep(backoff)
                backoff = min(30, backoff * 2)


class HealthReader:
    """Publish only complete snapshots of two independently watched prefixes."""

    def __init__(self, directory, *, clock=time.monotonic):
        self.directory = directory
        self.clock = clock
        self.snapshot = None
        self.health = _PrefixMirror(directory, _HEALTH_PREFIX, _decode_health)
        self.logins = _PrefixMirror(directory, _LOGIN_PREFIX, self._decode_login)

    def _decode_login(self, key, value):
        info = self.directory._info(value)
        if info.node_type != 5 or info.zone_id <= 0 or key != self.directory._service_key(info):
            raise ValueError("invalid Login registration")
        return info.zone_id

    def publish(self):
        if not self.health.synced or not self.logins.synced or self.health.bad_keys or self.logins.bad_keys:
            return False
        latest = {}
        for key in sorted(self.health.entries):
            (zone, record), revision = self.health.entries[key]
            previous = latest.get(zone)
            if previous is None or revision > previous[0]:
                latest[zone] = (revision, record)
        login_zones = frozenset(value for value, _ in self.logins.entries.values())
        self.snapshot = HealthSnapshot({zone: value[1] for zone, value in latest.items()},
                                       login_zones, self.clock())
        return True

    def current(self):
        result = self.snapshot
        if result is None or not 0 <= self.clock() - result.captured_at <= _STATUS_TTL:
            return None
        return result

    async def run(self):
        tasks = [asyncio.create_task(self.health.run(), name="portal-health-watch"),
                 asyncio.create_task(self.logins.run(), name="portal-login-watch")]
        try:
            while True:
                self.publish()
                await asyncio.sleep(_POLL_INTERVAL)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def _zone_view(row, snapshot, now):
    zone_id, name, status, _capacity, message, open_time, recommended, _, closed, created = row
    zone_id = int(zone_id)
    manual = ("OPEN", "MAINTENANCE", "CLOSED", "PREVIEW")
    display = manual[status] if 0 <= status < len(manual) else "OPEN"
    record = snapshot.by_zone.get(zone_id) if snapshot else None
    auto_down = bool(snapshot and (record is not None and record.status == "DOWN"
                                  or record is None and zone_id not in snapshot.login_zones))
    if display == "OPEN" and auto_down:
        display = "MAINTENANCE"
    result = {"zone_id": zone_id, "name": name, "status": display,
              "is_new": bool(created and created > now - timedelta(days=7)),
              "recommended": as_bool(recommended), "create_role_closed": as_bool(closed)}
    ratio = None
    if display == "OPEN" and record is not None:
        ratio = record.ratio
        result["load_level"] = "SMOOTH" if ratio < .5 else "BUSY" if ratio < .8 else "FULL"
    if display in ("MAINTENANCE", "CLOSED") and message is not None:
        result["maintenance_msg"] = message
    if status == 3 and open_time is not None:
        result["open_time"] = int(open_time.replace(tzinfo=timezone.utc).timestamp())
    return result, ratio


def make_list(rows, snapshot, now):
    results = [_zone_view(row, snapshot, now) for row in sorted(rows, key=lambda item: item[7])]
    for item, _ in results:
        item["recommended"] = (item["recommended"] and item["status"] == "OPEN"
                               and not item["create_role_closed"] and item.get("load_level") != "FULL")
    if not any(item["recommended"] for item, _ in results):
        available = [(ratio, index) for index, (item, ratio) in enumerate(results)
                     if item["status"] == "OPEN" and not item["create_role_closed"] and ratio is not None]
        if available:
            results[min(available)[1]][0]["recommended"] = True
    return {"zones": [item for item, _ in results]}


def register_routes(app, deps):
    lock = asyncio.Lock()
    cached_at, cached_rows = 0.0, None
    reader = HealthReader(deps.directory) if deps.directory is not None else None

    async def health_lifecycle(_app):
        task = asyncio.create_task(reader.run(), name="portal-zone-health") if reader else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    app.cleanup_ctx.append(health_lifecycle)

    async def get_rows():
        nonlocal cached_at, cached_rows
        clock = asyncio.get_running_loop().time
        if cached_rows is not None and clock() - cached_at < 1:
            return cached_rows
        async with asyncio.timeout(3):
            async with lock:
                if cached_rows is not None and clock() - cached_at < 1:
                    return cached_rows
                async with connection(deps.db) as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute("SELECT zone_id,name,manual_status,capacity,maintenance_msg,open_time,"
                                             "recommended,sort_order,create_role_closed,created_at FROM zone_config")
                        rows = await cursor.fetchall()
                    await conn.rollback()
                cached_rows, cached_at = tuple(rows), clock()
                return cached_rows

    async def server_list(request):
        rows = await get_rows()
        return web.json_response(make_list(rows, reader.current() if reader else None,
                                           datetime.now(timezone.utc).replace(tzinfo=None)))

    app.router.add_get("/api/server-list", server_list)
