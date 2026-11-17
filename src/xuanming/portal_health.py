"""Publish expiring per-zone health snapshots for other portal replicas."""

import asyncio
import json
import logging
import time
import uuid

import grpc

_LOG = logging.getLogger(__name__)


def summarize(zone, capacity, gates, scenes):
    online = sum(node.player_count for node in gates)
    caps = [node.max_connections for node in gates]
    gate_cap = sum((value * 90 + 99) // 100 for value in caps) if gates and all(caps) else 0
    capacity = max(0, capacity)
    effective = min(capacity, gate_cap) if capacity and gate_cap else max(capacity, gate_cap)
    return {"zoneId": zone, "status": "DOWN" if not gates else "DEGRADED" if not scenes else "HEALTHY",
            "onlineCount": online, "capacity": effective}


def _level(record):
    capacity = record["capacity"]
    ratio = record["onlineCount"] / capacity if capacity else 0
    return 0 if ratio < .5 else 1 if ratio < .8 else 2


def should_publish(old, new, age):
    if old["status"] != new["status"] or old["capacity"] != new["capacity"]:
        return True
    capacity = new["capacity"]
    if capacity <= 0 or abs(new["onlineCount"] - old["onlineCount"]) < max(1, (capacity + 99) // 100):
        return False
    return _level(old) != _level(new) or age >= 30


class HealthPublisher:
    def __init__(self, deps):
        self.deps = deps
        self.instance = str(uuid.uuid4())
        self.current_keys = set()
        self.lease = 0
        self.published = {}

    async def _lease(self):
        deps = self.deps
        if self.lease:
            try:
                request = deps.protocol.message("etcdserverpb.LeaseKeepAliveRequest", ID=self.lease)
                call = deps.directory.client.channel.stream_stream("/etcdserverpb.Lease/LeaseKeepAlive")()
                try:
                    await call.write(request.SerializeToString())
                    async with asyncio.timeout(4):
                        raw = await call.read()
                    if raw is not grpc.aio.EOF:
                        result = deps.protocol.parse("etcdserverpb.LeaseKeepAliveResponse", raw)
                        if result.ID == self.lease and result.TTL > 0:
                            return
                finally:
                    call.cancel()
            except (grpc.RpcError, OSError, TimeoutError):
                pass
            self.lease = 0
            self.published.clear()
        grant = await deps.directory._rpc("Lease", "LeaseGrant", TTL=15)
        if not grant.ID:
            raise RuntimeError("etcd did not grant a health lease")
        self.lease = grant.ID

    async def publish(self):
        deps = self.deps
        await self._lease()
        for zone in deps.settings.home_zone_ids:
            key = f"/mmorpg/zone_health/zone/{zone}/publisher/{self.instance}".encode()
            try:
                async with asyncio.timeout(4):
                    gates, scenes = await asyncio.gather(deps.directory.candidates(4, zone),
                                                         deps.directory.candidates(3, zone))
                    async with deps.db.acquire() as conn:
                        async with conn.cursor() as cursor:
                            await cursor.execute("SELECT capacity FROM zone_config WHERE zone_id=%s", (zone,))
                            row = await cursor.fetchone()
                        await conn.rollback()
                    record = summarize(zone, int(row[0]) if row else 0, gates, scenes)
                    now = time.monotonic()
                    previous = self.published.get(zone)
                    if previous is None or should_publish(previous[0], record, now - previous[1]):
                        data = json.dumps(record, separators=(",", ":")).encode()
                        await deps.directory._rpc("KV", "Put", key=key, value=data, lease=self.lease)
                        self.published[zone] = (record, now, now)
                    else:
                        self.published[zone] = (previous[0], previous[1], now)
                    self.current_keys.add(key)
            except Exception:
                _LOG.warning("could not publish zone health for %s", zone, exc_info=True)
                previous = self.published.get(zone)
                if previous and time.monotonic() - previous[2] > 15:
                    try:
                        await deps.directory._rpc("KV", "DeleteRange", key=key)
                        self.published.pop(zone, None)
                        self.current_keys.discard(key)
                    except Exception:
                        _LOG.warning("could not withdraw stale zone health", exc_info=True)

    async def close(self):
        for key in self.current_keys:
            try:
                await self.deps.directory._rpc("KV", "DeleteRange", key=key)
            except Exception:
                _LOG.warning("could not remove health key; lease will expire", exc_info=True)

    async def run(self):
        try:
            while True:
                try:
                    await self.publish()
                except Exception:
                    _LOG.warning("could not create zone health lease", exc_info=True)
                await asyncio.sleep(5)
        finally:
            await self.close()


def register_background(app, deps):
    async def start(_app):
        app["health_task"] = (asyncio.create_task(HealthPublisher(deps).run())
                              if deps.settings.home_zone_ids else None)

    async def stop(_app):
        task = app["health_task"]
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    app.on_startup.append(start)
    app.on_cleanup.append(stop)
