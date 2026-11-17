"""Publish home-zone login policy and enforce the soft role-count cap."""

import asyncio
import json
import logging

import grpc

from .portal_db import as_bool
from .portal_sql import connection

_LOG = logging.getLogger(__name__)


async def _row(deps, zone, columns):
    async with asyncio.timeout(3):
        async with connection(deps.db) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(f"SELECT {columns} FROM zone_config WHERE zone_id=%s", (zone,))
                row = await cursor.fetchone()
            await conn.rollback()
            return row


async def publish_zone(deps, zone):
    row = await _row(deps, zone, "capacity,create_role_closed")
    if row is None:
        return "zone_missing"
    desired = {"capacity": max(0, int(row[0])), "createRoleClosed": as_bool(row[1])}
    key = f"/mmorpg/zone_policy/zone/{zone}".encode()
    current = await deps.directory._rpc("KV", "Range", key=key)
    if current.kvs:
        try:
            if json.loads(current.kvs[0].value) == desired:
                return "unchanged"
        except (UnicodeDecodeError, ValueError):
            pass
    encoded = json.dumps(desired, separators=(",", ":")).encode()
    await deps.directory._rpc("KV", "Put", key=key, value=encoded)
    return "written"


async def check_capacity(deps, zone):
    row = await _row(deps, zone, "role_capacity,create_role_closed")
    if row is None:
        return "zone_missing"
    capacity, closed = int(row[0]), as_bool(row[1])
    if capacity <= 0:
        return "no_cap"
    if closed:
        return "already_closed"
    req = deps.protocol.message("loginpb.ZoneRoleCountRequest", zone_id=zone)
    try:
        response = await deps.rpc.call(zone, "LoginPreGate", "GetZoneRoleCount", req, "ZoneRoleCountResponse")
    except grpc.aio.AioRpcError as exc:
        if exc.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            raise
        # Role count is read-only, and native gateway retries transport failures once.
        response = await deps.rpc.call(zone, "LoginPreGate", "GetZoneRoleCount", req, "ZoneRoleCountResponse")
    if response.roles < capacity:
        return "below"
    async with asyncio.timeout(3):
        async with connection(deps.db) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("UPDATE zone_config SET create_role_closed=TRUE,version=version+1,"
                                     "updated_at=CURRENT_TIMESTAMP WHERE zone_id=%s AND role_capacity=%s "
                                     "AND create_role_closed=FALSE", (zone, capacity))
                count = cursor.rowcount
            await conn.commit()
    return "closed" if count else "raced"


async def _loop(deps, operation, interval):
    while True:
        for zone in deps.settings.home_zone_ids:
            try:
                await operation(deps, zone)
            except Exception:
                _LOG.warning("portal zone background operation %s failed for zone %s", operation.__name__, zone,
                             exc_info=True)
        await asyncio.sleep(interval)


def register_background(app, deps):
    async def start(_app):
        app["zone_background_tasks"] = [
            asyncio.create_task(_loop(deps, publish_zone, deps.settings.zone_policy_interval_seconds)),
            asyncio.create_task(_loop(deps, check_capacity, deps.settings.role_capacity_interval_seconds)),
        ] if deps.settings.home_zone_ids else []

    async def stop(_app):
        tasks = app["zone_background_tasks"]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    app.on_startup.append(start)
    app.on_cleanup.append(stop)
