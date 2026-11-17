"""Client-facing zone list using the SQL directory and live node discovery."""

import asyncio
from datetime import datetime, timedelta, timezone

import grpc
from aiohttp import web

from .portal_db import as_bool


def _zone_view(row, nodes, now):
    zone_id, name, status, capacity, message, open_time, recommended, _, closed, created = row
    zone_id = int(zone_id)
    manual = ("OPEN", "MAINTENANCE", "CLOSED", "PREVIEW")
    display = manual[status] if 0 <= status < len(manual) else "OPEN"
    gates = [node for node in nodes.get(4, ()) if node.zone_id == zone_id] if nodes is not None else []
    if nodes is not None and display == "OPEN" and not gates:
        display = "MAINTENANCE"
    result = {"zone_id": zone_id, "name": name, "status": display,
              "is_new": bool(created and created > now - timedelta(days=7)),
              "recommended": as_bool(recommended), "create_role_closed": as_bool(closed)}
    ratio = None
    if display == "OPEN" and gates:
        online = sum(node.player_count for node in gates)
        limits = [node.max_connections for node in gates]
        available = sum((limit * 90 + 99) // 100 for limit in limits) if all(limits) else 0
        effective = min(capacity, available) if capacity > 0 and available else max(capacity, available)
        ratio = online / effective if effective > 0 else 0.0
        result["load_level"] = "SMOOTH" if ratio < .5 else "BUSY" if ratio < .8 else "FULL"
    if display in ("MAINTENANCE", "CLOSED") and message is not None:
        result["maintenance_msg"] = message
    if status == 3 and open_time is not None:
        result["open_time"] = int(open_time.replace(tzinfo=timezone.utc).timestamp())
    return result, ratio


def make_list(rows, nodes, now):
    results = [_zone_view(row, nodes, now) for row in sorted(rows, key=lambda item: item[7])]
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

    async def get_rows():
        nonlocal cached_at, cached_rows
        clock = asyncio.get_running_loop().time
        if cached_rows is not None and clock() - cached_at < 1:
            return cached_rows
        async with asyncio.timeout(3):
            async with lock:
                if cached_rows is not None and clock() - cached_at < 1:
                    return cached_rows
                async with deps.db.acquire() as connection:
                    async with connection.cursor() as cursor:
                        await cursor.execute("SELECT zone_id,name,manual_status,capacity,maintenance_msg,open_time,"
                                             "recommended,sort_order,create_role_closed,created_at FROM zone_config")
                        rows = await cursor.fetchall()
                    await connection.rollback()
                cached_rows, cached_at = tuple(rows), clock()
                return cached_rows

    async def server_list(request):
        rows = await get_rows()
        nodes = None
        if deps.directory is not None:
            try:
                gates, scenes = await asyncio.gather(deps.directory.candidates(4, None),
                                                     deps.directory.candidates(3, None))
                nodes = {4: gates, 3: scenes}
            except (grpc.RpcError, OSError, TimeoutError):
                pass
        return web.json_response(make_list(rows, nodes, datetime.now(timezone.utc).replace(tzinfo=None)))

    app.router.add_get("/api/server-list", server_list)
