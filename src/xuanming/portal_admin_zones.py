"""Administrative zone and whitelist endpoints backed by the gateway SQL tables."""

import asyncio
import datetime as dt
import re
import secrets
import time

from aiohttp import web

from .portal_admin_auth import authorize


_COLUMNS = ("zone_id", "name", "manual_status", "capacity", "maintenance_msg", "open_time", "recommended",
            "sort_order", "create_role_closed", "role_capacity", "version", "created_at", "updated_at")
_SELECT = "SELECT " + ",".join(_COLUMNS) + " FROM zone_config"
_last_row_id = 0


def _row_id():
    global _last_row_id
    _last_row_id = max(_last_row_id + 1, (time.time_ns() // 1_000_000) * 1024 + secrets.randbelow(1024))
    return _last_row_id


def _problem(request, status, reason, detail, field=None):
    body = {"type": "about:blank", "title": {400: "Bad Request", 412: "Precondition Failed",
            428: "Precondition Required"}[status], "status": status, "detail": detail,
            "instance": request.path, "reason": reason}
    if field is not None:
        body["field"] = field
    return web.json_response(body, status=status, content_type="application/problem+json")


class _BadField(Exception):
    def __init__(self, field, reason, detail):
        self.field, self.reason, self.detail = field, reason, detail


def _bad(field, detail, reason="invalid"):
    raise _BadField(field, reason, detail)


async def _body(request, *, optional=False):
    if not request.can_read_body:
        if optional:
            return {}
        _bad(None, "request body is missing", "missing")
    try:
        value = await request.json()
    except (ValueError, UnicodeDecodeError):
        _bad(None, "request body must be valid JSON")
    if value is None and optional:
        return {}
    if not isinstance(value, dict):
        _bad(None, "request body must be a JSON object")
    return value


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and -(2**63) <= value < 2**63


def _settings(body, path_id=None):
    zone_id = body.get("zone_id")
    if path_id is None:
        if zone_id is None:
            _bad("zone_id", "zone_id is required", "missing")
        if not _integer(zone_id) or zone_id <= 0:
            _bad("zone_id", "zone_id must be a positive number")
    elif zone_id is not None:
        if not _integer(zone_id):
            _bad("zone_id", "zone_id must be an integer")
        if zone_id != path_id:
            _bad("zone_id", "zone_id in body differs from path", "mismatch")
    name = body.get("name")
    if name is None:
        _bad("name", "name is required", "missing")
    if not isinstance(name, str) or not name.strip():
        _bad("name", "name must not be blank")
    if len(name) > 64:
        _bad("name", "name exceeds 64 characters", "too_long")
    status = body.get("manual_status")
    if status is None:
        _bad("manual_status", "manual_status is required", "missing")
    if not _integer(status) or status not in range(4):
        _bad("manual_status", "manual_status must be a defined status code")
    capacity = body.get("capacity")
    if capacity is None:
        _bad("capacity", "capacity is required", "missing")
    if not _integer(capacity) or not 0 < capacity <= 2**31 - 1:
        _bad("capacity", "capacity must be positive")
    message = body.get("maintenance_msg")
    if message is None:
        _bad("maintenance_msg", 'maintenance_msg is required (use "" for none)', "missing")
    if not isinstance(message, str):
        _bad("maintenance_msg", "maintenance_msg must be a string")
    if len(message) > 256:
        _bad("maintenance_msg", "maintenance_msg exceeds 256 characters", "too_long")
    if "open_time" not in body:
        _bad("open_time", "open_time is required (use null for none)", "missing")
    open_time = body["open_time"]
    if open_time is not None:
        try:
            open_time = dt.datetime.fromisoformat(open_time)
            if open_time.tzinfo is not None or open_time.microsecond:
                raise ValueError
        except (TypeError, ValueError):
            _bad("open_time", "open_time must be a local date-time in whole seconds")
    recommended = body.get("recommended")
    if recommended is None:
        _bad("recommended", "recommended is required", "missing")
    if not isinstance(recommended, bool):
        _bad("recommended", "recommended must be a boolean")
    order = body.get("sort_order")
    if order is None:
        _bad("sort_order", "sort_order is required", "missing")
    if not _integer(order) or not -(2**31) <= order < 2**31:
        _bad("sort_order", "sort_order must be an integer")
    return (path_id if path_id is not None else zone_id, name, status, capacity, message,
            open_time, recommended, order)


def _zone(row):
    obj = dict(zip(_COLUMNS, row))
    obj.pop("version")
    for field in ("recommended", "create_role_closed"):
        value = obj[field]
        obj[field] = bool(int.from_bytes(value, "big")) if isinstance(value, bytes) else bool(value)
    for field in ("open_time", "created_at", "updated_at"):
        if isinstance(obj[field], dt.datetime):
            obj[field] = obj[field].isoformat()
    return obj


async def _retry_write(db, fn, retry=False):
    for attempt in range(3 if retry else 1):
        try:
            async with db.acquire() as conn:
                await conn.begin()
                try:
                    async with conn.cursor() as cur:
                        value = await fn(cur)
                    await conn.commit()
                    return value
                except BaseException:
                    await conn.rollback()
                    raise
        except Exception as exc:
            if not retry or attempt == 2 or not exc.args or exc.args[0] != 1213:
                raise
            await asyncio.sleep((0.01 * 2**attempt) * (0.8 + secrets.randbelow(401) / 1000))


async def _query(db, sql, args=(), *, one=False):
    async with db.acquire() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql, args)
                return await (cur.fetchone() if one else cur.fetchall())
        finally:
            await conn.rollback()


def _path_id(request, field="zone_id"):
    text = request.match_info[field]
    if not re.fullmatch(r"-?[0-9]+", text) or not -(2**63) <= int(text) < 2**63:
        _bad(field, f"{field} must be an integer")
    return int(text)


def _version(request):
    raw = ",".join(request.headers.getall("If-Match", [])).strip()
    if not raw or "*" in (part.strip() for part in raw.split(",")):
        return _problem(request, 428, "if_match_required", "If-Match must name the ETag returned by GET", "If-Match")
    if not re.fullmatch(r'(?:W/)?"[\x21\x23-\x7e\x80-\xff]*"', raw):
        return _problem(request, 400, "if_match_invalid", "If-Match must carry exactly one quoted entity tag", "If-Match")
    if raw.startswith("W/") or not re.fullmatch(r'"[1-9][0-9]{0,18}"', raw):
        return None
    value = int(raw[1:-1])
    return value if value < 2**63 else None


def register_routes(app, deps):
    def checked(fn):
        async def handler(request):
            authorize(request, deps)
            try:
                return await fn(request)
            except _BadField as err:
                return _problem(request, 400, err.reason, err.detail, err.field)
        return handler

    async def list_zones(request):
        return web.json_response([_zone(row) for row in await _query(deps.db, _SELECT)])

    async def get_zone(request):
        zone_id = _path_id(request)
        row = await _query(deps.db, _SELECT + " WHERE zone_id=%s", (zone_id,), one=True)
        if row is None:
            raise web.HTTPNotFound()
        etag = f'"{row[10]}"'
        tags = [tag.strip() for tag in request.headers.get("If-None-Match", "").split(",")]
        if "*" in tags or etag in tags or "W/" + etag in tags:
            return web.Response(status=304, headers={"ETag": etag})
        return web.json_response(_zone(row), headers={"ETag": etag})

    async def create_zone(request):
        values = _settings(await _body(request))
        now = dt.datetime.now()
        version = 1 + secrets.randbelow(2**53 - 1)
        async def create(cur):
            await cur.execute("INSERT INTO zone_config (zone_id,name,manual_status,capacity,maintenance_msg,open_time,"
                              "recommended,sort_order,version,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                              "ON DUPLICATE KEY UPDATE zone_id=zone_id", (*values, version, now, now))
            await cur.execute(_SELECT + " WHERE zone_id=%s FOR UPDATE", (values[0],))
            row = await cur.fetchone()
            if row is None:
                raise RuntimeError("zone row invisible after insert")
            return row
        row = await _retry_write(deps.db, create, retry=True)
        same = (_zone(row)["name"], row[2], row[3], row[4], row[5], _zone(row)["recommended"], row[7]) == values[1:]
        return web.json_response(_zone(row), status=200 if same else 409)

    async def update_zone(request):
        zone_id = _path_id(request)
        values = _settings(await _body(request), zone_id)
        expected = _version(request)
        if isinstance(expected, web.StreamResponse):
            return expected
        async def update(cur):
            await cur.execute("SELECT zone_id FROM zone_config WHERE zone_id=%s FOR UPDATE", (zone_id,))
            if await cur.fetchone() is None:
                return 404, None
            if expected is None:
                return 412, None
            await cur.execute("UPDATE zone_config SET name=%s,manual_status=%s,capacity=%s,maintenance_msg=%s,"
                              "open_time=%s,recommended=%s,sort_order=%s,version=version+1,updated_at=%s "
                              "WHERE zone_id=%s AND version=%s", (*values[1:], dt.datetime.now(), zone_id, expected))
            if not cur.rowcount:
                return 412, None
            await cur.execute(_SELECT + " WHERE zone_id=%s", (zone_id,))
            return 200, await cur.fetchone()
        status, row = await _retry_write(deps.db, update)
        if status == 404:
            raise web.HTTPNotFound()
        if status == 412:
            return _problem(request, 412, "version_mismatch", "If-Match is not the current version of this zone", "If-Match")
        return web.json_response(_zone(row))

    async def delete_zone(request):
        zone_id = _path_id(request)
        async def remove(cur):
            await cur.execute("DELETE FROM zone_config WHERE zone_id=%s", (zone_id,))
            return cur.rowcount
        if not await _retry_write(deps.db, remove):
            raise web.HTTPNotFound()
        return web.Response(status=204)

    async def update_command(request):
        zone_id = _path_id(request)
        action = request.match_info["action"]
        if action == "maintenance":
            body = await _body(request, optional=True)
            message = body.get("maintenance_msg")
            if message is not None and not isinstance(message, str):
                _bad("maintenance_msg", "maintenance_msg must be a string")
            sql, args = "manual_status=1,maintenance_msg=COALESCE(%s,maintenance_msg)", (message,)
        elif action == "open":
            sql, args = "manual_status=0,maintenance_msg=%s", ("",)
        elif action == "create-role/close":
            sql, args = "create_role_closed=%s", (True,)
        elif action == "create-role/open":
            sql, args = "create_role_closed=%s", (False,)
        else:
            body = await _body(request)
            value = body.get("role_capacity")
            if value is None:
                _bad("role_capacity", "role_capacity is required (0 = no cap)", "missing")
            if not _integer(value) or not 0 <= value < 2**31:
                _bad("role_capacity", "role_capacity must be >= 0")
            sql, args = "role_capacity=%s", (value,)
        async def update(cur):
            await cur.execute("SELECT zone_id FROM zone_config WHERE zone_id=%s FOR UPDATE", (zone_id,))
            if await cur.fetchone() is None:
                return None
            await cur.execute("UPDATE zone_config SET " + sql + ",version=version+1,updated_at=%s WHERE zone_id=%s",
                              (*args, dt.datetime.now(), zone_id))
            await cur.execute(_SELECT + " WHERE zone_id=%s", (zone_id,))
            return await cur.fetchone()
        row = await _retry_write(deps.db, update)
        if row is None:
            raise web.HTTPNotFound()
        return web.json_response(_zone(row))

    async def list_whitelist(request):
        rows = await _query(deps.db, "SELECT id,zone_id,account_id,note FROM zone_whitelist WHERE zone_id=%s",
                            (_path_id(request),))
        return web.json_response([dict(zip(("id", "zone_id", "account_id", "note"), row)) for row in rows])

    async def add_whitelist(request):
        body = await _body(request)
        for field in ("zone_id", "account_id"):
            if body.get(field) is None:
                _bad(field, f"{field} is required", "missing")
            if not _integer(body[field]):
                _bad(field, f"{field} must be an integer")
        zone_id, account_id, note = body["zone_id"], body["account_id"], body.get("note", "")
        if note is not None and (not isinstance(note, str) or len(note) > 128):
            _bad("note", "note must be at most 128 characters")
        async def add(cur):
            await cur.execute("INSERT INTO zone_whitelist (id,zone_id,account_id,note) VALUES (%s,%s,%s,%s) "
                              "ON DUPLICATE KEY UPDATE note=VALUES(note)", (_row_id(), zone_id, account_id, note))
            await cur.execute("SELECT id,zone_id,account_id,note FROM zone_whitelist "
                              "WHERE zone_id=%s AND account_id=%s FOR UPDATE", (zone_id, account_id))
            row = await cur.fetchone()
            if row is None or row[3] != note:
                raise RuntimeError("whitelist row collision or visibility violation")
            return row
        row = await _retry_write(deps.db, add, retry=True)
        return web.json_response(dict(zip(("id", "zone_id", "account_id", "note"), row)))

    async def delete_whitelist(request):
        zone_id, account_id = _path_id(request), _path_id(request, "account_id")
        async def remove(cur):
            await cur.execute("DELETE FROM zone_whitelist WHERE zone_id=%s AND account_id=%s", (zone_id, account_id))
        await _retry_write(deps.db, remove)
        return web.Response(status=204)

    app.router.add_get("/admin/zones", checked(list_zones))
    app.router.add_post("/admin/zones", checked(create_zone))
    app.router.add_get("/admin/zones/{zone_id}", checked(get_zone))
    app.router.add_put("/admin/zones/{zone_id}", checked(update_zone))
    app.router.add_delete("/admin/zones/{zone_id}", checked(delete_zone))
    app.router.add_post(r"/admin/zones/{zone_id}/{action:maintenance|open|create-role/close|create-role/open}", checked(update_command))
    app.router.add_put(r"/admin/zones/{zone_id}/{action:role-capacity}", checked(update_command))
    app.router.add_get("/admin/whitelist/{zone_id}", checked(list_whitelist))
    app.router.add_post("/admin/whitelist", checked(add_whitelist))
    app.router.add_delete("/admin/whitelist/{zone_id}/{account_id}", checked(delete_whitelist))
