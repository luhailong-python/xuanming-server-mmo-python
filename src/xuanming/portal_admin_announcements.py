"""Announcement administration using the existing portal SQL schema."""

import secrets
from datetime import datetime, timezone

from aiohttp import web

from .portal_admin_auth import authorize
from .portal_sql import connection


def _next_id():
    return secrets.randbits(62) | 1


def _date(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("invalid date")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return result.astimezone(timezone.utc).replace(tzinfo=None) if result.tzinfo else result


def _view(row):
    result = dict(zip(("id", "title", "content", "type", "start_time", "end_time", "created_at"), row))
    for field in ("start_time", "end_time", "created_at"):
        if result[field] is not None:
            result[field] = result[field].isoformat()
    return result


def register_routes(app, deps):
    async def list_all(request):
        authorize(request, deps)
        async with connection(deps.db) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT id,title,content,type,start_time,end_time,created_at FROM announcement")
                rows = await cursor.fetchall()
            await conn.rollback()
        return web.json_response([_view(row) for row in rows])

    async def create(request):
        authorize(request, deps)
        try:
            body = await request.json()
            title, content = body["title"], body.get("content")
            kind = body.get("type", "notice")
            if (not isinstance(title, str) or not title or len(title) > 128
                    or content is not None and not isinstance(content, str)
                    or not isinstance(kind, str) or not kind or len(kind) > 32):
                raise ValueError("invalid announcement")
            start, end = _date(body.get("start_time")), _date(body.get("end_time"))
        except (ValueError, TypeError, KeyError):
            raise web.HTTPBadRequest(text="invalid announcement")
        identifier = _next_id()
        created = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        async with connection(deps.db) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("INSERT INTO announcement "
                                     "(id,title,content,type,start_time,end_time,created_at) "
                                     "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                     (identifier, title, content, kind, start, end, created))
            await conn.commit()
        return web.json_response(_view((identifier, title, content, kind, start, end, created)))

    async def delete(request):
        authorize(request, deps)
        try:
            identifier = int(request.match_info["id"])
        except ValueError:
            raise web.HTTPBadRequest(text="invalid id")
        async with connection(deps.db) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("DELETE FROM announcement WHERE id = %s", (identifier,))
            await conn.commit()
        return web.Response(status=204)

    app.router.add_get("/admin/announcements", list_all)
    app.router.add_post("/admin/announcements", create)
    app.router.add_delete("/admin/announcements/{id}", delete)
