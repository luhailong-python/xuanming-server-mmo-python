"""Public portal endpoints for signed CDN URLs and client update checks."""

import hashlib
import hmac
import re
import time
from datetime import timezone

from aiohttp import web

from .portal_sql import connection


_VERSION = re.compile(r"[0-9]{1,9}(?:\.[0-9]{1,9}){0,3}\Z")


def _version(value):
    if not isinstance(value, str) or not _VERSION.fullmatch(value):
        raise ValueError("version must contain at most four numeric segments")
    return tuple(int(part) for part in value.split(".")) + (0,) * (4 - value.count(".") - 1)


def validate_releases(platforms):
    if not isinstance(platforms, dict):
        raise ValueError("hotfix platforms must be a mapping")
    for platform, release in platforms.items():
        if not isinstance(platform, str) or not platform or not isinstance(release, dict):
            raise ValueError("invalid hotfix platform")
        latest = _version(release.get("latest_version"))
        minimum = release.get("min_version")
        if minimum is not None and _version(minimum) > latest:
            raise ValueError("minimum version exceeds latest version")
        if not isinstance(release.get("patch_url"), str) or not release["patch_url"].strip():
            raise ValueError("hotfix patch_url is required")


def check_hotfix(body, platforms):
    if not isinstance(body, dict) or not isinstance(body.get("platform"), str) or not body["platform"].strip():
        raise ValueError("platform and a numeric client_version are required")
    version = _version(body.get("client_version"))
    release = platforms.get(body["platform"])
    result = {"need_update": False, "force_update": False}
    if release is None:
        return result
    result["latest_version"] = release["latest_version"]
    if version < _version(release["latest_version"]):
        result["need_update"] = True
        result["force_update"] = (release.get("min_version") is not None
                                  and version < _version(release["min_version"]))
        result["patch_url"] = release["patch_url"]
        if release.get("changelog") is not None:
            result["changelog"] = release["changelog"]
    return result


def register_routes(app, deps):
    settings = deps.settings
    validate_releases(settings.hotfix_platforms)
    if settings.cdn_sign_ttl_seconds <= 0 or not settings.cdn_signing_secret:
        raise ValueError("CDN signing secret and positive TTL are required")

    async def cdn_sign(request):
        try:
            body = await request.json()
            path = body.get("resource_path") if isinstance(body, dict) else None
            if not isinstance(path, str):
                raise ValueError("resource_path is required")
            expires = int(time.time()) + settings.cdn_sign_ttl_seconds
            signature = hmac.new(settings.cdn_signing_secret.encode(), f"{path}|{expires}".encode(),
                                 hashlib.sha256).hexdigest()
            return web.json_response({"signed_url": f"{settings.cdn_base_url}{path}?expire={expires}&sign={signature}",
                                      "expire_at": expires})
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="invalid request")

    async def hotfix_check(request):
        try:
            return web.json_response(check_hotfix(await request.json(), settings.hotfix_platforms))
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="invalid request")

    async def announcements(request):
        async with connection(deps.db) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT id,title,content,type,start_time,end_time FROM announcement "
                    "WHERE (start_time IS NULL OR start_time <= UTC_TIMESTAMP()) "
                    "AND (end_time IS NULL OR end_time >= UTC_TIMESTAMP()) ORDER BY created_at DESC"
                )
                rows = await cursor.fetchall()
            await conn.rollback()
        items = []
        for row in rows:
            item = dict(zip(("id", "title", "content", "type"), row[:4]))
            for key, value in zip(("start_time", "end_time"), row[4:]):
                if value is not None:
                    item[key] = int(value.replace(tzinfo=timezone.utc).timestamp())
            items.append(item)
        return web.json_response({"items": items})

    app.router.add_post("/api/cdn-sign", cdn_sign)
    app.router.add_post("/api/hotfix-check", hotfix_check)
    app.router.add_get("/api/announcement", announcements)
