"""Bounded, signed proxy for the optional native mall administration service."""

import hashlib
import hmac
import json
import re
import time
import uuid
from importlib.resources import files
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web
from yarl import URL

from .portal_admin_auth import authorize


_COMMANDS = ("skus/list", "skus/create", "skus/update", "skus/stock", "order-view")
_STATUSES = {200, 400, 401, 404, 409, 413, 503}
_MAX_REQUEST = 16_384
_MAX_RESPONSE = 262_144
_CSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"


def _config(settings):
    base = getattr(settings, "mall_admin_base_url", "")
    secret = getattr(settings, "mall_admin_secret", "")
    subject = getattr(settings, "mall_admin_subject", "mall-admin")
    if not base:
        return None
    if not isinstance(base, str):
        raise ValueError("invalid mall admin address")
    try:
        parts = urlsplit(base)
        port = parts.port
        if (parts.scheme not in ("http", "https") or not parts.hostname or parts.username is not None
                or parts.password is not None or "?" in base or "#" in base
                or any(ord(ch) <= 32 or ord(ch) == 127 for ch in base)
                or any(value in parts.path for value in ("%", ";", "..", "//"))
                or port is not None and not 0 < port < 65536):
            raise ValueError
    except ValueError:
        raise ValueError("invalid mall admin address") from None
    if (not isinstance(secret, str) or len(secret.encode()) < 32 or not isinstance(subject, str)
            or not re.fullmatch(r"[a-z][a-z0-9-]{2,63}", subject)):
        raise ValueError("invalid mall admin signing configuration")
    return base.rstrip("/"), secret.encode(), subject


def _signature(secret, subject, timestamp, nonce, path, body):
    digest = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(("v1", "admin", subject, timestamp, nonce, "POST", path, digest)).encode()
    return hmac.new(secret, canonical, hashlib.sha256).hexdigest()


def _is_object(body):
    try:
        def invalid_constant(_):
            raise ValueError("invalid JSON constant")
        return isinstance(json.loads(body, parse_constant=invalid_constant), dict)
    except (ValueError, UnicodeDecodeError, RecursionError):
        return False


async def _body(request):
    if request.content_length is not None and request.content_length > _MAX_REQUEST:
        raise web.HTTPRequestEntityTooLarge(max_size=_MAX_REQUEST, actual_size=request.content_length)
    body = bytearray()
    async for chunk in request.content.iter_chunked(8192):
        body.extend(chunk)
        if len(body) > _MAX_REQUEST:
            raise web.HTTPRequestEntityTooLarge(max_size=_MAX_REQUEST, actual_size=len(body))
    return bytes(body)


def register_routes(app, deps):
    config = _config(deps.settings)
    session = None

    async def client_session(_app):
        nonlocal session
        timeout = aiohttp.ClientTimeout(total=8, connect=3)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as client:
            session = client
            try:
                yield
            finally:
                session = None

    if config is not None:
        app.cleanup_ctx.append(client_session)

    assets = files("xuanming").joinpath("mall_admin")
    contents = {name: assets.joinpath(name).read_bytes() for name in
                ("mall-admin.html", "mall-admin.js", "mall-admin.css")}

    def asset(name, media_type, *, page=False):
        async def handler(request):
            headers = {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"}
            if page:
                headers["Content-Security-Policy"] = _CSP
            return web.Response(body=contents[name], headers=headers, content_type=media_type)
        return handler

    async def execute(request):
        authorize(request, deps)
        key = deps.settings.admin_api_key
        if len(key.encode()) < 32:
            raise web.HTTPServiceUnavailable(text="admin token must be at least 32 bytes")
        if len(request.headers["X-Admin-Key"]) > 256:
            raise web.HTTPUnauthorized()
        command = request.path.removeprefix("/admin/mall/")
        if "?" in request.raw_path or command not in _COMMANDS:
            raise web.HTTPBadRequest(text="invalid management path")
        body = await _body(request)
        if config is None:
            raise web.HTTPServiceUnavailable(text="mall management proxy is not configured")
        if not body or not _is_object(body):
            raise web.HTTPBadRequest(text="management request must be a JSON object")
        base, secret, subject = config
        url = URL(base + "/internal/admin/" + command, encoded=True)
        timestamp, nonce = str(time.time_ns() // 1_000_000), str(uuid.uuid4())
        headers = {"Content-Type": "application/json", "X-Commerce-Subject": subject,
                   "X-Commerce-Timestamp": timestamp, "X-Commerce-Nonce": nonce,
                   "X-Commerce-Signature": _signature(secret, subject, timestamp, nonce, url.raw_path, body)}
        try:
            async with session.post(url, data=body, headers=headers, allow_redirects=False) as response:
                if response.status not in _STATUSES:
                    raise ValueError("unexpected upstream status")
                output = bytearray()
                async for chunk in response.content.iter_chunked(8192):
                    output.extend(chunk)
                    if len(output) > _MAX_RESPONSE:
                        raise ValueError("upstream response is too large")
                if not _is_object(output):
                    raise ValueError("upstream response is not a JSON object")
                return web.Response(body=bytes(output), status=response.status, content_type="application/json",
                                    headers={"Cache-Control": "no-store"})
        except (aiohttp.ClientError, TimeoutError, ValueError, OSError):
            raise web.HTTPBadGateway(text="mall management response unavailable; check using the same request id") from None

    app.router.add_get("/mall-admin", asset("mall-admin.html", "text/html", page=True))
    app.router.add_get("/mall-admin.js", asset("mall-admin.js", "text/javascript"))
    app.router.add_get("/mall-admin.css", asset("mall-admin.css", "text/css"))
    for command in _COMMANDS:
        app.router.add_post("/admin/mall/" + command, execute, name="mall-" + command.replace("/", "-"))
