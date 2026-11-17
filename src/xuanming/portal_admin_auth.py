"""Fail-closed authentication for each portal management handler."""

import hashlib
import hmac

from aiohttp import web


def authorize(request, deps):
    configured = getattr(deps.settings, "admin_api_key", "")
    supplied = request.headers.getall("X-Admin-Key", [])
    if not isinstance(configured, str) or not configured.strip() or len(supplied) != 1:
        raise web.HTTPUnauthorized(text='{"error":"Unauthorized"}', content_type="application/json")
    expected = hashlib.sha256(configured.encode()).digest()
    actual = hashlib.sha256(supplied[0].encode()).digest()
    if not hmac.compare_digest(expected, actual):
        raise web.HTTPUnauthorized(text='{"error":"Unauthorized"}', content_type="application/json")
