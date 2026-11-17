"""Honest portal health, bounded metrics and local W3C trace propagation."""

import asyncio
import contextvars
import importlib.metadata
import logging
import secrets
import time
from collections import Counter, defaultdict

from aiohttp import web

_LOG = logging.getLogger(__name__)
_trace_id = contextvars.ContextVar("portal_trace_id", default=None)


def trace_metadata():
    """Make a fresh client span under this HTTP request's own root trace."""
    trace = _trace_id.get()
    if not trace:
        return ()
    return (("traceparent", f"00-{trace}-{secrets.token_hex(8)}-01"),)


class Metrics:
    def __init__(self):
        self.http_count = Counter()
        self.http_seconds = defaultdict(float)
        self.rate_decisions = Counter()
        self.rate_fail_open = Counter()
        self.ready = False

    @web.middleware
    async def instrument(self, request, handler):
        # Public callers cannot choose our trace or join another request's
        # trace: W3C, B3 and baggage input are intentionally never read.
        trace = secrets.token_hex(16)
        token = _trace_id.set(trace)
        started = time.monotonic()
        status = 500
        try:
            response = await handler(request)
            status = response.status
            return response
        except web.HTTPException as exc:
            status = exc.status
            raise
        finally:
            elapsed = max(0.0, time.monotonic() - started)
            resource = request.match_info.route.resource
            route = resource.canonical if resource else "UNKNOWN"
            method = request.method if request.method in {"GET", "POST", "PUT", "DELETE", "HEAD"} else "OTHER"
            key = method, route, status
            self.http_count[key] += 1
            self.http_seconds[key] += elapsed
            _LOG.info("portal request method=%s route=%s status=%d trace_id=%s duration_ms=%.1f",
                      method, route, status, trace, elapsed * 1000)
            _trace_id.reset(token)

    def render(self, db):
        lines = [
            "# HELP portal_http_requests_total HTTP requests handled by the portal.",
            "# TYPE portal_http_requests_total counter",
        ]
        for (method, route, status), value in sorted(self.http_count.items()):
            labels = f'method="{method}",route="{_label(route)}",status="{status}"'
            lines.append(f"portal_http_requests_total{{{labels}}} {value}")
        lines.extend(("# HELP portal_http_request_seconds_total Time spent handling portal requests.",
                      "# TYPE portal_http_request_seconds_total counter"))
        for (method, route, status), value in sorted(self.http_seconds.items()):
            labels = f'method="{method}",route="{_label(route)}",status="{status}"'
            lines.append(f"portal_http_request_seconds_total{{{labels}}} {value:.9f}")
        lines.extend(("# HELP gateway_rate_limit_decisions_total Portal rate limit decisions.",
                      "# TYPE gateway_rate_limit_decisions_total counter"))
        for outcome, value in sorted(self.rate_decisions.items()):
            lines.append(f'gateway_rate_limit_decisions_total{{outcome="{outcome}"}} {value}')
        lines.extend(("# HELP gateway_rate_limit_fail_open_total Portal Redis limiter bypass decisions.",
                      "# TYPE gateway_rate_limit_fail_open_total counter"))
        for reason, value in sorted(self.rate_fail_open.items()):
            lines.append(f'gateway_rate_limit_fail_open_total{{reason="{reason}"}} {value}')
        # aiomysql exposes actual pool counts. Do not present fabricated
        # Hikari statistics under the old JVM metric names.
        for name, value in (("size", db.size), ("free", db.freesize), ("max", db.maxsize)):
            lines.append(f"portal_mysql_pool_connections{{state=\"{name}\"}} {value}")
        return "\n".join(lines) + "\n"


def _label(value):
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


async def _mysql_health(db):
    async with asyncio.timeout(2):
        async with db.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
                row = await cursor.fetchone()
            await connection.rollback()
            if not row or row[0] != 1:
                raise RuntimeError("unexpected MySQL health response")


async def _redis_health(redis):
    async with asyncio.timeout(2):
        if not await redis.ping():
            raise RuntimeError("unexpected Redis health response")


def create_management_app(deps):
    app = web.Application()

    async def live(_request):
        return web.json_response({"status": "UP"})

    async def ready(_request):
        return web.json_response({"status": "UP" if deps.metrics.ready else "DOWN"},
                                 status=200 if deps.metrics.ready else 503)

    async def health(_request):
        results = await asyncio.gather(_mysql_health(deps.db), _redis_health(deps.redis), return_exceptions=True)
        ok = all(not isinstance(value, BaseException) for value in results)
        return web.json_response({"status": "UP" if ok else "DOWN"}, status=200 if ok else 503)

    async def info(_request):
        try:
            version = importlib.metadata.version("xuanming-server-mmo-python")
        except importlib.metadata.PackageNotFoundError:
            return web.json_response({})
        return web.json_response({"build": {"version": version}})

    async def prometheus(_request):
        return web.Response(text=deps.metrics.render(deps.db), content_type="text/plain")

    app.router.add_get("/actuator/health/liveness", live)
    app.router.add_get("/actuator/health/readiness", ready)
    app.router.add_get("/actuator/health", health)
    app.router.add_get("/actuator/info", info)
    app.router.add_get("/actuator/prometheus", prometheus)
    return app
