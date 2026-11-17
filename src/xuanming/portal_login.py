"""HTTP login and gate admission adapter for the native Login service."""

import asyncio
import base64
import hashlib
import ipaddress
import logging
import time

import grpc
from aiohttp import web

from .grpc_runtime import GrpcClient

_LOG = logging.getLogger(__name__)
_FAULTS = frozenset((1002, 1003, 1004, 1007, 1009, 1011, 1012, 1016, 1018, 2015, 2018, 2019,
    2020, 2021, 2022, 2023, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2035, 2036,
    3003, 3010, 3012, 3016, 3018, 3019, 3023, 3028, 4030, 5003, 6002, 9000, 14008,
    14041, 16004, 26016, 27004, 27007, 27008, 28010, 29014, 30011))

# One-key scripts work with Redis Cluster. IP is refunded when the zone bucket is empty.
_TAKE = """
local tm=redis.call('TIME')
local now=tonumber(tm[1])*1000+math.floor(tonumber(tm[2])/1000)
local rps,burst=tonumber(ARGV[1]),tonumber(ARGV[2])*1000
local s=redis.call('HMGET',KEYS[1],'t','ms')
local t,last=tonumber(s[1]),tonumber(s[2])
local level=burst
if t and last then level=math.min(burst,t+math.max(0,now-last)*rps) end
if level<1000 then return {1,math.ceil((1000-level)/rps)} end
local left=level-1000
redis.call('HSET',KEYS[1],'t',left,'ms',now)
redis.call('PEXPIRE',KEYS[1],math.ceil((burst-left)/rps))
return {0,0}
"""
_REFUND = """
local tm=redis.call('TIME')
local now=tonumber(tm[1])*1000+math.floor(tonumber(tm[2])/1000)
local rps,burst=tonumber(ARGV[1]),tonumber(ARGV[2])*1000
local s=redis.call('HMGET',KEYS[1],'t','ms')
local t,last=tonumber(s[1]),tonumber(s[2])
if not t or not last then return 0 end
local level=math.min(burst,t+math.max(0,now-last)*rps)+1000
if level>=burst then redis.call('DEL',KEYS[1]); return 1 end
redis.call('HSET',KEYS[1],'t',level,'ms',now)
redis.call('PEXPIRE',KEYS[1],math.ceil((burst-level)/rps))
return 1
"""


def client_ip(request, networks):
    peer = request.remote or "unknown"
    def trusted(value):
        try:
            address = ipaddress.ip_address(value)
            return any(address in network for network in networks)
        except ValueError:
            return False
    if trusted(peer):
        for hop in reversed(request.headers.get("X-Forwarded-For", "").split(",")):
            hop = hop.strip()
            if hop and not trusted(hop):
                # Reject malformed address chains instead of letting attacker create arbitrary bucket keys.
                try:
                    return str(ipaddress.ip_address(hop))
                except ValueError:
                    return peer
    return peer


class Limiter:
    def __init__(self, redis, settings):
        self.redis, self.settings = redis, settings
        self.networks = tuple(ipaddress.ip_network(item, strict=False) for item in settings.trusted_proxies)
        self.cooldown = {}
        self.bypass_until = 0.0
        self.bypass_generation = 0
        self.wave_start = getattr(settings, "wave_start_epoch_seconds", 0) or int(time.time())
        self.wave_steps = sorted(getattr(settings, "wave_steps", []), key=lambda step: step["offset_seconds"])

    def wave_wait(self, zone):
        if not getattr(self.settings, "wave_enabled", False) or not self.wave_steps:
            return None
        now = int(time.time())
        current = None
        for step in self.wave_steps:
            if self.wave_start + step["offset_seconds"] > now:
                break
            current = step
        if current and (-1 in current["allow_zones"] or zone in current["allow_zones"]):
            return None
        for step in self.wave_steps:
            at = self.wave_start + step["offset_seconds"]
            if at > now and (-1 in step["allow_zones"] or zone in step["allow_zones"]):
                return (at - now) * 1000
        return 3_600_000

    async def check(self, zone, ip, account, scope):
        s = self.settings
        if not s.rate_limit_enabled:
            return None
        wait = self.wave_wait(zone)
        if wait is not None:
            return ("wave", wait)
        now = time.monotonic()
        probe_generation = None
        if self.bypass_until and now >= self.bypass_until:
            # Reserve the half-open probe before awaiting I/O. The event loop
            # runs this section without yielding, so concurrent requests see
            # the new hold and bypass instead of all probing at once.
            self.bypass_generation += 1
            probe_generation = self.bypass_generation
            self.bypass_until = now + 5
        if not self.bypass_until or probe_generation is not None:
            try:
                result = await self._admit(zone, ip)
            except Exception:
                _LOG.warning("portal rate limiter unavailable, temporarily bypassing Redis buckets", exc_info=True)
                self.bypass_generation += 1
                self.bypass_until = now + 5
            else:
                # An older in-flight request may have failed during the probe;
                # only the probe that still owns the generation can close it.
                if probe_generation is not None and probe_generation == self.bypass_generation:
                    self.bypass_until = 0.0
                if result:
                    return result
        if account:
            key = (scope, account, ip)
            if now - self.cooldown.get(key, float('-inf')) < s.account_cooldown_ms / 1000:
                return ("deny", "ACCOUNT_COOLDOWN")
            self.cooldown[key] = now
            if len(self.cooldown) >= 1024:
                self.cooldown = {k: t for k, t in self.cooldown.items()
                                 if now - t < s.account_cooldown_ms / 1000}
        return None

    async def _admit(self, zone, ip):
        s = self.settings
        ip_key = "rl:ip:" + ip
        kind, wait = await asyncio.wait_for(self.redis.eval(_TAKE, 1, ip_key, s.ip_rps, s.ip_burst), 1)
        if kind not in (0, 1):
            raise ValueError("invalid Redis IP bucket response")
        if kind:
            return ("deny", "IP_RATE_LIMIT")
        if zone:
            override = getattr(s, "zone_overrides", {}).get(str(zone), {})
            rps = override.get("rps", 0)
            burst = override.get("burst", 0)
            rps = rps if rps > 0 else s.zone_rps
            burst = burst if burst > 0 else s.zone_burst
            kind, wait = await asyncio.wait_for(self.redis.eval(
                _TAKE, 1, f"rl:zone:{zone}", rps, burst), 1)
            if kind not in (0, 1):
                raise ValueError("invalid Redis zone bucket response")
            if kind:
                try:
                    await asyncio.wait_for(self.redis.eval(_REFUND, 1, ip_key, s.ip_rps, s.ip_burst), 1)
                except Exception:
                    _LOG.warning("failed to refund IP bucket", exc_info=True)
                return ("queue", int(wait))
        return None


class LoginRpc:
    def __init__(self, protocol, endpoints, directory=None):
        self.protocol, self.directory = protocol, directory
        self.static = {int(k): ([v] if isinstance(v, str) else list(v)) for k, v in endpoints.items()}
        self.dynamic = {}
        self.refreshed = 0.0
        self.counter = 0
        self.clients = {}

    async def call(self, zone, service, method, request, response, *, zoned=True):
        now = time.monotonic()
        if self.directory and now >= self.refreshed:
            self.refreshed = now + 5
            try:
                nodes = await self.directory.candidates(5, None)
                found = {}
                for node in nodes:
                    if node.zone_id and node.endpoint.ip and node.endpoint.port:
                        found.setdefault(node.zone_id, []).append(f"{node.endpoint.ip}:{node.endpoint.port}")
                self.dynamic = found
            except Exception:
                _LOG.warning("Login discovery failed; retaining last known endpoints", exc_info=True)
        if zoned:
            if zone <= 0:
                raise ValueError("zone id required")
            endpoints = self.dynamic.get(zone) or self.static.get(zone) or []
        else:
            endpoints = [item for values in (self.dynamic or self.static).values() for item in values]
        if not endpoints:
            raise ValueError("no login endpoint for zone")
        endpoint = endpoints[self.counter % len(endpoints)]
        self.counter += 1
        client = self.clients.get(endpoint)
        if client is None:
            client = self.clients[endpoint] = GrpcClient(endpoint, timeout=3, enable_retries=False)
        raw = await client.call(f"/loginpb.{service}/{method}", request.SerializeToString())
        return self.protocol.parse("loginpb." + response, raw)

    async def close(self):
        await asyncio.gather(*(client.close() for client in self.clients.values()))


async def _zone_admission(db, zone):
    if zone <= 0:
        return None
    try:
        async with asyncio.timeout(3):
            async with db.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT manual_status FROM zone_config WHERE zone_id=%s", (zone,))
                    row = await cur.fetchone()
                await conn.rollback()
    except Exception:
        _LOG.warning("zone admission lookup failed", exc_info=True)
        return {"code": 500, "error": "zone_admission_unavailable"}
    if row is None:
        return {"code": 404, "error": "zone_not_found"}
    status = row[0] if not isinstance(row, dict) else row["manual_status"]
    reason = {1: "zone_maintenance", 2: "zone_closed", 3: "zone_not_open"}.get(status)
    return {"code": 503, "error": reason} if reason else None


def _body(request):
    return request.json()


def _zone(body):
    zone = body.get("zone_id", 0)
    if isinstance(zone, bool) or not isinstance(zone, int) or not 0 <= zone < 2**32:
        raise ValueError("invalid zone_id")
    return zone


def _text(body, field):
    value = body.get(field, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("invalid string field")
    return value


def _tokens(response):
    return {"access_token": response.access_token, "refresh_token": response.refresh_token,
            "access_token_expire": response.access_token_expire,
            "refresh_token_expire": response.refresh_token_expire}


def _tip(response):
    tip = response.error_message
    if tip.id or tip.parameters:
        return {"code": 500 if tip.id in _FAULTS else 401,
                "message": tip.parameters or f"upstream_err={tip.id}"}
    return None


def _gate_response(response, queue_token=""):
    result = {"gate_port": 0, "token_deadline": 0}
    if response.status == 1:
        return result | {"code": 100, "queue_source": "login", "queue_token": queue_token or response.queue_token,
                         "queue_rank": response.queue_rank, "queue_total": response.queue_total,
                         "retry_after_ms": response.retry_after_ms or 2000}
    if response.status in (2, 3, 4):
        return result | {"code": {2: 500, 3: 410, 4: 401}[response.status],
                         "error": response.error or {2: "unknown_error", 3: "queue_token_expired", 4: "unauthenticated"}[response.status]}
    if response.status != 0 or not response.ip.strip():
        return result | {"code": 500, "error": response.error or "admitted_without_endpoint"}
    return {"code": 0, "gate_ip": response.ip, "gate_port": response.port,
            "token_payload": base64.b64encode(response.token_payload).decode(),
            "token_signature": base64.b64encode(response.token_signature).decode(),
            "token_deadline": response.token_deadline}


def register_login_routes(app, deps):
    p, rpc, limiter = deps.protocol, deps.rpc, deps.limiter

    async def handle(request):
        try:
            body = await _body(request)
            if not isinstance(body, dict):
                raise ValueError("invalid body")
            path = request.path.rsplit("/", 1)[-1]
            zone = _zone(body) if path != "refresh-token" else 0
            if path in ("login", "assign-gate"):
                account = _text(body, "account") if path == "login" else _text(body, "access_token")
                if path == "login" and not account:
                    account = _text(body, "auth_token")
                if account:
                    account = "tok:" + hashlib.sha256(account.encode()).hexdigest()[:32]
                if path == "assign-gate" and _text(body, "queue_token").strip():
                    account = ""
                decision = await limiter.check(zone, client_ip(request, limiter.networks), account, path)
                if decision:
                    kind, value = decision
                    if path == "login":
                        return web.json_response({"code": 100, "message": "QUEUEING", "retry_after_ms": value,
                                                  "queue_pos": -1 if kind == "wave" else 0} if kind in ("queue", "wave") else {"code": 429, "message": value})
                    return web.json_response({"code": 100, "retry_after_ms": value, "queue_pos": -1 if kind == "wave" else 0,
                                              "queue_source": "ratelimit", "gate_port": 0, "token_deadline": 0}
                                             if kind in ("queue", "wave") else {"code": 429, "error": value,
                                                                     "gate_port": 0, "token_deadline": 0})
            if path == "login":
                req = p.message("loginpb.LoginRequest", **{field: _text(body, field) for field in
                                ("account", "password", "auth_type", "auth_token")})
                response = await rpc.call(zone, "ClientPlayerLogin", "Login", req, "LoginResponse")
                result = _tip(response)
                if result is None:
                    result = {"code": 0, "players": [{"player_id": w.player.player_id,
                              "name": w.player.name or None, "level": 0, "zone_id": w.player.zone_id}
                              for w in response.players]} | _tokens(response)
            elif path == "refresh-token":
                token = _text(body, "refresh_token")
                if not token.strip():
                    return web.json_response({"code": 401, "message": "empty_refresh_token"})
                req = p.message("loginpb.RefreshTokenRequest", refresh_token=token)
                response = await rpc.call(0, "ClientPlayerLogin", "RefreshToken", req,
                                          "RefreshTokenResponse", zoned=False)
                result = _tip(response) or ({"code": 0} | _tokens(response))
            else:
                token = _text(body, "queue_token")
                if path == "queue-status" and not token.strip():
                    return web.json_response({"code": 410, "error": "missing_queue_token", "gate_port": 0,
                                              "token_deadline": 0})
                denied = await _zone_admission(deps.db, zone)
                if denied:
                    return web.json_response({"gate_port": 0, "token_deadline": 0} | denied)
                if path == "assign-gate":
                    req = p.message("loginpb.AssignGateRequest", zone_id=zone, queue_token=token,
                                    access_token=_text(body, "access_token"), device_id=_text(body, "device_id"))
                    response = await rpc.call(zone, "LoginPreGate", "AssignGate", req, "AssignGateResponse")
                    result = _gate_response(response)
                else:
                    req = p.message("loginpb.QueryQueueStatusRequest", queue_token=token)
                    response = await rpc.call(zone, "LoginPreGate", "QueryQueueStatus", req, "QueryQueueStatusResponse")
                    result = _gate_response(response, token)
            return web.json_response(result)
        except (ValueError, TypeError):
            return web.json_response({"code": 500, "message": "internal_error"} if request.path.endswith(("login", "refresh-token"))
                                     else {"code": 500, "error": "internal_error", "gate_port": 0, "token_deadline": 0})
        except grpc.aio.AioRpcError as exc:
            status = exc.code()
            message = "login_unavailable" if status in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED,
                                                          grpc.StatusCode.RESOURCE_EXHAUSTED) else status.name
            if request.path.endswith(("login", "refresh-token")):
                return web.json_response({"code": 401 if status in (grpc.StatusCode.UNAUTHENTICATED,
                                          grpc.StatusCode.PERMISSION_DENIED) else 500, "message": message})
            return web.json_response({"code": 500, "error": message, "gate_port": 0, "token_deadline": 0})

    for route in ("login", "refresh-token", "assign-gate", "queue-status"):
        app.router.add_post("/api/" + route, handle)
