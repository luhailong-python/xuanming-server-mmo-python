"""Authenticated management of the shared etcd RPC kill-switch rules."""

import json
import logging
import re

import grpc
from aiohttp import web

from .kill_switch import parse_key, parse_rule
from .portal_admin_auth import authorize

_LOG = logging.getLogger(__name__)
_PREFIX = "/mmorpg/killswitch/"
_CHARS = re.compile(r"[A-Za-z0-9_.$/*-]+\Z")
_TYPE = re.compile(r"[A-Z][A-Za-z0-9]*NodeService\Z")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")


def _target(value):
    value = value.strip().lstrip("/") if isinstance(value, str) else ""
    if not value or len(value) > 200 or not _CHARS.fullmatch(value):
        raise web.HTTPBadRequest(text="invalid target")
    if value != "*":
        parts = value.split("/")
        if (len(parts) != 2 or not all(parts) or parts[0] in ("type", "instance")
                or "*" in parts[0] or "*" in parts[1] and parts[1] != "*"):
            raise web.HTTPBadRequest(text="invalid target")
    return value


def _view(relative, raw):
    parsed = parse_key(relative)
    rule = parse_rule(raw) if parsed else None
    scope, scope_value, target = parsed or ("malformed", "", "")
    return {"key": relative, "scope": scope, "scope_value": scope_value, "target": target,
            "valid": rule is not None, "deny": rule.deny if rule else False,
            "reason": rule.reason if rule else "", "code": rule.code if rule else 0, "raw": raw}


def register_routes(app, deps):
    async def call(method, **fields):
        if deps.directory is None:
            raise web.HTTPServiceUnavailable(text="etcd unavailable")
        try:
            req = deps.protocol.message(f"etcdserverpb.{method}Request", **fields)
            result = await deps.directory.client.call(f"/etcdserverpb.KV/{method}", req.SerializeToString())
            return deps.protocol.parse(f"etcdserverpb.{method}Response", result)
        except (grpc.RpcError, OSError, TimeoutError):
            raise web.HTTPServiceUnavailable(text="etcd unavailable")

    async def scan(prefix):
        key = prefix.encode()
        response = await call("Range", key=key, range_end=key[:-1] + bytes((key[-1] + 1,)))
        return [(kv.key.decode(), kv.value.decode(errors="replace")) for kv in response.kvs]

    async def list_rules(request):
        authorize(request, deps)
        return web.json_response([_view(key[len(_PREFIX):], value) for key, value in sorted(await scan(_PREFIX))])

    async def put_rule(request):
        authorize(request, deps)
        try:
            body = await request.json()
            target = _target(body.get("target"))
            deny = body.get("deny")
            reason, code = body.get("reason") or "", body.get("code") or 0
            if (type(deny) is not bool or not isinstance(reason, str) or len(reason) > 256
                    or any(ord(c) < 32 or ord(c) == 127 for c in reason)
                    or type(code) is not int or not 0 <= code <= 16):
                raise ValueError("invalid rule")
            node_type, instance = body.get("node_type") or "", body.get("instance") or ""
            if not isinstance(node_type, str) or not isinstance(instance, str):
                raise ValueError("invalid scope")
            node_type, instance = node_type.strip(), instance.strip().lower()
            if instance and not node_type or node_type and not _TYPE.fullmatch(node_type):
                raise ValueError("invalid node type")
            scope = ""
            if node_type:
                entries = await scan(f"{node_type}.rpc/")
                uuids = set()
                for key, raw in entries:
                    if key.startswith(f"{node_type}.rpc/allocated/"):
                        continue
                    try:
                        info = json.loads(raw)
                        if isinstance(info, dict) and isinstance(info.get("nodeUuid"), str):
                            uuids.add(info["nodeUuid"])
                    except ValueError:
                        continue
                if instance:
                    if not _UUID.fullmatch(instance) or instance not in uuids:
                        raise ValueError("instance not registered")
                    scope = f"instance/{instance}/"
                elif uuids:
                    scope = f"type/{node_type}/"
                else:
                    raise ValueError("node type not registered")
            value = {"deny": deny}
            if reason:
                value["reason"] = reason
            if code:
                value["code"] = code
            raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            relative = scope + target
            await call("Put", key=(_PREFIX + relative).encode(), value=raw.encode())
            _LOG.warning("AUDIT killswitch set key=%s deny=%s code=%s remote=%s", relative, deny, code, request.remote)
            return web.json_response(_view(relative, raw))
        except (ValueError, TypeError, AttributeError):
            raise web.HTTPBadRequest(text="invalid rule")

    async def delete_rule(request):
        authorize(request, deps)
        relative = request.query.get("key", "").strip().lstrip("/")
        if not relative or len(relative) > 264 or not _CHARS.fullmatch(relative):
            raise web.HTTPBadRequest(text="invalid key")
        response = await call("DeleteRange", key=(_PREFIX + relative).encode())
        _LOG.warning("AUDIT killswitch removed key=%s remote=%s", relative, request.remote)
        return web.json_response({"key": relative, "deleted": response.deleted})

    app.router.add_get("/admin/killswitch", list_rules)
    app.router.add_put("/admin/killswitch", put_rule)
    app.router.add_delete("/admin/killswitch", delete_rule)
