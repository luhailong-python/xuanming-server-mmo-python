"""运维/GM 配置表热更轨道指针管理入口，对标 Java gateway_node AdminTableBundleController。"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Callable

import grpc
from aiohttp import web

from .portal_admin_auth import authorize

_LOG = logging.getLogger(__name__)

POINTER_PREFIX = "/mmorpg/tables/pointer/"
STATUS_PREFIX = "/mmorpg/tables/status/"
MIN_ACTIVATE_LEAD_MS = 60_000
MAX_REASON_CHARS = 256
MAX_OPERATOR_CHARS = 64
OPERATOR_HEADER = "X-Admin-Operator"

_VALID_TRACKS = frozenset(("stable", "canary"))
_SCHEMA_DIGEST = re.compile(r"[0-9a-f]{16}\Z")
_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")
_OPERATOR_CHARS = re.compile(r"[A-Za-z0-9_.@-]+\Z")


class EtcdUnavailableError(RuntimeError):
    """etcd 不可达或调用超时时抛出的内部异常。"""


@dataclass(frozen=True, slots=True)
class TablePointerEntry:
    """单条 etcd 键值及其 mod_revision（不存在时用 0 表示）。"""

    value: str
    mod_revision: int


@dataclass(frozen=True, slots=True)
class PointerTarget:
    content_digest: str | None
    bundle_sha256: str | None
    version: int = 0
    activate_at_ms: int | None = None
    immediate: bool | None = None

    def to_dict(self) -> dict:
        out: dict = {"version": self.version}
        if self.content_digest is not None:
            out["content_digest"] = self.content_digest
        if self.bundle_sha256 is not None:
            out["bundle_sha256"] = self.bundle_sha256
        if self.activate_at_ms is not None:
            out["activate_at_ms"] = self.activate_at_ms
        if self.immediate is not None:
            out["immediate"] = self.immediate
        return out


@dataclass(frozen=True, slots=True)
class PointerLine:
    current: PointerTarget | None
    next: PointerTarget | None
    updated_at_ms: int
    updated_by: str | None
    reason: str | None

    def to_dict(self) -> dict:
        out: dict = {"updated_at_ms": self.updated_at_ms}
        if self.current is not None:
            out["current"] = self.current.to_dict()
        if self.next is not None:
            out["next"] = self.next.to_dict()
        if self.updated_by is not None:
            out["updated_by"] = self.updated_by
        if self.reason is not None:
            out["reason"] = self.reason
        return out


@dataclass(frozen=True, slots=True)
class _PodStatus:
    pod: str
    service: str
    track: str
    schema_digest: str
    loaded_digest: str
    pending_state: str
    pending_digest: str
    pending_reason: str


class EtcdTablePointerStore:
    """基于 etcd Txn mod_revision CAS 的配置表指针键值存储，对标 EtcdTablePointerStore。"""

    def __init__(self, protocol, directory, *, timeout_sec: float = 3.0) -> None:
        self._protocol = protocol
        self._directory = directory
        self._timeout_sec = timeout_sec

    async def _call(self, method: str, **fields):
        if self._directory is None:
            raise EtcdUnavailableError("etcd directory is not configured")
        try:
            async with asyncio.timeout(self._timeout_sec):
                req = self._protocol.message(f"etcdserverpb.{method}Request", **fields)
                raw = await self._directory.client.call(
                    f"/etcdserverpb.KV/{method}", req.SerializeToString()
                )
                return self._protocol.parse(f"etcdserverpb.{method}Response", raw)
        except (grpc.RpcError, OSError, TimeoutError) as exc:
            raise EtcdUnavailableError(f"etcd {method} failed") from exc

    async def get(self, key: str) -> TablePointerEntry | None:
        resp = await self._call("Range", key=key.encode("utf-8"))
        if not resp.kvs:
            return None
        kv = resp.kvs[0]
        return TablePointerEntry(
            kv.value.decode("utf-8", errors="replace"),
            kv.mod_revision,
        )

    async def list(self, prefix: str) -> dict[str, TablePointerEntry]:
        key = prefix.encode("utf-8")
        range_end = key[:-1] + bytes((key[-1] + 1,))
        resp = await self._call("Range", key=key, range_end=range_end)
        items = [
            (
                kv.key.decode("utf-8"),
                TablePointerEntry(kv.value.decode("utf-8", errors="replace"), kv.mod_revision),
            )
            for kv in resp.kvs
        ]
        return dict(sorted(items, key=lambda pair: pair[0]))

    async def compare_and_put(self, key: str, expected_mod_revision: int, value: str) -> bool:
        encoded_key = key.encode("utf-8")
        resp = await self._call(
            "Txn",
            compare=[{
                "key": encoded_key,
                "result": "EQUAL",
                "target": "MOD",
                "mod_revision": expected_mod_revision,
            }],
            success=[{
                "request_put": {
                    "key": encoded_key,
                    "value": value.encode("utf-8"),
                },
            }],
        )
        return bool(resp.succeeded)

    async def compare_and_delete(self, key: str, expected_mod_revision: int) -> bool:
        encoded_key = key.encode("utf-8")
        resp = await self._call(
            "Txn",
            compare=[{
                "key": encoded_key,
                "result": "EQUAL",
                "target": "MOD",
                "mod_revision": expected_mod_revision,
            }],
            success=[{
                "request_delete_range": {"key": encoded_key},
            }],
        )
        return bool(resp.succeeded)


def pointer_key(track: str, schema_digest: str) -> str:
    return f"{POINTER_PREFIX}{track}/{schema_digest}"


def active_target(line: PointerLine | None, now_ms: int) -> PointerTarget | None:
    """根据当前时间戳判断本条 schema 线实际生效的目标包。"""
    if line is None:
        return None
    if line.next is not None and line.next.content_digest:
        act_at = 0 if line.next.activate_at_ms is None else line.next.activate_at_ms
        if line.next.immediate is True or now_ms >= act_at:
            return line.next
    if line.current is not None and line.current.content_digest:
        return line.current
    return None


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _strict_loads(raw: str):
    def _reject_const(_token: str):
        raise ValueError("invalid JSON constant")

    return json.loads(raw, parse_constant=_reject_const)


def _parse_target(data) -> PointerTarget | None:
    if data is None:
        return None
    if not isinstance(data, dict):
        raise ValueError("target must be object")
    content_digest = data.get("content_digest")
    bundle_sha256 = data.get("bundle_sha256")
    version = data.get("version", 0)
    activate_at_ms = data.get("activate_at_ms")
    immediate = data.get("immediate")
    if content_digest is not None and not isinstance(content_digest, str):
        raise ValueError("invalid content_digest")
    if bundle_sha256 is not None and not isinstance(bundle_sha256, str):
        raise ValueError("invalid bundle_sha256")
    if not _is_int(version):
        raise ValueError("invalid version")
    if activate_at_ms is not None and not _is_int(activate_at_ms):
        raise ValueError("invalid activate_at_ms")
    if immediate is not None and type(immediate) is not bool:
        raise ValueError("invalid immediate")
    return PointerTarget(
        content_digest=content_digest,
        bundle_sha256=bundle_sha256,
        version=version,
        activate_at_ms=activate_at_ms,
        immediate=immediate,
    )


def parse_pointer_line(raw: str) -> PointerLine | None:
    """严格解析 etcd 中存储的 PointerLine JSON，损坏时返回 None。"""
    try:
        node = _strict_loads(raw)
        if not isinstance(node, dict):
            return None
        current = _parse_target(node.get("current"))
        next_target = _parse_target(node.get("next"))
        updated_at_ms = node.get("updated_at_ms", 0)
        updated_by = node.get("updated_by")
        reason = node.get("reason")
        if not _is_int(updated_at_ms):
            return None
        if updated_by is not None and not isinstance(updated_by, str):
            return None
        if reason is not None and not isinstance(reason, str):
            return None
        return PointerLine(
            current=current,
            next=next_target,
            updated_at_ms=updated_at_ms,
            updated_by=updated_by,
            reason=reason,
        )
    except (ValueError, TypeError):
        return None


def _text_of(node: dict, field: str) -> str:
    value = node.get(field)
    return value if isinstance(value, str) else ""


def _parse_pod_status(track: str, pod: str, raw: str) -> _PodStatus | None:
    try:
        node = _strict_loads(raw)
        if not isinstance(node, dict):
            return None
        return _PodStatus(
            pod=pod,
            service=_text_of(node, "service"),
            track=track,
            schema_digest=_text_of(node, "schema_digest"),
            loaded_digest=_text_of(node, "loaded_digest"),
            pending_state=_text_of(node, "pending_state"),
            pending_digest=_text_of(node, "pending_digest"),
            pending_reason=_text_of(node, "pending_reason"),
        )
    except (ValueError, TypeError):
        return None


def _line_view(
    track: str,
    schema_digest: str,
    line: PointerLine,
    active: PointerTarget | None,
    valid: bool,
    summary: dict,
    rejected_pods: list[dict],
    needs_restart_pods: list[dict],
) -> dict:
    out: dict = {
        "track": track,
        "schema_digest": schema_digest,
        "updated_at_ms": line.updated_at_ms,
        "valid": valid,
        "summary": summary,
        "rejected_pods": rejected_pods,
        "needs_restart_pods": needs_restart_pods,
    }
    if line.current is not None:
        out["current"] = line.current.to_dict()
    if line.next is not None:
        out["next"] = line.next.to_dict()
    if active is not None:
        out["active"] = active.to_dict()
    if line.updated_by is not None:
        out["updated_by"] = line.updated_by
    if line.reason is not None:
        out["reason"] = line.reason
    return out


def _empty_summary() -> dict:
    return {"total": 0, "loaded": 0, "prepared": 0, "rejected": 0, "needs_restart": 0}


def _require_track(raw) -> str:
    track = raw.strip() if isinstance(raw, str) else ""
    if track not in _VALID_TRACKS:
        raise web.HTTPBadRequest(text="track must be 'stable' or 'canary'")
    return track


def _require_schema_digest(raw) -> str:
    text = raw.strip() if isinstance(raw, str) else ""
    if not _SCHEMA_DIGEST.fullmatch(text):
        raise web.HTTPBadRequest(text="schema_digest must be 16 lowercase hex characters")
    return text


def _require_sha256(raw, field: str) -> str:
    text = raw.strip() if isinstance(raw, str) else ""
    if not _SHA256_HEX.fullmatch(text):
        raise web.HTTPBadRequest(text=f"{field} must be 64 lowercase hex characters")
    return text


def _valid_reason(raw) -> str:
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise web.HTTPBadRequest(text="reason must be a string")
    reason = raw.strip()
    if len(reason) > MAX_REASON_CHARS or any(ord(c) < 0x20 or 0x7F <= ord(c) <= 0x9F for c in reason):
        raise web.HTTPBadRequest(
            text=f"reason must be at most {MAX_REASON_CHARS} characters without control characters"
        )
    return reason


def _operator(request: web.Request) -> str:
    raw = request.headers.get(OPERATOR_HEADER)
    if raw is None or not raw.strip():
        return "-"
    text = raw.strip()
    if len(text) > MAX_OPERATOR_CHARS or not _OPERATOR_CHARS.fullmatch(text):
        return "invalid"
    return text


def _to_json(line: PointerLine) -> str:
    return json.dumps(line.to_dict(), ensure_ascii=False, separators=(",", ":"))


def register_routes(
    app: web.Application,
    deps,
    *,
    store=None,
    clock: Callable[[], int] | None = None,
) -> None:
    """注册 /admin/tables 配置表热更轨道指针管理路由。"""
    pointer_store = store or EtcdTablePointerStore(deps.protocol, deps.directory)
    now_ms_fn = clock or (lambda: time.time_ns() // 1_000_000)

    async def _store_call(coro):
        try:
            return await coro
        except web.HTTPException:
            raise
        except EtcdUnavailableError as exc:
            _LOG.warning("Table bundle admin call failed because etcd is unavailable: %s", exc)
            raise web.HTTPServiceUnavailable(text="etcd unavailable") from None
        except Exception as exc:
            _LOG.warning("Table bundle admin call failed unexpectedly: %s", type(exc).__name__)
            raise web.HTTPServiceUnavailable(text="etcd unavailable") from None

    async def list_lines(request: web.Request) -> web.Response:
        authorize(request, deps)
        now_ms = now_ms_fn()
        pointer_raw = await _store_call(pointer_store.list(POINTER_PREFIX))
        status_raw = await _store_call(pointer_store.list(STATUS_PREFIX))

        lines: dict[tuple[str, str], PointerLine] = {}
        line_valid: dict[tuple[str, str], bool] = {}

        for full_key, entry in pointer_raw.items():
            rel = full_key[len(POINTER_PREFIX):]
            slash = rel.find("/")
            if slash <= 0 or slash == len(rel) - 1 or "/" in rel[slash + 1:]:
                continue
            key = (rel[:slash], rel[slash + 1:])
            parsed = parse_pointer_line(entry.value)
            if parsed is None:
                lines[key] = PointerLine(None, None, 0, "", "")
                line_valid[key] = False
            else:
                lines[key] = parsed
                line_valid[key] = True

        pods_by_line: dict[tuple[str, str], list[_PodStatus]] = {}
        for full_key, entry in status_raw.items():
            rel = full_key[len(STATUS_PREFIX):]
            slash = rel.find("/")
            if slash <= 0 or slash == len(rel) - 1:
                continue
            track, pod = rel[:slash], rel[slash + 1:]
            ps = _parse_pod_status(track, pod, entry.value)
            if ps is None or not ps.schema_digest:
                continue
            key = (track, ps.schema_digest)
            pods_by_line.setdefault(key, []).append(ps)
            lines.setdefault(key, PointerLine(None, None, 0, "", ""))
            line_valid.setdefault(key, True)

        views = []
        for key in sorted(lines):
            track, schema_digest = key
            line = lines[key]
            active = active_target(line, now_ms)
            pods = pods_by_line.get(key, [])

            loaded = prepared = rejected = needs_restart = 0
            rejected_pods = []
            needs_restart_pods = []

            if line.next is not None:
                desired_digest = line.next.content_digest or ""
            elif active is not None:
                desired_digest = active.content_digest or ""
            else:
                desired_digest = ""

            for ps in pods:
                state = ps.pending_state
                if state == "rejected":
                    rejected += 1
                    rejected_pods.append({
                        "pod": ps.pod,
                        "service": ps.service,
                        "state": state,
                        "digest": ps.pending_digest,
                        "reason": ps.pending_reason,
                    })
                elif state == "needs_restart":
                    needs_restart += 1
                    needs_restart_pods.append({
                        "pod": ps.pod,
                        "service": ps.service,
                        "state": state,
                        "digest": ps.pending_digest,
                        "reason": ps.pending_reason,
                    })
                elif state == "prepared":
                    prepared += 1
                elif not desired_digest or desired_digest == ps.loaded_digest:
                    loaded += 1

            summary = {
                "total": len(pods),
                "loaded": loaded,
                "prepared": prepared,
                "rejected": rejected,
                "needs_restart": needs_restart,
            }
            views.append(
                _line_view(
                    track,
                    schema_digest,
                    line,
                    active,
                    line_valid.get(key, True),
                    summary,
                    rejected_pods,
                    needs_restart_pods,
                )
            )
        return web.json_response(views)

    async def publish_line(request: web.Request) -> web.Response:
        authorize(request, deps)
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            raise web.HTTPBadRequest(text="request body must be valid JSON") from None
        if not isinstance(body, dict):
            raise web.HTTPBadRequest(text="request body is required")

        track = _require_track(body.get("track"))
        schema_digest = _require_schema_digest(body.get("schema_digest"))
        content_digest = _require_sha256(body.get("content_digest"), "content_digest")
        bundle_sha256 = _require_sha256(body.get("bundle_sha256"), "bundle_sha256")

        raw_version = body.get("version")
        if raw_version is None:
            version = 0
        elif _is_int(raw_version) and raw_version >= 0:
            version = raw_version
        else:
            raise web.HTTPBadRequest(text="version must be non-negative")

        reason = _valid_reason(body.get("reason"))
        raw_immediate = body.get("immediate")
        if raw_immediate is not None and type(raw_immediate) is not bool:
            raise web.HTTPBadRequest(text="immediate must be a boolean")
        immediate = raw_immediate is True

        now_ms = now_ms_fn()
        if immediate:
            activate_at_ms = None
        else:
            min_activate = now_ms + MIN_ACTIVATE_LEAD_MS
            raw_act = body.get("activate_at_ms")
            if raw_act is None:
                requested = min_activate
            elif _is_int(raw_act):
                requested = raw_act
            else:
                raise web.HTTPBadRequest(text="activate_at_ms must be an integer")
            if requested < min_activate:
                raise web.HTTPBadRequest(
                    text="activate_at_ms must be at least 60 seconds in the future unless immediate=true"
                )
            activate_at_ms = requested

        key = pointer_key(track, schema_digest)
        existing = await _store_call(pointer_store.get(key))
        expected_rev = 0 if existing is None else existing.mod_revision
        prev = None if existing is None else parse_pointer_line(existing.value)

        # 原来已生效的那一包折进 current，清掉 activate_at_ms / immediate。
        folded = None if prev is None else active_target(prev, now_ms)
        if folded is not None:
            folded = PointerTarget(
                content_digest=folded.content_digest,
                bundle_sha256=folded.bundle_sha256,
                version=folded.version,
                activate_at_ms=None,
                immediate=None,
            )
        next_target = PointerTarget(
            content_digest=content_digest,
            bundle_sha256=bundle_sha256,
            version=version,
            activate_at_ms=activate_at_ms,
            immediate=True if immediate else None,
        )
        operator = _operator(request)
        updated = PointerLine(
            current=folded,
            next=next_target,
            updated_at_ms=now_ms,
            updated_by=operator,
            reason=reason or None,
        )
        ok = await _store_call(pointer_store.compare_and_put(key, expected_rev, _to_json(updated)))
        if not ok:
            raise web.HTTPConflict(text="concurrent table pointer update detected; retry")

        _LOG.warning(
            'AUDIT table bundle pointer set track=%s schema_digest=%s content_digest=%s '
            'version=%d immediate=%s activate_at_ms=%s reason="%s" operator=%s remote=%s',
            track,
            schema_digest,
            content_digest[:12],
            version,
            immediate,
            activate_at_ms,
            reason,
            operator,
            request.remote,
        )
        return web.json_response(
            _line_view(
                track,
                schema_digest,
                updated,
                active_target(updated, now_ms),
                True,
                _empty_summary(),
                [],
                [],
            )
        )

    async def cancel_next(request: web.Request) -> web.Response:
        authorize(request, deps)
        track = _require_track(request.query.get("track"))
        schema_digest = _require_schema_digest(request.query.get("schema_digest"))
        now_ms = now_ms_fn()

        key = pointer_key(track, schema_digest)
        existing = await _store_call(pointer_store.get(key))
        if existing is None:
            raise web.HTTPNotFound(text="pointer line not found")
        prev = parse_pointer_line(existing.value)
        if prev is None or prev.next is None:
            raise web.HTTPNotFound(text="no pending next target to cancel")
        next_target = prev.next
        if next_target.immediate is True or (
            next_target.activate_at_ms is not None and now_ms >= next_target.activate_at_ms
        ):
            raise web.HTTPConflict(
                text="next target has already reached activate_at; publish a rollback instead"
            )

        operator = _operator(request)
        updated = PointerLine(
            current=prev.current,
            next=None,
            updated_at_ms=now_ms,
            updated_by=operator,
            reason="cancelled",
        )
        ok = await _store_call(
            pointer_store.compare_and_put(key, existing.mod_revision, _to_json(updated))
        )
        if not ok:
            raise web.HTTPConflict(text="concurrent table pointer update detected; retry")

        cancelled_digest = next_target.content_digest or ""
        _LOG.warning(
            "AUDIT table bundle next cancelled track=%s schema_digest=%s cancelled_digest=%s operator=%s remote=%s",
            track,
            schema_digest,
            cancelled_digest[:12],
            operator,
            request.remote,
        )
        return web.json_response(
            _line_view(
                track,
                schema_digest,
                updated,
                active_target(updated, now_ms),
                True,
                _empty_summary(),
                [],
                [],
            )
        )

    async def delete_line(request: web.Request) -> web.Response:
        authorize(request, deps)
        track = _require_track(request.query.get("track"))
        schema_digest = _require_schema_digest(request.query.get("schema_digest"))

        # 只要该 schema 线仍有在线实例回报就拒绝删除，防止进程重启退回旧镜像表。
        track_status_prefix = f"{STATUS_PREFIX}{track}/"
        statuses = await _store_call(pointer_store.list(track_status_prefix))
        active_pods = []
        for full_key, entry in statuses.items():
            pod = full_key[len(track_status_prefix):]
            ps = _parse_pod_status(track, pod, entry.value)
            if ps is not None and ps.schema_digest == schema_digest:
                active_pods.append(pod)
        if active_pods:
            raise web.HTTPConflict(
                text=f"schema line still has {len(active_pods)} active reporting pod(s), e.g. {active_pods[0]}"
            )

        key = pointer_key(track, schema_digest)
        existing = await _store_call(pointer_store.get(key))
        if existing is None:
            raise web.HTTPNotFound(text="pointer line not found")
        deleted = await _store_call(pointer_store.compare_and_delete(key, existing.mod_revision))
        if not deleted:
            raise web.HTTPConflict(text="concurrent table pointer update detected; retry")

        _LOG.warning(
            "AUDIT table bundle schema line deleted track=%s schema_digest=%s operator=%s remote=%s",
            track,
            schema_digest,
            _operator(request),
            request.remote,
        )
        return web.json_response({
            "track": track,
            "schema_digest": schema_digest,
            "deleted": True,
        })

    app.router.add_get("/admin/tables", list_lines)
    app.router.add_put("/admin/tables", publish_line)
    app.router.add_delete("/admin/tables/next", cancel_next)
    app.router.add_delete("/admin/tables", delete_line)
