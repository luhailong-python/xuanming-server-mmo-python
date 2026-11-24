"""Scene 在线玩家 GM 命令入口（对齐 C++ gm_player_command_security.h 与 scene_node_service.cpp）。

负责对独立 GM 后台发来的 ``/scene_node.SceneNodeGrpc/GmExecutePlayerCommand`` 进行：
1. ``gm-player-command-v1`` HMAC-SHA256 签名与时间戳窗口校验；
2. 进程内按 ``(operator, command_id)`` 幂等去重与 nonce 防重放；
3. dev/test 运行模式闸、在线会话身份核对与可写闸校验；
4. 7 项固定类型 GM 命令执行与原子存档提交。
"""

from __future__ import annotations

import hashlib
import heapq
import hmac
import logging
import os
import re
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from inspect import isawaitable
from secrets import randbelow

from ..data import StoreRejected, StoreUnavailable
from ..data.asset_inventory import (
    FEATURE_UNAVAILABLE,
    INVALID,
    INVALID_DATA,
    UNAVAILABLE,
    balance,
    clone,
)
from ..grpc_runtime import GrpcUnavailable
from .scene_mission import LEVEL, effective_level
from .scene_stats import pet_level, pet_stats, player_stats

logger = logging.getLogger(__name__)

GM_PLAYER_COMMAND_METHOD = "/scene_node.SceneNodeGrpc/GmExecutePlayerCommand"
GM_PLAYER_COMMAND_SECRET_ENV = "MMORPG_GM_PLAYER_COMMAND_SECRET"
GM_AUTH_SKEW_ENV = "GATE_GM_AUTH_SKEW_SECONDS"

DRAFT_TTL_SECONDS = 300
DEFAULT_SKEW_SECONDS = 300
MIN_SKEW_SECONDS = 30
MAX_SKEW_SECONDS = 900
MIN_SECRET_BYTES = 32
MAX_REQUEST_BYTES = 16 * 1024
MAX_NONCES = 1024
DEFAULT_CACHE_CAPACITY = 10_000

_UINT32_MAX = 0xFFFF_FFFF
_INT64_MAX = (1 << 63) - 1
_ASCII_SPACE = " \t\n\r\v\f"
_UNIX_DIGITS = re.compile(r"[0-9]{1,20}")
_SIGNED_DIGITS = re.compile(r"[+-]?[0-9]{1,20}")
_LOWER_HEX = re.compile(r"[0-9a-f]{64}")

_TX_GM_GRANT = 9
_TX_GM_DEDUCT = 10
_EQUIP_GRANT_INVALID = 28009
_PET_ROW_MISSING = 26001
_PET_FULL = 26002
_PET_ID_UNAVAILABLE = 26016


def resolve_gm_skew_seconds(environ=None) -> int:
    """解析允许的时钟偏差秒数（默认 300s，钳制在 [30, 900]）。"""
    env = os.environ if environ is None else environ
    raw = env.get(GM_AUTH_SKEW_ENV, "").lstrip(_ASCII_SPACE)
    if not _SIGNED_DIGITS.fullmatch(raw):
        return DEFAULT_SKEW_SECONDS
    parsed = int(raw)
    if not (-1 - _INT64_MAX) <= parsed <= _INT64_MAX:
        return DEFAULT_SKEW_SECONDS
    return min(MAX_SKEW_SECONDS, max(MIN_SKEW_SECONDS, parsed))


def sanitize_audit_text(value: str) -> str:
    """清理审计日志中的控制字符，防止日志注入。"""
    return "".join(" " if ord(ch) < 32 or ord(ch) == 127 else ch for ch in value)


def command_digest(request) -> str:
    """计算清空 ``auth`` 后的确定性 protobuf SHA-256 小写十六进制摘要。"""
    unsigned = clone(request)
    unsigned.ClearField("auth")
    return hashlib.sha256(unsigned.SerializeToString(deterministic=True)).hexdigest()


def _has_control_or_pipe(text: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 or ch == "|" for ch in text)


def has_valid_envelope_fields(request) -> bool:
    """校验 GM 玩家命令的外层业务字段与尺寸边界。"""
    operator = getattr(request, "operator", "")
    reason = request.reason
    auth = request.auth
    return (
        request.command_id != 0
        and request.player_id != 0
        and request.issued_at_ms != 0
        and request.target_node_id != 0
        and request.expected_session_id not in (0, _UINT32_MAX)
        and request.expected_login_generation != 0
        and 1 <= len(operator.encode("utf-8")) <= 64
        and not _has_control_or_pipe(operator)
        and 1 <= len(reason.encode("utf-8")) <= 1000
        and len(auth.encode("utf-8")) <= 512
        and request.ByteSize() <= MAX_REQUEST_BYTES
        and request.WhichOneof("command") is not None
    )


def _parse_envelope(field: str) -> tuple[str, str, str, str] | None:
    parts = field.split("|", 4)
    if len(parts) != 4:
        return None
    operator, timestamp_raw, nonce, signature = parts
    if (
        not 1 <= len(operator.encode("utf-8")) <= 64
        or not _UNIX_DIGITS.fullmatch(timestamp_raw)
        or not 1 <= len(nonce.encode("utf-8")) <= 128
        or not _LOWER_HEX.fullmatch(signature)
    ):
        return None
    if int(timestamp_raw) > _INT64_MAX:
        return None
    return operator, timestamp_raw, nonce, signature


class _NonceCache:
    """窗口内 nonce 去重表；表满时不提前淘汰未过期项，防止重开重放窗口。"""

    def __init__(self, max_entries: int = MAX_NONCES) -> None:
        self._max_entries = max_entries
        self._entries: OrderedDict[str, int] = OrderedDict()

    def _prune(self, now_unix: int) -> None:
        while self._entries and next(iter(self._entries.values())) <= now_unix:
            self._entries.popitem(last=False)

    def register(self, nonce: str, now_unix: int, retain_seconds: int) -> bool:
        self._prune(now_unix)
        if nonce in self._entries or len(self._entries) >= self._max_entries:
            return False
        self._entries[nonce] = now_unix + retain_seconds
        return True


@dataclass(slots=True)
class _CachedCommand:
    digest: str
    response: object
    expires_at: int


class CommandCache:
    """按 ``(operator, command_id)`` 缓存执行结果，仅回收彻底超出有效期与偏差窗口的旧条目。"""

    def __init__(self, capacity: int = DEFAULT_CACHE_CAPACITY) -> None:
        self._capacity = capacity
        self._entries: dict[tuple[str, int], _CachedCommand] = {}
        self._expiry_heap: list[tuple[int, tuple[str, int]]] = []

    def __len__(self) -> int:
        return len(self._entries)

    @staticmethod
    def _key(request) -> tuple[str, int]:
        return getattr(request, "operator", ""), int(request.command_id)

    @staticmethod
    def _expires_at(issued_at_ms: int, skew_seconds: int) -> int:
        issued_sec = issued_at_ms // 1000
        retention = DRAFT_TTL_SECONDS + max(0, skew_seconds)
        return min(_INT64_MAX, issued_sec + retention)

    def _prune(self, now_unix: int) -> None:
        if now_unix < 0:
            return
        while self._expiry_heap and self._expiry_heap[0][0] < now_unix:
            expires_at, key = heapq.heappop(self._expiry_heap)
            entry = self._entries.get(key)
            if entry is not None and entry.expires_at <= expires_at:
                del self._entries[key]

    def lookup(self, request, digest: str, now_unix: int) -> tuple[str, object | None]:
        """只读查重：返回 ``('new', None)``、``('replay', response)`` 或 ``('conflict', None)``。"""
        self._prune(now_unix)
        entry = self._entries.get(self._key(request))
        if entry is None:
            return "new", None
        if entry.digest != digest:
            return "conflict", None
        return "replay", clone(entry.response)

    def begin(
        self,
        request,
        digest: str,
        initial_response,
        now_unix: int,
        skew_seconds: int,
    ) -> tuple[str, object | None]:
        """占位登记新命令，返回 ``('new', None)``、``('replay', response)``、``('conflict', None)`` 或 ``('full', None)``。"""
        self._prune(now_unix)
        key = self._key(request)
        entry = self._entries.get(key)
        if entry is not None:
            if entry.digest != digest:
                return "conflict", None
            return "replay", clone(entry.response)
        if len(self._entries) >= self._capacity:
            return "full", None
        expires_at = self._expires_at(request.issued_at_ms, skew_seconds)
        self._entries[key] = _CachedCommand(digest=digest, response=clone(initial_response), expires_at=expires_at)
        heapq.heappush(self._expiry_heap, (expires_at, key))
        return "new", None

    def complete(self, request, response) -> None:
        entry = self._entries.get(self._key(request))
        if entry is not None:
            entry.response = clone(response)


class SceneGmCommands:
    """Scene 节点在线玩家 GM 命令处理器（挂接 ``scene_node.SceneNodeGrpc/GmExecutePlayerCommand``）。"""

    def __init__(
        self,
        scene,
        *,
        secret: bytes | str | None = None,
        allow_gm: bool | None = None,
        skew_seconds: int | None = None,
        clock: Callable[[], float] = time.time,
        cache_capacity: int = DEFAULT_CACHE_CAPACITY,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        if secret is None:
            self._secret: bytes | None = None
        else:
            self._secret = secret.encode("utf-8") if isinstance(secret, str) else bytes(secret)
        self._allow_gm = allow_gm
        self._skew_seconds = skew_seconds
        self._clock = clock
        self._nonces = _NonceCache()
        self._cache = CommandCache(capacity=cache_capacity)

    def grpc_methods(self) -> dict[str, Callable]:
        return {"GmExecutePlayerCommand": self.execute}

    @property
    def allow_gm(self) -> bool:
        if self._allow_gm is not None:
            return self._allow_gm
        assets = getattr(self.scene, "assets", None)
        return bool(assets is not None and getattr(assets, "allow_gm", False))

    def _resolve_secret(self) -> bytes:
        if self._secret is not None:
            return self._secret
        return os.getenv(GM_PLAYER_COMMAND_SECRET_ENV, "").encode("utf-8")

    def _resolve_skew(self) -> int:
        if self._skew_seconds is not None:
            return self._skew_seconds
        return resolve_gm_skew_seconds()

    def _verify_auth(self, request, now_unix: int, skew_seconds: int) -> tuple[str, str, str]:
        """校验签名信封，返回 ``(result, digest, nonce)``；未通过时不写任何去重表。"""
        secret = self._resolve_secret()
        if len(secret) < MIN_SECRET_BYTES or not secret.strip(_ASCII_SPACE.encode("ascii")):
            return "secret_not_configured", "", ""
        if not has_valid_envelope_fields(request) or request.target_node_id != self.scene.info.node_id:
            return "malformed_envelope", "", ""
        parsed = _parse_envelope(request.auth)
        if parsed is None:
            return "malformed_envelope", "", ""
        operator, timestamp_raw, nonce, signature = parsed
        if operator != getattr(request, "operator", ""):
            return "malformed_envelope", "", ""
        issued_sec = request.issued_at_ms // 1000
        if (
            now_unix < 0
            or timestamp_raw != str(issued_sec)
            or abs(issued_sec - now_unix) > DRAFT_TTL_SECONDS
            or abs(int(timestamp_raw) - now_unix) > skew_seconds
        ):
            return "timestamp_out_of_window", "", ""
        digest = command_digest(request)
        reason = f"gm-player-command-v1\n{digest}"
        canonical = "\n".join((
            GM_PLAYER_COMMAND_METHOD,
            str(self.scene.info.node_id),
            operator,
            timestamp_raw,
            nonce,
            reason,
        )).encode("utf-8")
        expected = hmac.digest(secret, canonical, "sha256").hex()
        if not hmac.compare_digest(expected, signature):
            return "signature_mismatch", "", ""
        return "ok", digest, nonce

    def _response(self, command_id: int, error: int = 0, *, balance_after: int = 0, pet_id: int = 0, executed: bool = False):
        reply = self.protocol.message(
            "scene_node.GmExecutePlayerCommandResponse",
            command_id=command_id,
            balance_after=balance_after,
            pet_id=pet_id,
            executed=executed,
        )
        if error:
            reply.error_message.id = error
        return reply

    def _find_online_player(self, request):
        """检查目标玩家是否在线位于本场景节点且会话身份精确匹配。"""
        world = self.scene.locations.get(request.player_id)
        player = world.players.get(request.player_id) if world is not None else None
        if player is None:
            return None
        if (
            player.disconnect_at is not None
            or self.scene.connection_holds.held(player)
            or not self.scene.mutable(player)
            or (self.scene.battle is not None and self.scene.battle.frozen(player.player_id))
        ):
            return None
        if (
            player.session_id != request.expected_session_id
            or player.generation != request.expected_login_generation
        ):
            return None
        if self.scene.sessions and self.scene.sessions.get((player.gate_uuid, player.session_id)) is not player:
            return None
        return player

    async def execute(self, request):
        """执行单条在线玩家 GM 命令（鉴权失败抛 PermissionError，节点停机抛 GrpcUnavailable）。"""
        if getattr(self.scene, "_closing", False) or getattr(self.scene, "_admission_closed", False):
            raise GrpcUnavailable("scene_node_not_ready")

        now_unix = int(self._clock())
        skew = self._resolve_skew()
        auth_result, digest, nonce = self._verify_auth(request, now_unix, skew)
        if auth_result != "ok":
            logger.warning("GM player command authentication rejected reason=%s", auth_result)
            raise PermissionError(f"GM player command rejected: {auth_result}")

        # 幂等重放先于 nonce 拦截：同一已认证命令携带原签名或新 nonce 重放均返回首次结果。
        status, cached = self._cache.lookup(request, digest, now_unix)
        if status == "replay":
            return cached
        if status == "conflict":
            logger.warning(
                "GM player command rejected command_id=%d reason=command_conflict",
                request.command_id,
            )
            return self._response(request.command_id, FEATURE_UNAVAILABLE)

        if not self._nonces.register(nonce, now_unix, skew * 2 + 1):
            logger.warning("GM player command authentication rejected reason=replayed_nonce")
            raise PermissionError("GM player command rejected: replayed_nonce")

        initial = self._response(request.command_id)
        admission, cached = self._cache.begin(request, digest, initial, now_unix, skew)
        if admission == "replay":
            return cached
        if admission != "new":
            logger.warning(
                "GM player command rejected command_id=%d reason=command_%s",
                request.command_id,
                admission,
            )
            return self._response(request.command_id, FEATURE_UNAVAILABLE)

        operator_text = sanitize_audit_text(getattr(request, "operator", ""))
        reason_text = sanitize_audit_text(request.reason)
        command_kind = request.WhichOneof("command")

        def finish(error: int, *, balance_after: int = 0, pet_id: int = 0):
            reply = self._response(
                request.command_id,
                error,
                balance_after=balance_after,
                pet_id=pet_id,
                executed=(error == 0),
            )
            self._cache.complete(request, reply)
            logger.info(
                "GM player command completed command_id=%d operator=%s player_id=%d command=%s reason=%s error=%d",
                request.command_id,
                operator_text,
                request.player_id,
                command_kind,
                reason_text,
                error,
            )
            return reply

        if not self.allow_gm:
            return finish(FEATURE_UNAVAILABLE)

        async with self.scene._lock(request.player_id):
            player = self._find_online_player(request)
            if player is None:
                return finish(FEATURE_UNAVAILABLE)
            try:
                await self.scene.flush_player_rows(player)
            except (StoreRejected, StoreUnavailable, OSError):
                return finish(UNAVAILABLE)
            # 再次核对玩家状态，防止 flush 期间离场或冻结。
            player = self._find_online_player(request)
            if player is None:
                return finish(FEATURE_UNAVAILABLE)

            logger.info(
                "GM player command executing command_id=%d operator=%s player_id=%d command=%s reason=%s",
                request.command_id,
                operator_text,
                request.player_id,
                command_kind,
                reason_text,
            )
            error, balance_after, pet_id = await self._dispatch(player, request, command_kind)
            return finish(error, balance_after=balance_after, pet_id=pet_id)

    async def _dispatch(self, player, request, command_kind: str | None) -> tuple[int, int, int]:
        assets = getattr(self.scene, "assets", None)
        if command_kind in ("add_currency", "deduct_currency", "block_currency", "unblock_currency", "grant_item"):
            if assets is None:
                return FEATURE_UNAVAILABLE, 0, 0
            data = self.scene.rows.assemble(player.snapshot)
            if command_kind == "add_currency":
                args = request.add_currency
                gains = []
                error = assets.add_currency(data, args.currency_type, args.amount, gains=gains)
                if error:
                    return error, 0, 0
                if not await assets._save(player, data, _TX_GM_GRANT, credit=True, gains=gains):
                    return UNAVAILABLE, 0, 0
                return 0, balance(data, args.currency_type), 0
            if command_kind == "deduct_currency":
                args = request.deduct_currency
                error = assets.deduct_currency(data, args.currency_type, args.amount)
                if error:
                    return error, 0, 0
                if not await assets._save(player, data, _TX_GM_DEDUCT, credit=False):
                    return UNAVAILABLE, 0, 0
                return 0, balance(data, args.currency_type), 0
            if command_kind == "block_currency":
                kind = request.block_currency.currency_type
                if kind not in assets.currency_types:
                    return INVALID, 0, 0
                if kind not in data.currency.blocked_types:
                    data.currency.blocked_types.append(kind)
                if not await assets._save(player, data, 0):
                    return UNAVAILABLE, 0, 0
                return 0, 0, 0
            if command_kind == "unblock_currency":
                kind = request.unblock_currency.currency_type
                if kind not in assets.currency_types:
                    return INVALID, 0, 0
                if kind in data.currency.blocked_types:
                    data.currency.blocked_types.remove(kind)
                if not await assets._save(player, data, 0):
                    return UNAVAILABLE, 0, 0
                return 0, 0, 0
            # grant_item
            args = request.grant_item
            if (
                not 0 < args.count <= 99
                or not args.config_id
                or assets.tables["Item"].get(args.config_id) is None
            ):
                return _EQUIP_GRANT_INVALID, 0, 0
            gains = []
            error = await assets.add_items(data, {args.config_id: args.count}, gains=gains)
            if error:
                return error, 0, 0
            if not await assets._save(player, data, _TX_GM_GRANT, gains=gains):
                return UNAVAILABLE, 0, 0
            return 0, 0, 0

        if command_kind == "grant_pet":
            return await self._grant_pet(player, request.grant_pet.pet_table_id)
        if command_kind == "set_level":
            return await self._set_level(player, request.set_level.level)
        return INVALID, 0, 0

    async def _grant_pet(self, player, pet_table_id: int) -> tuple[int, int, int]:
        pets = getattr(self.scene, "pets", None)
        assets = getattr(self.scene, "assets", None)
        if pets is None or assets is None:
            return FEATURE_UNAVAILABLE, 0, 0
        row = pets.tables["Pet"].get(pet_table_id)
        if row is None:
            return _PET_ROW_MISSING, 0, 0
        if pets.pool is None:
            return INVALID_DATA, 0, 0
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component") or not data.HasField("level_component"):
            return UNAVAILABLE, 0, 0
        comp = data.pet_component
        if len(comp.pets) >= pets.max_pets:
            return _PET_FULL, 0, 0
        if assets.allocate_item_id is None:
            return _PET_ID_UNAVAILABLE, 0, 0
        try:
            pet_id = await assets.allocate_item_id()
        except (OSError, RuntimeError, ValueError):
            return _PET_ID_UNAVAILABLE, 0, 0
        if (
            type(pet_id) is not int
            or not 0 < pet_id < (1 << 63)
            or any(existing.pet_id == pet_id for existing in comp.pets)
        ):
            return _PET_ID_UNAVAILABLE, 0, 0
        pet = comp.pets.add(
            pet_id=pet_id,
            pet_table_id=row.id,
            level=pet_level(effective_level(data), row),
            created_at=int(self._clock()),
        )
        for index, dim in enumerate(pets.dimensions):
            low = row.aptitude_min[index] if index < len(row.aptitude_min) else 10_000
            high = row.aptitude_max[index] if index < len(row.aptitude_max) else 10_000
            low, high = sorted((low or high or 10_000, high or low or 10_000))
            pet.aptitude[dim.id] = low + randbelow(high - low + 1) if low != high else low
        stats = pet_stats(pets.tables, pet)
        pet.health, pet.mana = stats["max_health"], stats["max_mana"]
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("GM pet grant save failed player_id=%d", player.player_id, exc_info=True)
            return UNAVAILABLE, 0, 0
        pets.notify(player)
        return 0, 0, pet_id

    async def _set_level(self, player, level: int) -> tuple[int, int, int]:
        attributes = getattr(self.scene, "attributes", None)
        assets = getattr(self.scene, "assets", None)
        if attributes is None or assets is None:
            return FEATURE_UNAVAILABLE, 0, 0
        if not 1 <= level <= 85:
            return INVALID, 0, 0
        data = self.scene.rows.assemble(player.snapshot)
        if not attributes._present(data):
            return UNAVAILABLE, 0, 0
        old = player_stats(self.protocol, attributes.tables, data)
        before = clone(data) if attributes.missions and assets.audit_enabled else None
        data.level_component.level = level
        attributes._rescale(old, data, level_changed=True)
        pets = getattr(self.scene, "pets", None)
        if pets is not None:
            pets.recalculate_all(data, level_changed=True)
        gains = []
        if attributes.missions is not None:
            stamp = attributes.missions.clock()
            await attributes.missions._event(
                data, attributes.missions.scopes(data)[0], LEVEL, level, level, stamp, gains=gains,
            )
        audit = assets.make_audit_callback(player, before, data, attributes.mission_tx, gains=gains, credit=True)

        async def committed(snapshot):
            if audit is not None:
                result = audit(snapshot)
                if isawaitable(result):
                    await result
            attributes.notify(player)
            if pets is not None:
                pets.notify(player)

        try:
            await self.scene.commit_player_data(player, data, on_committed=committed)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("GM set level save failed player_id=%d", player.player_id, exc_info=True)
            return UNAVAILABLE, 0, 0
        return 0, 0, 0
