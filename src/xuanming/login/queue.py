"""登录排队子系统（对应 Go ``pkg/loginqueue`` 与 ``shared/gatepick``）。

使用控制集群同槽 hash tag ``{q:<zone>}``，Lua 脚本与 Go 版逐字一致，支持与 Go login 混跑共享 Redis。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import inspect
import json
import logging
import random
import secrets
import time
import uuid
from dataclasses import dataclass

from ..nodes.gate import issue_gate_ticket
from . import keys

logger = logging.getLogger(__name__)

# AssignGateResponse.status / QueryQueueStatusResponse.status 枚举值
STATUS_ADMITTED = 0
STATUS_QUEUEING = 1
STATUS_ERROR = 2
STATUS_EXPIRED = 3
STATUS_UNAUTHENTICATED = 4
STATUS_QUEUE_FULL = 5

# 排队轮询间隔与水位常量（与 Go retry_after.go / gatepick 一致）
RETRY_AFTER_FULL_RATE_RANKS = 2000
MAX_RETRY_AFTER_MS = 30_000
FULL_PERCENT = 90
DEFAULT_HANDOFF_GRACE_MS = 20_000
ADMITTED_KEY_GC_SLACK_MS = 60_000
QUEUE_TOKEN_MAX_AGE_S = 24 * 3600

# meta 字段名与前缀
META_FIELD_ZONE_ID = "zone_id"
META_FIELD_ADMITTED_AT = "admitted_at"
META_FIELD_TAKEN_SLOT = "taken_slot"
META_FIELD_RETAKE_UNTIL = "retake_until"
_ACCOUNT_QUEUE_ID_PREFIX = "a-"

# enqueueScript 返回码
ENQUEUE_KEPT = 1
ENQUEUE_ADMIT_WAITING = 2
ENQUEUE_NEW = 3
ENQUEUE_FULL = 4

# lookupScript 返回码
LOOKUP_META_GONE = 1
LOOKUP_ADMIT_TAKEN = 2
LOOKUP_QUEUEING = 3
LOOKUP_QUEUE_GONE = 4
LOOKUP_ADMIT_WINDOW_PASSED = 5
LOOKUP_ADMIT_RETAKEN = 6

# admitEntryScript 返回码
ADMIT_ENTRY_GONE = 1
ADMIT_ENTRY_META_GONE = 2
ADMIT_ENTRY_ALREADY = 3
ADMIT_ENTRY_WRITTEN = 4
ADMIT_ENTRY_REPLAYED = 5
ADMIT_ENTRY_OVER_BUDGET = 6

# Lua 脚本与 Go ``pkg/loginqueue/queue.go`` 逐字对齐，支持双栈混跑
ENQUEUE_SCRIPT = """
if redis.call('EXISTS', KEYS[4]) == 1 then
\tredis.call('SET', KEYS[3], ARGV[1], 'PX', ARGV[3])
\treturn {2, 0, redis.call('ZCARD', KEYS[1])}
end
local queued = redis.call('ZSCORE', KEYS[1], ARGV[1])
local kept = queued and redis.call('TYPE', KEYS[2]).ok == 'hash' and redis.call('HEXISTS', KEYS[2], ARGV[9]) == 0
if not kept then
\tif not queued and tonumber(ARGV[4]) > 0 and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then
\t\treturn {4, 0, 0}
\tend
\tredis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
\tredis.call('DEL', KEYS[2])
\tredis.call('HSET', KEYS[2], 'account', ARGV[5], 'device', ARGV[6], ARGV[8], ARGV[7], 'createdAt', ARGV[2])
end
redis.call('SET', KEYS[3], ARGV[1], 'PX', ARGV[3])
local idle = tonumber(ARGV[3])
for i = 1, 2 do
\tif redis.call('PTTL', KEYS[i]) < idle then
\t\tredis.call('PEXPIRE', KEYS[i], ARGV[3])
\tend
end
local code = 3
if kept then
\tcode = 1
end
return {code, redis.call('ZRANK', KEYS[1], ARGV[1]), redis.call('ZCARD', KEYS[1])}
"""

LOOKUP_SCRIPT = """
if redis.call('GET', KEYS[3]) ~= ARGV[1] then
\treturn {1, 0, 0, '', ''}
end
local slot = redis.call('GETDEL', KEYS[4])
if slot then
\tif redis.call('TYPE', KEYS[1]).ok ~= 'hash' then
\t\treturn {1, 0, 0, '', ''}
\tend
\tredis.call('HSET', KEYS[1], ARGV[7], slot, ARGV[8], ARGV[10])
\treturn {2, 0, 0, slot, redis.call('HGET', KEYS[1], 'account') or ''}
end
if redis.call('TYPE', KEYS[1]).ok ~= 'hash' then
\treturn {1, 0, 0, '', ''}
end
local f = redis.call('HMGET', KEYS[1], ARGV[3], ARGV[4], ARGV[7], ARGV[8], 'account')
if f[1] ~= ARGV[2] then
\treturn {1, 0, 0, '', ''}
end
if f[2] then
\tif f[3] and tonumber(ARGV[9]) < (tonumber(f[4]) or 0) then
\t\treturn {6, 0, 0, f[3], f[5] or ''}
\tend
\treturn {5, 0, 0, '', ''}
end
local rank = redis.call('ZRANK', KEYS[2], ARGV[1])
if not rank then
\treturn {4, 0, 0, '', ''}
end
local idle = tonumber(ARGV[5])
for i = 1, 3 do
\tlocal ttl = redis.call('PTTL', KEYS[i])
\tif ttl ~= -2 and ttl < idle then
\t\tredis.call('PEXPIRE', KEYS[i], ARGV[6])
\tend
end
return {3, rank, redis.call('ZCARD', KEYS[2]), '', ''}
"""

ADMIT_ENTRY_SCRIPT = """
if not redis.call('ZSCORE', KEYS[4], ARGV[6]) then
\tif redis.call('TYPE', KEYS[2]).ok == 'hash' and redis.call('HGET', KEYS[2], ARGV[3]) == ARGV[4] then
\t\treturn 5
\tend
\treturn 1
end
if redis.call('TYPE', KEYS[2]).ok ~= 'hash' then
\tredis.call('ZREM', KEYS[4], ARGV[6])
\treturn 2
end
if redis.call('HEXISTS', KEYS[2], ARGV[3]) == 1 then
\tredis.call('ZREM', KEYS[4], ARGV[6])
\treturn 3
end
if redis.call('ZCOUNT', KEYS[3], '(' .. ARGV[8], '+inf') >= tonumber(ARGV[9]) then
\treturn 6
end
redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[5], ARGV[6])
if redis.call('PTTL', KEYS[3]) < tonumber(ARGV[7]) then
\tredis.call('PEXPIRE', KEYS[3], ARGV[7])
end
redis.call('ZREM', KEYS[4], ARGV[6])
return 4
"""

ADMITTED_HOLD_SCRIPT = """
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[3]) then
\tredis.call('PEXPIRE', KEYS[1], ARGV[3])
end
return 1
"""

ADMITTED_COUNT_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
return redis.call('ZCARD', KEYS[1])
"""

RESERVE_FAST_PATH_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if not redis.call('ZSCORE', KEYS[1], ARGV[3]) and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[3])
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[5]) then
\tredis.call('PEXPIRE', KEYS[1], ARGV[5])
end
return 1
"""


class QueueFullError(Exception):
    """本区排队人数已达配置上限。"""


class QueueTokenError(Exception):
    """排队令牌格式非法、验签失败或已过绝对有效期。"""


@dataclass(slots=True)
class GateCandidate:
    """挑 gate 所需的最小候选视图（对应 Go ``loginqueue.GateCandidate``）。"""

    node_id: int
    ip: str
    port: int
    zone_id: int
    player_count: int = 0
    max_connections: int = 0
    stale: bool = False
    draining: bool = False
    inflight: int = 0
    ws_url: str = ""
    build: str = ""
    instance_id: str = ""

    @property
    def load(self) -> int:
        return self.player_count + self.inflight


@dataclass(frozen=True, slots=True)
class AdmitSlot:
    """放行槽内容（仅含选定 gate，不含签名；客户端领票时现签）。"""

    node_id: int
    ip: str
    port: int
    zone_id: int
    ws_url: str = ""

    def to_json(self) -> str:
        data: dict[str, object] = {
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "zone_id": self.zone_id,
        }
        if self.ws_url:
            data["ws_url"] = self.ws_url
        return json.dumps(data, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str | bytes) -> AdmitSlot:
        data = json.loads(raw)
        return cls(
            node_id=int(data["node_id"]),
            ip=str(data["ip"]),
            port=int(data["port"]),
            zone_id=int(data["zone_id"]),
            ws_url=str(data.get("ws_url", "")),
        )


@dataclass(frozen=True, slots=True)
class AdmitToken:
    """签发给客户端连接 gate 的完整票据。"""

    ip: str
    port: int
    token_payload: bytes
    token_signature: bytes
    token_deadline: int
    ws_url: str = ""


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    queue_id: str
    queue_token: str
    rank: int
    total: int
    rejoined: bool = False


@dataclass(frozen=True, slots=True)
class QueueState:
    status: int
    admit: AdmitToken | None = None
    rank: int = 0
    total: int = 0


# -- 候选分档、容量计算与挑选（Go ``shared/gatepick`` + ``capacity.go``）---------------------------


def is_gate_full(player_count: int, max_connections: int) -> bool:
    """按整数 90% 水位判断 gate 是否满载（不引入浮点舍入）。"""
    return max_connections > 0 and player_count * 100 >= max_connections * FULL_PERCENT


def gate_verdict(candidate: GateCandidate) -> str:
    """返回候选 gate 的不合格原因；合格返回空串。"""
    if candidate.stale:
        return "stale"
    if candidate.draining:
        return "draining"
    if is_gate_full(candidate.player_count, candidate.max_connections):
        return "full"
    return ""


def eligible_gates(candidates: list[GateCandidate]) -> list[GateCandidate]:
    """三档择优：完全合格 -> 仅未停写（含排空/满载）-> 全部候选。"""
    if not candidates:
        return []
    healthy = [c for c in candidates if gate_verdict(c) == ""]
    if healthy:
        return healthy
    reachable = [c for c in candidates if not c.stale]
    if reachable:
        return reachable
    return list(candidates)


def watermark_capacity(max_connections: int) -> int:
    """单台 gate 达到 90% 水位时的可容纳人数（向上取整）。"""
    if max_connections <= 0:
        return 0
    return (max_connections * FULL_PERCENT + 99) // 100


def admission_capacity(candidates: list[GateCandidate]) -> tuple[int, bool]:
    """计算当前在接客档位 gate 到水位为止的总承载；返回 ``(capacity, bounded)``。"""
    if not candidates:
        return 0, True
    serving = [c for c in candidates if not c.stale and not c.draining]
    if serving:
        group = serving
    else:
        reachable = [c for c in candidates if not c.stale]
        group = reachable if reachable else candidates
    total = 0
    for c in group:
        if c.max_connections <= 0:
            return 0, False
        total += watermark_capacity(c.max_connections)
    return total, True


def candidates_from_nodes(nodes, zone_id: int | None = None, *, draining: dict[int, bool] | None = None) -> list[GateCandidate]:
    """将 etcd ``NodeInfo`` 列表按客户端地址去重（最新 ``launch_time`` 胜出）并转为 ``GateCandidate``。"""
    by_address: dict[tuple[str, int], object] = {}
    for info in nodes:
        if zone_id is not None and info.zone_id != zone_id:
            continue
        endpoint = info.client_endpoint if info.HasField("client_endpoint") else info.endpoint
        address = (endpoint.ip, endpoint.port)
        if endpoint.ip and endpoint.port and (
            address not in by_address or info.launch_time > by_address[address].launch_time
        ):
            by_address[address] = info
    draining = draining or {}
    out: list[GateCandidate] = []
    for info in by_address.values():
        endpoint = info.client_endpoint if info.HasField("client_endpoint") else info.endpoint
        out.append(GateCandidate(
            node_id=int(info.node_id),
            ip=endpoint.ip,
            port=int(endpoint.port),
            zone_id=int(info.zone_id),
            player_count=int(info.player_count),
            max_connections=int(info.max_connections),
            stale=bool(getattr(info, "stale", False)),
            draining=bool(draining.get(int(info.node_id), False)),
            ws_url=str(getattr(info, "client_ws_url", "")),
            build=str(getattr(info, "build", "")),
            instance_id=str(getattr(info, "node_uuid", "")),
        ))
    return out


def two_choices(candidates: list[GateCandidate], *, rng=None) -> GateCandidate | None:
    """Power of Two Choices：随机抽两台不同候选，取负载较轻者（同负载取首次抽中）。"""
    n = len(candidates)
    if n == 0:
        return None
    if n == 1:
        return candidates[0]
    if rng is None:
        first, second = random.sample(range(n), 2)
    else:
        first = rng(n)
        second = rng(n - 1)
        if second >= first:
            second += 1
    a, b = candidates[first], candidates[second]
    return b if b.load < a.load else a


def pick_gate(candidates: list[GateCandidate], *, rng=None) -> GateCandidate | None:
    """分档后随机二选一挑选 gate。"""
    return two_choices(eligible_gates(candidates), rng=rng)


def ticket_account_digest(account: str) -> bytes:
    """计算 gate 票据绑定账号的 16 字节 SHA-256 摘要（空串返回空字节，表示不绑定账号）。"""
    if not account:
        return b""
    return hashlib.sha256(f"gate-ticket-account|{account}".encode("utf-8")).digest()[:16]


def ticket_issued_to(digest: bytes | bytearray, account: str) -> bool:
    """判断 gate 票据是否允许该账号使用：未绑摘要的票放行，绑了摘要则须常数时间比对一致。"""
    if not digest:
        return True
    want = ticket_account_digest(account)
    return bool(want) and hmac.compare_digest(bytes(digest), want)


def sign_gate_token(
    protocol,
    gate: GateCandidate,
    secret: bytes,
    ttl_s: int,
    *,
    player_id: int = 0,
    account: str = "",
) -> AdmitToken:
    """为指定 gate 签发 ``GateTokenPayload`` 与 HMAC-SHA256 签名（非空 ``account`` 时绑定持票账号摘要）。"""
    if account:
        deadline = int(time.time()) + int(ttl_s)
        payload = protocol.message(
            "GateTokenPayload",
            gate_node_id=gate.node_id,
            zone_id=gate.zone_id,
            expire_timestamp=deadline,
            player_id=player_id,
            hmac_session_key=secrets.token_bytes(32),
            account_digest=ticket_account_digest(account),
        ).SerializeToString()
        signature = hmac.new(secret, payload, "sha256").hexdigest().encode()
        return AdmitToken(
            ip=gate.ip,
            port=gate.port,
            ws_url=gate.ws_url,
            token_payload=payload,
            token_signature=signature,
            token_deadline=deadline,
        )
    ticket = issue_gate_ticket(protocol, gate.node_id, gate.zone_id, secret, ttl=ttl_s, player_id=player_id)
    deadline = protocol.parse("GateTokenPayload", ticket.payload).expire_timestamp
    return AdmitToken(
        ip=gate.ip,
        port=gate.port,
        ws_url=gate.ws_url,
        token_payload=ticket.payload,
        token_signature=ticket.signature,
        token_deadline=deadline,
    )


def retry_after_for_rank(base_ms: int, rank: int, admit_ttl_ms: int) -> int:
    """按 0 基名次线性拉长轮询间隔，上限 ``min(30000, admit_ttl/2)``，下限 ``base_ms``。"""
    if base_ms <= 0 or rank < RETRY_AFTER_FULL_RATE_RANKS:
        return base_ms
    limit = MAX_RETRY_AFTER_MS
    half = admit_ttl_ms // 2
    if half < limit:
        limit = half
    scaled = (base_ms * (rank + 1)) // RETRY_AFTER_FULL_RATE_RANKS
    if scaled > limit:
        scaled = limit
    if scaled < base_ms:
        scaled = base_ms
    return scaled


def retry_after_when_full() -> int:
    """队列已满时返回 30~40 秒随机退避间隔，打散重试波峰。"""
    return MAX_RETRY_AFTER_MS + random.randrange(MAX_RETRY_AFTER_MS // 3)


# -- 排队令牌 HMAC 签发与验签（与 Go ``signQueueToken`` / ``ParseAndVerifyQueueTokenMulti`` 对齐）--


def _hmac_hex(secret: bytes, data: bytes) -> bytes:
    return hmac.new(secret, data, hashlib.sha256).hexdigest().encode("ascii")


def queue_id_for_account(secret: bytes, zone_id: int, account: str) -> str:
    """同一账号在同一区恒得同一个确定性条目号，避免重复排队与日志泄露账号原文。"""
    digest = _hmac_hex(secret, f"queue-entry|{zone_id}|{account}".encode("utf-8"))
    return _ACCOUNT_QUEUE_ID_PREFIX + digest[:32].decode("ascii")


def sign_queue_token(secret: bytes, queue_id: str, zone_id: int, expire_ts: int) -> str:
    body = json.dumps(
        {"k": "queue", "q": queue_id, "z": zone_id, "e": expire_ts},
        separators=(",", ":"),
    ).encode("utf-8")
    sig = _hmac_hex(secret, body)
    return base64.urlsafe_b64encode(body + b"." + sig).rstrip(b"=").decode("ascii")


def _split_queue_token(token: str) -> tuple[bytes, bytes]:
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
    except ValueError as error:
        raise QueueTokenError("decode queue token failed") from error
    dot = raw.rfind(b".")
    if dot <= 0 or dot == len(raw) - 1:
        raise QueueTokenError("malformed queue token")
    return raw[:dot], raw[dot + 1:]


def verify_queue_token(secrets_: bytes | list[bytes] | tuple[bytes, ...], token: str, *,
                       now_s: int | None = None) -> tuple[str, int]:
    """用单密钥或多密钥候选验证排队令牌，返回 ``(queue_id, zone_id)``；失败抛 ``QueueTokenError``。"""
    secret_list = [secrets_] if isinstance(secrets_, (bytes, bytearray)) else list(secrets_)
    if not secret_list or not any(secret_list):
        raise QueueTokenError("queue token secret not configured")
    body_bytes, sig = _split_queue_token(token)
    # 遍历全部候选密钥不短路，避免耗时侧信道泄露命中的密钥位置
    matched = False
    for sec in secret_list:
        if sec and hmac.compare_digest(sig, _hmac_hex(sec, body_bytes)):
            matched = True
    if not matched:
        raise QueueTokenError("queue token signature mismatch")
    try:
        body = json.loads(body_bytes)
    except ValueError as error:
        raise QueueTokenError("unmarshal queue token failed") from error
    if body.get("k") != "queue":
        raise QueueTokenError("queue token wrong kind")
    now = int(time.time()) if now_s is None else now_s
    if now > int(body.get("e", 0)):
        raise QueueTokenError("queue token expired")
    queue_id = str(body.get("q", ""))
    zone_id = int(body.get("z", 0))
    if not queue_id or zone_id <= 0:
        raise QueueTokenError("queue token missing identity")
    return queue_id, zone_id


def _text(value) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else "" if value is None else str(value)


class LoginQueue:
    """基于控制域 Redis ZSET 的 FIFO 登录排队器（对应 Go ``loginqueue.Queue``）。"""

    def __init__(
        self,
        redis,
        hmac_secret: bytes,
        *,
        entry_ttl_s: int = 3600,
        admit_ttl_s: int = 60,
        max_length: int = 1_000_000,
        handoff_grace_ms: int = DEFAULT_HANDOFF_GRACE_MS,
        clock_ms=None,
    ) -> None:
        if not 1 <= entry_ttl_s <= QUEUE_TOKEN_MAX_AGE_S:
            raise ValueError("loginqueue: queue entry ttl must be in [1s, 24h]")
        if admit_ttl_s <= 0:
            raise ValueError("loginqueue: admit ttl must be positive")
        self.redis = redis
        self.hmac_secret = hmac_secret
        self.entry_ttl_ms = entry_ttl_s * 1000
        self.admit_ttl_ms = admit_ttl_s * 1000
        self.max_length = max_length
        self.handoff_grace_ms = handoff_grace_ms
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    @property
    def entry_renew_ttl_ms(self) -> int:
        return self.entry_ttl_ms + self.entry_ttl_ms // 4

    @property
    def admitted_key_ttl_ms(self) -> int:
        return max(self.admit_ttl_ms, self.handoff_grace_ms) + ADMITTED_KEY_GC_SLACK_MS

    async def _eval(self, script: str, keys_: list[str], *args):
        return await self.redis.eval(script, len(keys_), *keys_, *args)

    async def queue_len(self, zone_id: int) -> int:
        return int(await self.redis.zcard(keys.queue_zset_key(zone_id)))

    async def admitted_count(self, zone_id: int) -> int:
        return int(await self._eval(ADMITTED_COUNT_SCRIPT, [keys.admitted_zset_key(zone_id)], self.clock_ms()))

    async def try_reserve_fast_path_slot(self, zone_id: int, budget: int, account: str = "") -> bool:
        """快速通道原子占位：同账号在途仅占一枚确定性名额（防单账号耗尽全区名额），匿名回落 ``fast:<uuid>``。"""
        if budget <= 0:
            return False
        now_ms = self.clock_ms()
        member = queue_id_for_account(self.hmac_secret, zone_id, account) if account else f"fast:{uuid.uuid4()}"
        result = int(await self._eval(
            RESERVE_FAST_PATH_SCRIPT,
            [keys.admitted_zset_key(zone_id)],
            now_ms, budget, member, now_ms + self.handoff_grace_ms, self.admitted_key_ttl_ms,
        ))
        return result == 1

    async def hold_reconnect_slot(self, zone_id: int, player_id: int) -> None:
        """重连/顶号免排队占位：连上 gate 上报在线前占住一个在途名额。"""
        now_ms = self.clock_ms()
        await self._eval(
            ADMITTED_HOLD_SCRIPT,
            [keys.admitted_zset_key(zone_id)],
            now_ms + self.handoff_grace_ms, f"reconnect:{player_id}", self.admitted_key_ttl_ms,
        )

    async def enqueue(self, zone_id: int, account: str = "", device_id: str = "") -> EnqueueResult:
        """入队并返回排队令牌、0 基名次与总人数；同一账号同区复用原条目。"""
        queue_id = queue_id_for_account(self.hmac_secret, zone_id, account) if account else str(uuid.uuid4())
        now_ms = self.clock_ms()
        token = sign_queue_token(self.hmac_secret, queue_id, zone_id, now_ms // 1000 + QUEUE_TOKEN_MAX_AGE_S)
        reply = await self._eval(
            ENQUEUE_SCRIPT,
            [
                keys.queue_zset_key(zone_id),
                keys.queue_meta_key(zone_id, queue_id),
                keys.queue_token_index_key(zone_id, token),
                keys.admit_key(zone_id, queue_id),
            ],
            queue_id, now_ms, self.entry_ttl_ms, self.max_length,
            account, device_id, zone_id, META_FIELD_ZONE_ID, META_FIELD_ADMITTED_AT,
        )
        code, rank, total = int(reply[0]), int(reply[1]), int(reply[2])
        if code == ENQUEUE_FULL:
            raise QueueFullError(f"zone {zone_id} queue is full")
        if code not in (ENQUEUE_KEPT, ENQUEUE_ADMIT_WAITING, ENQUEUE_NEW):
            raise RuntimeError(f"enqueue script returned unknown code {code}")
        return EnqueueResult(
            queue_id=queue_id,
            queue_token=token,
            rank=rank,
            total=total,
            rejoined=(code != ENQUEUE_NEW),
        )

    async def poll(self, token: str, zone_id: int, sign_fn) -> QueueState:
        """轮询排队状态：仍排队则续期返回名次，已放行则取走放行槽并现签 gate 票。"""
        if zone_id <= 0:
            return QueueState(status=STATUS_EXPIRED)
        try:
            body_bytes, _ = _split_queue_token(token)
            queue_id = str(json.loads(body_bytes).get("q", ""))
        except Exception:
            return QueueState(status=STATUS_EXPIRED)
        if not queue_id:
            return QueueState(status=STATUS_EXPIRED)
        now_ms = self.clock_ms()
        reply = await self._eval(
            LOOKUP_SCRIPT,
            [
                keys.queue_meta_key(zone_id, queue_id),
                keys.queue_zset_key(zone_id),
                keys.queue_token_index_key(zone_id, token),
                keys.admit_key(zone_id, queue_id),
            ],
            queue_id, str(zone_id), META_FIELD_ZONE_ID, META_FIELD_ADMITTED_AT,
            self.entry_ttl_ms, self.entry_renew_ttl_ms,
            META_FIELD_TAKEN_SLOT, META_FIELD_RETAKE_UNTIL, now_ms, now_ms + self.handoff_grace_ms,
        )
        code = int(reply[0])
        if code in (LOOKUP_ADMIT_TAKEN, LOOKUP_ADMIT_RETAKEN):
            slot = AdmitSlot.from_json(_text(reply[3]))
            account = _text(reply[4]) if len(reply) > 4 else ""
            if len(inspect.signature(sign_fn).parameters) >= 2:
                admit = await sign_fn(slot, account)
            else:
                admit = await sign_fn(slot)
            # 票交出后延长在途占位至 handoff_grace，避免在 gate 上报人数前被重复放行
            try:
                await self._eval(
                    ADMITTED_HOLD_SCRIPT,
                    [keys.admitted_zset_key(zone_id)],
                    self.clock_ms() + self.handoff_grace_ms, queue_id, self.admitted_key_ttl_ms,
                )
            except Exception:
                logger.exception("Hold admitted placeholder failed zone=%d queue_id=%s", zone_id, queue_id)
            return QueueState(status=STATUS_ADMITTED, admit=admit)
        if code == LOOKUP_QUEUEING:
            return QueueState(status=STATUS_QUEUEING, rank=int(reply[1]), total=int(reply[2]))
        return QueueState(status=STATUS_EXPIRED)

    async def pop_admit(self, zone_id: int, limit: int, budget: int, pick_fn) -> int:
        """放行队首最多 ``limit`` 条记录（受在途名额 ``budget`` 原子约束），返回本批放行条数。"""
        if limit <= 0 or budget <= 0:
            return 0
        members = await self.redis.zrange(keys.queue_zset_key(zone_id), 0, limit - 1)
        admitted = 0
        for raw_member in members:
            queue_id = _text(raw_member)
            slot = pick_fn(zone_id, queue_id)
            if slot is None:
                continue
            now_ms = self.clock_ms()
            result = int(await self._eval(
                ADMIT_ENTRY_SCRIPT,
                [
                    keys.admit_key(zone_id, queue_id),
                    keys.queue_meta_key(zone_id, queue_id),
                    keys.admitted_zset_key(zone_id),
                    keys.queue_zset_key(zone_id),
                ],
                slot.to_json(), self.admit_ttl_ms, META_FIELD_ADMITTED_AT, now_ms,
                now_ms + self.admit_ttl_ms, queue_id, self.admitted_key_ttl_ms,
                now_ms, budget,
            ))
            if result in (ADMIT_ENTRY_WRITTEN, ADMIT_ENTRY_REPLAYED):
                admitted += 1
            elif result == ADMIT_ENTRY_OVER_BUDGET:
                break
        return admitted


async def free_slots(
    candidates: list[GateCandidate],
    zone_capacity: int,
    queue: LoginQueue,
    zone_id: int,
    soft_cap_multiplier: float = 1.2,
) -> tuple[int, int, int, int]:
    """计算区当前空余准入名额，返回 ``(free, capacity, online, admitted)``。"""
    if not candidates:
        raise RuntimeError(f"no gates for zone {zone_id}")
    online = sum(c.player_count for c in candidates)
    capacity = zone_capacity
    if capacity <= 0:
        base = max(online, 1)
        mult = max(soft_cap_multiplier, 1.0)
        capacity = int(base * mult)
    gate_cap, bounded = admission_capacity(candidates)
    if bounded and gate_cap < capacity:
        capacity = gate_cap
    admitted = await queue.admitted_count(zone_id)
    used = online + admitted
    free = max(0, capacity - used)
    return free, capacity, online, admitted
