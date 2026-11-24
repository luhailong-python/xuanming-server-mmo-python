"""Gate 排空监控、停写标记与排空会话迁移（对应 Go ``gatedrain*.go`` / ``gatestale.go`` / ``svc/gate_migration.go``）。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from ..cluster import GATE
from . import keys, login_session, session_fence
from .queue import (
    GateCandidate,
    candidates_from_nodes,
    gate_verdict,
    sign_gate_token,
    two_choices,
)

logger = logging.getLogger(__name__)

# GateMigrationResult 枚举值（与 proto/login/login.proto 一致）
MIGRATION_UNSPECIFIED = 0
MIGRATION_OK = 1
MIGRATION_NOT_DRAINING = 2
MIGRATION_NO_TARGET = 3
MIGRATION_DISABLED = 4

# drained 标记的判定理由
DRAIN_REASON_BELOW_THRESHOLD = "below_threshold"
DRAIN_REASON_DEADLINE = "deadline"

# 停写阈值与 Redis 标记过期时长（三个 10s 上报周期）
REPORT_PERIOD_S = 10
STALE_AFTER_S = 3 * REPORT_PERIOD_S
STALE_MARK_TTL_S = STALE_AFTER_S


@dataclass(frozen=True, slots=True)
class GateDrainPolicy:
    drained_below_players: int = 0
    deadline_seconds: int = 0


@dataclass(frozen=True, slots=True)
class GateDrainState:
    node_id: int
    online: int
    drained: bool
    reason: str
    waited_sec: int


@dataclass(frozen=True, slots=True)
class GateOnline:
    node_id: int
    player_count: int
    instance_id: str = ""


@dataclass(frozen=True, slots=True)
class DrainMigratePolicy:
    enabled: bool = False
    rate_per_second: int = 50
    start_after_seconds: int = 0
    drained_below_players: int = 0
    lease_ms: int = 30_000

    @property
    def active(self) -> bool:
        return self.enabled and self.rate_per_second > 0 and self.lease_ms > 0


@dataclass(frozen=True, slots=True)
class DrainMigrationLease:
    node_id: int
    instance_id: str
    drain_marked_at: int
    online: int
    rate_per_second: int
    lease_ms: int


def _text(value) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else "" if value is None else str(value)


# -- 排空与停写 Redis 标记读写 ----------------------------------------------------------------------


async def mark_gate_draining(redis, zone_id: int, node_id: int, *, now_unix: int | None = None,
                             ttl_seconds: int = 3600) -> None:
    """将指定 gate 标记为排空中；强制带 TTL 防止缩容脚本挂死导致容量永久消失。"""
    if redis is None:
        raise ValueError("gate drain: nil redis client")
    if ttl_seconds <= 0:
        ttl_seconds = 3600
    ts = int(time.time()) if now_unix is None else now_unix
    await redis.set(keys.gate_draining_key(zone_id, node_id), str(ts), ex=ttl_seconds)


async def clear_gate_draining(redis, zone_id: int, node_id: int) -> None:
    """清除 gate 排空标记（取消缩容或排空完成）。"""
    if redis is None:
        raise ValueError("gate drain: nil redis client")
    await redis.delete(keys.gate_draining_key(zone_id, node_id))


async def draining_gates(redis, zone_id: int, node_ids: list[int]) -> dict[int, bool]:
    """登录选路用的排空查询（fail-open：Redis 失败返回空映射，不阻断玩家登录）。"""
    if redis is None or not node_ids:
        return {}
    try:
        return await draining_gate_set(redis, zone_id, node_ids)
    except Exception:
        logger.exception("Query draining gates failed; treating all gates as available zone=%d", zone_id)
        return {}


async def draining_gate_set(redis, zone_id: int, node_ids: list[int]) -> dict[int, bool]:
    """迁移签票用的排空查询（fail-closed：Redis 失败向上抛错，绝不盲迁）。"""
    if redis is None:
        raise RuntimeError("gate drain: nil redis client")
    if not node_ids:
        return {}
    redis_keys = [keys.gate_draining_key(zone_id, nid) for nid in node_ids]
    values = await redis.mget(*redis_keys)
    return {nid: True for nid, val in zip(node_ids, values) if val is not None}


async def publish_stale_marks(redis, zone_id: int, node_ids: list[int]) -> None:
    """为停写 gate 写入 Redis 停写标记（幂等 SET + TTL，不主动删，恢复后自然过期）。"""
    if redis is None or not node_ids:
        return
    async with redis.pipeline(transaction=False) as pipe:
        for nid in node_ids:
            pipe.set(keys.gate_stale_key(zone_id, nid), "1", ex=STALE_MARK_TTL_S)
        await pipe.execute()


# -- 排空判定与迁移许可计算（纯函数 + 单轮评估）------------------------------------------------------


def evaluate_gate_drain(online: int, marked_at_unix: int, now_unix: int, policy: GateDrainPolicy) -> GateDrainState:
    """判断单台排空中 gate 是否已满足可安全缩容条件（纯函数）。"""
    waited = max(0, now_unix - marked_at_unix)
    if online <= policy.drained_below_players:
        return GateDrainState(0, online, True, DRAIN_REASON_BELOW_THRESHOLD, waited)
    if policy.deadline_seconds > 0 and waited >= policy.deadline_seconds:
        return GateDrainState(0, online, True, DRAIN_REASON_DEADLINE, waited)
    return GateDrainState(0, online, False, "", waited)


async def evaluate_draining_gates(
    redis,
    zone_id: int,
    gates: list[GateOnline],
    policy: GateDrainPolicy,
    *,
    now_unix: int | None = None,
) -> list[GateDrainState]:
    """跑一轮排空判定：已排空则按 draining 剩余 TTL 写入 drained 标记，非排空则清理残留。"""
    if redis is None or not gates:
        return []
    now = int(time.time()) if now_unix is None else now_unix
    drained: list[GateDrainState] = []
    for gate in gates:
        draining_key = keys.gate_draining_key(zone_id, gate.node_id)
        drained_key = keys.gate_drained_key(zone_id, gate.node_id)
        raw = await redis.get(draining_key)
        if not raw:
            await redis.delete(drained_key)
            continue
        try:
            marked_at = int(_text(raw))
        except ValueError:
            marked_at = 0
        state = evaluate_gate_drain(gate.player_count, marked_at, now, policy)
        state = GateDrainState(gate.node_id, state.online, state.drained, state.reason, state.waited_sec)
        if not state.drained:
            continue
        # drained 寿命不得长于 draining；无正 TTL 时跳过写入（fail-safe）
        ttl_s = int(await redis.ttl(draining_key))
        if ttl_s == -2:
            await redis.delete(drained_key)
            continue
        if ttl_s <= 0:
            logger.error("Gate draining mark has no positive TTL; refusing to write drained mark gate=%d", gate.node_id)
            continue
        await redis.set(drained_key, state.reason, ex=ttl_s)
        if state.reason == DRAIN_REASON_DEADLINE:
            logger.error("Gate marked drained by deadline gate=%d online=%d waited=%ds",
                         gate.node_id, state.online, state.waited_sec)
        else:
            logger.info("Gate marked drained gate=%d online=%d threshold=%d",
                        gate.node_id, state.online, policy.drained_below_players)
        drained.append(state)
    return drained


def decide_drain_migration(gate: GateOnline, marked_at_raw: str, now_unix: int,
                           policy: DrainMigratePolicy) -> DrainMigrationLease | None:
    """判断这一拍是否需要给排空中 gate 下发迁移许可（纯函数）。"""
    if not policy.active or not marked_at_raw or not gate.instance_id:
        return None
    if gate.player_count <= policy.drained_below_players:
        return None
    try:
        marked_at = int(marked_at_raw)
    except ValueError:
        marked_at = 0
    waited = max(0, now_unix - marked_at)
    if waited < policy.start_after_seconds:
        return None
    return DrainMigrationLease(
        node_id=gate.node_id,
        instance_id=gate.instance_id,
        drain_marked_at=marked_at,
        online=gate.player_count,
        rate_per_second=policy.rate_per_second,
        lease_ms=policy.lease_ms,
    )


async def drain_migrations_due(
    redis,
    zone_id: int,
    gates: list[GateOnline],
    policy: DrainMigratePolicy,
    *,
    now_unix: int | None = None,
) -> list[DrainMigrationLease]:
    """一次 MGET 读全部排空标记，返回本拍应发迁移许可的 gate 列表。"""
    if redis is None or not gates or not policy.active:
        return []
    now = int(time.time()) if now_unix is None else now_unix
    redis_keys = [keys.gate_draining_key(zone_id, g.node_id) for g in gates]
    try:
        values = await redis.mget(*redis_keys)
    except Exception:
        logger.exception("Read draining marks for migration failed; skipping leases zone=%d", zone_id)
        return []
    due: list[DrainMigrationLease] = []
    for gate, raw in zip(gates, values):
        text = _text(raw) if raw is not None else ""
        lease = decide_drain_migration(gate, text, now, policy)
        if lease is not None:
            due.append(lease)
    return due


# -- RequestGateMigration 目标选择与签票（对应 Go ``PickMigrationTarget`` + ``SignGateMigration``）----


def pick_migration_target(
    candidates: list[GateCandidate],
    draining: dict[int, bool],
    caller_node_id: int,
    *,
    rng=None,
) -> GateCandidate | None:
    """为排空中 gate 的连接选择目标 gate：剔除自身/排空/停写/满载，优先非外出构建，**不回落**。"""
    targets = [
        c for c in candidates
        if c.node_id != caller_node_id and not draining.get(c.node_id, False) and gate_verdict(c) == ""
    ]
    if not targets:
        return None
    outgoing = {c.build for c in candidates if c.build and draining.get(c.node_id, False)}
    if outgoing:
        preferred = [c for c in targets if not c.build or c.build not in outgoing]
        if preferred:
            targets = preferred
    return two_choices(targets, rng=rng)


def _gate_registered_in_zone(nodes, zone_id: int, node_id: int, instance_id: str) -> bool:
    if not node_id or not instance_id:
        return False
    return any(
        n.zone_id == zone_id and n.node_id == node_id and n.node_uuid == instance_id
        for n in nodes
    )


def plan_gate_migration(
    nodes,
    zone_id: int,
    caller_node_id: int,
    caller_instance_id: str,
    draining: dict[int, bool],
    *,
    rng=None,
) -> tuple[GateCandidate | None, int]:
    """签票前纯判定：核对调用方属于本区且正在排空，再选出合格目标 gate。"""
    if not _gate_registered_in_zone(nodes, zone_id, caller_node_id, caller_instance_id) or not draining.get(caller_node_id):
        return None, MIGRATION_NOT_DRAINING
    candidates = candidates_from_nodes(nodes, zone_id, draining=draining)
    target = pick_migration_target(candidates, draining, caller_node_id, rng=rng)
    if target is None:
        return None, MIGRATION_NO_TARGET
    return target, MIGRATION_OK


async def sign_gate_migration(ctx, details):
    """处理 ``RequestGateMigration``：校验排空状态、登录态、目标 gate 并签发迁移票据。"""
    protocol, config = ctx.protocol, ctx.config
    if not config.gate_drain_migrate_enabled:
        return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_DISABLED)
    if not config.gate_token_secret:
        logger.error("Gate migration rejected: gate_token_secret not configured")
        return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_UNSPECIFIED)
    conn = session_fence.connection_of(details)
    if conn is None or not details.gate_node_id:
        logger.error("Gate migration rejected: incomplete gate connection identity")
        return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_UNSPECIFIED)
    control_redis = ctx.control_redis if ctx.control_redis is not None else ctx.login_redis
    if ctx.directory is None or control_redis is None:
        return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_UNSPECIFIED)

    # 选角界面（player_id == 0）必须确认存在活登录态，防止绕过排队获取无绑票据
    if not details.player_id:
        try:
            account = await login_session.get_account(ctx.login_redis, conn)
        except Exception:
            logger.exception("Gate migration read login session failed gate=%d", details.gate_node_id)
            return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_UNSPECIFIED)
        if account is None:
            return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_UNSPECIFIED)

    try:
        nodes = await ctx.directory.candidates(GATE, config.zone_id)
    except Exception:
        logger.exception("Gate migration read gate directory failed zone=%d", config.zone_id)
        return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_UNSPECIFIED)

    zone_node_ids = list(dict.fromkeys(int(n.node_id) for n in nodes if n.zone_id == config.zone_id))
    try:
        draining = await draining_gate_set(control_redis, config.zone_id, zone_node_ids)
    except Exception:
        logger.exception("Gate migration read draining marks failed zone=%d", config.zone_id)
        return protocol.message("loginpb.GateMigrationResponse", result=MIGRATION_UNSPECIFIED)

    target, result = plan_gate_migration(
        nodes, config.zone_id, int(details.gate_node_id), details.gate_instance_id, draining,
    )
    if result != MIGRATION_OK or target is None:
        return protocol.message("loginpb.GateMigrationResponse", result=result)

    # target_zone_id 恒为 0：让到达后的 EnterGame 走租约内重连，回到原场景
    admit = sign_gate_token(
        protocol, target, config.gate_token_secret, config.gate_migration_token_ttl,
        player_id=int(details.player_id),
    )
    return protocol.message(
        "loginpb.GateMigrationResponse",
        result=MIGRATION_OK,
        target_gate_ip=admit.ip,
        target_gate_port=admit.port,
        target_gate_ws_url=admit.ws_url,
        token_payload=admit.token_payload,
        token_signature=admit.token_signature,
        token_deadline=admit.token_deadline,
    )
