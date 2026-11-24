"""两域 Redis 键构造器与归属代次/标记/修订标签解析器。

严格区分两个 Redis 域，禁止在单段 Lua 脚本中混用：
- SceneControlRedis（控制域）：hash tag 为 ``{ctl:<zone>}`` 或 ``{gm:<zone>}``；
- PlayerRedis（玩家域）：hash tag 为 ``{<player_id>}``。

所有键名格式与 Go ``shared/redisslot``、``shared/ownerepoch``、``shared/handofffence`` 逐字节一致。
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass

from .errors import CrossDomainRedisKeyError

PENDING_ROLLBACK_RECEIPT_PREFIX = "pending:"
HANDOFF_FENCE_FIELD = "handoff_fence"
MAX_UINT64 = (1 << 64) - 1
MAX_MINTED_OWNER_EPOCH = (1 << 53) - 2

_CANONICAL_UINT_RE = re.compile(r"^(?:0|[1-9][0-9]*)$")
_HEX32_RE = re.compile(r"^[0-9a-f]{32}$")
_ZERO32 = "0" * 32


# ── Hash Tag 与域守卫 ──────────────────────────────────────────────────────────

def scene_control_tag(zone_id: int) -> str:
    return f"{{ctl:{zone_id}}}"


def gate_mark_tag(zone_id: int) -> str:
    return f"{{gm:{zone_id}}}"


def player_tag(player_id: int) -> str:
    return f"{{{player_id}}}"


def extract_hash_tag(key: str) -> str | None:
    start = key.find("{")
    if start < 0:
        return None
    end = key.find("}", start + 1)
    if end <= start + 1:
        return None
    return key[start + 1:end]


def assert_control_domain_keys(keys: Sequence[str], *, require_same_tag: bool = True) -> None:
    """校验所有键均属于控制域（{ctl:*} 或 {gm:*}），且同批 Lua/MGET 落同一 hash tag。"""
    seen_tag: str | None = None
    for key in keys:
        tag = extract_hash_tag(key)
        if tag is None or not (tag.startswith("ctl:") or tag.startswith("gm:")):
            raise CrossDomainRedisKeyError(f"非控制域键不得发往 SceneControlRedis: {key!r}")
        if require_same_tag:
            if seen_tag is None:
                seen_tag = tag
            elif seen_tag != tag:
                raise CrossDomainRedisKeyError(f"控制域多键跨槽: {seen_tag!r} vs {tag!r} ({key!r})")


def assert_player_domain_keys(keys: Sequence[str], *, require_same_tag: bool = True) -> None:
    """校验所有键均属于玩家域（{<player_id>}），且同批 Lua 落同一玩家 slot。"""
    seen_tag: str | None = None
    for key in keys:
        tag = extract_hash_tag(key)
        if tag is None or not tag.isascii() or not tag.isdigit():
            raise CrossDomainRedisKeyError(f"非玩家域键不得发往 PlayerRedis: {key!r}")
        if require_same_tag:
            if seen_tag is None:
                seen_tag = tag
            elif seen_tag != tag:
                raise CrossDomainRedisKeyError(f"玩家域多键跨槽: {seen_tag!r} vs {tag!r} ({key!r})")


# ── SceneControlRedis 键（{ctl:<zone>} 与 {gm:<zone>}） ────────────────────────

def scene_node_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:node"


def instance_members_key(zone_id: int, scene_id: int) -> str:
    return f"instance:{scene_control_tag(zone_id)}:{scene_id}:members"


def node_members_key(zone_id: int, node_id: str) -> str:
    return f"node:{scene_control_tag(zone_id)}:{node_id}:members"


def active_instances_key(zone_id: int) -> str:
    return f"instances:{scene_control_tag(zone_id)}:active"


def scene_mirror_flag_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:mirror"


def scene_source_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:source"


def scene_mirrors_key(zone_id: int, source_scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{source_scene_id}:mirrors"


def node_scenes_key(zone_id: int, node_id: str) -> str:
    return f"node:{scene_control_tag(zone_id)}:{node_id}:scenes"


def node_scene_count_key(zone_id: int, node_id: str) -> str:
    return f"node:{scene_control_tag(zone_id)}:{node_id}:scene_count"


def node_scene_node_type_key(zone_id: int, node_id: str) -> str:
    return f"node:{scene_control_tag(zone_id)}:{node_id}:scene_node_type"


def node_load_key(zone_id: int) -> str:
    return f"scene_nodes:{scene_control_tag(zone_id)}:load"


def node_death_at_key(zone_id: int, node_id: str) -> str:
    return f"node:{scene_control_tag(zone_id)}:{node_id}:death_at"


def scene_agones_gs_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:agones_gs"


def scene_registration_record_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:registration"


def scene_teardown_job_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:teardown"


def scene_teardown_pending_key(zone_id: int) -> str:
    return f"scene_teardown:{scene_control_tag(zone_id)}:pending"


def agones_room_holds_key(zone_id: int) -> str:
    return f"agones_rooms:{scene_control_tag(zone_id)}:holds"


def world_channels_key(zone_id: int, conf_id: int) -> str:
    return f"world_channels:{scene_control_tag(zone_id)}:{conf_id}"


def world_draining_set_key(zone_id: int, conf_id: int) -> str:
    return f"world_channels:draining:{scene_control_tag(zone_id)}:{conf_id}"


def world_channel_lineno_key(zone_id: int, conf_id: int) -> str:
    """分线号映射表：HASH(scene_id -> 1-based line_no)，与 Go WorldChannelLineNoKeyFmt 逐字节一致。"""
    return f"world_channel_lineno:{scene_control_tag(zone_id)}:{conf_id}"


def world_channel_directory_key(zone_id: int, conf_id: int) -> str:
    """分线目录快照：STRING(SceneChannelDirectory protobuf)，与 nodes/scene_channels.py 逐字节一致。"""
    return f"world_channel_directory:{scene_control_tag(zone_id)}:{conf_id}"


def dead_node_reconcile_tasks_key(zone_id: int) -> str:
    return f"scene_manager:dead_node_reconcile:{scene_control_tag(zone_id)}"


def node_reconcile_scenes_key(zone_id: int, node_id: str) -> str:
    return f"node:{scene_control_tag(zone_id)}:{node_id}:reconcile_scenes"


def node_reconcile_instance_key(zone_id: int, node_id: str) -> str:
    return f"node:{scene_control_tag(zone_id)}:{node_id}:reconcile_instance"


def pending_dead_node_member(zone_id: int, node_id: str) -> str:
    return f"{zone_id}/{node_id}"


def parse_dead_node_reconcile_member(member: str) -> tuple[int, str] | None:
    if "/" not in member:
        return None
    zone_part, node_id = member.split("/", 1)
    if not node_id or not zone_part.isascii() or not zone_part.isdigit():
        return None
    return int(zone_part), node_id


def world_desired_channels_key(zone_id: int) -> str:
    return f"world_channels:desired:{scene_control_tag(zone_id)}"


def scene_draining_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:draining"


def world_init_lock_key(zone_id: int) -> str:
    return f"world_init:lock:{scene_control_tag(zone_id)}"


def world_placement_seq_key(zone_id: int) -> str:
    return f"world_channels:placement_seq:{scene_control_tag(zone_id)}"


def scene_placement_gen_key(zone_id: int, scene_id: int) -> str:
    return f"scene:{scene_control_tag(zone_id)}:{scene_id}:placement_gen"


def gate_draining_key(zone_id: int, gate_node_id: int) -> str:
    return f"gate:{gate_mark_tag(zone_id)}:{gate_node_id}:draining"


def gate_stale_key(zone_id: int, gate_node_id: int) -> str:
    return f"gate:{gate_mark_tag(zone_id)}:{gate_node_id}:stale"


# ── PlayerRedis 键（{<player_id>}） ────────────────────────────────────────────

def location_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:location"


def location_revision_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:location_revision"


def location_revision_floor_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:location_revision_floor"


def owner_epoch_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:owner_epoch"


def owner_epoch_confirmed_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:owner_epoch_confirmed"


def handoff_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:handoff"


def handoff_fence_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:handoff_fence"


def holder_lease_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:holder_lease"


def leave_release_wait_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:leave_release_wait"


def channel_switch_cooldown_key(player_id: int) -> str:
    return f"player:{player_tag(player_id)}:channel_switch_cooldown"


def enter_scene_dedup_key(player_id: int, request_id: str) -> str:
    return f"enter_scene:dedup:{player_tag(player_id)}:{request_id}"


# ── 多键组合辅助（顺序与 lua.py 脚本契约严格对应） ────────────────────────────────

def location_snapshot_keys(player_id: int) -> tuple[str, str]:
    return (location_key(player_id), location_revision_key(player_id))


def placement_keys(player_id: int) -> tuple[str, str, str, str, str]:
    return (
        owner_epoch_key(player_id),
        location_key(player_id),
        handoff_key(player_id),
        location_revision_key(player_id),
        location_revision_floor_key(player_id),
    )


def rollback_placement_keys(player_id: int) -> tuple[str, str, str, str, str, str]:
    return (
        location_key(player_id),
        owner_epoch_key(player_id),
        handoff_key(player_id),
        location_revision_key(player_id),
        location_revision_floor_key(player_id),
        handoff_fence_key(player_id),
    )


def leave_delete_keys(player_id: int) -> tuple[str, str, str, str, str, str]:
    return (
        location_key(player_id),
        owner_epoch_key(player_id),
        handoff_key(player_id),
        leave_release_wait_key(player_id),
        location_revision_key(player_id),
        location_revision_floor_key(player_id),
    )


def enter_scene_dedupe_complete_keys(player_id: int, request_id: str) -> tuple[str, str]:
    return (enter_scene_dedup_key(player_id, request_id), location_revision_floor_key(player_id))


# ── 修订标签、归属代次、handoff 标记与 handoff_fence 解析器 ──────────────────────

def revision_text(revision: int) -> str:
    return f"{revision:020d}"


def optional_revision_text(revision: int) -> str:
    return "" if revision == 0 else revision_text(revision)


def location_revision_tag(revision: int, raw: bytes) -> str:
    if revision == 0 or not raw:
        return ""
    digest = hashlib.sha256(raw).hexdigest()
    return f"{revision_text(revision)}:{digest}"


def parse_tagged_location_revision(tag: str, raw: bytes) -> int:
    """校验 location_revision 侧车标签并返回对应的 owner_revision（空标签返回 0）。"""
    if not tag:
        return 0
    if len(tag) != 85 or tag[20] != ":":
        raise ValueError("位置原修订标签损坏")
    rev_part = tag[:20]
    if not rev_part.isascii() or not rev_part.isdigit():
        raise ValueError("位置原修订标签损坏")
    revision = int(rev_part)
    if revision <= 0 or tag != location_revision_tag(revision, raw):
        raise ValueError("位置原修订与原文不匹配")
    return revision


def parse_epoch(raw: str | bytes | None) -> int:
    """解析 owner_epoch 键原文；缺键/空串返回 0，非法十进制抛出 ValueError。"""
    if not raw:
        return 0
    text = raw.decode("ascii") if isinstance(raw, (bytes, bytearray)) else str(raw)
    if not text.isascii() or not text.isdigit():
        raise ValueError(f"invalid owner_epoch: {text!r}")
    value = int(text)
    if value < 0 or value > MAX_UINT64:
        raise ValueError(f"owner_epoch out of uint64 range: {text!r}")
    return value


@dataclass(frozen=True, slots=True)
class HandoffMarker:
    """已解析的 player:{<pid>}:handoff 落盘标记（格式 ``{epoch}:{saved_at_ms}``）。"""

    epoch: int
    saved_at_ms: int
    raw: str


def parse_handoff(raw: str | bytes | None) -> HandoffMarker | None:
    """解析 ``{epoch}:{saved_at_ms}``；空串返回 None，格式损坏抛出 ValueError。"""
    if not raw:
        return None
    text = raw.decode("utf-8", errors="strict") if isinstance(raw, (bytes, bytearray)) else str(raw)
    if ":" not in text:
        raise ValueError(f"malformed handoff marker: {text!r}")
    epoch_str, saved_str = text.split(":", 1)
    if not epoch_str.isascii() or not epoch_str.isdigit():
        raise ValueError(f"malformed handoff epoch: {text!r}")
    if not saved_str.isascii() or not (saved_str.isdigit() or (saved_str.startswith("-") and saved_str[1:].isdigit())):
        raise ValueError(f"malformed handoff saved_at_ms: {text!r}")
    epoch = int(epoch_str)
    saved_at = int(saved_str)
    if epoch > MAX_UINT64:
        raise ValueError(f"handoff epoch out of range: {text!r}")
    return HandoffMarker(epoch=epoch, saved_at_ms=saved_at, raw=text)


def marker_releases_epoch(raw: str | bytes | None, epoch: int) -> bool:
    """handoff 标记是否证明 epoch 这一代已落盘释放（损坏或缺失均返回 False）。"""
    if epoch == 0 or not raw:
        return False
    try:
        marker = parse_handoff(raw)
    except ValueError:
        return False
    return marker is not None and marker.epoch == epoch


def pending_rollback_receipt(marker: str) -> str:
    return PENDING_ROLLBACK_RECEIPT_PREFIX + marker


def is_pending_rollback_receipt(receipt: str) -> bool:
    return receipt.startswith(PENDING_ROLLBACK_RECEIPT_PREFIX)


def final_rollback_receipt(receipt: str) -> tuple[str, bool]:
    if receipt.startswith(PENDING_ROLLBACK_RECEIPT_PREFIX):
        return receipt[len(PENDING_ROLLBACK_RECEIPT_PREFIX):], True
    return "", False


def _is_canonical_uint(text: str, *, allow_zero: bool = False) -> bool:
    if not text or len(text) > 20 or not _CANONICAL_UINT_RE.match(text):
        return False
    if text == "0":
        return allow_zero
    return int(text) <= MAX_UINT64


@dataclass(frozen=True, slots=True)
class HandoffFence:
    """交接资格原件 ``E|S|N|A|K|T``，与 Go ``handofffence.ParseHandoffFence`` 同口径。"""

    owner_epoch: int
    scene_instance: str
    generation: int
    admission_token: str
    state: str
    token: str


def parse_handoff_fence(raw: str | bytes | None) -> HandoffFence | None:
    """解析 6 段 ``E|S|N|A|K|T``；不规范返回 None。"""
    if not raw:
        return None
    text = raw.decode("utf-8", errors="strict") if isinstance(raw, (bytes, bytearray)) else str(raw)
    parts = text.split("|")
    if len(parts) != 6:
        return None
    e_str, inst, n_str, a_str, state, t_str = parts
    if (
        not _is_canonical_uint(e_str, allow_zero=False)
        or not _is_canonical_uint(n_str, allow_zero=True)
        or not _HEX32_RE.match(a_str)
        or not _HEX32_RE.match(t_str)
        or t_str == _ZERO32
        or len(inst) > 256
        or any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in inst)
        or state not in ("H", "F", "R")
    ):
        return None
    if n_str == "0":
        if state != "R" or inst != "" or a_str != _ZERO32:
            return None
    else:
        if inst == "" or a_str == _ZERO32:
            return None
    return HandoffFence(
        owner_epoch=int(e_str),
        scene_instance=inst,
        generation=int(n_str),
        admission_token=a_str,
        state=state,
        token=t_str,
    )
