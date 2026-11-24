"""Redis key builders, byte-for-byte identical to Go ``shared/redisslot``.

Login-domain keys are tagged either by the gate connection id or by ``account_tag(account)``;
player-domain keys use ``{player_id}``. Never build these keys anywhere else.
"""

from binascii import crc_hqx
from functools import cache
from itertools import product
from string import ascii_lowercase

SLOT_COUNT = 16384
_SLOT_TAG_MAX_LEN = 4


def crc16(value: str) -> int:
    """CRC16-XMODEM, the Redis Cluster hash."""
    return crc_hqx(value.encode(), 0)


@cache
def _slot_tags() -> tuple[str, ...]:
    # Shortest-first, then lexicographic: the first string hashing to a slot is its tag.
    tags = [""] * SLOT_COUNT
    filled = 0
    for length in range(1, _SLOT_TAG_MAX_LEN + 1):
        for letters in product(ascii_lowercase, repeat=length):
            tag = "".join(letters)
            slot = crc16(tag) & (SLOT_COUNT - 1)
            if not tags[slot]:
                tags[slot] = tag
                filled += 1
                if filled == SLOT_COUNT:
                    return tuple(tags)
    raise RuntimeError("slot-tag table does not cover all slots")


def slot_tag(slot: int) -> str:
    return _slot_tags()[slot]


def account_tag(account: str) -> str:
    return slot_tag(crc16(account) & (SLOT_COUNT - 1))


def valid_account_tag(tag: str) -> bool:
    return (0 < len(tag) <= _SLOT_TAG_MAX_LEN and all("a" <= c <= "z" for c in tag)
            and slot_tag(crc16(tag) & (SLOT_COUNT - 1)) == tag)


def connection_id(gate_instance_id: str, incarnation: int) -> str:
    return f"{gate_instance_id}:{incarnation}"


def login_session_key(conn: str) -> str:
    return f"login_session:{{{conn}}}"


def session_fence_key(conn: str) -> str:
    return f"login:sessfence:{{{conn}}}"


def account_login_lock_key(account: str) -> str:
    return f"account_lock:login:{{{account_tag(account)}}}:{account}"


def account_create_lock_key(account: str) -> str:
    return f"account_lock:create:{{{account_tag(account)}}}:{account}"


def account_device_set_key(account: str) -> str:
    return f"login_account_sessions:{{{account_tag(account)}}}:{account}"


def account_refresh_key_prefix(tag: str) -> str:
    return f"account_refresh:{{{tag}}}:"


def account_refresh_key(account: str) -> str:
    return account_refresh_key_prefix(account_tag(account)) + account


def access_token_key(tag: str, secret: str) -> str:
    return f"access_token:{{{tag}}}:{secret}"


def refresh_token_key_prefix(tag: str) -> str:
    return f"refresh_token:{{{tag}}}:"


def refresh_token_key(tag: str, secret: str) -> str:
    return refresh_token_key_prefix(tag) + secret


def refresh_family_key_prefix(tag: str) -> str:
    return f"refresh_family:{{{tag}}}:"


def refresh_family_key(tag: str, family_id: str) -> str:
    return refresh_family_key_prefix(tag) + family_id


def refresh_rotated_key(tag: str, secret: str) -> str:
    return f"refresh_rotated:{{{tag}}}:{secret}"


def player_locker_key(player_id: int) -> str:
    return f"player_locker:{{{player_id}}}"


def login_generation_key(player_id: int) -> str:
    return f"player:{{{player_id}}}:login_generation"


def login_idempotent_key(player_id: int, request_id: str) -> str:
    return f"login_idempotent:{{{player_id}}}:{request_id}"


# -- 控制集群键（排队族 {q:<zone>}、gate 标记族 {gm:<zone>}）-----------------------------------------


def queue_tag(zone_id: int) -> str:
    """登录排队控制族 hash tag，与 Go ``redisslot.ZoneControlTag(ControlFamilyLoginQueue, zone)`` 一致。"""
    return f"{{q:{zone_id}}}"


def queue_zset_key(zone_id: int) -> str:
    return f"queue:{queue_tag(zone_id)}"


def admitted_zset_key(zone_id: int) -> str:
    return f"admitted:zset:{queue_tag(zone_id)}"


def admit_key(zone_id: int, queue_id: str) -> str:
    return f"admit:{queue_tag(zone_id)}:{queue_id}"


def queue_meta_key(zone_id: int, queue_id: str) -> str:
    return f"queue:meta:{queue_tag(zone_id)}:{queue_id}"


def queue_token_index_key(zone_id: int, token: str) -> str:
    return f"queue:token:{queue_tag(zone_id)}:{token}"


def gate_mark_tag(zone_id: int) -> str:
    """gate 状态标记族 hash tag，与 Go ``gatepick.MarkTag`` 一致。"""
    return f"{{gm:{zone_id}}}"


def gate_draining_key(zone_id: int, node_id: int) -> str:
    return f"gate:{gate_mark_tag(zone_id)}:{node_id}:draining"


def gate_drained_key(zone_id: int, node_id: int) -> str:
    return f"gate:{gate_mark_tag(zone_id)}:{node_id}:drained"


def gate_stale_key(zone_id: int, node_id: int) -> str:
    return f"gate:{gate_mark_tag(zone_id)}:{node_id}:stale"


# -- 口令失败配额与实名键（登录域）-----------------------------------------------------------------


def password_quota_key(fingerprint: str) -> str:
    """生产口令账号失败配额键（不含账号原文），与 Go ``redisslot.PasswordQuotaKey`` 一致。"""
    return f"login:password:quota:v1:{{{fingerprint}}}"


def real_name_pending_key(account: str) -> str:
    """账号认证中实名提交的 ai，与 Go ``redisslot.RealNamePendingKey`` 一致。"""
    return f"login_realname_pending:{{{account_tag(account)}}}:{account}"


def real_name_attempts_key(account: str, day: str) -> str:
    """账号当日（北京日期 YYYY-MM-DD）实名提交次数键，与 Go ``redisslot.RealNameAttemptsKey`` 一致。"""
    return f"login_realname_attempts:{{{account_tag(account)}}}:{account}:{day}"
