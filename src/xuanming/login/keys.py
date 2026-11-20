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
