"""Verify gate-signed session identities (Go ``shared/callerauth`` + login session interceptor)."""

import base64
import hashlib
import os
import re
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from google.protobuf.message import DecodeError

SESSION_DETAIL = "x-session-detail-bin"
_CALLER, _TIMESTAMP, _NONCE, _SIGNATURE = "x-caller-id", "x-caller-ts", "x-caller-nonce", "x-caller-sig"
_MAX_DETAIL_BYTES = 4096


class AuthError(Exception):
    """The identity claim is present but does not verify."""


class _NonceSet:
    """Two-generation replay table covering 2x clock skew (Go ``nonce.go``)."""

    def __init__(self, window: float, limit: int) -> None:
        self.window, self.limit = window, limit
        self.current: set[str] = set()
        self.previous: set[str] = set()
        self.started = time.monotonic()

    def admit(self, nonce: str) -> bool:
        now = time.monotonic()
        if now - self.started >= self.window or len(self.current) >= self.limit:
            self.previous, self.current, self.started = self.current, set(), now
        if nonce in self.current or nonce in self.previous:
            return False
        self.current.add(nonce)
        return True


class CallerVerifier:
    def __init__(self, public_keys, *, allowed_callers=(), max_skew_ms: int = 30_000,
                 max_nonces: int = 500_000, clock=time.time) -> None:
        self.keys = tuple(public_keys)
        self.allowed = frozenset(allowed_callers)
        self.skew_ms = max_skew_ms
        self.clock = clock
        self.nonces = _NonceSet(2 * max_skew_ms / 1000, max_nonces)

    @classmethod
    def from_env(cls, environ=None, **options):
        environ = os.environ if environ is None else environ
        values = [item for item in environ.get("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS", "").split(",") if item]
        if not values or any(not re.fullmatch(r"[0-9a-fA-F]{64}", item) for item in values):
            raise ValueError("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS 格式无效")
        return cls([Ed25519PublicKey.from_public_bytes(bytes.fromhex(item)) for item in values], **options)

    def authenticate(self, metadata, method: str) -> bytes | None:
        """Return the raw SessionDetails bytes, None when no claim is present, or raise AuthError."""
        values: dict[str, list] = {}
        for key, value in metadata or ():
            values.setdefault(key, []).append(value)
        claims = values.get(SESSION_DETAIL, [])
        if not claims or claims == [b""] or claims == [""]:
            return None
        if len(claims) != 1 or any(len(values.get(key, [])) > 1 for key in (_CALLER, _TIMESTAMP, _NONCE, _SIGNATURE)):
            raise AuthError("bad_payload")
        try:
            raw = base64.b64decode(claims[0], validate=True)
        except ValueError as error:
            raise AuthError("bad_payload") from error
        if len(raw) > _MAX_DETAIL_BYTES:
            raise AuthError("bad_payload")
        first = {key: (v[0].decode() if isinstance(v[0], bytes) else v[0]) for key, v in values.items()}
        caller = first.get(_CALLER, "")
        if not caller:
            raise AuthError("missing_caller")
        if self.allowed and caller not in self.allowed:
            raise AuthError("caller_not_allowed")
        try:
            signature = bytes.fromhex(first.get(_SIGNATURE, ""))
        except ValueError as error:
            raise AuthError("bad_signature") from error
        if not signature:
            raise AuthError("bad_signature")
        timestamp = first.get(_TIMESTAMP, "")
        if not re.fullmatch(r"[+-]?[0-9]{1,19}", timestamp):
            raise AuthError("bad_timestamp")
        if abs(self.clock() * 1000 - int(timestamp)) > self.skew_ms:
            raise AuthError("clock_skew")
        nonce = first.get(_NONCE, "")
        if not 16 <= len(nonce) <= 128:
            raise AuthError("bad_nonce")
        canonical = "\n".join(("mmorpg-gate-identity/v2", caller, method, hashlib.sha256(raw).hexdigest(),
                               str(int(timestamp)), nonce)).encode()
        if not any(_verifies(key, signature, canonical) for key in self.keys):
            raise AuthError("bad_signature")
        # Signature before nonce: unauthenticated traffic must not fill the replay table.
        if not self.nonces.admit(nonce):
            raise AuthError("replay")
        return raw


def _verifies(key: Ed25519PublicKey, signature: bytes, data: bytes) -> bool:
    try:
        key.verify(signature, data)
        return True
    except InvalidSignature:
        return False


def parse_details(protocol, raw: bytes):
    """Parse verified bytes; the session must carry a full connection identity."""
    try:
        details = protocol.parse("SessionDetails", raw)
    except DecodeError as error:
        raise AuthError("bad_payload") from error
    if not details.session_id or not details.gate_instance_id or not details.session_incarnation:
        raise AuthError("bad_payload")
    return details
