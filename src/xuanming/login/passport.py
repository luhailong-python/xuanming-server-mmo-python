"""官方通行证管理客户端与会话强制执行器（对应 Go ``pkg/auth/passport_admin.go`` + ``pkg/passportsession``）。

将官方通行证（Casdoor）中的账号变化（改密、封禁、删除）落到游戏会话上：
- ``check_login``：通行证登录签发令牌前核对通行证状态，吊销改密前旧会话并拒绝过期/被封/被删身份；
- ``check_refresh``：刷新令牌轮换前核对通行证状态，改密/封禁/删除时拒绝续期并吊销踢线；
- ``handle_event``：处理网关转来的 Casdoor 回调，只拿回调找人，按通行证此刻权威状态吊销并踢线（幂等）。
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol as TypingProtocol

from . import tips
from .tokens import TokenData, TokenManager

logger = logging.getLogger(__name__)

PASSPORT_AUTH_TYPE = "passport"
PASSPORT_ACCOUNT_PREFIX = "passport_"

# SessionVerdict 结论常量（空串表示正常）
PASSPORT_SESSION_USER_GONE = "user_gone"
PASSPORT_SESSION_USER_FORBIDDEN = "user_forbidden"
PASSPORT_SESSION_PASSWORD_CHANGED = "password_changed"

MAX_EVENT_BYTES = 64 * 1024
MAX_EVENT_USERS = 4
BUILT_IN_ORGANIZATION = "built-in"

_MAX_RESPONSE_BYTES = 64 * 1024
_DEFAULT_LOOKUP_TIMEOUT_SEC = 1.0
_PASSPORT_GET_USER_PATH = "/api/get-user"
_PASSPORT_DELETE_USER_PATH = "/api/delete-user"
_PASSPORT_USER_REMOVED = "Affected"


class PassportProviderUnavailableError(RuntimeError):
    """官方通行证管理接口不可达或应答异常。"""


class BadPassportEventError(ValueError):
    """通行证回调请求体不符合 Casdoor Record 格式或超长（应回 InvalidArgument，无需重投）。"""


def passport_account(sub: str) -> str:
    """将通行证用户 id（sub）映射为游戏内部账号名。"""
    return PASSPORT_ACCOUNT_PREFIX + sub


def passport_subject(account: str) -> str | None:
    """从游戏账号名提取通行证用户 id（非通行证账号返回 ``None``）。"""
    if account.startswith(PASSPORT_ACCOUNT_PREFIX) and len(account) > len(PASSPORT_ACCOUNT_PREFIX):
        return account[len(PASSPORT_ACCOUNT_PREFIX) :]
    return None


@dataclass(frozen=True, slots=True)
class PassportUserStatus:
    """官方通行证中一名用户此刻的权威状态。"""

    sub: str = ""
    gone: bool = False
    forbidden: bool = False
    password_changed_at: int = 0

    def session_verdict(self, issued_at: int) -> str:
        """判断签发于 ``issued_at``（Unix 秒，通行证时钟）的登录是否仍然有效；空串表示有效。"""
        if self.gone:
            return PASSPORT_SESSION_USER_GONE
        if self.forbidden:
            return PASSPORT_SESSION_USER_FORBIDDEN
        if self.password_changed_at > 0 and self.password_changed_at >= int(issued_at):
            return PASSPORT_SESSION_PASSWORD_CHANGED
        return ""


@dataclass(frozen=True, slots=True)
class PassportUserRef:
    """回调中点到的一名待核对用户引用（按 sub 或按 owner/name）。"""

    sub: str = ""
    owner: str = ""
    name: str = ""


class PassportAdminClient(TypingProtocol):
    """官方通行证管理接口协议（生产使用 ``HttpPassportAdmin``，单测可注入内存替身）。"""

    async def lookup_user(self, sub: str) -> PassportUserStatus: ...
    async def lookup_user_by_name(self, owner: str, name: str) -> PassportUserStatus: ...
    async def delete_user(self, sub: str) -> None: ...


def _parse_rfc3339_unix(raw: str) -> int:
    text = raw.strip()
    if not text:
        return 0
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise PassportProviderUnavailableError(
            "passport: get-user lastChangePasswordTime is not RFC 3339"
        ) from exc
    if parsed.tzinfo is None:
        raise PassportProviderUnavailableError(
            "passport: get-user lastChangePasswordTime missing timezone"
        )
    return int(parsed.timestamp())


def _status_from_user_dict(user: dict) -> PassportUserStatus:
    changed_raw = str(user.get("lastChangePasswordTime") or "")
    changed_at = _parse_rfc3339_unix(changed_raw) if changed_raw else 0
    return PassportUserStatus(
        sub=str(user.get("id") or ""),
        gone=bool(user.get("isDeleted", False)),
        forbidden=bool(user.get("isForbidden", False)),
        password_changed_at=changed_at,
    )


class HttpPassportAdmin:
    """基于标准库 ``urllib.request`` 的 Casdoor 管理接口纯 Python 客户端。"""

    def __init__(
        self,
        issuer: str,
        *,
        endpoint: str = "",
        client_id: str,
        client_secret: str,
        timeout_sec: float = 5.0,
    ) -> None:
        if not issuer or not client_id or not client_secret:
            raise ValueError("passport: issuer, client_id and client_secret are all required")
        self._base_url = (endpoint or issuer).rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._timeout_sec = timeout_sec

    def _call_sync(self, method: str, path: str, query: dict[str, str] | None, body: bytes | None) -> dict:
        url = self._base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{self._client_id}:{self._client_secret}".encode("utf-8")).decode("ascii")
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_sec) as resp:
                if not 200 <= resp.status < 300:
                    raise PassportProviderUnavailableError(f"passport: admin api http status {resp.status}")
                raw = resp.read(_MAX_RESPONSE_BYTES + 1)
        except PassportProviderUnavailableError:
            raise
        except Exception as exc:
            raise PassportProviderUnavailableError(f"passport: admin api request failed: {exc}") from exc
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise PassportProviderUnavailableError("passport: admin api response too large")
        try:
            reply = json.loads(raw)
        except ValueError as exc:
            raise PassportProviderUnavailableError("passport: bad admin api json") from exc
        if not isinstance(reply, dict) or reply.get("status") != "ok":
            msg = str(reply.get("msg", ""))[:120] if isinstance(reply, dict) else ""
            raise PassportProviderUnavailableError(f"passport: admin api refused request ({msg!r})")
        return reply

    async def _get_user(self, query: dict[str, str]) -> dict | None:
        reply = await asyncio.to_thread(self._call_sync, "GET", _PASSPORT_GET_USER_PATH, query, None)
        data = reply.get("data")
        if data is None:
            return None
        if not isinstance(data, dict):
            raise PassportProviderUnavailableError("passport: get-user returned unexpected user")
        return data

    async def lookup_user(self, sub: str) -> PassportUserStatus:
        user = await self._get_user({"userId": sub})
        if user is None:
            return PassportUserStatus(sub=sub, gone=True)
        if str(user.get("id") or "") != sub or not user.get("owner") or not user.get("name"):
            raise PassportProviderUnavailableError("passport: get-user returned unexpected user")
        return _status_from_user_dict(user)

    async def lookup_user_by_name(self, owner: str, name: str) -> PassportUserStatus:
        user = await self._get_user({"id": f"{owner}/{name}"})
        if user is None:
            return PassportUserStatus(gone=True)
        if str(user.get("owner") or "") != owner or str(user.get("name") or "") != name or not user.get("id"):
            raise PassportProviderUnavailableError("passport: get-user returned unexpected user")
        return _status_from_user_dict(user)

    async def delete_user(self, sub: str) -> None:
        user = await self._get_user({"userId": sub})
        if user is None:
            return
        if str(user.get("id") or "") != sub or not user.get("owner") or not user.get("name"):
            raise PassportProviderUnavailableError("passport: get-user returned unexpected user")
        body = json.dumps({"owner": str(user["owner"]), "name": str(user["name"])}).encode("utf-8")
        reply = await asyncio.to_thread(self._call_sync, "POST", _PASSPORT_DELETE_USER_PATH, None, body)
        if reply.get("data") != _PASSPORT_USER_REMOVED:
            raise PassportProviderUnavailableError("passport: delete-user removed nothing")
        remaining = await self._get_user({"userId": sub})
        if remaining is not None:
            raise PassportProviderUnavailableError("passport: user still present after delete-user")


def parse_event_users(payload: bytes) -> list[PassportUserRef]:
    """从 Casdoor 回调 JSON 中提取待核对的用户引用，过滤管理组织与含 ``/`` 的应用伪用户。"""
    if not payload or len(payload) > MAX_EVENT_BYTES:
        raise BadPassportEventError("passport webhook payload is not a Casdoor record")
    try:
        event = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise BadPassportEventError("passport webhook payload is not a Casdoor record") from exc
    if not isinstance(event, dict):
        raise BadPassportEventError("passport webhook payload is not a Casdoor record")

    refs: list[PassportUserRef] = []

    def add(owner: str, name: str, uid: str) -> None:
        if (
            owner == BUILT_IN_ORGANIZATION
            or "/" in owner
            or "/" in name
            or len(refs) >= MAX_EVENT_USERS
        ):
            return
        if uid:
            refs.append(PassportUserRef(sub=uid))
        elif owner and name:
            refs.append(PassportUserRef(owner=owner, name=name))

    ext = event.get("extendedUser")
    if isinstance(ext, dict):
        add(str(ext.get("owner") or ""), str(ext.get("name") or ""), str(ext.get("id") or ""))

    add(str(event.get("organization") or ""), str(event.get("user") or ""), "")

    obj_raw = event.get("object")
    if isinstance(obj_raw, str) and obj_raw:
        try:
            obj = json.loads(obj_raw)
        except ValueError:
            obj = None
        if isinstance(obj, dict):
            add(str(obj.get("owner") or ""), str(obj.get("name") or ""), str(obj.get("id") or ""))
    elif isinstance(obj_raw, dict):
        add(str(obj_raw.get("owner") or ""), str(obj_raw.get("name") or ""), str(obj_raw.get("id") or ""))

    return refs


class PassportSessionEnforcer:
    """将官方通行证状态落到游戏会话上（登录、刷新与 574 回调共用同一套核对/吊销/踢线逻辑）。"""

    def __init__(
        self,
        admin: PassportAdminClient,
        tokens: TokenManager,
        kick_account_players: Callable[[str, int], Awaitable[None]] | None = None,
        *,
        lookup_timeout_sec: float = _DEFAULT_LOOKUP_TIMEOUT_SEC,
    ) -> None:
        self._admin = admin
        self._tokens = tokens
        self._kick = kick_account_players
        self._lookup_timeout_sec = lookup_timeout_sec

    async def _lookup_ref(self, ref: PassportUserRef) -> PassportUserStatus:
        async with asyncio.timeout(self._lookup_timeout_sec):
            if ref.sub:
                return await self._admin.lookup_user(ref.sub)
            return await self._admin.lookup_user_by_name(ref.owner, ref.name)

    async def _settle(self, phase: str, account: str, status: PassportUserStatus) -> bool:
        revoked = False
        if status.gone or status.forbidden:
            await self._tokens.revoke_all(account)
            revoked = True
        elif status.password_changed_at > 0:
            count = await self._tokens.revoke_issued_before(account, status.password_changed_at)
            revoked = count > 0
        if revoked:
            logger.info(
                "[passport] sessions revoked: phase=%s account=%s gone=%s forbidden=%s password_changed_at=%d",
                phase,
                account,
                status.gone,
                status.forbidden,
                status.password_changed_at,
            )
            if self._kick is not None:
                try:
                    await self._kick(account, tips.PASSPORT_CREDENTIAL_CHANGED)
                except Exception as exc:
                    logger.error(
                        "[passport] kicking online players failed (tokens are revoked): account=%s err=%s",
                        account,
                        exc,
                    )
        return revoked

    async def check_login(self, account: str, issued_at: int) -> int:
        """通行证登录签发令牌前核对状态；返回 0 放行，否则返回拒绝 tip（2059）。"""
        sub = passport_subject(account)
        if not sub:
            return 0
        try:
            async with asyncio.timeout(self._lookup_timeout_sec):
                status = await self._admin.lookup_user(sub)
        except Exception as exc:
            logger.error("[passport] login check failed, letting login through: account=%s err=%s", account, exc)
            return 0
        try:
            await self._settle("login", account, status)
        except Exception as exc:
            logger.error("[passport] revoking earlier sessions at login failed: account=%s err=%s", account, exc)
        verdict = status.session_verdict(issued_at)
        if not verdict:
            return 0
        logger.info("[passport] login refused: account=%s reason=%s", account, verdict)
        return tips.PASSPORT_CREDENTIAL_CHANGED

    async def check_refresh(self, data: TokenData | None) -> bool:
        """刷新令牌轮换前核对通行证状态；返回 True 表示拒绝续期并已尝试吊销。"""
        if data is None or data.auth_type != PASSPORT_AUTH_TYPE:
            return False
        sub = passport_subject(data.account)
        if not sub:
            return False
        try:
            async with asyncio.timeout(self._lookup_timeout_sec):
                status = await self._admin.lookup_user(sub)
        except Exception as exc:
            logger.error("[passport] session check failed, renewing anyway: account=%s err=%s", data.account, exc)
            return False
        verdict = status.session_verdict(data.identity_issued_at)
        if not verdict:
            return False
        try:
            await self._settle("refresh", data.account, status)
        except Exception as exc:
            logger.error(
                "[passport] revoking sessions failed, refresh refused: account=%s reason=%s err=%s",
                data.account,
                verdict,
                exc,
            )
            return True
        logger.info("[passport] refresh refused, sessions revoked: account=%s reason=%s", data.account, verdict)
        return True

    async def handle_event(self, payload: bytes) -> int:
        """处理 574 通行证回调：现查通行证权威状态并吊销踢线，返回实际核对的用户数（幂等）。"""
        refs = parse_event_users(payload)
        checked = 0
        seen: set[str] = set()
        for ref in refs:
            status = await self._lookup_ref(ref)
            if not status.sub or status.sub in seen:
                continue
            seen.add(status.sub)
            account = passport_account(status.sub)
            await self._settle("event", account, status)
            checked += 1
        return checked
