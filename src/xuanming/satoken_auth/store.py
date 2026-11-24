"""Sa-Token 认证用户存储与会话令牌存储（Memory + SQL + Redis）。

存储契约：
1. 用户与 OAuth 绑定存储（``MemoryAuthStore`` / ``SqlAuthStore``）：
   严格对齐 ``config/satoken_auth_schema.sql`` 中的 ``sys_user`` 与 ``sys_user_oauth`` 表；
2. 口令哈希复用 ``xuanming.login.password_auth`` 的标准库 ``hashlib.scrypt`` PHC 实现与常数时间校验；
3. 会话令牌存储（``MemoryTokenSessionStore`` / ``RedisTokenSessionStore``）：
   使用与 Go login ``SaTokenProvider`` 及 Java ``SaTokenResponses`` 完全一致的 Redis 键格式：
   - ``<token_name>:login:token:<token_value>`` -> ``<login_id>``
   - ``<token_name>:login:session:<login_id>`` -> JSON 会话元数据（含活跃 token 列表与扩展信息）
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from xuanming.login.password_auth import hash_password, verify_password_hash

logger = logging.getLogger(__name__)

DUP_ENTRY = 1062


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def token_redis_key(token_name: str, token_value: str) -> str:
    """生成 Sa-Token 令牌映射键：``satoken:login:token:<token>``。"""
    return f"{token_name}:login:token:{token_value}"


def session_redis_key(token_name: str, login_id: str) -> str:
    """生成 Sa-Token 用户会话键：``satoken:login:session:<user_id>``。"""
    return f"{token_name}:login:session:{login_id}"


class AuthStoreError(RuntimeError):
    """认证存储层基础异常。"""


class DuplicateUserError(AuthStoreError):
    """用户名或 OAuth 唯一键已存在。"""


# -- 领域实体模型 -------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class UserRecord:
    """系统用户实体（对应 ``sys_user``）。"""

    id: int
    username: str
    password_hash: str = field(default="", repr=False)
    nickname: str = ""
    avatar: str = ""
    is_guest: bool = False
    status: int = 1
    created_ms: int = 0
    last_login_ms: int = 0

    def to_public_dict(self) -> dict[str, Any]:
        """脱敏对外用户信息字典（绝不返回 ``password_hash``）。"""
        return {
            "userId": self.id,
            "username": self.username,
            "nickname": self.nickname,
            "avatar": self.avatar,
            "isGuest": self.is_guest,
            "status": self.status,
            "createdMs": self.created_ms,
            "lastLoginMs": self.last_login_ms,
        }


@dataclass(frozen=True, slots=True)
class UserOauthRecord:
    """第三方 OAuth 绑定实体（对应 ``sys_user_oauth``）。"""

    id: int
    user_id: int
    provider: str
    open_id: str
    union_id: str = ""
    created_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "userId": self.user_id,
            "provider": self.provider,
            "openId": self.open_id,
            "unionId": self.union_id,
            "createdMs": self.created_ms,
        }


@dataclass(frozen=True, slots=True)
class TokenSession:
    """签发给客户端的 Sa-Token 会话快照。"""

    token_name: str
    token_value: str
    login_id: str
    login_type: str = "login"
    created_ms: int = 0
    expires_at_ms: int = 0
    ttl_sec: int = 86400
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def redis_token_key(self) -> str:
        return token_redis_key(self.token_name, self.token_value)

    @property
    def redis_session_key(self) -> str:
        return session_redis_key(self.token_name, self.login_id)

    def remaining_sec(self, now_ms: int | None = None) -> int:
        ts = _now_ms() if now_ms is None else now_ms
        if self.expires_at_ms <= 0:
            return self.ttl_sec
        return max(0, (self.expires_at_ms - ts) // 1000)


# -- 用户存储接口与实现（MemoryAuthStore + SqlAuthStore） ---------------------------------------------


class AuthStore(ABC):
    """用户与 OAuth 绑定存储抽象接口。"""

    @abstractmethod
    async def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        nickname: str = "",
        avatar: str = "",
        is_guest: bool = False,
    ) -> UserRecord:
        """创建新用户；用户名冲突时抛出 ``DuplicateUserError``。"""

    @abstractmethod
    async def get_user_by_username(self, username: str) -> UserRecord | None:
        """按用户名查询用户记录。"""

    @abstractmethod
    async def get_user_by_id(self, user_id: int) -> UserRecord | None:
        """按主键 ID 查询用户记录。"""

    @abstractmethod
    async def touch_last_login(self, user_id: int) -> None:
        """更新用户最近登录时间戳。"""

    @abstractmethod
    async def find_oauth(self, provider: str, open_id: str) -> UserOauthRecord | None:
        """按 ``(provider, open_id)`` 查询第三方绑定记录。"""

    @abstractmethod
    async def bind_oauth(
        self,
        user_id: int,
        provider: str,
        open_id: str,
        *,
        union_id: str = "",
    ) -> UserOauthRecord:
        """将第三方身份绑定到已有用户。"""

    @abstractmethod
    async def find_or_create_oauth_user(
        self,
        provider: str,
        open_id: str,
        *,
        nickname: str = "",
        avatar: str = "",
        union_id: str = "",
    ) -> tuple[UserRecord, bool]:
        """查找已绑定的用户，或自动创建用户并写入 ``sys_user_oauth``（对应 ``OauthService.findOrCreateAndBind``）。"""

    @abstractmethod
    async def list_user_oauths(self, user_id: int) -> list[UserOauthRecord]:
        """列出用户的全部第三方 OAuth 绑定。"""


class MemoryAuthStore(AuthStore):
    """纯内存用户与 OAuth 存储（供单元测试与无库本地模式使用）。"""

    def __init__(self, *, clock_ms: Callable[[], int] = _now_ms) -> None:
        self._clock_ms = clock_ms
        self._lock = asyncio.Lock()
        self._next_user_id = 1
        self._next_oauth_id = 1
        self._users_by_id: dict[int, UserRecord] = {}
        self._user_id_by_name: dict[str, int] = {}
        self._oauth_by_key: dict[tuple[str, str], UserOauthRecord] = {}

    async def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        nickname: str = "",
        avatar: str = "",
        is_guest: bool = False,
    ) -> UserRecord:
        async with self._lock:
            if username in self._user_id_by_name:
                raise DuplicateUserError(f"username already exists: {username}")
            uid = self._next_user_id
            self._next_user_id += 1
            now = self._clock_ms()
            record = UserRecord(
                id=uid,
                username=username,
                password_hash=password_hash,
                nickname=nickname or username,
                avatar=avatar,
                is_guest=is_guest,
                status=1,
                created_ms=now,
                last_login_ms=0,
            )
            self._users_by_id[uid] = record
            self._user_id_by_name[username] = uid
            return record

    async def get_user_by_username(self, username: str) -> UserRecord | None:
        async with self._lock:
            uid = self._user_id_by_name.get(username)
            return self._users_by_id.get(uid) if uid is not None else None

    async def get_user_by_id(self, user_id: int) -> UserRecord | None:
        async with self._lock:
            return self._users_by_id.get(int(user_id))

    async def touch_last_login(self, user_id: int) -> None:
        async with self._lock:
            existing = self._users_by_id.get(int(user_id))
            if existing is None:
                return
            self._users_by_id[int(user_id)] = UserRecord(
                id=existing.id,
                username=existing.username,
                password_hash=existing.password_hash,
                nickname=existing.nickname,
                avatar=existing.avatar,
                is_guest=existing.is_guest,
                status=existing.status,
                created_ms=existing.created_ms,
                last_login_ms=self._clock_ms(),
            )

    async def find_oauth(self, provider: str, open_id: str) -> UserOauthRecord | None:
        async with self._lock:
            return self._oauth_by_key.get((provider.lower(), open_id))

    async def bind_oauth(
        self,
        user_id: int,
        provider: str,
        open_id: str,
        *,
        union_id: str = "",
    ) -> UserOauthRecord:
        norm_provider = provider.lower()
        async with self._lock:
            key = (norm_provider, open_id)
            existing = self._oauth_by_key.get(key)
            if existing is not None:
                if existing.user_id != int(user_id):
                    raise DuplicateUserError(f"oauth identity already bound: {norm_provider}:{open_id}")
                return existing
            if int(user_id) not in self._users_by_id:
                raise AuthStoreError(f"user not found: {user_id}")
            oid = self._next_oauth_id
            self._next_oauth_id += 1
            record = UserOauthRecord(
                id=oid,
                user_id=int(user_id),
                provider=norm_provider,
                open_id=open_id,
                union_id=union_id,
                created_ms=self._clock_ms(),
            )
            self._oauth_by_key[key] = record
            return record

    async def find_or_create_oauth_user(
        self,
        provider: str,
        open_id: str,
        *,
        nickname: str = "",
        avatar: str = "",
        union_id: str = "",
    ) -> tuple[UserRecord, bool]:
        norm_provider = provider.lower()
        async with self._lock:
            key = (norm_provider, open_id)
            existing_oauth = self._oauth_by_key.get(key)
            if existing_oauth is not None:
                user = self._users_by_id[existing_oauth.user_id]
                now = self._clock_ms()
                updated = UserRecord(
                    id=user.id,
                    username=user.username,
                    password_hash=user.password_hash,
                    nickname=user.nickname,
                    avatar=user.avatar,
                    is_guest=user.is_guest,
                    status=user.status,
                    created_ms=user.created_ms,
                    last_login_ms=now,
                )
                self._users_by_id[user.id] = updated
                return updated, False

            uid = self._next_user_id
            self._next_user_id += 1
            now = self._clock_ms()
            username = f"{norm_provider}_{uid}_{secrets.token_hex(4)}"
            effective_nickname = nickname.strip() if nickname and nickname.strip() else f"{norm_provider}_{uid % 10000}"
            user = UserRecord(
                id=uid,
                username=username,
                password_hash="",
                nickname=effective_nickname,
                avatar=avatar,
                is_guest=False,
                status=1,
                created_ms=now,
                last_login_ms=now,
            )
            self._users_by_id[uid] = user
            self._user_id_by_name[username] = uid

            oid = self._next_oauth_id
            self._next_oauth_id += 1
            self._oauth_by_key[key] = UserOauthRecord(
                id=oid,
                user_id=uid,
                provider=norm_provider,
                open_id=open_id,
                union_id=union_id,
                created_ms=now,
            )
            return user, True

    async def list_user_oauths(self, user_id: int) -> list[UserOauthRecord]:
        async with self._lock:
            return [rec for rec in self._oauth_by_key.values() if rec.user_id == int(user_id)]


def _is_duplicate_error(exc: Exception) -> bool:
    args = getattr(exc, "args", ())
    return bool(args and isinstance(args[0], int) and args[0] == DUP_ENTRY)


class SqlAuthStore(AuthStore):
    """基于异步 ``aiomysql`` 连接池的用户与 OAuth 存储（对齐 ``config/satoken_auth_schema.sql``）。"""

    def __init__(self, pool: Any, *, clock_ms: Callable[[], int] = _now_ms) -> None:
        if pool is None:
            raise ValueError("SqlAuthStore requires a non-null async pool")
        self._pool = pool
        self._clock_ms = clock_ms

    @staticmethod
    def _row_to_user(row: Any) -> UserRecord:
        if isinstance(row, Mapping):
            return UserRecord(
                id=int(row["id"]),
                username=str(row["username"]),
                password_hash=str(row["password_hash"] or ""),
                nickname=str(row["nickname"] or ""),
                avatar=str(row["avatar"] or ""),
                is_guest=bool(row["is_guest"]),
                status=int(row["status"]),
                created_ms=int(row["created_ms"]),
                last_login_ms=int(row["last_login_ms"]),
            )
        return UserRecord(
            id=int(row[0]),
            username=str(row[1]),
            password_hash=str(row[2] or ""),
            nickname=str(row[3] or ""),
            avatar=str(row[4] or ""),
            is_guest=bool(row[5]),
            status=int(row[6]),
            created_ms=int(row[7]),
            last_login_ms=int(row[8]),
        )

    @staticmethod
    def _row_to_oauth(row: Any) -> UserOauthRecord:
        if isinstance(row, Mapping):
            return UserOauthRecord(
                id=int(row["id"]),
                user_id=int(row["user_id"]),
                provider=str(row["provider"]),
                open_id=str(row["open_id"]),
                union_id=str(row["union_id"] or ""),
                created_ms=int(row["created_ms"]),
            )
        return UserOauthRecord(
            id=int(row[0]),
            user_id=int(row[1]),
            provider=str(row[2]),
            open_id=str(row[3]),
            union_id=str(row[4] or ""),
            created_ms=int(row[5]),
        )

    async def create_user(
        self,
        username: str,
        password_hash: str,
        *,
        nickname: str = "",
        avatar: str = "",
        is_guest: bool = False,
    ) -> UserRecord:
        now = self._clock_ms()
        eff_nick = nickname or username
        sql = (
            "INSERT INTO `sys_user` "
            "(`username`, `password_hash`, `nickname`, `avatar`, `is_guest`, `status`, `created_ms`, `last_login_ms`) "
            "VALUES (%s, %s, %s, %s, %s, 1, %s, 0)"
        )
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, (username, password_hash, eff_nick, avatar, int(is_guest), now))
                    uid = int(cur.lastrowid)
        except Exception as exc:
            if _is_duplicate_error(exc):
                raise DuplicateUserError(f"username already exists: {username}") from exc
            raise
        return UserRecord(
            id=uid,
            username=username,
            password_hash=password_hash,
            nickname=eff_nick,
            avatar=avatar,
            is_guest=is_guest,
            status=1,
            created_ms=now,
            last_login_ms=0,
        )

    async def get_user_by_username(self, username: str) -> UserRecord | None:
        sql = (
            "SELECT `id`, `username`, `password_hash`, `nickname`, `avatar`, `is_guest`, "
            "`status`, `created_ms`, `last_login_ms` FROM `sys_user` WHERE `username` = %s LIMIT 1"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (username,))
                row = await cur.fetchone()
        return self._row_to_user(row) if row else None

    async def get_user_by_id(self, user_id: int) -> UserRecord | None:
        sql = (
            "SELECT `id`, `username`, `password_hash`, `nickname`, `avatar`, `is_guest`, "
            "`status`, `created_ms`, `last_login_ms` FROM `sys_user` WHERE `id` = %s LIMIT 1"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (int(user_id),))
                row = await cur.fetchone()
        return self._row_to_user(row) if row else None

    async def touch_last_login(self, user_id: int) -> None:
        sql = "UPDATE `sys_user` SET `last_login_ms` = %s WHERE `id` = %s"
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (self._clock_ms(), int(user_id)))

    async def find_oauth(self, provider: str, open_id: str) -> UserOauthRecord | None:
        sql = (
            "SELECT `id`, `user_id`, `provider`, `open_id`, `union_id`, `created_ms` "
            "FROM `sys_user_oauth` WHERE `provider` = %s AND `open_id` = %s LIMIT 1"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (provider.lower(), open_id))
                row = await cur.fetchone()
        return self._row_to_oauth(row) if row else None

    async def bind_oauth(
        self,
        user_id: int,
        provider: str,
        open_id: str,
        *,
        union_id: str = "",
    ) -> UserOauthRecord:
        norm_provider = provider.lower()
        now = self._clock_ms()
        sql = (
            "INSERT INTO `sys_user_oauth` (`user_id`, `provider`, `open_id`, `union_id`, `created_ms`) "
            "VALUES (%s, %s, %s, %s, %s)"
        )
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, (int(user_id), norm_provider, open_id, union_id, now))
                    oid = int(cur.lastrowid)
        except Exception as exc:
            if _is_duplicate_error(exc):
                existing = await self.find_oauth(norm_provider, open_id)
                if existing is not None and existing.user_id == int(user_id):
                    return existing
                raise DuplicateUserError(f"oauth identity already bound: {norm_provider}:{open_id}") from exc
            raise
        return UserOauthRecord(
            id=oid,
            user_id=int(user_id),
            provider=norm_provider,
            open_id=open_id,
            union_id=union_id,
            created_ms=now,
        )

    async def find_or_create_oauth_user(
        self,
        provider: str,
        open_id: str,
        *,
        nickname: str = "",
        avatar: str = "",
        union_id: str = "",
    ) -> tuple[UserRecord, bool]:
        norm_provider = provider.lower()
        existing = await self.find_oauth(norm_provider, open_id)
        if existing is not None:
            user = await self.get_user_by_id(existing.user_id)
            if user is not None:
                await self.touch_last_login(user.id)
                return user, False

        username = f"{norm_provider}_{secrets.token_hex(8)}"
        user = await self.create_user(
            username=username,
            password_hash="",
            nickname=nickname.strip() if nickname and nickname.strip() else username,
            avatar=avatar,
            is_guest=False,
        )
        try:
            await self.bind_oauth(user.id, norm_provider, open_id, union_id=union_id)
        except DuplicateUserError:
            # 并发首次 OAuth 登录竞争同一 (provider, open_id) 时回读已胜出记录。
            winner = await self.find_oauth(norm_provider, open_id)
            if winner is None:
                raise
            winner_user = await self.get_user_by_id(winner.user_id)
            if winner_user is None:
                raise
            await self.touch_last_login(winner_user.id)
            return winner_user, False
        await self.touch_last_login(user.id)
        return user, True

    async def list_user_oauths(self, user_id: int) -> list[UserOauthRecord]:
        sql = (
            "SELECT `id`, `user_id`, `provider`, `open_id`, `union_id`, `created_ms` "
            "FROM `sys_user_oauth` WHERE `user_id` = %s ORDER BY `id` ASC"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (int(user_id),))
                rows = await cur.fetchall()
        return [self._row_to_oauth(row) for row in rows]


# -- Sa-Token 会话令牌存储（MemoryTokenSessionStore + RedisTokenSessionStore） ------------------------


class TokenSessionStore(ABC):
    """Sa-Token 会话与令牌存储接口（键格式 ``satoken:login:token:<token>`` / ``satoken:login:session:<user_id>``）。"""

    @abstractmethod
    async def create_session(
        self,
        login_id: str,
        *,
        token_name: str = "satoken",
        ttl_sec: int = 86400,
        extra: Mapping[str, Any] | None = None,
    ) -> TokenSession:
        """签发新令牌并写入令牌键与用户会话键。"""

    @abstractmethod
    async def get_session_by_token(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> TokenSession | None:
        """按令牌查询有效会话；过期或不存在返回 ``None``。"""

    @abstractmethod
    async def get_login_id(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> str | None:
        """按令牌快速查询对应 ``login_id``。"""

    @abstractmethod
    async def revoke_token(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> bool:
        """吊销单个令牌并从用户会话键中移除。"""

    @abstractmethod
    async def revoke_user_sessions(
        self,
        login_id: str,
        *,
        token_name: str = "satoken",
    ) -> int:
        """踢下线指定用户的全部会话令牌，返回清理的令牌数量。"""


class MemoryTokenSessionStore(TokenSessionStore):
    """纯内存 Sa-Token 会话存储，严格维护与 Redis 一致的键名布局与 TTL 过期语义。"""

    def __init__(self, *, clock_ms: Callable[[], int] = _now_ms) -> None:
        self._clock_ms = clock_ms
        self._lock = asyncio.Lock()
        # 与 Redis 完全同构的键值对：key -> (value, expires_at_ms)
        self._kv: dict[str, tuple[str, int]] = {}
        self._sessions: dict[str, TokenSession] = {}

    def _evict_expired(self, key: str, now_ms: int) -> bool:
        entry = self._kv.get(key)
        if entry is None:
            return False
        _, exp = entry
        if exp > 0 and now_ms >= exp:
            self._kv.pop(key, None)
            self._sessions.pop(key, None)
            return False
        return True

    def keys_snapshot(self) -> dict[str, str]:
        """返回当前未过期的底层 Sa-Token 键值快照（便于单测断言 Redis 键格式）。"""
        now = self._clock_ms()
        result: dict[str, str] = {}
        for key in list(self._kv.keys()):
            if self._evict_expired(key, now):
                result[key] = self._kv[key][0]
        return result

    async def create_session(
        self,
        login_id: str,
        *,
        token_name: str = "satoken",
        ttl_sec: int = 86400,
        extra: Mapping[str, Any] | None = None,
    ) -> TokenSession:
        norm_login_id = str(login_id).strip()
        if not norm_login_id:
            raise ValueError("login_id must not be empty")
        token_value = secrets.token_urlsafe(32)
        now = self._clock_ms()
        expires_at = now + int(ttl_sec) * 1000
        tkey = token_redis_key(token_name, token_value)
        skey = session_redis_key(token_name, norm_login_id)
        session = TokenSession(
            token_name=token_name,
            token_value=token_value,
            login_id=norm_login_id,
            login_type="login",
            created_ms=now,
            expires_at_ms=expires_at,
            ttl_sec=int(ttl_sec),
            extra=dict(extra or {}),
        )
        async with self._lock:
            self._kv[tkey] = (norm_login_id, expires_at)
            self._sessions[tkey] = session
            # 维护 satoken:login:session:<user_id> 的活跃令牌集合与会话元数据。
            existing_tokens: list[str] = []
            if self._evict_expired(skey, now):
                try:
                    existing_payload = json.loads(self._kv[skey][0])
                    existing_tokens = list(existing_payload.get("tokens") or [])
                except ValueError:
                    existing_tokens = []
            if token_value not in existing_tokens:
                existing_tokens.append(token_value)
            session_doc = json.dumps(
                {
                    "loginId": norm_login_id,
                    "loginType": "login",
                    "tokens": existing_tokens,
                    "updatedMs": now,
                    "extra": dict(extra or {}),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            self._kv[skey] = (session_doc, expires_at)
        return session

    async def get_session_by_token(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> TokenSession | None:
        if not token_value:
            return None
        tkey = token_redis_key(token_name, token_value)
        now = self._clock_ms()
        async with self._lock:
            if not self._evict_expired(tkey, now):
                return None
            return self._sessions.get(tkey)

    async def get_login_id(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> str | None:
        session = await self.get_session_by_token(token_value, token_name=token_name)
        return session.login_id if session is not None else None

    async def revoke_token(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> bool:
        if not token_value:
            return False
        tkey = token_redis_key(token_name, token_value)
        now = self._clock_ms()
        async with self._lock:
            entry = self._kv.pop(tkey, None)
            self._sessions.pop(tkey, None)
            if entry is None:
                return False
            login_id, _ = entry
            skey = session_redis_key(token_name, login_id)
            if self._evict_expired(skey, now):
                try:
                    payload = json.loads(self._kv[skey][0])
                    tokens = [t for t in payload.get("tokens", []) if t != token_value]
                    if tokens:
                        payload["tokens"] = tokens
                        payload["updatedMs"] = now
                        self._kv[skey] = (
                            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                            self._kv[skey][1],
                        )
                    else:
                        self._kv.pop(skey, None)
                except ValueError:
                    self._kv.pop(skey, None)
            return True

    async def revoke_user_sessions(
        self,
        login_id: str,
        *,
        token_name: str = "satoken",
    ) -> int:
        skey = session_redis_key(token_name, str(login_id).strip())
        removed = 0
        async with self._lock:
            entry = self._kv.pop(skey, None)
            if entry is None:
                return 0
            try:
                payload = json.loads(entry[0])
                tokens = list(payload.get("tokens") or [])
            except ValueError:
                tokens = []
            for tok in tokens:
                tkey = token_redis_key(token_name, tok)
                if self._kv.pop(tkey, None) is not None:
                    removed += 1
                self._sessions.pop(tkey, None)
        return removed


class RedisTokenSessionStore(TokenSessionStore):
    """基于 ``redis.asyncio`` 的生产 Sa-Token 会话存储。

    直接写入 ``<token_name>:login:token:<token>`` 与 ``<token_name>:login:session:<user_id>``，
    与 Go login ``SaTokenProvider`` 跨进程零协议差异互通。
    """

    def __init__(self, redis_client: Any, *, clock_ms: Callable[[], int] = _now_ms) -> None:
        if redis_client is None:
            raise ValueError("RedisTokenSessionStore requires a non-null redis client")
        self._redis = redis_client
        self._clock_ms = clock_ms

    @staticmethod
    def _decode(raw: Any) -> str | None:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw.decode("utf-8")
        return str(raw)

    async def create_session(
        self,
        login_id: str,
        *,
        token_name: str = "satoken",
        ttl_sec: int = 86400,
        extra: Mapping[str, Any] | None = None,
    ) -> TokenSession:
        norm_login_id = str(login_id).strip()
        if not norm_login_id:
            raise ValueError("login_id must not be empty")
        token_value = secrets.token_urlsafe(32)
        now = self._clock_ms()
        ttl = int(ttl_sec)
        expires_at = now + ttl * 1000
        tkey = token_redis_key(token_name, token_value)
        skey = session_redis_key(token_name, norm_login_id)

        existing_raw = self._decode(await self._redis.get(skey))
        tokens: list[str] = []
        if existing_raw:
            try:
                existing_doc = json.loads(existing_raw)
                tokens = list(existing_doc.get("tokens") or [])
            except ValueError:
                tokens = []
        if token_value not in tokens:
            tokens.append(token_value)

        session_doc = json.dumps(
            {
                "loginId": norm_login_id,
                "loginType": "login",
                "tokens": tokens,
                "createdMs": now,
                "updatedMs": now,
                "extra": dict(extra or {}),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        await self._redis.set(tkey, norm_login_id, ex=ttl)
        await self._redis.set(skey, session_doc, ex=ttl)

        return TokenSession(
            token_name=token_name,
            token_value=token_value,
            login_id=norm_login_id,
            login_type="login",
            created_ms=now,
            expires_at_ms=expires_at,
            ttl_sec=ttl,
            extra=dict(extra or {}),
        )

    async def get_session_by_token(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> TokenSession | None:
        if not token_value:
            return None
        tkey = token_redis_key(token_name, token_value)
        login_id = self._decode(await self._redis.get(tkey))
        if not login_id:
            return None
        ttl_remaining = await self._redis.ttl(tkey)
        ttl_sec = int(ttl_remaining) if isinstance(ttl_remaining, int) and ttl_remaining > 0 else 0
        now = self._clock_ms()
        skey = session_redis_key(token_name, login_id)
        session_raw = self._decode(await self._redis.get(skey))
        extra: dict[str, Any] = {}
        created_ms = now
        if session_raw:
            try:
                doc = json.loads(session_raw)
                extra = dict(doc.get("extra") or {})
                created_ms = int(doc.get("createdMs", now))
            except ValueError:
                pass
        return TokenSession(
            token_name=token_name,
            token_value=token_value,
            login_id=login_id,
            login_type="login",
            created_ms=created_ms,
            expires_at_ms=now + ttl_sec * 1000 if ttl_sec > 0 else 0,
            ttl_sec=ttl_sec,
            extra=extra,
        )

    async def get_login_id(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> str | None:
        if not token_value:
            return None
        return self._decode(await self._redis.get(token_redis_key(token_name, token_value)))

    async def revoke_token(
        self,
        token_value: str,
        *,
        token_name: str = "satoken",
    ) -> bool:
        if not token_value:
            return False
        tkey = token_redis_key(token_name, token_value)
        login_id = self._decode(await self._redis.get(tkey))
        deleted = await self._redis.delete(tkey)
        if login_id:
            skey = session_redis_key(token_name, login_id)
            session_raw = self._decode(await self._redis.get(skey))
            if session_raw:
                try:
                    doc = json.loads(session_raw)
                    tokens = [t for t in doc.get("tokens", []) if t != token_value]
                    if tokens:
                        doc["tokens"] = tokens
                        doc["updatedMs"] = self._clock_ms()
                        ttl = await self._redis.ttl(skey)
                        ex = int(ttl) if isinstance(ttl, int) and ttl > 0 else None
                        await self._redis.set(
                            skey,
                            json.dumps(doc, ensure_ascii=False, separators=(",", ":")),
                            ex=ex,
                        )
                    else:
                        await self._redis.delete(skey)
                except ValueError:
                    await self._redis.delete(skey)
        return bool(deleted)

    async def revoke_user_sessions(
        self,
        login_id: str,
        *,
        token_name: str = "satoken",
    ) -> int:
        skey = session_redis_key(token_name, str(login_id).strip())
        session_raw = self._decode(await self._redis.get(skey))
        tokens: list[str] = []
        if session_raw:
            try:
                doc = json.loads(session_raw)
                tokens = list(doc.get("tokens") or [])
            except ValueError:
                tokens = []
        keys_to_delete = [token_redis_key(token_name, t) for t in tokens]
        removed = 0
        if keys_to_delete:
            removed = int(await self._redis.delete(*keys_to_delete))
        await self._redis.delete(skey)
        return removed


__all__ = [
    "AuthStore",
    "AuthStoreError",
    "DuplicateUserError",
    "MemoryAuthStore",
    "MemoryTokenSessionStore",
    "RedisTokenSessionStore",
    "SqlAuthStore",
    "TokenSession",
    "TokenSessionStore",
    "UserOauthRecord",
    "UserRecord",
    "hash_password",
    "session_redis_key",
    "token_redis_key",
    "verify_password_hash",
]
