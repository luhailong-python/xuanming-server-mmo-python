"""content_guard 外部平台内容安全审核层（微信 msgSecCheck v2 / 抖音 antidirt 与渠道身份路由器）。

路由与失败口径与 Go ``go/content_guard/internal/platform`` 逐项对齐：
- 按玩家渠道路由：``wx_`` / ``wechat_minigame`` 走微信 msgSecCheck v2，``dy_`` / ``douyin_minigame`` 走抖音 antidirt；
- 私有标签、队伍聊天、私聊不走外部平台审核；
- 名字/公告等同步场景（默认 1500ms）：外部不可用或查渠道失败时 fail-closed 返回 ``UNAVAILABLE``；
- 世界频道在 ``async_platform=True`` 时仅占短预算（默认 250ms），超时或不可用返回 ``PENDING`` 由调用方异步补审
  （``CheckPlatformText`` 走完整 ``async_timeout_ms`` 默认 2500ms）；
- 严禁在任何日志或异常信息中输出玩家原文、AppSecret 或 access_token。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
import json
import logging
import time
from typing import Any, Protocol as TypingProtocol
from urllib.parse import quote, urlencode

import aiohttp

from ..grpc_runtime import GrpcClient
from ..protocol import Protocol
from .config import (
    DEFAULT_ASYNC_TIMEOUT_MS,
    DEFAULT_LOOKUP_TIMEOUT_MS,
    DEFAULT_SYNC_TIMEOUT_MS,
    DEFAULT_WORLD_TIMEOUT_MS,
)
from .service import ReviewOutcome, ReviewResult
from .wordlist import Category

logger = logging.getLogger(__name__)

PROVIDER_WECHAT = "wechat"
PROVIDER_DOUYIN = "douyin"

DEFAULT_WECHAT_BASE_URL = "https://api.weixin.qq.com"
DEFAULT_DOUYIN_BASE_URL = "https://developer.toutiao.com"

DEFAULT_IDENTITY_TTL_SEC = 300.0
IDENTITY_CACHE_MAX_SIZE = 65536
TOKEN_REFRESH_MARGIN_SEC = 300.0
MAX_PLATFORM_RESP_BYTES = 64 * 1024
DEFAULT_HTTP_TIMEOUT_SEC = 5.0

DATA_SERVICE_GET_IDENTITY_METHOD = "/data_service.DataService/GetPlayerChannelIdentity"

# 协议 content_guard.TextScene 枚举值
TEXT_SCENE_ROLE_NAME = 1
TEXT_SCENE_GUILD_NAME = 2
TEXT_SCENE_PET_NAME = 3
TEXT_SCENE_GUILD_ANNOUNCEMENT = 4
TEXT_SCENE_CHAT_WORLD = 5
TEXT_SCENE_CHAT_TEAM = 6
TEXT_SCENE_CHAT_PRIVATE = 7
TEXT_SCENE_PRIVATE_LABEL = 8
TEXT_SCENE_TRADE_TITLE = 9

_PLATFORM_SKIPPED_SCENES = frozenset((
    TEXT_SCENE_PRIVATE_LABEL,
    TEXT_SCENE_CHAT_TEAM,
    TEXT_SCENE_CHAT_PRIVATE,
))
_WECHAT_NAME_SCENES = frozenset((
    TEXT_SCENE_ROLE_NAME,
    TEXT_SCENE_GUILD_NAME,
    TEXT_SCENE_PET_NAME,
    TEXT_SCENE_TRADE_TITLE,
))
_WECHAT_TOKEN_INVALID_CODES = frozenset((40001, 40014, 42001))
_DOUYIN_TOKEN_INVALID_CODES = frozenset((401, 40002, 40020))


@dataclass(frozen=True, slots=True)
class Identity:
    """玩家登录渠道与平台用户标识，来自 account 前缀解析或 DataService 查询。"""

    channel: str = ""
    open_id: str = ""
    account: str = ""


class IdentityLookup(TypingProtocol):
    """根据 ``player_id`` 查询玩家渠道与 openid 的异步接口。"""

    async def lookup_identity(self, player_id: int) -> Identity:
        """查询玩家渠道身份；不存在时返回空 ``Identity()``，故障时抛异常。"""
        ...


class PlatformAdapter(TypingProtocol):
    """单个外部平台（微信 / 抖音）审核适配器接口。"""

    @property
    def provider(self) -> str:
        """返回平台标识（如 ``wechat`` / ``douyin``）。"""
        ...

    async def check(self, scene: int, open_id: str, text: str) -> ReviewResult:
        """向外部平台提交单次文本审核请求。"""
        ...


def parse_channel_account(account: str) -> tuple[str, str]:
    """从登录系统写入的账号名解析渠道与 openid。"""
    if account.startswith("wx_") and len(account) > 3:
        return PROVIDER_WECHAT, account[3:]
    if account.startswith("dy_") and len(account) > 3:
        return PROVIDER_DOUYIN, account[3:]
    return "", ""


def provider_from_identity(identity: Identity) -> tuple[str, str]:
    """从 ``Identity`` 推导平台标识与 openid，优先使用显式 channel 字段。"""
    channel = identity.channel.strip().lower()
    if channel in ("wechat", "wechat_minigame"):
        return PROVIDER_WECHAT, identity.open_id
    if channel in ("douyin", "douyin_minigame"):
        return PROVIDER_DOUYIN, identity.open_id
    provider, open_id = parse_channel_account(identity.account)
    if provider:
        return provider, identity.open_id or open_id
    return "", ""


def is_platform_skipped_scene(scene: int) -> bool:
    """私有标签、队伍聊天、私聊跳过外部平台审核，避免配额耗尽与延迟放大。"""
    return scene in _PLATFORM_SKIPPED_SCENES


def wechat_scene_code(scene: int) -> int:
    """将协议 ``TextScene`` 映射为微信 msgSecCheck v2 ``scene`` 参数（1 资料 / 2 评论 / 3 论坛 / 4 社交日志）。"""
    if scene in _WECHAT_NAME_SCENES:
        return 1
    if scene == TEXT_SCENE_GUILD_ANNOUNCEMENT:
        return 2
    if scene == TEXT_SCENE_CHAT_WORLD:
        return 3
    return 4


def wechat_label_category(label: int) -> Category:
    """将微信 msgSecCheck v2 命中标签映射为统一 ``Category``。"""
    if label == 20001:
        return Category.POLITICS
    if label in (20002, 20003):
        return Category.PORN
    if label in (20012, 20013):
        return Category.ABUSE
    if label == 10001:
        return Category.AD
    return Category.OTHER


class _TokenError(RuntimeError):
    """内部凭据获取失败异常（消息中严禁包含密钥或令牌）。"""


async def _read_json_object(resp: aiohttp.ClientResponse) -> dict[str, Any]:
    """按 64 KiB 上限读取响应体并解析为 JSON 对象。"""
    raw = await resp.content.read(MAX_PLATFORM_RESP_BYTES + 1)
    if len(raw) > MAX_PLATFORM_RESP_BYTES:
        raise ValueError("platform response exceeded max bytes")
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("platform response must be a json object")
    return parsed


class _TokenClientBase:
    """封装带提前刷新窗口的 access_token 内存缓存与 ``aiohttp.ClientSession`` 生命周期。"""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        base_url: str,
        default_base_url: str,
        *,
        session: aiohttp.ClientSession | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._base_url = (base_url or default_base_url).rstrip("/")
        self._session = session
        self._owns_session = session is None
        self._clock = monotonic_clock
        self._token_lock = asyncio.Lock()
        self._token = ""
        self._expires_at = 0.0

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=DEFAULT_HTTP_TIMEOUT_SEC),
            )
            self._owns_session = True
        return self._session

    async def _fetch_token(self) -> tuple[str, int]:
        raise NotImplementedError

    async def _access_token(self) -> str:
        now = self._clock()
        if self._token and now + TOKEN_REFRESH_MARGIN_SEC < self._expires_at:
            return self._token
        async with self._token_lock:
            now = self._clock()
            if self._token and now + TOKEN_REFRESH_MARGIN_SEC < self._expires_at:
                return self._token
            token, expires_in = await self._fetch_token()
            ttl_sec = float(expires_in)
            if ttl_sec <= TOKEN_REFRESH_MARGIN_SEC:
                ttl_sec = TOKEN_REFRESH_MARGIN_SEC + 60.0
            self._token = token
            self._expires_at = self._clock() + ttl_sec
            return token

    def _invalidate_token(self, stale: str) -> None:
        if self._token == stale:
            self._token = ""
            self._expires_at = 0.0

    async def close(self) -> None:
        """关闭自建 HTTP 会话；外部注入会话由调用方自行管理。"""
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()


class WeChatAdapter(_TokenClientBase):
    """微信小游戏内容安全接口（msgSecCheck v2）适配器。"""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        *,
        base_url: str = DEFAULT_WECHAT_BASE_URL,
        session: aiohttp.ClientSession | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(
            app_id,
            app_secret,
            base_url,
            DEFAULT_WECHAT_BASE_URL,
            session=session,
            monotonic_clock=monotonic_clock,
        )

    @property
    def provider(self) -> str:
        return PROVIDER_WECHAT

    async def _fetch_token(self) -> tuple[str, int]:
        query = urlencode({
            "grant_type": "client_credential",
            "appid": self._app_id,
            "secret": self._app_secret,
        })
        endpoint = f"{self._base_url}/cgi-bin/token?{query}"
        try:
            async with self._get_session().get(endpoint) as resp:
                if resp.status != 200:
                    raise _TokenError(f"http_status={resp.status}")
                out = await _read_json_object(resp)
        except _TokenError:
            raise
        except Exception as exc:
            raise _TokenError(f"error={type(exc).__name__}") from exc

        try:
            errcode = int(out.get("errcode") or 0)
            access_token = str(out.get("access_token") or "")
            expires_in = int(out.get("expires_in") or 0)
            errmsg = str(out.get("errmsg") or "")
        except (TypeError, ValueError) as exc:
            raise _TokenError("error=invalid_token_payload") from exc

        if errcode != 0 or not access_token:
            raise _TokenError(f"errcode={errcode} errmsg={errmsg}")
        return access_token, expires_in

    async def check(self, scene: int, open_id: str, text: str) -> ReviewResult:
        """调用微信 ``/wxa/msg_sec_check``；缺 openid 时跳过外部层，凭据失效时自动刷新并重试一次。"""
        if not open_id:
            return ReviewResult(ReviewOutcome.SKIP, provider=PROVIDER_WECHAT)

        payload = json.dumps(
            {
                "content": text,
                "version": 2,
                "scene": wechat_scene_code(scene),
                "openid": open_id,
            },
            ensure_ascii=False,
        ).encode("utf-8")

        for _attempt in range(2):
            try:
                token = await self._access_token()
            except _TokenError as exc:
                logger.error("content_guard wechat token fetch failed %s", exc)
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)

            endpoint = f"{self._base_url}/wxa/msg_sec_check?access_token={quote(token, safe='')}"
            try:
                async with self._get_session().post(
                    endpoint,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    if resp.status != 200:
                        logger.error("content_guard wechat msg_sec_check failed status=%d", resp.status)
                        return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)
                    out = await _read_json_object(resp)
            except (ValueError, json.JSONDecodeError):
                logger.error("content_guard wechat msg_sec_check invalid json")
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)
            except Exception as exc:
                logger.error("content_guard wechat msg_sec_check request failed error=%s", type(exc).__name__)
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)

            try:
                errcode = int(out.get("errcode") or 0)
                errmsg = str(out.get("errmsg") or "")
            except (TypeError, ValueError):
                logger.error("content_guard wechat msg_sec_check invalid errcode")
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)

            if errcode != 0:
                if errcode in _WECHAT_TOKEN_INVALID_CODES:
                    self._invalidate_token(token)
                    continue
                logger.error("content_guard wechat msg_sec_check errcode=%d errmsg=%s", errcode, errmsg)
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)

            result_obj = out.get("result")
            if not isinstance(result_obj, dict):
                result_obj = {}
            suggest = str(result_obj.get("suggest") or "")
            try:
                label = int(result_obj.get("label") or 0)
            except (TypeError, ValueError):
                label = 0
            if suggest == "pass":
                return ReviewResult(ReviewOutcome.PASS, provider=PROVIDER_WECHAT)
            return ReviewResult(
                ReviewOutcome.REJECT,
                category=wechat_label_category(label),
                provider=PROVIDER_WECHAT,
            )

        return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_WECHAT)


class DouyinAdapter(_TokenClientBase):
    """抖音小游戏内容安全检测接口（antidirt）适配器。"""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        *,
        base_url: str = DEFAULT_DOUYIN_BASE_URL,
        session: aiohttp.ClientSession | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(
            app_id,
            app_secret,
            base_url,
            DEFAULT_DOUYIN_BASE_URL,
            session=session,
            monotonic_clock=monotonic_clock,
        )

    @property
    def provider(self) -> str:
        return PROVIDER_DOUYIN

    async def _fetch_token(self) -> tuple[str, int]:
        payload = json.dumps(
            {
                "appid": self._app_id,
                "secret": self._app_secret,
                "grant_type": "client_credential",
            },
            ensure_ascii=False,
        ).encode("utf-8")
        endpoint = f"{self._base_url}/api/apps/v2/token"
        try:
            async with self._get_session().post(
                endpoint,
                data=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status != 200:
                    raise _TokenError(f"http_status={resp.status}")
                out = await _read_json_object(resp)
        except _TokenError:
            raise
        except Exception as exc:
            raise _TokenError(f"error={type(exc).__name__}") from exc

        try:
            err_no = int(out.get("err_no") or 0)
            err_tips = str(out.get("err_tips") or "")
            data = out.get("data")
            if not isinstance(data, dict):
                data = {}
            access_token = str(data.get("access_token") or "")
            expires_in = int(data.get("expires_in") or 0)
        except (TypeError, ValueError) as exc:
            raise _TokenError("error=invalid_token_payload") from exc

        if err_no != 0 or not access_token:
            raise _TokenError(f"err_no={err_no} err_tips={err_tips}")
        return access_token, expires_in

    async def check(self, scene: int, open_id: str, text: str) -> ReviewResult:
        """调用抖音 ``/api/v2/tags/text/antidirt``；HTTP 401 或凭据错误码时自动刷新并重试一次。"""
        _ = (scene, open_id)
        payload = json.dumps({"tasks": [{"content": text}]}, ensure_ascii=False).encode("utf-8")
        endpoint = f"{self._base_url}/api/v2/tags/text/antidirt"

        for _attempt in range(2):
            try:
                token = await self._access_token()
            except _TokenError as exc:
                logger.error("content_guard douyin token fetch failed %s", exc)
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)

            try:
                async with self._get_session().post(
                    endpoint,
                    data=payload,
                    headers={
                        "Content-Type": "application/json",
                        "X-Token": token,
                    },
                ) as resp:
                    if resp.status == 401:
                        self._invalidate_token(token)
                        continue
                    if resp.status != 200:
                        logger.error("content_guard douyin antidirt failed status=%d", resp.status)
                        return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)
                    out = await _read_json_object(resp)
            except (ValueError, json.JSONDecodeError):
                logger.error("content_guard douyin antidirt invalid json")
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)
            except Exception as exc:
                logger.error("content_guard douyin antidirt request failed error=%s", type(exc).__name__)
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)

            try:
                code = int(out.get("code") or 0)
                message = str(out.get("message") or "")
            except (TypeError, ValueError):
                logger.error("content_guard douyin antidirt invalid code")
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)

            if code in _DOUYIN_TOKEN_INVALID_CODES:
                self._invalidate_token(token)
                continue

            items = out.get("data")
            if code != 0 or not isinstance(items, list) or len(items) == 0:
                logger.error("content_guard douyin antidirt failed code=%d message=%s", code, message)
                return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)

            for item in items:
                if not isinstance(item, dict):
                    return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)
                try:
                    item_code = int(item.get("code") or 0)
                except (TypeError, ValueError):
                    return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)
                if item_code != 0:
                    return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)
                predicts = item.get("predicts")
                if not isinstance(predicts, list):
                    continue
                for predict in predicts:
                    if not isinstance(predict, dict):
                        continue
                    hit = bool(predict.get("hit", False))
                    try:
                        prob = float(predict.get("prob") or 0.0)
                    except (TypeError, ValueError):
                        return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)
                    if hit or prob >= 1.0:
                        return ReviewResult(
                            ReviewOutcome.REJECT,
                            category=Category.OTHER,
                            provider=PROVIDER_DOUYIN,
                        )
            return ReviewResult(ReviewOutcome.PASS, provider=PROVIDER_DOUYIN)

        return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=PROVIDER_DOUYIN)


class GrpcIdentityLookup:
    """通过 ``DataService.GetPlayerChannelIdentity`` 查询玩家登录渠道与 open_id。"""

    def __init__(
        self,
        protocol: Protocol,
        endpoint: str,
        *,
        timeout_sec: float = DEFAULT_LOOKUP_TIMEOUT_MS / 1000.0,
        client: GrpcClient | None = None,
    ) -> None:
        self._protocol = protocol
        self._owns_client = client is None
        self._timeout_sec = timeout_sec if timeout_sec > 0 else DEFAULT_LOOKUP_TIMEOUT_MS / 1000.0
        self._client = client if client is not None else GrpcClient(endpoint, timeout=self._timeout_sec)

    async def lookup_identity(self, player_id: int) -> Identity:
        """发起单次 gRPC 查询；服务端请求重试或传输异常时抛错触发上层 fail-closed。"""
        req = self._protocol.message("data_service.GetPlayerChannelIdentityRequest", player_id=player_id)
        raw = await self._client.call(
            DATA_SERVICE_GET_IDENTITY_METHOD,
            req.SerializeToString(deterministic=True),
            timeout=self._timeout_sec,
        )
        resp = self._protocol.parse("data_service.GetPlayerChannelIdentityResponse", raw)
        if resp.retry:
            raise RuntimeError(f"GetPlayerChannelIdentity requested retry player={player_id}")
        if not resp.found:
            return Identity()
        return Identity(channel=resp.channel, open_id=resp.open_id, account=resp.account)

    async def close(self) -> None:
        """关闭自建 gRPC 客户端通道。"""
        if self._owns_client:
            await self._client.close()


class _IdentityCache:
    """带 TTL 与容量上限的玩家渠道身份内存缓存。"""

    def __init__(
        self,
        lookup: IdentityLookup | Callable[[int], Awaitable[Identity]],
        *,
        ttl_sec: float = DEFAULT_IDENTITY_TTL_SEC,
        timeout_sec: float = DEFAULT_LOOKUP_TIMEOUT_MS / 1000.0,
        max_size: int = IDENTITY_CACHE_MAX_SIZE,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lookup = lookup
        self._ttl_sec = ttl_sec if ttl_sec > 0 else DEFAULT_IDENTITY_TTL_SEC
        self._timeout_sec = timeout_sec if timeout_sec > 0 else DEFAULT_LOOKUP_TIMEOUT_MS / 1000.0
        self._max_size = max_size if max_size > 0 else IDENTITY_CACHE_MAX_SIZE
        self._clock = monotonic_clock
        self._entries: dict[int, tuple[Identity, float]] = {}

    async def get(self, player_id: int) -> Identity:
        if player_id <= 0:
            return Identity()
        now = self._clock()
        cached = self._entries.get(player_id)
        if cached is not None and now < cached[1]:
            return cached[0]

        call = self._lookup.lookup_identity if hasattr(self._lookup, "lookup_identity") else self._lookup
        identity = await asyncio.wait_for(call(player_id), timeout=self._timeout_sec)
        if identity.channel or identity.account or identity.open_id:
            if player_id not in self._entries and len(self._entries) >= self._max_size:
                self._entries.clear()
            self._entries[player_id] = (identity, now + self._ttl_sec)
        return identity

    async def close(self) -> None:
        close_fn = getattr(self._lookup, "close", None)
        if callable(close_fn):
            await close_fn()


class PlatformRouter:
    """按场景策略与玩家渠道分发到对应外部平台适配器，实现 ``PlatformReviewer`` 协议。"""

    def __init__(
        self,
        adapters: Iterable[PlatformAdapter],
        *,
        lookup: IdentityLookup | Callable[[int], Awaitable[Identity]] | None = None,
        sync_timeout_ms: int = DEFAULT_SYNC_TIMEOUT_MS,
        world_timeout_ms: int = DEFAULT_WORLD_TIMEOUT_MS,
        async_timeout_ms: int = DEFAULT_ASYNC_TIMEOUT_MS,
        lookup_timeout_ms: int = DEFAULT_LOOKUP_TIMEOUT_MS,
        identity_ttl_sec: float = DEFAULT_IDENTITY_TTL_SEC,
        identity_cache_max_size: int = IDENTITY_CACHE_MAX_SIZE,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._adapters: dict[str, PlatformAdapter] = {}
        for adapter in adapters:
            if adapter is not None and adapter.provider:
                self._adapters[adapter.provider] = adapter

        self._sync_timeout_sec = (sync_timeout_ms if sync_timeout_ms > 0 else DEFAULT_SYNC_TIMEOUT_MS) / 1000.0
        self._world_timeout_sec = (world_timeout_ms if world_timeout_ms > 0 else DEFAULT_WORLD_TIMEOUT_MS) / 1000.0
        self._async_timeout_sec = (async_timeout_ms if async_timeout_ms > 0 else DEFAULT_ASYNC_TIMEOUT_MS) / 1000.0
        lookup_timeout_sec = (lookup_timeout_ms if lookup_timeout_ms > 0 else DEFAULT_LOOKUP_TIMEOUT_MS) / 1000.0

        self._identities = (
            _IdentityCache(
                lookup,
                ttl_sec=identity_ttl_sec,
                timeout_sec=lookup_timeout_sec,
                max_size=identity_cache_max_size,
                monotonic_clock=monotonic_clock,
            )
            if lookup is not None
            else None
        )

    @property
    def providers(self) -> tuple[str, ...]:
        """返回已装配的外部平台标识列表（按字典序）。"""
        return tuple(sorted(self._adapters))

    async def _resolve_adapter(self, player_id: int, account: str) -> tuple[PlatformAdapter | None, str]:
        if account:
            provider, open_id = parse_channel_account(account)
            if provider:
                return self._adapters.get(provider), open_id
        if player_id > 0 and self._identities is not None:
            identity = await self._identities.get(player_id)
            provider, open_id = provider_from_identity(identity)
            if provider:
                return self._adapters.get(provider), open_id
            return None, ""
        if player_id == 0 and account == "":
            douyin = self._adapters.get(PROVIDER_DOUYIN)
            if douyin is not None:
                return douyin, ""
        return None, ""

    async def _check_with_timeout(
        self,
        adapter: PlatformAdapter,
        scene: int,
        open_id: str,
        text: str,
        timeout_sec: float,
    ) -> ReviewResult:
        try:
            return await asyncio.wait_for(adapter.check(scene, open_id, text), timeout=timeout_sec)
        except TimeoutError:
            logger.error(
                "content_guard platform check timed out provider=%s scene=%d timeout_ms=%d",
                adapter.provider,
                scene,
                int(timeout_sec * 1000),
            )
            return ReviewResult(ReviewOutcome.UNAVAILABLE, provider=adapter.provider)

    async def review(
        self,
        scene: int,
        player_id: int,
        account: str,
        text: str,
        async_platform: bool,
    ) -> ReviewResult:
        """按场景与渠道调用对应平台审核；世界频道短超时且 ``async_platform=True`` 时回 ``PENDING``。"""
        if not self._adapters or not text or is_platform_skipped_scene(scene):
            return ReviewResult(ReviewOutcome.SKIP)

        try:
            adapter, open_id = await self._resolve_adapter(player_id, account)
        except Exception as exc:
            if scene == TEXT_SCENE_CHAT_WORLD:
                logger.error(
                    "content_guard platform lookup identity failed for world chat player=%d error=%s",
                    player_id,
                    type(exc).__name__,
                )
                return ReviewResult(ReviewOutcome.PENDING if async_platform else ReviewOutcome.SKIP)
            logger.error(
                "content_guard platform lookup identity failed for sync review player=%d error=%s",
                player_id,
                type(exc).__name__,
            )
            return ReviewResult(ReviewOutcome.UNAVAILABLE)

        if adapter is None:
            return ReviewResult(ReviewOutcome.SKIP)
        if adapter.provider == PROVIDER_WECHAT and not open_id:
            return ReviewResult(ReviewOutcome.SKIP, provider=PROVIDER_WECHAT)

        if scene == TEXT_SCENE_CHAT_WORLD:
            res = await self._check_with_timeout(adapter, scene, open_id, text, self._world_timeout_sec)
            if res.outcome == ReviewOutcome.UNAVAILABLE:
                return ReviewResult(
                    ReviewOutcome.PENDING if async_platform else ReviewOutcome.SKIP,
                    provider=res.provider,
                )
            return res

        return await self._check_with_timeout(adapter, scene, open_id, text, self._sync_timeout_sec)

    async def review_sync(
        self,
        scene: int,
        player_id: int,
        account: str,
        text: str,
    ) -> ReviewResult:
        """使用完整同步/异步预算执行外部平台审核（供 ``CheckPlatformText`` RPC 调用）。"""
        if not self._adapters or not text or is_platform_skipped_scene(scene):
            return ReviewResult(ReviewOutcome.SKIP)

        try:
            adapter, open_id = await self._resolve_adapter(player_id, account)
        except Exception as exc:
            logger.error(
                "content_guard platform lookup identity failed during sync review player=%d error=%s",
                player_id,
                type(exc).__name__,
            )
            return ReviewResult(ReviewOutcome.UNAVAILABLE)

        if adapter is None:
            return ReviewResult(ReviewOutcome.SKIP)
        if adapter.provider == PROVIDER_WECHAT and not open_id:
            return ReviewResult(ReviewOutcome.SKIP, provider=PROVIDER_WECHAT)

        budget_sec = self._async_timeout_sec if scene == TEXT_SCENE_CHAT_WORLD else self._sync_timeout_sec
        return await self._check_with_timeout(adapter, scene, open_id, text, budget_sec)

    async def close(self) -> None:
        """释放已装配适配器与身份查询客户端占用的连接资源。"""
        for adapter in self._adapters.values():
            close_fn = getattr(adapter, "close", None)
            if callable(close_fn):
                await close_fn()
        if self._identities is not None:
            await self._identities.close()
