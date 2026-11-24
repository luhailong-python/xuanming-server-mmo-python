"""content_guard gRPC 服务适配层（``content_guard.ContentGuard``）。

对外暴露无鉴权内部只读方法 ``CheckText`` 与 ``CheckPlatformText``：
- 严格校验场景枚举 -> UTF-8 字节上限 -> 词库就绪状态 -> 本地引擎判定 -> 可选外部平台复审；
- 严禁在任何日志或错误消息中输出玩家正文或打码文本。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum
import inspect
from typing import Protocol as TypingProtocol

import grpc
from google.protobuf.message import Message

from ..protocol import Protocol
from .config import DEFAULT_MAX_TEXT_BYTES
from .engine import Engine, SceneClass, Verdict
from .wordlist import Category

SERVICE_NAME = "content_guard.ContentGuard"
CHECK_TEXT_FULL_METHOD = "/content_guard.ContentGuard/CheckText"
CHECK_PLATFORM_TEXT_FULL_METHOD = "/content_guard.ContentGuard/CheckPlatformText"

CODE = grpc.StatusCode

# 协议 TextScene 枚举值 -> (引擎动作大类, 指标场景名)；增补 proto 场景时须同步维护此表。
SCENE_CLASS_MAP: dict[int, tuple[SceneClass, str]] = {
    1: (SceneClass.NAME, "role_name"),             # TEXT_SCENE_ROLE_NAME
    2: (SceneClass.NAME, "guild_name"),            # TEXT_SCENE_GUILD_NAME
    3: (SceneClass.NAME, "pet_name"),              # TEXT_SCENE_PET_NAME
    4: (SceneClass.NOTICE, "guild_announcement"),  # TEXT_SCENE_GUILD_ANNOUNCEMENT
    5: (SceneClass.CHAT, "chat_world"),            # TEXT_SCENE_CHAT_WORLD
    6: (SceneClass.CHAT, "chat_team"),             # TEXT_SCENE_CHAT_TEAM
    7: (SceneClass.CHAT, "chat_private"),          # TEXT_SCENE_CHAT_PRIVATE
    8: (SceneClass.PRIVATE_LABEL, "private_label"),  # TEXT_SCENE_PRIVATE_LABEL
    9: (SceneClass.NAME, "trade_title"),           # TEXT_SCENE_TRADE_TITLE
}


class ReviewOutcome(IntEnum):
    """外部平台内容审核结论。"""

    SKIP = 0
    PASS = 1
    REJECT = 2
    UNAVAILABLE = 3
    PENDING = 4


@dataclass(frozen=True, slots=True)
class ReviewResult:
    """外部平台单次审核结果。"""

    outcome: ReviewOutcome = ReviewOutcome.SKIP
    category: Category = Category.UNSPECIFIED
    provider: str = ""


class PlatformReviewer(TypingProtocol):
    """可选外部平台审核接口（微信 msgSecCheck / 抖音 antidirt 等通过依赖注入接入）。"""

    def review(
        self,
        scene: int,
        player_id: int,
        account: str,
        text: str,
        async_platform: bool,
    ) -> Awaitable[ReviewResult] | ReviewResult:
        """在本地词库放行或打码后执行平台审核。"""
        ...

    def review_sync(
        self,
        scene: int,
        player_id: int,
        account: str,
        text: str,
    ) -> Awaitable[ReviewResult] | ReviewResult:
        """以同步完整预算执行平台复审（供 ``CheckPlatformText`` 使用）。"""
        ...


def class_of(scene: int) -> tuple[SceneClass, str] | None:
    """将协议 ``TextScene`` 枚举值映射为引擎 ``SceneClass`` 与规范场景名。"""
    return SCENE_CLASS_MAP.get(scene)


class ContentGuardService:
    """实现 ``content_guard.ContentGuard`` 的全部 RPC 方法。"""

    def __init__(
        self,
        protocol: Protocol,
        engine: Engine,
        *,
        require_lexicon: bool = False,
        max_text_bytes: int = DEFAULT_MAX_TEXT_BYTES,
        reviewer: PlatformReviewer | None = None,
    ) -> None:
        self.protocol = protocol
        self.engine = engine
        self._require_lexicon = require_lexicon
        self._max_text_bytes = max_text_bytes
        self._reviewer = reviewer

    def ready(self) -> bool:
        """报告本实例是否具备对外判定能力；要求词库时必须已加载非 0 版本。"""
        return not self._require_lexicon or self.engine.current().version != 0

    def services(self) -> dict[str, dict[str, Callable]]:
        """返回供 ``serve_grpc`` 注册的服务方法映射。"""
        return {
            SERVICE_NAME: {
                "CheckText": self.check_text,
                "CheckPlatformText": self.check_platform_text,
            }
        }

    async def _validate_and_local_check(self, scene: int, text: str, context) -> Message:
        mapped = class_of(scene)
        if mapped is None:
            await context.abort(CODE.INVALID_ARGUMENT, "unknown text scene")
        scene_class, _scene_name = mapped

        text_bytes = len(text.encode("utf-8"))
        if text_bytes > self._max_text_bytes:
            await context.abort(
                CODE.INVALID_ARGUMENT,
                f"text is {text_bytes} bytes, the limit is {self._max_text_bytes}",
            )
        if not self.ready():
            await context.abort(CODE.UNAVAILABLE, "lexicon not loaded")

        result = self.engine.check(scene_class, text)
        response = self.protocol.message(
            "content_guard.CheckTextResponse",
            verdict=int(result.verdict),
            masked_text=result.masked if result.verdict == Verdict.MASKED else "",
            category=int(result.category),
            lexicon_version=result.version,
        )
        return response

    async def _apply_platform_result(self, response: Message, ext: ReviewResult, context) -> None:
        if ext.outcome == ReviewOutcome.REJECT:
            response.verdict = int(Verdict.REJECT)
            response.masked_text = ""
            if ext.category != Category.UNSPECIFIED:
                response.category = int(ext.category)
        elif ext.outcome == ReviewOutcome.PENDING:
            response.pending_platform_review = True
        elif ext.outcome == ReviewOutcome.UNAVAILABLE:
            await context.abort(CODE.UNAVAILABLE, "external content reviewer unavailable")

    async def check_text(self, request: Message, context, path: str) -> Message:
        """本地词库判定 + 可选外部平台审核。"""
        response = await self._validate_and_local_check(int(request.scene), request.text, context)
        if response.verdict != int(Verdict.REJECT) and self._reviewer is not None:
            ext = self._reviewer.review(
                int(request.scene),
                int(request.player_id),
                request.account,
                request.text,
                bool(request.async_platform),
            )
            if inspect.isawaitable(ext):
                ext = await ext
            await self._apply_platform_result(response, ext, context)
        return response

    async def check_platform_text(self, request: Message, context, path: str) -> Message:
        """本地校验后同步执行外部平台复审（供世界频道超时先放行后后台复检）。"""
        response = await self._validate_and_local_check(int(request.scene), request.text, context)
        if response.verdict != int(Verdict.REJECT) and self._reviewer is not None:
            ext = self._reviewer.review_sync(
                int(request.scene),
                int(request.player_id),
                request.account,
                request.text,
            )
            if inspect.isawaitable(ext):
                ext = await ext
            await self._apply_platform_result(response, ext, context)
        return response
