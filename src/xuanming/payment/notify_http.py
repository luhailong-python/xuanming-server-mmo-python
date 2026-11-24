"""渠道回调 HTTP 入口（aiohttp）：``POST /notify/{channel}`` 与小游戏 ``GET /notify/{channel}`` 握手。

与 Go ``go/payment/internal/notify/notify.go`` 对齐：
- 请求体上限 256 KiB；超限或验签失败按渠道约定回 ``AckKind.REJECTED``；
- 仅等事实落库后应答 ``AckKind.OK``，不等待商城推送，满足微信 5s / 小游戏 3s 时限；
- 落库失败或远端验签临时不可达回 ``AckKind.RETRY`` 让渠道按幂等重试。
"""

from __future__ import annotations

import asyncio
import logging

from aiohttp import web

from .channels import HandshakeChannel
from .errors import BadSignatureError, TempUnavailableError
from .model import AckKind
from .service import PaymentCore

logger = logging.getLogger(__name__)

MAX_BODY_BYTES = 256 * 1024
PARSE_TIMEOUT_SEC = 5.0
HANDLE_TIMEOUT_SEC = 2.5
RECEIPT_HANDLE_TIMEOUT_SEC = 8.0


class PaymentNotifyServer:
    """基于 aiohttp 的独立回调 HTTP 服务器。"""

    def __init__(self, core: PaymentCore, *, host: str = "127.0.0.1", port: int = 18130) -> None:
        self._core = core
        self._host = host
        self._port = port
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._bound_port: int = 0
        self._app = web.Application(client_max_size=MAX_BODY_BYTES + 65536)
        self._app.router.add_route("*", "/notify/{channel}", self._handle_notify)

    @property
    def bound_port(self) -> int:
        return self._bound_port

    async def start(self) -> int:
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()
        sockets = self._site._server.sockets if self._site._server is not None else ()
        self._bound_port = int(sockets[0].getsockname()[1]) if sockets else self._port
        logger.info("payment notify HTTP listening on %s:%d", self._host, self._bound_port)
        return self._bound_port

    async def close(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
            self._bound_port = 0

    async def _handle_notify(self, request: web.Request) -> web.Response:
        channel_name = request.match_info.get("channel", "")
        ch = self._core.channel_by_name(channel_name)
        if ch is None:
            return web.Response(status=404, text="not found")

        if request.method == "GET":
            if isinstance(ch, HandshakeChannel):
                res = ch.handshake(request.query)
                if res is not None:
                    status, body, content_type = res
                    headers = {"Content-Type": content_type} if content_type else None
                    return web.Response(status=status, body=body, headers=headers)
            return web.Response(status=405, text="method not allowed")

        if request.method != "POST":
            return web.Response(status=405, text="method not allowed")

        def ack(kind: AckKind) -> web.Response:
            status, body, content_type = ch.format_ack(kind)
            headers = {"Content-Type": content_type} if content_type else None
            return web.Response(status=status, body=body, headers=headers)

        try:
            body = await request.content.read(MAX_BODY_BYTES + 1)
        except Exception as exc:
            logger.warning("payment notify read body failed channel=%s: %s", ch.name, exc)
            return ack(AckKind.REJECTED)

        if len(body) > MAX_BODY_BYTES:
            logger.warning("payment notify body exceeded limit channel=%s size=%d", ch.name, len(body))
            return ack(AckKind.REJECTED)

        try:
            async with asyncio.timeout(PARSE_TIMEOUT_SEC):
                note = await ch.parse_notification(request.headers, request.query, body)
        except TempUnavailableError as exc:
            logger.error("payment notify verify temp unavailable channel=%s: %s", ch.name, exc)
            return ack(AckKind.RETRY)
        except BadSignatureError:
            return ack(AckKind.REJECTED)
        except Exception as exc:
            logger.error("payment notify parse failed channel=%s: %s", ch.name, exc)
            return ack(AckKind.REJECTED)

        budget = RECEIPT_HANDLE_TIMEOUT_SEC if note.receipts else HANDLE_TIMEOUT_SEC
        try:
            async with asyncio.timeout(budget):
                await asyncio.shield(self._core.handle_notification(ch, note))
        except Exception as exc:
            logger.error("payment notify handle failed channel=%s: %s", ch.name, exc)
            return ack(AckKind.RETRY)

        return ack(AckKind.OK)
