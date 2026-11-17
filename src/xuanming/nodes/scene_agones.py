"""Scene Agones readiness, health, and planned-drain signal."""

import asyncio
import logging

from .battle_agones import BattleAgones

logger = logging.getLogger(__name__)


class SceneAgones(BattleAgones):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._label_triggered = False
        self._shutdown_sent = False

    async def watch_drain(self, request_shutdown):
        while not self._label_triggered:
            if self._draining:
                self._label_triggered = True
                request_shutdown()
                # Runtime treats an unexpectedly completed background task as
                # a failure; remain alive until normal shutdown cancels us.
                await asyncio.Future()
            await asyncio.sleep(1)

    async def close(self):
        if self._label_triggered and not self._shutdown_sent:
            self._shutdown_sent = True
            try:
                await self._request("shutdown")
            except self._http_errors:
                logger.warning("Scene Agones 请求停止失败", exc_info=True)
        await super().close()
