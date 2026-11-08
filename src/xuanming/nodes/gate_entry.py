"""Gate 场景入场欠账的预算和退避。"""

import asyncio
from dataclasses import dataclass

from google.protobuf.message import Message

from ..node_link import SceneLinkNotReady, SceneNodeNotFound
from ..transport import RpcBusy, RpcNotSent

ENTRY_BUDGET = 20.0
DISCOVERY_BUDGET = 3.0
INITIAL_BACKOFF = 0.25
MAX_BACKOFF = 2.0
MAX_ATTEMPTS = 16
FORCE_CLOSE_DELAY = 1.0


@dataclass(slots=True)
class PendingSceneEntry:
    event: Message
    enter_type: int
    started: float
    binding: tuple | None
    task: asyncio.Task | None = None
    attempted_send: bool = False
    attempts: int = 0
    first_failure: str = ""
    last_failure: str = ""

    def cancel_if_unsent(self) -> None:
        if self.task is not None and not self.attempted_send:
            self.task.cancel()

    def record_failure(self, error: Exception) -> None:
        self.last_failure = type(error).__name__
        if not self.first_failure:
            self.first_failure = self.last_failure


async def retry_entry(entry: PendingSceneEntry, current, attempt, give_up) -> None:
    loop = asyncio.get_running_loop()
    deadline = entry.started + ENTRY_BUDGET
    discovery_deadline = entry.started + DISCOVERY_BUDGET
    delay = INITIAL_BACKOFF
    for index in range(MAX_ATTEMPTS):
        if not current():
            return
        try:
            entry.attempts += 1
            await attempt(max(deadline - loop.time(), 0.001))
            return
        except SceneNodeNotFound as error:
            entry.record_failure(error)
            entry.attempted_send = False
            limit = min(discovery_deadline, deadline)
        except (SceneLinkNotReady, RpcBusy, RpcNotSent) as error:
            entry.record_failure(error)
            entry.attempted_send = False
            limit = deadline
        except Exception as error:
            # 已发送或无法证明未发送的请求不重放。
            entry.record_failure(error)
            break
        if not current():
            return
        now = loop.time()
        if now >= limit or index + 1 >= MAX_ATTEMPTS:
            break
        await asyncio.sleep(min(delay, limit - now))
        delay = min(delay * 2, MAX_BACKOFF)
    if current():
        await give_up()
