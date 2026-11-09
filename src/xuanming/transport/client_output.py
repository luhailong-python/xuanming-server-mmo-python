"""保持客户端消息队列接口，同时跟踪等待写出的帧字节。"""

import asyncio
from collections import deque

from .framing import message_frame_size


class ClientOutputQueue(asyncio.Queue):
    def __init__(self, maxsize: int = 128) -> None:
        super().__init__(maxsize)
        self.buffered_bytes = 0
        self._frame_sizes: deque[int] = deque()

    def put_nowait(self, message) -> None:
        size = message_frame_size(message)
        super().put_nowait(message)
        self._frame_sizes.append(size)
        self.buffered_bytes += size

    def get_nowait(self):
        message = super().get_nowait()
        self.buffered_bytes -= self._frame_sizes.popleft()
        return message
