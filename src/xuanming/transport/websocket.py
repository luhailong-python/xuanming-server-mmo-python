"""WebSocket adapters carrying one original muduo packet per binary message."""

from __future__ import annotations

import asyncio

from websockets.exceptions import ConnectionClosed

from .framing import DEFAULT_GATE_FRAME_SIZE, FrameError, decode_packet

_MAX_OUTGOING_SIZE = (1 << 31) - 1


class WebSocketPacketReader:
    def __init__(self, websocket, *, max_size: int = DEFAULT_GATE_FRAME_SIZE) -> None:
        self.websocket = websocket
        self.max_size = max_size
        self._buffer = bytearray()

    async def readexactly(self, size: int) -> bytes:
        if not self._buffer:
            try:
                frame = await self.websocket.recv()
            except ConnectionClosed as exc:
                raise asyncio.IncompleteReadError(b"", size) from exc
            if not isinstance(frame, bytes):
                raise FrameError("WebSocket 客户端只允许发送二进制消息")
            decode_packet(frame, max_size=self.max_size)
            self._buffer.extend(frame)
        if len(self._buffer) < size:
            raise FrameError("一个 WebSocket 消息必须只包含一条完整协议帧")
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result


class _WebSocketTransport:
    def __init__(self, writer: WebSocketPacketWriter) -> None:
        self.writer = writer

    def get_write_buffer_size(self) -> int:
        return len(self.writer._pending) + self.writer._sending_size

    def abort(self) -> None:
        self.writer._close(1011, "transport aborted")


class WebSocketPacketWriter:
    def __init__(self, websocket, *, max_size: int = _MAX_OUTGOING_SIZE) -> None:
        self.websocket = websocket
        self.max_size = max_size
        self._pending = bytearray()
        self._sending_size = 0
        self._closing = False
        self.transport = _WebSocketTransport(self)

    def write(self, data: bytes) -> None:
        if self._closing:
            return
        if self._pending:
            raise FrameError("WebSocket writer requires one complete protocol frame per drain")
        self._pending.extend(data)

    async def drain(self) -> None:
        if self._closing or not self._pending:
            return
        frame = bytes(self._pending)
        self._pending.clear()
        decode_packet(frame, max_size=self.max_size)
        self._sending_size = len(frame)
        try:
            await self.websocket.send(frame)
        except ConnectionClosed as exc:
            self._closing = True
            raise OSError("WebSocket connection closed") from exc
        finally:
            self._sending_size = 0

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        self._close(1000, "")

    def _close(self, code: int, reason: str) -> None:
        if self._closing:
            return
        self._closing = True
        try:
            asyncio.get_running_loop().create_task(self.websocket.close(code=code, reason=reason))
        except RuntimeError:
            pass
