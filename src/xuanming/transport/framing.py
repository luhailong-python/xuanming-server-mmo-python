"""Gate 客户端和节点间 RPC0 使用的两种 muduo 二进制帧。"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import struct
from typing import TypeVar
import zlib

from google.protobuf.message import DecodeError, Message

_UINT32 = struct.Struct("!I")
_WORD_SIZE = _UINT32.size
_MAX_WIRE_SIZE = (1 << 31) - 1
_RPC_TAG = b"RPC0"
_MIN_PACKET_SIZE = 2 * _WORD_SIZE + 2
_MIN_RPC_SIZE = len(_RPC_TAG) + _WORD_SIZE
DEFAULT_GATE_FRAME_SIZE = 64 * 1024
DEFAULT_RPC_FRAME_SIZE = 64 * 1024 * 1024
_Message = TypeVar("_Message", bound=Message)


class FrameError(ValueError):
    """长度、校验和、类型名或消息体无效。"""


@dataclass(frozen=True, slots=True)
class Packet:
    type_name: str
    payload: bytes

    @classmethod
    def from_message(cls, message: Message) -> Packet:
        return cls(message.DESCRIPTOR.full_name, message.SerializeToString())

    def parse_as(self, message_type: type[_Message]) -> _Message:
        if self.type_name != message_type.DESCRIPTOR.full_name:
            raise FrameError(f"unexpected protobuf type {self.type_name!r}")
        message = message_type()
        try:
            message.ParseFromString(self.payload)
        except DecodeError as exc:
            raise FrameError("invalid protobuf payload") from exc
        if not message.IsInitialized():
            raise FrameError("protobuf payload has missing required fields")
        return message


def _validate_size(size: int, min_size: int, max_size: int) -> None:
    if not min_size <= size <= min(max_size, _MAX_WIRE_SIZE):
        raise FrameError(f"invalid frame size {size}")


def _seal(data: bytes, min_size: int, max_size: int) -> bytes:
    size = len(data) + _WORD_SIZE
    _validate_size(size, min_size, max_size)
    return _UINT32.pack(size) + data + _UINT32.pack(zlib.adler32(data))


def _unseal(body: bytes) -> bytes:
    data = body[:-_WORD_SIZE]
    expected, = _UINT32.unpack_from(body, len(data))
    if zlib.adler32(data) != expected:
        raise FrameError("invalid frame checksum")
    return data


def _decode_body(frame: bytes, min_size: int, max_size: int) -> bytes:
    if len(frame) < _WORD_SIZE:
        raise FrameError("missing frame size")
    size, = _UINT32.unpack_from(frame)
    _validate_size(size, min_size, max_size)
    if len(frame) != _WORD_SIZE + size:
        raise FrameError("frame size does not match available bytes")
    return _unseal(frame[_WORD_SIZE:])


async def _read_body(
    reader: asyncio.StreamReader,
    min_size: int,
    max_size: int,
    on_read: Callable[[], None] | None = None,
) -> bytes | None:
    try:
        header = await _read_exactly(reader, _WORD_SIZE, on_read)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise FrameError("truncated frame header") from exc

    size, = _UINT32.unpack(header)
    _validate_size(size, min_size, max_size)
    try:
        body = await _read_exactly(reader, size, on_read)
    except asyncio.IncompleteReadError as exc:
        raise FrameError("truncated frame body") from exc
    return _unseal(body)


async def _read_exactly(
    reader: asyncio.StreamReader,
    size: int,
    on_read: Callable[[], None] | None,
) -> bytes:
    if on_read is None:
        return await reader.readexactly(size)
    data = bytearray()
    while len(data) < size:
        chunk = await reader.read(size - len(data))
        if not chunk:
            raise asyncio.IncompleteReadError(bytes(data), size)
        data.extend(chunk)
        on_read()
    return bytes(data)


def encode_packet(packet: Packet, *, max_size: int = _MAX_WIRE_SIZE) -> bytes:
    """编码客户端帧。Gate 的 64 KiB 限制用于接收侧。"""
    try:
        name = packet.type_name.encode("ascii")
    except UnicodeEncodeError as exc:
        raise FrameError("invalid protobuf type name") from exc
    if not name or b"\0" in name:
        raise FrameError("invalid protobuf type name")

    data = _UINT32.pack(len(name) + 1) + name + b"\0" + packet.payload
    return _seal(data, _MIN_PACKET_SIZE, max_size)


def _parse_packet(data: bytes) -> Packet:
    name_size, = _UINT32.unpack_from(data)
    name_end = _WORD_SIZE + name_size
    if name_size < 2 or name_end > len(data):
        raise FrameError("invalid protobuf type name length")
    name = data[_WORD_SIZE:name_end - 1]
    if data[name_end - 1] != 0 or b"\0" in name:
        raise FrameError("invalid protobuf type name terminator")
    try:
        return Packet(name.decode("ascii"), data[name_end:])
    except UnicodeDecodeError as exc:
        raise FrameError("invalid protobuf type name") from exc


def decode_packet(frame: bytes, *, max_size: int = DEFAULT_GATE_FRAME_SIZE) -> Packet:
    return _parse_packet(_decode_body(frame, _MIN_PACKET_SIZE, max_size))


async def read_packet(
    reader: asyncio.StreamReader,
    *,
    max_size: int = DEFAULT_GATE_FRAME_SIZE,
) -> Packet | None:
    """读取一个客户端帧。只有帧间的正常 EOF 返回 None。"""
    data = await _read_body(reader, _MIN_PACKET_SIZE, max_size)
    return _parse_packet(data) if data is not None else None


async def write_packet(
    writer: asyncio.StreamWriter,
    packet: Packet,
    *,
    max_size: int = _MAX_WIRE_SIZE,
) -> None:
    writer.write(encode_packet(packet, max_size=max_size))
    await writer.drain()


def encode_rpc_frame(payload: bytes, *, max_size: int = DEFAULT_RPC_FRAME_SIZE) -> bytes:
    return _seal(_RPC_TAG + payload, _MIN_RPC_SIZE, max_size)


def _parse_rpc_frame(data: bytes) -> bytes:
    if not data.startswith(_RPC_TAG):
        raise FrameError("invalid RPC0 tag")
    return data[len(_RPC_TAG):]


def decode_rpc_frame(frame: bytes, *, max_size: int = DEFAULT_RPC_FRAME_SIZE) -> bytes:
    return _parse_rpc_frame(_decode_body(frame, _MIN_RPC_SIZE, max_size))


async def read_rpc_frame(
    reader: asyncio.StreamReader,
    *,
    max_size: int = DEFAULT_RPC_FRAME_SIZE,
    on_read: Callable[[], None] | None = None,
) -> bytes | None:
    """读取 RPC0 帧。on_read 在读取每段非空字节后触发。"""
    data = await _read_body(reader, _MIN_RPC_SIZE, max_size, on_read)
    return _parse_rpc_frame(data) if data is not None else None


async def write_rpc_frame(
    writer: asyncio.StreamWriter,
    payload: bytes,
    *,
    max_size: int = DEFAULT_RPC_FRAME_SIZE,
) -> None:
    writer.write(encode_rpc_frame(payload, max_size=max_size))
    await writer.drain()
