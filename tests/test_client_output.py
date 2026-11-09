import asyncio
from contextlib import suppress
from types import SimpleNamespace

import pytest

from xuanming.nodes.gate import GateNode, Session
from xuanming.protocol import Protocol
from xuanming.transport import FrameError, Packet, encode_packet
from xuanming.transport.client_output import ClientOutputQueue


class BufferedWriter:
    def __init__(self):
        self.transport = self
        self.buffer = bytearray()
        self.closed = False
        self.draining = asyncio.Event()
        self.release = asyncio.Event()

    def get_write_buffer_size(self):
        return len(self.buffer)

    def write(self, frame):
        self.buffer.extend(frame)

    async def drain(self):
        self.draining.set()
        await self.release.wait()
        self.buffer.clear()

    def close(self):
        self.closed = True
        self.buffer.clear()

    def is_closing(self):
        return self.closed


def test_output_queue_counts_variable_encoded_frames_and_rejects_oversize_atomically():
    p = Protocol()
    queue = ClientOutputQueue(2)
    messages = [p.content(5, body=b"short"), p.message("ClientHeartbeatResponse", seq=1000)]
    sizes = [len(encode_packet(Packet.from_message(message))) for message in messages]
    for message in messages:
        queue.put_nowait(message)
    assert queue.buffered_bytes == sum(sizes)
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(messages[0])
    assert queue.buffered_bytes == sum(sizes)
    assert queue.get_nowait() is messages[0]
    assert queue.buffered_bytes == sizes[1]

    oversized = SimpleNamespace(DESCRIPTOR=SimpleNamespace(full_name="MessageContent"), ByteSize=lambda: 1 << 31)
    writer = BufferedWriter()
    session = Session(33, 1, writer, outbound=queue)
    session.emit(oversized)
    assert writer.closed and queue.qsize() == 1 and queue.buffered_bytes == sizes[1]
    assert queue.get_nowait() is messages[1]
    with pytest.raises(FrameError):
        queue.put_nowait(oversized)
    assert queue.buffered_bytes == 0 and queue.empty()


def test_write_queue_and_transport_accounting_survive_cancellation_and_close():
    async def run():
        p, writer = Protocol(), BufferedWriter()
        session = Session(33, 1, writer)
        waiting = asyncio.create_task(session.outbound.get())
        await asyncio.sleep(0)
        waiting.cancel()
        with suppress(asyncio.CancelledError):
            await waiting
        assert session.buffered_output_bytes == 0

        first = p.content(1, body=b"x" * 140_000)
        reliable = p.content(2, 81, body=b"saved-row-receipt")
        terminal = p.content(166, body=b"terminal")
        session.emit(first)
        session.emit(reliable)
        expected = sum(len(encode_packet(Packet.from_message(message))) for message in (first, reliable))
        writing = asyncio.create_task(GateNode._write(None, session))
        await asyncio.wait_for(writer.draining.wait(), 1)
        assert session.outbound.qsize() == 1 and session.buffered_output_bytes == expected
        session.emit(terminal)
        writing.cancel()
        with suppress(asyncio.CancelledError):
            await writing
        session.close()
        expected_pending = sum(len(encode_packet(Packet.from_message(message))) for message in (reliable, terminal))
        assert session.buffered_output_bytes == expected_pending
        assert session.outbound.get_nowait() is reliable
        assert session.outbound.get_nowait() is terminal
        assert session.buffered_output_bytes == 0

    asyncio.run(run())
