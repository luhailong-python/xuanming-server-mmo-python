import asyncio
from contextlib import asynccontextmanager
import struct
from types import SimpleNamespace
import zlib

import pytest

from xuanming.protocol import Protocol
from xuanming.cluster import GATE, SCENE, LocalDirectory
from xuanming.node_link import GateLinks, SceneLinks
from xuanming.transport import (
    DEFAULT_GATE_FRAME_SIZE,
    FrameError,
    Packet,
    RpcChannel,
    RpcDisconnected,
    RpcError,
    RpcErrorCode,
    RpcInvalidResponse,
    RpcLinkHeartbeat,
    RpcMessageType,
    WebSocketPacketReader,
    WebSocketPacketWriter,
    decode_packet,
    decode_rpc_frame,
    encode_packet,
    encode_rpc_frame,
    read_packet,
    read_rpc_frame,
    write_rpc_frame,
)


@pytest.fixture(scope="module")
def protocol():
    return Protocol()


def test_gate_frame_matches_muduo_layout(protocol):
    heartbeat = protocol.message("ClientHeartbeatRequest", seq=1)
    frame = bytes.fromhex(
        "00000021 00000017 436c69656e744865617274626561745265717565737400 0801 801f08f9"
    )
    assert encode_packet(Packet.from_message(heartbeat)) == frame
    packet = decode_packet(frame)
    assert packet.parse_as(type(heartbeat)) == heartbeat
    with pytest.raises(FrameError, match="unexpected protobuf type"):
        packet.parse_as(protocol.message_class("ClientHeartbeatResponse"))
    with pytest.raises(FrameError, match="invalid protobuf payload"):
        Packet("ClientHeartbeatRequest", b"\x08").parse_as(type(heartbeat))


def test_gate_rejects_bad_lengths_checksum_and_name_terminator():
    frame = encode_packet(Packet("Some.Message", b"payload"))
    for malformed in (frame[:3], frame[:-1], frame + b"x", frame[:-1] + bytes([frame[-1] ^ 1])):
        with pytest.raises(FrameError):
            decode_packet(malformed)

    invalid = b"\x00\x00\x00\x02ab"
    bad_name = struct.pack("!I", len(invalid) + 4) + invalid + struct.pack("!I", zlib.adler32(invalid))
    with pytest.raises(FrameError, match="terminator"):
        decode_packet(bad_name)


def test_gate_frame_stream_handles_partial_and_concatenated_reads():
    async def scenario():
        reader = asyncio.StreamReader()
        first = Packet("First", b"a")
        second = Packet("Second", b"b")
        frame = encode_packet(first)
        reader.feed_data(frame[:3])
        reading = asyncio.create_task(read_packet(reader))
        await asyncio.sleep(0)
        assert not reading.done()
        reader.feed_data(frame[3:] + encode_packet(second))
        reader.feed_eof()
        assert await reading == first
        assert await read_packet(reader) == second
        assert await read_packet(reader) is None

        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack("!I", DEFAULT_GATE_FRAME_SIZE + 1))
        with pytest.raises(FrameError, match="invalid frame size"):
            await asyncio.wait_for(read_packet(reader), 1)

        reader = asyncio.StreamReader()
        reader.feed_data(frame[:-1])
        reader.feed_eof()
        with pytest.raises(FrameError, match="truncated frame body"):
            await read_packet(reader)

    asyncio.run(scenario())


def test_websocket_adapters_keep_one_original_frame_per_binary_message(protocol):
    class MemoryWebSocket:
        def __init__(self, incoming=()):
            self.incoming = list(incoming)
            self.outgoing = []

        async def recv(self):
            if not self.incoming:
                raise RuntimeError("no queued frame")
            return self.incoming.pop(0)

        async def send(self, frame):
            self.outgoing.append(frame)

        async def close(self, **_kwargs):
            return None

    async def scenario():
        request = protocol.message("ClientHeartbeatRequest", seq=42)
        frame = encode_packet(Packet.from_message(request))
        websocket = MemoryWebSocket([frame])
        reader = WebSocketPacketReader(websocket)
        assert await reader.readexactly(4) == frame[:4]
        assert await reader.readexactly(len(frame) - 4) == frame[4:]

        writer = WebSocketPacketWriter(websocket)
        writer.write(frame)
        await writer.drain()
        assert websocket.outgoing == [frame]
        assert decode_packet(websocket.outgoing[0]).parse_as(type(request)) == request

        text_socket = MemoryWebSocket(["text"])
        with pytest.raises(FrameError, match="二进制"):
            await WebSocketPacketReader(text_socket).readexactly(4)

        combined = MemoryWebSocket([frame + frame])
        with pytest.raises(FrameError):
            await WebSocketPacketReader(combined).readexactly(4)

    asyncio.run(scenario())


def test_rpc0_heartbeat_frame_matches_muduo_layout(protocol):
    envelope = protocol.message("GameRpcMessage", type=RpcMessageType.HEARTBEAT)
    frame = bytes.fromhex("0000000a 52504330 0805 05330123")
    assert encode_rpc_frame(envelope.SerializeToString()) == frame
    assert type(envelope).FromString(decode_rpc_frame(frame)) == envelope
    assert decode_rpc_frame(encode_rpc_frame(b"")) == b""
    with pytest.raises(FrameError, match="RPC0 tag"):
        decode_rpc_frame(encode_packet(Packet("NotRpc0", b"")))


@asynccontextmanager
async def tcp_pair():
    accepted = asyncio.get_running_loop().create_future()

    def accept(reader, writer):
        accepted.set_result((reader, writer))

    async with await asyncio.start_server(accept, "127.0.0.1", 0) as server:
        addr = server.sockets[0].getsockname()
        client = await asyncio.open_connection(*addr)
        peer = await accepted
        try:
            yield client, peer
        finally:
            for _, writer in (client, peer):
                writer.close()
                await writer.wait_closed()


def test_rpc0_correlates_reordered_calls_and_allows_reverse_calls(protocol):
    async def scenario():
        slow_entered = asyncio.Event()
        slow_release = asyncio.Event()
        one_way = asyncio.Event()

        async def return_body(body):
            if body == b"slow":
                slow_entered.set()
                await slow_release.wait()
            if body == b"nested":
                return await server.call(123, b"reverse", timeout=1)
            if body == b"notification":
                one_way.set()
            return body

        envelope = protocol.message_class("GameRpcMessage")
        async with tcp_pair() as (client_io, server_io):
            client = RpcChannel(*client_io, envelope, {123: return_body})
            server = RpcChannel(*server_io, envelope, {42: return_body})
            try:
                slow = asyncio.create_task(client.call(42, b"slow", timeout=1))
                await asyncio.wait_for(slow_entered.wait(), 1)
                assert await client.call(42, b"", timeout=1) == b""
                assert await client.call(42, b"nested", timeout=1) == b"reverse"
                assert not slow.done()
                slow_release.set()
                assert await slow == b"slow"
                await client.send_heartbeat()
                await client.send(42, b"notification")
                await asyncio.wait_for(one_way.wait(), 1)
                with pytest.raises(RpcError) as exc:
                    await client.call(999, b"", timeout=1)
                assert exc.value.code == RpcErrorCode.INVALID_REQUEST
            finally:
                await asyncio.gather(client.aclose(), server.aclose())

    asyncio.run(scenario())


def test_rpc0_rejects_mismatched_response_and_expires_calls(protocol):
    async def scenario():
        envelope = protocol.message_class("GameRpcMessage")
        async with tcp_pair() as (client_io, (peer_reader, peer_writer)):
            client = RpcChannel(*client_io, envelope)
            try:
                timed_out = asyncio.create_task(client.call(1, b"", timeout=0.01))
                late = envelope.FromString(await asyncio.wait_for(read_rpc_frame(peer_reader), 1))
                with pytest.raises(TimeoutError):
                    await timed_out
                reply = envelope(type=RpcMessageType.RESPONSE, message_id=1, id=late.id)
                await write_rpc_frame(peer_writer, reply.SerializeToString())

                active = asyncio.create_task(client.call(2, b"", timeout=1))
                request = envelope.FromString(await asyncio.wait_for(read_rpc_frame(peer_reader), 1))
                reply = envelope(type=RpcMessageType.RESPONSE, message_id=1, id=request.id)
                await write_rpc_frame(peer_writer, reply.SerializeToString())
                with pytest.raises(RpcInvalidResponse):
                    await active

                active = asyncio.create_task(client.call(3, b"", timeout=1))
                await asyncio.wait_for(read_rpc_frame(peer_reader), 1)
                peer_writer.close()
                with pytest.raises(RpcDisconnected):
                    await active
                await asyncio.wait_for(client.wait_closed(), 1)
            finally:
                await client.aclose()

    asyncio.run(scenario())


def test_rpc0_auto_heartbeat_writes_then_closes_idle_peer(protocol):
    async def scenario():
        envelope = protocol.message_class("GameRpcMessage")
        async with tcp_pair() as (client_io, (peer_reader, _)):
            config = RpcLinkHeartbeat(interval=0.04, idle_timeout=0.2, check_period=0.02)
            client = RpcChannel(*client_io, envelope, heartbeat=config)
            try:
                frame = await asyncio.wait_for(read_rpc_frame(peer_reader), 1)
                heartbeat = envelope.FromString(frame)
                assert heartbeat == envelope(type=RpcMessageType.HEARTBEAT)
                await asyncio.wait_for(client.wait_closed(), 2)
            finally:
                await client.aclose()

    asyncio.run(scenario())


def test_gate_scene_link_handshake_ordered_calls_and_push(protocol):
    async def scenario():
        directory = LocalDirectory()
        gate_info = protocol.message("NodeInfo", node_type=GATE, zone_id=1, node_id=7, node_uuid="gate-a")
        scene_info = protocol.message("NodeInfo", node_type=SCENE, zone_id=1, node_id=6, node_uuid="scene-a")
        scene = SceneLinks(protocol, scene_info, directory)
        directory.publish(gate_info)
        entered = asyncio.Event()
        loaded = asyncio.Event()
        processed = asyncio.Event()
        pushes = asyncio.Queue()
        session_id = 7 << 17 | 42

        class Runtime:
            async def enter(self, request, gate_uuid):
                assert gate_uuid == gate_info.node_uuid
                entered.set()
                await loaded.wait()
                player = SimpleNamespace(gate_uuid=gate_uuid, player_id=request.player_id, session_id=request.session_id)
                scene.emit(player, protocol.content(111, body=b"entered"))
                return protocol.message("Empty")

            async def process(self, request, gate_uuid):
                assert gate_uuid == gate_info.node_uuid
                if request.session_id == session_id:
                    assert loaded.is_set()
                    processed.set()
                return protocol.message("ProcessClientPlayerMessageResponse", session_id=request.session_id)

        scene.bind(Runtime())
        gate = GateLinks(protocol, gate_info, directory, pushes.put_nowait)
        async with await asyncio.start_server(scene.serve, "127.0.0.1", 0) as server:
            scene_info.endpoint.ip, scene_info.endpoint.port = server.sockets[0].getsockname()
            directory.publish(scene_info)
            try:
                enter = protocol.message("PlayerEnterGameNodeRequest", player_id=20, session_id=session_id)
                await gate.call(6, 83, enter, one_way=True, expected_uuid="scene-a")
                await asyncio.wait_for(entered.wait(), 1)
                request = protocol.message("ProcessClientPlayerMessageRequest", player_id=20, session_id=session_id)
                waiting = asyncio.create_task(gate.call(6, 10, request, timeout=1))
                await asyncio.sleep(0)
                other_session = protocol.message(
                    "ProcessClientPlayerMessageRequest", player_id=21, session_id=session_id + 1,
                )
                await gate.call(6, 10, other_session, timeout=1)
                assert not processed.is_set()
                loaded.set()
                response = protocol.parse("ProcessClientPlayerMessageResponse", await waiting)
                assert response.session_id == session_id

                push = await asyncio.wait_for(pushes.get(), 1)
                assert push.header.session_id == session_id
                assert push.header.target_player_id == 20
                assert push.message_content.serialized_message == b"entered"

                request.session_id = 8 << 17 | 42
                with pytest.raises(RpcError) as exc:
                    await gate.call(6, 10, request, timeout=1)
                assert exc.value.code == RpcErrorCode.INVALID_REQUEST

                replacement = protocol.message("NodeInfo")
                replacement.CopyFrom(scene_info)
                replacement.node_uuid = "scene-b"
                directory.publish(replacement)
                with pytest.raises(RpcDisconnected, match="握手校验失败"):
                    await gate.call(6, 10, request, expected_uuid="scene-b")
                with pytest.raises(RpcDisconnected, match="实例已更换"):
                    await gate.call(6, 10, request, expected_uuid="scene-a")
            finally:
                await gate.close()
                await scene.close()

    asyncio.run(scenario())
