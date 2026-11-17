"""Opt-in Match failover integration with a fresh, isolated local etcd."""

import asyncio
import ctypes
import os
from pathlib import Path
import socket
import subprocess
import sys

import grpc
import pytest

from xuanming.cluster import EtcdDirectory
from xuanming.grpc_runtime import GrpcClient
from xuanming.match_client import MatchClient
from xuanming.protocol import Protocol


PATH = "/match.MatchInternal/CommitBattleOutcome"


def _owned_listener(port, pid):
    """Check the Windows socket owner before ever connecting to the port."""
    class Row(ctypes.Structure):
        _fields_ = [(name, ctypes.c_uint32) for name in
                    ("state", "local_address", "local_port", "remote_address", "remote_port", "pid")]

    size = ctypes.c_ulong()
    table = ctypes.windll.iphlpapi.GetExtendedTcpTable
    status = table(None, ctypes.byref(size), False, socket.AF_INET, 3, 0)
    if status != 122:  # ERROR_INSUFFICIENT_BUFFER
        raise OSError(f"GetExtendedTcpTable size failed: {status}")
    buffer = ctypes.create_string_buffer(size.value)
    status = table(buffer, ctypes.byref(size), False, socket.AF_INET, 3, 0)
    if status:
        raise OSError(f"GetExtendedTcpTable failed: {status}")
    count = ctypes.c_uint32.from_buffer(buffer).value
    rows = (Row * count).from_buffer(buffer, ctypes.sizeof(ctypes.c_uint32))
    return any(row.state == 2 and row.pid == pid and socket.ntohs(row.local_port & 0xffff) == port for row in rows)


def _port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


async def _server(label, calls):
    async def commit(data, context):
        calls.append((label, data))
        return label.encode()

    server = grpc.aio.server()
    method = grpc.unary_unary_rpc_method_handler(commit, request_deserializer=lambda data: data,
                                                  response_serializer=lambda data: data)
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        "match.MatchInternal", {"CommitBattleOutcome": method}),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


@pytest.mark.skipif(not os.getenv("XUANMING_ETCD_EXE"), reason="set XUANMING_ETCD_EXE for isolated etcd")
@pytest.mark.skipif(sys.platform != "win32", reason="isolated listener ownership check requires Windows")
def test_etcd_discovery_and_live_replica_failover(tmp_path, monkeypatch):
    exe = Path(os.environ["XUANMING_ETCD_EXE"])
    assert exe.is_file()
    client_port, peer_port = _port(), _port()
    while peer_port == client_port:
        peer_port = _port()
    client_url, peer_url = f"http://127.0.0.1:{client_port}", f"http://127.0.0.1:{peer_port}"
    command = [str(exe), "--name", "match-test", "--data-dir", str(tmp_path / "data"),
               "--listen-client-urls", client_url, "--advertise-client-urls", client_url,
               "--listen-peer-urls", peer_url, "--initial-advertise-peer-urls", peer_url,
               "--initial-cluster", f"match-test={peer_url}"]

    async def exercise(process):
        protocol = Protocol()
        directory = EtcdDirectory(protocol, f"127.0.0.1:{client_port}")
        calls, attempted = [], []
        first, first_port = await _server("first", calls)
        second, second_port = await _server("second", calls)
        addresses = [f"127.0.0.1:{first_port}", f"127.0.0.1:{second_port}"]
        match = MatchClient(directory, "127.0.0.1:1")
        real_call = GrpcClient.call

        async def traced_call(client, path, data, **kwargs):
            if path == PATH:
                attempted.append(next(address for address, value in match.clients.items() if value is client))
            return await real_call(client, path, data, **kwargs)

        monkeypatch.setattr(GrpcClient, "call", traced_call)
        monkeypatch.setattr("xuanming.match_client.random.shuffle", lambda values: values.sort(key=lambda item: item != addresses[0]))
        try:
            async with asyncio.timeout(15):
                while True:
                    assert process.poll() is None, "isolated etcd exited before becoming ready"
                    if not _owned_listener(client_port, process.pid):
                        await asyncio.sleep(.1)
                        continue
                    try:
                        await directory.candidates(16, None)
                        break
                    except grpc.RpcError:
                        await asyncio.sleep(.1)

            nodes = []
            for node_id, port in enumerate((first_port, second_port), start=1):
                node = protocol.message("NodeInfo", node_type=16, node_id=node_id, zone_id=1,
                                        node_uuid=f"isolated-match-{node_id}", protocol_type=1,
                                        release_track="stable")
                node.grpc_endpoint.ip, node.grpc_endpoint.port = "127.0.0.1", port
                await directory._rpc("KV", "Put", key=directory._service_key(node),
                                     value=directory._serialized(node))
                nodes.append(node)

            assert await match.call(PATH, b"original-1") == b"first"
            assert len(await directory.candidates(16, None)) == 2
            # The first address remains registered but is now unreachable.
            await first.stop(None)
            # Make both channels eligible for deterministic first-address
            # selection, then observe the actual failed call and retry.
            second_client = match.clients.pop(addresses[1])
            await second_client.close()
            attempted.clear()
            assert await match.call(PATH, b"original-2") == b"second"
            assert attempted == addresses
            assert calls[-1] == ("second", b"original-2")

            await directory._rpc("KV", "DeleteRange", key=directory._service_key(nodes[0]))
            match._expires = 0
            attempted.clear()
            assert await match.call(PATH, b"original-3") == b"second"
            assert attempted == [addresses[1]]
            assert addresses[0] not in match.clients
            assert [node.node_uuid for node in await directory.candidates(16, None)] == ["isolated-match-2"]
        finally:
            await match.close()
            await directory.close()
            await asyncio.gather(first.stop(None), second.stop(None))

    with (tmp_path / "etcd.log").open("wb") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        try:
            asyncio.run(exercise(process))
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
