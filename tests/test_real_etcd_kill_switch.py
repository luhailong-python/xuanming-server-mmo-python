"""Opt-in integration against a disposable local etcd process."""

import asyncio
import os
import socket
import subprocess
from pathlib import Path

import pytest

from xuanming.grpc_runtime import GrpcClient
from xuanming.kill_switch import EtcdKillSwitch, KillSwitch
from xuanming.protocol import Protocol


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.skipif(not os.environ.get("XUANMING_ETCD_EXE"), reason="set XUANMING_ETCD_EXE to run real etcd integration")
def test_real_etcd_range_watch_and_delete(tmp_path):
    executable = Path(os.environ["XUANMING_ETCD_EXE"])
    if not executable.is_file():
        pytest.fail("XUANMING_ETCD_EXE does not identify an executable")
    client_port, peer_port = _free_port(), _free_port()
    while peer_port == client_port:
        peer_port = _free_port()
    client_url, peer_url = f"http://127.0.0.1:{client_port}", f"http://127.0.0.1:{peer_port}"
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    with (tmp_path / "etcd.log").open("wb") as log:
        process = subprocess.Popen([
            str(executable), "--name", "integration", "--data-dir", str(tmp_path / "data"),
            "--listen-client-urls", client_url, "--advertise-client-urls", client_url,
            "--listen-peer-urls", peer_url, "--initial-advertise-peer-urls", peer_url,
            "--initial-cluster", f"integration={peer_url}",
        ], stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
        try:
            asyncio.run(_exercise(process, client_port))
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


async def _exercise(process, port):
    protocol = Protocol()
    switch = KillSwitch(protocol, protocol.message("NodeInfo", node_type=3, node_uuid="real-etcd"))
    endpoint = f"127.0.0.1:{port}"
    sync, client = EtcdKillSwitch(protocol, endpoint, switch), GrpcClient(endpoint, timeout=1)
    key = b"/mmorpg/killswitch/SceneBagClientPlayer/GetBag"

    async def eventually(predicate):
        async with asyncio.timeout(10):
            while not predicate():
                if process.poll() is not None:
                    raise RuntimeError(f"etcd exited with code {process.returncode}")
                await asyncio.sleep(.05)

    try:
        sync.start()
        await eventually(lambda: sync._ready.is_set())
        put = protocol.message("etcdserverpb.PutRequest", key=key, value=b"true")
        await client.call("/etcdserverpb.KV/Put", put.SerializeToString())
        await eventually(lambda: switch.check("SceneBagClientPlayer", "GetBag") is not None)
        delete = protocol.message("etcdserverpb.DeleteRangeRequest", key=key)
        await client.call("/etcdserverpb.KV/DeleteRange", delete.SerializeToString())
        await eventually(lambda: switch.check("SceneBagClientPlayer", "GetBag") is None)
    finally:
        await sync.close()
        await client.close()
