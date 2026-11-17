"""Opt-in test of portal health snapshots against an isolated real etcd."""

import asyncio
import os
from pathlib import Path
import socket
import subprocess

import grpc
import pytest

from xuanming import portal_server_list as module
from xuanming.cluster import EtcdDirectory
from xuanming.portal_server_list import HealthReader
from xuanming.protocol import Protocol


def _port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@pytest.mark.skipif(not os.getenv("XUANMING_ETCD_EXE"), reason="set XUANMING_ETCD_EXE for real etcd")
def test_real_etcd_portal_health_snapshot(tmp_path, monkeypatch):
    exe = Path(os.environ["XUANMING_ETCD_EXE"])
    assert exe.is_file()
    client_port, peer_port = _port(), _port()
    while peer_port == client_port:
        peer_port = _port()
    client_url, peer_url = f"http://127.0.0.1:{client_port}", f"http://127.0.0.1:{peer_port}"
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    with (tmp_path / "etcd.log").open("wb") as log:
        command = [str(exe), "--name", "portal-test", "--data-dir", str(tmp_path / "data"),
                   "--listen-client-urls", client_url, "--advertise-client-urls", client_url,
                   "--listen-peer-urls", peer_url, "--initial-advertise-peer-urls", peer_url,
                   "--initial-cluster", f"portal-test={peer_url}"]
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)

        async def run():
            nonlocal process
            protocol = Protocol()
            directory = EtcdDirectory(protocol, f"127.0.0.1:{client_port}")
            reader = HealthReader(directory)
            task = asyncio.create_task(reader.run())
            try:
                async with asyncio.timeout(15):
                    while reader.current() is None:
                        assert process.poll() is None
                        await asyncio.sleep(.05)
                info = protocol.message("NodeInfo", node_type=5, zone_id=1, node_id=1, node_uuid="login")
                await directory._rpc("KV", "Put", key=directory._service_key(info), value=directory._serialized(info))
                key = b"/mmorpg/zone_health/zone/1/publisher/a"
                await directory._rpc("KV", "Put", key=key,
                                     value=b'{"zoneId":1,"status":"HEALTHY","onlineCount":7,"capacity":100}')
                async with asyncio.timeout(5):
                    while not reader.current().by_zone.get(1) or 1 not in reader.current().login_zones:
                        await asyncio.sleep(.05)
                assert reader.current().by_zone[1].online == 7
                assert 1 in reader.current().login_zones
                # A corrupted record retains the last complete snapshot.
                await directory._rpc("KV", "Put", key=key, value=b"invalid")
                async with asyncio.timeout(5):
                    while not reader.health.bad_keys:
                        await asyncio.sleep(.05)
                assert reader.current().by_zone[1].online == 7
                await directory._rpc("KV", "Put", key=key,
                                     value=b'{"zoneId":1,"status":"DEGRADED","onlineCount":9,"capacity":100}')
                async with asyncio.timeout(5):
                    while reader.current().by_zone[1].online != 9:
                        await asyncio.sleep(.05)
                await directory._rpc("KV", "DeleteRange", key=key)
                async with asyncio.timeout(5):
                    while 1 in reader.current().by_zone:
                        await asyncio.sleep(.05)
                # A real etcd shutdown closes both watches; the reader must
                # re-list after restart before serving the new update.
                process.terminate()
                process.wait(timeout=10)
                async with asyncio.timeout(8):
                    while reader.health.synced or reader.logins.synced:
                        await asyncio.sleep(.05)
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
                async with asyncio.timeout(20):
                    while True:
                        assert process.poll() is None
                        try:
                            await directory._rpc("KV", "Put", key=key, value=(
                                b'{"zoneId":1,"status":"HEALTHY","onlineCount":11,"capacity":100}'))
                            break
                        except grpc.RpcError:
                            await asyncio.sleep(.1)
                    while (not reader.current() or 1 not in reader.current().by_zone
                           or reader.current().by_zone[1].online != 11):
                        await asyncio.sleep(.05)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await directory.close()

        try:
            monkeypatch.setattr(module, "_POLL_INTERVAL", .05)
            asyncio.run(run())
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
