"""验证 Python 服务端 Actor / 消息隔离边界回归测试。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from xuanming.nodes.gate import GateNode, Session
from xuanming.nodes.local_login import LocalLogin
from xuanming.nodes.scene_skill_craft import (
    get_proxy_stall_directory,
    record_proxy_order_reputation,
)
from xuanming.protocol import Protocol


class _StubWriter:
    def __init__(self) -> None:
        self.closed = False
        self.transport = None

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True


def _make_verified_session(session_id: int, player_id: int, scene_id: int = 1) -> Session:
    session = Session(session_id=session_id, incarnation=1, writer=_StubWriter())
    session.verified = True
    session.player_id = player_id
    session.scene_id = scene_id
    return session


def test_gate_broadcast_enqueues_isolated_copies_without_sharing_mutable_protobuf() -> None:
    protocol = Protocol()
    info = protocol.message("NodeInfo", node_id=1, node_uuid="gate-1", zone_id=1)
    gate = GateNode(protocol, info, b"k" * 32, None)
    s1 = _make_verified_session(101, 1001)
    s2 = _make_verified_session(102, 1002)
    gate.sessions[101] = s1
    gate.sessions[102] = s2

    msg_id = next(iter(protocol.message_ids.values()))
    content = protocol.content(msg_id, body=b"hello-world")
    gate.broadcast_content(content)

    # 广播后立即篡改调用方可变 Protobuf，两个会话队列中的独立副本均不受影响。
    content.serialized_message = b"CORRUPTED"

    m1 = s1.outbound.get_nowait()
    m2 = s2.outbound.get_nowait()
    assert m1 is not m2
    assert m1 is not content
    assert m2 is not content
    m1.serialized_message = b"SLOT1-ONLY"
    assert m2.serialized_message == b"hello-world"


def test_local_login_role_of_returns_isolated_copy(tmp_path: Path) -> None:
    protocol = Protocol()
    scene_info = protocol.message("NodeInfo", node_id=2, node_uuid="scene-1", zone_id=1)
    login = LocalLogin(protocol, scene_info, access_token="test-token", store_path=tmp_path / "roles.json")
    role = protocol.message("AccountSimplePlayer", player_id=42, name="原名", class_id=1, gender=1)
    login._profiles[42] = role

    copy1 = login.role_of(42)
    assert copy1 is not None
    copy1.name = "被外部篡改"

    copy2 = login.role_of(42)
    assert copy2 is not None
    assert copy2.name == "原名"


def test_proxy_reputation_defers_to_master_actor_when_master_lock_is_busy() -> None:
    async def scenario() -> None:
        protocol = Protocol()
        commits: list[int] = []

        class _FakeRows:
            def assemble(self, snap):
                return snap.data

        class _FakePlayer:
            def __init__(self, pid: int, data_msg) -> None:
                self.player_id = pid
                self.snapshot = SimpleNamespace(
                    owner_epoch=1,
                    released=False,
                    data=data_msg,
                )

        class _FakeScene:
            def __init__(self, master: _FakePlayer) -> None:
                self.protocol = protocol
                self.rows = _FakeRows()
                self._locks: dict[int, asyncio.Lock] = {}
                self._actor_tasks: set[asyncio.Task] = set()
                self.locations = {1: SimpleNamespace(players={master.player_id: master})}
                self.commit_observed_locked: list[bool] = []

            def player_lock(self, pid: int) -> asyncio.Lock:
                return self._locks.setdefault(int(pid), asyncio.Lock())

            def mutable(self, player) -> bool:
                return True

            def schedule_player_task(self, pid: int, coro_factory):
                async def _run():
                    async with self.player_lock(pid):
                        await coro_factory()

                t = asyncio.create_task(_run())
                self._actor_tasks.add(t)
                t.add_done_callback(self._actor_tasks.discard)
                return t

            async def commit_player_data(self, player, staged) -> None:
                self.commit_observed_locked.append(self.player_lock(player.player_id).locked())
                commits.append(player.player_id)
                player.snapshot.data = staged

        real_data = protocol.message("player_database", player_id=200)
        master = _FakePlayer(200, real_data)
        scene = _FakeScene(master)

        # 模拟宗师 200 正处于自己的 Actor 锁内：另一玩家触发口碑写入绝不能死锁，也不能无锁落盘。
        async with scene.player_lock(200):
            await asyncio.wait_for(
                record_proxy_order_reputation(scene, 200, supreme_delta=1, net_gold_earned=500, buyer_name="买家甲"),
                timeout=1.0,
            )
            assert commits == []
            assert get_proxy_stall_directory(scene).peek_pending_reputation(200) is not None

        # 宗师释放锁后，异步排队的 Actor 回账任务在持有宗师锁的情况下自动完成落盘。
        await asyncio.gather(*tuple(scene._actor_tasks))
        assert commits == [200]
        assert scene.commit_observed_locked == [True]
        assert get_proxy_stall_directory(scene).peek_pending_reputation(200) is None
        assert master.snapshot.data.craft_component.proxy_orders_completed == 1
        assert master.snapshot.data.craft_component.unread_proxy_digest_net_gold == 500

    asyncio.run(scenario())
