import asyncio
from types import SimpleNamespace

import pytest

from xuanming.node_link import SceneLinkNotReady, SceneNodeNotFound
from xuanming.nodes import gate_entry
from xuanming.nodes import gate as gate_module
from xuanming.nodes.gate import GateNode, Session
from xuanming.protocol import Protocol
from xuanming.transport import RpcDisconnected


class _Writer:
    closed = False

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(gate_entry, "INITIAL_BACKOFF", 0.005)
    monkeypatch.setattr(gate_entry, "MAX_BACKOFF", 0.010)
    monkeypatch.setattr(gate_entry, "DISCOVERY_BUDGET", 0.045)
    monkeypatch.setattr(gate_entry, "ENTRY_BUDGET", 0.250)
    monkeypatch.setattr(gate_module, "FORCE_CLOSE_DELAY", 0.01)
    protocol = Protocol()
    info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-a")
    gate = GateNode(protocol, info, b"x" * 32, None)
    session = Session(1 << 17 | 2, 1, _Writer(), player_id=42, version=1, login_generation=10,
                      owner_revision=2, account="owner", verified=True, enter_requested=True,
                      enter_player_id=42, enter_type=1)
    gate.sessions[session.session_id] = session
    return gate, session


def _route(gate, session, *, target=2, scene=23, epoch=7, generation=10):
    return gate.protocol.message("contracts.kafka.RoutePlayerEvent", session_id=session.session_id, player_id=42,
                                 scene_id=scene, target_node_id=target, owner_revision=2, owner_epoch=epoch,
                                 login_generation=generation)


def _link(node_id):
    return SimpleNamespace(info=SimpleNamespace(node_uuid=f"scene-{node_id}"))


def test_retries_readiness_without_replaying_business_frame(setup):
    gate, session = setup

    class Scene:
        attempts = 0
        sent = []

        async def prepare_entry(self, node_id):
            self.attempts += 1
            if self.attempts < 3:
                raise SceneLinkNotReady("等待握手")
            return _link(node_id)

        async def send_entry(self, link, request, *, timeout, can_send):
            assert can_send() and 0 < timeout <= gate_entry.ENTRY_BUDGET
            self.sent.append(request)

    async def scenario():
        gate.scene = Scene()
        await gate.route(_route(gate, session), 0)
        assert gate.scene.attempts == 3 and len(gate.scene.sent) == 1
        assert gate.scene.sent[0].enter_gs_type == 1
        assert session.scene_id == 23 and session.scene_instance_id == "scene-2"
        assert session.owner_epoch == 7 and not session.enter_type and session.pending_route is None

    asyncio.run(scenario())


def test_discovery_budget_fails_with_tip_and_kick(setup):
    gate, session = setup

    class Scene:
        attempts = 0

        async def prepare_entry(self, node_id):
            self.attempts += 1
            raise SceneNodeNotFound("权威发现中没有节点")

    async def scenario():
        gate.scene = Scene()
        await gate.route(_route(gate, session), 0)
        assert 2 <= gate.scene.attempts < gate_entry.MAX_ATTEMPTS
        assert session.pending_route is None and session.writer.closed
        tip, kick = session.outbound.get_nowait(), session.outbound.get_nowait()
        assert (tip.message_id, kick.message_id) == (23, 34)
        assert gate.protocol.parse("TipInfoMessage", tip.serialized_message).id == 3023

    asyncio.run(scenario())


def test_new_bind_and_route_supersede_inflight_old_scene_entry(setup):
    gate, session = setup

    async def scenario():
        old_sent = asyncio.Event()
        complete_old = asyncio.Event()
        new_sent = asyncio.Event()
        complete_new = asyncio.Event()
        sent = []

        class Scene:
            async def prepare_entry(self, node_id):
                return _link(node_id)

            async def send_entry(self, link, request, *, timeout, can_send):
                assert can_send()
                sent.append((request.scene_id, request.login_generation))
                if request.scene_id == 23:
                    old_sent.set()
                    await complete_old.wait()
                else:
                    new_sent.set()
                    await complete_new.wait()

        gate.scene = Scene()
        await gate.route(_route(gate, session), 0, background=True)
        await asyncio.wait_for(old_sent.wait(), 1)
        await gate.bind(gate.protocol.message(
            "contracts.kafka.BindSessionEvent", session_id=session.session_id, player_id=42, owner_revision=2,
            authenticated_account="owner", session_incarnation=1, session_version=2, login_generation=11,
            enter_gs_type=3,
        ))
        await gate.route(_route(gate, session, target=3, scene=24, epoch=8, generation=11), 0, background=True)
        newest = session.pending_route.task
        complete_old.set()
        await asyncio.wait_for(new_sent.wait(), 1)
        assert not session.scene_id and session.version == 2 and session.login_generation == 11
        complete_new.set()
        await newest
        assert sent == [(23, 10), (24, 11)]
        assert session.scene_id == 24 and session.scene_instance_id == "scene-3" and session.owner_epoch == 8
        assert session.pending_route is None

    asyncio.run(scenario())


def test_route_before_bind_only_discovers_then_forwards_latest(setup):
    gate, session = setup
    session.player_id = session.version = session.login_generation = session.owner_revision = 0
    session.account = ""

    async def scenario():
        discovered = []
        sent = []

        class Scene:
            async def discover_entry(self, node_id):
                discovered.append(node_id)

            async def prepare_entry(self, node_id):
                return _link(node_id)

            async def send_entry(self, link, request, *, timeout, can_send):
                assert can_send()
                sent.append(request)

        gate.scene = Scene()
        await gate.route(_route(gate, session), 0)
        await session.pending_route.task
        assert discovered == [2] and not sent and not session.scene_id
        await gate.bind(gate.protocol.message(
            "contracts.kafka.BindSessionEvent", session_id=session.session_id, player_id=42, owner_revision=2,
            authenticated_account="owner", session_incarnation=1, session_version=1, login_generation=10,
            enter_gs_type=1,
        ))
        await session.pending_route.task
        assert len(sent) == 1 and sent[0].enter_gs_type == 1 and session.scene_id == 23

    asyncio.run(scenario())


def test_sent_entry_with_unknown_outcome_is_never_retried(setup):
    gate, session = setup

    async def scenario():
        sent = []

        class Scene:
            async def prepare_entry(self, node_id):
                return _link(node_id)

            async def send_entry(self, link, request, *, timeout, can_send):
                sent.append(request)
                raise RpcDisconnected("请求已经发送，未获应答")

        gate.scene = Scene()
        await gate.route(_route(gate, session), 0)
        assert len(sent) == 1 and session.writer.closed and session.entry_failed

    asyncio.run(scenario())
