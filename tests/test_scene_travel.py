import asyncio
from dataclasses import replace
import os
from secrets import randbits
import time
from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables, PlayerSnapshot
from xuanming.nodes import _handoff_lua as lua
from xuanming.nodes.battle_control import service_digest, sign_service_call, verify_service_call
from xuanming.nodes.gate import GateNode, Session, issue_gate_ticket
from xuanming.nodes.scene_travel import SceneTravel, _keys
from xuanming.protocol import Protocol


class _RecordingRedis:
    def __init__(self, calls):
        self.calls = calls
        self.values = {}

    async def hget(self, key, field):
        return self.values.get(key)

    async def set(self, key, value):
        self.values[key] = value

    async def get(self, key):
        return self.values.get(key)

    async def eval(self, script, key_count, *arguments):
        keys, args = arguments[:key_count], arguments[key_count:]
        self.calls.append(("redis", script))
        if script == lua.REGISTER:
            self.values[keys[1]] = args[1].encode()
            return 1
        if script == lua.CONFIRM:
            return 0
        if script in (lua.PUBLISH, lua.CREDENTIAL):
            self.values[keys[1]] = args[4].encode()
            self.values[keys[2]] = args[1].encode()
            return 1
        if script == lua.JUDGE:
            self.values[keys[1]] = args[2].encode()
            self.values.pop(keys[2], None)
            return [self.values[keys[0]], self.values.get(keys[3])]
        if script == lua.RESUME:
            self.values[keys[1]] = args[1].encode()
            return 1
        if script == lua.REVOKE:
            self.values[keys[1]] = args[2].encode()
            return 1
        raise AssertionError("unexpected Redis script")

    async def aclose(self):
        pass


class _Data:
    def __init__(self, protocol, calls):
        self.protocol = protocol
        self.calls = calls

    async def call(self, path, payload):
        method = path.rsplit("/", 1)[-1]
        request = self.protocol.parse(f"data_service.{method}Request", payload)
        assert verify_service_call(path, request, "scene", time.time_ns() // 1_000_000, b"x" * 32)
        self.calls.append(("data", method))
        if method == "RegisterRoleAdmission":
            response = self.protocol.message("data_service.RegisterRoleAdmissionResponse", identity=request.identity)
        elif method == "AckRoleTradeDrain":
            assert request.required_save_seq == 3
            assert self.calls.index(("scene", "evict")) < self.calls.index(("data", method))
            response = self.protocol.message("google.protobuf.Empty")
        else:
            response = self.protocol.message("data_service.CloseRoleAdmissionsResponse", closed_through=request.through_generation)
        return response.SerializeToString()

    async def close(self):
        pass


@pytest.mark.parametrize("outcome", ("granted", "rejected", "rolled_back", "sealed", "credential", "grouped", "team_retry"))
def test_cross_zone_handoff_persists_before_dispatch_and_reconciles(outcome):
    async def scenario():
        protocol = Protocol()
        calls = []
        redis_url = os.getenv("XUANMING_TEST_REDIS")
        player_id = (1 << 63) | randbits(63) if redis_url else 42
        owner_key, fence_key, mark_key, location_key = _keys(player_id)
        if redis_url:
            from redis.asyncio import Redis

            redis = Redis.from_url(redis_url)
            assert not await redis.exists(owner_key, fence_key, mark_key, location_key)
            assert await redis.set(owner_key, b"9007199254740994", nx=True)
        else:
            redis = _RecordingRedis(calls)
            await redis.set(owner_key, b"9007199254740994")
        epoch = int(await redis.get(owner_key))
        session_id = 1 << 17 | 1
        player = SimpleNamespace(
            player_id=player_id, owner_revision=7, session_id=session_id, generation=12, gate_uuid="gate-a",
            admission=None, disconnect_at=None, motion=SimpleNamespace(velocity=(1., 0., 0.)),
            snapshot=PlayerSnapshot(player_id, b"scene-a", epoch, 1, {}),
        )
        world = SimpleNamespace(players={player_id: player}, info=protocol.message("SceneInfoComp", scene_id=23))

        class Store:
            async def load_and_claim(self, player_id, owner, owner_epoch):
                calls.append(("sql", "reclaim"))
                assert owner_epoch == epoch + 2
                return replace(player.snapshot, owner_epoch=owner_epoch)

            async def commit(self, snapshot, rows, *, release):
                assert release and snapshot.save_seq == 2
                calls.append(("sql", "release"))
                return replace(snapshot, save_seq=3, released=True)

            async def release_detached(self, target, owner, owner_epoch):
                assert (target, owner, owner_epoch) == (player_id, b"scene-a", epoch)
                assert await redis.get(mark_key) is None
                calls.append(("sql", "release_detached"))
                return True

        class Scene:
            def __init__(self):
                self.protocol = protocol
                self.info = protocol.message("NodeInfo", node_id=2, zone_id=1, node_uuid="scene-a")
                self.locations = {player_id: world}
                self.battle = None
                self.store = Store()
                self.lock = asyncio.Lock()

            def register(self, *args):
                pass

            def _lock(self, player_id):
                return self.lock

            def _position_rows(self, player):
                return {}

            async def flush_player_rows(self, player):
                calls.append(("sql", "flush"))

            async def commit_player_rows(self, player, rows, *, force):
                assert force and await redis.get(mark_key) is None
                calls.append(("sql", "save"))
                player.snapshot = replace(player.snapshot, save_seq=2)
                return player.snapshot

            def _push(self, player, message, content):
                calls.append(("client", content.id if message.endswith("SendTipToClient") else content.reason.id))

            def _evict(self, world, player):
                calls.append(("scene", "evict"))
                self.locations.pop(player.player_id)

        class Manager:
            async def call(self, path, payload):
                assert path == "/scene_manager.SceneManager/EnterScene"
                request = protocol.parse("scene_manager.EnterSceneRequest", payload)
                assert (request.owner_revision, request.login_generation, request.zone_id) == (7, 12, 2)
                assert ("sql", "save") in calls
                mark = await redis.get(mark_key)
                assert mark.startswith(f"{epoch}:".encode())
                assert b"|F|" in await redis.hget(fence_key, "handoff_fence")
                calls.append(("rpc", "enter"))
                if outcome == "granted":
                    await redis.set(owner_key, str(epoch + 1).encode())
                    fields = {"redirect": {"target_gate_ip": "127.0.0.1", "target_gate_port": 8610}}
                elif outcome == "rolled_back":
                    await redis.set(owner_key, str(epoch + 2).encode())
                    location = protocol.message("storage.PlayerLocation", zone_id=1, node_id="2",
                                                owner_epoch=epoch + 2, rollback_receipt=mark.decode())
                    await redis.set(location_key, location.SerializeToString())
                    fields = {"error_code": 17, "owner_epoch_after_rollback": epoch + 2}
                else:
                    # RPC 回显自身不能授权另一个归属代次。
                    fields = {"error_code": 17, "owner_epoch_after_rollback": epoch + 10}
                return protocol.message("scene_manager.EnterSceneResponse", player_id=player_id,
                                        correlation_id=request.correlation_id, **fields).SerializeToString()

            async def close(self):
                pass

        class Team:
            async def call(self, path, payload):
                assert path == "/teampb.TeamAuthority/ReadMembership"
                request = protocol.parse("teampb.ReadTeamMembershipRequest", payload)
                assert request.admission == player.admission.identity
                assert verify_service_call(path, request, "scene", time.time_ns() // 1_000_000, b"x" * 32)
                response = protocol.message("teampb.ReadTeamMembershipResponse", request_digest=service_digest(request))
                if outcome == "grouped":
                    response.team_id, response.membership_epoch, response.version = 4, 2, 8
                    response.record.team_id = 4
                    response.record.leader_id = player_id
                    response.record.members.add(player_id=player_id)
                elif outcome == "team_retry":
                    response.retry = True
                sign_service_call(path + "/reply", response, "team", time.time_ns() // 1_000_000, b"x" * 32)
                return response.SerializeToString()

            async def close(self):
                pass

        scene = Scene()
        travel = SceneTravel(scene, ConfigTables(), Manager(), redis, _Data(protocol, calls), b"x" * 32,
                             team_client=Team(), trade_secret=b"t" * 32)
        try:
            admission = protocol.message("PlayerEnterGameNodeRequest", player_id=player_id, session_id=session_id,
                                         scene_id=23, owner_revision=7, owner_epoch=epoch, login_generation=12)
            player.admission = await travel.admit(admission)
            await travel.confirm(player.admission)
            if outcome == "credential":
                assert not await travel.release_credential(player_id, epoch)
                del scene.locations[player_id]
                assert not await travel.release_credential(player_id, epoch - 1)
                assert await travel.release_credential(player_id, epoch)
                assert ("sql", "release_detached") in calls
                assert (await redis.get(mark_key)).startswith(f"{epoch}:".encode())
                return
            if outcome == "sealed":
                request = protocol.message("SealAccountRoleTradeRequest", seal={
                    "listing_id": 15, "player_id": player_id, "owner_revision": 7,
                    "scene_instance_id": "scene-a", "through_generation": player.admission.identity.generation,
                    "scene_node_id": "2", "zone_id": 1,
                })
                with pytest.raises(PermissionError):
                    await travel.seal(request)
                sign_service_call("/scene_node.SceneNodeGrpc/SealAccountRoleTrade", request, "trade",
                                  time.time_ns() // 1_000_000, b"t" * 32)
                await travel.seal(request)
                assert travel.sealed(player_id)
                await asyncio.gather(*travel._trade_tasks)
                assert player_id not in scene.locations and not travel.sealed(player_id)
                assert ("sql", "release") in calls and ("data", "AckRoleTradeDrain") in calls
                assert b"|R|" in await redis.hget(fence_key, "handoff_fence")
                await travel.seal(request)
                assert calls.count(("data", "AckRoleTradeDrain")) == 1
                return
            response = await travel.travel_to_zone(world, player, None,
                                                  protocol.message("TravelToZoneRequest", target_zone_id=2))
            if outcome in ("grouped", "team_retry"):
                assert response.error_message.id == (3026 if outcome == "grouped" else 3027)
                assert not travel.frozen(player_id) and ("sql", "save") not in calls
                return
            assert not response.error_message.id and travel.frozen(player_id) and player.motion.velocity == (0., 0., 0.)
            await asyncio.gather(*travel._tasks)
            assert not travel.frozen(player_id)
            if outcome == "granted":
                assert player_id not in scene.locations and ("data", "CloseRoleAdmissions") in calls
                assert calls.count(("sql", "save")) == 1
            else:
                assert player_id in scene.locations and ("client", 3027) in calls
                assert await redis.get(mark_key) is None
                assert b"|H|" in await redis.hget(fence_key, "handoff_fence")
                assert player.snapshot.owner_epoch == epoch + (2 if outcome == "rolled_back" else 0)
        finally:
            if redis_url:
                await redis.delete(owner_key, fence_key, mark_key, location_key)
            await travel.close()

    asyncio.run(scenario())


def test_gate_redirect_stops_old_session_and_forwards_exact_ticket():
    async def scenario():
        protocol = Protocol()
        info = protocol.message("NodeInfo", node_id=1, zone_id=1, node_uuid="gate-a")
        gate = GateNode(protocol, info, b"x" * 32, None)
        session = Session(1 << 17 | 2, 1, SimpleNamespace(is_closing=lambda: False), player_id=42)
        session.verified = session.enter_requested = True
        session.requests.put_nowait(object())
        gate.sessions[session.session_id] = session
        ticket = issue_gate_ticket(protocol, 2, 2, b"x" * 32, player_id=42)
        event = protocol.message("contracts.kafka.RedirectToGateEvent", session_id=session.session_id, player_id=42,
                                 target_gate_ip="192.0.2.20", target_gate_port=8610, token_payload=ticket.payload,
                                 token_signature=ticket.signature, token_deadline=int(time.time()) + 60)
        payload = protocol.message("contracts.kafka.GateCommand", target_gate_id=1, target_instance_id="gate-a",
                                   event_id=protocol.event_ids["ContractsKafkaRedirectToGateEvent"],
                                   payload=event.SerializeToString()).SerializeToString()
        await gate.command(payload)
        assert session.redirected and session.requests.empty()
        content = session.outbound.get_nowait()
        assert content.message_id == 124
        redirected = protocol.parse("RedirectToGateNotify", content.serialized_message)
        assert (redirected.target_port, redirected.token_payload, redirected.token_signature) == (
            8610, ticket.payload, ticket.signature,
        )

    asyncio.run(scenario())
