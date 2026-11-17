import asyncio
import time

from xuanming.nodes.gate import GateNode, RouterForwarder, Session
from xuanming.protocol import Protocol


class Writer:
    def __init__(self):
        self.closed = False

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True


def test_drain_lease_routes_ticket_and_closes_lingering_connection():
    async def exercise():
        protocol = Protocol()
        info = protocol.message("NodeInfo", node_id=1, zone_id=2, node_uuid="gate-a")
        now = [100.0]
        ticket = protocol.message("GateTokenPayload", gate_node_id=2, zone_id=2,
                                  player_id=45, expire_timestamp=int(time.time()) + 60)
        response = protocol.message("loginpb.GateMigrationResponse", result=1, target_gate_ip="127.0.0.1",
                                    target_gate_port=1234, token_payload=ticket.SerializeToString(),
                                    token_signature=b"signature", token_deadline=int(time.time()) + 60)
        calls = []

        class Forward:
            async def request_migration(self, gate, session):
                calls.append(session.session_id)
                return response.SerializeToString()

        gate = GateNode(protocol, info, b"x" * 32, Forward())
        drain = gate.drain_migration
        drain.clock = lambda: now[0]
        session = Session((1 << 17) | 1, 1, Writer(), verified=True, logged_in=True,
                          player_id=45, scene_id=8, scene_node_id=2)
        gate.sessions[session.session_id] = session
        try:
            drain._runner = asyncio.current_task()
            event = protocol.message("contracts.kafka.GateDrainMigrationEvent", rate_per_second=1, lease_ms=30000)
            command = protocol.message("contracts.kafka.GateCommand", target_gate_id=1, target_instance_id="gate-a",
                                       event_id=protocol.event_ids["ContractsKafkaGateDrainMigrationEvent"],
                                       payload=event.SerializeToString())
            await gate.command(command.SerializeToString())
            session.lane_busy = True
            drain.tick()
            assert not calls and not drain._inflight
            session.lane_busy = False
            drain.tick()
            await asyncio.gather(*tuple(drain._inflight.values()))
            assert calls == [session.session_id] and session.redirected
            content = session.outbound.get_nowait()
            assert content.message_id == protocol.message_ids["SceneClientPlayerCommonRedirectToGate"]
            notify = protocol.parse("RedirectToGateNotify", content.serialized_message)
            assert notify.target_port == 1234 and notify.token_payload == ticket.SerializeToString()
            now[0] += 60
            drain.tick()
            assert session.writer.closed
        finally:
            drain._runner = None
            await gate.close()

    asyncio.run(exercise())


def test_empty_drain_kafka_payload_does_not_cancel_live_lease():
    async def exercise():
        protocol = Protocol()
        info = protocol.message("NodeInfo", node_id=1, zone_id=2, node_uuid="gate-a")
        gate = GateNode(protocol, info, b"x" * 32, None)
        gate.drain_migration.clock = lambda: 100.0
        gate.drain_migration.lease(protocol.message("contracts.kafka.GateDrainMigrationEvent",
                                                    rate_per_second=2, lease_ms=30000))
        command = protocol.message("contracts.kafka.GateCommand", target_gate_id=1, target_instance_id="gate-a",
                                   event_id=protocol.event_ids["ContractsKafkaGateDrainMigrationEvent"])
        try:
            await gate.command(command.SerializeToString())
            assert (gate.drain_migration.rate, gate.drain_migration.deadline) == (2, 130.0)
        finally:
            await gate.close()

    asyncio.run(exercise())


def test_drain_ignores_stale_reply_and_respects_retries_and_role_selection():
    async def exercise():
        protocol = Protocol()
        gate = GateNode(protocol, protocol.message("NodeInfo", node_id=1, zone_id=2, node_uuid="gate-a"),
                        b"x" * 32, lambda *args: None)
        drain = gate.drain_migration
        now, release, started = [100.0], asyncio.Event(), asyncio.Event()
        result = [3]

        async def sender(_, session):
            started.set()
            await release.wait()
            return protocol.message("loginpb.GateMigrationResponse", result=result[0]).SerializeToString()

        drain.sender, drain.clock, drain._runner = sender, lambda: now[0], asyncio.current_task()
        session = Session((1 << 17) | 1, 1, Writer(), verified=True, logged_in=True, last_enter_at=0.0)
        gate.sessions[session.session_id] = session
        try:
            drain.lease(protocol.message("contracts.kafka.GateDrainMigrationEvent", rate_per_second=1, lease_ms=60000))
            drain.tick()
            assert not drain._inflight  # recently forwarded EnterGame, still settling
            now[0] = 301
            drain.deadline = 400
            drain.tick()
            await started.wait()
            release.set()
            await asyncio.gather(*tuple(drain._inflight.values()))
            assert drain._retry[(session.session_id, session.incarnation)] == 311
            now[0] = 311
            started.clear()
            release.clear()
            drain.tick()
            await started.wait()
            replacement = Session(session.session_id, 2, Writer(), verified=True, logged_in=True)
            gate.sessions[session.session_id] = replacement
            release.set()
            await asyncio.gather(*tuple(drain._inflight.values()))
            assert not replacement.redirected and replacement.outbound.empty()
        finally:
            drain._runner = None
            await gate.close()

    asyncio.run(exercise())


def test_router_drain_request_is_server_only_and_lease_can_stop():
    async def exercise():
        protocol = Protocol()
        session = Session((1 << 17) | 1, 1, Writer(), verified=True, logged_in=True)
        response = protocol.message("loginpb.GateMigrationResponse", result=3)

        class Call:
            def __init__(self, metadata):
                self.metadata = metadata

            def __await__(self):
                async def result():
                    return protocol.content(375, body=response.SerializeToString()).SerializeToString()
                return result().__await__()

            async def initial_metadata(self):
                return (self.metadata[0],)

        class Client:
            def request(self, path, payload, *, metadata):
                request = protocol.parse("client_rpc_router.ForwardRequest", payload)
                assert request.request.message_id == 375 and not request.request.id and not request.request.body
                return Call(metadata)

        gate = GateNode(protocol, protocol.message("NodeInfo", node_id=1, zone_id=2, node_uuid="gate-a"),
                        b"x" * 32, RouterForwarder(protocol, Client(), b"y" * 32))
        try:
            request = protocol.message("ClientRequest", id=1, message_id=375)
            assert not gate._admit_request(session, request, 10, 0.0)
            assert await gate.forward.request_migration(gate, session) == response.SerializeToString()
            gate.drain_migration._runner = asyncio.current_task()
            gate.drain_migration.lease(protocol.message("contracts.kafka.GateDrainMigrationEvent",
                                                       rate_per_second=1, lease_ms=10000))
            gate.drain_migration.lease(protocol.message("contracts.kafka.GateDrainMigrationEvent"))
            assert not gate.drain_migration.rate
        finally:
            gate.drain_migration._runner = None
            await gate.close()

    asyncio.run(exercise())
