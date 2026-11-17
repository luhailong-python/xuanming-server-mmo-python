import asyncio
import time
from contextlib import suppress

import xuanming.nodes.gate_scene_lane as lane_module
from test_transport import tcp_pair
from xuanming.nodes.gate import GateNode, Session, issue_gate_ticket
from xuanming.nodes.gate_scene_lane import SceneLane
from xuanming.protocol import Protocol
from xuanming.transport import RpcChannel, RpcNotSent


class Writer:
    def is_closing(self):
        return False

    def close(self):
        pass


def setup():
    protocol = Protocol()
    gate = GateNode(protocol, protocol.message("NodeInfo", node_id=1, zone_id=1), b"x" * 32, None)
    session = Session(131073, 1, Writer(), verified=True, logged_in=True, player_id=42,
                      scene_id=4, scene_node_id=6, scene_instance_id="scene-a")
    gate.sessions[session.session_id] = session
    session.scene_lane = SceneLane(gate, session)
    return protocol, gate, session


def request(protocol, name, call_id):
    return protocol.message("ClientRequest", id=call_id, message_id=protocol.message_ids[name])


async def stop(task):
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


def test_handoff_retry_preserves_request_order_and_previous_route():
    async def run():
        protocol, gate, session = setup()
        sent, replies = asyncio.Queue(), asyncio.Queue()

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                assert can_send()
                await sent.put((node, payload.message_content.id, payload.received_on_previous_route, one_way))
                if on_sent:
                    on_sent()
                if one_way:
                    return b""
                return (await replies.get()).SerializeToString()

        gate.scene = Scene()
        lane = session.scene_lane
        worker = asyncio.create_task(lane.run())
        first = request(protocol, "SceneBagClientPlayerGetBag", 1)
        second = request(protocol, "SceneBagClientPlayerGetBag", 2)
        movement = request(protocol, "SceneMovementClientPlayerMoveStart", 0)
        try:
            lane.admit(first, False)
            lane.admit(movement, True)
            lane.admit(second, False)
            assert await asyncio.wait_for(sent.get(), 1) == (6, 1, False, False)
            assert await asyncio.wait_for(sent.get(), 1) == (6, 0, False, True)
            assert sent.empty()
            session.scene_node_id, session.scene_id, session.scene_instance_id = 7, 5, "scene-b"
            await replies.put(protocol.message("ProcessClientPlayerMessageResponse", session_id=session.session_id,
                                               dispatch_verdict=1))
            assert await asyncio.wait_for(sent.get(), 1) == (7, 1, True, False)
            await replies.put(protocol.message("ProcessClientPlayerMessageResponse", session_id=session.session_id,
                                               message_content=protocol.content(first.message_id, 1)))
            assert await asyncio.wait_for(sent.get(), 1) == (7, 2, True, False)
            await replies.put(protocol.message("ProcessClientPlayerMessageResponse", session_id=session.session_id,
                                               message_content=protocol.content(second.message_id, 2)))
            for expected in (1, 2):
                result = await asyncio.wait_for(session.outbound.get(), 1)
                assert (result.id, result.error_message.id) == (expected, 0)
        finally:
            await stop(worker)

    asyncio.run(run())


def test_timeout_is_not_retried_and_redirect_fails_queued_before_notification():
    async def run():
        protocol, gate, session = setup()
        calls, started, finish = [], asyncio.Event(), asyncio.Event()

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                calls.append(payload.message_content.id)
                on_sent()
                started.set()
                await finish.wait()
                raise TimeoutError()

        gate.scene = Scene()
        lane = session.scene_lane
        worker = asyncio.create_task(lane.run())
        first = request(protocol, "SceneBagClientPlayerGetBag", 1)
        second = request(protocol, "SceneBagClientPlayerGetBag", 2)
        try:
            lane.admit(first, False)
            await asyncio.wait_for(started.wait(), 1)
            lane.admit(second, False)
            assert gate.drain_migration.classify(session, 0) is None
            session.redirected = True
            lane.redirect()
            failed = session.outbound.get_nowait()
            assert (failed.id, failed.error_message.id) == (2, 1003)
            finish.set()
            failed = await asyncio.wait_for(session.outbound.get(), 1)
            assert (failed.id, failed.error_message.id) == (1, 1003)
            await asyncio.sleep(0.25)
            assert calls == [1] and session.outbound.empty()
        finally:
            await stop(worker)

    asyncio.run(run())


def test_full_lane_and_replaced_session_are_fenced():
    async def run():
        protocol, gate, session = setup()
        lane = session.scene_lane
        for call_id in range(1, 65):
            lane.admit(request(protocol, "SceneBagClientPlayerGetBag", call_id), False)
        lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 65), False)
        assert session.outbound.get_nowait().error_message.id == 1008
        gate.sessions[session.session_id] = Session(session.session_id, 2, Writer())
        worker = asyncio.create_task(lane.run())
        try:
            for _ in range(64):
                assert (await asyncio.wait_for(session.outbound.get(), 1)).error_message.id == 1003
        finally:
            await stop(worker)

    asyncio.run(run())


def test_scene_lane_orders_actual_rpc0_frames():
    async def run():
        protocol, gate, session = setup()
        entered, release = asyncio.Queue(), asyncio.Event()
        method = protocol.message_ids["SceneProcessClientPlayerMessage"]

        async def handler(body):
            envelope = protocol.parse("ProcessClientPlayerMessageRequest", body)
            content = envelope.message_content
            await entered.put(content.id)
            if content.id:
                await release.wait()
                return protocol.message("ProcessClientPlayerMessageResponse", message_content=protocol.content(
                    content.message_id, content.id)).SerializeToString()
            return b""

        async with tcp_pair() as (client_io, server_io):
            client = RpcChannel(*client_io, protocol.message_class("GameRpcMessage"))
            server = RpcChannel(*server_io, protocol.message_class("GameRpcMessage"), {method: handler})

            class Scene:
                async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                    if one_way:
                        await client.send(message_id, payload.SerializeToString(), can_send=can_send)
                        return b""
                    return await client.call(message_id, payload.SerializeToString(), can_send=can_send, on_sent=on_sent)

            gate.scene = Scene()
            worker = asyncio.create_task(session.scene_lane.run())
            try:
                session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 1), False)
                session.scene_lane.admit(request(protocol, "SceneMovementClientPlayerMoveStart", 0), True)
                assert await asyncio.wait_for(entered.get(), 1) == 1
                assert await asyncio.wait_for(entered.get(), 1) == 0
                release.set()
                assert (await asyncio.wait_for(session.outbound.get(), 1)).id == 1
            finally:
                await stop(worker)
                await asyncio.gather(client.aclose(), server.aclose())

    asyncio.run(run())


def test_redirect_replies_to_queued_requests_before_notice_and_allows_inflight_reply():
    async def run():
        protocol, gate, session = setup()
        session.enter_requested = True
        sent, release = asyncio.Event(), asyncio.Event()

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                on_sent()
                sent.set()
                await release.wait()
                return protocol.message("ProcessClientPlayerMessageResponse", message_content=protocol.content(
                    payload.message_content.message_id, payload.message_content.id)).SerializeToString()

        gate.scene = Scene()
        worker = asyncio.create_task(session.scene_lane.run())
        try:
            session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 1), False)
            await asyncio.wait_for(sent.wait(), 1)
            session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 2), False)
            ticket = issue_gate_ticket(protocol, 2, 1, b"x" * 32, player_id=42)
            event = protocol.message("contracts.kafka.RedirectToGateEvent", session_id=session.session_id, player_id=42,
                                     target_gate_ip="127.0.0.1", target_gate_port=1234, token_payload=ticket.payload,
                                     token_signature=ticket.signature, token_deadline=int(time.time()) + 60)
            gate._redirect(event)
            first = session.outbound.get_nowait()
            notice = session.outbound.get_nowait()
            assert (first.id, first.error_message.id) == (2, 1003)
            assert notice.message_id == protocol.message_ids["SceneClientPlayerCommonRedirectToGate"]
            release.set()
            last = await asyncio.wait_for(session.outbound.get(), 1)
            assert (last.id, last.error_message.id) == (1, 0)
            assert session.outbound.empty()
        finally:
            await stop(worker)
            await gate.drain_migration.close()

    asyncio.run(run())


def test_retry_expires_and_late_reply_cannot_reach_reused_session(monkeypatch):
    async def run():
        monkeypatch.setattr(lane_module, "MAX_WAIT", 0.12)
        monkeypatch.setattr(lane_module, "RETRY_INTERVAL", 0.02)
        protocol, gate, session = setup()
        started, release = asyncio.Event(), asyncio.Event()
        calls = []

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                calls.append(payload.message_content.id)
                on_sent()
                if payload.message_content.id == 2:
                    started.set()
                    await release.wait()
                    return protocol.message("ProcessClientPlayerMessageResponse", message_content=protocol.content(
                        payload.message_content.message_id, 2)).SerializeToString()
                return protocol.message("ProcessClientPlayerMessageResponse", dispatch_verdict=1).SerializeToString()

        gate.scene = Scene()
        worker = asyncio.create_task(session.scene_lane.run())
        try:
            session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 1), False)
            result = await asyncio.wait_for(session.outbound.get(), 1)
            assert (result.id, result.error_message.id) == (1, 1003)
            assert len(calls) > 1
            session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 2), False)
            await asyncio.wait_for(started.wait(), 1)
            replacement = Session(session.session_id, 2, Writer())
            gate.sessions[session.session_id] = replacement
            release.set()
            await asyncio.sleep(0.05)
            assert session.outbound.empty() and replacement.outbound.empty()
        finally:
            await stop(worker)

    asyncio.run(run())


def test_only_definitely_unsent_route_change_is_retried():
    async def run():
        protocol, gate, session = setup()
        attempts = []

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                attempts.append((node, payload.received_on_previous_route, payload.message_content.id))
                if len(attempts) == 1:
                    session.scene_node_id, session.scene_id, session.scene_instance_id = 7, 5, "scene-b"
                    assert not can_send()
                    raise RpcNotSent("write guard rejected before sending")
                on_sent()
                return protocol.message("ProcessClientPlayerMessageResponse", message_content=protocol.content(
                    payload.message_content.message_id, payload.message_content.id)).SerializeToString()

        gate.scene = Scene()
        worker = asyncio.create_task(session.scene_lane.run())
        try:
            session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 1), False)
            reply = await asyncio.wait_for(session.outbound.get(), 1)
            assert reply.id == 1 and not reply.error_message.id
            assert attempts == [(6, False, 1), (7, True, 1)]
        finally:
            await stop(worker)

    asyncio.run(run())


def test_full_holding_lane_still_sends_one_way():
    async def run():
        protocol, gate, session = setup()
        sent = asyncio.Queue()

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                await sent.put((payload.message_content.id, one_way))
                return b""

        gate.scene = Scene()
        lane = session.scene_lane
        for call_id in range(1, 65):
            lane.admit(request(protocol, "SceneBagClientPlayerGetBag", call_id), False)
        lane.queue[0].retry_at = time.monotonic() + 1
        lane.admit(request(protocol, "SceneMovementClientPlayerMoveStart", 0), True)
        assert len(lane.queue) == 64 and len(lane.bypass) == 1
        worker = asyncio.create_task(lane.run())
        try:
            assert await asyncio.wait_for(sent.get(), 1) == (0, True)
            assert len(lane.queue) == 64
        finally:
            await stop(worker)

    asyncio.run(run())


def test_congested_bypass_backpressures_without_losing_one_way_messages():
    async def run():
        protocol, gate, session = setup()
        started, release = asyncio.Event(), asyncio.Event()
        sent = []

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                assert one_way and can_send()
                sent.append(payload.message_content.serialized_message)
                if len(sent) == 1:
                    started.set()
                    await release.wait()
                return b""

        gate.scene = Scene()
        lane = session.scene_lane
        for call_id in range(1, 65):
            lane.admit(request(protocol, "SceneBagClientPlayerGetBag", call_id), False)
        lane.queue[0].retry_at = time.monotonic() + 1
        message_id = protocol.message_ids["SceneMovementClientPlayerMoveStart"]
        for index in range(lane_module.MAX_BYPASS):
            assert lane.admit(protocol.message("ClientRequest", message_id=message_id, body=bytes([index])), True)
        extra = asyncio.create_task(lane.admit_wait(
            protocol.message("ClientRequest", message_id=message_id, body=b"extra"), True,
        ))
        worker = None
        try:
            await asyncio.sleep(0)
            assert not extra.done()
            session.scene_node_id, session.scene_id, session.scene_instance_id = 7, 5, "scene-b"
            worker = asyncio.create_task(lane.run())
            await asyncio.wait_for(started.wait(), 1)
            await asyncio.wait_for(extra, 1)
            assert lane.bypass[-1].route == (6, 4, "scene-a")
            release.set()
            async with asyncio.timeout(1):
                while len(sent) < lane_module.MAX_BYPASS + 1:
                    await asyncio.sleep(0)
            assert sent == [bytes([index]) for index in range(lane_module.MAX_BYPASS)] + [b"extra"]
        finally:
            release.set()
            if worker is not None:
                await stop(worker)
            if not extra.done():
                extra.cancel()
                with suppress(asyncio.CancelledError):
                    await extra

    asyncio.run(run())


def test_route_change_after_send_does_not_retry_even_on_unsent_exception():
    async def run():
        protocol, gate, session = setup()
        attempts = []

        class Scene:
            async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                attempts.append(node)
                on_sent()
                session.scene_node_id, session.scene_id, session.scene_instance_id = 7, 5, "scene-b"
                raise RpcNotSent("late failure")

        gate.scene = Scene()
        worker = asyncio.create_task(session.scene_lane.run())
        try:
            session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 1), False)
            result = await asyncio.wait_for(session.outbound.get(), 1)
            assert (result.id, result.error_message.id) == (1, 1003)
            assert attempts == [6]
        finally:
            await stop(worker)

    asyncio.run(run())


def test_real_rpc0_write_guard_retries_only_on_new_route():
    async def run():
        protocol, gate, session = setup()
        linking, link_ready = asyncio.Event(), asyncio.Event()
        received = []
        method = protocol.message_ids["SceneProcessClientPlayerMessage"]

        async def handler(body):
            payload = protocol.parse("ProcessClientPlayerMessageRequest", body)
            received.append(payload.received_on_previous_route)
            return protocol.message("ProcessClientPlayerMessageResponse", message_content=protocol.content(
                payload.message_content.message_id, payload.message_content.id)).SerializeToString()

        async with tcp_pair() as (client_io, server_io):
            client = RpcChannel(*client_io, protocol.message_class("GameRpcMessage"))
            server = RpcChannel(*server_io, protocol.message_class("GameRpcMessage"), {method: handler})

            class Scene:
                async def call(self, node, message_id, payload, *, one_way, expected_uuid, can_send, on_sent=None):
                    if node == 6:
                        linking.set()
                        await link_ready.wait()
                    return await client.call(message_id, payload.SerializeToString(), can_send=can_send, on_sent=on_sent)

            gate.scene = Scene()
            worker = asyncio.create_task(session.scene_lane.run())
            try:
                session.scene_lane.admit(request(protocol, "SceneBagClientPlayerGetBag", 1), False)
                await asyncio.wait_for(linking.wait(), 1)
                session.scene_node_id, session.scene_id, session.scene_instance_id = 7, 5, "scene-b"
                link_ready.set()
                result = await asyncio.wait_for(session.outbound.get(), 1)
                assert result.id == 1 and not result.error_message.id
                assert received == [True]
            finally:
                await stop(worker)
                await asyncio.gather(client.aclose(), server.aclose())

    asyncio.run(run())
