import asyncio
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

import xuanming.nodes.scene as scene_module
from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.player_inbox import BATTLE
from xuanming.nodes.scene import ZERO, SceneNode
from xuanming.nodes.scene_connection_hold import (
    HOLD_WINDOW,
    HoldDecision,
    HoldVerdict,
    SceneConnectionHolds,
    afk_pass_key,
    decide_hold,
    judge_hold,
    parse_afk_pass,
)
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect


def test_native_hold_and_monthly_pass_boundaries():
    facts = dict(exiting=False, handoff_in_flight=False, evacuating=False, bound=True, already_held=False)
    assert HOLD_WINDOW == 25
    assert decide_hold(**facts) is HoldDecision.HOLD
    assert decide_hold(**(facts | {"already_held": True, "bound": False})) is HoldDecision.KEEP_HOLDING
    for gate in ("exiting", "handoff_in_flight", "evacuating"):
        assert decide_hold(**(facts | {gate: True})) is HoldDecision.EXIT_NOW
    assert decide_hold(**(facts | {"bound": False})) is HoldDecision.EXIT_NOW
    assert afk_pass_key(42) == "player:afk_pass:{42}"
    for value in (None, b"", b" 106", b"+106", b"106\n", b"1_06", b"105", b"18446744073709551616"):
        assert parse_afk_pass(value, 100) == 0
    assert parse_afk_pass(b"000106", 100) == 106
    assert judge_hold(24.999, None, 100, False) is HoldVerdict.PROBE
    assert judge_hold(24.999, None, 100, True) is HoldVerdict.KEEP
    assert judge_hold(25, None, 100, True) is HoldVerdict.EXIT
    assert judge_hold(25, 0, 100, False) is HoldVerdict.EXIT
    assert judge_hold(100, 150, 144, False) is HoldVerdict.KEEP
    assert judge_hold(100, 150, 145, False) is HoldVerdict.PROBE
    assert judge_hold(100, 150, 179, True) is HoldVerdict.KEEP
    assert judge_hold(100, 150, 180, True) is HoldVerdict.EXIT


def test_monthly_pass_renews_once_and_late_probe_cannot_host_new_hold():
    async def exercise():
        clock = SimpleNamespace(monotonic=5., unix=100)
        player = SimpleNamespace(player_id=42, session_id=13, generation=3)
        started = []
        old_probe = asyncio.Event()
        calls = 0

        async def read_pass(player_id):
            nonlocal calls
            assert player_id == player.player_id
            calls += 1
            if calls == 1:
                try:
                    await old_probe.wait()
                except asyncio.CancelledError:
                    return b"9999"
            return {2: b"150", 3: b"220", 4: None}[calls]

        holds = SceneConnectionHolds(
            read_pass, started.append, clock=lambda: clock.monotonic, unix_clock=lambda: clock.unix,
        )
        try:
            assert holds.start(player)
            await asyncio.sleep(0)
            assert holds.held(player) and not holds.hosted(player)
            assert not holds.start(player)
            assert not holds.resumable(player, 13, 3)
            assert not holds.resumable(player, 0xFFFFFFFF, 4)
            assert holds.resumable(player, 13, 4)
            holds.forget(player)
            player.generation = 4
            assert holds.start(player)
            await asyncio.sleep(0)
            assert calls == 2 and started == [player]
            assert holds.hosted(player)

            clock.monotonic, clock.unix = 50., 144
            holds.sweep()
            await asyncio.sleep(0)
            assert calls == 2 and not holds.expired(player)
            clock.monotonic, clock.unix = 51., 145
            holds.sweep()
            holds.sweep()
            await asyncio.sleep(0)
            assert calls == 3 and started == [player] and not holds.expired(player)
            clock.monotonic, clock.unix = 121., 215
            holds.sweep()
            await asyncio.sleep(0)
            assert calls == 4 and holds.expired(player)
        finally:
            await holds.close()

    asyncio.run(exercise())


def test_scene_disconnect_retains_actor_saves_position_and_honors_release_fences(tmp_path, monkeypatch):
    async def exercise():
        clock = SimpleNamespace(now=100.)
        monkeypatch.setattr(scene_module, "time", SimpleNamespace(monotonic=lambda: clock.now, time_ns=time.time_ns))
        p, emitted = Protocol(), []
        store = SqlitePlayerStore(tmp_path / "holds.sqlite3")
        scene = SceneNode(
            p, p.message("NodeInfo", node_id=3, node_uuid="hold-scene", zone_id=1), store, PlayerRowCodec(p),
            lambda player, content: emitted.append((player.player_id, content)),
        )
        scene.connection_holds.clock = lambda: clock.now
        scene.create(p.message("CreateSceneRequest", scene_id=1, config_id=1))

        async def enter(player, session, generation=1):
            await scene.enter(p.message(
                "PlayerEnterGameNodeRequest", player_id=player, session_id=session, scene_id=1,
                owner_revision=1, login_generation=generation,
            ), "gate")
            return scene.locations[player].players[player]

        async def exit_rpc(player, session, reason):
            return await scene.process(p.message(
                "ProcessClientPlayerMessageRequest", player_id=player, session_id=session,
                message_content=p.content(p.message_ids["ScenePlayerExitGame"], 1, body=p.message(
                    "GameNodeExitGameRequest", reason=reason,
                ).SerializeToString()),
            ), "gate")

        try:
            actor = await enter(1, 11)
            observer = await enter(2, 12)
            world = scene.locations[1]
            x, y, z = actor.transform.position
            scene._move_position(world, actor, (x + 1, y, z))
            actor.motion.velocity = (2., 0., 0.)
            before = actor.snapshot.save_seq
            emitted.clear()
            await exit_rpc(1, 11, 2)
            assert scene.connection_holds.held(actor) and ("gate", 11) not in scene.sessions
            assert actor.motion.velocity == ZERO and actor.entity in world.nearby(observer)
            message_id = p.message_ids["SceneMovementClientPlayerNotifyActorMove"]
            stops = [p.parse("ActorMoveS2C", content.serialized_message) for recipient, content in emitted
                     if recipient == 2 and content.message_id == message_id]
            assert len(stops) == 1 and stops[0].entity == actor.entity and stops[0].move_state == 0
            with pytest.raises(ValueError, match="已断开"):
                await enter(1, 11)
            await scene.tick(0.05)
            await asyncio.gather(*tuple(scene._maintenance.values()))
            assert actor.snapshot.save_seq > before and not actor.snapshot.released
            assert scene.rows.assemble(actor.snapshot).transform.location.x == x + 1

            clock.now = 124.
            await exit_rpc(1, 11, 2)
            assert not scene.connection_holds.expired(actor)
            assert await enter(1, 11, generation=2) is actor
            assert not scene.connection_holds.held(actor) and scene.sessions["gate", 11] is actor

            await exit_rpc(1, 11, 2)
            request = p.message(
                "scene_node.ReleasePlayerRequest", player_id=1, session_ended=True,
                expected_owner_epoch=actor.snapshot.owner_epoch, covered_login_generation=1, covered_session_id=11,
            )
            await scene.release(request)
            assert scene.connection_holds.held(actor)
            request.covered_login_generation = 2
            await scene.release(request)
            assert 1 not in scene.locations and actor.snapshot.released

            actor = await enter(1, 21, generation=3)
            await exit_rpc(1, 21, 2)
            await scene.lease_expired(1, 20, "gate", 1)
            assert scene.connection_holds.held(actor)
            await scene.lease_expired(1, 21, "gate", 1)
            assert 1 not in scene.locations and actor.snapshot.released

            actor = await enter(1, 22, generation=4)
            await exit_rpc(1, 22, 2)
            clock.now += HOLD_WINDOW
            await scene.tick(0.05)
            await asyncio.gather(*tuple(scene._maintenance.values()))
            assert 1 not in scene.locations and actor.snapshot.released
            await exit_rpc(2, 12, 1)
            assert 2 not in scene.locations and observer.snapshot.released
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_battle_inbox_settles_while_scene_holds_disconnected_actor(tmp_path):
    async def exercise():
        defaults = Settings()
        settings = replace(
            defaults, database=tmp_path / "held-battle.sqlite3",
            gate=replace(defaults.gate, port=0),
            scene=replace(defaults.scene, port=0, grpc_port=0),
            battle=replace(defaults.battle, port=0, grpc_port=0),
        )
        async with Runtime(settings, "local", dev_token="test-only") as runtime:
            p, scene, gate = runtime.protocol, runtime.scene, runtime.gate
            ids = p.message_ids
            async with connect(p, gate.info.endpoint) as client:
                await client.send(gate.issue_ticket())
                assert (await client.receive("ClientTokenVerifyResponse")).success
                assert not (await client.rpc(ids["ClientPlayerLoginLogin"], 1, p.message(
                    "loginpb.LoginRequest", account="local", password="test-only",
                ))).error_message.id
                assert (await client.rpc(ids["ClientPlayerLoginEnterGame"], 2, p.message(
                    "loginpb.EnterGameRequest", player_id=1, request_id="held-battle-entry",
                ))).player_id == 1
                await client.content(ids["SceneSceneClientPlayerNotifyEnterScene"])
                actor = scene.locations[1].players[1]
                now = time.time_ns() // 1_000_000
                prepare = p.message("PrepareBattleRequest", player_id=1, battle_id=42, deadline_ms=now + 60_000)
                ready = await scene.battle.prepare(prepare)
                assert not ready.error_message.id
                await scene.battle.confirm(p.message(
                    "BattleConfirmedEvent", player_id=1, battle_id=42, deadline_ms=now + 60_000,
                ))
                assert scene.battle.frozen(1)

            async with asyncio.timeout(3):
                while not scene.connection_holds.held(actor):
                    await asyncio.sleep(.01)
            await runtime.local_inbox.enqueue([p.message(
                "BattleSettlementData", player_id=1, battle_id=42, outcome=1,
                health=ready.snapshot.max_health, currency_gains=[{"currency_type": 0, "amount": 37}],
            )])
            scene.inbox.wake(1, BATTLE)
            async with asyncio.timeout(3):
                while await asyncio.to_thread(runtime.local_inbox._fetch, 1):
                    await asyncio.sleep(.02)
            assert scene.connection_holds.held(actor) and not scene.battle.frozen(1)
            saved = scene.rows.assemble(actor.snapshot)
            assert saved.currency.values[0] == 37 and saved.battle_results.entries[0].battle_id == 42
            prepare.battle_id += 1
            assert (await scene.battle.prepare(prepare)).error_message.id == 1011

    asyncio.run(exercise())
