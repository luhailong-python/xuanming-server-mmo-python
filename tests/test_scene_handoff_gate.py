import asyncio
from types import SimpleNamespace

import pytest

from test_scene_assets import _scene
from xuanming.nodes.scene import _may_retry_handoff, _replayable_on_another_scene
from xuanming.nodes.scene_travel import SceneTravel
from xuanming.protocol import Protocol


def test_handoff_whitelist_and_trade_seal_are_distinct():
    p = Protocol()
    for name in ("SceneBagClientPlayerGetBag", "SceneSkillClientPlayerListSkills",
                 "SceneSkillClientPlayerUpgradeSkill"):
        assert _may_retry_handoff(p.routes[p.message_ids[name]])
    for name in ("SceneSceneClientPlayerGetSyncDigest", "SceneSceneClientPlayerSceneInfoC2S"):
        route = p.routes[p.message_ids[name]]
        assert _replayable_on_another_scene(route) and not _may_retry_handoff(route)
    for name in ("SceneSkillClientPlayerReleaseSkill", "SceneMovementClientPlayerTeleportRequest",
                 "SceneSceneClientPlayerEnterScene"):
        assert not _replayable_on_another_scene(p.routes[p.message_ids[name]])
    travel = object.__new__(SceneTravel)
    travel._trips, travel._trade_drains = {}, {1: object()}
    assert travel.frozen(1) and not travel.handoff_frozen(1)
    travel._trips[1] = SimpleNamespace(frozen=True)
    assert travel.handoff_frozen(1)


def test_scene_handoff_verdict_and_old_route_fences(tmp_path):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        assets.register_client_handlers()

        def request(name, *, session=9, player=1, previous=False, body=None):
            message_id = p.message_ids[name]
            route = p.routes[message_id]
            return p.message("ProcessClientPlayerMessageRequest", session_id=session, player_id=player,
                             received_on_previous_route=previous,
                             message_content=p.content(message_id, 1, body=(
                                 body if body is not None else p.message(route.request_type).SerializeToString())))

        bag = "SceneBagClientPlayerGetBag"
        teleport = "SceneMovementClientPlayerTeleportRequest"
        try:
            # Unknown session is a transient state; a known session with a different player is not.
            unknown = await scene.process(request(bag, session=10), "gate")
            assert unknown.dispatch_verdict == 1 and not unknown.HasField("message_content")
            with pytest.raises(ValueError):
                await scene.process(request(bag, player=2), "gate")
            legacy = await scene.process(request(bag, player=0), "gate")
            assert legacy.HasField("message_content") and not legacy.message_content.error_message.id
            # 兼容缺失玩家 ID 不放宽 Gate UUID/会话绑定。
            other_gate = await scene.process(request(bag, player=0), "other-gate")
            assert other_gate.dispatch_verdict == 1 and not other_gate.HasField("message_content")
            with pytest.raises(ValueError):
                await scene.process(request(teleport, session=10), "gate")

            scene.travel = SimpleNamespace(handoff_frozen=lambda _: True)
            held = await scene.process(request(bag), "gate")
            assert held.dispatch_verdict == 1 and not held.HasField("message_content")
            actor = scene.locations[1].players[1]
            actor.pending_save = SimpleNamespace(release=True)
            exiting = await scene.process(request(bag), "gate")
            assert exiting.dispatch_verdict == 0
            assert p.parse("GetBagResponse", exiting.message_content.serialized_message).error_message.id == 1006
            actor.pending_save = None
            # A malformed request never receives a promise of retry.
            invalid = await scene.process(request(bag, body=b"\xff"), "gate")
            assert invalid.dispatch_verdict == 0 and invalid.message_content.error_message.id == 1005
            # Scene-bound requests are rejected on an old route even during a handoff.
            stale = await scene.process(request(teleport, previous=True), "gate")
            assert stale.dispatch_verdict == 0 and not stale.HasField("message_content")
            old_exit = await scene.process(request("ScenePlayerExitGame", previous=True), "gate")
            assert not old_exit.HasField("message_content") and scene.sessions[("gate", 9)] is actor

            scene.travel = SimpleNamespace(handoff_frozen=lambda _: False)
            accepted = await scene.process(request(bag, previous=True), "gate")
            assert accepted.dispatch_verdict == 0 and accepted.HasField("message_content")
            assert not accepted.message_content.error_message.id
        finally:
            scene.travel = None
            await scene.close()
            await assets.close()

    asyncio.run(exercise())
