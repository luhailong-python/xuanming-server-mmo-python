import asyncio
from types import SimpleNamespace

from test_scene_assets import _scene
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.player_inbox import BATTLE, LocalInbox, SceneInbox
from xuanming.nodes.scene_mission import Missions
from xuanming.nodes.scene_battle import SceneBattle
from xuanming.data import ConfigTables, PlayerRowCodec, PlayerRowKind, PlayerSnapshot
from xuanming.protocol import Protocol


def test_settlement_consumes_own_hold_and_keeps_other_hold(tmp_path):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        config = next(row.id for row in assets.tables["Item"].rows() if row.battle_usable)
        peer = LocalInbox(protocol, tmp_path / "hold-inbox.sqlite3")
        settlement = BattleSettlement(scene, assets.tables, assets, Missions(scene, assets.tables, assets))
        inbox = scene.inbox = SceneInbox(scene, peer, settlement, assets, clock=lambda: 100)
        try:
            data = scene.rows.assemble(player.snapshot)
            data.bag_component.items.add(bag_type=0, pos=1, item_uuid=99, config_id=config, stack_size=10)
            assert not assets.place_item_hold(data, 40, {config: 6}, created_at_ms=1, review_after_ms=2)
            assert not assets.place_item_hold(data, 41, {config: 2}, created_at_ms=1, review_after_ms=2)
            await scene.commit_player_data(player, data)
            await peer.enqueue([protocol.message("BattleSettlementData", player_id=1, battle_id=40,
                                                  outcome=1, health=100,
                                                  items_consumed=[{"item_table_id": config, "count": 9}])])
            await inbox._poll(1, BATTLE)
            saved = scene.rows.assemble(player.snapshot)
            assert [(h.hold_id, h.lines[0].count) for h in saved.item_holds.holds] == [(41, 2)]
            assert assets.removable_count(saved, config) == 0
            assert sum(item.size for _, item in assets.entries(saved.bag_component, 0)
                       if item.config_id == config) == 2
            assert saved.battle_results.entries[0].items_consumed[0].count == 8
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_prepare_does_not_freeze_after_hold_save_passes_deadline():
    async def exercise():
        protocol, tables = Protocol(), ConfigTables()
        now = [1000]
        config = next(row.id for row in tables["Item"].rows() if row.battle_usable)
        data = protocol.message("player_database", player_id=1,
                                derived_attributes_component={"health": 100}, level_component={"level": 1})
        data.bag_component.items.add(item_uuid=1, config_id=config, stack_size=2)
        player = SimpleNamespace(player_id=1, snapshot=data, gate_uuid="gate", session_id=1 << 17,
                                 disconnect_at=None, motion=SimpleNamespace(velocity=(1., 0., 0.)))
        writes = []

        async def flush(_):
            pass

        async def commit(player, data):
            player.snapshot = type(data).FromString(data.SerializeToString())
            writes.append(len(player.snapshot.item_holds.holds))
            now[0] = 3001

        scene = SimpleNamespace(protocol=protocol, rows=SimpleNamespace(assemble=lambda snapshot: snapshot),
                                _lock=lambda _: asyncio.Lock(), locations={1: SimpleNamespace(players={1: player})},
                                mutable=lambda _: True, flush_player_rows=flush, commit_player_data=commit,
                                info=SimpleNamespace(node_id=1, node_uuid="scene", zone_id=1))
        battle = SceneBattle(scene, tables, now_ms=lambda: now[0])
        request = protocol.message("PrepareBattleRequest", player_id=1, battle_id=40, battle_node_id=2,
                                   deadline_ms=8000, prepare_deadline_ms=3000)
        result = await battle.prepare(request)
        assert result.error_message.id == 1005
        assert writes == [1, 0] and not battle.frozen(1)
        assert player.motion.velocity == (1., 0., 0.)

    asyncio.run(exercise())


def test_prepare_releases_item_hold_when_redis_lock_write_fails():
    async def exercise():
        protocol, tables = Protocol(), ConfigTables()
        config = next(row.id for row in tables["Item"].rows() if row.battle_usable)
        for failure in ("conflict", "error"):
            data = protocol.message("player_database", player_id=1,
                                    derived_attributes_component={"health": 100}, level_component={"level": 1})
            data.bag_component.items.add(item_uuid=1, config_id=config, stack_size=2)
            player = SimpleNamespace(player_id=1, snapshot=data, gate_uuid="gate", session_id=1 << 17,
                                     disconnect_at=None, motion=SimpleNamespace(velocity=(1., 0., 0.)))
            writes = []

            async def flush(_):
                pass

            async def commit(target, updated):
                target.snapshot = type(updated).FromString(updated.SerializeToString())
                writes.append(len(target.snapshot.item_holds.holds))

            class FailingRedis:
                async def eval(self, *_args):
                    if failure == "error":
                        raise OSError("redis timeout")
                    return 0

            scene = SimpleNamespace(protocol=protocol, rows=SimpleNamespace(assemble=lambda snapshot: snapshot),
                                    _lock=lambda _: asyncio.Lock(), locations={1: SimpleNamespace(players={1: player})},
                                    mutable=lambda _: True, flush_player_rows=flush, commit_player_data=commit,
                                    info=SimpleNamespace(node_id=1, node_uuid="scene", zone_id=1))
            battle = SceneBattle(scene, tables, now_ms=lambda: 1000, redis=FailingRedis())
            request = protocol.message("PrepareBattleRequest", player_id=1, battle_id=40, battle_node_id=2,
                                       deadline_ms=8000, prepare_deadline_ms=3000)
            result = await battle.prepare(request)
            assert result.error_message.id == 1003
            assert writes == [1, 0] and not battle.frozen(1)
            assert not player.snapshot.item_holds.holds

    asyncio.run(exercise())


def test_reap_caches_clean_save_seq_and_parses_only_profile_row():
    async def exercise():
        protocol = Protocol()
        codec = PlayerRowCodec(protocol)
        clean = protocol.message("player_database", player_id=1, profile_component={"name": "hero"})
        snapshot = PlayerSnapshot(1, b"scene", 1, save_seq=5, rows=codec.project(clean))
        player = SimpleNamespace(player_id=1, snapshot=snapshot)
        world = SimpleNamespace(players={1: player})
        parse_calls = 0
        original_parse = protocol.parse

        def counting_parse(name, raw):
            nonlocal parse_calls
            parse_calls += 1
            return original_parse(name, raw)

        def forbidden_assemble(_snapshot):
            raise AssertionError("reap must not assemble all 6 rows")

        scene = SimpleNamespace(
            protocol=SimpleNamespace(parse=counting_parse, pool=protocol.pool),
            rows=SimpleNamespace(assemble=forbidden_assemble),
            worlds={1: world}, locations={1: world}, mutable=lambda _: True,
        )
        battle = SceneBattle(scene, None, now_ms=lambda: 500)
        battle.reap()
        battle.reap()
        assert parse_calls == 1 and battle._hold_checked_seq[1] == 5

        held = protocol.message("player_database", player_id=1, profile_component={"name": "hero"})
        held.item_holds.holds.add(hold_id=40, kind=1, review_after_ms=600).lines.add(config_id=3, count=1)
        player.snapshot = PlayerSnapshot(1, b"scene", 1, save_seq=6, rows={
            PlayerRowKind.PROFILE: codec.project(held)[PlayerRowKind.PROFILE],
        })
        battle.reap()
        assert parse_calls == 2 and 1 not in battle._hold_checked_seq and not battle._hold_reviews

        battle.departed(player)
        assert 1 not in battle._hold_checked_seq

    asyncio.run(exercise())
