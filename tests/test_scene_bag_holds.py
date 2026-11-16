import asyncio

from test_scene_assets import _scene
from xuanming.nodes.scene_player import _bag


def test_inventory_shows_only_held_quantities_in_current_inventory(tmp_path):
    async def exercise():
        protocol, scene, assets, _ = await _scene(tmp_path)
        player = scene.locations[1].players[1]
        configs = sorted(row.id for row in assets.tables["Item"].rows() if row.max_stack_size > 1)[:3]
        try:
            data = scene.rows.assemble(player.snapshot)
            data.bag_component.ClearField("items")
            data.bag_component.items.add(bag_type=0, pos=0, item_uuid=101, config_id=configs[1], stack_size=4)
            data.bag_component.items.add(bag_type=0, pos=1, item_uuid=102, config_id=configs[0], stack_size=7)
            data.bag_component.items.add(bag_type=1, pos=0, item_uuid=103, config_id=configs[2], stack_size=6)
            hold = data.item_holds.holds.add(hold_id=10, kind=1)
            hold.lines.add(config_id=configs[0], count=2)
            hold.lines.add(config_id=configs[1], count=9)
            hold.lines.add(config_id=configs[2], count=2)
            data.item_holds.holds.add(hold_id=11, kind=1).lines.add(config_id=configs[0], count=3)
            await scene.commit_player_data(player, data)

            inventory = _bag(scene, assets.tables, player, 0)
            assert not inventory.error_message.id
            assert [(line.config_id, line.count) for line in inventory.bag.held_items] == [
                (configs[0], 5), (configs[1], 4),
            ]
            assert not _bag(scene, assets.tables, player, 1).bag.held_items
        finally:
            await scene.close()

    asyncio.run(exercise())
