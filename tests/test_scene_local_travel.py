from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables
from xuanming.nodes.scene import SceneNode
from xuanming.protocol import Protocol


@pytest.mark.parametrize("zone,config,frozen,expected", [
    (0, 0, False, 3024),
    (1, 0, False, 3024),
    (2, 999999, False, 3007),
    (2, 0, True, 3025),
    (2, 0, False, 3027),
])
def test_local_cross_zone_travel_returns_a_specific_rejection(zone, config, frozen, expected):
    protocol = Protocol()
    scene = SimpleNamespace(protocol=protocol, info=SimpleNamespace(zone_id=1), tables=ConfigTables(),
                            battle=SimpleNamespace(frozen=lambda player_id: frozen))
    request = protocol.message("TravelToZoneRequest", target_zone_id=zone, scene_config_id=config)
    reply = SceneNode.local_zone_travel(scene, None, SimpleNamespace(player_id=1), None, request)
    assert reply.error_message.id == expected
