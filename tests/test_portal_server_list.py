from datetime import datetime, timedelta
from types import SimpleNamespace

from xuanming.portal_server_list import make_list


def test_recommendation_health_and_manual_status():
    now = datetime(2026, 11, 17)
    rows = [
        (1, "busy", 0, 100, "", None, True, 1, False, now),
        (2, "free", 0, 100, "", None, False, 2, False, now - timedelta(days=8)),
        (3, "maintenance", 1, 100, "update", None, True, 3, False, now),
    ]
    nodes = {4: [SimpleNamespace(zone_id=1, player_count=90, max_connections=100),
                 SimpleNamespace(zone_id=2, player_count=20, max_connections=100)]}
    zones = make_list(rows, nodes, now)["zones"]
    assert zones[0]["load_level"] == "FULL" and not zones[0]["recommended"]
    assert zones[1]["load_level"] == "SMOOTH" and zones[1]["recommended"] and not zones[1]["is_new"]
    assert zones[2]["status"] == "MAINTENANCE" and zones[2]["maintenance_msg"] == "update"


def test_unavailable_discovery_does_not_claim_zone_is_down():
    rows = [(1, "one", 0, 100, "", None, True, 1, False, None)]
    zone = make_list(rows, None, datetime(2026, 11, 17))["zones"][0]
    assert zone["status"] == "OPEN" and zone["recommended"] and "load_level" not in zone
    assert make_list(rows, {4: []}, datetime(2026, 11, 17))["zones"][0]["status"] == "MAINTENANCE"


def test_mysql_bit_columns_use_the_bit_value():
    rows = [(1, "one", 0, 100, "", None, b"\x01", 1, b"\x00", None)]
    zone = make_list(rows, None, datetime(2026, 11, 17))["zones"][0]
    assert zone["recommended"] is True and zone["create_role_closed"] is False
