from types import SimpleNamespace

from xuanming.portal_health import should_publish, summarize


def test_health_publication_thresholds_and_capacity():
    gates = [SimpleNamespace(player_count=10, max_connections=100)]
    record = summarize(1, 100, gates, [object()])
    assert record == {"zoneId": 1, "status": "HEALTHY", "onlineCount": 10, "capacity": 90}
    assert not should_publish(record, record | {"onlineCount": 11}, 29)
    assert should_publish(record, record | {"onlineCount": 11}, 30)
    assert should_publish(record, record | {"onlineCount": 45}, 1)
    assert should_publish(record, record | {"status": "DOWN"}, 0)
    unlimited = record | {"capacity": 0}
    assert not should_publish(unlimited, unlimited | {"onlineCount": 1000}, 3600)
