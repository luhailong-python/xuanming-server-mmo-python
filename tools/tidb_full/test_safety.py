import json
from types import SimpleNamespace

import pytest

import safety


def test_sql_listener_must_have_exactly_one_loopback_binding(monkeypatch):
    checked = []
    monkeypatch.setattr(safety, "assert_owned_tidb", lambda *args: checked.append(args))
    bindings = [{"HostIp": "127.0.0.1", "HostPort": "12345"}]
    monkeypatch.setattr(safety.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(
        stdout=json.dumps([{"NetworkSettings": {"Ports": {"4000/tcp": bindings}}}])))
    safety.assert_exclusive_tidb(12345, "project", "container", "run")
    assert checked == [(12345, "project", "container", "run")]
    bindings.append({"HostIp": "0.0.0.0", "HostPort": "12345"})
    with pytest.raises(RuntimeError):
        safety.assert_exclusive_tidb(12345, "project", "container", "run")
    bindings[:] = [{"HostIp": "127.0.0.1", "HostPort": "54321"}]
    with pytest.raises(RuntimeError):
        safety.assert_exclusive_tidb(12345, "project", "container", "run")
