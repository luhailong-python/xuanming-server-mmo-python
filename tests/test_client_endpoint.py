import pytest

from xuanming.transport.client_endpoint import ClientEndpointSettings


def test_configured_default_and_explicit_none():
    configured = ("toml.example.com", 30080)
    ignored = {"CLIENT_ENDPOINT_HOST": "unused host", "CLIENT_ENDPOINT_PORT": "-1"}
    assert ClientEndpointSettings.from_env(ignored).resolve(configured=configured) == configured
    assert ClientEndpointSettings.from_env(ignored | {"CLIENT_ENDPOINT_SOURCE": "none"}).resolve(
        configured=configured,
    ) is None


def test_static_advertised_endpoint_and_agones_override():
    settings = ClientEndpointSettings.from_env({
        "CLIENT_ENDPOINT_SOURCE": "static", "CLIENT_ENDPOINT_HOST": "gate-1.example.com",
        "CLIENT_ENDPOINT_PORT": "0030800", "CLIENT_ENDPOINT_REQUIRED": "1",
    })
    assert settings.required
    assert settings.resolve(configured=("127.0.0.1", 8610)) == ("gate-1.example.com", 30800)
    settings = ClientEndpointSettings.from_env({"CLIENT_ENDPOINT_SOURCE": "agones"})
    assert settings.resolve(agones=("192.0.2.40", 30000)) == ("192.0.2.40", 30000)
    settings = ClientEndpointSettings.from_env({
        "CLIENT_ENDPOINT_SOURCE": "agones", "CLIENT_ENDPOINT_HOST": "127.0.0.1",
    })
    assert settings.resolve(agones=("192.0.2.40", 30000)) == ("127.0.0.1", 30000)
    with pytest.raises(ValueError, match="assigned client address"):
        settings.resolve(agones=("", 30000))


@pytest.mark.parametrize("env,setting", [
    ({"CLIENT_ENDPOINT_SOURCE": "STATIC"}, "SOURCE"),
    ({"CLIENT_ENDPOINT_REQUIRED": "true"}, "REQUIRED"),
    ({"CLIENT_ENDPOINT_SOURCE": "none", "CLIENT_ENDPOINT_REQUIRED": "1"}, "REQUIRED"),
    ({"CLIENT_ENDPOINT_REQUIRED": "1"}, "REQUIRED"),
    ({"CLIENT_ENDPOINT_SOURCE": "static"}, "HOST"),
    ({"CLIENT_ENDPOINT_SOURCE": "agones", "CLIENT_ENDPOINT_HOST": "https://host"}, "HOST"),
    ({"CLIENT_ENDPOINT_SOURCE": "agones", "CLIENT_ENDPOINT_HOST": "host:123"}, "HOST"),
    ({"CLIENT_ENDPOINT_SOURCE": "agones", "CLIENT_ENDPOINT_HOST": "$(POD_HOST)"}, "HOST"),
    ({"CLIENT_ENDPOINT_SOURCE": "agones", "CLIENT_ENDPOINT_PORT": "80"}, "PORT"),
    ({"CLIENT_ENDPOINT_SOURCE": "static", "CLIENT_ENDPOINT_HOST": "host", "CLIENT_ENDPOINT_PORT": "65536"}, "PORT"),
    ({"CLIENT_ENDPOINT_SOURCE": "static", "CLIENT_ENDPOINT_HOST": "host", "CLIENT_ENDPOINT_PORT": " 80"}, "PORT"),
    ({"CLIENT_ENDPOINT_SOURCE": "static", "CLIENT_ENDPOINT_HOST": "host", "CLIENT_ENDPOINT_PORT": "＋80"}, "PORT"),
])
def test_invalid_native_environment_fails_closed(env, setting):
    with pytest.raises(ValueError, match=f"CLIENT_ENDPOINT_{setting}"):
        ClientEndpointSettings.from_env(env)
