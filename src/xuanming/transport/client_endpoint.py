"""Resolve the address advertised to Gate and Battle clients."""

import re
from collections.abc import Mapping
from dataclasses import dataclass

_BARE_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?")


@dataclass(frozen=True, slots=True)
class ClientEndpointSettings:
    source: str | None = None
    host: str = ""
    port: int = 0
    required: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str]):
        source = env.get("CLIENT_ENDPOINT_SOURCE") or None
        required = env.get("CLIENT_ENDPOINT_REQUIRED", "")
        if required not in ("", "0", "1"):
            raise ValueError("CLIENT_ENDPOINT_REQUIRED must be 0 or 1")
        if source in (None, "none"):
            if required == "1":
                raise ValueError("CLIENT_ENDPOINT_REQUIRED=1 requires CLIENT_ENDPOINT_SOURCE=static or agones")
            return cls(source=source)
        if source not in ("static", "agones"):
            raise ValueError("CLIENT_ENDPOINT_SOURCE must be none, static or agones")

        host = env.get("CLIENT_ENDPOINT_HOST", "")
        if (source == "static" or host) and (len(host) > 253 or not _BARE_HOST.fullmatch(host)):
            raise ValueError("CLIENT_ENDPOINT_HOST must be a bare IPv4 literal or DNS name")
        raw_port = env.get("CLIENT_ENDPOINT_PORT", "")
        if source == "agones":
            if raw_port:
                raise ValueError("CLIENT_ENDPOINT_PORT must be unset for CLIENT_ENDPOINT_SOURCE=agones")
            return cls(source=source, host=host, required=required == "1")

        digits = raw_port.lstrip("0")
        if not (raw_port.isascii() and raw_port.isdecimal() and 1 <= len(digits) <= 5 and int(digits) <= 65535):
            raise ValueError("CLIENT_ENDPOINT_PORT must be a decimal integer in 1..65535")
        return cls(source=source, host=host, port=int(digits), required=required == "1")

    def resolve(
        self, *, configured: tuple[str, int] | None = None, agones: tuple[str, int] | None = None,
    ) -> tuple[str, int] | None:
        if self.source is None:
            return configured
        if self.source == "none":
            return None
        if self.source == "static":
            return self.host, self.port
        if not agones or not agones[0] or not 1 <= agones[1] <= 65535:
            raise ValueError("Agones did not provide an assigned client address and port")
        return self.host or agones[0], agones[1]
