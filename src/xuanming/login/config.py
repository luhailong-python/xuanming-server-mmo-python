"""Login settings with the Go ``etc/login.yaml`` defaults."""

import os
from dataclasses import dataclass

from ..settings import secret


@dataclass(frozen=True, slots=True)
class LoginConfig:
    zone_id: int
    gate_token_secret: bytes
    directory_secret: bytes = b""
    dev_password_secret: str = ""
    dev_password_prefixes: tuple[str, ...] = ("robot_", "dev_")
    session_ttl_ms: int = 30 * 60_000
    account_lock_ttl_ms: int = 20_000
    player_lock_ttl_ms: int = 120_000
    max_devices_per_account: int = 3
    legacy_gate_login_enabled: bool = True
    scene_manager_timeout: float = 13.5
    placement_region: int = 0
    storage_shards: int = 1
    enter_chain_budget: float = 300
    preload_pool_size: int = 256
    gate_token_ttl: int = 600

    @classmethod
    def from_env(cls, zone_id: int, environ=None):
        environ = os.environ if environ is None else environ
        if zone_id < 1:
            raise ValueError("Login 需要非零 zone_id")
        return cls(
            zone_id=zone_id, gate_token_secret=secret("MMORPG_GATE_TOKEN_SECRET"),
            directory_secret=secret("MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN"),
            dev_password_secret=environ.get("LOGIN_DEV_PASSWORD_SHARED_SECRET", ""),
        )

    def storage_id_for(self, player_id: int) -> int:
        # placement.RegionStorageIDForPlayer: FirstStorageID + region * stride + player_id % shards.
        if not 0 <= self.placement_region <= 63 or not 1 <= self.storage_shards <= 1000 or not player_id:
            raise ValueError("placement 配置无效")
        return 1_000_000 + self.placement_region * 1000 + player_id % self.storage_shards
