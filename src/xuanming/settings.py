import ipaddress
import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path


@dataclass(frozen=True, slots=True)
class NodeSettings:
    node_id: int
    host: str = "127.0.0.1"
    port: int = 0
    websocket_port: int | None = None
    grpc_port: int = 0
    advertise_host: str = ""
    client_host: str = ""
    client_port: int = 0
    rpc_port: int | None = None


@dataclass(frozen=True, slots=True)
class Settings:
    gate: NodeSettings = NodeSettings(1, port=8610)
    scene: NodeSettings = NodeSettings(2, port=8611, grpc_port=8612)
    battle: NodeSettings = NodeSettings(3, port=8613, grpc_port=8614)
    login: NodeSettings = NodeSettings(1, grpc_port=53000)
    zone_id: int = 1
    scene_node_type: int = 0
    physical_region: int = 1
    release_track: str = "stable"
    max_gate_connections: int = 1024
    max_battle_connections: int = 4096
    scene_tick_seconds: float = 0.05
    database: Path = Path("run/players.sqlite3")
    local_player_store: Path | None = None
    anomaly_journal: Path | None = None
    etcd: str = ""
    kafka: str = ""
    kafka_generation: int = 1
    audit_generation: int = 1
    kafka_partitions: int = 256
    router: str = ""
    player_store: str = ""
    player_inbox: str = ""
    data_service: str = ""
    scene_manager: str = ""
    player_locator: str = ""
    redis: str = ""
    login_redis: str = ""
    redis_cluster: bool = False
    scene_control_redis: str = ""
    match: str = ""
    data_service_node: dict = field(default_factory=dict)  # raw [data_service_node], parsed by data_service.config
    router_node: dict = field(default_factory=dict)  # raw [router_node], parsed by client_rpc_router.config
    db_node: dict = field(default_factory=dict)  # raw [db_node], parsed by db.config
    player_locator_node: dict = field(default_factory=dict)  # raw [player_locator_node], parsed by player_locator.config
    player_inbox_node: dict = field(default_factory=dict)  # raw [player_inbox_node], parsed by player_inbox.config

    @classmethod
    def load(cls, path: Path | None):
        if path is None:
            return cls()
        with path.open("rb") as config:
            values = tomllib.load(config)
        defaults = cls()
        for role in ("gate", "scene", "battle", "login"):
            if role in values:
                settings = getattr(defaults, role)
                merged = {field.name: getattr(settings, field.name) for field in fields(settings)}
                values[role] = NodeSettings(**(merged | values[role]))
        for name in ("database", "local_player_store", "anomaly_journal"):
            if name in values and values[name] is not None:
                values[name] = path.parent / values[name]
        return cls(**values)

    def gate_uses_router(self) -> bool:
        mode = os.getenv("GATE_CLIENT_RPC_ROUTER", "").strip().lower()
        return mode in ("1", "true", "on") if mode else bool(self.router)

    def validate(self, role: str) -> None:
        if (self.zone_id < 1 or self.release_track not in ("stable", "canary")
                or self.max_gate_connections < 1 or not 1 <= self.max_battle_connections <= 65535
                or not 0.01 <= self.scene_tick_seconds <= 1
                or self.kafka_generation < 1 or self.audit_generation < 1 or self.kafka_partitions < 1):
            raise ValueError("区服或节点公共配置无效")
        active = ("gate", "scene", "battle") if role == "local" else (role,)
        for name in active:
            node = getattr(self, name)
            if not 0 < node.node_id < (1 << (15 if name == "gate" else 17)):
                raise ValueError(f"{name} 节点号无效")
            ports = [node.port, node.grpc_port, node.client_port, node.rpc_port or 0]
            if node.websocket_port is not None:
                ports.append(node.websocket_port)
            if any(not 0 <= port <= 65535 for port in ports):
                raise ValueError(f"{name} 端口无效")
            if role == "local" and not ipaddress.ip_address(node.host).is_loopback:
                raise ValueError("本地开发模式只允许监听回环地址")
            if role != "local" and not node.advertise_host:
                raise ValueError(f"集群模式必须设置 {name}.advertise_host")
        if role != "local" and not self.etcd:
            raise ValueError("集群模式需要 etcd 地址")
        if role != "local" and not self.kafka:
            raise ValueError("集群模式需要 Kafka 地址")
        if role == "gate" and self.gate_uses_router() and not self.router:
            raise ValueError("Gate 路由模式需要客户端 RPC Router 的 gRPC 地址")
        if role == "scene" and not self.player_store:
            raise ValueError("集群 Scene 需要 PlayerStore 的 gRPC 地址")
        if role == "scene" and not all((self.data_service, self.scene_manager, self.redis, self.player_inbox)):
            raise ValueError("集群 Scene 需要 DataService、SceneManager、PlayerInbox 和玩家域 Redis")
        if role in ("scene", "battle") and not self.match:
            raise ValueError(f"集群 {role} 需要 Match 的 gRPC 地址")
        if role == "login" and not all((self.data_service, self.scene_manager, self.player_locator, self.redis)):
            raise ValueError("集群 Login 需要 DataService、SceneManager、PlayerLocator 和玩家域 Redis")


def secret(name: str, *, fallback: bytes = b"") -> bytes:
    value = os.environ.get(name, "").encode() or fallback
    if len(value) < 32:
        raise ValueError(f"{name} 至少需要 32 字节")
    return value
