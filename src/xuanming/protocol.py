from dataclasses import dataclass
from enum import IntEnum
from importlib.resources import files
from types import MappingProxyType

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import Message


class RpcKind(IntEnum):
    REQUEST = 0
    ONE_WAY = 1
    PUSH = 2
    SERVER_ONLY = 3


class RouteNode(IntEnum):
    GATE = 2
    SCENE = 3
    BATTLE = 30


@dataclass(frozen=True, slots=True)
class Route:
    message_id: int
    service: str
    method: str
    request_type: str
    response_type: str
    kind: RpcKind
    node_type: int
    client_protocol: bool

    @property
    def path(self) -> str:
        return f"/{self.service}/{self.method}"


def _ids(content: str) -> dict[str, int]:
    pairs = (line.split("=", 1) for line in content.splitlines() if "=" in line)
    return {name: int(number) for number, name in pairs if not name.startswith("reserved:")}


class Protocol:
    def __init__(self) -> None:
        assets = files("xuanming").joinpath("contracts")
        schema = descriptor_pb2.FileDescriptorSet.FromString(assets.joinpath("schema.pb").read_bytes())
        self.pool = descriptor_pool.DescriptorPool()
        descriptors = [self.pool.AddSerializedFile(file.SerializeToString()) for file in schema.file]
        self.message_ids = MappingProxyType(_ids(assets.joinpath("message_id.txt").read_text("utf-8")))
        self.event_ids = MappingProxyType(_ids(assets.joinpath("event_id.txt").read_text("utf-8")))
        file_node = self.pool.FindExtensionByName("OptionFileDefaultNode")
        client_service = self.pool.FindExtensionByName("OptionIsClientProtocolService")
        rpc_kind = self.pool.FindExtensionByName("OptionClientRpcKind")

        def option(descriptor, extension):
            options = descriptor.GetOptions()
            cls = self.message_class(options.DESCRIPTOR.full_name)
            return cls.FromString(options.SerializeToString()).Extensions[extension]

        routes = {}
        for file in descriptors:
            node_type = option(file, file_node)
            for service in file.services_by_name.values():
                client_protocol = option(service, client_service)
                for method in service.methods:
                    message_id = self.message_ids.get(service.name + method.name)
                    if message_id is None:
                        if client_protocol:
                            raise ValueError(f"客户端 RPC 缺少消息号: {method.full_name}")
                        continue
                    if message_id in routes:
                        raise ValueError(f"消息号重复: {message_id}")
                    routes[message_id] = Route(
                        message_id, service.full_name, method.name,
                        method.input_type.full_name, method.output_type.full_name,
                        RpcKind(option(method, rpc_kind)), node_type, client_protocol,
                    )
        self.routes = MappingProxyType(routes)

    def message_class(self, name: str) -> type[Message]:
        return message_factory.GetMessageClass(self.pool.FindMessageTypeByName(name))

    def message(self, name: str, /, **fields) -> Message:
        return self.message_class(name)(**fields)

    def parse(self, name: str, body: bytes) -> Message:
        return self.message_class(name).FromString(body)

    def content(self, message_id: int, call_id: int = 0, *, body: bytes = b"", error: int = 0) -> Message:
        result = self.message("MessageContent", message_id=message_id, id=call_id, serialized_message=body)
        if error:
            result.error_message.id = error
        return result
