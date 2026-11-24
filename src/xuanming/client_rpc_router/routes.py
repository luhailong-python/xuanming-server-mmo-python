"""基于 schema.pb 推导与 Go protogen 完全一致的 message_id -> eNodeType 路由表。"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from types import MappingProxyType

from ..protocol import Protocol

BATTLE_NODE_TYPE = 28  # common.base.eNodeType.BattleNodeService
_SKIP_FILE_DEFAULT_NODES = frozenset({2, 3, 5})  # NODE_GATE, NODE_SCENE, NODE_DB


@dataclass(frozen=True, slots=True)
class RouteEntry:
    message_id: int
    full_method: str
    service: str
    service_full_name: str
    method: str
    node_type: int
    node_type_name: str
    client_protocol: bool


def _to_camel(raw: str) -> str:
    """将 snake_case 或点分包名/目录名转为与 protogen 一致的驼峰前缀。"""
    leaf = raw.rsplit(".", 1)[-1]
    if leaf.endswith("pb") and len(leaf) > 2:
        leaf = leaf[:-2]
    parts = [part for part in leaf.replace("-", "_").split("_") if part]
    return "".join(part[:1].upper() + part[1:] for part in parts)


def _resolve_node_type(service_name: str, package_name: str, file_name: str, enum_by_name: dict) -> tuple[int, str] | None:
    """按 protogen 三级回退查找 common.base.eNodeType。"""
    dir_name = posixpath.basename(posixpath.dirname(file_name))
    candidates = [
        f"{service_name}NodeService",
        f"{_to_camel(package_name)}NodeService" if package_name else "",
        f"{_to_camel(dir_name)}NodeService" if dir_name else "",
    ]
    for candidate in candidates:
        if candidate and candidate in enum_by_name:
            return enum_by_name[candidate].number, candidate
    return None


def build_route_table(protocol: Protocol) -> MappingProxyType[int, RouteEntry]:
    """扫描 schema.pb 构造路由表，复现 Go route_table.go 的过滤与映射规则。"""
    enum_by_name = protocol.pool.FindEnumTypeByName("common.base.eNodeType").values_by_name
    file_node_ext = protocol.pool.FindExtensionByName("OptionFileDefaultNode")
    client_service_ext = protocol.pool.FindExtensionByName("OptionIsClientProtocolService")

    def read_extension(descriptor, extension):
        options = descriptor.GetOptions()
        cls = protocol.message_class(options.DESCRIPTOR.full_name)
        return cls.FromString(options.SerializeToString()).Extensions[extension]

    entries: dict[int, RouteEntry] = {}
    seen_files: set[str] = set()
    for route in protocol.routes.values():
        service = protocol.pool.FindServiceByName(route.service)
        file_desc = service.file
        if file_desc.name in seen_files:
            continue
        seen_files.add(file_desc.name)
        if file_desc.GetOptions().cc_generic_services:
            continue
        file_default_node = read_extension(file_desc, file_node_ext)
        if file_default_node in _SKIP_FILE_DEFAULT_NODES:
            continue
        for svc in file_desc.services_by_name.values():
            is_client = bool(read_extension(svc, client_service_ext))
            resolved = _resolve_node_type(svc.name, file_desc.package, file_desc.name, enum_by_name)
            if resolved is None:
                if is_client:
                    raise ValueError(f"客户端协议服务无法映射到 common.base.eNodeType: {svc.full_name}")
                continue
            node_type, node_type_name = resolved
            for method in svc.methods:
                message_id = protocol.message_ids.get(svc.name + method.name)
                if message_id is None:
                    continue
                entries[message_id] = RouteEntry(
                    message_id=message_id,
                    full_method=f"/{svc.full_name}/{method.name}",
                    service=svc.name,
                    service_full_name=svc.full_name,
                    method=method.name,
                    node_type=node_type,
                    node_type_name=node_type_name,
                    client_protocol=is_client,
                )
    return MappingProxyType(entries)


def watched_node_types(routes: MappingProxyType[int, RouteEntry]) -> tuple[tuple[int, str], ...]:
    """返回所有需要监听的客户端协议目标节点类型（排除 BattleNodeService=28）。"""
    pairs = {
        (entry.node_type, entry.node_type_name)
        for entry in routes.values()
        if entry.client_protocol and entry.node_type != BATTLE_NODE_TYPE
    }
    return tuple(sorted(pairs, key=lambda item: item[0]))
