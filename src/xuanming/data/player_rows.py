"""玩家持久数据按 Protobuf 契约分行。"""

from enum import IntEnum

from google.protobuf.descriptor_pb2 import DescriptorProto
from google.protobuf.message import DecodeError, Message
from google.protobuf.message_factory import GetMessageClass
from google.protobuf.unknown_fields import UnknownFieldSet


class PlayerRowKind(IntEnum):
    GROWTH = 3
    PROFILE = 4
    CHARACTER = 6
    WALLET = 7
    BAG = 8
    LOCATION = 9


class PlayerRowCodec:
    """按 OptionPersistGroup 读写字段，保留组件内部的未知字段。"""

    def __init__(self, protocol) -> None:
        pool = protocol.pool
        descriptor = pool.FindMessageTypeByName("player_database")
        self._message_type = GetMessageClass(descriptor)
        option = pool.FindExtensionByName("OptionPersistGroup")
        options_type = GetMessageClass(option.containing_type)
        self._groups = {}
        for field in descriptor.fields:
            options = options_type.FromString(field.GetOptions().SerializeToString())
            if not options.HasExtension(option):
                raise ValueError(f"持久字段缺少 OptionPersistGroup: {field.full_name}")
            kind = options.Extensions[option]
            if kind and kind not in PlayerRowKind._value2member_map_:
                raise ValueError(f"持久字段分组无效: {field.full_name} ({kind})")
            self._groups[field.name] = kind
        schema = DescriptorProto()
        descriptor.CopyToProto(schema)
        self._retired = tuple((r.start, r.end) for r in schema.reserved_range)

    def project(self, data: Message) -> dict[int, bytes]:
        if data.DESCRIPTOR is not self._message_type.DESCRIPTOR:
            raise TypeError("需要当前协议池中的 player_database")
        if len(UnknownFieldSet(data)):
            raise ValueError("player_database 包含未知顶层字段")
        parts = {kind: self._message_type() for kind in PlayerRowKind}
        for field, value in data.ListFields():
            kind = self._groups[field.name]
            if kind:
                _copy_field(parts[kind], field, value)
        return {
            kind: part.SerializeToString(deterministic=True)
            for kind, part in parts.items()
        }

    def assemble(self, snapshot) -> Message:
        result = self._message_type(
            player_id=snapshot.player_id, persist_schema_gen=snapshot.schema_gen
        )
        for kind, payload in snapshot.rows.items():
            if kind not in PlayerRowKind._value2member_map_:
                raise ValueError(f"未知或已退役的玩家数据行: {kind}")
            try:
                part = self._message_type.FromString(payload)
            except DecodeError as exc:
                raise ValueError(f"玩家数据行解析失败: {kind}") from exc
            for unknown in UnknownFieldSet(part):
                if not any(lo <= unknown.field_number < hi for lo, hi in self._retired):
                    raise ValueError(f"玩家数据行包含较新字段: {kind}/{unknown.field_number}")
            for field, value in part.ListFields():
                if self._groups[field.name] == kind:
                    _copy_field(result, field, value)
        return result


def _copy_field(target: Message, field, value) -> None:
    if not field.is_repeated and field.message_type is None:
        setattr(target, field.name, value)
        return
    output = getattr(target, field.name)
    if not field.is_repeated:
        output.CopyFrom(value)
    elif field.message_type is not None and field.message_type.GetOptions().map_entry:
        map_value = field.message_type.fields_by_name["value"]
        if map_value.message_type is None:
            output.update(value)
        else:
            for key, item in value.items():
                output[key].CopyFrom(item)
    else:
        output.extend(value)
