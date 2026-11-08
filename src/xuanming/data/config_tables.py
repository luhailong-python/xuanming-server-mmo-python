"""从同一份导表清单加载 JSON 或 protobuf，原子发布整批配置表。"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType

from google.protobuf import descriptor_pb2, descriptor_pool, json_format
from google.protobuf.message import DecodeError, Message
from google.protobuf.message_factory import GetMessageClass


class ConfigTableError(RuntimeError):
    """配置表的清单、产物或 schema 无效。"""


def _resource(name: str) -> Path:
    return Path(__file__).with_name(name)


@lru_cache(maxsize=1)
def _descriptors() -> descriptor_pool.DescriptorPool:
    return _descriptor_pool(_resource("_table_descriptors.pb").read_bytes())


def _descriptor_pool(payload: bytes) -> descriptor_pool.DescriptorPool:
    bundle = descriptor_pb2.FileDescriptorSet.FromString(payload)
    pool = descriptor_pool.DescriptorPool()
    for file in bundle.file:
        pool.AddSerializedFile(file.SerializeToString())
    return pool


def _options(pool, owner, *names: str) -> tuple:
    extensions = [pool.FindExtensionByName(f"mmorpg.cfgtable.v1.{name}") for name in names]
    options_type = GetMessageClass(extensions[0].containing_type)
    options = options_type.FromString(owner.GetOptions().SerializeToString())
    return tuple(options.Extensions[extension] for extension in extensions)


class ConfigTable:
    """一张强类型表的只读索引。查询返回独立消息，调用方可安全修改。"""

    def __init__(self, name: str, data: Message, pool: descriptor_pool.DescriptorPool) -> None:
        self.name = name
        row_desc = data.DESCRIPTOR.fields_by_name["data"].message_type
        self._row_type = GetMessageClass(row_desc)
        schema = pool.FindMessageTypeByName(f"mmorpg.cfgtable.v1.{name}Table")
        self.primary_key, = _options(pool, schema, "cfg_primary_key")
        self._data = tuple(row.SerializeToString(deterministic=True) for row in data.data)
        self._foreign: dict[str, tuple[str, bool]] = {}
        self._indexes: dict[str, dict[object, list[int]]] = {}
        self._single: set[str] = set()
        self._bits: dict[object, int] | None = None
        for field in schema.fields:
            field_name = field.name
            if field_name not in row_desc.fields_by_name:
                continue
            key, multi, index, bit, fk, gfk = _options(
                pool, field, "cfg_key", "cfg_multi", "cfg_index", "cfg_bit_index", "cfg_fk", "cfg_gfk"
            )
            if fk or gfk:
                self._foreign[field_name] = (fk or gfk, bool(gfk))
            if field_name != self.primary_key and not (key or index or gfk):
                continue
            positions: dict[object, list[int]] = defaultdict(list)
            repeated = field.is_repeated
            for position, row in enumerate(data.data):
                value = getattr(row, field_name)
                values = value if repeated else (value,)
                for item in values:
                    positions[item].append(position)
            if (key or field_name == self.primary_key) and not multi and not repeated:
                # 与 C++ 的 emplace 一致，唯一索引保留首次出现的行。
                positions = {value: items[:1] for value, items in positions.items()}
                self._single.add(field_name)
            self._indexes[field_name] = dict(positions)
            if bit:
                self._bits = {value: items[0] for value, items in positions.items()}

    def __len__(self) -> int:
        return len(self._data)

    def _decode(self, position: int) -> Message:
        return self._row_type.FromString(self._data[position])

    def rows(self) -> tuple[Message, ...]:
        return tuple(self._decode(position) for position in range(len(self)))

    def get(self, value: object, *, field: str | None = None) -> Message | None:
        field = self.primary_key if field is None else field
        if field not in self._single:
            raise KeyError(f"{self.name}.{field} 不是唯一索引")
        matches = self._indexes[field].get(value)
        return self._decode(matches[0]) if matches else None

    def find_all(self, value: object, *, field: str | None = None) -> tuple[Message, ...]:
        field = self.primary_key if field is None else field
        return tuple(self._decode(i) for i in self._indexes[field].get(value, ()))

    def bit_index(self, value: object) -> int | None:
        if self._bits is None:
            raise KeyError(f"{self.name} 没有存档位图索引")
        return self._bits.get(value)


class ConfigTables:
    """与一次导表清单绑定的配置表快照。"""

    def __init__(
        self,
        directory: str | Path | None = None,
        fmt: str = "json",
        *,
        descriptor_set: str | Path | None = None,
    ) -> None:
        self.directory = Path(directory) if directory is not None else _resource("tables")
        self.descriptor_set = Path(descriptor_set) if descriptor_set is not None else None
        self.format = fmt.strip().lower()
        if self.format not in {"json", "pb"}:
            raise ConfigTableError("配置表格式应为 json 或 pb")
        try:
            manifest = json.loads((self.directory / "manifest.json").read_bytes())
            if manifest["schema_version"] != 1:
                raise ValueError("不支持的清单版本")
            if _content_digest(manifest["tables"]) != manifest["content_digest"]:
                raise ValueError("配置表清单的内容指纹不一致")
            self._pool = (
                _descriptors() if self.descriptor_set is None
                else _descriptor_pool(self.descriptor_set.read_bytes())
            )
            tables: dict[str, ConfigTable] = {}
            for entry in manifest["tables"]:
                name = entry["name"]
                if name in tables:
                    raise ValueError(f"重复的配置表 {name}")
                tables[name] = self._load_table(name, entry)
            if len(tables) != manifest["table_count"]:
                raise ValueError("配置表数量与清单不一致")
            self.version = manifest["version"]
            self.content_digest = manifest["content_digest"]
        except (OSError, KeyError, ValueError, TypeError, DecodeError, json_format.ParseError) as exc:
            raise ConfigTableError(f"无法加载配置表 {self.directory}: {exc}") from exc
        self.tables = MappingProxyType(tables)

    def _load_table(self, name: str, entry: dict) -> ConfigTable:
        kind = "json" if self.format == "json" else "binary"
        assets = [asset for asset in entry["artifacts"] if asset["kind"] == kind]
        if len(assets) != 1:
            raise ValueError(f"{name} 缺少唯一的 {self.format} 产物")
        asset = assets[0]
        filename = Path(asset["file"])
        if filename.name != asset["file"] or filename.suffix != f".{self.format}":
            raise ValueError(f"无效的配置表文件名: {filename}")
        payload = (self.directory / filename).read_bytes()
        if len(payload) != asset["size"] or hashlib.sha256(payload).hexdigest() != asset["sha256"]:
            raise ValueError(f"配置表内容与清单不一致: {filename}")
        descriptor = self._pool.FindMessageTypeByName(f"{name}TableData")
        data = GetMessageClass(descriptor)()
        if self.format == "json":
            json_format.Parse(payload.decode("utf-8"), data)
        else:
            data.ParseFromString(payload)
        if len(data.data) != entry["rows"]:
            raise ValueError(f"配置表行数与清单不一致: {filename}")
        return ConfigTable(name, data, self._pool)

    def __getitem__(self, name: str) -> ConfigTable:
        return self.tables[name]

    def resolve_foreign(self, name: str, row: Message, field: str) -> tuple[Message, ...]:
        target, repeated = self.tables[name]._foreign[field]
        target_table, dot, target_field = target.partition(".")
        table = self.tables[target_table]
        value = getattr(row, field)
        keys = value if repeated else (value,)
        return tuple(
            item
            for key in keys
            for item in table.find_all(key, field=target_field if dot else None)
        )


class ConfigTableRegistry:
    """新批次加载和验证成功后整体切换，正在读的旧快照保持有效。"""

    def __init__(self, snapshot: ConfigTables) -> None:
        self._snapshot = snapshot
        self._reload_lock = asyncio.Lock()

    @property
    def snapshot(self) -> ConfigTables:
        return self._snapshot

    async def reload(self, directory: str | Path | None = None) -> ConfigTables:
        async with self._reload_lock:
            current = self._snapshot
            path = current.directory if directory is None else directory
            candidate = await asyncio.to_thread(
                ConfigTables, path, current.format, descriptor_set=current.descriptor_set,
            )
            if candidate.version < current.version or (
                candidate.version == current.version
                and candidate.content_digest != current.content_digest
            ):
                raise ConfigTableError("拒绝配置表版本倒退或同版本内容冲突")
            self._snapshot = candidate
            return candidate


def _content_digest(entries: list[dict]) -> str:
    """复用原导表器的内容指纹口径。"""
    digest = hashlib.sha256()
    for entry in entries:
        digest.update(entry["name"].encode("utf-8"))
        digest.update(str(entry["rows"]).encode("utf-8"))
        digest.update(entry["source"].get("sha256", "").encode("utf-8"))
        for artifact in entry["artifacts"]:
            for key in ("kind", "file", "sha256"):
                digest.update(artifact[key].encode("utf-8"))
    return digest.hexdigest()
