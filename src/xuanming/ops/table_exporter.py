"""纯 Python 零额外依赖配置表导出工具（移植自 C++/Go 仓 ``tools/data_table_exporter``）。

核心能力（完全兼容 :class:`xuanming.data.config_tables.ConfigTables` 加载口径）：
1. 标准库原生 ``.xlsx`` 读写（``zipfile`` + ``xml.etree.ElementTree``，支持 sharedStrings、
   inlineStr、数值、布尔与 ``A..ZZ`` 列坐标），同时支持 ``.csv`` / ``.json`` 源表；
2. 4 行表头解析（列名、类型、注释、选项标签）与全量数据校验：主键唯一非空、唯一键、
   跨表外键结构与取值、永久 ID 台账只增不删、单调不可复用 ``bit_index`` 映射以及
   ``Tip.xlsx`` 单轴错误码校验与 ``tip_text.json`` 导出；
3. 生成 ``*_table.proto``（wire 与 ``mmorpg.cfgtable.v1`` schema）、经 ``grpc_tools.protoc``
   编译 ``_table_descriptors.pb``、产出字节确定性的 ``<table>.json`` / ``<table>.pb``、
   ``manifest.json``（与 ``ConfigTables`` 同一内容指纹口径）及可选 ``table_constants.py``；
4. 提供 ``export``、``validate``、``pb2json`` 三个 CLI 子命令。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import keyword
import logging
import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import grpc_tools
from google.protobuf import descriptor_pb2, descriptor_pool, json_format
from google.protobuf.message_factory import GetMessageClass
from grpc_tools import protoc

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 常量定义
# ---------------------------------------------------------------------------

MANIFEST_FILENAME = "manifest.json"
MANIFEST_SCHEMA_VERSION = 2
DESCRIPTOR_SET_FILENAME = "_table_descriptors.pb"
TIP_TEXT_FILENAME = "tip_text.json"
TIP_STATE_FILENAME = "tip_enum_ids.json"
TIP_STATE_SCHEMA_VERSION = 2
CONSTANTS_FILENAME = "table_constants.py"
SCHEMA_DIGEST_HEX_LEN = 16

SUPPORTED_SCALAR_TYPES: frozenset[str] = frozenset({
    "int32", "uint32", "int64", "uint64", "float", "double", "bool", "string",
})

_INTEGER_BOUNDS: dict[str, tuple[int, int]] = {
    "int32": (-(1 << 31), (1 << 31) - 1),
    "uint32": (0, (1 << 32) - 1),
    "int64": (-(1 << 63), (1 << 63) - 1),
    "uint64": (0, (1 << 64) - 1),
}

_EMPTY_FK_SENTINELS: frozenset[Any] = frozenset({None, "", 0, -1})
_BOOL_TRUE: frozenset[str] = frozenset({"1", "true", "yes", "y", "on", "是"})
_BOOL_FALSE: frozenset[str] = frozenset({"", "0", "false", "no", "n", "off", "否"})
_FAULT_TRUE: frozenset[str] = frozenset({"1", "true", "yes", "是"})
_FAULT_FALSE: frozenset[str] = frozenset({"", "0", "false", "no", "否"})

_VALID_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CELL_REF_RE = re.compile(r"^([A-Za-z]+)([0-9]+)$")
_NON_IDENT_RE = re.compile(r"\W+")

_SPREADSHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_DOC_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_REL_PKG_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"

_SUPPORTED_SOURCE_SUFFIXES: frozenset[str] = frozenset({".xlsx", ".csv", ".json"})

_PERMANENT_ID_HEADER = (
    "# {name} 表出现过的全部行 id（升序）。由导表器维护，只增不删。\n"
    "# 表中删除已登记的行 id 将导致导表失败。\n"
)


# ---------------------------------------------------------------------------
# 异常体系
# ---------------------------------------------------------------------------

class TableExporterError(RuntimeError):
    """导表工具基础异常。"""


class SchemaError(TableExporterError):
    """表头定义、字段类型或选项配置不合法。"""


class PrimaryKeyError(TableExporterError):
    """主键或唯一键为空、缺失或出现重复。"""


class ForeignKeyError(TableExporterError):
    """跨表外键结构无效或数据行存在悬空引用。"""


class PermanentIdsError(TableExporterError):
    """永久 ID 台账损坏或已发布的永久行 ID 被删除。"""


class BitIndexStateError(TableExporterError):
    """位图索引映射状态损坏或缺失（fail-closed）。"""


class TipAxisError(TableExporterError):
    """Tip 单轴错误码校验失败（重复 ID/名称、空文案、非法段或历史号篡改）。"""


class ProtoCompileError(TableExporterError):
    """使用 grpc_tools.protoc 编译描述符集失败。"""


# ---------------------------------------------------------------------------
# 原生标准库 .xlsx 读写与列坐标工具
# ---------------------------------------------------------------------------

def col_letters_to_index(letters: str) -> int:
    """将 Excel 列标（如 ``A``、``Z``、``AA``、``ZZ``）转为 0-based 列号。"""
    cleaned = letters.strip().upper()
    if not cleaned or not cleaned.isalpha():
        raise SchemaError(f"invalid Excel column reference: {letters!r}")
    index = 0
    for ch in cleaned:
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return index - 1


def index_to_col_letters(index: int) -> str:
    """将 0-based 列号转为 Excel 列标（0 -> ``A``，26 -> ``AA``）。"""
    if index < 0:
        raise SchemaError(f"column index must be non-negative: {index}")
    chars: list[str] = []
    n = index + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        chars.append(chr(ord("A") + rem))
    return "".join(reversed(chars))


def _local_tag(tag: str) -> str:
    """剥离 XML 命名空间前缀，仅返回本地标签名。"""
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _parse_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    """解析 ``xl/sharedStrings.xml``，兼容普通 ``<t>`` 与富文本 ``<r><t>``。"""
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for si in root:
        if _local_tag(si.tag) != "si":
            continue
        parts: list[str] = []
        for elem in si.iter():
            if _local_tag(elem.tag) == "t" and elem.text is not None:
                parts.append(elem.text)
        strings.append("".join(parts))
    return strings


def _resolve_sheet_targets(zf: zipfile.ZipFile) -> list[tuple[str, str]]:
    """从 workbook.xml 与 rels 解析工作表名称到归档内部路径的有序映射。"""
    names = set(zf.namelist())
    if "xl/workbook.xml" not in names:
        fallback = sorted(p for p in names if p.startswith("xl/worksheets/sheet") and p.endswith(".xml"))
        return [(Path(p).stem, p) for p in fallback]

    rels: dict[str, str] = {}
    if "xl/_rels/workbook.xml.rels" in names:
        rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        for rel in rel_root:
            if _local_tag(rel.tag) != "Relationship":
                continue
            rid = rel.attrib.get("Id", "")
            target = rel.attrib.get("Target", "").lstrip("/")
            if rid and target:
                if not target.startswith("xl/"):
                    target = f"xl/{target}"
                rels[rid] = target

    wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
    sheets: list[tuple[str, str]] = []
    for elem in wb_root.iter():
        if _local_tag(elem.tag) != "sheet":
            continue
        sname = elem.attrib.get("name", "")
        rid = elem.attrib.get(f"{{{_REL_DOC_NS}}}id") or elem.attrib.get("r:id", "")
        target = rels.get(rid, "")
        if not target and sname:
            idx = len(sheets) + 1
            candidate = f"xl/worksheets/sheet{idx}.xml"
            if candidate in names:
                target = candidate
        if sname and target in names:
            sheets.append((sname, target))
    return sheets


def _parse_numeric_text(raw: str) -> int | float:
    """解析 Excel 数值单元格文本，整数形态返回 ``int``，否则返回 ``float``。"""
    text = raw.strip()
    try:
        return int(text)
    except ValueError:
        val = float(text)
        if val.is_integer() and "e" not in text.lower() and "." not in text:
            return int(val)
        return val


def _extract_cell_value(cell: ET.Element, shared_strings: list[str]) -> Any:
    """按 OOXML 单元格类型提取 Python 值。"""
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        parts: list[str] = []
        for elem in cell.iter():
            if _local_tag(elem.tag) == "t" and elem.text is not None:
                parts.append(elem.text)
        return "".join(parts)

    value_elem: ET.Element | None = None
    for child in cell:
        if _local_tag(child.tag) == "v":
            value_elem = child
            break
    if value_elem is None or value_elem.text is None:
        return None
    raw = value_elem.text

    if cell_type == "s":
        idx = int(raw)
        if idx < 0 or idx >= len(shared_strings):
            raise SchemaError(f"sharedStrings index out of range: {idx}")
        return shared_strings[idx]
    if cell_type in ("str", "e"):
        return raw
    if cell_type == "b":
        return raw.strip() == "1"
    return _parse_numeric_text(raw)


def list_xlsx_sheets(path: str | Path) -> list[str]:
    """返回 ``.xlsx`` 工作簿中的所有工作表名称。"""
    file_path = Path(path)
    try:
        with zipfile.ZipFile(file_path, "r") as zf:
            return [name for name, _ in _resolve_sheet_targets(zf)]
    except (OSError, zipfile.BadZipFile, ET.ParseError) as exc:
        raise SchemaError(f"failed to read xlsx workbook {file_path}: {exc}") from exc


def read_xlsx_sheet(path: str | Path, sheet_name: str | None = None) -> list[list[Any]]:
    """使用标准库 ``zipfile`` + ``xml.etree.ElementTree`` 读取 ``.xlsx`` 指定工作表。

    支持 ``sharedStrings``、``inlineStr``、布尔、数值以及稀疏行/列坐标（``A``..``ZZ`` 等）。
    """
    file_path = Path(path)
    try:
        with zipfile.ZipFile(file_path, "r") as zf:
            sheets = _resolve_sheet_targets(zf)
            if not sheets:
                raise SchemaError(f"workbook has no readable worksheets: {file_path}")
            target_path: str | None = None
            if sheet_name is None:
                target_path = sheets[0][1]
            else:
                for name, target in sheets:
                    if name == sheet_name:
                        target_path = target
                        break
                if target_path is None:
                    available = ", ".join(name for name, _ in sheets)
                    raise SchemaError(
                        f"worksheet {sheet_name!r} not found in {file_path} (available: {available})"
                    )
            shared_strings = _parse_shared_strings(zf)
            sheet_root = ET.fromstring(zf.read(target_path))
    except SchemaError:
        raise
    except (OSError, zipfile.BadZipFile, KeyError, ET.ParseError) as exc:
        raise SchemaError(f"failed to parse xlsx {file_path}: {exc}") from exc

    rows_by_num: dict[int, dict[int, Any]] = {}
    max_row = 0
    max_col = 0
    fallback_row_num = 0

    for sheet_data in sheet_root.iter():
        if _local_tag(sheet_data.tag) != "sheetData":
            continue
        for row_elem in sheet_data:
            if _local_tag(row_elem.tag) != "row":
                continue
            r_attr = row_elem.attrib.get("r")
            row_num = int(r_attr) if (r_attr and r_attr.isdigit()) else fallback_row_num + 1
            fallback_row_num = row_num
            row_cells = rows_by_num.setdefault(row_num, {})
            fallback_col = 0
            for cell in row_elem:
                if _local_tag(cell.tag) != "c":
                    continue
                ref = cell.attrib.get("r", "")
                match = _CELL_REF_RE.match(ref)
                if match:
                    col_idx = col_letters_to_index(match.group(1))
                else:
                    col_idx = fallback_col
                fallback_col = col_idx + 1
                val = _extract_cell_value(cell, shared_strings)
                if val is not None:
                    row_cells[col_idx] = val
                    max_row = max(max_row, row_num)
                    max_col = max(max_col, col_idx + 1)

    if max_row == 0:
        return []

    matrix: list[list[Any]] = []
    for row_num in range(1, max_row + 1):
        row_cells = rows_by_num.get(row_num, {})
        matrix.append([row_cells.get(col_idx) for col_idx in range(max_col)])
    return matrix


def _xml_bytes(root: ET.Element) -> bytes:
    """将 ElementTree 节点序列化为带 UTF-8 声明的字节串。"""
    return b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + ET.tostring(
        root, encoding="utf-8"
    )


def write_xlsx_sheet(
    path: str | Path,
    rows: Sequence[Sequence[Any]],
    sheet_name: str = "Sheet1",
) -> Path:
    """使用标准库 ``zipfile`` + ``xml.etree.ElementTree`` 写出最小合法 ``.xlsx`` 文件。"""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    shared_list: list[str] = []
    shared_index: dict[str, int] = {}

    def intern_string(text: str) -> int:
        idx = shared_index.get(text)
        if idx is None:
            idx = len(shared_list)
            shared_list.append(text)
            shared_index[text] = idx
        return idx

    ws_root = ET.Element("worksheet", {"xmlns": _SPREADSHEET_NS})
    sheet_data = ET.SubElement(ws_root, "sheetData")
    for row_idx, row in enumerate(rows, start=1):
        row_elem = ET.SubElement(sheet_data, "row", {"r": str(row_idx)})
        for col_idx, val in enumerate(row):
            if val is None:
                continue
            ref = f"{index_to_col_letters(col_idx)}{row_idx}"
            if isinstance(val, bool):
                c_elem = ET.SubElement(row_elem, "c", {"r": ref, "t": "b"})
                v_elem = ET.SubElement(c_elem, "v")
                v_elem.text = "1" if val else "0"
            elif isinstance(val, (int, float)):
                c_elem = ET.SubElement(row_elem, "c", {"r": ref})
                v_elem = ET.SubElement(c_elem, "v")
                v_elem.text = str(val)
            else:
                c_elem = ET.SubElement(row_elem, "c", {"r": ref, "t": "s"})
                v_elem = ET.SubElement(c_elem, "v")
                v_elem.text = str(intern_string(str(val)))

    sst_root = ET.Element(
        "sst",
        {
            "xmlns": _SPREADSHEET_NS,
            "count": str(len(shared_list)),
            "uniqueCount": str(len(shared_list)),
        },
    )
    for text in shared_list:
        si = ET.SubElement(sst_root, "si")
        t_elem = ET.SubElement(si, "t")
        if text.startswith(" ") or text.endswith(" ") or "\n" in text:
            t_elem.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        t_elem.text = text

    wb_root = ET.Element(
        "workbook",
        {"xmlns": _SPREADSHEET_NS, "xmlns:r": _REL_DOC_NS},
    )
    sheets_elem = ET.SubElement(wb_root, "sheets")
    ET.SubElement(
        sheets_elem,
        "sheet",
        {"name": sheet_name, "sheetId": "1", f"{{{_REL_DOC_NS}}}id": "rId1"},
    )

    wb_rels = ET.Element("Relationships", {"xmlns": _REL_PKG_NS})
    ET.SubElement(
        wb_rels,
        "Relationship",
        {
            "Id": "rId1",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet",
            "Target": "worksheets/sheet1.xml",
        },
    )
    ET.SubElement(
        wb_rels,
        "Relationship",
        {
            "Id": "rId2",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings",
            "Target": "sharedStrings.xml",
        },
    )

    root_rels = ET.Element("Relationships", {"xmlns": _REL_PKG_NS})
    ET.SubElement(
        root_rels,
        "Relationship",
        {
            "Id": "rId1",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
            "Target": "xl/workbook.xml",
        },
    )

    ct_root = ET.Element("Types", {"xmlns": _CT_NS})
    ET.SubElement(
        ct_root,
        "Default",
        {"Extension": "rels", "ContentType": "application/vnd.openxmlformats-package.relationships+xml"},
    )
    ET.SubElement(ct_root, "Default", {"Extension": "xml", "ContentType": "application/xml"})
    ET.SubElement(
        ct_root,
        "Override",
        {
            "PartName": "/xl/workbook.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml",
        },
    )
    ET.SubElement(
        ct_root,
        "Override",
        {
            "PartName": "/xl/worksheets/sheet1.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
        },
    )
    ET.SubElement(
        ct_root,
        "Override",
        {
            "PartName": "/xl/sharedStrings.xml",
            "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml",
        },
    )

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", _xml_bytes(ct_root))
        zf.writestr("_rels/.rels", _xml_bytes(root_rels))
        zf.writestr("xl/workbook.xml", _xml_bytes(wb_root))
        zf.writestr("xl/_rels/workbook.xml.rels", _xml_bytes(wb_rels))
        zf.writestr("xl/sharedStrings.xml", _xml_bytes(sst_root))
        zf.writestr("xl/worksheets/sheet1.xml", _xml_bytes(ws_root))
    return out_path


def read_source_matrix(path: str | Path, sheet_name: str | None = None) -> tuple[str, list[list[Any]]]:
    """统一读取 ``.xlsx`` / ``.csv`` / ``.json`` 源表为二维矩阵与表名。"""
    file_path = Path(path)
    suffix = file_path.suffix.lower()
    table_name = file_path.stem
    if suffix == ".xlsx":
        return table_name, read_xlsx_sheet(file_path, sheet_name=sheet_name)
    if suffix == ".csv":
        try:
            with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
                rows: list[list[Any]] = [
                    [cell if cell != "" else None for cell in raw_row]
                    for raw_row in csv.reader(f)
                ]
        except OSError as exc:
            raise SchemaError(f"failed to read csv {file_path}: {exc}") from exc
        return table_name, rows
    if suffix == ".json":
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SchemaError(f"failed to read json source {file_path}: {exc}") from exc
        if isinstance(payload, list):
            return table_name, [list(r) for r in payload]
        if isinstance(payload, dict) and "columns" in payload:
            name = str(payload.get("name") or table_name)
            cols = payload["columns"]
            if not isinstance(cols, list) or not cols:
                raise SchemaError(f"json source {file_path} has empty columns")
            names_row = [c["name"] for c in cols]
            types_row = [c["type"] for c in cols]
            comments_row = [c.get("comment", "") for c in cols]
            options_row = [
                " ".join(c["options"]) if isinstance(c.get("options"), list) else str(c.get("options", ""))
                for c in cols
            ]
            matrix: list[list[Any]] = [names_row, types_row, comments_row, options_row]
            for item in payload.get("rows", []):
                if isinstance(item, dict):
                    matrix.append([item.get(col_name) for col_name in names_row])
                elif isinstance(item, list):
                    matrix.append(list(item))
                else:
                    raise SchemaError(f"invalid row entry in json source {file_path}")
            return name, matrix
        raise SchemaError(f"unsupported json source structure in {file_path}")
    raise SchemaError(f"unsupported table source format: {file_path}")


# ---------------------------------------------------------------------------
# Schema 模型与 4 行表头解析
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ForeignKeyRef:
    """列级外键引用（解析自 ``fk:Table`` 或 ``fk:Table.field``）。"""

    target_table: str
    target_column: str = "id"

    @classmethod
    def parse(cls, token: str) -> ForeignKeyRef | None:
        raw = token.strip()
        if not raw.lower().startswith("fk:"):
            return None
        body = raw[3:].strip()
        if not body:
            raise SchemaError(f"empty foreign key target in option: {token!r}")
        if "." in body:
            target_table, target_column = body.split(".", 1)
            target_table = target_table.strip()
            target_column = target_column.strip()
            if not target_table or not target_column:
                raise SchemaError(f"malformed foreign key option: {token!r}")
            return cls(target_table=target_table, target_column=target_column)
        return cls(target_table=body, target_column="id")


@dataclass(frozen=True)
class ColumnDef:
    """单个逻辑列的元信息定义。"""

    name: str
    data_type: str
    repeated: bool = False
    comment: str = ""
    options: tuple[str, ...] = ()
    owner: str = "common"
    excel_indices: tuple[int, ...] = (0,)
    is_primary_key: bool = False
    is_unique: bool = False
    is_multi: bool = False
    is_indexed: bool = False
    has_bit_index: bool = False
    is_tip_ref: bool = False
    foreign_key: ForeignKeyRef | None = None
    group_foreign_key: str | None = None
    is_constants_name: bool = False

    @property
    def is_server(self) -> bool:
        return self.owner in ("server", "common")


@dataclass(frozen=True)
class TableSchema:
    """单张配置表的完整 schema 描述。"""

    name: str
    source_path: Path | None
    columns: tuple[ColumnDef, ...]
    primary_key: str
    multi_primary_key: bool = False
    ids_permanent: bool = False
    hot_reload: bool = False
    field_numbers: dict[str, int] = field(default_factory=dict)

    @property
    def server_columns(self) -> tuple[ColumnDef, ...]:
        return tuple(c for c in self.columns if c.is_server)

    @property
    def bit_index_columns(self) -> tuple[ColumnDef, ...]:
        return tuple(c for c in self.server_columns if c.has_bit_index)

    @property
    def foreign_key_columns(self) -> tuple[ColumnDef, ...]:
        return tuple(c for c in self.server_columns if c.foreign_key is not None or c.group_foreign_key is not None)

    def column_by_name(self, name: str) -> ColumnDef | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None


@dataclass(frozen=True)
class ParsedTable:
    """解析后的单张配置表（schema + 强类型数据行 + 缺失主键追踪）。"""

    schema: TableSchema
    rows: tuple[dict[str, Any], ...]
    missing_pk_row_numbers: tuple[int, ...] = ()
    constants_names: dict[Any, str] = field(default_factory=dict)


def _parse_type_spec(raw_type: Any, table_name: str, col_name: str) -> tuple[str, bool]:
    """解析列类型声明，支持 ``int32``、``repeated int32``、``int32[]`` 等写法。"""
    if raw_type is None or not str(raw_type).strip():
        raise SchemaError(f"[{table_name}.{col_name}] missing column type declaration")
    text = str(raw_type).strip()
    lower = text.lower()
    repeated = False
    if lower.startswith("repeated "):
        repeated = True
        base = lower[len("repeated "):].strip()
    elif lower.endswith("[]"):
        repeated = True
        base = lower[:-2].strip()
    else:
        base = lower
    if base not in SUPPORTED_SCALAR_TYPES:
        raise SchemaError(f"[{table_name}.{col_name}] unsupported column type: {text!r}")
    return base, repeated


def _split_option_tokens(raw_options: Any) -> list[str]:
    """把第 4 行选项单元格拆分成标准化 token 列表。"""
    if raw_options is None:
        return []
    text = str(raw_options).strip()
    if not text:
        return []
    return [tok for tok in re.split(r"[\s,;]+", text) if tok]


def _coerce_scalar(raw: Any, data_type: str, context: str) -> Any:
    """将单个单元格值转换为指定 proto 标量对应的 Python 值并校验范围。"""
    if data_type == "string":
        if raw is None:
            return ""
        return str(raw)
    if data_type == "bool":
        if raw is None:
            return False
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            if raw in (0, 0.0):
                return False
            if raw in (1, 1.0):
                return True
            raise SchemaError(f"{context}: invalid bool value {raw!r}")
        s = str(raw).strip().lower()
        if s in _BOOL_TRUE:
            return True
        if s in _BOOL_FALSE:
            return False
        raise SchemaError(f"{context}: invalid bool value {raw!r}")
    if data_type in ("float", "double"):
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return 0.0
        if isinstance(raw, bool):
            raise SchemaError(f"{context}: expected {data_type}, got bool {raw!r}")
        try:
            return float(raw)
        except (TypeError, ValueError) as exc:
            raise SchemaError(f"{context}: invalid {data_type} value {raw!r}") from exc
    if data_type in _INTEGER_BOUNDS:
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return 0
        if isinstance(raw, bool):
            raise SchemaError(f"{context}: expected {data_type}, got bool {raw!r}")
        if isinstance(raw, float):
            if not raw.is_integer():
                raise SchemaError(f"{context}: expected integer {data_type}, got float {raw!r}")
            val = int(raw)
        elif isinstance(raw, int):
            val = raw
        else:
            text = str(raw).strip()
            try:
                val = int(text)
            except ValueError as exc:
                try:
                    fval = float(text)
                    if not fval.is_integer():
                        raise SchemaError(f"{context}: invalid {data_type} value {raw!r}") from exc
                    val = int(fval)
                except ValueError:
                    raise SchemaError(f"{context}: invalid {data_type} value {raw!r}") from exc
        lo, hi = _INTEGER_BOUNDS[data_type]
        if not (lo <= val <= hi):
            raise SchemaError(f"{context}: {data_type} value {val} out of range [{lo}, {hi}]")
        return val
    raise SchemaError(f"{context}: unsupported scalar type {data_type!r}")


def _parse_repeated_cell(raw: Any, data_type: str, context: str) -> list[Any]:
    """解析单格 repeated 字段，支持 JSON 列表、逗号/分号/竖线分隔或单值。"""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [_coerce_scalar(item, data_type, context) for item in raw if item is not None and item != ""]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SchemaError(f"{context}: invalid JSON array {raw!r}: {exc}") from exc
            if not isinstance(parsed, list):
                raise SchemaError(f"{context}: expected JSON array, got {raw!r}")
            return [_coerce_scalar(item, data_type, context) for item in parsed if item is not None and item != ""]
        tokens = [tok.strip() for tok in re.split(r"[,;|]+", text) if tok.strip()]
        return [_coerce_scalar(tok, data_type, context) for tok in tokens]
    return [_coerce_scalar(raw, data_type, context)]


def _is_empty_cell(val: Any) -> bool:
    return val is None or (isinstance(val, str) and not val.strip())


def parse_table_matrix(
    table_name: str,
    matrix: Sequence[Sequence[Any]],
    source_path: Path | None = None,
) -> ParsedTable:
    """按 4 行表头（``name`` / ``type`` / ``comment`` / ``options``）解析配置表矩阵。"""
    if not table_name or not _VALID_IDENT_RE.match(table_name):
        raise SchemaError(f"invalid table name: {table_name!r}")
    if len(matrix) < 4:
        raise SchemaError(
            f"[{table_name}] expected at least 4 header rows (name, type, comment, options), got {len(matrix)}"
        )

    names_row = matrix[0]
    types_row = matrix[1]
    comments_row = matrix[2]
    options_row = matrix[3]
    col_count = max(len(names_row), len(types_row), len(comments_row), len(options_row))

    def cell_at(row: Sequence[Any], idx: int) -> Any:
        return row[idx] if idx < len(row) else None

    columns: list[ColumnDef] = []
    columns_by_name: dict[str, int] = {}
    ids_permanent = False
    hot_reload = False
    explicit_pks: list[str] = []

    for col_idx in range(col_count):
        raw_name = cell_at(names_row, col_idx)
        raw_type = cell_at(types_row, col_idx)
        raw_comment = cell_at(comments_row, col_idx)
        raw_options = cell_at(options_row, col_idx)
        if _is_empty_cell(raw_name) and _is_empty_cell(raw_type):
            continue
        if _is_empty_cell(raw_name):
            raise SchemaError(f"[{table_name}] column {index_to_col_letters(col_idx)} has type but no name")

        col_name = str(raw_name).strip()
        if not _VALID_IDENT_RE.match(col_name):
            raise SchemaError(f"[{table_name}] invalid column name: {col_name!r}")

        data_type, repeated = _parse_type_spec(raw_type, table_name, col_name)
        comment = "" if raw_comment is None else str(raw_comment).strip()
        tokens = _split_option_tokens(raw_options)

        # 同名 repeated 列可跨多列展开（兼容 C++ 导表器多槽位写法）。
        if col_name in columns_by_name:
            prev_pos = columns_by_name[col_name]
            prev = columns[prev_pos]
            if not (prev.repeated and repeated and prev.data_type == data_type):
                raise SchemaError(f"[{table_name}] duplicate column name: {col_name!r}")
            columns[prev_pos] = ColumnDef(
                name=prev.name,
                data_type=prev.data_type,
                repeated=True,
                comment=prev.comment,
                options=prev.options,
                owner=prev.owner,
                excel_indices=prev.excel_indices + (col_idx,),
                is_primary_key=prev.is_primary_key,
                is_unique=prev.is_unique,
                is_multi=prev.is_multi,
                is_indexed=prev.is_indexed,
                has_bit_index=prev.has_bit_index,
                is_tip_ref=prev.is_tip_ref,
                foreign_key=prev.foreign_key,
                group_foreign_key=prev.group_foreign_key,
                is_constants_name=prev.is_constants_name,
            )
            continue

        owner = "common"
        is_pk = False
        is_unique = False
        is_multi = False
        is_indexed = False
        has_bit_index = False
        is_tip_ref = False
        fk_ref: ForeignKeyRef | None = None
        gfk_target: str | None = None
        is_constants_name = False

        for tok in tokens:
            low = tok.lower()
            if low in ("pk", "primary_key"):
                is_pk = True
                explicit_pks.append(col_name)
            elif low in ("unique", "key"):
                is_unique = True
            elif low == "multi":
                is_multi = True
            elif low in ("idx", "index"):
                is_indexed = True
            elif low == "bit_index":
                has_bit_index = True
            elif low == "tip_ref":
                is_tip_ref = True
            elif low == "ids_permanent":
                ids_permanent = True
            elif low == "hot_reload":
                hot_reload = True
            elif low == "constants_name":
                is_constants_name = True
                owner = "design"
            elif low in ("server", "common", "client", "design"):
                owner = low
            elif low.startswith("owner:"):
                val = low.split(":", 1)[1].strip()
                if val not in ("server", "common", "client", "design"):
                    raise SchemaError(f"[{table_name}.{col_name}] unknown owner option: {tok!r}")
                owner = val
            elif low.startswith("fk:"):
                fk_ref = ForeignKeyRef.parse(tok)
            elif low.startswith("gfk:"):
                target = tok[4:].strip()
                if not target:
                    raise SchemaError(f"[{table_name}.{col_name}] empty gfk target: {tok!r}")
                gfk_target = target
            else:
                raise SchemaError(f"[{table_name}.{col_name}] unknown option token: {tok!r}")

        if fk_ref is not None and not repeated:
            is_indexed = True

        col_def = ColumnDef(
            name=col_name,
            data_type=data_type,
            repeated=repeated,
            comment=comment,
            options=tuple(tokens),
            owner=owner,
            excel_indices=(col_idx,),
            is_primary_key=is_pk,
            is_unique=is_unique,
            is_multi=is_multi,
            is_indexed=is_indexed,
            has_bit_index=has_bit_index,
            is_tip_ref=is_tip_ref,
            foreign_key=fk_ref,
            group_foreign_key=gfk_target,
            is_constants_name=is_constants_name,
        )
        columns_by_name[col_name] = len(columns)
        columns.append(col_def)

    server_cols = [c for c in columns if c.is_server]
    if not server_cols:
        raise SchemaError(f"[{table_name}] table has no server/common columns")

    if len(explicit_pks) > 1:
        raise SchemaError(f"[{table_name}] multiple primary key columns declared: {explicit_pks}")
    if explicit_pks:
        pk_name = explicit_pks[0]
    else:
        pk_name = "id" if any(c.name == "id" and c.is_server for c in columns) else server_cols[0].name

    pk_col = next((c for c in server_cols if c.name == pk_name), None)
    if pk_col is None:
        raise SchemaError(f"[{table_name}] primary key column {pk_name!r} must have server/common owner")
    if pk_col.repeated:
        raise SchemaError(f"[{table_name}] primary key column {pk_name!r} cannot be repeated")
    if pk_col.is_multi and any(c.has_bit_index for c in server_cols):
        raise SchemaError(f"[{table_name}] multi primary key table cannot declare bit_index")
    for col in server_cols:
        if col.has_bit_index and col.data_type not in _INTEGER_BOUNDS:
            raise SchemaError(f"[{table_name}.{col.name}] bit_index column must be an integer type")

    # 显式把主键列的 is_primary_key 标记齐。
    normalized_cols: list[ColumnDef] = []
    for col in columns:
        if col.name == pk_name and not col.is_primary_key:
            normalized_cols.append(
                ColumnDef(
                    name=col.name,
                    data_type=col.data_type,
                    repeated=col.repeated,
                    comment=col.comment,
                    options=col.options,
                    owner=col.owner,
                    excel_indices=col.excel_indices,
                    is_primary_key=True,
                    is_unique=col.is_unique,
                    is_multi=col.is_multi,
                    is_indexed=col.is_indexed,
                    has_bit_index=col.has_bit_index,
                    is_tip_ref=col.is_tip_ref,
                    foreign_key=col.foreign_key,
                    group_foreign_key=col.group_foreign_key,
                    is_constants_name=col.is_constants_name,
                )
            )
        else:
            normalized_cols.append(col)

    field_numbers = {
        col.name: idx
        for idx, col in enumerate((c for c in normalized_cols if c.is_server), start=1)
    }

    schema = TableSchema(
        name=table_name,
        source_path=source_path,
        columns=tuple(normalized_cols),
        primary_key=pk_name,
        multi_primary_key=pk_col.is_multi,
        ids_permanent=ids_permanent,
        hot_reload=hot_reload,
        field_numbers=field_numbers,
    )

    # 解析数据行（第 5 行起，0-based index 4）。
    const_col = next((c for c in normalized_cols if c.is_constants_name), None)
    parsed_rows: list[dict[str, Any]] = []
    missing_pk_rows: list[int] = []
    constants_names: dict[Any, str] = {}

    for row_offset, raw_row in enumerate(matrix[4:], start=5):
        if all(_is_empty_cell(val) for val in raw_row):
            continue
        row_obj: dict[str, Any] = {}
        for col in schema.server_columns:
            ctx = f"[{table_name}!row {row_offset}.{col.name}]"
            if col.repeated:
                if len(col.excel_indices) == 1:
                    raw_val = cell_at(raw_row, col.excel_indices[0])
                    row_obj[col.name] = _parse_repeated_cell(raw_val, col.data_type, ctx)
                else:
                    items: list[Any] = []
                    for idx in col.excel_indices:
                        raw_val = cell_at(raw_row, idx)
                        if _is_empty_cell(raw_val):
                            continue
                        items.append(_coerce_scalar(raw_val, col.data_type, ctx))
                    row_obj[col.name] = items
            else:
                raw_val = cell_at(raw_row, col.excel_indices[0])
                if col.is_primary_key and _is_empty_cell(raw_val):
                    missing_pk_rows.append(row_offset)
                row_obj[col.name] = _coerce_scalar(raw_val, col.data_type, ctx)

        if const_col is not None:
            raw_const = cell_at(raw_row, const_col.excel_indices[0])
            if not _is_empty_cell(raw_const):
                constants_names[row_obj[pk_name]] = str(raw_const).strip()
        parsed_rows.append(row_obj)

    return ParsedTable(
        schema=schema,
        rows=tuple(parsed_rows),
        missing_pk_row_numbers=tuple(missing_pk_rows),
        constants_names=constants_names,
    )


def compute_schema_digest(schemas: Sequence[TableSchema]) -> str:
    """计算整批配置表的服务端 proto 布局摘要（16 位小写十六进制）。"""
    canonical: list[dict[str, Any]] = []
    for schema in sorted(schemas, key=lambda s: s.name):
        fields: list[dict[str, Any]] = []
        for col in schema.server_columns:
            fields.append({
                "number": schema.field_numbers[col.name],
                "name": col.name,
                "kind": "repeated" if col.repeated else "scalar",
                "type": col.data_type,
            })
        fields.sort(key=lambda f: (f["number"], f["name"]))
        canonical.append({
            "name": schema.name,
            "file": schema.name.lower(),
            "fields": fields,
        })
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:SCHEMA_DIGEST_HEX_LEN]


# ---------------------------------------------------------------------------
# 校验器：主键 / 唯一键 / 外键 / 永久 ID 台账 / 位图索引 / Tip 单轴
# ---------------------------------------------------------------------------

def validate_primary_keys(tables: Sequence[ParsedTable]) -> None:
    """校验所有表的主键非空唯一性及显式 ``unique`` 列的唯一性。"""
    errors: list[str] = []
    for table in tables:
        schema = table.schema
        pk = schema.primary_key
        if table.missing_pk_row_numbers:
            rows_str = ", ".join(str(r) for r in table.missing_pk_row_numbers[:10])
            errors.append(f"[{schema.name}] empty primary key {pk!r} at row(s): {rows_str}")

        pk_values = [row[pk] for row in table.rows]
        if schema.column_by_name(pk).data_type == "string":  # type: ignore[union-attr]
            empty_str_rows = [idx for idx, val in enumerate(pk_values, start=5) if not str(val).strip()]
            if empty_str_rows and not table.missing_pk_row_numbers:
                errors.append(f"[{schema.name}] blank string primary key {pk!r} at row(s): {empty_str_rows[:10]}")

        if not schema.multi_primary_key:
            dupes = {val: cnt for val, cnt in Counter(pk_values).items() if cnt > 1}
            if dupes:
                sample = ", ".join(f"{k}x{v}" for k, v in sorted(dupes.items(), key=lambda kv: str(kv[0]))[:5])
                errors.append(
                    f"[{schema.name}] duplicate primary key {pk!r}: {len(dupes)} value(s) repeated ({sample})"
                )

        # 二级 unique/key 列（非 multi、非 repeated）同样做唯一校验。
        for col in schema.server_columns:
            if col.is_primary_key or not col.is_unique or col.is_multi or col.repeated:
                continue
            non_empty = [row[col.name] for row in table.rows if row[col.name] not in ("", None)]
            dupes = {val: cnt for val, cnt in Counter(non_empty).items() if cnt > 1}
            if dupes:
                sample = ", ".join(f"{k}x{v}" for k, v in sorted(dupes.items(), key=lambda kv: str(kv[0]))[:5])
                errors.append(
                    f"[{schema.name}.{col.name}] duplicate unique key: {len(dupes)} value(s) repeated ({sample})"
                )

    if errors:
        raise PrimaryKeyError("primary/unique key validation failed:\n  " + "\n  ".join(errors))


def validate_foreign_keys(tables: Sequence[ParsedTable]) -> None:
    """校验跨表 FK / GFK 引用（结构一致性 + 数据层存在性）。"""
    table_map: dict[str, ParsedTable] = {t.schema.name: t for t in tables}
    errors: list[str] = []

    for table in tables:
        schema = table.schema
        for col in schema.foreign_key_columns:
            if col.foreign_key is not None:
                kind = "FK"
                target_table_name = col.foreign_key.target_table
                target_col_name = col.foreign_key.target_column
            else:
                kind = "GFK"
                assert col.group_foreign_key is not None
                target_table_name = col.group_foreign_key
                target_col_name = "id"

            label = f"[{schema.name}.{col.name}] {kind} -> {target_table_name}.{target_col_name}"
            target_table = table_map.get(target_table_name)
            if target_table is None:
                errors.append(f"{label}: target table {target_table_name!r} does not exist")
                continue

            target_col = target_table.schema.column_by_name(target_col_name)
            if target_col is None or not target_col.is_server:
                errors.append(f"{label}: target column {target_col_name!r} does not exist")
                continue

            if col.data_type != target_col.data_type:
                errors.append(
                    f"{label}: type mismatch ({col.data_type} vs {target_table_name}.{target_col_name}:{target_col.data_type})"
                )
                continue

            if target_col_name == target_table.schema.primary_key and target_table.schema.multi_primary_key:
                errors.append(f"{label}: cannot reference multi primary key table {target_table_name!r}")
                continue

            target_keys: set[Any] = set()
            for trow in target_table.rows:
                tval = trow[target_col_name]
                if target_col.repeated:
                    target_keys.update(tval)
                else:
                    target_keys.add(tval)

            missing: set[Any] = set()
            for row in table.rows:
                raw_val = row[col.name]
                candidates = raw_val if col.repeated else (raw_val,)
                for item in candidates:
                    if item in _EMPTY_FK_SENTINELS:
                        continue
                    if item not in target_keys:
                        missing.add(item)

            if missing:
                shown = sorted(missing, key=str)[:10]
                suffix = f" (+{len(missing) - len(shown)} more)" if len(missing) > len(shown) else ""
                errors.append(
                    f"{label}: {len(missing)} dangling reference(s): {', '.join(str(v) for v in shown)}{suffix}"
                )

    if errors:
        raise ForeignKeyError("foreign key validation failed:\n  " + "\n  ".join(errors))


def _permanent_ids_path(mapping_dir: Path, table_name: str) -> Path:
    return mapping_dir / "permanent_ids" / f"{table_name.lower()}_ids.txt"


def _load_permanent_id_ledger(path: Path) -> set[int] | None:
    if not path.exists():
        return None
    ids: set[int] = set()
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            value = int(line)
        except ValueError as exc:
            raise PermanentIdsError(f"{path}:{line_no}: non-integer row id {line!r}") from exc
        if value in ids:
            raise PermanentIdsError(f"{path}:{line_no}: duplicate row id {value}")
        ids.add(value)
    return ids


def validate_permanent_ids(tables: Sequence[ParsedTable], mapping_dir: Path) -> None:
    """检查声明了 ``ids_permanent`` 的表是否存在已发布行 ID 被删除的情况。"""
    errors: list[str] = []
    for table in tables:
        if not table.schema.ids_permanent:
            continue
        path = _permanent_ids_path(mapping_dir, table.schema.name)
        published = _load_permanent_id_ledger(path)
        if published is None:
            continue
        pk = table.schema.primary_key
        current = {int(row[pk]) for row in table.rows}
        removed = sorted(published - current)
        if removed:
            sample = ", ".join(str(v) for v in removed[:10])
            errors.append(
                f"[{table.schema.name}] {len(removed)} permanent row id(s) removed: {sample}"
            )
    if errors:
        raise PermanentIdsError("permanent id check failed:\n  " + "\n  ".join(errors))


def record_permanent_ids(tables: Sequence[ParsedTable], mapping_dir: Path) -> None:
    """将 ``ids_permanent`` 表的新增行 ID 持久化到只增不删台账（LF 换行）。"""
    for table in tables:
        if not table.schema.ids_permanent:
            continue
        path = _permanent_ids_path(mapping_dir, table.schema.name)
        published = _load_permanent_id_ledger(path)
        pk = table.schema.primary_key
        current = {int(row[pk]) for row in table.rows}
        if published is None:
            logger.warning("[%s] Permanent id ledger missing; initializing %s", table.schema.name, path)
            published = set()
        elif current <= published:
            continue
        merged = sorted(published | current)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(_PERMANENT_ID_HEADER.format(name=table.schema.name))
            for value in merged:
                f.write(f"{value}\n")
        logger.info("[%s] Updated permanent id ledger (%d total ids)", table.schema.name, len(merged))


def load_bit_index_mapping(path: Path, table_name: str, *, strict_missing: bool = False) -> dict[int, int]:
    """严格读取 ``bit_index`` 持久化映射，文件损坏一律 fail-closed。"""
    if not path.exists():
        if strict_missing:
            raise BitIndexStateError(
                f"[{table_name}] bit-index mapping file is missing: {path}"
            )
        logger.warning("[%s] Bit-index mapping file missing; initializing empty state at %s", table_name, path)
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BitIndexStateError(f"[{table_name}] failed to parse bit-index mapping {path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise BitIndexStateError(f"[{table_name}] bit-index mapping root must be a JSON object: {path}")

    mapping: dict[int, int] = {}
    used_indices: set[int] = set()
    for raw_k, raw_v in raw.items():
        if isinstance(raw_v, bool):
            raise BitIndexStateError(f"[{table_name}] boolean bit index in {path}: {raw_k!r}={raw_v!r}")
        try:
            row_id = int(raw_k)
            bit_idx = int(raw_v)
        except (TypeError, ValueError) as exc:
            raise BitIndexStateError(
                f"[{table_name}] non-integer entry in {path}: {raw_k!r}={raw_v!r}"
            ) from exc
        if bit_idx < 0:
            raise BitIndexStateError(f"[{table_name}] negative bit index in {path}: {row_id}={bit_idx}")
        if row_id in mapping:
            raise BitIndexStateError(f"[{table_name}] duplicate row id {row_id} in {path}")
        if bit_idx in used_indices:
            raise BitIndexStateError(f"[{table_name}] duplicate bit index {bit_idx} in {path}")
        mapping[row_id] = bit_idx
        used_indices.add(bit_idx)
    return mapping


def allocate_bit_indices(
    table_name: str,
    row_ids: Sequence[int],
    mapping_path: Path,
    *,
    strict_missing: bool = False,
    persist: bool = True,
) -> dict[int, int]:
    """单调分配并持久化 ``ID -> bit_index`` 映射，已删除 ID 的位序永久占位绝不复用。"""
    existing = load_bit_index_mapping(mapping_path, table_name, strict_missing=strict_missing)
    next_idx = max(existing.values(), default=-1) + 1

    added: list[tuple[int, int]] = []
    for raw_id in row_ids:
        row_id = int(raw_id)
        if row_id in existing:
            continue
        existing[row_id] = next_idx
        added.append((row_id, next_idx))
        next_idx += 1

    ordered = dict(sorted(existing.items(), key=lambda kv: kv[1]))
    retired = sorted(set(ordered) - {int(i) for i in row_ids})
    if added:
        logger.info("[%s] Allocated %d new bit index(es)", table_name, len(added))
    if retired:
        logger.warning(
            "[%s] %d retired id(s) remain permanently reserved in bit-index mapping: %s",
            table_name,
            len(retired),
            ", ".join(f"{rid}(bit={ordered[rid]})" for rid in retired[:10]),
        )

    if persist:
        mapping_path.parent.mkdir(parents=True, exist_ok=True)
        serialized = {str(rid): idx for rid, idx in ordered.items()}
        with open(mapping_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(serialized, f, indent=2, ensure_ascii=False)
            f.write("\n")
    return ordered


# ---------------------------------------------------------------------------
# Tip.xlsx 单轴校验与 tip_text.json 生成
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TipEntry:
    """单条激活的 Tip 错误码记录。"""

    tip_id: int
    enum_name: str
    text: str
    group: str = "default"
    is_fault: bool = False


@dataclass(frozen=True)
class TipAxisResult:
    """Tip 单轴解析与校验结果。"""

    entries: tuple[TipEntry, ...]
    id_by_name: dict[str, int]
    text_by_id: dict[str, str]
    fault_ids: tuple[int, ...]


def _parse_fault_flag(value: Any, ctx: str) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip().lower()
    if text in _FAULT_TRUE:
        return True
    if text in _FAULT_FALSE:
        return False
    raise TipAxisError(f"{ctx}: unrecognized fault value {value!r}")


def _load_tip_state(path: Path, *, strict_missing: bool = False) -> dict[str, Any]:
    if not path.exists():
        if strict_missing:
            raise TipAxisError(f"tip state file is missing: {path}")
        return {"schema_version": TIP_STATE_SCHEMA_VERSION, "ids": {}, "groups": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TipAxisError(f"failed to parse tip state {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise TipAxisError(f"tip state root must be a JSON object: {path}")
    ids: dict[str, int] = {}
    if isinstance(raw.get("ids"), dict):
        for k, v in raw["ids"].items():
            ids[str(k)] = int(v)
    groups = raw.get("groups") if isinstance(raw.get("groups"), dict) else {}
    for gdata in groups.values():
        if isinstance(gdata, dict) and isinstance(gdata.get("ids"), dict):
            for k, v in gdata["ids"].items():
                ids[str(k)] = int(v)
    return {
        "schema_version": TIP_STATE_SCHEMA_VERSION,
        "ids": ids,
        "groups": groups,
    }


def _save_tip_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, sort_keys=True)
        f.write("\n")


def _parse_segmented_tip_matrix(
    matrix: Sequence[Sequence[Any]],
    state: dict[str, Any],
) -> tuple[list[TipEntry], dict[str, Any]]:
    """解析 C++ 风格的组头分段 ``Tip.xlsx``（``//group base=N width=M``）。"""
    header = matrix[0] if matrix else []
    fault_col: int | None = None
    for idx, val in enumerate(header):
        if val is not None and str(val).strip().lower() == "fault":
            fault_col = idx
            break

    groups: dict[str, dict[str, Any]] = {}
    current_group: str | None = None
    for row_num, row in enumerate(matrix[1:], start=2):
        if not row:
            continue
        first = row[0]
        if _is_empty_cell(first):
            continue
        text0 = str(first).strip()
        if text0.startswith("//"):
            rest = text0[2:]
            if not rest or rest[0].isspace():
                continue
            parts = rest.strip().split()
            gname = parts[0]
            kv = dict(p.split("=", 1) for p in parts[1:] if "=" in p)
            if "base" not in kv or "width" not in kv:
                raise TipAxisError(f"Tip row {row_num}: group header missing base/width: {text0!r}")
            try:
                base = int(kv["base"])
                width = int(kv["width"])
            except ValueError as exc:
                raise TipAxisError(f"Tip row {row_num}: non-integer base/width in {text0!r}") from exc
            if base <= 0 or width <= 0:
                raise TipAxisError(f"Tip row {row_num}: base and width must be positive: {text0!r}")
            if gname in groups:
                raise TipAxisError(f"Tip row {row_num}: duplicate group {gname!r}")
            groups[gname] = {"base": base, "width": width, "items": []}
            current_group = gname
        elif current_group is not None:
            if not _VALID_IDENT_RE.match(text0):
                raise TipAxisError(f"Tip row {row_num}: invalid enum name {text0!r}")
            msg = "" if len(row) < 2 or row[1] is None else str(row[1]).strip()
            if not msg:
                raise TipAxisError(f"Tip row {row_num}: tip {text0!r} has empty text")
            fault_raw = row[fault_col] if (fault_col is not None and fault_col < len(row)) else None
            is_fault = _parse_fault_flag(fault_raw, f"Tip row {row_num}")
            groups[current_group]["items"].append((text0, msg, is_fault))

    if not groups:
        raise TipAxisError("Tip.xlsx contains no tip groups")

    # 检查段不重叠。
    segs = sorted((g["base"], g["base"] + g["width"], name) for name, g in groups.items())
    for (lo1, hi1, n1), (lo2, hi2, n2) in zip(segs, segs[1:]):
        if lo2 < hi1:
            raise TipAxisError(f"overlapping tip segments: {n1}[{lo1},{hi1}) and {n2}[{lo2},{hi2})")

    prev_groups = state.get("groups", {})
    prev_all_ids: dict[str, int] = dict(state.get("ids", {}))
    out_groups = dict(prev_groups)
    entries: list[TipEntry] = []
    seen_names: set[str] = set()
    used_codes: dict[int, str] = {code: name for name, code in prev_all_ids.items()}

    for gname, g in groups.items():
        base, width = g["base"], g["width"]
        known = {k: int(v) for k, v in prev_groups.get(gname, {}).get("ids", {}).items()}
        for k, code in known.items():
            if not (base <= code < base + width):
                raise TipAxisError(f"tip {gname}.{k}={code} falls outside segment [{base}, {base + width})")
        cursor = max(known.values(), default=base - 1) + 1
        for enum_name, msg, is_fault in g["items"]:
            if enum_name in seen_names:
                raise TipAxisError(f"duplicate tip enum name: {enum_name!r}")
            seen_names.add(enum_name)
            if enum_name in known:
                code = known[enum_name]
            else:
                while cursor in used_codes and cursor < base + width:
                    cursor += 1
                if cursor >= base + width:
                    raise TipAxisError(f"tip group {gname!r} segment [{base}, {base + width}) is full")
                code = cursor
                known[enum_name] = code
                used_codes[code] = enum_name
                prev_all_ids[enum_name] = code
                cursor += 1
            entries.append(TipEntry(tip_id=code, enum_name=enum_name, text=msg, group=gname, is_fault=is_fault))
        out_groups[gname] = {
            "base": base,
            "width": width,
            "ids": dict(sorted(known.items(), key=lambda kv: kv[1])),
        }

    new_state = {
        "schema_version": TIP_STATE_SCHEMA_VERSION,
        "ids": dict(sorted(prev_all_ids.items(), key=lambda kv: kv[1])),
        "groups": out_groups,
    }
    return entries, new_state


def _parse_tabular_tip_matrix(
    matrix: Sequence[Sequence[Any]],
    state: dict[str, Any],
) -> tuple[list[TipEntry], dict[str, Any]]:
    """解析表格型 ``Tip.xlsx``（支持 1 行表头或 4 行标准表头）。"""
    if not matrix:
        raise TipAxisError("Tip.xlsx is empty")

    header_row = [str(c).strip().lower() if c is not None else "" for c in matrix[0]]
    # 自动检测第 2 行是否为 4 行 schema 表头的类型行。
    data_start = 1
    if len(matrix) >= 4:
        row2_tokens = [str(c).strip().lower() for c in matrix[1] if not _is_empty_cell(c)]
        if row2_tokens and all(
            t in SUPPORTED_SCALAR_TYPES or t.startswith("repeated ") or t.endswith("[]")
            for t in row2_tokens
        ):
            data_start = 4

    def find_col(*aliases: str) -> int | None:
        for idx, col in enumerate(header_row):
            if col in aliases:
                return idx
        return None

    id_col = find_col("id", "tip_id", "code")
    name_col = find_col("name", "enum_name", "enum", "key")
    text_col = find_col("text", "message", "msg", "desc", "comment", "content")
    group_col = find_col("group", "domain", "module")
    fault_col = find_col("fault", "is_fault")

    if name_col is None or text_col is None:
        raise TipAxisError(
            f"Tip.xlsx header must contain enum name and text columns (got headers: {header_row})"
        )

    prev_ids: dict[str, int] = dict(state.get("ids", {}))
    id_to_owner: dict[int, str] = {code: name for name, code in prev_ids.items()}
    max_allocated = max(prev_ids.values(), default=0)

    seen_names: set[str] = set()
    seen_ids: set[int] = set()
    entries: list[TipEntry] = []

    for row_num, row in enumerate(matrix[data_start:], start=data_start + 1):
        if all(_is_empty_cell(c) for c in row):
            continue

        def at(idx: int | None) -> Any:
            return row[idx] if (idx is not None and idx < len(row)) else None

        raw_name = at(name_col)
        raw_text = at(text_col)
        raw_id = at(id_col)
        raw_group = at(group_col)
        raw_fault = at(fault_col)

        if _is_empty_cell(raw_name):
            raise TipAxisError(f"Tip row {row_num}: empty enum name")
        enum_name = str(raw_name).strip()
        if not _VALID_IDENT_RE.match(enum_name):
            raise TipAxisError(f"Tip row {row_num}: invalid enum name {enum_name!r}")
        if enum_name in seen_names:
            raise TipAxisError(f"Tip row {row_num}: duplicate enum name {enum_name!r}")
        seen_names.add(enum_name)

        if _is_empty_cell(raw_text):
            raise TipAxisError(f"Tip row {row_num}: tip {enum_name!r} has empty text")
        text = str(raw_text).strip()

        if not _is_empty_cell(raw_id):
            try:
                if isinstance(raw_id, bool):
                    raise ValueError("bool is not valid tip id")
                if isinstance(raw_id, float) and not raw_id.is_integer():
                    raise ValueError("float is not integer tip id")
                tip_id = int(raw_id)
            except (TypeError, ValueError) as exc:
                raise TipAxisError(f"Tip row {row_num}: non-integer tip id {raw_id!r}") from exc
            if tip_id <= 0:
                raise TipAxisError(f"Tip row {row_num}: tip id must be positive, got {tip_id}")
            if tip_id in seen_ids:
                raise TipAxisError(f"Tip row {row_num}: duplicate numeric tip id {tip_id}")
            if enum_name in prev_ids and prev_ids[enum_name] != tip_id:
                raise TipAxisError(
                    f"Tip row {row_num}: enum {enum_name!r} changed id from {prev_ids[enum_name]} to {tip_id}"
                )
            owner = id_to_owner.get(tip_id)
            if owner is not None and owner != enum_name:
                raise TipAxisError(
                    f"Tip row {row_num}: tip id {tip_id} is already permanently reserved by {owner!r}"
                )
            if enum_name not in prev_ids and tip_id <= max_allocated and prev_ids:
                raise TipAxisError(
                    f"Tip row {row_num}: new tip id {tip_id} for {enum_name!r} violates monotonic allocation (max={max_allocated})"
                )
            max_allocated = max(max_allocated, tip_id)
        else:
            if enum_name in prev_ids:
                tip_id = prev_ids[enum_name]
            else:
                max_allocated += 1
                while max_allocated in id_to_owner or max_allocated in seen_ids:
                    max_allocated += 1
                tip_id = max_allocated

        seen_ids.add(tip_id)
        prev_ids[enum_name] = tip_id
        id_to_owner[tip_id] = enum_name

        group = "default" if _is_empty_cell(raw_group) else str(raw_group).strip()
        is_fault = _parse_fault_flag(raw_fault, f"Tip row {row_num}")
        entries.append(TipEntry(tip_id=tip_id, enum_name=enum_name, text=text, group=group, is_fault=is_fault))

    if not entries:
        raise TipAxisError("Tip.xlsx contains no tip entries")

    new_state = {
        "schema_version": TIP_STATE_SCHEMA_VERSION,
        "ids": dict(sorted(prev_ids.items(), key=lambda kv: kv[1])),
        "groups": state.get("groups", {}),
    }
    return entries, new_state


def _evaluate_tips(
    tip_path: Path,
    mapping_dir: Path,
    *,
    strict_missing: bool = False,
) -> tuple[TipAxisResult, Path, dict[str, Any]]:
    """读取并校验 Tip 单轴文件，返回结果与待落盘状态（不直接写盘）。"""
    _, matrix = read_source_matrix(tip_path)
    state_path = mapping_dir / TIP_STATE_FILENAME
    state = _load_tip_state(state_path, strict_missing=strict_missing)

    has_group_headers = any(
        row and not _is_empty_cell(row[0]) and str(row[0]).strip().startswith("//") and "base=" in str(row[0])
        for row in matrix[1:]
    )
    if has_group_headers:
        entries, new_state = _parse_segmented_tip_matrix(matrix, state)
    else:
        entries, new_state = _parse_tabular_tip_matrix(matrix, state)

    entries_sorted = tuple(sorted(entries, key=lambda e: e.tip_id))
    result = TipAxisResult(
        entries=entries_sorted,
        id_by_name={e.enum_name: e.tip_id for e in entries_sorted},
        text_by_id={str(e.tip_id): e.text for e in entries_sorted},
        fault_ids=tuple(e.tip_id for e in entries_sorted if e.is_fault),
    )
    return result, state_path, new_state


def validate_and_allocate_tips(
    tip_path: Path,
    mapping_dir: Path,
    *,
    persist: bool = True,
    strict_missing: bool = False,
) -> TipAxisResult:
    """校验并单调分配 ``Tip.xlsx`` 单轴错误码，返回可供导出 ``tip_text.json`` 的结果。"""
    result, state_path, new_state = _evaluate_tips(
        tip_path, mapping_dir, strict_missing=strict_missing
    )
    if persist:
        _save_tip_state(state_path, new_state)
    return result


def validate_tip_references(tables: Sequence[ParsedTable], tip_result: TipAxisResult | None) -> None:
    """校验普通数据表中标有 ``tip_ref`` 的列引用的 tip ID 均有效。"""
    if tip_result is None:
        return
    valid_ids = {0} | {e.tip_id for e in tip_result.entries}
    errors: list[str] = []
    for table in tables:
        for col in table.schema.server_columns:
            if not col.is_tip_ref or col.data_type not in _INTEGER_BOUNDS:
                continue
            for row_idx, row in enumerate(table.rows, start=5):
                raw_val = row[col.name]
                values = raw_val if col.repeated else (raw_val,)
                for code in values:
                    if code not in valid_ids:
                        errors.append(
                            f"[{table.schema.name}!row {row_idx}.{col.name}] unknown tip id {code}"
                        )
    if errors:
        raise TipAxisError("invalid embedded tip reference(s):\n  " + "\n  ".join(errors))


# ---------------------------------------------------------------------------
# Proto 代码生成与描述符编译（与 ConfigTables 完全对齐）
# ---------------------------------------------------------------------------

def render_cfg_options_proto() -> str:
    """生成 ``mmorpg.cfgtable.v1`` 的自定义选项定义文件 ``cfg_options.proto``。"""
    return """syntax = "proto3";

package mmorpg.cfgtable.v1;

import "google/protobuf/descriptor.proto";

enum CfgShape {
  CFG_SHAPE_UNSPECIFIED = 0;
  CFG_SHAPE_SCALAR = 1;
  CFG_SHAPE_LIST = 2;
  CFG_SHAPE_SET = 3;
  CFG_SHAPE_MAP = 4;
  CFG_SHAPE_STRUCT_LIST = 5;
}

extend google.protobuf.MessageOptions {
  string cfg_sheet = 60001;
  string cfg_source_file = 60002;
  string cfg_primary_key = 60003;
  bool cfg_ids_permanent = 60005;
  bool cfg_hot_reload = 60006;
}

extend google.protobuf.FieldOptions {
  string cfg_col = 60101;
  string cfg_owner = 60102;
  uint32 cfg_slots = 60103;
  CfgShape cfg_shape = 60104;
  bool cfg_key = 60105;
  bool cfg_multi = 60106;
  bool cfg_index = 60107;
  bool cfg_bit_index = 60108;
  bool cfg_tip_ref = 60109;
  string cfg_fk = 60110;
  string cfg_gfk = 60111;
  string cfg_expr_type = 60112;
  repeated string cfg_expr_param = 60113;
  string cfg_composite = 60114;
}
"""


def _proto_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


def render_wire_proto(schema: TableSchema) -> str:
    """生成无 package 的 wire proto（``<Name>Table`` 与 ``<Name>TableData``）。"""
    lines = [
        'syntax = "proto3";',
        "",
        f"message {schema.name}Table {{",
    ]
    for col in schema.server_columns:
        prefix = "repeated " if col.repeated else ""
        lines.append(f"  {prefix}{col.data_type} {col.name} = {schema.field_numbers[col.name]};")
    lines.extend([
        "}",
        "",
        f"message {schema.name}TableData {{",
        f"  repeated {schema.name}Table data = 1;",
        "}",
        "",
    ])
    return "\n".join(lines)


def render_schema_proto(schema: TableSchema) -> str:
    """生成带 ``mmorpg.cfgtable.v1`` 元数据扩展的 schema proto，供 ``ConfigTables`` 建索引。"""
    source_name = schema.source_path.name if schema.source_path is not None else f"{schema.name}.xlsx"
    lines = [
        'syntax = "proto3";',
        "",
        "package mmorpg.cfgtable.v1;",
        "",
        'import "cfg_options.proto";',
        "",
        f"message {schema.name}Table {{",
        f'  option (cfg_sheet) = "{_proto_escape(schema.name)}";',
        f'  option (cfg_source_file) = "{_proto_escape(source_name)}";',
        f'  option (cfg_primary_key) = "{_proto_escape(schema.primary_key)}";',
    ]
    if schema.ids_permanent:
        lines.append("  option (cfg_ids_permanent) = true;")
    if schema.hot_reload:
        lines.append("  option (cfg_hot_reload) = true;")

    for col in schema.server_columns:
        prefix = "repeated " if col.repeated else ""
        opts: list[str] = []
        if col.is_primary_key or col.is_unique:
            opts.append("(cfg_key) = true")
        if col.is_multi:
            opts.append("(cfg_multi) = true")
        if col.is_indexed:
            opts.append("(cfg_index) = true")
        if col.has_bit_index:
            opts.append("(cfg_bit_index) = true")
        if col.is_tip_ref:
            opts.append("(cfg_tip_ref) = true")
        if col.foreign_key is not None:
            fk_str = (
                f"{col.foreign_key.target_table}.{col.foreign_key.target_column}"
                if col.foreign_key.target_column != "id"
                else col.foreign_key.target_table
            )
            opts.append(f'(cfg_fk) = "{_proto_escape(fk_str)}"')
        if col.group_foreign_key is not None:
            opts.append(f'(cfg_gfk) = "{_proto_escape(col.group_foreign_key)}"')
        opt_suffix = f" [{', '.join(opts)}]" if opts else ""
        lines.append(f"  {prefix}{col.data_type} {col.name} = {schema.field_numbers[col.name]}{opt_suffix};")
    lines.extend(["}", ""])
    return "\n".join(lines)


def _write_text_lf(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)


def compile_descriptor_set(
    schemas: Sequence[TableSchema],
    proto_dir: Path,
    descriptor_out: Path,
) -> bytes:
    """写出 ``*.proto`` 并调用 ``grpc_tools.protoc`` 生成 ``_table_descriptors.pb``。"""
    proto_dir = proto_dir.resolve()
    wire_dir = proto_dir / "table_wire"
    proto_dir.mkdir(parents=True, exist_ok=True)
    wire_dir.mkdir(parents=True, exist_ok=True)
    descriptor_out.parent.mkdir(parents=True, exist_ok=True)

    cfg_options_path = proto_dir / "cfg_options.proto"
    _write_text_lf(cfg_options_path, render_cfg_options_proto())

    sources: list[Path] = [cfg_options_path]
    for schema in sorted(schemas, key=lambda s: s.name):
        wire_path = wire_dir / f"{schema.name.lower()}_table.proto"
        schema_path = proto_dir / f"{schema.name.lower()}_table.proto"
        _write_text_lf(wire_path, render_wire_proto(schema))
        _write_text_lf(schema_path, render_schema_proto(schema))
        sources.append(wire_path)
        sources.append(schema_path)

    well_known = Path(grpc_tools.__file__).parent / "_proto"
    argv = [
        "protoc",
        f"-I{proto_dir}",
        f"-I{well_known}",
        "--include_imports",
        f"--descriptor_set_out={descriptor_out.resolve()}",
        *(str(src) for src in sources),
    ]
    exit_code = protoc.main(argv)
    if exit_code != 0 or not descriptor_out.is_file():
        raise ProtoCompileError(f"protoc failed with exit code {exit_code} for {len(schemas)} table(s)")
    return descriptor_out.read_bytes()


def _build_descriptor_pool(payload: bytes) -> descriptor_pool.DescriptorPool:
    bundle = descriptor_pb2.FileDescriptorSet.FromString(payload)
    pool = descriptor_pool.DescriptorPool()
    for file_proto in bundle.file:
        pool.AddSerializedFile(file_proto.SerializeToString())
    return pool


# ---------------------------------------------------------------------------
# Manifest / 产物生成 / Python 常量模块 / pb2json
# ---------------------------------------------------------------------------

def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def compute_content_digest(entries: Sequence[dict[str, Any]]) -> str:
    """按 ``xuanming.data.config_tables._content_digest`` 相同口径计算清单内容指纹。"""
    digest = hashlib.sha256()
    for entry in entries:
        digest.update(entry["name"].encode("utf-8"))
        digest.update(str(entry["rows"]).encode("utf-8"))
        digest.update(entry["source"].get("sha256", "").encode("utf-8"))
        for artifact in entry["artifacts"]:
            for key in ("kind", "file", "sha256"):
                digest.update(artifact[key].encode("utf-8"))
    return digest.hexdigest()


def _safe_python_ident(name: str) -> str:
    cleaned = _NON_IDENT_RE.sub("_", name.strip())
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    if keyword.iskeyword(cleaned):
        cleaned += "_"
    return cleaned


def render_python_constants(
    tables: Sequence[ParsedTable],
    bit_indices: dict[str, dict[int, int]],
    tip_result: TipAxisResult | None,
) -> str:
    """生成纯 Python 强类型配置表常量模块（``table_constants.py``）。"""
    lines = [
        '"""自动生成的配置表与 Tip 错误码常量模块，禁止手动修改。"""',
        "",
        "from __future__ import annotations",
        "",
        "from enum import IntEnum",
        "from typing import Final",
        "",
    ]

    for table in sorted(tables, key=lambda t: t.schema.name):
        upper = table.schema.name.upper()
        if table.constants_names:
            lines.append(f"class {table.schema.name}Ids(IntEnum):")
            lines.append(f'    """{table.schema.name} 表具名常量 ID。"""')
            for row_id, const_name in sorted(table.constants_names.items(), key=lambda kv: kv[0]):
                lines.append(f"    {_safe_python_ident(const_name).upper()} = {int(row_id)}")
            lines.append("")
        if table.schema.name in bit_indices:
            mapping = bit_indices[table.schema.name]
            capacity = max(mapping.values(), default=-1) + 1
            lines.append(f"{upper}_BITMAP_CAPACITY: Final[int] = {capacity}")
            lines.append(f"{upper}_BIT_INDEX: Final[dict[int, int]] = {{")
            for row_id, bit_idx in sorted(mapping.items(), key=lambda kv: kv[1]):
                lines.append(f"    {row_id}: {bit_idx},")
            lines.append("}")
            lines.append("")

    if tip_result is not None and tip_result.entries:
        lines.append("class TipCode(IntEnum):")
        lines.append('    """Tip.xlsx 单轴错误码常量。"""')
        for entry in tip_result.entries:
            lines.append(f"    {_safe_python_ident(entry.enum_name)} = {entry.tip_id}")
        lines.append("")
        if tip_result.fault_ids:
            fault_items = ", ".join(str(code) for code in tip_result.fault_ids)
            lines.append(f"TIP_FAULT_CODES: Final[frozenset[int]] = frozenset({{{fault_items}}})")
        else:
            lines.append("TIP_FAULT_CODES: Final[frozenset[int]] = frozenset()")
        lines.append("")

    return "\n".join(lines)


def discover_sources(input_dir: Path) -> tuple[list[Path], Path | None]:
    """扫描输入目录，区分普通数据表源与 ``Tip.xlsx``。"""
    if not input_dir.is_dir():
        raise SchemaError(f"input directory does not exist: {input_dir}")
    table_files: list[Path] = []
    tip_file: Path | None = None
    for path in sorted(input_dir.iterdir(), key=lambda p: p.name.lower()):
        if not path.is_file() or path.name.startswith("~$") or path.name.startswith("."):
            continue
        if path.suffix.lower() not in _SUPPORTED_SOURCE_SUFFIXES:
            continue
        if path.stem.lower() == "tip":
            tip_file = path
        elif path.stem.lower() in ("manifest", "tip_text", "tip_enum_ids"):
            continue
        else:
            table_files.append(path)
    return table_files, tip_file


def load_and_validate_directory(
    input_dir: Path,
    mapping_dir: Path,
    *,
    persist_state: bool = False,
    strict_missing: bool = False,
) -> tuple[list[ParsedTable], dict[str, dict[int, int]], TipAxisResult | None]:
    """加载并全量校验目录内所有数据表与 ``Tip.xlsx``（全部校验通过后才按需落盘状态）。"""
    table_files, tip_file = discover_sources(input_dir)
    if not table_files and tip_file is None:
        raise SchemaError(f"no table source files found in {input_dir}")

    tables: list[ParsedTable] = []
    seen_names: set[str] = set()
    for src in table_files:
        name, matrix = read_source_matrix(src)
        if name in seen_names:
            raise SchemaError(f"duplicate table name discovered: {name!r}")
        seen_names.add(name)
        tables.append(parse_table_matrix(name, matrix, source_path=src))

    validate_primary_keys(tables)
    validate_foreign_keys(tables)
    validate_permanent_ids(tables, mapping_dir)

    tip_result: TipAxisResult | None = None
    tip_state_to_save: tuple[Path, dict[str, Any]] | None = None
    if tip_file is not None:
        tip_result, tip_state_path, new_tip_state = _evaluate_tips(
            tip_file, mapping_dir, strict_missing=strict_missing
        )
        validate_tip_references(tables, tip_result)
        tip_state_to_save = (tip_state_path, new_tip_state)

    # 先内存校验全部 bit_index 映射文件，确保任一表映射损坏时不会半途写盘。
    bit_indices: dict[str, dict[int, int]] = {}
    bit_maps_to_save: list[tuple[Path, dict[int, int]]] = []
    for table in tables:
        if not table.schema.bit_index_columns:
            continue
        pk = table.schema.primary_key
        row_ids = [int(row[pk]) for row in table.rows]
        mapping_path = mapping_dir / f"{table.schema.name.lower()}_mapping.json"
        ordered = allocate_bit_indices(
            table.schema.name,
            row_ids,
            mapping_path,
            strict_missing=strict_missing,
            persist=False,
        )
        bit_indices[table.schema.name] = ordered
        bit_maps_to_save.append((mapping_path, ordered))

    if persist_state:
        if tip_state_to_save is not None:
            _save_tip_state(tip_state_to_save[0], tip_state_to_save[1])
        for mapping_path, ordered in bit_maps_to_save:
            mapping_path.parent.mkdir(parents=True, exist_ok=True)
            serialized = {str(rid): idx for rid, idx in ordered.items()}
            with open(mapping_path, "w", encoding="utf-8", newline="\n") as f:
                json.dump(serialized, f, indent=2, ensure_ascii=False)
                f.write("\n")
        record_permanent_ids(tables, mapping_dir)

    return tables, bit_indices, tip_result


@dataclass(frozen=True)
class ExportSummary:
    """一次导表执行的结构化结果摘要。"""

    output_dir: Path
    version: int
    schema_digest: str
    content_digest: str
    table_count: int
    tables: tuple[str, ...]
    tip_count: int = 0
    constants_path: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "output_dir": str(self.output_dir),
            "version": self.version,
            "schema_digest": self.schema_digest,
            "content_digest": self.content_digest,
            "table_count": self.table_count,
            "tables": list(self.tables),
            "tip_count": self.tip_count,
            "constants_path": str(self.constants_path) if self.constants_path else None,
        }


def _load_previous_manifest(manifest_path: Path) -> dict[str, Any] | None:
    if not manifest_path.is_file():
        return None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def export_tables(
    input_dir: str | Path,
    output_dir: str | Path,
    *,
    mapping_dir: str | Path | None = None,
    version: int | None = None,
    strict_missing: bool = False,
    emit_constants: bool = True,
) -> ExportSummary:
    """执行完整导表管线：校验 -> 生成 proto/描述符 -> 写出 JSON/PB/manifest/Tip/常量。"""
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    map_path = Path(mapping_dir) if mapping_dir is not None else out_path / "_mapping"

    tables, bit_indices, tip_result = load_and_validate_directory(
        in_path, map_path, persist_state=True, strict_missing=strict_missing
    )

    out_path.mkdir(parents=True, exist_ok=True)
    schemas = [t.schema for t in tables]
    descriptor_path = out_path / DESCRIPTOR_SET_FILENAME
    descriptor_bytes = compile_descriptor_set(schemas, out_path / "proto", descriptor_path)
    pool = _build_descriptor_pool(descriptor_bytes)

    manifest_entries: list[dict[str, Any]] = []
    for table in sorted(tables, key=lambda t: t.schema.name):
        stem = table.schema.name.lower()
        json_path = out_path / f"{stem}.json"
        pb_path = out_path / f"{stem}.pb"

        json_text = json.dumps(
            {"data": list(table.rows)},
            sort_keys=True,
            indent=1,
            separators=(",", ": "),
            ensure_ascii=False,
        )
        json_bytes = json_text.encode("utf-8")
        json_path.write_bytes(json_bytes)

        data_desc = pool.FindMessageTypeByName(f"{table.schema.name}TableData")
        msg = GetMessageClass(data_desc)()
        json_format.Parse(json_text, msg)
        pb_bytes = msg.SerializeToString(deterministic=True)
        pb_path.write_bytes(pb_bytes)

        src = table.schema.source_path
        if src is not None and src.is_file():
            source_info = {
                "file": src.name,
                "size": src.stat().st_size,
                "sha256": _sha256_file(src),
            }
        else:
            source_info = {"file": "", "size": 0, "sha256": ""}

        manifest_entries.append({
            "name": table.schema.name,
            "hot_reload": table.schema.hot_reload,
            "ids_permanent": table.schema.ids_permanent,
            "rows": len(table.rows),
            "source": source_info,
            "artifacts": [
                {
                    "kind": "json",
                    "file": json_path.name,
                    "size": len(json_bytes),
                    "sha256": _sha256_bytes(json_bytes),
                },
                {
                    "kind": "binary",
                    "file": pb_path.name,
                    "size": len(pb_bytes),
                    "sha256": _sha256_bytes(pb_bytes),
                },
            ],
        })

    schema_digest = compute_schema_digest(schemas)
    content_digest = compute_content_digest(manifest_entries)

    manifest_path = out_path / MANIFEST_FILENAME
    previous = _load_previous_manifest(manifest_path)
    if version is not None:
        if version < 1:
            raise SchemaError(f"manifest version must be >= 1, got {version}")
        resolved_version = version
    elif previous is not None and previous.get("content_digest") == content_digest:
        try:
            resolved_version = max(1, int(previous.get("version", 1)))
        except (TypeError, ValueError):
            resolved_version = 1
    elif previous is not None:
        try:
            resolved_version = max(1, int(previous.get("version", 0)) + 1)
        except (TypeError, ValueError):
            resolved_version = 1
    else:
        resolved_version = 1

    manifest_obj: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "schema_digest": schema_digest,
        "version": resolved_version,
        "content_digest": content_digest,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "table_count": len(manifest_entries),
        "tables": manifest_entries,
    }
    _write_text_lf(manifest_path, json.dumps(manifest_obj, indent=2, ensure_ascii=False) + "\n")

    tip_count = 0
    if tip_result is not None:
        tip_count = len(tip_result.entries)
        tip_out = out_path / TIP_TEXT_FILENAME
        _write_text_lf(
            tip_out,
            json.dumps(tip_result.text_by_id, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )

    constants_path: Path | None = None
    if emit_constants:
        constants_path = out_path / CONSTANTS_FILENAME
        _write_text_lf(constants_path, render_python_constants(tables, bit_indices, tip_result))

    logger.info(
        "Exported %d table(s) to %s (version=%d, digest=%s)",
        len(manifest_entries),
        out_path,
        resolved_version,
        content_digest[:12],
    )
    return ExportSummary(
        output_dir=out_path,
        version=resolved_version,
        schema_digest=schema_digest,
        content_digest=content_digest,
        table_count=len(manifest_entries),
        tables=tuple(e["name"] for e in manifest_entries),
        tip_count=tip_count,
        constants_path=constants_path,
    )


def pb_to_json_dict(
    pb_path: str | Path,
    descriptor_set_path: str | Path | None = None,
    *,
    table_name: str | None = None,
) -> dict[str, Any]:
    """使用 ``_table_descriptors.pb`` 将二进制 ``<table>.pb`` 反向解析为 JSON 字典。"""
    pb_file = Path(pb_path)
    if not pb_file.is_file():
        raise TableExporterError(f"protobuf binary file not found: {pb_file}")
    desc_file = (
        Path(descriptor_set_path)
        if descriptor_set_path is not None
        else pb_file.with_name(DESCRIPTOR_SET_FILENAME)
    )
    if not desc_file.is_file():
        raise TableExporterError(f"descriptor set file not found: {desc_file}")

    bundle = descriptor_pb2.FileDescriptorSet.FromString(desc_file.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    target_msg_name: str | None = None
    want_lower = f"{(table_name or pb_file.stem).lower()}tabledata"

    for file_proto in bundle.file:
        pool.AddSerializedFile(file_proto.SerializeToString())
        if file_proto.package:
            continue
        for msg_proto in file_proto.message_type:
            if msg_proto.name.lower() == want_lower:
                target_msg_name = msg_proto.name

    if target_msg_name is None:
        raise TableExporterError(
            f"message {want_lower!r} not found in descriptor set {desc_file}"
        )

    desc = pool.FindMessageTypeByName(target_msg_name)
    msg = GetMessageClass(desc)()
    msg.ParseFromString(pb_file.read_bytes())
    result = json_format.MessageToDict(
        msg,
        always_print_fields_with_no_presence=True,
        preserving_proto_field_name=True,
        unquote_int64_if_possible=True,
    )
    result.setdefault("data", [])
    return result


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="table-exporter",
        description="Pure-Python data table exporter compatible with xuanming.data.ConfigTables.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    exp = sub.add_parser("export", help="Validate and export tables to JSON/PB/descriptors/manifest.")
    exp.add_argument("--input-dir", required=True, type=Path, help="Directory of .xlsx/.csv/.json tables")
    exp.add_argument("--output-dir", required=True, type=Path, help="Output directory for generated artifacts")
    exp.add_argument("--mapping-dir", type=Path, default=None, help="Directory for persistent bit_index/tip state")
    exp.add_argument("--version", type=int, default=None, help="Explicit manifest version integer")
    exp.add_argument("--strict-mapping", action="store_true", help="Fail if bit_index/tip mapping file is missing")
    exp.add_argument("--no-constants", action="store_true", help="Skip generating table_constants.py")
    exp.add_argument("--json", dest="as_json", action="store_true", help="Print JSON summary to stdout")

    val = sub.add_parser("validate", help="Dry-run schema, primary key, foreign key, and Tip validation.")
    val.add_argument("--input-dir", required=True, type=Path, help="Directory of .xlsx/.csv/.json tables")
    val.add_argument("--mapping-dir", type=Path, default=None, help="Directory for persistent state checks")
    val.add_argument("--strict-mapping", action="store_true", help="Fail if bit_index/tip mapping file is missing")
    val.add_argument("--json", dest="as_json", action="store_true", help="Print JSON validation report to stdout")

    p2j = sub.add_parser("pb2json", help="Decode a binary .pb table file to JSON using a descriptor set.")
    p2j.add_argument("pb_file", type=Path, help="Path to <table>.pb binary file")
    p2j.add_argument("--descriptor-set", type=Path, default=None, help="Path to _table_descriptors.pb")
    p2j.add_argument("--table", type=str, default=None, help="Explicit table name (inferred from filename if omitted)")
    p2j.add_argument("--output", "-o", type=Path, default=None, help="Optional output JSON file path")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """配置表导出工具 CLI 入口。"""
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        if args.command == "export":
            summary = export_tables(
                args.input_dir,
                args.output_dir,
                mapping_dir=args.mapping_dir,
                version=args.version,
                strict_missing=args.strict_mapping,
                emit_constants=not args.no_constants,
            )
            if args.as_json:
                print(json.dumps(summary.to_dict(), indent=2, ensure_ascii=False))
            else:
                print(
                    f"Exported {summary.table_count} table(s) to {summary.output_dir} "
                    f"(version={summary.version}, digest={summary.content_digest[:12]})"
                )
            return 0

        if args.command == "validate":
            map_dir = args.mapping_dir if args.mapping_dir is not None else args.input_dir / "_mapping"
            tables, bit_indices, tip_result = load_and_validate_directory(
                args.input_dir,
                map_dir,
                persist_state=False,
                strict_missing=args.strict_mapping,
            )
            payload = {
                "ok": True,
                "table_count": len(tables),
                "tables": [t.schema.name for t in sorted(tables, key=lambda item: item.schema.name)],
                "bit_index_tables": sorted(bit_indices.keys()),
                "tip_count": len(tip_result.entries) if tip_result else 0,
            }
            if args.as_json:
                print(json.dumps(payload, indent=2, ensure_ascii=False))
            else:
                print(
                    f"Validated {payload['table_count']} table(s)"
                    + (f" and {payload['tip_count']} tip(s)" if tip_result else "")
                )
            return 0

        if args.command == "pb2json":
            decoded = pb_to_json_dict(
                args.pb_file,
                descriptor_set_path=args.descriptor_set,
                table_name=args.table,
            )
            rendered = json.dumps(decoded, sort_keys=True, indent=1, separators=(",", ": "), ensure_ascii=False)
            if args.output is not None:
                _write_text_lf(args.output, rendered)
            else:
                print(rendered)
            return 0

        parser.error(f"unknown command: {args.command}")
        return 2
    except TableExporterError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
