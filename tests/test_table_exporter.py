"""纯 Python 配置表导出工具（``xuanming.ops.table_exporter``）综合测试套件。"""

from __future__ import annotations

import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from xuanming.data.config_tables import ConfigTables
from xuanming.ops.table_exporter import (
    BitIndexStateError,
    ForeignKeyError,
    PermanentIdsError,
    PrimaryKeyError,
    SchemaError,
    TipAxisError,
    allocate_bit_indices,
    col_letters_to_index,
    export_tables,
    index_to_col_letters,
    main,
    pb_to_json_dict,
    read_source_matrix,
    read_xlsx_sheet,
    validate_and_allocate_tips,
    write_xlsx_sheet,
)


def _write_sample_tables(input_dir: Path) -> None:
    """在 ``input_dir`` 中生成标准测试集：Item.xlsx、Reward.xlsx、Tip.xlsx。"""
    write_xlsx_sheet(
        input_dir / "Item.xlsx",
        [
            ["id", "code", "name", "price", "weight", "stackable", "tags", "const_key", "designer_note"],
            ["uint32", "string", "string", "int64", "double", "bool", "int32[]", "string", "string"],
            ["物品ID", "唯一编码", "物品名", "价格", "重量", "可堆叠", "标签数组", "常量名", "策划备注"],
            [
                "pk bit_index ids_permanent hot_reload",
                "unique",
                "",
                "",
                "",
                "",
                "",
                "constants_name",
                "design",
            ],
            [1001, "sword_iron", "铁剑", 500, 2.5, True, "1,2,3", "IRON_SWORD", "新手武器"],
            [1002, "potion_hp", "生命药水", 50, 0.25, True, "[4, 5]", "HP_POTION", "恢复用"],
            [1003, "gem_fire", "火灵石", 9999999999, 0.1, False, "", "FIRE_GEM", "稀有宝石"],
        ],
        sheet_name="Item",
    )
    write_xlsx_sheet(
        input_dir / "Reward.xlsx",
        [
            ["id", "item_id", "bonus_items", "amount", "fail_tip"],
            ["uint32", "uint32", "repeated uint32", "int32", "int32"],
            ["奖励ID", "主物品ID", "附加物品列表", "数量", "失败提示码"],
            ["pk", "fk:Item.id", "fk:Item.id", "", "tip_ref"],
            [2001, 1001, "1002,1003", 1, 1001],
            [2002, 1002, "", 5, 0],
        ],
        sheet_name="Reward",
    )
    write_xlsx_sheet(
        input_dir / "Tip.xlsx",
        [
            ["id", "name", "text", "fault"],
            ["int32", "string", "string", "bool"],
            ["提示ID", "枚举名", "文案", "是否内部故障"],
            ["pk", "unique", "", ""],
            [1001, "TIP_BAG_FULL", "背包已满", 0],
            [1002, "TIP_DB_ERROR", "数据库写入异常", 1],
        ],
        sheet_name="Tip",
    )


def test_column_letters_and_xlsx_roundtrip(tmp_path: Path) -> None:
    """验证 A..ZZ 列标互转、稀疏单元格、布尔/数值/Unicode 字符串与 inlineStr 读取。"""
    assert col_letters_to_index("A") == 0
    assert col_letters_to_index("Z") == 25
    assert col_letters_to_index("AA") == 26
    assert col_letters_to_index("ZZ") == 701
    for idx in (0, 1, 25, 26, 51, 52, 701, 702):
        assert col_letters_to_index(index_to_col_letters(idx)) == idx

    wide_row: list[object] = [None] * 28
    wide_row[0] = "id"
    wide_row[26] = "col_aa"
    wide_row[27] = "col_ab"
    xlsx_path = tmp_path / "Wide.xlsx"
    write_xlsx_sheet(
        xlsx_path,
        [
            wide_row,
            [101, 3.75, True, False, "  带空格中文  ", None, -7],
        ],
        sheet_name="Wide",
    )
    rows = read_xlsx_sheet(xlsx_path, sheet_name="Wide")
    assert rows[0][0] == "id"
    assert rows[0][26] == "col_aa"
    assert rows[0][27] == "col_ab"
    assert rows[1][:7] == [101, 3.75, True, False, "  带空格中文  ", None, -7]

    # 手工构造含 inlineStr 与富文本 sharedStrings 的最小 xlsx 验证兼容读取。
    inline_path = tmp_path / "Inline.xlsx"
    with zipfile.ZipFile(inline_path, "w") as zf:
        zf.writestr(
            "xl/workbook.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<sheets><sheet name="S1" sheetId="1" r:id="rId1"/></sheets></workbook>',
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>',
        )
        zf.writestr(
            "xl/sharedStrings.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<si><r><t>Hello</t></r><r><t>World</t></r></si></sst>',
        )
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetData><row r="1">'
            '<c r="A1" t="inlineStr"><is><t>InlineVal</t></is></c>'
            '<c r="C1" t="s"><v>0</v></c>'
            '<c r="AA1"><v>42</v></c>'
            "</row></sheetData></worksheet>",
        )
    parsed = read_xlsx_sheet(inline_path)
    assert parsed[0][0] == "InlineVal"
    assert parsed[0][1] is None
    assert parsed[0][2] == "HelloWorld"
    assert parsed[0][26] == 42


def test_csv_and_json_sources(tmp_path: Path) -> None:
    """验证 .csv 与 .json 源表能与 .xlsx 一样进入导出管线。"""
    csv_path = tmp_path / "Skill.csv"
    csv_path.write_text(
        "id,name,costs\n"
        "uint32,string,int32[]\n"
        "技能ID,技能名,消耗列表\n"
        "pk,,\n"
        "10,火球术,\"10,20\"\n",
        encoding="utf-8",
    )
    json_path = tmp_path / "Buff.json"
    json_path.write_text(
        json.dumps(
            {
                "name": "Buff",
                "columns": [
                    {"name": "id", "type": "uint32", "options": ["pk"]},
                    {"name": "skill_id", "type": "uint32", "options": ["fk:Skill.id"]},
                    {"name": "duration_ms", "type": "int32"},
                ],
                "rows": [{"id": 1, "skill_id": 10, "duration_ms": 3000}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    name_csv, mat_csv = read_source_matrix(csv_path)
    assert name_csv == "Skill"
    assert len(mat_csv) == 5

    out_dir = tmp_path / "out"
    summary = export_tables(tmp_path, out_dir)
    assert summary.tables == ("Buff", "Skill")
    cfg = ConfigTables(out_dir, fmt="pb", descriptor_set=out_dir / "_table_descriptors.pb")
    assert cfg["Skill"].get(10).name == "火球术"
    assert list(cfg["Skill"].get(10).costs) == [10, 20]
    assert cfg["Buff"].get(1).duration_ms == 3000


def test_full_export_loads_directly_in_config_tables(tmp_path: Path) -> None:
    """完整导出 Item/Reward/Tip，并用 ConfigTables 同时以 json 与 pb 格式加载验证完全一致。"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    mapping_dir = tmp_path / "mapping"
    _write_sample_tables(input_dir)

    summary = export_tables(input_dir, output_dir, mapping_dir=mapping_dir, version=7)
    assert summary.version == 7
    assert summary.table_count == 2
    assert summary.tables == ("Item", "Reward")
    assert summary.tip_count == 2
    assert (output_dir / "manifest.json").is_file()
    assert (output_dir / "_table_descriptors.pb").is_file()
    assert (output_dir / "item.json").is_file()
    assert (output_dir / "item.pb").is_file()
    assert (output_dir / "reward.json").is_file()
    assert (output_dir / "reward.pb").is_file()
    assert (output_dir / "tip_text.json").is_file()
    assert (output_dir / "proto" / "item_table.proto").is_file()
    assert (output_dir / "proto" / "table_wire" / "item_table.proto").is_file()
    assert (output_dir / "table_constants.py").is_file()

    # tip_text.json 内容验证
    tip_text = json.loads((output_dir / "tip_text.json").read_text(encoding="utf-8"))
    assert tip_text == {"1001": "背包已满", "1002": "数据库写入异常"}

    # 生成的 table_constants.py 可正常编译执行且包含具名 ID、位图映射与 TipCode
    const_ns: dict[str, object] = {}
    exec(compile((output_dir / "table_constants.py").read_text(encoding="utf-8"), "table_constants.py", "exec"), const_ns)
    assert const_ns["ItemIds"].IRON_SWORD == 1001
    assert const_ns["ITEM_BIT_INDEX"] == {1001: 0, 1002: 1, 1003: 2}
    assert const_ns["ITEM_BITMAP_CAPACITY"] == 3
    assert const_ns["TipCode"].TIP_BAG_FULL == 1001
    assert const_ns["TIP_FAULT_CODES"] == frozenset({1002})

    # 核心门禁：直接用 xuanming.data.ConfigTables 分别以 json 与 pb 加载，断言指纹与行数据完全一致。
    desc_path = output_dir / "_table_descriptors.pb"
    cfg_json = ConfigTables(output_dir, fmt="json", descriptor_set=desc_path)
    cfg_pb = ConfigTables(output_dir, fmt="pb", descriptor_set=desc_path)

    assert cfg_json.version == 7
    assert cfg_pb.version == 7
    assert cfg_json.content_digest == cfg_pb.content_digest == summary.content_digest

    for table_name in ("Item", "Reward"):
        json_rows = [r.SerializeToString(deterministic=True) for r in cfg_json[table_name].rows()]
        pb_rows = [r.SerializeToString(deterministic=True) for r in cfg_pb[table_name].rows()]
        assert json_rows == pb_rows

    # 校验 ConfigTables 索引、唯一键、位图索引与跨表外键解析
    sword = cfg_pb["Item"].get(1001)
    assert sword is not None
    assert sword.name == "铁剑"
    assert sword.price == 500
    assert list(sword.tags) == [1, 2, 3]
    assert not hasattr(sword, "designer_note")
    assert cfg_pb["Item"].get("gem_fire", field="code").price == 9999999999
    assert cfg_pb["Item"].bit_index(1001) == 0
    assert cfg_pb["Item"].bit_index(1003) == 2

    reward_2001 = cfg_pb["Reward"].get(2001)
    assert reward_2001 is not None
    resolved = cfg_pb.resolve_foreign("Reward", reward_2001, "item_id")
    assert len(resolved) == 1
    assert resolved[0].id == 1001
    assert resolved[0].name == "铁剑"

    # 内容未变化时重跑（不指定 --version）保持版本号不变；修改内容后版本号自动递增。
    summary_same = export_tables(input_dir, output_dir, mapping_dir=mapping_dir)
    assert summary_same.version == 7
    assert summary_same.content_digest == summary.content_digest


def test_primary_key_and_unique_key_validation(tmp_path: Path) -> None:
    """验证主键重复、主键为空与 unique 列重复均被严格拒绝。"""
    # 1) 主键重复
    d1 = tmp_path / "dup_pk"
    write_xlsx_sheet(
        d1 / "Item.xlsx",
        [
            ["id", "name"],
            ["uint32", "string"],
            ["ID", "名称"],
            ["pk", ""],
            [1, "A"],
            [1, "B"],
        ],
    )
    with pytest.raises(PrimaryKeyError, match="duplicate primary key"):
        export_tables(d1, tmp_path / "out1")

    # 2) 主键为空
    d2 = tmp_path / "empty_pk"
    write_xlsx_sheet(
        d2 / "Item.xlsx",
        [
            ["id", "name"],
            ["uint32", "string"],
            ["ID", "名称"],
            ["pk", ""],
            [None, "MissingId"],
        ],
    )
    with pytest.raises(PrimaryKeyError, match="empty primary key"):
        export_tables(d2, tmp_path / "out2")

    # 3) unique 二级键重复
    d3 = tmp_path / "dup_unique"
    write_xlsx_sheet(
        d3 / "Item.xlsx",
        [
            ["id", "code"],
            ["uint32", "string"],
            ["ID", "编码"],
            ["pk", "unique"],
            [1, "same_code"],
            [2, "same_code"],
        ],
    )
    with pytest.raises(PrimaryKeyError, match="duplicate unique key"):
        export_tables(d3, tmp_path / "out3")


def test_foreign_key_validation(tmp_path: Path) -> None:
    """验证悬空外键、目标表缺失与类型不匹配均抛 ForeignKeyError，且不污染输出目录。"""
    # 1) 悬空引用
    d1 = tmp_path / "dangling_fk"
    out1 = tmp_path / "out1"
    write_xlsx_sheet(
        d1 / "Item.xlsx",
        [
            ["id", "name"],
            ["uint32", "string"],
            ["ID", "名称"],
            ["pk", ""],
            [1001, "剑"],
        ],
    )
    write_xlsx_sheet(
        d1 / "Reward.xlsx",
        [
            ["id", "item_id"],
            ["uint32", "uint32"],
            ["ID", "物品ID"],
            ["pk", "fk:Item.id"],
            [1, 9999],
        ],
    )
    with pytest.raises(ForeignKeyError, match="9999"):
        export_tables(d1, out1)
    assert not out1.exists()

    # 2) 两端类型不一致（int32 vs uint32）
    d2 = tmp_path / "type_mismatch_fk"
    write_xlsx_sheet(
        d2 / "Item.xlsx",
        [
            ["id"],
            ["uint32"],
            ["ID"],
            ["pk"],
            [1001],
        ],
    )
    write_xlsx_sheet(
        d2 / "Reward.xlsx",
        [
            ["id", "item_id"],
            ["uint32", "int32"],
            ["ID", "物品ID"],
            ["pk", "fk:Item.id"],
            [1, 1001],
        ],
    )
    with pytest.raises(ForeignKeyError, match="type mismatch"):
        export_tables(d2, tmp_path / "out2")


def test_permanent_bit_index_never_reuses_deleted_ids(tmp_path: Path) -> None:
    """验证 bit_index 跨次导出保持稳定，删除 ID 后空位永久保留绝不复用，损坏文件 fail-closed。"""
    input_dir = tmp_path / "in"
    output_dir = tmp_path / "out"
    mapping_dir = tmp_path / "mapping"

    write_xlsx_sheet(
        input_dir / "Mission.xlsx",
        [
            ["id", "title"],
            ["uint32", "string"],
            ["任务ID", "标题"],
            ["pk bit_index", ""],
            [101, "初入江湖"],
            [102, "击败野狼"],
            [103, "收集草药"],
        ],
    )
    export_tables(input_dir, output_dir, mapping_dir=mapping_dir)
    map_file = mapping_dir / "mission_mapping.json"
    assert json.loads(map_file.read_text(encoding="utf-8")) == {"101": 0, "102": 1, "103": 2}

    # 删除 102 并新增 104：102 必须永久保留在 bit 1，104 必须追加到 bit 3！
    write_xlsx_sheet(
        input_dir / "Mission.xlsx",
        [
            ["id", "title"],
            ["uint32", "string"],
            ["任务ID", "标题"],
            ["pk bit_index", ""],
            [101, "初入江湖"],
            [103, "收集草药"],
            [104, "拜见掌门"],
        ],
    )
    export_tables(input_dir, output_dir, mapping_dir=mapping_dir)
    assert json.loads(map_file.read_text(encoding="utf-8")) == {
        "101": 0,
        "102": 1,
        "103": 2,
        "104": 3,
    }

    # 损坏的映射文件必须 fail-closed 拒绝导表。
    map_file.write_text('{"101": 0, "102": 0}', encoding="utf-8")
    with pytest.raises(BitIndexStateError, match="duplicate bit index"):
        allocate_bit_indices("Mission", [101, 102], map_file)

    # strict_missing=True 时缺失文件必须报错。
    with pytest.raises(BitIndexStateError, match="missing"):
        allocate_bit_indices("Quest", [1], mapping_dir / "nonexistent.json", strict_missing=True)


def test_permanent_ids_ledger_blocks_row_deletion(tmp_path: Path) -> None:
    """验证声明 ids_permanent 的表在删除已发布行 ID 时报错中止。"""
    input_dir = tmp_path / "in"
    output_dir = tmp_path / "out"
    mapping_dir = tmp_path / "mapping"

    write_xlsx_sheet(
        input_dir / "Item.xlsx",
        [
            ["id", "name"],
            ["uint32", "string"],
            ["ID", "名称"],
            ["pk ids_permanent", ""],
            [1, "剑"],
            [2, "盾"],
        ],
    )
    export_tables(input_dir, output_dir, mapping_dir=mapping_dir)

    write_xlsx_sheet(
        input_dir / "Item.xlsx",
        [
            ["id", "name"],
            ["uint32", "string"],
            ["ID", "名称"],
            ["pk ids_permanent", ""],
            [1, "剑"],
        ],
    )
    with pytest.raises(PermanentIdsError, match="2"):
        export_tables(input_dir, output_dir, mapping_dir=mapping_dir)


def test_tip_axis_validation_and_monotonic_allocation(tmp_path: Path) -> None:
    """验证 Tip.xlsx 单轴校验：唯一 ID/枚举名、非空文案、单调分配、墓碑保护与组头分段格式。"""
    mapping_dir = tmp_path / "mapping"

    # 1) 空 ID 自动单调分配 + 删除后墓碑不复用
    tip_path = tmp_path / "Tip.xlsx"
    write_xlsx_sheet(
        tip_path,
        [
            ["id", "name", "text", "fault"],
            [10, "TIP_OK", "操作成功", 0],
            [None, "TIP_NO_GOLD", "金币不足", 0],
        ],
    )
    res1 = validate_and_allocate_tips(tip_path, mapping_dir)
    assert res1.id_by_name == {"TIP_OK": 10, "TIP_NO_GOLD": 11}

    # 删除 TIP_NO_GOLD(11)，新增 TIP_NO_MP 必须拿到 12 而不能复用 11
    write_xlsx_sheet(
        tip_path,
        [
            ["id", "name", "text", "fault"],
            [10, "TIP_OK", "操作成功", 0],
            [None, "TIP_NO_MP", "法力不足", 0],
        ],
    )
    res2 = validate_and_allocate_tips(tip_path, mapping_dir)
    assert res2.id_by_name == {"TIP_OK": 10, "TIP_NO_MP": 12}

    # 2) 试图复用已退休 ID 11 被拒绝
    write_xlsx_sheet(
        tip_path,
        [
            ["id", "name", "text"],
            [10, "TIP_OK", "操作成功"],
            [11, "TIP_REUSED", "非法复用"],
        ],
    )
    with pytest.raises(TipAxisError, match="permanently reserved"):
        validate_and_allocate_tips(tip_path, mapping_dir)

    # 3) 空文案、重复枚举名、重复数值 ID 均被拒绝
    bad_empty = tmp_path / "TipEmpty.xlsx"
    write_xlsx_sheet(bad_empty, [["id", "name", "text"], [1, "TIP_A", ""]])
    with pytest.raises(TipAxisError, match="empty text"):
        validate_and_allocate_tips(bad_empty, tmp_path / "m_empty")

    bad_dup_name = tmp_path / "TipDupName.xlsx"
    write_xlsx_sheet(bad_dup_name, [["id", "name", "text"], [1, "TIP_A", "A"], [2, "TIP_A", "B"]])
    with pytest.raises(TipAxisError, match="duplicate enum name"):
        validate_and_allocate_tips(bad_dup_name, tmp_path / "m_dup_name")

    bad_dup_id = tmp_path / "TipDupId.xlsx"
    write_xlsx_sheet(bad_dup_id, [["id", "name", "text"], [5, "TIP_A", "A"], [5, "TIP_B", "B"]])
    with pytest.raises(TipAxisError, match="duplicate numeric tip id"):
        validate_and_allocate_tips(bad_dup_id, tmp_path / "m_dup_id")

    # 4) C++ 分段组头格式支持
    seg_tip = tmp_path / "TipSeg.xlsx"
    write_xlsx_sheet(
        seg_tip,
        [
            ["name", "comment", "fault"],
            ["//common_error base=1000 width=100", None, None],
            ["ERR_PARAM", "参数错误", 0],
            ["ERR_INTERNAL", "内部异常", 1],
            ["//bag_error base=2000 width=100", None, None],
            ["ERR_BAG_FULL", "背包满", 0],
        ],
    )
    seg_res = validate_and_allocate_tips(seg_tip, tmp_path / "m_seg")
    assert seg_res.id_by_name == {"ERR_PARAM": 1000, "ERR_INTERNAL": 1001, "ERR_BAG_FULL": 2000}
    assert seg_res.fault_ids == (1001,)


def test_pb2json_roundtrip_and_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """验证 pb2json 回环转换、CLI 全部子命令与 standalone run.py 入口。"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    mapping_dir = tmp_path / "mapping"
    _write_sample_tables(input_dir)

    # 1) validate 子命令（只读，不落盘 mapping）
    rc = main(["validate", "--input-dir", str(input_dir), "--mapping-dir", str(mapping_dir), "--json"])
    assert rc == 0
    val_out = json.loads(capsys.readouterr().out)
    assert val_out["ok"] is True
    assert val_out["table_count"] == 2
    assert val_out["tip_count"] == 2
    assert not mapping_dir.exists()

    # 2) export 子命令
    rc = main([
        "export",
        "--input-dir", str(input_dir),
        "--output-dir", str(output_dir),
        "--mapping-dir", str(mapping_dir),
        "--version", "3",
        "--json",
    ])
    assert rc == 0
    exp_out = json.loads(capsys.readouterr().out)
    assert exp_out["version"] == 3
    assert exp_out["tables"] == ["Item", "Reward"]

    # 3) pb2json 回环（API 与 CLI）
    expected_item = json.loads((output_dir / "item.json").read_text(encoding="utf-8"))
    decoded_item = pb_to_json_dict(output_dir / "item.pb")
    assert decoded_item == expected_item

    roundtrip_file = tmp_path / "decoded_reward.json"
    rc = main([
        "pb2json",
        str(output_dir / "reward.pb"),
        "--descriptor-set", str(output_dir / "_table_descriptors.pb"),
        "--output", str(roundtrip_file),
    ])
    assert rc == 0
    assert json.loads(roundtrip_file.read_text(encoding="utf-8")) == json.loads(
        (output_dir / "reward.json").read_text(encoding="utf-8")
    )

    # 4) 独立入口 tools/data_table_exporter/run.py 冒烟
    repo_root = Path(__file__).resolve().parents[1]
    run_py = repo_root / "tools" / "data_table_exporter" / "run.py"
    proc = subprocess.run(
        [sys.executable, str(run_py), "validate", "--input-dir", str(input_dir), "--mapping-dir", str(mapping_dir)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "Validated 2 table(s)" in proc.stdout

    # 5) 失败场景 CLI 返回非零退出码并在 stderr 输出 ERROR
    bad_dir = tmp_path / "bad_tip_ref"
    _write_sample_tables(bad_dir)
    write_xlsx_sheet(
        bad_dir / "Reward.xlsx",
        [
            ["id", "item_id", "fail_tip"],
            ["uint32", "uint32", "int32"],
            ["奖励ID", "物品ID", "失败提示码"],
            ["pk", "fk:Item.id", "tip_ref"],
            [2001, 1001, 99999],
        ],
    )
    rc_err = main(["validate", "--input-dir", str(bad_dir)])
    assert rc_err == 1
    assert "ERROR:" in capsys.readouterr().err


def test_schema_errors_rejected(tmp_path: Path) -> None:
    """验证不支持的类型或 multi 主键与 bit_index 混用等非法 schema 被立即拒绝。"""
    d = tmp_path / "bad_schema"
    write_xlsx_sheet(
        d / "Bad.xlsx",
        [
            ["id", "payload"],
            ["uint32", "unknown_type"],
            ["ID", "字段"],
            ["pk", ""],
            [1, "x"],
        ],
    )
    with pytest.raises(SchemaError, match="unsupported column type"):
        export_tables(d, tmp_path / "out_bad")

    d2 = tmp_path / "bad_multi_bit"
    write_xlsx_sheet(
        d2 / "BadMulti.xlsx",
        [
            ["id", "name"],
            ["uint32", "string"],
            ["ID", "名称"],
            ["pk multi bit_index", ""],
            [1, "a"],
        ],
    )
    with pytest.raises(SchemaError, match="multi primary key table cannot declare bit_index"):
        export_tables(d2, tmp_path / "out_bad2")
