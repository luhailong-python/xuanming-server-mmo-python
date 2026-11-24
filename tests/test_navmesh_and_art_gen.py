"""纯 Python 导航网格烘焙器（navmesh_baker）与战斗美术生成器（battle_art_gen）单测。"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pytest

from xuanming.data.navigation import AuthoredNavBounds, NavigationMesh, NavMask
from xuanming.ops import battle_art_gen, navmesh_baker

REPO_NAV_DIR = Path(__file__).resolve().parents[1] / "src" / "xuanming" / "data" / "nav"


# ---------------------------------------------------------------------------
# Part 1: navmesh_baker 测试
# ---------------------------------------------------------------------------


def test_bake_authored_flat_map_loadable_and_pathfinding(tmp_path: Path) -> None:
    """烘焙带矩形与多边形障碍的跨 tile 平面地图，直连 xuanming.data.navigation 寻路验证。"""
    # 16x12 格（每格 2m -> Unity 32m x 24m），tile_cells=8 强制跨 2x2 tiles。
    # 在 Unity x∈[14, 18], z∈[4, 20] 放置一道纵向障碍墙，并在左上角放置多边形柱体障碍。
    spec = navmesh_baker.NavGridSpec(
        map_id="99001",
        columns=16,
        rows=12,
        cell_size=2.0,
        min_x=0.0,
        min_z=0.0,
        tile_cells=8,
        blocked_rects=((14.0, 4.0, 18.0, 20.0),),
        obstacle_polygons=(((2.0, 18.0), (6.0, 18.0), (6.0, 22.0), (2.0, 22.0)),),
        spawn=(4.0, 0.0, 12.0),
        probes=((28.0, 0.0, 12.0),),
        contract="authored_flat",
    )

    out_dir = tmp_path / "baked_99001"
    result = navmesh_baker.bake_authored_flat_map(spec, out_dir)

    assert result.tile_count == 4
    assert result.blocked_cells > 0
    assert result.walkable_cells + result.blocked_cells == 16 * 12

    bin_path = Path(result.bin_path)
    authored_path = Path(result.companion_path)
    sha_path = Path(result.sha256_path)
    meta_path = Path(result.metadata_path)
    assert bin_path.is_file()
    assert authored_path.is_file()
    assert sha_path.is_file()
    assert meta_path.is_file()
    assert not (out_dir / "scene.bin.mask").exists()

    # 验证 SHA-256 双摘要配对与 xuanming.data.navigation 原生格式一致。
    sha_lines = sha_path.read_text(encoding="ascii").split()
    assert sha_lines == [
        hashlib.sha256(bin_path.read_bytes()).hexdigest(),
        hashlib.sha256(authored_path.read_bytes()).hexdigest(),
    ]

    # 直接用 xuanming.data.navigation 加载，不走任何封装旁路。
    bounds = AuthoredNavBounds.from_bytes(authored_path.read_bytes())
    mesh = NavigationMesh.from_bytes(bin_path.read_bytes(), authored_bounds=bounds)

    # 1) 无遮挡直线寻路（server 坐标：(z, x, y)）。
    start_srv = (12.0, 4.0, 0.0)
    near_srv = (12.0, 10.0, 0.0)
    straight = navmesh_baker.query_path(mesh, start_srv, near_srv)
    assert straight.reachable is True
    assert straight.start_on_mesh is True
    assert straight.goal_on_mesh is True
    assert straight.direct_clear is True
    assert math.isclose(straight.distance, 6.0, abs_tol=1e-4)
    assert len(straight.waypoints) == 2

    # 2) 绕障寻路：从墙左侧 (z=12, x=4) 到墙右侧 (z=12, x=28)，必须绕过 z∈[4,20] 墙体。
    goal_srv = (12.0, 28.0, 0.0)
    detour = navmesh_baker.query_path(mesh, start_srv, goal_srv)
    assert detour.reachable is True
    assert detour.direct_clear is False
    assert detour.distance > math.dist(start_srv, goal_srv) + 1.0
    assert len(detour.waypoints) >= 3
    for wp in detour.waypoints:
        assert mesh.snap(wp) is not None

    # 3) 障碍内部不可达点 & snap 拒绝。
    blocked_srv = (12.0, 16.0, 0.0)
    snap_blocked = navmesh_baker.snap_point(mesh, blocked_srv)
    assert snap_blocked.on_mesh is False
    unreach = navmesh_baker.query_path(mesh, start_srv, blocked_srv)
    assert unreach.reachable is False
    assert unreach.waypoints == ()

    # 4) Unity 坐标查询模式验证。
    unity_q = navmesh_baker.query_path(
        mesh, (4.0, 0.0, 12.0), (10.0, 0.0, 12.0), coord_system="unity"
    )
    assert unity_q.reachable is True
    assert math.isclose(unity_q.distance, 6.0, abs_tol=1e-4)


def test_bake_versioned_mask_and_verify_geometry(tmp_path: Path) -> None:
    """烘焙 versioned_mask 契约地图并通过 NavMask 与 verify_mask_geometry 校验。"""
    spec = navmesh_baker.NavGridSpec(
        map_id="mask_test",
        columns=10,
        rows=10,
        cell_size=1.0,
        blocked_cells=((4, 4), (4, 5), (5, 4), (5, 5)),
        spawn=(1.5, 0.0, 1.5),
        contract="versioned_mask",
    )
    out_dir = tmp_path / "masked"
    result = navmesh_baker.bake_authored_flat_map(spec, out_dir)
    assert Path(result.companion_path).name == "scene.bin.mask"

    mask = NavMask.from_bytes(Path(result.companion_path).read_bytes())
    mesh = NavigationMesh.from_bytes(
        Path(result.bin_path).read_bytes(), mask=mask, versioned_mask=True
    )
    mesh.verify_mask_geometry()

    detour = navmesh_baker.query_path(mesh, (5.5, 1.5, 0.0), (5.5, 8.5, 0.0))
    assert detour.reachable is True
    assert detour.direct_clear is False


def test_verify_real_repo_nav_directory_and_parity() -> None:
    """对仓库真实 src/xuanming/data/nav/ 运行目录校验与 BaseScene 对账。"""
    assert REPO_NAV_DIR.is_dir()
    report = navmesh_baker.verify_nav_directory(REPO_NAV_DIR)
    assert report.ok is True, f"real nav dir verification failed: {report.errors}"
    assert report.checked_count >= 10
    assert report.failed_count == 0
    contracts = {entry.contract for entry in report.entries}
    assert {"legacy", "authored_flat", "versioned_mask"}.issubset(contracts)

    parity = navmesh_baker.verify_map_parity(REPO_NAV_DIR, check_tables=True)
    assert parity.ok is True, f"real nav parity failed: {parity.scene_errors}"
    assert parity.tables_checked is True
    assert parity.scene_count >= 1


def test_install_authored_map_and_tamper_rejection(tmp_path: Path) -> None:
    """验证原子安装收据，以及 SHA-256 篡改/残留 mask 的 fail-closed 拦截。"""
    spec = navmesh_baker.NavGridSpec(
        map_id="26099",
        columns=8,
        rows=8,
        cell_size=2.0,
        blocked_cells=((3, 3),),
        spawn=(2.0, 0.0, 2.0),
    )
    src = tmp_path / "stage"
    navmesh_baker.bake_authored_flat_map(spec, src)

    target_root = tmp_path / "authored_maps"
    receipt = navmesh_baker.install_authored_map(src, target_root, "26099")
    assert receipt.map_id == "26099"
    assert Path(receipt.receipt_path).is_file()
    assert "scene.bin" in receipt.files
    assert "scene.bin.authored" in receipt.files
    assert navmesh_baker.verify_nav_directory(target_root).ok is True

    # 篡改 SHA-256 后 verify 与 install 均必须拒绝。
    bad_src = tmp_path / "tampered"
    navmesh_baker.bake_authored_flat_map(spec, bad_src)
    (bad_src / "scene.bin.sha256").write_text("0" * 64 + "\n" + "0" * 64 + "\n", encoding="ascii")
    assert navmesh_baker.verify_nav_directory(bad_src).ok is False
    with pytest.raises(navmesh_baker.NavBakeError):
        navmesh_baker.install_authored_map(bad_src, target_root, "26098")


def test_navmesh_baker_cli_roundtrip(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """验证 navmesh_baker CLI 的 bake / query / verify / install --json 全链路。"""
    baked_dir = tmp_path / "cli_baked"
    rc = navmesh_baker.main(
        [
            "bake",
            "--map-id",
            "cli_map",
            "--out",
            str(baked_dir),
            "--cols",
            "10",
            "--rows",
            "10",
            "--cell-size",
            "2.0",
            "--blocked-rect",
            "8,4,12,16",
            "--spawn",
            "2,0,10",
            "--json",
        ]
    )
    assert rc == 0
    bake_json = json.loads(capsys.readouterr().out)
    assert bake_json["map_id"] == "cli_map"
    assert bake_json["blocked_cells"] > 0

    rc = navmesh_baker.main(
        [
            "query",
            "--nav",
            str(baked_dir / "scene.bin"),
            "--point",
            "10,2,0",
            "--start",
            "10,2,0",
            "--goal",
            "10,18,0",
            "--json",
        ]
    )
    assert rc == 0
    query_json = json.loads(capsys.readouterr().out)
    assert query_json["snap"]["on_mesh"] is True
    assert query_json["path"]["reachable"] is True
    assert query_json["path"]["direct_clear"] is False
    assert query_json["path"]["distance"] > 16.0

    rc = navmesh_baker.main(["verify", "--nav-dir", str(baked_dir), "--json"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True

    install_root = tmp_path / "cli_installed"
    rc = navmesh_baker.main(
        [
            "install",
            "--source",
            str(baked_dir),
            "--target",
            str(install_root),
            "--map-id",
            "cli_map",
            "--json",
        ]
    )
    assert rc == 0
    assert json.loads(capsys.readouterr().out)["map_id"] == "cli_map"


# ---------------------------------------------------------------------------
# Part 2: battle_art_gen 测试
# ---------------------------------------------------------------------------


def test_rgba_canvas_primitives_and_png_codec() -> None:
    """验证 RgbaCanvas 基础图元、描边、遮罩提取及 PNG 编解码/损坏校验。"""
    canvas = battle_art_gen.RgbaCanvas(32, 32)
    canvas.set_pixel(1, 1, (255, 0, 0, 255))
    assert canvas.get_pixel(1, 1) == (255, 0, 0, 255)

    canvas.blend_pixel(1, 1, (0, 0, 255, 128))
    r, g, b, a = canvas.get_pixel(1, 1)
    assert a == 255 and r > 0 and b > 0

    canvas.fill_rect(4, 4, 8, 8, (10, 200, 30, 255))
    canvas.fill_circle(22, 10, 5, (250, 180, 20, 255))
    canvas.fill_ellipse(16, 22, 7, 4, (80, 160, 240, 255), angle_deg=15.0)
    canvas.draw_line(2, 28, 29, 28, (255, 255, 255, 255), thickness=2.0)
    canvas.fill_polygon(((12, 12), (20, 12), (16, 18)), (220, 40, 180, 255))
    canvas.apply_outline((0, 0, 0, 255), thickness=1)

    mask = canvas.extract_mask(alpha_threshold=128)
    assert len(mask) == 32 and len(mask[0]) == 32
    assert mask[6][6] is True
    assert mask[0][31] is False

    png_bytes = canvas.encode_png()
    header = battle_art_gen.decode_png_header(png_bytes)
    assert header.width == 32
    assert header.height == 32
    assert header.bit_depth == 8
    assert header.color_type == 6
    assert header.chunk_types == ("IHDR", "IDAT", "IEND")

    decoded = battle_art_gen.decode_png_rgba(png_bytes)
    assert decoded.pixels == canvas.pixels

    # 篡改 CRC 必须被拒绝。
    corrupted = bytearray(png_bytes)
    corrupted[-1] ^= 0xFF
    with pytest.raises(battle_art_gen.BattleArtError):
        battle_art_gen.decode_png_header(bytes(corrupted))

    with pytest.raises(battle_art_gen.BattleArtError):
        battle_art_gen.decode_png_header(b"NOT_A_PNG_STREAM")


def test_battle_art_gen_all_categories_deterministic_and_verified(tmp_path: Path) -> None:
    """全量生成 7 类战斗美术资产，校验 PNG 结构、非空像素覆盖、怪物剪影与双次字节确定性。"""
    run1_dir = tmp_path / "run1"
    run2_dir = tmp_path / "run2"

    report1 = battle_art_gen.generate_all(run1_dir, verify=True)
    report2 = battle_art_gen.generate_all(run2_dir, verify=True)

    assert report1.categories == battle_art_gen.ALL_CATEGORIES
    assert set(report1.results.keys()) == set(battle_art_gen.ALL_CATEGORIES)
    assert report1.total_png_files == report2.total_png_files
    assert report1.total_png_files >= 70
    assert report1.verified is True
    assert report1.monster_verify is not None
    assert report1.monster_verify.ok is True
    assert report1.monster_verify.max_pair_iou <= battle_art_gen.MONSTER_MAX_SILHOUETTE_IOU

    # 逐文件验证：合法 PNG 魔数/IHDR/IDAT/IEND、非空不透明像素覆盖、与第二次运行逐字节一致。
    png_files1 = sorted(run1_dir.rglob("*.png"))
    png_files2 = sorted(run2_dir.rglob("*.png"))
    assert len(png_files1) == report1.total_png_files
    assert [p.relative_to(run1_dir) for p in png_files1] == [
        p.relative_to(run2_dir) for p in png_files2
    ]

    for p1, p2 in zip(png_files1, png_files2):
        b1 = p1.read_bytes()
        b2 = p2.read_bytes()
        assert b1 == b2, f"non-deterministic PNG output: {p1.relative_to(run1_dir)}"
        info = battle_art_gen.decode_png_header(b1)
        assert info.width > 0 and info.height > 0
        assert info.chunk_types[0] == "IHDR" and info.chunk_types[-1] == "IEND"
        decoded = battle_art_gen.decode_png_rgba(b1)
        assert decoded.opaque_pixel_count(alpha_threshold=8) > 0, f"empty PNG: {p1}"


def test_battle_art_gen_cli_only_and_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """验证 battle_art_gen CLI 的 --only、--verify 与 --json 参数。"""
    out_dir = tmp_path / "cli_art"
    rc = battle_art_gen.main(
        ["--out", str(out_dir), "--only", "monsters,digits,ui", "--verify", "--json"]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["categories"] == ["monsters", "digits", "ui"]
    assert payload["verified"] is True
    assert payload["monster_verify"]["ok"] is True
    assert (out_dir / "UI/digits_meta.json").is_file()
    assert (out_dir / "MONSTER_MANIFEST.json").is_file()
    assert not (out_dir / "Buff").exists()
