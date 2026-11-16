from importlib.resources import files
from dataclasses import replace
import hashlib
from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables
from xuanming.data.navigation import AuthoredNavBounds, NavigationMesh, load_navigation


def test_original_maps_snap_spawns_and_share_identical_config_paths():
    tables = ConfigTables()
    meshes = load_navigation(tables)
    rows = [row for row in tables["BaseScene"].rows() if row.nav_bin_file]
    assert set(meshes) == {row.id for row in rows} == set(range(1, 22))
    assert meshes[1] is meshes[16] and meshes[17] is meshes[19]
    assert meshes[1] is not meshes[2]
    for row in rows:
        spawn = row.spawn_x, row.spawn_y, row.spawn_z
        assert meshes[row.id].snap(spawn) == spawn


def test_original_main_mesh_blocks_wall_and_routes_around_it():
    main = load_navigation(ConfigTables())[1]
    start = (133.0, 199.0, 0.0)
    assert main.snap((133.0, 199.0, 2.0)) == start
    assert main.snap((133.0, 199.0, 4.0)) is None
    assert main.validate_move((0.0, 0.0, 0.0), start) == (False, None)

    # This segment crosses the native tile seam x=154.01 at ground level.
    assert main.validate_move(start, (160.0, 199.0, 1.0)) == (True, (160.0, 199.0, 0.0))
    allowed, position = main.validate_move(start, (200.0, 199.0, 0.0))
    assert not allowed and position == pytest.approx((177.96, 199.0, 0.0))

    target = (95.0, 80.0, 0.0)
    assert not main.validate_move(start, target)[0]
    path = main.find_path(start, target)
    assert path[0] == start and path[-1] == target and len(path) > 2
    assert all(main.validate_move(a, b) == (True, b) for a, b in zip(path, path[1:]))
    assert main.find_path(start, (200.0, 199.0, 0.0)) is None


def test_mset_loader_rejects_truncation_and_unsupported_tile_layout():
    valid = files("xuanming.data").joinpath("nav", "main_scene.bin").read_bytes()
    with pytest.raises(ValueError, match="tile range"):
        NavigationMesh.from_bytes(valid[:-1])
    layered = bytearray(valid)
    layered[114] = 1  # MSET header + first tile header + Detour version.
    with pytest.raises(ValueError, match="flat ground"):
        NavigationMesh.from_bytes(layered)


def test_yuexi_bay_routes_and_mask_geometry():
    mesh = load_navigation(ConfigTables())[3]
    spawn = (36.73125, 20.19375, 0.0)
    for dock in ((44.4375, 59.34375, 0.0), (21.88125, 48.15, 0.0)):
        path = mesh.find_path(spawn, dock)
        assert path and path[0] == spawn and path[-1] == dock
        assert all(mesh.validate_move(a, b)[0] for a, b in zip(path, path[1:]))
    assert mesh.snap((spawn[0], 75.0, 0.0)) is None

    mask = mesh.mask
    bitmap = bytearray(mask.packed)
    bitmap[0] ^= 0x80
    mesh.mask = replace(mask, packed=bytes(bitmap))
    with pytest.raises(ValueError, match="does not match"):
        mesh.verify_mask_geometry()


def test_authored_navigation_contract_load_and_limits(tmp_path, monkeypatch):
    from xuanming.data import navigation

    root = tmp_path / "nav" / "authored_maps" / "sample"
    root.mkdir(parents=True)
    mesh_data = files("xuanming.data").joinpath("nav", "main_scene.bin").read_bytes()
    bounds_data = b"ANAV 1\n0 0 300 300 0\n"
    (root / "scene.bin").write_bytes(mesh_data)
    (root / "scene.bin.authored").write_bytes(bounds_data)
    (root / "scene.bin.sha256").write_text(
        hashlib.sha256(mesh_data).hexdigest() + "\n" + hashlib.sha256(bounds_data).hexdigest() + "\n"
    )
    monkeypatch.setattr(navigation, "files", lambda name: tmp_path)
    row = SimpleNamespace(id=1, nav_bin_file="data/scene_nav_bin/authored_maps/sample/scene.bin",
                          spawn_x=133., spawn_y=199., spawn_z=0.)
    tables = {"BaseScene": SimpleNamespace(rows=lambda: [row])}
    mesh = load_navigation(tables)[1]
    spawn = (133., 199., 0.)
    assert mesh.snap(spawn) == spawn
    assert mesh.snap((133., 300., 0.)) is None
    assert mesh.validate_move(spawn, (133., 300., 0.)) == (False, spawn)
    assert mesh.snap((133., 199., 0.1)) is None
    assert mesh.find_path(spawn, (133., 300., 0.)) is None

    (root / "scene.bin.mask").write_bytes(b"old mask")
    assert not load_navigation(tables)
    (root / "scene.bin.mask").unlink()
    (root / "scene.bin.authored").write_bytes(b"ANAV 1\n0 0 300 300 1\n")
    assert not load_navigation(tables)

    for invalid in (b"ANAV 1\n0 0 0 300 0\n", b"ANAV 1\n0 0 300 300 1\n"):
        with pytest.raises(ValueError):
            AuthoredNavBounds.from_bytes(invalid)
