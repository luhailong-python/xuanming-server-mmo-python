"""纯 Python 导航网格烘焙、校验、寻路查询与原子安装工具。

职责与兼容性：
1. ``bake_authored_flat_map``：将可走网格/多边形障碍烘焙为与 ``xuanming.data.navigation``
   及服务端 UE5 Detour MSET v1 完全逐字节兼容的 ``scene.bin``，同时生成 ``.authored``（或
   ``.mask``）、``.sha256`` 双摘要配对文件与 ``metadata.json``；
2. ``verify_nav_directory`` / ``verify_map_parity``：校验目录内全部 ``.bin`` 的 SHA-256
   配对、边界、掩码几何一致性、连通性与可加载性，并支持与配置表 ``BaseScene`` 做对账；
3. ``snap_point`` / ``query_path``：基于 ``NavigationMesh`` 执行贴地采样与 A* 寻路，输出
   对齐 ``navmesh_query.cpp`` 的结构化结果（``reachable`` / ``distance`` / ``waypoints``）；
4. ``install_authored_map``：先在源目录完成完整校验，再通过临时文件 + ``os.replace``
   原子安装至目标目录，失败自动回滚并输出 SHA-256 收据。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import struct
import sys
import tempfile
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from xuanming.data.navigation import (
    DEFAULT_SPAWN,
    AuthoredNavBounds,
    NavigationMesh,
    NavMask,
    Point,
    navigation_contract,
)

logger = logging.getLogger(__name__)

# 与 xuanming.data.navigation / UE5 Detour MSET v1 完全对齐的二进制布局。
_MSET = struct.Struct("<3i4x9d2i")
_TILE = struct.Struct("<Qi4x")
_HEADER = struct.Struct("<4H2i7H2x6d")
_VERTEX = struct.Struct("<3d")
_POLY = struct.Struct("<I12HHBB")
_POLY_DETAIL = struct.Struct("<HHBB")
_MSET_MAGIC = 0x4D534554
_MSET_VERSION = 1
_DETOUR_TILE_VERSION = 7
_EXTERNAL_LINK = 0x8000
_SIDE_POS_X = 0
_SIDE_POS_Z = 2
_SIDE_NEG_X = 4
_SIDE_NEG_Z = 6
_MAX_FILE_BYTES = 64 * 1024 * 1024
_SNAP_TOLERANCE = 0.02
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAP_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_VALID_CONTRACTS = frozenset({"authored_flat", "versioned_mask", "exact_mask"})


class NavBakeError(ValueError):
    """导航烘焙、校验、查询或安装失败。"""


@dataclass(frozen=True)
class NavGridSpec:
    """平面导航网格烘焙规格（输入坐标采用 Unity Y-up：x 向东、z 向北、y 向上）。"""

    map_id: str
    columns: int
    rows: int
    cell_size: float = 1.0
    min_x: float = 0.0
    min_z: float = 0.0
    ground_y: float = 0.0
    # 行主序二维布尔表，row=0 位于 max_z 侧（与 FMASK / NavMask 约定一致）；None 表示全可走。
    walkable_grid: tuple[tuple[bool, ...], ...] | None = None
    blocked_cells: tuple[tuple[int, int], ...] = ()
    blocked_rects: tuple[tuple[float, float, float, float], ...] = ()
    obstacle_polygons: tuple[tuple[tuple[float, float], ...], ...] = ()
    spawn: tuple[float, float, float] | None = None
    probes: tuple[tuple[float, float, float], ...] = ()
    tile_cells: int = 32
    contract: str = "authored_flat"
    filename: str = "scene.bin"
    walkable_height: float = 1.8
    walkable_radius: float = 0.0
    walkable_climb: float = 0.35

    @property
    def width(self) -> float:
        return self.columns * self.cell_size

    @property
    def height(self) -> float:
        return self.rows * self.cell_size

    def validate(self) -> None:
        """校验烘焙规格的数值范围、标识合法性与几何约束。"""
        if not self.map_id or not _MAP_ID_RE.fullmatch(self.map_id):
            raise NavBakeError(f"invalid map_id: {self.map_id!r}")
        if not self.filename.endswith(".bin") or "/" in self.filename or "\\" in self.filename:
            raise NavBakeError(f"invalid nav filename: {self.filename!r}")
        if self.contract not in _VALID_CONTRACTS:
            raise NavBakeError(f"unsupported navigation contract: {self.contract!r}")
        if not (0 < self.columns <= 4096 and 0 < self.rows <= 4096):
            raise NavBakeError(f"grid dimensions out of range: {self.columns}x{self.rows}")
        if not (1 <= self.tile_cells <= 256):
            raise NavBakeError(f"tile_cells out of range: {self.tile_cells}")
        nums = (
            self.cell_size,
            self.min_x,
            self.min_z,
            self.ground_y,
            self.walkable_height,
            self.walkable_radius,
            self.walkable_climb,
        )
        if not all(math.isfinite(v) for v in nums):
            raise NavBakeError("nonfinite geometry or agent parameter")
        if self.cell_size <= 0.04:
            raise NavBakeError("cell_size must be > 0.04m to separate adjacent cells cleanly")
        if self.ground_y != 0.0:
            raise NavBakeError("authored flat maps require ground_y == 0.0")
        if self.walkable_height <= 0 or self.walkable_radius < 0 or self.walkable_climb < 0:
            raise NavBakeError("invalid agent dimensions")
        if self.walkable_grid is not None:
            if len(self.walkable_grid) != self.rows or any(
                len(row) != self.columns for row in self.walkable_grid
            ):
                raise NavBakeError("walkable_grid shape does not match rows x columns")


@dataclass(frozen=True)
class BakeResult:
    """导航烘焙输出清单与摘要信息。"""

    map_id: str
    contract: str
    bin_path: str
    companion_path: str
    sha256_path: str
    metadata_path: str
    bin_sha256: str
    companion_sha256: str
    tile_count: int
    poly_count: int
    walkable_cells: int
    blocked_cells: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "map_id": self.map_id,
            "contract": self.contract,
            "bin_path": self.bin_path,
            "companion_path": self.companion_path,
            "sha256_path": self.sha256_path,
            "metadata_path": self.metadata_path,
            "bin_sha256": self.bin_sha256,
            "companion_sha256": self.companion_sha256,
            "tile_count": self.tile_count,
            "poly_count": self.poly_count,
            "walkable_cells": self.walkable_cells,
            "blocked_cells": self.blocked_cells,
        }


@dataclass(frozen=True)
class SnapResult:
    """单点贴地查询结果。"""

    input: Point
    found: bool
    on_mesh: bool
    nearest: Point | None
    distance: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "input": list(self.input),
            "found": self.found,
            "on_mesh": self.on_mesh,
            "nearest": list(self.nearest) if self.nearest is not None else None,
            "distance": self.distance,
        }


@dataclass(frozen=True)
class PathQueryResult:
    """两点寻路查询结果（对齐 ``navmesh_query.cpp`` 输出语义）。"""

    start: Point
    goal: Point
    reachable: bool
    start_on_mesh: bool
    goal_on_mesh: bool
    distance: float
    waypoints: tuple[Point, ...]
    direct_clear: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": list(self.start),
            "goal": list(self.goal),
            "reachable": self.reachable,
            "start_on_mesh": self.start_on_mesh,
            "goal_on_mesh": self.goal_on_mesh,
            "distance": self.distance,
            "waypoints": [list(pt) for pt in self.waypoints],
            "direct_clear": self.direct_clear,
        }


@dataclass(frozen=True)
class NavFileReport:
    """单个导航文件的校验报告。"""

    relative_path: str
    contract: str
    ok: bool
    tile_count: int = 0
    poly_count: int = 0
    components: int = 0
    bin_sha256: str = ""
    companion_sha256: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "contract": self.contract,
            "ok": self.ok,
            "tile_count": self.tile_count,
            "poly_count": self.poly_count,
            "components": self.components,
            "bin_sha256": self.bin_sha256,
            "companion_sha256": self.companion_sha256,
            "error": self.error,
        }


@dataclass(frozen=True)
class NavVerifyReport:
    """导航目录全量校验报告。"""

    nav_dir: str
    ok: bool
    checked_count: int
    failed_count: int
    entries: tuple[NavFileReport, ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "nav_dir": self.nav_dir,
            "ok": self.ok,
            "checked_count": self.checked_count,
            "failed_count": self.failed_count,
            "entries": [entry.to_dict() for entry in self.entries],
            "errors": list(self.errors),
        }


@dataclass(frozen=True)
class NavParityReport:
    """导航目录与配置表（BaseScene）一致性对账报告。"""

    nav_dir: str
    ok: bool
    verify: NavVerifyReport
    tables_checked: bool
    scene_count: int
    scene_errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "nav_dir": self.nav_dir,
            "ok": self.ok,
            "verify": self.verify.to_dict(),
            "tables_checked": self.tables_checked,
            "scene_count": self.scene_count,
            "scene_errors": list(self.scene_errors),
        }


@dataclass(frozen=True)
class InstallReceipt:
    """原子安装手绘地图后的 SHA-256 收据。"""

    map_id: str
    source_dir: str
    target_dir: str
    contract: str
    files: dict[str, str]
    receipt_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "map_id": self.map_id,
            "source_dir": self.source_dir,
            "target_dir": self.target_dir,
            "contract": self.contract,
            "files": dict(self.files),
            "receipt_path": self.receipt_path,
        }


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _aligned(size: int) -> int:
    return (size + 7) & ~7


def _next_pow2(value: int) -> int:
    power = 1
    while power < value:
        power <<= 1
    return power


def _unity_to_server(point: Sequence[float]) -> Point:
    """Unity Y-up (x, y, z) -> 服务端 Z-up (z, x, y)。"""
    x, y, z = float(point[0]), float(point[1]), float(point[2])
    return (z, x, y)


def _server_to_unity(point: Sequence[float]) -> Point:
    """服务端 Z-up (sx, sy, sz) -> Unity Y-up (sy, sz, sx)。"""
    sx, sy, sz = float(point[0]), float(point[1]), float(point[2])
    return (sy, sz, sx)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """使用同目录临时文件 + ``os.replace`` 原子写入，避免留下半写文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".navbake-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def _point_in_polygon(px: float, pz: float, polygon: Sequence[tuple[float, float]]) -> bool:
    """射线法判定二维点 (px, pz) 是否位于多边形内部（含边上）。"""
    n = len(polygon)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        xi, zi = polygon[i]
        xj, zj = polygon[j]
        # 边上点直接视为命中障碍
        dx, dz = xj - xi, zj - zi
        seg_len_sq = dx * dx + dz * dz
        if seg_len_sq > 0:
            t = max(0.0, min(1.0, ((px - xi) * dx + (pz - zi) * dz) / seg_len_sq))
            if math.hypot(px - (xi + t * dx), pz - (zi + t * dz)) <= 1e-9:
                return True
        if (zi > pz) != (zj > pz):
            x_cross = xi + (pz - zi) * (xj - xi) / (zj - zi)
            if px < x_cross:
                inside = not inside
        j = i
    return inside


def _build_walkable_matrix(spec: NavGridSpec) -> list[list[bool]]:
    """根据规格生成行主序可走矩阵（row=0 对应 max_z 侧）。"""
    if spec.walkable_grid is not None:
        grid = [[bool(cell) for cell in row] for row in spec.walkable_grid]
    else:
        grid = [[True] * spec.columns for _ in range(spec.rows)]

    for col, row in spec.blocked_cells:
        if 0 <= col < spec.columns and 0 <= row < spec.rows:
            grid[row][col] = False

    if spec.blocked_rects or spec.obstacle_polygons:
        for row in range(spec.rows):
            cz = spec.min_z + (spec.rows - row - 0.5) * spec.cell_size
            for col in range(spec.columns):
                if not grid[row][col]:
                    continue
                cx = spec.min_x + (col + 0.5) * spec.cell_size
                for x0, z0, x1, z1 in spec.blocked_rects:
                    lo_x, hi_x = (x0, x1) if x0 <= x1 else (x1, x0)
                    lo_z, hi_z = (z0, z1) if z0 <= z1 else (z1, z0)
                    if lo_x <= cx <= hi_x and lo_z <= cz <= hi_z:
                        grid[row][col] = False
                        break
                if not grid[row][col]:
                    continue
                for poly in spec.obstacle_polygons:
                    if _point_in_polygon(cx, cz, poly):
                        grid[row][col] = False
                        break
    return grid


def _pack_mask_bytes(spec: NavGridSpec, grid: list[list[bool]]) -> bytes:
    """将可走矩阵编码为 ``xuanming.data.navigation.NavMask`` 兼容的 FMASK v1 文本字节。"""
    total = spec.columns * spec.rows
    packed = bytearray((total + 7) // 8)
    for row in range(spec.rows):
        base = row * spec.columns
        for col in range(spec.columns):
            if grid[row][col]:
                idx = base + col
                packed[idx >> 3] |= 0x80 >> (idx & 7)
    lines = [
        "FMASK 1",
        f"{spec.columns} {spec.rows}",
        " ".join(
            format(v, ".17g")
            for v in (spec.cell_size, spec.min_x, spec.min_z, spec.width, spec.height)
        ),
        " ".join(
            format(v, ".17g")
            for v in (spec.min_x, spec.min_z, spec.width, spec.height)
        ),
        packed.hex(),
    ]
    return ("\n".join(lines) + "\n").encode("ascii")


def _pack_authored_bounds(spec: NavGridSpec) -> bytes:
    """将地图外框编码为 ``AuthoredNavBounds`` 兼容的 ANAV v1 文本字节。"""
    values = (spec.min_x, spec.min_z, spec.width, spec.height, spec.ground_y)
    body = " ".join(format(v, ".17g") for v in values)
    return f"ANAV 1\n{body}\n".encode("ascii")


def _build_mset_binary(spec: NavGridSpec, grid: list[list[bool]]) -> tuple[bytes, int, int]:
    """将可走矩阵烘焙为 UE5 Detour MSET v1 二进制块。

    每个可走格生成一个水平凸四边形（在服务端 Z-up 坐标下逆时针绕序），同 tile 内共享边
    通过内部邻接索引连接，跨 tile 共享边通过 ``0x8000 | side`` 外部传送门连接。
    """
    cs = spec.cell_size
    tile_cells = spec.tile_cells
    tile_world = tile_cells * cs
    tiles_x = math.ceil(spec.columns / tile_cells)
    tiles_y = math.ceil(spec.rows / tile_cells)

    # 按 Detour tile 坐标 (tx, ty) 归类可走格：tx 沿 +nav_x，ty 沿 +nav_z。
    tile_buckets: dict[tuple[int, int], list[tuple[int, int]]] = {}
    for row in range(spec.rows):
        iz = spec.rows - 1 - row
        ty = iz // tile_cells
        for col in range(spec.columns):
            if not grid[row][col]:
                continue
            tx = col // tile_cells
            tile_buckets.setdefault((tx, ty), []).append((col, row))

    if not tile_buckets:
        raise NavBakeError("cannot bake navmesh: zero walkable cells")

    tile_payloads: list[tuple[int, bytes]] = []
    total_polys = 0
    max_poly_in_tile = 0

    for tile_idx, (tx, ty) in enumerate(sorted(tile_buckets.keys())):
        cells = tile_buckets[(tx, ty)]
        cell_to_local = {cell: idx for idx, cell in enumerate(cells)}
        poly_count = len(cells)
        total_polys += poly_count
        if poly_count > max_poly_in_tile:
            max_poly_in_tile = poly_count

        # 复用格点顶点，降低顶点数与文件体积。
        vertex_index: dict[tuple[int, int], int] = {}
        vertices: list[tuple[float, float, float]] = []

        def get_vertex(gx: int, gz: int) -> int:
            key = (gx, gz)
            idx = vertex_index.get(key)
            if idx is None:
                idx = len(vertices)
                vertex_index[key] = idx
                vertices.append((spec.min_x + gx * cs, spec.ground_y, spec.min_z + gz * cs))
            return idx

        polys_packed = bytearray()
        min_vx = math.inf
        min_vz = math.inf
        max_vx = -math.inf
        max_vz = -math.inf

        for col, row in cells:
            iz = spec.rows - 1 - row
            # 四角在服务端 (sx=nav_z, sy=nav_x) 下逆时针：
            # v0=(x0,z0), v1=(x0,z1), v2=(x1,z1), v3=(x1,z0)
            v0 = get_vertex(col, iz)
            v1 = get_vertex(col, iz + 1)
            v2 = get_vertex(col + 1, iz + 1)
            v3 = get_vertex(col + 1, iz)

            x0 = spec.min_x + col * cs
            x1 = spec.min_x + (col + 1) * cs
            z0 = spec.min_z + iz * cs
            z1 = spec.min_z + (iz + 1) * cs
            if x0 < min_vx:
                min_vx = x0
            if x1 > max_vx:
                max_vx = x1
            if z0 < min_vz:
                min_vz = z0
            if z1 > max_vz:
                max_vz = z1

            # 四条边依次为：
            # e0: v0->v1 (固定 x=x0, -X 侧, col-1)
            # e1: v1->v2 (固定 z=z1, +Z 侧, row-1)
            # e2: v2->v3 (固定 x=x1, +X 侧, col+1)
            # e3: v3->v0 (固定 z=z0, -Z 侧, row+1)
            neighbors = [0, 0, 0, 0, 0, 0]
            edge_specs = (
                (0, col - 1, row, _SIDE_NEG_X),
                (1, col, row - 1, _SIDE_POS_Z),
                (2, col + 1, row, _SIDE_POS_X),
                (3, col, row + 1, _SIDE_NEG_Z),
            )
            for edge_i, nc, nr, side in edge_specs:
                if not (0 <= nc < spec.columns and 0 <= nr < spec.rows):
                    continue
                if not grid[nr][nc]:
                    continue
                local_nei = cell_to_local.get((nc, nr))
                if local_nei is not None:
                    neighbors[edge_i] = local_nei + 1
                else:
                    neighbors[edge_i] = _EXTERNAL_LINK | side

            polys_packed += _POLY.pack(
                0,
                v0,
                v1,
                v2,
                v3,
                0,
                0,
                neighbors[0],
                neighbors[1],
                neighbors[2],
                neighbors[3],
                0,
                0,
                1,  # flags = 1 (可走)
                4,  # vertCount = 4
                0,  # areaAndtype = 0 (ground)
            )

        vertex_count = len(vertices)
        link_count = max(1, poly_count * 4)
        detail_count = poly_count
        detail_triangles = poly_count * 2
        bv_nodes = 0

        header_bytes = _HEADER.pack(
            _DETOUR_TILE_VERSION,
            0,
            poly_count,
            vertex_count,
            tx,
            ty,
            link_count,
            detail_count,
            0,
            detail_triangles,
            bv_nodes,
            0,
            poly_count,
            min_vx,
            spec.ground_y,
            min_vz,
            max_vx,
            spec.ground_y,
            max_vz,
        )

        verts_bytes = b"".join(_VERTEX.pack(*v) for v in vertices)
        links_bytes = bytes(link_count * 16)

        detail_meshes = bytearray()
        detail_tris = bytearray()
        for local_i in range(poly_count):
            detail_meshes += _POLY_DETAIL.pack(0, local_i * 2, 0, 2)
            detail_tris += bytes((0, 1, 2, 0, 0, 2, 3, 0))
        detail_meshes += bytes(_aligned(len(detail_meshes)) - len(detail_meshes))
        detail_tris += bytes(_aligned(len(detail_tris)) - len(detail_tris))

        payload = (
            header_bytes
            + verts_bytes
            + bytes(polys_packed)
            + links_bytes
            + bytes(detail_meshes)
            + bytes(detail_tris)
        )
        tile_ref = (1 << 32) | (tile_idx + 1)
        tile_payloads.append((tile_ref, payload))

    max_tiles = _next_pow2(max(1, tiles_x * tiles_y))
    max_polys = _next_pow2(max(16, max_poly_in_tile))
    mset_header = _MSET.pack(
        _MSET_MAGIC,
        _MSET_VERSION,
        len(tile_payloads),
        spec.walkable_height,
        spec.walkable_radius,
        spec.walkable_climb,
        1.0 / cs,
        spec.min_x,
        spec.ground_y,
        spec.min_z,
        tile_world,
        tile_world,
        max_tiles,
        max_polys,
    )

    out = bytearray(mset_header)
    for tile_ref, payload in tile_payloads:
        out += _TILE.pack(tile_ref, len(payload))
        out += payload
    return bytes(out), len(tile_payloads), total_polys


def bake_authored_flat_map(spec: NavGridSpec, output_dir: Path | str) -> BakeResult:
    """将 ``NavGridSpec`` 烘焙为可由 ``xuanming.data.navigation`` 直接加载的导航产物集。"""
    spec.validate()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    grid = _build_walkable_matrix(spec)
    walkable_cells = sum(1 for row in grid for cell in row if cell)
    blocked_cells = spec.columns * spec.rows - walkable_cells
    if walkable_cells == 0:
        raise NavBakeError("cannot bake navmesh: no walkable cells remain after obstacles")

    bin_bytes, tile_count, poly_count = _build_mset_binary(spec, grid)
    if spec.contract == "authored_flat":
        companion_suffix = ".authored"
        companion_bytes = _pack_authored_bounds(spec)
        authored_bounds = AuthoredNavBounds.from_bytes(companion_bytes)
        mask_obj = None
    else:
        companion_suffix = ".mask"
        companion_bytes = _pack_mask_bytes(spec, grid)
        authored_bounds = None
        mask_obj = NavMask.from_bytes(companion_bytes)

    # 写盘前先用正式运行时解析器自检，确保 100% 兼容且探针全通过。
    mesh = NavigationMesh.from_bytes(
        bin_bytes,
        mask=mask_obj,
        versioned_mask=(spec.contract == "versioned_mask"),
        authored_bounds=authored_bounds,
    )
    mesh.verify_mask_geometry()

    probes_to_check: list[tuple[float, float, float]] = list(spec.probes)
    if spec.spawn is not None:
        probes_to_check.insert(0, spec.spawn)
    for probe in probes_to_check:
        server_pt = _unity_to_server(probe)
        snapped = mesh.snap(server_pt)
        if snapped is None or any(
            abs(a - b) > _SNAP_TOLERANCE for a, b in zip(snapped, server_pt)
        ):
            raise NavBakeError(f"probe point is off baked navmesh (Unity): {probe}")

    bin_sha = _sha256_bytes(bin_bytes)
    comp_sha = _sha256_bytes(companion_bytes)
    sha_bytes = f"{bin_sha}\n{comp_sha}\n".encode("ascii")

    bin_path = out_dir / spec.filename
    companion_path = out_dir / (spec.filename + companion_suffix)
    sha256_path = out_dir / (spec.filename + ".sha256")
    metadata_path = out_dir / "metadata.json"

    # authored_flat 严禁残留 .mask 文件。
    if spec.contract == "authored_flat":
        stale_mask = out_dir / (spec.filename + ".mask")
        if stale_mask.exists():
            stale_mask.unlink()

    metadata = {
        "schemaVersion": 1,
        "mapId": spec.map_id,
        "contract": spec.contract,
        "filename": spec.filename,
        "companion": companion_path.name,
        "sha256File": sha256_path.name,
        "binSha256": bin_sha,
        "companionSha256": comp_sha,
        "grid": {
            "columns": spec.columns,
            "rows": spec.rows,
            "cellSize": spec.cell_size,
            "minX": spec.min_x,
            "minZ": spec.min_z,
            "width": spec.width,
            "height": spec.height,
            "groundY": spec.ground_y,
        },
        "spawnUnity": list(spec.spawn) if spec.spawn is not None else None,
        "spawnServer": list(_unity_to_server(spec.spawn)) if spec.spawn is not None else None,
        "stats": {
            "tileCount": tile_count,
            "polyCount": poly_count,
            "walkableCells": walkable_cells,
            "blockedCells": blocked_cells,
        },
    }
    metadata_bytes = (json.dumps(metadata, ensure_ascii=False, indent=2) + "\n").encode("utf-8")

    _atomic_write_bytes(bin_path, bin_bytes)
    _atomic_write_bytes(companion_path, companion_bytes)
    _atomic_write_bytes(sha256_path, sha_bytes)
    _atomic_write_bytes(metadata_path, metadata_bytes)

    logger.info(
        "Baked navmesh map_id=%s contract=%s tiles=%d polys=%d walkable=%d blocked=%d",
        spec.map_id,
        spec.contract,
        tile_count,
        poly_count,
        walkable_cells,
        blocked_cells,
    )
    return BakeResult(
        map_id=spec.map_id,
        contract=spec.contract,
        bin_path=str(bin_path),
        companion_path=str(companion_path),
        sha256_path=str(sha256_path),
        metadata_path=str(metadata_path),
        bin_sha256=bin_sha,
        companion_sha256=comp_sha,
        tile_count=tile_count,
        poly_count=poly_count,
        walkable_cells=walkable_cells,
        blocked_cells=blocked_cells,
    )


def _infer_contract_for_bin(bin_path: Path, nav_root: Path | None = None) -> str:
    """根据相对路径或伴随文件推断导航安全契约类型。"""
    if nav_root is not None:
        try:
            rel_parts = bin_path.resolve().relative_to(nav_root.resolve()).parts
        except ValueError:
            rel_parts = ()
        if rel_parts:
            head = rel_parts[0]
            if head == "authored_maps":
                return "authored_flat"
            if head == "current_maps":
                return "versioned_mask"
            if head == "festival_maps":
                return "exact_mask"
    if (bin_path.parent / (bin_path.name + ".authored")).is_file():
        return "authored_flat"
    if (bin_path.parent / (bin_path.name + ".mask")).is_file():
        return "versioned_mask"
    return "legacy"


def load_nav_map(
    path: Path | str,
    *,
    nav_root: Path | None = None,
    contract: str | None = None,
) -> NavigationMesh:
    """加载单个导航文件或地图目录，强制校验 SHA-256 配对与掩码几何一致性。"""
    target = Path(path)
    bin_path = target / "scene.bin" if target.is_dir() else target
    if not bin_path.is_file():
        raise NavBakeError(f"navigation binary not found: {bin_path}")

    resolved_contract = contract or _infer_contract_for_bin(bin_path, nav_root)
    payload = bin_path.read_bytes()
    if not (0 < len(payload) <= _MAX_FILE_BYTES):
        raise NavBakeError(f"invalid nav binary size: {len(payload)}")

    mask: NavMask | None = None
    authored_bounds: AuthoredNavBounds | None = None
    versioned_mask = resolved_contract == "versioned_mask"

    if resolved_contract in _VALID_CONTRACTS:
        parent = bin_path.parent
        if resolved_contract == "authored_flat" and (parent / (bin_path.name + ".mask")).exists():
            raise NavBakeError(f"authored navigation has a residual mask: {bin_path}")
        comp_suffix = ".authored" if resolved_contract == "authored_flat" else ".mask"
        comp_path = parent / (bin_path.name + comp_suffix)
        sha_path = parent / (bin_path.name + ".sha256")
        if not comp_path.is_file():
            raise NavBakeError(f"missing companion file: {comp_path}")
        if not sha_path.is_file():
            raise NavBakeError(f"missing sha256 pairing file: {sha_path}")

        comp_payload = comp_path.read_bytes()
        if not (0 < len(comp_payload) <= _MAX_FILE_BYTES):
            raise NavBakeError(f"invalid companion size: {len(comp_payload)}")
        sha_fields = sha_path.read_text(encoding="ascii").split()
        if len(sha_fields) != 2 or any(_SHA256_RE.fullmatch(item) is None for item in sha_fields):
            raise NavBakeError(f"malformed sha256 pairing file: {sha_path}")
        if _sha256_bytes(payload) != sha_fields[0] or _sha256_bytes(comp_payload) != sha_fields[1]:
            raise NavBakeError(f"navigation sha256 checksum mismatch: {bin_path}")

        if resolved_contract == "authored_flat":
            authored_bounds = AuthoredNavBounds.from_bytes(comp_payload)
        else:
            mask = NavMask.from_bytes(comp_payload)

    mesh = NavigationMesh.from_bytes(
        payload,
        mask=mask,
        versioned_mask=versioned_mask,
        authored_bounds=authored_bounds,
    )
    mesh.verify_mask_geometry()
    return mesh


def _resolve_mesh(nav_map: NavigationMesh | Path | str) -> NavigationMesh:
    if isinstance(nav_map, NavigationMesh):
        return nav_map
    return load_nav_map(nav_map)


def _count_polygon_components(mesh: NavigationMesh) -> int:
    """使用 BFS 统计导航网格可走多边形的连通分量个数。"""
    poly_count = len(mesh._polygons)
    visited = [False] * poly_count
    components = 0
    for idx in range(poly_count):
        if visited[idx] or not mesh._polygons[idx].flags:
            continue
        components += 1
        visited[idx] = True
        queue = deque([idx])
        while queue:
            cur = queue.popleft()
            for neighbor, _, _, _ in mesh._portals[cur]:
                if not visited[neighbor] and mesh._polygons[neighbor].flags:
                    visited[neighbor] = True
                    queue.append(neighbor)
    return components


def snap_point(
    nav_map: NavigationMesh | Path | str,
    point: Sequence[float],
    *,
    coord_system: str = "server",
) -> SnapResult:
    """将查询点吸附到导航网格上，并返回是否命中网格及偏移距离。"""
    if len(point) != 3 or not all(math.isfinite(float(v)) for v in point):
        raise NavBakeError(f"invalid 3D point: {point!r}")
    if coord_system not in {"server", "unity"}:
        raise NavBakeError(f"unsupported coord_system: {coord_system!r}")

    mesh = _resolve_mesh(nav_map)
    raw_pt: Point = (float(point[0]), float(point[1]), float(point[2]))
    server_pt = raw_pt if coord_system == "server" else _unity_to_server(raw_pt)
    snapped_server = mesh.snap(server_pt)
    if snapped_server is None:
        raw_near = mesh._nearest_raw(server_pt)
        if raw_near is None:
            return SnapResult(input=raw_pt, found=False, on_mesh=False, nearest=None, distance=None)
        near_server = raw_near[1]
        near_out = near_server if coord_system == "server" else _server_to_unity(near_server)
        return SnapResult(
            input=raw_pt,
            found=True,
            on_mesh=False,
            nearest=near_out,
            distance=math.dist(raw_pt, near_out),
        )

    near_out = snapped_server if coord_system == "server" else _server_to_unity(snapped_server)
    on_mesh = all(abs(a - b) <= _SNAP_TOLERANCE for a, b in zip(snapped_server, server_pt))
    return SnapResult(
        input=raw_pt,
        found=True,
        on_mesh=on_mesh,
        nearest=near_out,
        distance=math.dist(raw_pt, near_out),
    )


def query_path(
    nav_map: NavigationMesh | Path | str,
    start: Sequence[float],
    goal: Sequence[float],
    *,
    coord_system: str = "server",
) -> PathQueryResult:
    """在导航网格上执行起点/终点校验与 A* / Detour 寻路。"""
    if len(start) != 3 or len(goal) != 3:
        raise NavBakeError("start and goal must be 3D points")
    if coord_system not in {"server", "unity"}:
        raise NavBakeError(f"unsupported coord_system: {coord_system!r}")

    mesh = _resolve_mesh(nav_map)
    start_in: Point = (float(start[0]), float(start[1]), float(start[2]))
    goal_in: Point = (float(goal[0]), float(goal[1]), float(goal[2]))
    start_srv = start_in if coord_system == "server" else _unity_to_server(start_in)
    goal_srv = goal_in if coord_system == "server" else _unity_to_server(goal_in)

    start_snap = mesh.snap(start_srv)
    goal_snap = mesh.snap(goal_srv)
    start_on = start_snap is not None and all(
        abs(a - b) <= _SNAP_TOLERANCE for a, b in zip(start_snap, start_srv)
    )
    goal_on = goal_snap is not None and all(
        abs(a - b) <= _SNAP_TOLERANCE for a, b in zip(goal_snap, goal_srv)
    )
    direct_clear = bool(start_snap is not None and mesh.validate_move(start_srv, goal_srv)[0])

    path_srv = mesh.find_path(start_srv, goal_srv) if (start_snap and goal_snap) else None
    if not path_srv:
        return PathQueryResult(
            start=start_in,
            goal=goal_in,
            reachable=False,
            start_on_mesh=start_on,
            goal_on_mesh=goal_on,
            distance=0.0,
            waypoints=(),
            direct_clear=False,
        )

    waypoints = tuple(
        pt if coord_system == "server" else _server_to_unity(pt) for pt in path_srv
    )
    total_dist = sum(math.dist(a, b) for a, b in zip(waypoints, waypoints[1:]))
    return PathQueryResult(
        start=start_in,
        goal=goal_in,
        reachable=True,
        start_on_mesh=start_on,
        goal_on_mesh=goal_on,
        distance=total_dist,
        waypoints=waypoints,
        direct_clear=direct_clear,
    )


def verify_nav_directory(nav_dir: Path | str) -> NavVerifyReport:
    """递归校验导航目录内全部 ``.bin`` 文件的配对校验和、边界、掩码几何与连通性。"""
    root = Path(nav_dir)
    if not root.is_dir():
        raise NavBakeError(f"navigation directory does not exist: {root}")

    bin_files = sorted(root.rglob("*.bin"))
    entries: list[NavFileReport] = []
    errors: list[str] = []

    if not bin_files:
        errors.append(f"no .bin navigation files found in {root}")

    # 检查是否存在孤立的 .mask / .authored / .sha256 伴随文件。
    for suffix in (".mask", ".authored", ".sha256"):
        for companion in sorted(root.rglob(f"*{suffix}")):
            stem_bin = companion.with_name(companion.name[: -len(suffix)])
            if not stem_bin.is_file():
                errors.append(
                    f"orphan companion file without matching .bin: {companion.relative_to(root).as_posix()}"
                )

    for bin_path in bin_files:
        rel = bin_path.relative_to(root).as_posix()
        contract = _infer_contract_for_bin(bin_path, root)
        bin_sha = _sha256_bytes(bin_path.read_bytes())
        comp_sha = ""
        try:
            mesh = load_nav_map(bin_path, nav_root=root, contract=contract)
            if contract in _VALID_CONTRACTS:
                comp_suffix = ".authored" if contract == "authored_flat" else ".mask"
                comp_sha = _sha256_bytes((bin_path.parent / (bin_path.name + comp_suffix)).read_bytes())

            # 额外核验：authored_bounds 覆盖网格所有可走多边形中心；至少存在 1 个连通分量。
            components = _count_polygon_components(mesh)
            if components < 1:
                raise NavBakeError("navmesh has no walkable connected components")
            if mesh.authored_bounds is not None:
                ab = mesh.authored_bounds
                for poly in mesh._polygons:
                    if not poly.flags:
                        continue
                    cx, cy, cz = poly.center
                    if not ab.contains(cy, cx) or abs(cz - ab.ground_y) > 1e-6:
                        raise NavBakeError("authored polygon center falls outside declared ANAV bounds")
            # 抽样首块可走多边形中心，确认 snap() 与运行时路径一致。
            first_walkable = next(poly for poly in mesh._polygons if poly.flags)
            if mesh.mask is None and mesh.snap(first_walkable.center) is None:
                raise NavBakeError("first walkable polygon center failed snap check")

            entries.append(
                NavFileReport(
                    relative_path=rel,
                    contract=contract,
                    ok=True,
                    tile_count=len(mesh._tiles),
                    poly_count=len(mesh._polygons),
                    components=components,
                    bin_sha256=bin_sha,
                    companion_sha256=comp_sha,
                )
            )
        except Exception as exc:
            msg = f"{rel}: {exc}"
            errors.append(msg)
            entries.append(
                NavFileReport(
                    relative_path=rel,
                    contract=contract,
                    ok=False,
                    bin_sha256=bin_sha,
                    companion_sha256=comp_sha,
                    error=str(exc),
                )
            )

    failed = sum(1 for item in entries if not item.ok)
    ok = failed == 0 and not errors
    logger.info(
        "Verified nav directory dir=%s checked=%d failed=%d errors=%d",
        root,
        len(entries),
        failed,
        len(errors),
    )
    return NavVerifyReport(
        nav_dir=str(root),
        ok=ok,
        checked_count=len(entries),
        failed_count=failed,
        entries=tuple(entries),
        errors=tuple(errors),
    )


def verify_map_parity(
    nav_dir: Path | str,
    *,
    check_tables: bool = True,
) -> NavParityReport:
    """校验导航目录完整性，并可选对账 ``BaseScene`` 表的导航路径与出生点贴地契约。"""
    root = Path(nav_dir)
    verify = verify_nav_directory(root)
    scene_errors: list[str] = []
    scene_count = 0
    tables_checked = False

    if check_tables:
        try:
            from xuanming.data import ConfigTables

            tables = ConfigTables()
            tables_checked = True
            cache: dict[str, NavigationMesh] = {}
            for row in tables["BaseScene"].rows():
                nav_file = getattr(row, "nav_bin_file", "")
                if not nav_file:
                    continue
                scene_count += 1
                norm = str(nav_file).replace("\\", "/")
                prefix = "data/scene_nav_bin/"
                if not norm.startswith(prefix):
                    scene_errors.append(f"scene={row.id}: unsupported nav_bin_file prefix {nav_file!r}")
                    continue
                rel = norm[len(prefix):]
                bin_path = root / rel
                if not bin_path.is_file():
                    scene_errors.append(f"scene={row.id}: missing nav file {rel}")
                    continue
                contract = navigation_contract(norm)
                if rel not in cache:
                    cache[rel] = load_nav_map(bin_path, nav_root=root, contract=contract)
                mesh = cache[rel]
                spawn = (float(row.spawn_x), float(row.spawn_y), float(row.spawn_z))
                if not any(spawn) or not all(math.isfinite(v) for v in spawn):
                    spawn = DEFAULT_SPAWN
                if mesh.snap(spawn) is None:
                    scene_errors.append(f"scene={row.id}: spawn {spawn} is not on navmesh {rel}")
        except Exception as exc:
            scene_errors.append(f"table parity check error: {exc}")

    ok = verify.ok and not scene_errors
    logger.info(
        "Map parity check dir=%s ok=%s tables_checked=%s scenes=%d scene_errors=%d",
        root,
        ok,
        tables_checked,
        scene_count,
        len(scene_errors),
    )
    return NavParityReport(
        nav_dir=str(root),
        ok=ok,
        verify=verify,
        tables_checked=tables_checked,
        scene_count=scene_count,
        scene_errors=tuple(scene_errors),
    )


def install_authored_map(
    source_dir: Path | str,
    target_dir: Path | str,
    map_id: str | int,
) -> InstallReceipt:
    """校验源手绘导航产物后原子安装至 ``target_dir / map_id``，并写入 SHA-256 收据。"""
    map_id_str = str(map_id)
    if not map_id_str or not _MAP_ID_RE.fullmatch(map_id_str):
        raise NavBakeError(f"invalid map_id for install: {map_id!r}")

    src = Path(source_dir).resolve()
    dst_root = Path(target_dir).resolve()
    if not src.is_dir():
        raise NavBakeError(f"source directory does not exist: {src}")

    src_bin = src / "scene.bin"
    if not src_bin.is_file():
        raise NavBakeError(f"source directory missing scene.bin: {src}")

    contract = _infer_contract_for_bin(src_bin)
    if contract not in _VALID_CONTRACTS:
        raise NavBakeError(f"install requires paired authored or mask companion in {src}")

    # 安装前强制完成 SHA-256 + Detour 反序列化 + 掩码几何校验。
    load_nav_map(src_bin, contract=contract)

    comp_name = "scene.bin.authored" if contract == "authored_flat" else "scene.bin.mask"
    required_names = ["scene.bin", comp_name, "scene.bin.sha256"]
    optional_names = ["metadata.json", "navigation.json", "source.json", "delivery-source.json"]
    copy_names = required_names + [name for name in optional_names if (src / name).is_file()]

    payloads: dict[str, bytes] = {name: (src / name).read_bytes() for name in copy_names}
    hashes: dict[str, str] = {name: _sha256_bytes(data) for name, data in payloads.items()}

    dst_map_dir = (dst_root / map_id_str).resolve()
    if not dst_map_dir.is_relative_to(dst_root) or dst_map_dir == dst_root:
        raise NavBakeError("target map path escapes target_dir")
    if contract == "authored_flat" and (dst_map_dir / "scene.bin.mask").exists():
        raise NavBakeError(f"target directory has residual legacy mask: {dst_map_dir}")

    receipt_path = dst_map_dir / "install_receipt.json"
    receipt = InstallReceipt(
        map_id=map_id_str,
        source_dir=str(src),
        target_dir=str(dst_map_dir),
        contract=contract,
        files=hashes,
        receipt_path=str(receipt_path),
    )
    receipt_bytes = (json.dumps(receipt.to_dict(), ensure_ascii=False, indent=2) + "\n").encode("utf-8")

    # 拍摄已有目标文件快照，确保任意写入异常时全量回滚。
    all_targets: list[tuple[Path, bytes]] = [
        (dst_map_dir / name, data) for name, data in payloads.items()
    ]
    all_targets.append((receipt_path, receipt_bytes))
    snapshots: dict[Path, bytes | None] = {
        path: path.read_bytes() if path.is_file() else None for path, _ in all_targets
    }

    written: list[Path] = []
    try:
        for path, data in all_targets:
            _atomic_write_bytes(path, data)
            written.append(path)
    except Exception:
        for path in reversed(written):
            prev = snapshots[path]
            if prev is None and path.exists():
                path.unlink()
            elif prev is not None:
                _atomic_write_bytes(path, prev)
        raise

    # 安装后复验目标目录，杜绝落盘损坏。
    load_nav_map(dst_map_dir / "scene.bin", contract=contract)
    logger.info(
        "Installed nav map map_id=%s contract=%s target=%s files=%d",
        map_id_str,
        contract,
        dst_map_dir,
        len(hashes),
    )
    return receipt


def _parse_point(text: str) -> tuple[float, float, float]:
    parts = [item.strip() for item in text.split(",")]
    if len(parts) != 3:
        raise NavBakeError(f"expected x,y,z coordinate, got {text!r}")
    vals = tuple(float(item) for item in parts)
    if not all(math.isfinite(v) for v in vals):
        raise NavBakeError(f"nonfinite coordinate: {text!r}")
    return vals  # type: ignore[return-value]


def _parse_rect(text: str) -> tuple[float, float, float, float]:
    parts = [item.strip() for item in text.split(",")]
    if len(parts) != 4:
        raise NavBakeError(f"expected x0,z0,x1,z1 rect, got {text!r}")
    vals = tuple(float(item) for item in parts)
    if not all(math.isfinite(v) for v in vals):
        raise NavBakeError(f"nonfinite rect: {text!r}")
    return vals  # type: ignore[return-value]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="navmesh-baker",
        description="Bake, verify, query, and install pure-Python MSET navigation maps.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_bake = sub.add_parser("bake", help="Bake an authored flat or masked navigation map.")
    p_bake.add_argument("--map-id", required=True, help="Map identifier (e.g. 26000)")
    p_bake.add_argument("--out", required=True, type=Path, help="Output directory")
    p_bake.add_argument("--cols", type=int, default=32, help="Grid columns")
    p_bake.add_argument("--rows", type=int, default=32, help="Grid rows")
    p_bake.add_argument("--cell-size", type=float, default=1.0, help="Cell size in meters")
    p_bake.add_argument("--min-x", type=float, default=0.0, help="Origin min X (Unity)")
    p_bake.add_argument("--min-z", type=float, default=0.0, help="Origin min Z (Unity)")
    p_bake.add_argument(
        "--contract",
        choices=sorted(_VALID_CONTRACTS),
        default="authored_flat",
        help="Navigation pairing contract",
    )
    p_bake.add_argument(
        "--blocked-rect",
        action="append",
        default=[],
        help="Blocked Unity rect x0,z0,x1,z1 (repeatable)",
    )
    p_bake.add_argument("--spawn", type=str, default=None, help="Optional Unity spawn x,y,z")
    p_bake.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON output")

    p_query = sub.add_parser("query", help="Snap points or query paths on a navigation map.")
    p_query.add_argument("--nav", required=True, type=Path, help="Path to scene.bin or map dir")
    p_query.add_argument("--point", type=str, default=None, help="Snap query point x,y,z")
    p_query.add_argument("--start", type=str, default=None, help="Path start x,y,z")
    p_query.add_argument("--goal", type=str, default=None, help="Path goal x,y,z")
    p_query.add_argument(
        "--coord",
        choices=("server", "unity"),
        default="server",
        help="Coordinate system for input/output points",
    )
    p_query.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON output")

    p_verify = sub.add_parser("verify", help="Verify navigation directory integrity and parity.")
    p_verify.add_argument("--nav-dir", required=True, type=Path, help="Navigation root directory")
    p_verify.add_argument(
        "--parity",
        action="store_true",
        help="Also run BaseScene table parity check",
    )
    p_verify.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON output")

    p_install = sub.add_parser("install", help="Atomically install a validated authored map.")
    p_install.add_argument("--source", required=True, type=Path, help="Validated source map dir")
    p_install.add_argument("--target", required=True, type=Path, help="Target authored_maps dir")
    p_install.add_argument("--map-id", required=True, help="Target map ID")
    p_install.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON output")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """导航烘焙/查询/校验/安装命令行入口。"""
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "bake":
            spec = NavGridSpec(
                map_id=args.map_id,
                columns=args.cols,
                rows=args.rows,
                cell_size=args.cell_size,
                min_x=args.min_x,
                min_z=args.min_z,
                blocked_rects=tuple(_parse_rect(r) for r in args.blocked_rect),
                spawn=_parse_point(args.spawn) if args.spawn else None,
                contract=args.contract,
            )
            res = bake_authored_flat_map(spec, args.out)
            payload = res.to_dict()
            if args.as_json:
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                print(
                    f"BAKED {res.map_id} ({res.contract}): {res.tile_count} tiles, "
                    f"{res.poly_count} polys -> {res.bin_path}"
                )
            return 0

        if args.command == "query":
            mesh = load_nav_map(args.nav)
            out: dict[str, Any] = {"coord": args.coord}
            if args.point is not None:
                out["snap"] = snap_point(mesh, _parse_point(args.point), coord_system=args.coord).to_dict()
            if args.start is not None or args.goal is not None:
                if args.start is None or args.goal is None:
                    raise NavBakeError("both --start and --goal are required for path query")
                out["path"] = query_path(
                    mesh,
                    _parse_point(args.start),
                    _parse_point(args.goal),
                    coord_system=args.coord,
                ).to_dict()
            if "snap" not in out and "path" not in out:
                raise NavBakeError("provide --point or --start/--goal")
            if args.as_json:
                print(json.dumps(out, ensure_ascii=False, indent=2))
            else:
                if "snap" in out:
                    s = out["snap"]
                    print(f"SNAP found={s['found']} on_mesh={s['on_mesh']} nearest={s['nearest']}")
                if "path" in out:
                    p = out["path"]
                    print(
                        f"PATH reachable={p['reachable']} distance={p['distance']:.3f} "
                        f"waypoints={len(p['waypoints'])}"
                    )
            return 0

        if args.command == "verify":
            if args.parity:
                report = verify_map_parity(args.nav_dir, check_tables=True)
                ok = report.ok
                payload = report.to_dict()
            else:
                report = verify_nav_directory(args.nav_dir)
                ok = report.ok
                payload = report.to_dict()
            if args.as_json:
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                print(f"VERIFY ok={ok} dir={args.nav_dir}")
            return 0 if ok else 1

        if args.command == "install":
            receipt = install_authored_map(args.source, args.target, args.map_id)
            if args.as_json:
                print(json.dumps(receipt.to_dict(), ensure_ascii=False, indent=2))
            else:
                print(f"INSTALLED {receipt.map_id} -> {receipt.target_dir}")
            return 0

        return 2
    except Exception as exc:
        if getattr(args, "as_json", False):
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
