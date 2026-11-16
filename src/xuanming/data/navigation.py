"""Read flat Detour MSET maps and strict walkability masks in server Z-up coordinates."""

from __future__ import annotations

import heapq
import hashlib
import logging
import math
import re
import struct
from collections import defaultdict, deque
from dataclasses import dataclass
from importlib.resources import files
from pathlib import PurePosixPath

logger = logging.getLogger(__name__)

Point = tuple[float, float, float]
_MSET = struct.Struct("<3i4x9d2i")
_TILE = struct.Struct("<Qi4x")
_HEADER = struct.Struct("<4H2i7H2x6d")
_VERTEX = struct.Struct("<3d")
_POLY = struct.Struct("<I12HHBB")
_BV_NODE = struct.Struct("<6Hi")
_MAGIC = 0x4D534554
_EXTERNAL = 0x8000
_SIDE_OFFSETS = {0: (1, 0), 2: (0, 1), 4: (-1, 0), 6: (0, -1)}
_EPS = 1e-8
_MAX_RAY_POLYS = 256
_MAX_PATH_POLYS = 4096
DEFAULT_SPAWN: Point = (180.0, 200.0, 0.0)


@dataclass(frozen=True, slots=True)
class _Polygon:
    vertices: tuple[Point, ...]
    edges: tuple[tuple[Point, Point], ...]
    bounds: tuple[float, float, float, float]
    center: Point
    flags: int


@dataclass(frozen=True, slots=True)
class _GroundTile:
    first: int
    count: int
    lower: Point
    upper: Point
    nodes: tuple


@dataclass(frozen=True, slots=True)
class NavMask:
    """Exact half-open walkability bitmap paired with a versioned nav mesh."""

    columns: int
    rows: int
    cell_size: float
    min_x: float
    min_z: float
    width: float
    height: float
    effective_x: float
    effective_z: float
    effective_width: float
    effective_height: float
    packed: bytes

    @classmethod
    def from_bytes(cls, payload: bytes) -> NavMask:
        try:
            fields = payload.decode("ascii").split()
        except UnicodeDecodeError as exc:
            raise ValueError("walkability mask is not ASCII") from exc
        if len(fields) != 14 or fields[:2] != ["FMASK", "1"]:
            raise ValueError("unsupported walkability mask header")
        try:
            columns, rows = int(fields[2]), int(fields[3])
            cell_size, min_x, min_z, width, height = map(float, fields[4:9])
            effective_x, effective_z, effective_width, effective_height = map(float, fields[9:13])
        except ValueError as exc:
            raise ValueError("invalid walkability mask geometry") from exc
        if (not 0 < columns <= 4096 or not 0 < rows <= 4096
                or not all(math.isfinite(value) for value in (
                    cell_size, min_x, min_z, width, height,
                    effective_x, effective_z, effective_width, effective_height,
                ))
                or cell_size <= 0 or width <= 0 or height <= 0
                or effective_width <= 0 or effective_height <= 0
                or columns != math.ceil(width / cell_size) or rows != math.ceil(height / cell_size)
                or effective_x < min_x or effective_z < min_z
                or effective_x + effective_width > min_x + width + 1e-9
                or effective_z + effective_height > min_z + height + 1e-9):
            raise ValueError("invalid walkability mask bounds")
        encoded = fields[13]
        cell_count = columns * rows
        if len(encoded) != ((cell_count + 7) // 8) * 2 or re.fullmatch(r"[0-9a-fA-F]+", encoded) is None:
            raise ValueError("invalid walkability mask bitmap")
        packed = bytes.fromhex(encoded)
        tail = (8 - cell_count % 8) % 8
        if packed[-1] & ((1 << tail) - 1):
            raise ValueError("nonzero padding in walkability mask")
        return cls(columns, rows, cell_size, min_x, min_z, width, height,
                   effective_x, effective_z, effective_width, effective_height, packed)

    def cell(self, col: int, row: int) -> bool:
        if not (0 <= col < self.columns and 0 <= row < self.rows):
            return False
        index = row * self.columns + col
        return bool(self.packed[index // 8] & (0x80 >> (index % 8)))

    def contains(self, x: float, z: float) -> bool:
        if (not math.isfinite(x) or not math.isfinite(z)
                or x < self.effective_x or z < self.effective_z
                or x >= self.effective_x + self.effective_width
                or z >= self.effective_z + self.effective_height):
            return False
        col = math.floor((x - self.min_x) / self.cell_size)
        row = self.rows - 1 - math.floor((z - self.min_z) / self.cell_size)
        return self.cell(col, row)

    def cell_bounds(self, col: int, row: int) -> tuple[float, float, float, float] | None:
        x0 = max(self.min_x + col * self.cell_size, self.effective_x)
        x1 = min(self.min_x + (col + 1) * self.cell_size, self.effective_x + self.effective_width)
        z0 = max(self.min_z + (self.rows - 1 - row) * self.cell_size, self.effective_z)
        z1 = min(self.min_z + (self.rows - row) * self.cell_size, self.effective_z + self.effective_height)
        return (x0, x1, z0, z1) if x0 < x1 and z0 < z1 else None

    def segment(self, x0: float, z0: float, x1: float, z1: float) -> bool:
        if not self.contains(x0, z0) or not self.contains(x1, z1):
            return False
        cuts = [0.0, 1.0]
        for first, last, origin in ((x0, x1, self.min_x), (z0, z1, self.min_z)):
            if first == last:
                continue
            low, high = sorted((first, last))
            for index in range(math.floor((low - origin) / self.cell_size) + 1,
                               math.floor((high - origin) / self.cell_size) + 1):
                fraction = (origin + index * self.cell_size - first) / (last - first)
                if 0 < fraction < 1:
                    cuts.append(fraction)
        cuts.sort()
        unique = [cuts[0]]
        for fraction in cuts[1:]:
            if abs(fraction - unique[-1]) > 1e-12:
                unique.append(fraction)
        previous_col = math.floor((x0 - self.min_x) / self.cell_size)
        previous_row = self.rows - 1 - math.floor((z0 - self.min_z) / self.cell_size)
        for first, last in zip(unique, unique[1:]):
            fraction = (first + last) * 0.5
            x = x0 + (x1 - x0) * fraction
            z = z0 + (z1 - z0) * fraction
            if not self.contains(x, z):
                return False
            col = math.floor((x - self.min_x) / self.cell_size)
            row = self.rows - 1 - math.floor((z - self.min_z) / self.cell_size)
            if col != previous_col and row != previous_row and not (
                self.cell(col, previous_row) and self.cell(previous_col, row)
            ):
                return False
            previous_col, previous_row = col, row
        end_col = math.floor((x1 - self.min_x) / self.cell_size)
        end_row = self.rows - 1 - math.floor((z1 - self.min_z) / self.cell_size)
        return (end_col == previous_col or end_row == previous_row
                or self.cell(end_col, previous_row) and self.cell(previous_col, end_row))


def navigation_contract(nav_bin_file: str) -> str:
    """Return the safety contract declared by a scene navigation path."""
    path = str(nav_bin_file).replace("\\", "/")
    if path.startswith("data/scene_nav_bin/festival_maps/"):
        return "exact_mask"
    if path.startswith("data/scene_nav_bin/current_maps/"):
        return "versioned_mask"
    if path.startswith("data/scene_nav_bin/authored_maps/"):
        return "authored_flat"
    return "legacy"


def _polygon(vertices, flags):
    count = len(vertices)
    edges = tuple(zip(vertices, vertices[1:] + vertices[:1]))
    x, y, z = zip(*vertices)
    if (any(a == b for a, b in edges) or sum(a[0] * b[1] - a[1] * b[0] for a, b in edges) <= 0
            or any(_cross(a, b, c) < -_EPS for a, b in edges for c in vertices)):
        raise ValueError("non-convex or degenerate Detour ground polygon")
    return _Polygon(vertices, edges, (min(x), min(y), max(x), max(y)),
                    (sum(x) / count, sum(y) / count, z[0]), flags)


def _cross(a, b, p) -> float:
    return (b[0] - a[0]) * (p[1] - a[1]) - (b[1] - a[1]) * (p[0] - a[0])


def _lerp(a: Point, b: Point, t: float) -> Point:
    return tuple(left + (right - left) * t for left, right in zip(a, b))


def _aligned(size: int) -> int:
    return (size + 7) & ~7


def _overlap(lo, hi, other_lo, other_hi):
    return all(a <= d and b >= c for a, b, c, d in zip(lo, hi, other_lo, other_hi))


def _closest_edge(a, b, p):
    dx, dy = b[0] - a[0], b[1] - a[1]
    t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / (dx * dx + dy * dy)
    return _lerp(a, b, max(0.0, min(1.0, t)))


def _closest(poly, p):
    if all(_cross(a, b, p) >= -_EPS for a, b in poly.edges):
        return p[0], p[1], poly.center[2]
    return min((_closest_edge(a, b, p) for a, b in poly.edges), key=lambda near: math.dist(near, p))


def _clip(poly, start, end):
    """Intersect a segment with a convex ground polygon's half-planes."""
    first, last = 0.0, 1.0
    exit_edge = None
    dx, dy = end[0] - start[0], end[1] - start[1]
    for i in range(-1, len(poly.edges) - 1):
        a, b = poly.edges[i]
        origin = _cross(a, b, start)
        direction = dx * (b[1] - a[1]) - dy * (b[0] - a[0])
        if abs(direction) < _EPS:
            if origin < 0:
                return None
            continue
        t = origin / direction
        if direction < 0:
            first = max(first, t)
        elif t < last:
            last, exit_edge = t, i % len(poly.edges)
        if first > last:
            return None
    return last, exit_edge


def _portal_overlap(a, b, c, d, side):
    # Side 0/4 lies on a fixed navigation X (server Y), 2/6 on a fixed nav Z.
    axis = 0 if side in (0, 4) else 1
    fixed = 1 - axis
    if max(abs(a[fixed] - point[fixed]) for point in (b, c, d)) > 0.01:
        return None
    lower = max(min(a[axis], b[axis]), min(c[axis], d[axis]))
    upper = min(max(a[axis], b[axis]), max(c[axis], d[axis]))
    if upper - lower <= 0.02:
        return None
    limits = sorted(((lower - a[axis]) / (b[axis] - a[axis]), (upper - a[axis]) / (b[axis] - a[axis])))
    # Detour compresses cross-tile portal extents to integer bytes, even on flat maps.
    return tuple(_lerp(a, b, int(max(0.0, min(1.0, t)) * 255) / 255) for t in limits)


class NavigationMesh:
    def __init__(self, polygons, portals, tiles, parameters, *, mask: NavMask | None = None,
                 versioned_mask: bool = False) -> None:
        self._polygons = tuple(polygons)
        self._portals = tuple(tuple(links) for links in portals)
        self._height = polygons[0].center[2]
        self._tiles = tiles
        self._quantization = parameters[3]
        self._origin = parameters[4:7]
        self._tile_width, self._tile_height = parameters[7:9]
        self.mask = mask
        self.versioned_mask = versioned_mask

    @classmethod
    def from_bytes(cls, data: bytes, *, mask: NavMask | None = None, versioned_mask: bool = False):
        if len(data) < _MSET.size:
            raise ValueError("truncated MSET header")
        magic, version, count, *parameters = _MSET.unpack_from(data)
        max_tiles, max_polys = parameters[-2:]
        if (magic != _MAGIC or version != 1 or not 0 < count <= min(max_tiles, 4096)
                or max_polys < 1 or not all(math.isfinite(value) for value in parameters)
                or not all(parameters[index] > 0 for index in (3, 7, 8))):
            raise ValueError("unsupported MSET header")
        polygons, tile_refs, tiles, local_links = [], set(), {}, []
        external = defaultdict(list)
        height = None
        offset = _MSET.size
        for _ in range(count):
            if offset + _TILE.size > len(data):
                raise ValueError("truncated MSET tile header")
            ref, size = _TILE.unpack_from(data, offset)
            offset += _TILE.size
            if not ref or ref in tile_refs or size < _HEADER.size or offset + size > len(data):
                raise ValueError("invalid MSET tile range")
            tile_refs.add(ref)
            (tile_version, layer, poly_count, vertex_count, x, y, link_count, detail_count, detail_vertices,
             detail_triangles, bv_nodes, off_mesh, off_mesh_base, *bounds) = _HEADER.unpack_from(data, offset)
            expected = (_HEADER.size + vertex_count * _VERTEX.size + poly_count * _POLY.size + link_count * 16
                        + _aligned(detail_count * 6) + _aligned(detail_triangles * 4) + bv_nodes * 16)
            if (tile_version != 7 or layer or detail_vertices or off_mesh or off_mesh_base != poly_count
                    or poly_count != detail_count or not 0 < poly_count <= max_polys or not vertex_count
                    or (x, y) in tiles or size != expected or not all(math.isfinite(v) for v in bounds)
                    or any(lower > upper for lower, upper in zip(bounds[:3], bounds[3:]))):
                raise ValueError("only the original flat ground Detour tiles are supported")
            first_poly = len(polygons)
            tree_start = offset + size - bv_nodes * _BV_NODE.size
            nodes = tuple(_BV_NODE.iter_unpack(data[tree_start:offset + size]))
            if any(node[-1] >= poly_count or node[-1] < index - bv_nodes for index, node in enumerate(nodes)):
                raise ValueError("invalid Detour bounding-volume tree")
            tiles[x, y] = _GroundTile(first_poly, poly_count, tuple(bounds[:3]), tuple(bounds[3:]), nodes)
            vertex_offset = offset + _HEADER.size
            raw_vertices = tuple(_VERTEX.iter_unpack(data[vertex_offset:vertex_offset + vertex_count * _VERTEX.size]))
            vertices = tuple((nav_z, nav_x, nav_y) for nav_x, nav_y, nav_z in raw_vertices)
            if height is None:
                height = vertices[0][2]
            if any(not all(math.isfinite(v) for v in point) or point[2] != height for point in vertices):
                raise ValueError("unsupported non-flat Detour ground tile")
            poly_offset = vertex_offset + vertex_count * _VERTEX.size
            for local in range(poly_count):
                row = _POLY.unpack_from(data, poly_offset + local * _POLY.size)
                flags, corners, area_type = row[-3:]
                indices = row[1:1 + corners]
                if area_type >> 6 or not 3 <= corners <= 6 or any(key >= vertex_count for key in indices):
                    raise ValueError("invalid Detour polygon")
                polygon = _polygon(tuple(vertices[key] for key in indices), flags)
                poly = len(polygons)
                polygons.append(polygon)
                for edge_index, (edge, neighbor) in enumerate(zip(polygon.edges, row[7:7 + corners])):
                    if neighbor & _EXTERNAL:
                        side = neighbor & 0xFF
                        if side not in _SIDE_OFFSETS:
                            raise ValueError("invalid Detour tile side")
                        external[x, y, side].append((poly, edge_index, *edge))
                    elif neighbor:
                        if neighbor > poly_count or neighbor == local + 1:
                            raise ValueError("invalid Detour internal neighbor")
                        local_links.append((poly, first_poly + neighbor - 1, edge_index, *edge))
            offset += size
        if offset != len(data) or not any(poly.flags for poly in polygons):
            raise ValueError("MSET has trailing data or no walkable polygons")

        portals = [[] for _ in polygons]
        for start, end, edge, a, b in local_links:
            if polygons[start].flags and polygons[end].flags:
                if (b, a) not in polygons[end].edges:
                    raise ValueError("invalid Detour shared edge")
                portals[start].append((end, edge, a, b))
        for (x, y, side), edges in external.items():
            dx, dy = _SIDE_OFFSETS[side]
            neighbors = external.get((x + dx, y + dy, (side + 4) % 8), ())
            for start, edge, a, b in edges:
                if not polygons[start].flags:
                    continue
                for end, _, c, d in neighbors:
                    overlap = _portal_overlap(a, b, c, d, side) if polygons[end].flags else None
                    if overlap is not None:
                        portals[start].insert(0, (end, edge, *overlap))
        return cls(polygons, portals, tiles, parameters, mask=mask, versioned_mask=versioned_mask)

    def _candidates(self, position):
        x, y, z = position
        lower, upper = (y - 2, z - 4, x - 2), (y + 2, z + 4, x + 2)
        ox, _, oz = self._origin
        first_x, last_x = (math.floor((point[0] - ox) / self._tile_width) for point in (lower, upper))
        first_y, last_y = (math.floor((point[2] - oz) / self._tile_height) for point in (lower, upper))
        yielded = 0
        for ty in range(first_y, last_y + 1):
            for tx in range(first_x, last_x + 1):
                tile = self._tiles.get((tx, ty))
                if tile is None or not _overlap(lower, upper, tile.lower, tile.upper):
                    continue
                for key in self._tile_candidates(tile, lower, upper):
                    if self._polygons[key].flags:
                        yield key
                        yielded += 1
                        if yielded >= 128:
                            return

    def _tile_candidates(self, tile, lower, upper):
        if not tile.nodes:
            for key in range(tile.first, tile.first + tile.count):
                x0, y0, x1, y1 = self._polygons[key].bounds
                if _overlap(lower, upper, (y0, self._height, x0), (y1, self._height, x1)):
                    yield key
            return

        def quantize(point, *, high=False):
            steps = (int(self._quantization * (max(lo, min(hi, p)) - lo) + high)
                     for p, lo, hi in zip(point, tile.lower, tile.upper))
            return tuple((step | 1) if high else (step & 0xFFFE) for step in steps)

        qmin, qmax = quantize(lower), quantize(upper, high=True)
        index = 0
        while index < len(tile.nodes):
            node = tile.nodes[index]
            overlap = _overlap(qmin, qmax, node[:3], node[3:6])
            key = node[-1]
            if key >= 0 and overlap:
                yield tile.first + key
            index += 1 if key >= 0 or overlap else -key

    def _nearest(self, position):
        if len(position) != 3 or not all(math.isfinite(v) for v in position):
            return None
        if abs(position[2] - self._height) >= 4:
            return None
        if self.mask is not None and (
            not self.mask.contains(position[1], position[0]) or abs(position[2]) > 0.2
        ):
            return None
        best = None
        for key in self._candidates(position):
            polygon = self._polygons[key]
            snapped = _closest(polygon, position)
            distance = math.dist(snapped, position)
            if best is None or distance < best[0]:
                best = distance, key, snapped
        if best is None:
            return None
        _, key, snapped = best
        if self.mask is not None:
            if (not self.mask.contains(snapped[1], snapped[0])
                    or abs(snapped[1] - position[1]) > 0.02
                    or abs(snapped[0] - position[0]) > 0.02 or abs(snapped[2]) > 0.02):
                return None
            snapped = snapped[0], snapped[1], 0.0
        return key, snapped

    def snap(self, position: Point) -> Point | None:
        nearest = self._nearest(position)
        return nearest[1] if nearest is not None else None

    def _ray(self, first, start, target) -> float:
        current, seen = first, set()
        farthest = 0.0
        for _ in range(_MAX_RAY_POLYS):
            if current in seen:
                return 0.0
            interval = _clip(self._polygons[current], start, target)
            if interval is None:
                return farthest
            seen.add(current)
            fraction, edge = interval
            farthest = max(farthest, fraction)
            if edge is None:
                return 1.0
            hit = _lerp(start, target, fraction)
            for neighbor, portal_edge, a, b in self._portals[current]:
                if portal_edge != edge or a == b:
                    continue
                nearest = _closest_edge(a, b, hit)
                if math.hypot(nearest[0] - hit[0], nearest[1] - hit[1]) <= _EPS:
                    current = neighbor
                    break
            else:
                return farthest
        return 0.0

    def validate_move(self, start: Point, target: Point) -> tuple[bool, Point | None]:
        nearest = self._nearest(start)
        if nearest is None:
            return False, None
        key, snapped = nearest
        if len(target) != 3 or not all(math.isfinite(v) for v in target):
            return False, snapped
        if self.mask is not None and (
            abs(target[2]) > 0.2 or not self.mask.segment(start[1], start[0], target[1], target[0])
        ):
            return False, snapped
        fraction = self._ray(key, snapped, target)
        if fraction == 1.0 and (accepted := self.snap(target)) is not None:
            return True, accepted
        length = math.dist(snapped, target)
        end = _lerp(snapped, target, max(0.0, fraction - 0.05 / length)) if length > 0.05 else snapped
        return False, self.snap(end) or snapped

    def find_path(self, start: Point, target: Point) -> tuple[Point, ...] | None:
        origin, destination = self._nearest(start), self._nearest(target)
        if origin is None or destination is None:
            return None
        first, start = origin
        last, target = destination
        if self._ray(first, start, target) == 1.0:
            path = (start, target) if start != target else (start,)
        else:
            def distance(key):
                return math.dist(self._polygons[key].center, target)

            costs, previous, closed = {first: 0.0}, {}, set()
            queue = [(distance(first), first)]
            closest = first
            best_distance = math.dist(_closest(self._polygons[first], target), target)
            while queue and len(closed) < _MAX_PATH_POLYS:
                _, current = heapq.heappop(queue)
                if current in closed:
                    continue
                if current == last:
                    closest = last
                    break
                closed.add(current)
                candidate_distance = math.dist(_closest(self._polygons[current], target), target)
                if candidate_distance < best_distance:
                    closest, best_distance = current, candidate_distance
                center = self._polygons[current].center
                for neighbor, _, a, b in self._portals[current]:
                    if neighbor in closed:
                        continue
                    cost = costs[current] + math.dist(center, self._polygons[neighbor].center)
                    if cost < costs.get(neighbor, math.inf):
                        costs[neighbor] = cost
                        previous[neighbor] = current, _lerp(a, b, 0.5)
                        heapq.heappush(queue, (cost + distance(neighbor), neighbor))

            if self.mask is not None and closest != last:
                return None
            point = target if closest == last else _closest(self._polygons[closest], target)
            raw_path = [point]
            while closest != first:
                parent, portal = previous[closest]
                # Avoid starting a smoothed segment exactly on a portal. Detour can
                # choose either incident polygon when snapping a boundary point.
                raw_path.append(_lerp(portal, self._polygons[closest].center, 0.01))
                raw_path.append(_lerp(portal, self._polygons[parent].center, 0.01))
                closest = parent
            raw_path.append(start)
            raw_path.reverse()
            path = self._straighten(raw_path)

        if self.mask is not None:
            valid_path = (path and path[0] == start and path[-1] == target
                          and all(self.mask.segment(a[1], a[0], b[1], b[0])
                                  for a, b in zip(path, path[1:])))
            if not valid_path:
                return self._versioned_mask_path(start, target) if self.versioned_mask else None
        return path

    def _versioned_mask_path(self, start: Point, target: Point) -> tuple[Point, ...] | None:
        mask = self.mask
        if mask is None:
            return None
        total = mask.columns * mask.rows
        if total > 262144:
            return None

        def index(point):
            col = math.floor((point[1] - mask.min_x) / mask.cell_size)
            row = mask.rows - 1 - math.floor((point[0] - mask.min_z) / mask.cell_size)
            return row * mask.columns + col

        first, goal = index(start), index(target)
        if first == goal:
            return (start, target) if self.validate_move(start, target)[0] else None

        def location(cell):
            if cell == first:
                return start
            if cell == goal:
                return target
            bounds = mask.cell_bounds(cell % mask.columns, cell // mask.columns)
            if bounds is None:
                return None
            x0, x1, z0, z1 = bounds
            return ((z0 + z1) / 2, (x0 + x1) / 2, 0.0)

        parent = [-1] * total
        parent[first] = first
        pending = deque((first,))
        steps = ((1, 0), (-1, 0), (0, 1), (0, -1))
        while pending and parent[goal] == -1:
            current = pending.popleft()
            point = location(current)
            if point is None:
                continue
            row, col = divmod(current, mask.columns)
            for dx, dy in steps:
                next_col, next_row = col + dx, row + dy
                if not mask.cell(next_col, next_row):
                    continue
                neighbor = next_row * mask.columns + next_col
                if parent[neighbor] != -1:
                    continue
                target_point = location(neighbor)
                if target_point is None or not self.validate_move(point, target_point)[0]:
                    continue
                parent[neighbor] = current
                pending.append(neighbor)
        if parent[goal] == -1:
            return None
        path = []
        cell = goal
        while cell != first:
            point = location(cell)
            if point is None:
                return None
            path.append(point)
            cell = parent[cell]
        path.append(start)
        path.reverse()
        return tuple(path)

    def _straighten(self, path):
        result = [path[0]]
        index = 0
        while index + 1 < len(path):
            key, start = self._nearest(path[index])
            farthest = index
            while farthest + 1 < len(path) and self._ray(key, start, path[farthest + 1]) == 1.0:
                farthest += 1
            if farthest == index:
                break  # A partial path may be followed safely up to this point.
            if path[farthest] != result[-1]:
                result.append(path[farthest])
            index = farthest
        return tuple(result)


def load_navigation(tables) -> dict[int, NavigationMesh]:
    """Share packaged meshes, verify versioned contracts, and probe scene spawns."""
    root = files("xuanming.data").joinpath("nav")
    cache = {}
    result = {}
    for row in tables["BaseScene"].rows():
        if not row.nav_bin_file:
            continue
        path = PurePosixPath(row.nav_bin_file.replace("\\", "/"))
        contract = navigation_contract(row.nav_bin_file)
        if (len(path.parts) < 3 or path.parts[:2] != ("data", "scene_nav_bin")
                or path.suffix != ".bin" or any(part in (".", "..") for part in path.parts)):
            logger.warning("跳过不支持的导航文件位置 scene_config_id=%s", row.id)
            continue
        if contract == "legacy" and len(path.parts) != 3:
            logger.warning("旧导航文件路径必须位于导航根目录 scene_config_id=%s", row.id)
            continue
        if contract == "authored_flat":
            logger.warning("当前 Python 导航读取器尚不支持手绘导航契约 scene_config_id=%s", row.id)
            continue
        relative = path.parts[2:]
        cache_key = "/".join(relative)
        if cache_key not in cache:
            try:
                payload = root.joinpath(*relative).read_bytes()
                mask = None
                versioned = contract == "versioned_mask"
                if contract in {"exact_mask", "versioned_mask"}:
                    parent = root.joinpath(*relative[:-1])
                    mask_payload = parent.joinpath(path.name + ".mask").read_bytes()
                    checksum_fields = parent.joinpath(path.name + ".sha256").read_text("ascii").split()
                    if (len(checksum_fields) != 2
                            or any(re.fullmatch(r"[0-9a-f]{64}", item) is None for item in checksum_fields)
                            or hashlib.sha256(payload).hexdigest() != checksum_fields[0]
                            or hashlib.sha256(mask_payload).hexdigest() != checksum_fields[1]):
                        raise ValueError("navigation checksums do not match")
                    mask = NavMask.from_bytes(mask_payload)
                cache[cache_key] = NavigationMesh.from_bytes(payload, mask=mask, versioned_mask=versioned)
            except (OSError, ValueError):
                logger.warning("场景导航文件加载失败 file=%s", path.as_posix(), exc_info=True)
                cache[cache_key] = None
        mesh = cache[cache_key]
        spawn = row.spawn_x, row.spawn_y, row.spawn_z
        if not any(spawn) or not all(math.isfinite(v) for v in spawn):
            spawn = DEFAULT_SPAWN
        if mesh is not None:
            if mesh.snap(spawn) is not None:
                result[row.id] = mesh
            else:
                logger.warning("场景出生点不在导航网格 scene_config_id=%s", row.id)
    return result
