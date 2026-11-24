"""纯 Python 标准库回合制战斗美术程序化生成器（对齐 Go ``tools/battle_art_gen``）。

特点：
1. 零第三方依赖：内置 ``RgbaCanvas`` 与基于 ``zlib`` + ``struct`` + CRC32 的纯标准库
   PNG 编解码与头/块校验器（``encode_png`` / ``decode_png_header`` / ``decode_png_rgba``）；
2. 覆盖 Go 版全部 7 类战斗美术资产：
   - ``ground``：敌我双排倾斜椭圆战斗地台（``UI/ground_band_{far,near}.png``）；
   - ``characters``：五行门派玩家精灵动作横条（``Characters/<class>/{idle,attack,cast,hit}_E_strip.png``）；
   - ``monsters``：6 种差异化程序化怪物帧条与剪影 IoU / 覆盖率自检（对齐 ``monsters_verify.go``）；
   - ``buffs``：64×64 增益/减益/控制圆角徽章与 8 种中央符号（``Buff/<id>.png``）；
   - ``fx``：9 套 8 帧技能序列帧（``Fx/<id>_strip.png``）；
   - ``digits``：4 套配色伤害/治疗/暴击/闪避数字字集与 ``UI/digits_meta.json``；
   - ``ui``：九宫面板、按钮、血条槽、HP/MP 填充条与 7 扇区命令环；
3. 严格确定性：固定随机种子与无时间戳 PNG 输出，重复运行产出逐字节一致的 ``.png``。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import random
import struct
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

logger = logging.getLogger(__name__)

RGBA = tuple[int, int, int, int]
RGB = tuple[int, int, int]

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
DEFAULT_SEED = 20260903
ALL_CATEGORIES: tuple[str, ...] = (
    "ground",
    "characters",
    "monsters",
    "buffs",
    "fx",
    "digits",
    "ui",
)

# 与 monsters_verify.go 对齐的剪影归一化网格与最大允许 IoU。
MONSTER_SILHOUETTE_W = 24
MONSTER_SILHOUETTE_H = 32
MONSTER_MAX_SILHOUETTE_IOU = 0.60


class BattleArtError(ValueError):
    """战斗美术生成或 PNG 校验失败。"""


def _clamp_u8(value: float) -> int:
    if value <= 0:
        return 0
    if value >= 255:
        return 255
    return int(value + 0.5)


def _clamp01(value: float) -> float:
    if value <= 0.0:
        return 0.0
    if value >= 1.0:
        return 1.0
    return value


def _mix_rgb(a: RGB, b: RGB, t: float) -> RGB:
    k = _clamp01(t)
    return (
        _clamp_u8(a[0] + (b[0] - a[0]) * k),
        _clamp_u8(a[1] + (b[1] - a[1]) * k),
        _clamp_u8(a[2] + (b[2] - a[2]) * k),
    )


def _scale_rgb(c: RGB, factor: float) -> RGB:
    return (_clamp_u8(c[0] * factor), _clamp_u8(c[1] * factor), _clamp_u8(c[2] * factor))


def _rgba(c: RGB, alpha: int = 255) -> RGBA:
    return (c[0], c[1], c[2], _clamp_u8(alpha))


def _ease_out(t: float) -> float:
    k = 1.0 - _clamp01(t)
    return 1.0 - k * k * k


# ---------------------------------------------------------------------------
# PNG 纯标准库编解码与格式校验
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PngHeaderInfo:
    """PNG 文件头与数据块结构校验结果。"""

    width: int
    height: int
    bit_depth: int
    color_type: int
    compression: int
    filter_method: int
    interlace: int
    idat_chunks: int
    idat_bytes: int
    chunk_types: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "width": self.width,
            "height": self.height,
            "bit_depth": self.bit_depth,
            "color_type": self.color_type,
            "compression": self.compression,
            "filter_method": self.filter_method,
            "interlace": self.interlace,
            "idat_chunks": self.idat_chunks,
            "idat_bytes": self.idat_bytes,
            "chunk_types": list(self.chunk_types),
        }


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    crc = zlib.crc32(chunk_type)
    crc = zlib.crc32(data, crc) & 0xFFFFFFFF
    return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", crc)


def _channels_for_color_type(color_type: int) -> int:
    mapping = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
    if color_type not in mapping:
        raise BattleArtError(f"unsupported PNG color_type={color_type}")
    return mapping[color_type]


def _parse_png_chunks(data: bytes) -> tuple[PngHeaderInfo, bytes]:
    """完整解析 PNG 魔数、IHDR、全部块 CRC32、拼接 IDAT 并验证 IEND 终止。"""
    if len(data) < 33 or data[:8] != PNG_SIGNATURE:
        raise BattleArtError("invalid PNG signature")

    offset = 8
    chunk_types: list[str] = []
    ihdr: tuple[int, int, int, int, int, int, int] | None = None
    idat_parts: list[bytes] = []
    saw_iend = False

    while offset < len(data):
        if offset + 12 > len(data):
            raise BattleArtError("truncated PNG chunk header")
        length = struct.unpack_from(">I", data, offset)[0]
        ctype = data[offset + 4 : offset + 8]
        data_start = offset + 8
        data_end = data_start + length
        crc_end = data_end + 4
        if crc_end > len(data):
            raise BattleArtError(f"truncated PNG chunk {ctype!r}")
        cdata = data[data_start:data_end]
        expected_crc = struct.unpack_from(">I", data, data_end)[0]
        actual_crc = zlib.crc32(cdata, zlib.crc32(ctype)) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise BattleArtError(f"PNG CRC mismatch in chunk {ctype!r}")

        name = ctype.decode("latin1")
        chunk_types.append(name)

        if len(chunk_types) == 1:
            if ctype != b"IHDR" or length != 13:
                raise BattleArtError("PNG must begin with 13-byte IHDR chunk")
            ihdr = struct.unpack(">IIBBBBB", cdata)
            w, h, bit_depth, color_type, comp, filt, interlace = ihdr
            if w <= 0 or h <= 0 or bit_depth not in (1, 2, 4, 8, 16):
                raise BattleArtError(f"invalid IHDR dimensions or bit_depth: {ihdr}")
            if comp != 0 or filt != 0 or interlace not in (0, 1):
                raise BattleArtError(f"invalid IHDR compression/filter/interlace: {ihdr}")
            _channels_for_color_type(color_type)
        elif ctype == b"IDAT":
            if saw_iend:
                raise BattleArtError("IDAT found after IEND")
            idat_parts.append(cdata)
        elif ctype == b"IEND":
            if length != 0:
                raise BattleArtError("non-empty IEND chunk")
            saw_iend = True
            offset = crc_end
            break

        offset = crc_end

    if ihdr is None or not saw_iend:
        raise BattleArtError("PNG missing IHDR or IEND chunk")
    if offset != len(data):
        raise BattleArtError("trailing bytes after PNG IEND chunk")
    if not idat_parts:
        raise BattleArtError("PNG contains no IDAT chunks")

    idat_joined = b"".join(idat_parts)
    try:
        raw_scanlines = zlib.decompress(idat_joined)
    except zlib.error as exc:
        raise BattleArtError(f"invalid zlib stream in PNG IDAT: {exc}") from exc

    w, h, bit_depth, color_type, comp, filt, interlace = ihdr
    if interlace == 0:
        channels = _channels_for_color_type(color_type)
        row_bytes = (w * channels * bit_depth + 7) // 8
        expected_raw = (1 + row_bytes) * h
        if len(raw_scanlines) != expected_raw:
            raise BattleArtError(
                f"unexpected PNG decompressed size: got {len(raw_scanlines)}, want {expected_raw}"
            )

    info = PngHeaderInfo(
        width=w,
        height=h,
        bit_depth=bit_depth,
        color_type=color_type,
        compression=comp,
        filter_method=filt,
        interlace=interlace,
        idat_chunks=len(idat_parts),
        idat_bytes=len(idat_joined),
        chunk_types=tuple(chunk_types),
    )
    return info, raw_scanlines


def decode_png_header(data: bytes) -> PngHeaderInfo:
    """校验 PNG 魔数、IHDR、IDAT 解压完整性、CRC32 与 IEND 并返回头信息。"""
    info, _ = _parse_png_chunks(data)
    return info


def _paeth(a: int, b: int, c: int) -> int:
    p = a + b - c
    pa = abs(p - a)
    pb = abs(p - b)
    pc = abs(p - c)
    if pa <= pb and pa <= pc:
        return a
    if pb <= pc:
        return b
    return c


def decode_png_rgba(data: bytes) -> RgbaCanvas:
    """将 8 位 RGBA PNG 字节流解码为 ``RgbaCanvas``（支持全部 5 种 PNG 行滤波器）。"""
    info, raw = _parse_png_chunks(data)
    if info.bit_depth != 8 or info.color_type != 6 or info.interlace != 0:
        raise BattleArtError(
            f"decode_png_rgba requires non-interlaced 8-bit RGBA, got {info}"
        )
    w, h = info.width, info.height
    stride = w * 4
    out = bytearray(stride * h)
    prev = bytes(stride)
    pos = 0
    for y in range(h):
        ftype = raw[pos]
        pos += 1
        src = raw[pos : pos + stride]
        pos += stride
        row = bytearray(stride)
        if ftype == 0:
            row[:] = src
        elif ftype == 1:
            for i in range(stride):
                left = row[i - 4] if i >= 4 else 0
                row[i] = (src[i] + left) & 0xFF
        elif ftype == 2:
            for i in range(stride):
                row[i] = (src[i] + prev[i]) & 0xFF
        elif ftype == 3:
            for i in range(stride):
                left = row[i - 4] if i >= 4 else 0
                row[i] = (src[i] + ((left + prev[i]) >> 1)) & 0xFF
        elif ftype == 4:
            for i in range(stride):
                left = row[i - 4] if i >= 4 else 0
                up = prev[i]
                up_left = prev[i - 4] if i >= 4 else 0
                row[i] = (src[i] + _paeth(left, up, up_left)) & 0xFF
        else:
            raise BattleArtError(f"unsupported PNG scanline filter: {ftype}")
        out[y * stride : (y + 1) * stride] = row
        prev = bytes(row)

    canvas = RgbaCanvas(w, h)
    canvas.pixels[:] = out
    return canvas


# ---------------------------------------------------------------------------
# 纯标准库 RGBA8 画布
# ---------------------------------------------------------------------------


class RgbaCanvas:
    """基于平铺 ``bytearray`` 的纯 Python RGBA8 画布与矢量光栅化图元集合。"""

    __slots__ = ("width", "height", "pixels")

    def __init__(self, width: int, height: int) -> None:
        if width <= 0 or height <= 0:
            raise BattleArtError(f"invalid canvas dimensions: {width}x{height}")
        self.width = int(width)
        self.height = int(height)
        self.pixels = bytearray(self.width * self.height * 4)

    def copy(self) -> RgbaCanvas:
        clone = RgbaCanvas(self.width, self.height)
        clone.pixels[:] = self.pixels
        return clone

    def get_pixel(self, x: int, y: int) -> RGBA:
        if x < 0 or y < 0 or x >= self.width or y >= self.height:
            return (0, 0, 0, 0)
        idx = (y * self.width + x) * 4
        p = self.pixels
        return (p[idx], p[idx + 1], p[idx + 2], p[idx + 3])

    def set_pixel(self, x: int, y: int, rgba: RGBA) -> None:
        if x < 0 or y < 0 or x >= self.width or y >= self.height:
            return
        idx = (y * self.width + x) * 4
        p = self.pixels
        p[idx] = rgba[0] & 0xFF
        p[idx + 1] = rgba[1] & 0xFF
        p[idx + 2] = rgba[2] & 0xFF
        p[idx + 3] = rgba[3] & 0xFF

    def blend_pixel(self, x: int, y: int, rgba: RGBA, *, additive: bool = False) -> None:
        """执行直通 Alpha Source-Over 混合或加法辉光混合。"""
        if x < 0 or y < 0 or x >= self.width or y >= self.height:
            return
        sa = rgba[3]
        if sa <= 0:
            return
        idx = (y * self.width + x) * 4
        p = self.pixels
        if additive:
            sr, sg, sb = rgba[0], rgba[1], rgba[2]
            da = p[idx + 3]
            if da == 0:
                p[idx] = sr
                p[idx + 1] = sg
                p[idx + 2] = sb
                p[idx + 3] = sa
                return
            fa = sa / 255.0
            fda = da / 255.0
            out_a = fda + fa - fda * fa
            r = min(255, int((p[idx] * fda + sr * fa) / out_a + 0.5))
            g = min(255, int((p[idx + 1] * fda + sg * fa) / out_a + 0.5))
            b = min(255, int((p[idx + 2] * fda + sb * fa) / out_a + 0.5))
            p[idx] = r
            p[idx + 1] = g
            p[idx + 2] = b
            p[idx + 3] = _clamp_u8(out_a * 255.0)
            return

        if sa >= 255 or p[idx + 3] == 0:
            p[idx] = rgba[0]
            p[idx + 1] = rgba[1]
            p[idx + 2] = rgba[2]
            p[idx + 3] = sa
            return

        da = p[idx + 3]
        inv_sa = 255 - sa
        out_a_255 = sa * 255 + da * inv_sa
        if out_a_255 <= 0:
            return
        p[idx] = (rgba[0] * sa * 255 + p[idx] * da * inv_sa) // out_a_255
        p[idx + 1] = (rgba[1] * sa * 255 + p[idx + 1] * da * inv_sa) // out_a_255
        p[idx + 2] = (rgba[2] * sa * 255 + p[idx + 2] * da * inv_sa) // out_a_255
        p[idx + 3] = (out_a_255 + 127) // 255

    def _fill_span(
        self,
        y: int,
        x0: int,
        x1: int,
        rgba: RGBA,
        *,
        blend: bool = True,
        additive: bool = False,
    ) -> None:
        if y < 0 or y >= self.height:
            return
        if x0 < 0:
            x0 = 0
        if x1 > self.width:
            x1 = self.width
        if x0 >= x1:
            return
        sa = rgba[3]
        if sa <= 0 and blend:
            return
        if (not blend or (sa >= 255 and not additive)):
            row_start = (y * self.width + x0) * 4
            row_end = (y * self.width + x1) * 4
            self.pixels[row_start:row_end] = bytes(rgba) * (x1 - x0)
            return
        for x in range(x0, x1):
            self.blend_pixel(x, y, rgba, additive=additive)

    def fill_rect(
        self,
        x: int,
        y: int,
        w: int,
        h: int,
        rgba: RGBA,
        *,
        blend: bool = True,
    ) -> None:
        if w <= 0 or h <= 0:
            return
        y0 = max(0, int(y))
        y1 = min(self.height, int(y + h))
        x0 = max(0, int(x))
        x1 = min(self.width, int(x + w))
        if x0 >= x1 or y0 >= y1:
            return
        if not blend or rgba[3] >= 255:
            row_bytes = bytes(rgba) * (x1 - x0)
            stride = self.width * 4
            p = self.pixels
            for row_y in range(y0, y1):
                start = row_y * stride + x0 * 4
                p[start : start + len(row_bytes)] = row_bytes
            return
        for row_y in range(y0, y1):
            self._fill_span(row_y, x0, x1, rgba, blend=True)

    def fill_round_rect(
        self,
        x0: float,
        y0: float,
        x1: float,
        y1: float,
        radius: float,
        rgba: RGBA | Callable[[int], RGBA],
        *,
        blend: bool = True,
    ) -> None:
        """使用扫描线绘制圆角矩形，支持纯色或按行 y 着色的渐变回调。"""
        if x1 <= x0 or y1 <= y0:
            return
        r = max(0.0, min(radius, (x1 - x0) * 0.5, (y1 - y0) * 0.5))
        iy0 = max(0, int(math.floor(y0)))
        iy1 = min(self.height, int(math.ceil(y1)))
        for y in range(iy0, iy1):
            py = y + 0.5
            if py < y0 or py > y1:
                continue
            inset = 0.0
            if py < y0 + r:
                dy = (y0 + r) - py
                inset = r - math.sqrt(max(0.0, r * r - dy * dy))
            elif py > y1 - r:
                dy = py - (y1 - r)
                inset = r - math.sqrt(max(0.0, r * r - dy * dy))
            lx = int(math.ceil(x0 + inset - 0.5))
            rx = int(math.floor(x1 - inset + 0.5))
            color = rgba(y) if callable(rgba) else rgba
            self._fill_span(y, lx, rx, color, blend=blend)

    def fill_circle(
        self,
        cx: float,
        cy: float,
        radius: float,
        rgba: RGBA,
        *,
        blend: bool = True,
        additive: bool = False,
    ) -> None:
        if radius <= 0:
            return
        y0 = max(0, int(math.floor(cy - radius)))
        y1 = min(self.height, int(math.ceil(cy + radius)) + 1)
        r2 = radius * radius
        for y in range(y0, y1):
            dy = y + 0.5 - cy
            rem = r2 - dy * dy
            if rem <= 0:
                continue
            dx = math.sqrt(rem)
            x0 = int(math.ceil(cx - dx - 0.5))
            x1 = int(math.floor(cx + dx + 0.5))
            self._fill_span(y, x0, x1, rgba, blend=blend, additive=additive)

    def fill_ellipse(
        self,
        cx: float,
        cy: float,
        rx: float,
        ry: float,
        rgba: RGBA,
        *,
        angle_deg: float = 0.0,
        blend: bool = True,
        additive: bool = False,
    ) -> None:
        if rx <= 0 or ry <= 0:
            return
        if abs(angle_deg) > 1e-6:
            pts = _ellipse_polygon(cx, cy, rx, ry, angle_deg, segments=48)
            self.fill_polygon(pts, rgba, blend=blend, additive=additive)
            return
        y0 = max(0, int(math.floor(cy - ry)))
        y1 = min(self.height, int(math.ceil(cy + ry)) + 1)
        inv_ry2 = 1.0 / (ry * ry)
        for y in range(y0, y1):
            dy = y + 0.5 - cy
            term = 1.0 - dy * dy * inv_ry2
            if term <= 0:
                continue
            dx = rx * math.sqrt(term)
            x0 = int(math.ceil(cx - dx - 0.5))
            x1 = int(math.floor(cx + dx + 0.5))
            self._fill_span(y, x0, x1, rgba, blend=blend, additive=additive)

    def draw_line(
        self,
        x0: float,
        y0: float,
        x1: float,
        y1: float,
        rgba: RGBA,
        *,
        thickness: float = 1.0,
        blend: bool = True,
        additive: bool = False,
    ) -> None:
        """绘制带圆头端点的抗锯齿粗线段。"""
        dx = x1 - x0
        dy = y1 - y0
        length = math.hypot(dx, dy)
        r = max(0.5, thickness * 0.5)
        if length < 1e-6:
            self.fill_circle(x0, y0, r, rgba, blend=blend, additive=additive)
            return
        if thickness <= 1.2:
            steps = max(1, int(math.ceil(length * 1.5)))
            for i in range(steps + 1):
                t = i / steps
                px = int(round(x0 + dx * t))
                py = int(round(y0 + dy * t))
                if blend:
                    self.blend_pixel(px, py, rgba, additive=additive)
                else:
                    self.set_pixel(px, py, rgba)
            return
        # 粗线段拆为矩形主体 + 两端圆帽，保证扫描线速度与干净端点。
        nx = -dy / length * r
        ny = dx / length * r
        quad = (
            (x0 + nx, y0 + ny),
            (x1 + nx, y1 + ny),
            (x1 - nx, y1 - ny),
            (x0 - nx, y0 - ny),
        )
        self.fill_polygon(quad, rgba, blend=blend, additive=additive)
        self.fill_circle(x0, y0, r, rgba, blend=blend, additive=additive)
        self.fill_circle(x1, y1, r, rgba, blend=blend, additive=additive)

    def fill_polygon(
        self,
        points: Sequence[tuple[float, float]],
        rgba: RGBA | Callable[[int], RGBA],
        *,
        blend: bool = True,
        additive: bool = False,
    ) -> None:
        """扫描线奇偶规则多边形填充，支持纯色或按行 y 回调渐变着色。"""
        n = len(points)
        if n < 3:
            return
        ys = [pt[1] for pt in points]
        y_min = max(0, int(math.floor(min(ys))))
        y_max = min(self.height - 1, int(math.ceil(max(ys))))
        edges = [(points[i], points[(i + 1) % n]) for i in range(n)]

        for y in range(y_min, y_max + 1):
            py = y + 0.5
            xs: list[float] = []
            for (x0, y0), (x1, y1) in edges:
                if (y0 <= py < y1) or (y1 <= py < y0):
                    xs.append(x0 + (py - y0) * (x1 - x0) / (y1 - y0))
            if len(xs) < 2:
                continue
            xs.sort()
            color = rgba(y) if callable(rgba) else rgba
            for i in range(0, len(xs) - 1, 2):
                sx0 = int(math.ceil(xs[i] - 0.5))
                sx1 = int(math.floor(xs[i + 1] + 0.5))
                self._fill_span(y, sx0, sx1, color, blend=blend, additive=additive)

    def extract_mask(self, alpha_threshold: int = 128) -> list[list[bool]]:
        """提取二维布尔不透明遮罩（``alpha >= alpha_threshold``）。"""
        w, h = self.width, self.height
        alphas = self.pixels[3::4]
        return [
            [alphas[y * w + x] >= alpha_threshold for x in range(w)]
            for y in range(h)
        ]

    def apply_outline(
        self,
        color: RGBA,
        thickness: int = 1,
        alpha_threshold: int = 128,
    ) -> None:
        """在现有不透明轮廓外圈叠加指定厚度的纯色描边（保持内部像素不变）。"""
        if thickness <= 0:
            return
        w, h = self.width, self.height
        alphas = self.pixels[3::4]
        offsets: list[tuple[int, int]] = []
        r2 = thickness * thickness + 0.25
        for dy in range(-thickness, thickness + 1):
            for dx in range(-thickness, thickness + 1):
                if (dx != 0 or dy != 0) and dx * dx + dy * dy <= r2:
                    offsets.append((dx, dy))

        # 仅在有效不透明像素包围盒附近做膨胀扫描，大幅缩短纯 Python 耗时。
        solid_indices = [i for i, a in enumerate(alphas) if a >= alpha_threshold]
        if not solid_indices:
            return
        to_paint: set[int] = set()
        for idx in solid_indices:
            y, x = divmod(idx, w)
            for dx, dy in offsets:
                nx, ny = x + dx, y + dy
                if 0 <= nx < w and 0 <= ny < h:
                    nidx = ny * w + nx
                    if alphas[nidx] < alpha_threshold:
                        to_paint.add(nidx)

        p = self.pixels
        r, g, b, a = color
        for nidx in to_paint:
            off = nidx * 4
            p[off] = r
            p[off + 1] = g
            p[off + 2] = b
            p[off + 3] = a

    def blit(
        self,
        src: RgbaCanvas,
        dx: int,
        dy: int,
        *,
        flip_x: bool = False,
        blend: bool = False,
    ) -> None:
        """将源画布拷贝或混合到本画布指定位置（支持水平镜像，用于拼横条与 W 朝向）。"""
        sw, sh = src.width, src.height
        dw, dh = self.width, self.height
        src_p = src.pixels
        dst_p = self.pixels
        for sy in range(sh):
            ty = dy + sy
            if ty < 0 or ty >= dh:
                continue
            if not flip_x and not blend:
                x0 = max(0, -dx)
                x1 = min(sw, dw - dx)
                if x0 < x1:
                    s0 = (sy * sw + x0) * 4
                    s1 = (sy * sw + x1) * 4
                    d0 = (ty * dw + dx + x0) * 4
                    dst_p[d0 : d0 + (s1 - s0)] = src_p[s0:s1]
                continue
            for sx in range(sw):
                tx = dx + (sw - 1 - sx if flip_x else sx)
                if tx < 0 or tx >= dw:
                    continue
                so = (sy * sw + sx) * 4
                px = (src_p[so], src_p[so + 1], src_p[so + 2], src_p[so + 3])
                if blend:
                    self.blend_pixel(tx, ty, px)
                else:
                    do = (ty * dw + tx) * 4
                    dst_p[do : do + 4] = src_p[so : so + 4]

    def opaque_pixel_count(self, alpha_threshold: int = 8) -> int:
        alphas = self.pixels[3::4]
        return sum(1 for a in alphas if a >= alpha_threshold)

    def encode_png(self) -> bytes:
        """将画布编码为确定性 8 位 RGBA PNG 字节流（纯 stdlib ``zlib`` + ``struct``）。"""
        w, h = self.width, self.height
        stride = w * 4
        mv = memoryview(self.pixels)
        raw = bytearray((stride + 1) * h)
        row_step = stride + 1
        for y in range(h):
            dst_off = y * row_step
            raw[dst_off] = 0  # Filter type 0 (None)
            raw[dst_off + 1 : dst_off + row_step] = mv[y * stride : (y + 1) * stride]

        compressed = zlib.compress(bytes(raw), level=6)
        ihdr = struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0)
        return (
            PNG_SIGNATURE
            + _png_chunk(b"IHDR", ihdr)
            + _png_chunk(b"IDAT", compressed)
            + _png_chunk(b"IEND", b"")
        )

    def save_png(self, path: Path) -> int:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.encode_png()
        path.write_bytes(payload)
        return len(payload)


def _ellipse_polygon(
    cx: float,
    cy: float,
    rx: float,
    ry: float,
    angle_deg: float,
    segments: int = 48,
) -> list[tuple[float, float]]:
    rad = math.radians(angle_deg)
    ca, sa = math.cos(rad), math.sin(rad)
    pts: list[tuple[float, float]] = []
    for i in range(segments):
        th = 2.0 * math.pi * i / segments
        ex = rx * math.cos(th)
        ey = ry * math.sin(th)
        pts.append((cx + ex * ca - ey * sa, cy + ex * sa + ey * ca))
    return pts


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# 结果数据结构
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AssetRecord:
    """单个生成产物的元数据记录。"""

    path: str
    category: str
    kind: str
    width: int
    height: int
    bytes_written: int
    sha256: str
    frames: int = 1
    desc: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "category": self.category,
            "kind": self.kind,
            "width": self.width,
            "height": self.height,
            "bytes": self.bytes_written,
            "sha256": self.sha256,
            "frames": self.frames,
            "desc": self.desc,
        }


@dataclass(frozen=True)
class CategoryResult:
    """单个美术大类的生成结果。"""

    category: str
    files: tuple[AssetRecord, ...]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "file_count": len(self.files),
            "files": [f.to_dict() for f in self.files],
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class MonsterVerifyReport:
    """怪物帧条与剪影区分度自检报告（对齐 ``monsters_verify.go``）。"""

    ok: bool
    strips_checked: int
    min_opaque_px_per_frame: int
    max_pair_iou: float
    worst_pair: str
    pairwise_iou: tuple[dict[str, Any], ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "strips_checked": self.strips_checked,
            "min_opaque_px_per_frame": self.min_opaque_px_per_frame,
            "max_pair_iou": round(self.max_pair_iou, 4),
            "worst_pair": self.worst_pair,
            "pairwise_iou": list(self.pairwise_iou),
            "errors": list(self.errors),
        }


@dataclass(frozen=True)
class GenerationReport:
    """总生成报告。"""

    out_dir: str
    seed: int
    categories: tuple[str, ...]
    total_png_files: int
    results: dict[str, CategoryResult]
    verified: bool
    monster_verify: MonsterVerifyReport | None
    manifest_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "out_dir": self.out_dir,
            "seed": self.seed,
            "categories": list(self.categories),
            "total_png_files": self.total_png_files,
            "verified": self.verified,
            "manifest_path": self.manifest_path,
            "results": {k: v.to_dict() for k, v in self.results.items()},
            "monster_verify": self.monster_verify.to_dict() if self.monster_verify else None,
        }


def _emit_png(
    out_root: Path,
    rel_path: str,
    canvas: RgbaCanvas,
    category: str,
    kind: str,
    *,
    frames: int = 1,
    desc: str = "",
) -> AssetRecord:
    full = out_root / rel_path
    data = canvas.encode_png()
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(data)
    return AssetRecord(
        path=rel_path.replace("\\", "/"),
        category=category,
        kind=kind,
        width=canvas.width,
        height=canvas.height,
        bytes_written=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
        frames=frames,
        desc=desc,
    )


# ---------------------------------------------------------------------------
# 1. Ground —— 敌我双排倾斜椭圆战斗地台
# ---------------------------------------------------------------------------


def generate_ground(
    out_dir: Path | str,
    *,
    width: int = 512,
    height: int = 256,
) -> CategoryResult:
    """生成敌方远带与我方近带两张 45° 透视半透明光带地台贴图。"""
    root = Path(out_dir)
    tilt_deg = -13.88
    specs = (
        {
            "file": "ground_band_far",
            "rx": width * 0.41,
            "ry": height * 0.20,
            "top": (28, 40, 62),
            "bottom": (120, 168, 196),
            "rim": (176, 226, 244),
            "peak_alpha": 115,
            "desc": "Enemy far arena ground band (cool cyan-slate, tilted -13.88 deg)",
        },
        {
            "file": "ground_band_near",
            "rx": width * 0.43,
            "ry": height * 0.23,
            "top": (52, 44, 28),
            "bottom": (224, 196, 128),
            "rim": (255, 238, 190),
            "peak_alpha": 148,
            "desc": "Ally near arena ground band (warm jade-gold, tilted -13.88 deg)",
        },
    )
    records: list[AssetRecord] = []
    cx, cy = width * 0.5, height * 0.5
    rad = math.radians(tilt_deg)

    for sp in specs:
        canvas = RgbaCanvas(width, height)
        rx: float = sp["rx"]
        ry: float = sp["ry"]
        half_h = math.hypot(rx * math.sin(rad), ry * math.cos(rad))
        y0, y1 = cy - half_h, cy + half_h
        top_rgb: RGB = sp["top"]
        bot_rgb: RGB = sp["bottom"]
        rim_rgb: RGB = sp["rim"]
        peak_a: int = sp["peak_alpha"]

        # 三层同心倾斜椭圆模拟柔和羽化边缘，确保外 2px 全透明且上暗下亮。
        layers = (
            (1.00, 0.35),
            (0.92, 0.55),
            (0.82, 0.80),
        )
        for scale, alpha_scale in layers:
            pts = _ellipse_polygon(cx, cy, rx * scale, ry * scale, tilt_deg, segments=64)

            def shade(y: int, a_scale: float = alpha_scale) -> RGBA:
                k = _clamp01((y + 0.5 - y0) / max(y1 - y0, 1.0))
                return _rgba(_mix_rgb(top_rgb, bot_rgb, k), int(peak_a * a_scale))

            canvas.fill_polygon(pts, shade, blend=True)

        # 内芯提亮光带与下沿微亮高光边。
        core_pts = _ellipse_polygon(cx, cy + ry * 0.14, rx * 0.68, ry * 0.46, tilt_deg, segments=48)
        canvas.fill_polygon(core_pts, _rgba(rim_rgb, int(peak_a * 0.35)), blend=True, additive=True)

        # 强制四边 2px 清零，避免客户端拉伸产生硬边。
        canvas.fill_rect(0, 0, width, 2, (0, 0, 0, 0), blend=False)
        canvas.fill_rect(0, height - 2, width, 2, (0, 0, 0, 0), blend=False)
        canvas.fill_rect(0, 0, 2, height, (0, 0, 0, 0), blend=False)
        canvas.fill_rect(width - 2, 0, 2, height, (0, 0, 0, 0), blend=False)

        records.append(
            _emit_png(
                root,
                f"UI/{sp['file']}.png",
                canvas,
                "ground",
                "ground_band",
                desc=sp["desc"],
            )
        )

    logger.info("Generated %d ground band textures in %s", len(records), root)
    return CategoryResult(
        category="ground",
        files=tuple(records),
        metadata={"tilt_deg": tilt_deg, "width": width, "height": height},
    )


# ---------------------------------------------------------------------------
# 2. Characters —— 五行门派玩家战斗精灵动作横条
# ---------------------------------------------------------------------------

_CHARACTER_SPECS: tuple[dict[str, Any], ...] = (
    {
        "id": "jin_sword",
        "name": "金系剑修",
        "robe": (226, 188, 72),
        "trim": (255, 244, 190),
        "dark": (118, 86, 26),
        "weapon": "sword",
        "aura": (255, 224, 120),
    },
    {
        "id": "mu_healer",
        "name": "木系医仙",
        "robe": (76, 188, 114),
        "trim": (196, 250, 206),
        "dark": (34, 96, 62),
        "weapon": "staff",
        "aura": (120, 245, 168),
    },
    {
        "id": "shui_spear",
        "name": "水系龙女",
        "robe": (74, 152, 224),
        "trim": (192, 232, 255),
        "dark": (34, 72, 130),
        "weapon": "spear",
        "aura": (120, 210, 255),
    },
    {
        "id": "huo_fan",
        "name": "火系炎灵",
        "robe": (224, 78, 60),
        "trim": (255, 204, 132),
        "dark": (122, 34, 28),
        "weapon": "fan",
        "aura": (255, 136, 72),
    },
    {
        "id": "tu_hammer",
        "name": "土系力士",
        "robe": (176, 132, 82),
        "trim": (236, 212, 166),
        "dark": (92, 66, 40),
        "weapon": "hammer",
        "aura": (230, 188, 110),
    },
)

_CHAR_ACTIONS: tuple[tuple[str, int], ...] = (
    ("idle", 6),
    ("attack", 12),
    ("cast", 10),
    ("hit", 12),
)


def _draw_character_frame(
    spec: dict[str, Any],
    action: str,
    frame_idx: int,
    frames: int,
    size: int,
) -> RgbaCanvas:
    c = RgbaCanvas(size, size)
    s = size / 64.0
    ph = 2.0 * math.pi * frame_idx / frames
    dx = 0.0
    dy = 0.0
    weapon_swing = 0.0
    cast_glow = 0.0
    hit_flash = 0.0

    if action == "idle":
        dy = -1.5 * math.sin(ph) * s
    elif action == "attack":
        lunge = (-2.0, -3.0, 2.0, 7.0, 8.0, 5.0, 2.0, 0.0)[frame_idx % 8]
        dx = lunge * s
        weapon_swing = (-0.4, -0.7, 0.2, 1.0, 0.9, 0.4, 0.1, 0.0)[frame_idx % 8]
    elif action == "cast":
        dy = (0.0, 1.5, 2.5, 0.5, -2.5, -2.0, -1.0, 0.0)[frame_idx % 8] * s
        cast_glow = (0.0, 0.2, 0.45, 0.75, 1.0, 0.8, 0.45, 0.15)[frame_idx % 8]
    elif action == "hit":
        dx = (0.0, -4.0, -6.0, -4.0, -2.0, -1.0, 0.0, 0.0)[frame_idx % 8] * s
        hit_flash = (0.0, 0.75, 0.55, 0.35, 0.2, 0.1, 0.0, 0.0)[frame_idx % 8]

    robe = _mix_rgb(spec["robe"], (255, 255, 255), hit_flash)
    trim = _mix_rgb(spec["trim"], (255, 255, 255), hit_flash)
    dark = _mix_rgb(spec["dark"], (255, 255, 255), hit_flash)
    skin = _mix_rgb((248, 218, 192), (255, 255, 255), hit_flash)
    ink = (22, 16, 26, 255)

    cx = 30.0 * s + dx
    feet_y = 58.0 * s  # 对齐 FeetPivot.y ≈ 0.08

    # 施法脚底灵气光环
    if cast_glow > 0.05:
        aura = spec["aura"]
        c.fill_ellipse(
            cx,
            feet_y - 2.0 * s,
            16.0 * s * (0.7 + 0.35 * cast_glow),
            5.0 * s,
            _rgba(aura, int(150 * cast_glow)),
            blend=True,
            additive=True,
        )

    # 双脚与长袍下摆
    c.fill_rect(int(cx - 7 * s), int(feet_y - 7 * s), int(5 * s), int(7 * s), _rgba(dark))
    c.fill_rect(int(cx + 2 * s), int(feet_y - 7 * s), int(5 * s), int(7 * s), _rgba(dark))
    robe_poly = (
        (cx - 11 * s, feet_y - 6 * s + dy * 0.3),
        (cx + 11 * s, feet_y - 6 * s + dy * 0.3),
        (cx + 8 * s, 26 * s + dy),
        (cx - 8 * s, 26 * s + dy),
    )
    c.fill_polygon(robe_poly, _rgba(robe))
    # 衣襟高光与腰带
    c.fill_rect(int(cx - 8 * s), int(38 * s + dy), int(16 * s), int(3 * s), _rgba(trim))
    c.fill_polygon(
        ((cx - 3 * s, 27 * s + dy), (cx + 3 * s, 27 * s + dy), (cx + 2 * s, 40 * s + dy), (cx - 2 * s, 40 * s + dy)),
        _rgba(trim),
    )

    # 头部、发冠与眼睛
    head_y = 18.0 * s + dy
    c.fill_circle(cx, head_y, 7.2 * s, _rgba(skin))
    c.fill_ellipse(cx - 1.0 * s, head_y - 4.5 * s, 7.8 * s, 4.5 * s, _rgba(dark))
    c.fill_circle(cx - 1.0 * s, head_y - 9.0 * s, 3.0 * s, _rgba(trim))
    c.fill_circle(cx + 3.0 * s, head_y - 0.5 * s, 1.3 * s, (25, 25, 35, 255))

    # 门派专属武器
    wtype = spec["weapon"]
    hand_x = cx + (9.0 + weapon_swing * 4.0) * s
    hand_y = 32.0 * s + dy - weapon_swing * 4.0 * s
    if wtype == "sword":
        c.draw_line(
            hand_x,
            hand_y,
            hand_x + (13.0 + weapon_swing * 5.0) * s,
            hand_y - (14.0 - weapon_swing * 10.0) * s,
            _rgba(trim),
            thickness=3.2 * s,
        )
    elif wtype == "staff":
        c.draw_line(hand_x, hand_y + 12 * s, hand_x + 4 * s, hand_y - 16 * s, _rgba(dark), thickness=2.8 * s)
        c.fill_circle(hand_x + 4 * s, hand_y - 17 * s, 4.2 * s, _rgba(spec["aura"]))
    elif wtype == "spear":
        c.draw_line(hand_x - 8 * s, hand_y + 6 * s, hand_x + 16 * s, hand_y - (8 - weapon_swing * 6) * s, _rgba(dark), thickness=2.4 * s)
        c.fill_polygon(
            (
                (hand_x + 15 * s, hand_y - (11 - weapon_swing * 6) * s),
                (hand_x + 21 * s, hand_y - (9 - weapon_swing * 6) * s),
                (hand_x + 16 * s, hand_y - (5 - weapon_swing * 6) * s),
            ),
            _rgba(trim),
        )
    elif wtype == "fan":
        fan_pts = (
            (hand_x, hand_y),
            (hand_x + 12 * s, hand_y - 10 * s),
            (hand_x + 15 * s, hand_y - 2 * s),
            (hand_x + 10 * s, hand_y + 5 * s),
        )
        c.fill_polygon(fan_pts, _rgba(trim))
    elif wtype == "hammer":
        c.draw_line(hand_x, hand_y + 8 * s, hand_x + 10 * s, hand_y - 8 * s, _rgba(dark), thickness=3.0 * s)
        c.fill_rect(int(hand_x + 6 * s), int(hand_y - 15 * s), int(11 * s), int(9 * s), _rgba(trim))

    c.apply_outline(ink, thickness=max(1, int(round(s))))
    return c


def generate_characters(
    out_dir: Path | str,
    *,
    frame_size: int = 64,
    frames: int = 8,
) -> CategoryResult:
    """程序化生成 5 大五行职业的 8 帧战斗动作横条与 ``CHARACTER_MANIFEST.json``。"""
    root = Path(out_dir)
    records: list[AssetRecord] = []
    manifest_chars: list[dict[str, Any]] = []

    for spec in _CHARACTER_SPECS:
        strips: list[dict[str, Any]] = []
        for action, fps in _CHAR_ACTIONS:
            strip = RgbaCanvas(frame_size * frames, frame_size)
            for f in range(frames):
                frame = _draw_character_frame(spec, action, f, frames, frame_size)
                strip.blit(frame, f * frame_size, 0)
            rel = f"Characters/{spec['id']}/{action}_E_strip.png"
            rec = _emit_png(
                root,
                rel,
                strip,
                "characters",
                "character_strip",
                frames=frames,
                desc=f"{spec['name']} {action} strip ({frames} frames @ {fps} FPS)",
            )
            records.append(rec)
            strips.append({"action": action, "fps": fps, "path": rel})
        manifest_chars.append(
            {"id": spec["id"], "name": spec["name"], "weapon": spec["weapon"], "strips": strips}
        )

    _write_json(
        root / "CHARACTER_MANIFEST.json",
        {
            "tool": "xuanming.ops.battle_art_gen",
            "frame_size": frame_size,
            "frames": frames,
            "characters": manifest_chars,
        },
    )
    logger.info("Generated %d character action strips in %s", len(records), root)
    return CategoryResult(
        category="characters",
        files=tuple(records),
        metadata={"character_count": len(_CHARACTER_SPECS), "frame_size": frame_size, "frames": frames},
    )


# ---------------------------------------------------------------------------
# 3. Monsters —— 6 种差异化形态怪物帧条与剪影自检
# ---------------------------------------------------------------------------

_MONSTER_ROSTER: tuple[dict[str, Any], ...] = (
    {
        "id": 1,
        "name": "野狼",
        "template": "wolf",
        "main": (126, 138, 158),
        "dark": (68, 78, 98),
        "belly": (198, 206, 216),
        "accent": (238, 243, 250),
        "eye": (255, 92, 72),
    },
    {
        "id": 2,
        "name": "山鬼",
        "template": "imp",
        "main": (98, 170, 112),
        "dark": (48, 104, 66),
        "belly": (188, 226, 172),
        "accent": (234, 222, 188),
        "eye": (255, 214, 64),
    },
    {
        "id": 3,
        "name": "狐妖",
        "template": "fox",
        "main": (234, 108, 64),
        "dark": (170, 58, 38),
        "belly": (255, 240, 218),
        "accent": (255, 198, 122),
        "eye": (126, 240, 255),
    },
    {
        "id": 4,
        "name": "石灵",
        "template": "golem",
        "main": (130, 134, 142),
        "dark": (72, 76, 86),
        "belly": (170, 174, 182),
        "accent": (104, 178, 96),
        "eye": (140, 230, 255),
    },
    {
        "id": 5,
        "name": "蛇妖",
        "template": "serpent",
        "main": (74, 184, 112),
        "dark": (34, 110, 72),
        "belly": (216, 240, 168),
        "accent": (255, 122, 152),
        "eye": (255, 200, 60),
    },
    {
        "id": 6,
        "name": "山贼",
        "template": "bandit",
        "main": (152, 114, 78),
        "dark": (92, 64, 44),
        "belly": (208, 182, 142),
        "accent": (216, 224, 234),
        "eye": (255, 122, 82),
    },
)

_MONSTER_ACTIONS: tuple[tuple[str, int], ...] = (
    ("idle", 6),
    ("attack", 12),
    ("hit", 12),
)


def _monster_baseline_row(frame_size: int) -> int:
    return frame_size - 1 - int(round(frame_size * 0.08))


def _draw_monster_frame(
    spec: dict[str, Any],
    action: str,
    frame_idx: int,
    frames: int,
    size: int,
) -> RgbaCanvas:
    c = RgbaCanvas(size, size)
    s = size / 64.0
    ph = 2.0 * math.pi * frame_idx / frames
    dx = 0.0
    dy = 0.0
    flash = 0.0
    open_k = 0.0

    if action == "idle":
        dy = -1.5 * math.sin(ph) * s
    elif action == "attack":
        dx = (-2.0, -3.5, 1.5, 5.5, 7.5, 5.0, 2.0, 0.0)[frame_idx % 8] * s
        dy = (0.0, 0.8, -1.5, -2.5, -1.5, -0.5, 0.0, 0.0)[frame_idx % 8] * s
        open_k = (0.1, 0.3, 0.6, 0.9, 1.0, 0.7, 0.3, 0.1)[frame_idx % 8]
    elif action == "hit":
        dx = (0.0, -3.5, -5.5, -4.0, -2.5, -1.5, -0.5, 0.0)[frame_idx % 8] * s
        flash = (0.65, 0.85, 0.45, 0.25, 0.12, 0.05, 0.0, 0.0)[frame_idx % 8]

    main = _mix_rgb(spec["main"], (255, 255, 255), flash)
    dark = _mix_rgb(spec["dark"], (255, 255, 255), flash)
    belly = _mix_rgb(spec["belly"], (255, 255, 255), flash)
    accent = _mix_rgb(spec["accent"], (255, 255, 255), flash)
    eye = spec["eye"]
    outline_px = max(1, int(round(s)))
    # 脚底预留 outline_px，使描边后最低不透明行精确落在 baseline_row。
    foot_y = float(_monster_baseline_row(size) - outline_px)

    tpl = spec["template"]
    ox = 30.0 * s + dx

    if tpl == "wolf":
        # 低矮水平四足野狼：长躯干、四条细腿、前伸尖吻、向后平直尾巴。
        by = foot_y - 11.0 * s + dy
        c.draw_line(ox - 12 * s, by, ox - 23 * s, by - 5 * s, _rgba(dark), thickness=4.0 * s)
        # 四条腿
        for lx, col in ((-10, dark), (-5, main), (5, dark), (11, main)):
            c.draw_line(ox + lx * s, by + 2 * s, ox + (lx + 1) * s, foot_y, _rgba(col), thickness=3.6 * s)
        c.fill_ellipse(ox, by, 15.0 * s, 7.0 * s, _rgba(main))
        c.fill_ellipse(ox, by + 2.5 * s, 11.0 * s, 3.5 * s, _rgba(belly))
        # 狼头与尖吻
        hx, hy = ox + 15.0 * s, by - 5.5 * s
        c.fill_circle(hx, hy, 5.5 * s, _rgba(main))
        c.fill_polygon(((hx + 3 * s, hy - 2 * s), (hx + 12 * s, hy + 1 * s), (hx + 3 * s, hy + 3.5 * s)), _rgba(accent))
        c.fill_polygon(((hx - 2 * s, hy - 4 * s), (hx - 5 * s, hy - 12 * s), (hx + 1 * s, hy - 6 * s)), _rgba(dark))
        c.fill_circle(hx + 2.2 * s, hy - 1.2 * s, 1.5 * s, _rgba(eye))

    elif tpl == "imp":
        # 矮壮驼背山鬼：宽肩、弯双角、高举向左上伸出的巨型骨棒。
        c.draw_line(ox - 10 * s, foot_y - 20 * s + dy, ox - 20 * s, foot_y - 42 * s + dy, _rgba(accent), thickness=5.5 * s)
        c.fill_circle(ox - 20 * s, foot_y - 43 * s + dy, 4.5 * s, _rgba(accent))
        # 粗短双腿
        c.fill_rect(int(ox - 9 * s), int(foot_y - 12 * s), int(6 * s), int(12 * s), _rgba(dark))
        c.fill_rect(int(ox + 3 * s), int(foot_y - 12 * s), int(6 * s), int(12 * s), _rgba(main))
        # 驼背躯干
        c.fill_ellipse(ox, foot_y - 20 * s + dy, 12.0 * s, 11.0 * s, _rgba(main))
        c.fill_ellipse(ox + 2 * s, foot_y - 19 * s + dy, 6.5 * s, 7.0 * s, _rgba(belly))
        # 大头与弯双角
        hx, hy = ox + 5.0 * s, foot_y - 33.0 * s + dy
        c.fill_circle(hx, hy, 7.0 * s, _rgba(main))
        c.draw_line(hx - 4 * s, hy - 5 * s, hx - 9 * s, hy - 14 * s, _rgba(accent), thickness=2.6 * s)
        c.draw_line(hx + 3 * s, hy - 5 * s, hx + 9 * s, hy - 14 * s, _rgba(accent), thickness=2.6 * s)
        c.fill_circle(hx + 3.0 * s, hy - 1.0 * s, 1.7 * s, _rgba(eye))

    elif tpl == "fox":
        # 灵动三尾赤狐：左后方巨大扇形三尾冲天，身形细长高耸大耳。
        by = foot_y - 12.0 * s + dy
        for ang, col in ((-1.15, dark), (-0.82, main), (-0.48, accent)):
            tx = ox - 10 * s + math.cos(ang - 1.2) * 20 * s
            ty = by + math.sin(ang - 1.2) * 24 * s
            c.draw_line(ox - 8 * s, by, tx, ty, _rgba(col), thickness=6.5 * s)
            c.fill_circle(tx, ty, 3.8 * s, _rgba(belly))
        c.draw_line(ox - 5 * s, by + 2 * s, ox - 7 * s, foot_y, _rgba(dark), thickness=2.6 * s)
        c.draw_line(ox + 7 * s, by + 2 * s, ox + 9 * s, foot_y, _rgba(main), thickness=2.6 * s)
        c.fill_ellipse(ox + 1 * s, by, 10.5 * s, 5.5 * s, _rgba(main))
        hx, hy = ox + 11.0 * s, by - 9.0 * s
        c.fill_circle(hx, hy, 5.0 * s, _rgba(main))
        # 标志性超大竖耳
        c.fill_polygon(((hx - 3 * s, hy - 3 * s), (hx - 7 * s, hy - 17 * s), (hx + 1 * s, hy - 5 * s)), _rgba(main))
        c.fill_polygon(((hx + 1 * s, hy - 4 * s), (hx + 4 * s, hy - 16 * s), (hx + 5 * s, hy - 3 * s)), _rgba(belly))
        c.fill_circle(hx + 2.2 * s, hy - 1.0 * s, 1.6 * s, _rgba(eye))

    elif tpl == "golem":
        # 厚重倒梯形石灵：双巨岩护肩 + 棱角身躯 + 粗石柱腿 + 青苔点缀。
        c.fill_rect(int(ox - 13 * s), int(foot_y - 14 * s), int(9 * s), int(14 * s), _rgba(dark))
        c.fill_rect(int(ox + 4 * s), int(foot_y - 14 * s), int(9 * s), int(14 * s), _rgba(main))
        torso = (
            (ox - 19 * s, foot_y - 42 * s + dy),
            (ox + 19 * s, foot_y - 42 * s + dy),
            (ox + 13 * s, foot_y - 13 * s + dy * 0.4),
            (ox - 13 * s, foot_y - 13 * s + dy * 0.4),
        )
        c.fill_polygon(torso, _rgba(main))
        # 左右巨拳下垂至膝侧
        c.fill_rect(int(ox - 24 * s), int(foot_y - 34 * s + dy), int(8 * s), int(18 * s), _rgba(dark))
        c.fill_rect(int(ox + 16 * s), int(foot_y - 34 * s + dy), int(8 * s), int(18 * s), _rgba(belly))
        # 青苔与冰蓝眼缝
        c.fill_polygon(
            ((ox - 14 * s, foot_y - 43 * s + dy), (ox - 2 * s, foot_y - 43 * s + dy), (ox - 6 * s, foot_y - 36 * s + dy)),
            _rgba(accent),
        )
        c.fill_rect(int(ox - 5 * s), int(foot_y - 35 * s + dy), int(12 * s), int(3.5 * s), _rgba(eye))

    elif tpl == "serpent":
        # S 形盘身翠蛇：宽底座盘绕 + 纤细 S 颈高耸 + 眼镜蛇兜帽 + 吐信。
        c.fill_ellipse(ox - 2 * s, foot_y - 5 * s, 18.0 * s, 5.5 * s, _rgba(dark))
        c.fill_ellipse(ox, foot_y - 11 * s, 14.0 * s, 4.5 * s, _rgba(main))
        c.draw_line(ox - 8 * s, foot_y - 13 * s, ox + 4 * s, foot_y - 26 * s + dy, _rgba(main), thickness=6.5 * s)
        c.draw_line(ox + 4 * s, foot_y - 26 * s + dy, ox - 4 * s, foot_y - 39 * s + dy, _rgba(main), thickness=6.0 * s)
        c.draw_line(ox - 4 * s, foot_y - 39 * s + dy, ox + 8 * s, foot_y - 46 * s + dy, _rgba(belly), thickness=5.2 * s)
        # 蛇头兜帽与分叉红信子
        hx, hy = ox + 10.0 * s, foot_y - 46.0 * s + dy
        c.fill_ellipse(hx - 2 * s, hy + 2 * s, 8.0 * s, 5.0 * s, _rgba(main))
        c.fill_circle(hx + 2 * s, hy, 4.5 * s, _rgba(main))
        tongue_len = (6.0 + 4.0 * open_k) * s
        c.draw_line(hx + 6 * s, hy + 1 * s, hx + 6 * s + tongue_len, hy + 1 * s, _rgba(accent), thickness=1.5 * s)
        c.fill_circle(hx + 3 * s, hy - 1.5 * s, 1.5 * s, _rgba(eye))

    else:  # bandit
        # 挺拔山贼刀客：高斗笠/头巾 + 右侧斜挥长环首刀 + 飘扬披风。
        # 左侧披风
        cloak = (
            (ox - 4 * s, foot_y - 34 * s + dy),
            (ox - 17 * s, foot_y - 9 * s + dy * 0.3),
            (ox - 6 * s, foot_y - 10 * s + dy * 0.3),
        )
        c.fill_polygon(cloak, _rgba(dark))
        # 修长双腿分立
        c.draw_line(ox - 4 * s, foot_y - 16 * s, ox - 9 * s, foot_y, _rgba(dark), thickness=4.2 * s)
        c.draw_line(ox + 4 * s, foot_y - 16 * s, ox + 9 * s, foot_y, _rgba(main), thickness=4.2 * s)
        # 躯干与胸带
        c.fill_rect(int(ox - 6 * s), int(foot_y - 35 * s + dy), int(12 * s), int(20 * s), _rgba(main))
        c.draw_line(ox - 6 * s, foot_y - 33 * s + dy, ox + 6 * s, foot_y - 18 * s + dy, _rgba(belly), thickness=2.6 * s)
        # 头部与宽檐斗笠
        hx, hy = ox, foot_y - 41.0 * s + dy
        c.fill_circle(hx, hy, 5.0 * s, _rgba(belly))
        c.fill_polygon(
            ((hx - 14 * s, hy - 2 * s), (hx + 14 * s, hy - 2 * s), (hx, hy - 11 * s)),
            _rgba(dark),
        )
        c.fill_circle(hx + 2.5 * s, hy, 1.3 * s, _rgba(eye))
        # 前伸长刀
        c.draw_line(
            ox + 6 * s,
            foot_y - 26 * s + dy,
            ox + (24 + open_k * 3) * s,
            foot_y - (34 - open_k * 10) * s + dy,
            _rgba(accent),
            thickness=3.2 * s,
        )

    c.apply_outline((20, 14, 24, 255), thickness=outline_px)
    return c


def _silhouette_grid(canvas: RgbaCanvas, frame_size: int) -> list[bool]:
    """抽取帧条第 0 帧 ``alpha >= 128`` 包围盒并归一化到 24×32 剪影网格。"""
    alphas = canvas.pixels[3::4]
    w = canvas.width
    x0, y0, x1, y1 = frame_size, frame_size, -1, -1
    for y in range(frame_size):
        row_off = y * w
        for x in range(frame_size):
            if alphas[row_off + x] >= 128:
                if x < x0:
                    x0 = x
                if x > x1:
                    x1 = x
                if y < y0:
                    y0 = y
                if y > y1:
                    y1 = y
    if x1 < 0:
        raise BattleArtError("monster frame 0 has no pixels with alpha >= 128")

    bw = float(x1 - x0 + 1)
    bh = float(y1 - y0 + 1)
    grid = [False] * (MONSTER_SILHOUETTE_W * MONSTER_SILHOUETTE_H)
    for gy in range(MONSTER_SILHOUETTE_H):
        sy0 = y0 + int(gy * bh / MONSTER_SILHOUETTE_H)
        sy1 = max(sy0 + 1, y0 + int((gy + 1) * bh / MONSTER_SILHOUETTE_H))
        for gx in range(MONSTER_SILHOUETTE_W):
            sx0 = x0 + int(gx * bw / MONSTER_SILHOUETTE_W)
            sx1 = max(sx0 + 1, x0 + int((gx + 1) * bw / MONSTER_SILHOUETTE_W))
            on = 0
            tot = 0
            for y in range(sy0, min(sy1, y1 + 1)):
                row_off = y * w
                for x in range(sx0, min(sx1, x1 + 1)):
                    tot += 1
                    if alphas[row_off + x] >= 128:
                        on += 1
            if tot > 0 and on / tot >= 0.5:
                grid[gy * MONSTER_SILHOUETTE_W + gx] = True
    return grid


def _grid_iou(a: Sequence[bool], b: Sequence[bool]) -> float:
    inter = sum(1 for x, y in zip(a, b) if x and y)
    union = sum(1 for x, y in zip(a, b) if x or y)
    return inter / union if union > 0 else 0.0


def verify_monsters(
    out_dir: Path | str,
    *,
    frame_size: int = 64,
    frames: int = 8,
    max_iou: float = MONSTER_MAX_SILHOUETTE_IOU,
) -> MonsterVerifyReport:
    """重新解码磁盘怪物 PNG，核验尺寸、覆盖率、真透明、脚底基线与两两剪影 IoU。"""
    root = Path(out_dir)
    monsters_root = root / "Monsters"
    errors: list[str] = []
    strips_checked = 0
    min_opaque = 1 << 30
    silhouettes: list[tuple[str, list[bool]]] = []
    want_bottom = _monster_baseline_row(frame_size)

    if not monsters_root.is_dir():
        return MonsterVerifyReport(
            ok=False,
            strips_checked=0,
            min_opaque_px_per_frame=0,
            max_pair_iou=1.0,
            worst_pair="",
            pairwise_iou=(),
            errors=(f"missing Monsters directory: {monsters_root}",),
        )

    monster_dirs = sorted(
        (p for p in monsters_root.iterdir() if p.is_dir()),
        key=lambda p: (int(p.name) if p.name.isdigit() else 999999, p.name),
    )
    if not monster_dirs:
        errors.append("no monster subdirectories found")

    for mdir in monster_dirs:
        if not (mdir / "meta.json").is_file():
            errors.append(f"monster {mdir.name}: missing meta.json")
        for action, _ in _MONSTER_ACTIONS:
            strip_path = mdir / f"{action}_E_strip.png"
            if not strip_path.is_file():
                errors.append(f"monster {mdir.name}: missing {strip_path.name}")
                continue
            try:
                canvas = decode_png_rgba(strip_path.read_bytes())
            except Exception as exc:
                errors.append(f"{strip_path}: decode failed: {exc}")
                continue
            strips_checked += 1
            if canvas.width != frame_size * frames or canvas.height != frame_size:
                errors.append(
                    f"{strip_path}: size {canvas.width}x{canvas.height}, want {frame_size * frames}x{frame_size}"
                )
                continue

            alphas = canvas.pixels[3::4]
            fully_clear = sum(1 for a in alphas if a == 0)
            fully_opaque = sum(1 for a in alphas if a == 255)
            if fully_clear == 0:
                errors.append(f"{strip_path}: missing transparent background (alpha == 0)")
            if fully_opaque == 0:
                errors.append(f"{strip_path}: missing solid pixels (alpha == 255)")

            bottom0 = -1
            for f in range(frames):
                ox = f * frame_size
                opaque = 0
                for y in range(frame_size):
                    row_off = y * canvas.width + ox
                    for x in range(frame_size):
                        if alphas[row_off + x] >= 8:
                            opaque += 1
                            if f == 0 and y > bottom0:
                                bottom0 = y
                if opaque == 0:
                    errors.append(f"{strip_path}: frame {f} is completely transparent")
                if opaque < min_opaque:
                    min_opaque = opaque

            if action == "idle":
                if abs(bottom0 - want_bottom) > 2:
                    errors.append(
                        f"{strip_path}: idle frame 0 bottom row {bottom0}, expected {want_bottom}±2"
                    )
                try:
                    silhouettes.append((mdir.name, _silhouette_grid(canvas, frame_size)))
                except BattleArtError as exc:
                    errors.append(f"{strip_path}: {exc}")

    pairwise: list[dict[str, Any]] = []
    worst_iou = 0.0
    worst_pair = ""
    for i in range(len(silhouettes)):
        for j in range(i + 1, len(silhouettes)):
            id_a, g_a = silhouettes[i]
            id_b, g_b = silhouettes[j]
            iou = _grid_iou(g_a, g_b)
            label = f"{id_a} vs {id_b}"
            pairwise.append({"pair": label, "iou": round(iou, 4)})
            if iou > worst_iou:
                worst_iou = iou
                worst_pair = label
            if iou > max_iou:
                errors.append(
                    f"monster silhouettes too similar ({label}): IoU={iou:.3f} > {max_iou:.2f}"
                )

    pairwise.sort(key=lambda item: item["iou"], reverse=True)
    ok = len(errors) == 0 and strips_checked > 0
    return MonsterVerifyReport(
        ok=ok,
        strips_checked=strips_checked,
        min_opaque_px_per_frame=0 if min_opaque == (1 << 30) else min_opaque,
        max_pair_iou=worst_iou,
        worst_pair=worst_pair,
        pairwise_iou=tuple(pairwise),
        errors=tuple(errors),
    )


def generate_monsters(
    out_dir: Path | str,
    *,
    frame_size: int = 64,
    frames: int = 8,
    count: int = 6,
    include_west: bool = False,
    verify: bool = True,
) -> CategoryResult:
    """程序化生成怪物帧条、单怪 ``meta.json``、``MONSTER_MANIFEST.json`` 并执行自检。"""
    root = Path(out_dir)
    selected = _MONSTER_ROSTER[: max(1, min(count, len(_MONSTER_ROSTER)))]
    records: list[AssetRecord] = []
    manifest_entries: list[dict[str, Any]] = []

    for spec in selected:
        mid = spec["id"]
        action_entries: list[dict[str, Any]] = []
        for action, fps in _MONSTER_ACTIONS:
            strip_e = RgbaCanvas(frame_size * frames, frame_size)
            for f in range(frames):
                frame = _draw_monster_frame(spec, action, f, frames, frame_size)
                strip_e.blit(frame, f * frame_size, 0)
            rel_e = f"Monsters/{mid}/{action}_E_strip.png"
            records.append(
                _emit_png(
                    root,
                    rel_e,
                    strip_e,
                    "monsters",
                    "monster_strip",
                    frames=frames,
                    desc=f"Monster {mid} ({spec['name']}) {action} E strip",
                )
            )
            entry = {"action": action, "fps": fps, "east": rel_e}
            if include_west:
                strip_w = RgbaCanvas(frame_size * frames, frame_size)
                for f in range(frames):
                    sub = RgbaCanvas(frame_size, frame_size)
                    for y in range(frame_size):
                        so = (y * strip_e.width + f * frame_size) * 4
                        do = (y * frame_size) * 4
                        sub.pixels[do : do + frame_size * 4] = strip_e.pixels[
                            so : so + frame_size * 4
                        ]
                    strip_w.blit(sub, f * frame_size, 0, flip_x=True)
                rel_w = f"Monsters/{mid}/{action}_W_strip.png"
                records.append(
                    _emit_png(
                        root,
                        rel_w,
                        strip_w,
                        "monsters",
                        "monster_strip",
                        frames=frames,
                        desc=f"Monster {mid} ({spec['name']}) {action} W strip",
                    )
                )
                entry["west"] = rel_w
            action_entries.append(entry)

        meta = {
            "id": mid,
            "name": spec["name"],
            "template": spec["template"],
            "frame_size": frame_size,
            "frames": frames,
            "actions": action_entries,
        }
        _write_json(root / f"Monsters/{mid}/meta.json", meta)
        manifest_entries.append(meta)

    verify_report: MonsterVerifyReport | None = None
    if verify:
        verify_report = verify_monsters(root, frame_size=frame_size, frames=frames)
        if not verify_report.ok:
            raise BattleArtError(f"monster verification failed: {verify_report.errors}")

    _write_json(
        root / "MONSTER_MANIFEST.json",
        {
            "tool": "xuanming.ops.battle_art_gen",
            "frame_size": frame_size,
            "frames": frames,
            "monsters": manifest_entries,
            "verification": verify_report.to_dict() if verify_report else None,
        },
    )
    logger.info("Generated %d monster strips (%d monsters) in %s", len(records), len(selected), root)
    return CategoryResult(
        category="monsters",
        files=tuple(records),
        metadata={
            "monster_count": len(selected),
            "frame_size": frame_size,
            "frames": frames,
            "verify": verify_report.to_dict() if verify_report else None,
        },
    )


# ---------------------------------------------------------------------------
# 4. Buffs —— 64×64 增益/减益/控制状态图标
# ---------------------------------------------------------------------------

_BUFF_PALETTES: dict[str, tuple[RGB, RGB, RGB]] = {
    "buff": ((120, 226, 134), (26, 116, 62), (186, 255, 196)),
    "debuff": ((238, 118, 98), (138, 26, 30), (255, 192, 172)),
    "control": ((180, 134, 240), (72, 38, 140), (224, 198, 255)),
}
_BUFF_CATEGORIES = ("buff", "debuff", "control")
_BUFF_SYMBOLS = ("arrow_up", "arrow_down", "chain", "flame", "ice", "shield", "sword", "leaf")


def _draw_buff_symbol(canvas: RgbaCanvas, symbol_idx: int, size: int) -> None:
    s = size / 64.0
    sym = RgbaCanvas(size, size)
    fg = (248, 250, 255, 255)
    kind = symbol_idx % len(_BUFF_SYMBOLS)

    if kind == 0:  # arrow_up
        pts = ((32, 13), (47, 31), (39, 31), (39, 51), (25, 51), (25, 31), (17, 31))
        sym.fill_polygon([(x * s, y * s) for x, y in pts], fg)
    elif kind == 1:  # arrow_down
        pts = ((32, 51), (47, 33), (39, 33), (39, 13), (25, 13), (25, 33), (17, 33))
        sym.fill_polygon([(x * s, y * s) for x, y in pts], fg)
    elif kind == 2:  # chain
        for cx, cy in ((24 * s, 26 * s), (40 * s, 39 * s)):
            sym.fill_circle(cx, cy, 12.0 * s, fg)
            sym.fill_circle(cx, cy, 7.2 * s, (0, 0, 0, 0), blend=False)
    elif kind == 3:  # flame
        pts = ((35, 10), (46, 26), (45, 42), (35, 53), (31, 41), (24, 52), (16, 39), (21, 25))
        sym.fill_polygon([(x * s, y * s) for x, y in pts], fg)
    elif kind == 4:  # ice
        for i in range(3):
            a = i * math.pi / 3.0 + math.pi / 2.0
            dx, dy = math.cos(a) * 19 * s, math.sin(a) * 19 * s
            sym.draw_line(32 * s - dx, 32 * s - dy, 32 * s + dx, 32 * s + dy, fg, thickness=4.5 * s)
    elif kind == 5:  # shield
        pts = ((32, 12), (49, 19), (49, 34), (32, 53), (15, 34), (15, 19))
        sym.fill_polygon([(x * s, y * s) for x, y in pts], fg)
        sym.draw_line(32 * s, 20 * s, 32 * s, 45 * s, (0, 0, 0, 0), thickness=2.6 * s, blend=False)
    elif kind == 6:  # sword
        pts = ((32, 9), (38, 20), (38, 40), (26, 40), (26, 20))
        sym.fill_polygon([(x * s, y * s) for x, y in pts], fg)
        sym.fill_round_rect(18 * s, 40 * s, 46 * s, 46 * s, 2.5 * s, fg)
        sym.fill_round_rect(28.5 * s, 46 * s, 35.5 * s, 55 * s, 2.0 * s, fg)
    else:  # leaf
        pts = ((16, 49), (17, 27), (31, 13), (49, 13), (47, 32), (35, 47))
        sym.fill_polygon([(x * s, y * s) for x, y in pts], fg)
        sym.draw_line(11 * s, 54 * s, 20 * s, 45 * s, fg, thickness=3.0 * s)

    sym.apply_outline((26, 18, 30, 235), thickness=max(1, int(round(2 * s))))
    canvas.blit(sym, 0, 0, blend=True)


def generate_buffs(
    out_dir: Path | str,
    *,
    count: int = 24,
    size: int = 64,
) -> CategoryResult:
    """生成 64×64 增益/减益/控制徽章状态图标（默认 24 张）。"""
    root = Path(out_dir)
    s = size / 64.0
    records: list[AssetRecord] = []
    mapping: list[dict[str, Any]] = []

    for i in range(count):
        buff_id = i + 1
        cat = _BUFF_CATEGORIES[i % len(_BUFF_CATEGORIES)]
        sym_name = _BUFF_SYMBOLS[i % len(_BUFF_SYMBOLS)]
        top_rgb, bot_rgb, rim_rgb = _BUFF_PALETTES[cat]

        c = RgbaCanvas(size, size)
        # 深色厚描边底托
        c.fill_round_rect(2 * s, 2 * s, 62 * s, 62 * s, 17 * s, (24, 17, 28, 255))
        # 内框亮边
        c.fill_round_rect(5 * s, 5 * s, 59 * s, 59 * s, 14 * s, _rgba(rim_rgb, 230))
        # 竖向渐变主体
        c.fill_round_rect(
            7.5 * s,
            7.5 * s,
            56.5 * s,
            56.5 * s,
            12 * s,
            lambda y, t=top_rgb, b=bot_rgb: _rgba(_mix_rgb(t, b, (y - 7 * s) / (50 * s))),
        )
        # 顶部柔光
        c.fill_ellipse(32 * s, 17 * s, 20 * s, 7 * s, (255, 255, 255, 68), blend=True)
        _draw_buff_symbol(c, i, size)

        rel = f"Buff/{buff_id}.png"
        records.append(
            _emit_png(root, rel, c, "buffs", "buff_icon", desc=f"Buff {buff_id} ({cat} / {sym_name})")
        )
        mapping.append({"id": buff_id, "category": cat, "symbol": sym_name, "file": rel})

    logger.info("Generated %d buff icons in %s", len(records), root)
    return CategoryResult(
        category="buffs",
        files=tuple(records),
        metadata={"count": count, "size": size, "mapping": mapping},
    )


# ---------------------------------------------------------------------------
# 5. FX —— 9 套 8 帧技能特效横条
# ---------------------------------------------------------------------------

_FX_CATALOG: tuple[tuple[str, int, tuple[float, float], str], ...] = (
    ("slash_arc", 16, (0.5, 0.5), "Crescent cyan-white sword slash arc"),
    ("thrust_line", 16, (0.5, 0.5), "Piercing spear thrust with shockwave"),
    ("fire_burst", 12, (0.5, 0.5), "Fiery explosion burst with embers"),
    ("ice_shard", 14, (0.5, 0.5), "Crystalline frost shard burst"),
    ("lightning_strike", 16, (0.5, 0.5), "Jagged cyan-violet lightning bolt"),
    ("heal_ring", 10, (0.5, 0.1), "Emerald ground healing aura ring"),
    ("buff_rise", 10, (0.5, 0.1), "Golden rising buff pillars"),
    ("hit_star", 18, (0.5, 0.5), "Radial impact starburst"),
    ("death_dissolve", 10, (0.5, 0.1), "Rising violet-ash soul dissolve wisps"),
)


def _render_fx_frame(
    fx_id: str,
    t: float,
    rng: random.Random,
    size: int,
) -> RgbaCanvas:
    c = RgbaCanvas(size, size)
    s = size / 64.0
    cx, cy = size * 0.5, size * 0.5
    amp = math.sin(math.pi * _clamp01(0.08 + 0.84 * t))
    if amp < 0.08:
        amp = 0.08

    if fx_id == "slash_arc":
        a0 = -1.1 + 0.3 * t
        a1 = a0 + 1.5 * _ease_out(min(1.0, t * 1.4 + 0.15))
        steps = 16
        pts = [
            (10 * s + 38 * s * math.cos(a0 + (a1 - a0) * i / steps), cy + 38 * s * math.sin(a0 + (a1 - a0) * i / steps))
            for i in range(steps + 1)
        ]
        for a, b in zip(pts, pts[1:]):
            c.draw_line(a[0], a[1], b[0], b[1], (30, 150, 235, int(170 * amp)), thickness=7 * s, additive=True)
            c.draw_line(a[0], a[1], b[0], b[1], (255, 255, 255, int(235 * amp)), thickness=2.6 * s, additive=True)
    elif fx_id == "thrust_line":
        hx = (14 + 44 * _ease_out(t)) * s
        tx = max(4 * s, hx - 32 * s)
        c.draw_line(tx, cy, hx, cy, (90, 160, 235, int(185 * amp)), thickness=7 * s, additive=True)
        c.draw_line(tx + 6 * s, cy, hx, cy, (255, 255, 255, int(240 * amp)), thickness=2.5 * s, additive=True)
        c.fill_ellipse(hx, cy, 4 * s, (10 + 14 * t) * s, (190, 228, 255, int(190 * amp)), additive=True)
    elif fx_id == "fire_burst":
        r = (8 + 20 * _ease_out(t)) * s
        c.fill_circle(cx, cy - 3 * t * s, r, (196, 42, 22, int(165 * amp)), additive=True)
        c.fill_circle(cx, cy - 3 * t * s, r * 0.68, (255, 150, 45, int(210 * amp)), additive=True)
        c.fill_circle(cx, cy - 2 * t * s, r * 0.36, (255, 248, 205, int(245 * amp)), additive=True)
        for _ in range(8):
            ang = rng.uniform(0, 2 * math.pi)
            dist = r * rng.uniform(0.6, 1.25)
            c.fill_circle(cx + math.cos(ang) * dist, cy + math.sin(ang) * dist, 2.2 * s, (255, 210, 90, int(220 * amp)), additive=True)
    elif fx_id == "ice_shard":
        spread = (6 + 18 * _ease_out(t)) * s
        for i in range(6):
            ang = i * math.pi / 3.0 + 0.25
            px = cx + math.cos(ang) * spread
            py = cy + math.sin(ang) * spread
            tip = (px + math.cos(ang) * 9 * s, py + math.sin(ang) * 9 * s)
            left = (px - math.sin(ang) * 3.5 * s, py + math.cos(ang) * 3.5 * s)
            right = (px + math.sin(ang) * 3.5 * s, py - math.cos(ang) * 3.5 * s)
            c.fill_polygon((left, tip, right, (cx, cy)), (110, 220, 255, int(190 * amp)), additive=True)
        c.fill_circle(cx, cy, 7 * s, (235, 250, 255, int(235 * amp)), additive=True)
    elif fx_id == "lightning_strike":
        pts = [(cx, 4 * s)]
        for i in range(1, 6):
            jitter = rng.uniform(-9.0, 9.0) * s
            pts.append((cx + jitter, (4 + i * 10.5) * s))
        pts.append((cx, 58 * s))
        for a, b in zip(pts, pts[1:]):
            c.draw_line(a[0], a[1], b[0], b[1], (140, 120, 255, int(180 * amp)), thickness=6.0 * s, additive=True)
            c.draw_line(a[0], a[1], b[0], b[1], (240, 252, 255, int(245 * amp)), thickness=2.2 * s, additive=True)
    elif fx_id == "heal_ring":
        ry = (8 + 8 * _ease_out(t)) * s
        rx = ry * 2.2
        base_y = (52 - 14 * t) * s
        c.fill_ellipse(cx, base_y, rx, ry * 0.55, (72, 236, 148, int(185 * amp)), additive=True)
        c.fill_ellipse(cx, base_y, rx * 0.72, ry * 0.34, (0, 0, 0, 0), blend=False)
        for i in range(5):
            px = cx + (i - 2) * 8 * s
            py = base_y - (6 + ((i * 7 + int(t * 20)) % 22)) * s
            c.fill_circle(px, py, 2.4 * s, (210, 255, 224, int(225 * amp)), additive=True)
    elif fx_id == "buff_rise":
        for i in range(4):
            px = (16 + i * 11) * s
            y_top = max(8 * s, (46 - 30 * t - (i % 2) * 6) * s)
            c.draw_line(px, 56 * s, px, y_top, (255, 206, 84, int(210 * amp)), thickness=4.0 * s, additive=True)
            c.fill_circle(px, y_top, 3.0 * s, (255, 248, 210, int(240 * amp)), additive=True)
    elif fx_id == "hit_star":
        r = (10 + 18 * _ease_out(t)) * s
        for i in range(8):
            ang = i * math.pi / 4.0
            c.draw_line(
                cx,
                cy,
                cx + math.cos(ang) * r,
                cy + math.sin(ang) * r,
                (255, 226, 128, int(225 * amp)),
                thickness=3.2 * s,
                additive=True,
            )
        c.fill_circle(cx, cy, 8 * s * amp + 2 * s, (255, 255, 255, int(245 * amp)), additive=True)
    else:  # death_dissolve
        for _ in range(14):
            px = cx + rng.uniform(-16.0, 16.0) * s
            py = (54 - 42 * t + rng.uniform(-8.0, 8.0)) * s
            rad = rng.uniform(2.5, 5.5) * s * (1.0 - 0.35 * t)
            c.fill_circle(px, py, max(1.5, rad), (168, 108, 218, int(205 * amp)), additive=True)

    return c


def generate_fx(
    out_dir: Path | str,
    *,
    frame_size: int = 64,
    frames: int = 8,
    seed: int = DEFAULT_SEED,
) -> CategoryResult:
    """生成 9 套 8 帧技能特效横条 PNG。"""
    root = Path(out_dir)
    records: list[AssetRecord] = []

    for idx, (fx_id, fps, pivot, desc) in enumerate(_FX_CATALOG):
        rng = random.Random(seed + idx + 1)
        strip = RgbaCanvas(frame_size * frames, frame_size)
        for f in range(frames):
            t = f / max(1, frames - 1)
            frame = _render_fx_frame(fx_id, t, rng, frame_size)
            strip.blit(frame, f * frame_size, 0)
        rel = f"Fx/{fx_id}_strip.png"
        records.append(
            _emit_png(
                root,
                rel,
                strip,
                "fx",
                "fx_strip",
                frames=frames,
                desc=f"{desc} ({frames} frames @ {fps} FPS, pivot={pivot})",
            )
        )

    logger.info("Generated %d FX strips in %s", len(records), root)
    return CategoryResult(
        category="fx",
        files=tuple(records),
        metadata={"fx_count": len(records), "frame_size": frame_size, "frames": frames, "seed": seed},
    )


# ---------------------------------------------------------------------------
# 6. Digits —— 0-9 与符号伤害数字字集（零系统字体依赖）
# ---------------------------------------------------------------------------

_DIGIT_CHARS = "0123456789-+"

# 归一化 [0..1]x[0..1] 矢量笔画定义，跨平台零字体依赖。
_GLYPH_STROKES: dict[str, tuple[tuple[tuple[float, float], tuple[float, float]], ...]] = {
    "0": (
        ((0.22, 0.14), (0.78, 0.14)),
        ((0.78, 0.14), (0.78, 0.86)),
        ((0.78, 0.86), (0.22, 0.86)),
        ((0.22, 0.86), (0.22, 0.14)),
    ),
    "1": (
        ((0.34, 0.24), (0.54, 0.14)),
        ((0.54, 0.14), (0.54, 0.86)),
        ((0.30, 0.86), (0.76, 0.86)),
    ),
    "2": (
        ((0.20, 0.22), (0.50, 0.14)),
        ((0.50, 0.14), (0.78, 0.24)),
        ((0.78, 0.24), (0.78, 0.44)),
        ((0.78, 0.44), (0.20, 0.86)),
        ((0.20, 0.86), (0.80, 0.86)),
    ),
    "3": (
        ((0.22, 0.16), (0.76, 0.16)),
        ((0.76, 0.16), (0.76, 0.48)),
        ((0.34, 0.48), (0.76, 0.48)),
        ((0.76, 0.48), (0.76, 0.86)),
        ((0.22, 0.86), (0.76, 0.86)),
    ),
    "4": (
        ((0.68, 0.14), (0.20, 0.58)),
        ((0.20, 0.58), (0.82, 0.58)),
        ((0.68, 0.14), (0.68, 0.86)),
    ),
    "5": (
        ((0.78, 0.14), (0.24, 0.14)),
        ((0.24, 0.14), (0.24, 0.48)),
        ((0.24, 0.48), (0.76, 0.48)),
        ((0.76, 0.48), (0.76, 0.86)),
        ((0.22, 0.86), (0.76, 0.86)),
    ),
    "6": (
        ((0.74, 0.14), (0.24, 0.14)),
        ((0.24, 0.14), (0.24, 0.86)),
        ((0.24, 0.86), (0.78, 0.86)),
        ((0.78, 0.86), (0.78, 0.50)),
        ((0.78, 0.50), (0.24, 0.50)),
    ),
    "7": (
        ((0.20, 0.14), (0.80, 0.14)),
        ((0.80, 0.14), (0.40, 0.86)),
    ),
    "8": (
        ((0.22, 0.14), (0.78, 0.14)),
        ((0.78, 0.14), (0.78, 0.86)),
        ((0.78, 0.86), (0.22, 0.86)),
        ((0.22, 0.86), (0.22, 0.14)),
        ((0.22, 0.50), (0.78, 0.50)),
    ),
    "9": (
        ((0.78, 0.50), (0.22, 0.50)),
        ((0.22, 0.50), (0.22, 0.14)),
        ((0.22, 0.14), (0.78, 0.14)),
        ((0.78, 0.14), (0.78, 0.86)),
        ((0.24, 0.86), (0.78, 0.86)),
    ),
    "-": (((0.22, 0.50), (0.78, 0.50)),),
    "+": (
        ((0.22, 0.50), (0.78, 0.50)),
        ((0.50, 0.24), (0.50, 0.76)),
    ),
}

_DIGIT_VARIANTS: tuple[dict[str, Any], ...] = (
    {
        "name": "normal",
        "top": (255, 255, 255),
        "bot": (214, 220, 232),
        "stroke": (16, 14, 18),
        "thickness": 7.0,
        "outline": 3,
        "note": "Normal damage: white fill with dark outline",
    },
    {
        "name": "crit",
        "top": (255, 236, 130),
        "bot": (255, 158, 34),
        "stroke": (104, 14, 10),
        "thickness": 8.5,
        "outline": 3,
        "note": "Critical hit: gold gradient with crimson outline",
    },
    {
        "name": "heal",
        "top": (206, 255, 224),
        "bot": (70, 224, 138),
        "stroke": (8, 68, 40),
        "thickness": 7.0,
        "outline": 3,
        "note": "Healing: emerald gradient with deep green outline",
    },
    {
        "name": "miss",
        "top": (232, 235, 242),
        "bot": (158, 164, 178),
        "stroke": (64, 68, 78),
        "thickness": 6.0,
        "outline": 2,
        "note": "Miss/dodge: muted silver-gray",
    },
)


def generate_digits(
    out_dir: Path | str,
    *,
    cell_w: int = 48,
    cell_h: int = 64,
) -> CategoryResult:
    """生成 4 套配色伤害数字精灵条（0-9 及 +/-）与 ``UI/digits_meta.json``。"""
    root = Path(out_dir)
    records: list[AssetRecord] = []
    variants_meta: list[dict[str, str]] = []

    for var in _DIGIT_VARIANTS:
        strip = RgbaCanvas(cell_w * len(_DIGIT_CHARS), cell_h)
        top_rgb: RGB = var["top"]
        bot_rgb: RGB = var["bot"]
        stroke_rgb: RGB = var["stroke"]
        thick: float = var["thickness"]
        outline_px: int = var["outline"]

        for idx, ch in enumerate(_DIGIT_CHARS):
            cell = RgbaCanvas(cell_w, cell_h)
            for (x0, y0), (x1, y1) in _GLYPH_STROKES[ch]:
                cell.draw_line(
                    x0 * cell_w,
                    y0 * cell_h,
                    x1 * cell_w,
                    y1 * cell_h,
                    (255, 255, 255, 255),
                    thickness=thick,
                )
            # 对实心笔画套竖向渐变着色，再加厚外描边。
            p = cell.pixels
            for y in range(cell_h):
                r, g, b = _mix_rgb(top_rgb, bot_rgb, y / max(1, cell_h - 1))
                row_off = y * cell_w * 4
                for x in range(cell_w):
                    off = row_off + x * 4
                    if p[off + 3] >= 128:
                        p[off] = r
                        p[off + 1] = g
                        p[off + 2] = b
                        p[off + 3] = 255
            cell.apply_outline(_rgba(stroke_rgb), thickness=outline_px)
            strip.blit(cell, idx * cell_w, 0)

        file_name = f"digits_{var['name']}.png"
        rel = f"UI/{file_name}"
        records.append(
            _emit_png(
                root,
                rel,
                strip,
                "digits",
                "digits_strip",
                frames=len(_DIGIT_CHARS),
                desc=var["note"],
            )
        )
        variants_meta.append({"name": var["name"], "file": file_name, "note": var["note"]})

    digits_meta = {
        "cell_width": cell_w,
        "cell_height": cell_h,
        "cell_count": len(_DIGIT_CHARS),
        "chars_string": _DIGIT_CHARS,
        "chars": list(_DIGIT_CHARS),
        "char_index": {ch: i for i, ch in enumerate(_DIGIT_CHARS)},
        "font": "procedural-vector-strokes",
        "variants": variants_meta,
    }
    _write_json(root / "UI/digits_meta.json", digits_meta)
    logger.info("Generated %d digit atlases in %s", len(records), root)
    return CategoryResult(category="digits", files=tuple(records), metadata=digits_meta)


# ---------------------------------------------------------------------------
# 7. UI —— 战斗 HUD 面板、按钮、HP/MP 条与 7 扇区命令环
# ---------------------------------------------------------------------------


def _make_panel_9slice(size: int = 128) -> RgbaCanvas:
    c = RgbaCanvas(size, size)
    s = size / 128.0
    c.fill_round_rect(2 * s, 2 * s, 126 * s, 126 * s, 14 * s, (26, 15, 10, 255))
    c.fill_round_rect(
        4 * s,
        4 * s,
        124 * s,
        124 * s,
        12 * s,
        lambda y: _rgba(_mix_rgb((255, 231, 152), (168, 120, 38), y / size)),
    )
    c.fill_round_rect(
        10 * s,
        10 * s,
        118 * s,
        118 * s,
        8 * s,
        lambda y: _rgba(_mix_rgb((92, 57, 35), (56, 32, 20), y / size)),
    )
    for rx, ry in ((16, 16), (112, 16), (16, 112), (112, 112)):
        c.fill_circle(rx * s, ry * s, 4.5 * s, (26, 15, 10, 255))
        c.fill_circle(rx * s, ry * s, 3.2 * s, (240, 202, 108, 255))
    return c


def _make_button_9slice() -> RgbaCanvas:
    w, h = 160, 64
    c = RgbaCanvas(w, h)
    c.fill_round_rect(2, 2, w - 2, h - 2, 16, (26, 15, 10, 255))
    c.fill_round_rect(4, 4, w - 4, h - 4, 14, lambda y: _rgba(_mix_rgb((255, 231, 152), (156, 110, 36), y / h)))
    c.fill_round_rect(
        10,
        10,
        w - 10,
        h - 10,
        10,
        lambda y: _rgba(_mix_rgb((222, 248, 232), (84, 158, 130), (y - 10) / (h - 20))),
    )
    c.fill_ellipse(w * 0.5, 18, 62, 9, (255, 255, 255, 76), blend=True)
    return c


def _make_bar_slot() -> RgbaCanvas:
    w, h = 256, 32
    c = RgbaCanvas(w, h)
    c.fill_round_rect(1, 1, w - 1, h - 1, 14, (26, 15, 10, 255))
    c.fill_round_rect(3, 3, w - 3, h - 3, 12, lambda y: _rgba(_mix_rgb((245, 214, 126), (146, 100, 30), y / h)))
    c.fill_round_rect(
        6,
        6,
        w - 6,
        h - 6,
        9,
        lambda y: _rgba(_mix_rgb((18, 11, 10), (66, 44, 34), (y - 6) / (h - 12))),
    )
    return c


def _make_bar_fill(top_rgb: RGB, bot_rgb: RGB) -> RgbaCanvas:
    w, h = 240, 20
    c = RgbaCanvas(w, h)
    c.fill_round_rect(
        1,
        1,
        w - 1,
        h - 1,
        8,
        lambda y: _rgba(_mix_rgb(top_rgb, bot_rgb, y / max(1, h - 1))),
    )
    c.fill_rect(8, 3, w - 16, 4, (255, 255, 255, 84), blend=True)
    return c


def _make_command_ring(size: int = 256) -> RgbaCanvas:
    c = RgbaCanvas(size, size)
    cx = cy = size * 0.5
    r_out = size * 0.46
    r_in = size * 0.19

    c.fill_circle(cx, cy, r_out + 2.5, (26, 15, 10, 255))
    c.fill_circle(cx, cy, r_out, (216, 163, 55, 255))
    c.fill_circle(cx, cy, r_out - 6, (76, 48, 30, 255))
    c.fill_circle(cx, cy, r_in + 6, (216, 163, 55, 255))
    c.fill_circle(cx, cy, r_in, (0, 0, 0, 0), blend=False)

    sectors = 7
    step = 2.0 * math.pi / sectors
    start = -math.pi * 0.5 - step * 0.5
    for i in range(sectors):
        a = start + i * step
        c.draw_line(
            cx + math.cos(a) * (r_in + 4),
            cy + math.sin(a) * (r_in + 4),
            cx + math.cos(a) * (r_out - 4),
            cy + math.sin(a) * (r_out - 4),
            (26, 15, 10, 255),
            thickness=4.0,
        )
        c.draw_line(
            cx + math.cos(a) * (r_in + 4),
            cy + math.sin(a) * (r_in + 4),
            cx + math.cos(a) * (r_out - 4),
            cy + math.sin(a) * (r_out - 4),
            (236, 192, 88, 255),
            thickness=1.6,
        )
        mid = a + step * 0.5
        c.fill_circle(
            cx + math.cos(mid) * (r_out - 13),
            cy + math.sin(mid) * (r_out - 13),
            3.2,
            (255, 231, 152, 255),
        )
    return c


def generate_ui(out_dir: Path | str) -> CategoryResult:
    """生成战斗 HUD 面板、按钮、血条槽、HP/MP 填充条与 7 扇区命令环。"""
    root = Path(out_dir)
    jobs: tuple[tuple[str, RgbaCanvas, str, str], ...] = (
        ("panel_9slice", _make_panel_9slice(), "ui_9slice", "Gold-trimmed wood HUD panel"),
        ("button_9slice", _make_button_9slice(), "ui_9slice", "Jade-gold battle action button"),
        ("bar_slot", _make_bar_slot(), "ui_9slice", "HP/MP recessed bar slot"),
        ("hp_bar_fill", _make_bar_fill((246, 98, 86), (168, 24, 28)), "ui_fill", "HP bar crimson fill"),
        ("mp_bar_fill", _make_bar_fill((98, 186, 255), (32, 90, 196)), "ui_fill", "MP bar sapphire fill"),
        ("command_ring", _make_command_ring(), "ui", "7-sector Daoist battle command wheel"),
    )
    records = [
        _emit_png(root, f"UI/{name}.png", canvas, "ui", kind, desc=desc)
        for name, canvas, kind, desc in jobs
    ]
    logger.info("Generated %d UI HUD assets in %s", len(records), root)
    return CategoryResult(
        category="ui",
        files=tuple(records),
        metadata={"ui_count": len(records)},
    )


# ---------------------------------------------------------------------------
# 统一调度与 CLI 入口
# ---------------------------------------------------------------------------


def generate_all(
    out_dir: Path | str,
    categories: Sequence[str] | None = None,
    *,
    seed: int = DEFAULT_SEED,
    verify: bool = True,
) -> GenerationReport:
    """按指定大类（默认全 7 类）确定性生成战斗美术资产并可选校验全部 PNG。"""
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)

    selected = tuple(ALL_CATEGORIES if categories is None else categories)
    invalid = [cat for cat in selected if cat not in ALL_CATEGORIES]
    if invalid:
        raise BattleArtError(
            f"unknown categories {invalid!r}; valid choices: {list(ALL_CATEGORIES)}"
        )

    dispatch: dict[str, Callable[[], CategoryResult]] = {
        "ground": lambda: generate_ground(root),
        "characters": lambda: generate_characters(root),
        "monsters": lambda: generate_monsters(root, verify=verify),
        "buffs": lambda: generate_buffs(root),
        "fx": lambda: generate_fx(root, seed=seed),
        "digits": lambda: generate_digits(root),
        "ui": lambda: generate_ui(root),
    }

    results: dict[str, CategoryResult] = {}
    all_records: list[AssetRecord] = []
    for cat in selected:
        res = dispatch[cat]()
        results[cat] = res
        all_records.extend(res.files)

    monster_verify: MonsterVerifyReport | None = None
    if verify:
        for rec in all_records:
            info = decode_png_header((root / rec.path).read_bytes())
            if info.width != rec.width or info.height != rec.height:
                raise BattleArtError(f"PNG header mismatch for {rec.path}")
        if "monsters" in results:
            monster_verify = verify_monsters(root)
            if not monster_verify.ok:
                raise BattleArtError(f"monster verification failed: {monster_verify.errors}")

    manifest_path = root / "ART_MANIFEST.json"
    _write_json(
        manifest_path,
        {
            "tool": "xuanming.ops.battle_art_gen",
            "seed": seed,
            "categories": list(selected),
            "total_png_files": len(all_records),
            "files": [rec.to_dict() for rec in all_records],
            "monster_verify": monster_verify.to_dict() if monster_verify else None,
        },
    )
    logger.info(
        "Generated %d battle art PNG files across %d categories in %s",
        len(all_records),
        len(selected),
        root,
    )
    return GenerationReport(
        out_dir=str(root),
        seed=seed,
        categories=selected,
        total_png_files=len(all_records),
        results=results,
        verified=verify,
        monster_verify=monster_verify,
        manifest_path=str(manifest_path),
    )


def _parse_only_categories(values: Sequence[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    out: list[str] = []
    for raw in values:
        for item in raw.split(","):
            token = item.strip().lower()
            if token and token not in out:
                out.append(token)
    return tuple(out) if out else None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="battle-art-gen",
        description="Procedural pure-Python PNG battle art generator.",
    )
    parser.add_argument("--out", required=True, type=Path, help="Output directory for generated PNGs")
    parser.add_argument(
        "--only",
        action="append",
        default=None,
        help="Comma-separated categories to generate: " + ",".join(ALL_CATEGORIES),
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Deterministic RNG seed")
    parser.add_argument(
        "--verify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verify PNG headers and monster silhouettes after generation",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Emit JSON report")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """战斗美术程序化生成命令行入口。"""
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        categories = _parse_only_categories(args.only)
        report = generate_all(
            args.out,
            categories=categories,
            seed=args.seed,
            verify=args.verify,
        )
        if args.as_json:
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        else:
            print(
                f"GENERATED {report.total_png_files} PNG files "
                f"({','.join(report.categories)}) verified={report.verified} -> {report.out_dir}"
            )
        return 0
    except Exception as exc:
        if getattr(args, "as_json", False):
            print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
