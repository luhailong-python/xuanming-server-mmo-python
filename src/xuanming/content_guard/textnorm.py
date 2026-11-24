"""文本折叠：把玩家原文折成用于匹配的规范序列，并保留每个折叠字符对应的原文下标。

顺序与 Go 版 ``content_guard/internal/textnorm`` 完全一致：
1. NFKD 兼容分解（全角转半角、带圈数字/上下标转数字、带音标字母拆为基字母+组合记号）；
2. 转小写；
3. 按内嵌 ``t2s.txt`` 繁转简；
4. 标记可跳过字符（空白、标点、符号、控制符、格式符、组合记号、私用区）。

本模块纯内存计算，不连外部依赖，不打日志。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import unicodedata

_T2S_PATH = Path(__file__).with_name("t2s.txt")


def _load_t2s_table(path: Path) -> dict[str, str]:
    """解析随代码提交的繁简字表；格式损坏直接抛错，避免带着半张表对外判定。"""
    data = path.read_text(encoding="utf-8")
    table: dict[str, str] = {}
    for raw_line in data.split("\n"):
        line = raw_line.rstrip("\r")
        if not line or line.startswith("#"):
            continue
        if len(line) != 2 or line[0] == line[1]:
            raise ValueError(f"textnorm: malformed t2s.txt line: {line!r}")
        table[line[0]] = line[1]
    return table


_T2S: dict[str, str] = _load_t2s_table(_T2S_PATH)


@dataclass(frozen=True, slots=True)
class Elem:
    """折叠后的单个字符及其在原文中的码点下标。"""

    r: str
    orig: int
    skip: bool


def is_ascii_alnum(ch: str) -> bool:
    """判断是否为 ASCII 字母或数字，用于整词匹配的边界判定。"""
    return ("0" <= ch <= "9") or ("a" <= ch <= "z") or ("A" <= ch <= "Z")


def is_mark(ch: str) -> bool:
    """判断原文字符是否为非间距或包围型组合记号（Mn / Me）。"""
    return unicodedata.category(ch) in ("Mn", "Me")


def skippable(ch: str) -> bool:
    """判断折叠后的字符在匹配时是否跳过。"""
    if ord(ch) < 128:
        return not is_ascii_alnum(ch)
    cat = unicodedata.category(ch)
    # Z*: 空白分隔；P*: 标点；S*: 符号（含表情）；Cc: 控制符；Cf: 格式符（含零宽字符）；Mn/Me: 组合记号；Co: 私用区
    return cat[0] in ("Z", "P", "S") or cat in ("Cc", "Cf", "Mn", "Me", "Co")


def fold(text: str) -> tuple[list[Elem], list[str]]:
    """逐码点折叠原文，返回 ``(折叠序列, 原文码点列表)``。"""
    orig = list(text)
    elems: list[Elem] = []
    t2s = _T2S
    for idx, ch in enumerate(orig):
        code = ord(ch)
        if code < 128:
            if 65 <= code <= 90:
                ch = chr(code + 32)
            elems.append(Elem(r=ch, orig=idx, skip=not is_ascii_alnum(ch)))
            continue
        for decomposed in unicodedata.normalize("NFKD", ch):
            for lowered in decomposed.lower():
                simplified = t2s.get(lowered, lowered)
                elems.append(Elem(r=simplified, orig=idx, skip=skippable(simplified)))
    return elems, orig


def fold_word(word: str) -> str:
    """折叠词条并剔除可跳过字符；全为符号时返回空串。"""
    elems, _ = fold(word)
    return "".join(elem.r for elem in elems if not elem.skip)
