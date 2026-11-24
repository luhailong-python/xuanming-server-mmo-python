"""词库模型、文本编解码（``#content_guard_lexicon 1``）与字典树匹配器。

与 Go 版 ``content_guard/internal/wordlist`` 保持逐项语义一致：
- 内置 6 条防冒充官方词永远生效且优先级高于外部词库；
- 支持 ``sub``（子串）、``token``（整词，需连续且前后无 ASCII 字母数字）、``prefix``（开头）三种模式；
- 同一匹配起点报告所有成立的命中，不在首个命中处早停，避免掩盖更高严重度的拒绝词；
- 完整落在任意白名单词范围内的拒绝命中会被豁免。

本模块纯内存计算，不连外部依赖，不打日志。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import IntEnum

from .textnorm import Elem, fold_word, is_ascii_alnum

MAX_WORD_RUNES = 64
MAX_ENTRIES = 1_000_000
FORMAT_HEADER = "#content_guard_lexicon 1"


class LexiconFormatError(ValueError):
    """词库文本格式不合法。"""


class LexiconWordError(ValueError):
    """单个词条不符合长度、字符或枚举约束。"""


class Category(IntEnum):
    """命中类别；数值与 ``content_guard.TextCategory`` 枚举保持一一对应。"""

    UNSPECIFIED = 0
    POLITICS = 1
    PORN = 2
    ABUSE = 3
    AD = 4
    IMPERSONATION = 5
    OTHER = 6


_CATEGORY_NAMES: dict[Category, str] = {
    Category.POLITICS: "politics",
    Category.PORN: "porn",
    Category.ABUSE: "abuse",
    Category.AD: "ad",
    Category.IMPERSONATION: "impersonation",
    Category.OTHER: "other",
}
_NAME_TO_CATEGORY: dict[str, Category] = {name: cat for cat, name in _CATEGORY_NAMES.items()}


def category_name(category: Category | int) -> str:
    """返回类别规范名；未指定或越界返回 ``"none"``。"""
    try:
        cat = Category(category)
    except ValueError:
        return "none"
    return _CATEGORY_NAMES.get(cat, "none")


def parse_category(name: str) -> Category | None:
    """按文本名解析类别；不接受 ``"none"``。"""
    return _NAME_TO_CATEGORY.get(name)


class Mode(IntEnum):
    """词条匹配方式。"""

    SUB = 0
    TOKEN = 1
    PREFIX = 2


_MODE_NAMES: dict[Mode, str] = {
    Mode.SUB: "sub",
    Mode.TOKEN: "token",
    Mode.PREFIX: "prefix",
}
_NAME_TO_MODE: dict[str, Mode] = {name: mode for mode, name in _MODE_NAMES.items()}


def mode_name(mode: Mode | int) -> str:
    """返回匹配方式名；越界返回 ``"invalid"``。"""
    try:
        parsed = Mode(mode)
    except ValueError:
        return "invalid"
    return _MODE_NAMES.get(parsed, "invalid")


def parse_mode(name: str) -> Mode | None:
    """按文本名解析匹配方式。"""
    return _NAME_TO_MODE.get(name)


def default_mode(word: str) -> Mode:
    """未显式指定模式时的默认取法：长度 <= 3 的纯 ASCII 词走整词，其余走子串。"""
    if len(word) <= 3 and all(ord(ch) < 128 for ch in word):
        return Mode.TOKEN
    return Mode.SUB


@dataclass(frozen=True, slots=True)
class Entry:
    """单条拒绝词。"""

    word: str
    category: Category
    mode: Mode


@dataclass(frozen=True, slots=True)
class Lexicon:
    """完整词库：拒绝词表与白名单词表。"""

    deny: tuple[Entry, ...] = ()
    allow: tuple[str, ...] = ()


# 内置防冒充官方词：不依赖 etcd 词库，永远先写入字典树。
BUILTIN_DENY: tuple[Entry, ...] = (
    Entry("管理员", Category.IMPERSONATION, Mode.SUB),
    Entry("客服", Category.IMPERSONATION, Mode.SUB),
    Entry("官方", Category.IMPERSONATION, Mode.SUB),
    Entry("系统", Category.IMPERSONATION, Mode.SUB),
    Entry("运营", Category.IMPERSONATION, Mode.SUB),
    Entry("gm", Category.IMPERSONATION, Mode.PREFIX),
)


@dataclass(frozen=True, slots=True)
class CompileReport:
    """词库编译统计。"""

    deny_words: int
    allow_words: int
    dropped: int
    duplicates: int


@dataclass(frozen=True, slots=True)
class Hit:
    """单处词库命中；``start``/``end`` 为原文码点闭区间下标。"""

    start: int
    end: int
    category: Category


@dataclass(frozen=True, slots=True)
class OrigSpan:
    """原文码点闭区间，供外围联系方式检测对齐白名单豁免。"""

    start: int
    end: int


def orig_covered(spans: Sequence[OrigSpan], start_orig: int, end_orig: int) -> bool:
    """判断原文闭区间 ``[start_orig, end_orig]`` 是否完全落在任意白名单区间内。"""
    for span in spans:
        if span.start <= start_orig and end_orig <= span.end:
            return True
    return False


def validate_word(word: str) -> None:
    """校验词条文本合法性：非空、合法 UTF-8、无制表符/换行且不超过码点上限。"""
    if not word:
        raise LexiconWordError("wordlist: invalid word: empty")
    try:
        word.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise LexiconWordError("wordlist: invalid word: not valid UTF-8") from exc
    if "\t" in word or "\r" in word or "\n" in word:
        raise LexiconWordError("wordlist: invalid word: contains a tab or line break")
    if len(word) > MAX_WORD_RUNES:
        raise LexiconWordError(
            f"wordlist: invalid word: {len(word)} characters, the limit is {MAX_WORD_RUNES}"
        )


def _validate_entry(entry: Entry) -> None:
    validate_word(entry.word)
    if entry.category == Category.UNSPECIFIED or entry.category not in _CATEGORY_NAMES:
        raise LexiconWordError(f"wordlist: invalid word: category {int(entry.category)} out of range")
    if entry.mode not in _MODE_NAMES:
        raise LexiconWordError(f"wordlist: invalid word: mode {int(entry.mode)} out of range")


def marshal_lexicon(lexicon: Lexicon) -> bytes:
    """将词库编码为 ``#content_guard_lexicon 1`` 制表符文本；任何条目非法均整体失败。"""
    total = len(lexicon.deny) + len(lexicon.allow)
    if total > MAX_ENTRIES:
        raise LexiconFormatError(f"wordlist: malformed lexicon text: {total} entries, the limit is {MAX_ENTRIES}")
    lines = [FORMAT_HEADER]
    for entry in lexicon.deny:
        _validate_entry(entry)
        lines.append(f"D\t{_CATEGORY_NAMES[entry.category]}\t{_MODE_NAMES[entry.mode]}\t{entry.word}")
    for word in lexicon.allow:
        validate_word(word)
        lines.append(f"A\t{word}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def parse_lexicon(raw: bytes) -> Lexicon:
    """解析词库文本；任何一行非法都整体拒绝，确保调用方保留旧快照而非加载半份词库。"""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise LexiconFormatError("wordlist: malformed lexicon text: not valid UTF-8") from exc

    lines = text.split("\n")
    if not lines or lines[0].rstrip("\r") != FORMAT_HEADER:
        raise LexiconFormatError(f"wordlist: malformed lexicon text: first line must be {FORMAT_HEADER!r}")

    deny: list[Entry] = []
    allow: list[str] = []
    for idx, raw_line in enumerate(lines[1:], start=2):
        line = raw_line.rstrip("\r")
        if not line:
            continue
        fields = line.split("\t")
        record_type = fields[0]
        if record_type == "D":
            if len(fields) != 4:
                raise LexiconFormatError(
                    f"wordlist: malformed lexicon text: line {idx}: a deny entry needs 4 tab-separated fields"
                )
            category = parse_category(fields[1])
            if category is None:
                raise LexiconFormatError(
                    f"wordlist: malformed lexicon text: line {idx}: unknown category {fields[1]!r}"
                )
            mode = parse_mode(fields[2])
            if mode is None:
                raise LexiconFormatError(
                    f"wordlist: malformed lexicon text: line {idx}: unknown mode {fields[2]!r}"
                )
            entry = Entry(word=fields[3], category=category, mode=mode)
            try:
                _validate_entry(entry)
            except LexiconWordError as exc:
                raise LexiconWordError(f"line {idx}: {exc}") from exc
            deny.append(entry)
        elif record_type == "A":
            if len(fields) != 2:
                raise LexiconFormatError(
                    f"wordlist: malformed lexicon text: line {idx}: an allow entry needs 2 tab-separated fields"
                )
            try:
                validate_word(fields[1])
            except LexiconWordError as exc:
                raise LexiconWordError(f"line {idx}: {exc}") from exc
            allow.append(fields[1])
        else:
            raise LexiconFormatError(
                f"wordlist: malformed lexicon text: line {idx}: unknown record type {record_type!r}"
            )
        if len(deny) + len(allow) > MAX_ENTRIES:
            raise LexiconFormatError(f"wordlist: malformed lexicon text: more than {MAX_ENTRIES} entries")
    return Lexicon(deny=tuple(deny), allow=tuple(allow))


class _TrieNode:
    __slots__ = ("children", "term")

    def __init__(self) -> None:
        self.children: dict[str, _TrieNode] = {}
        # 下标对应 Mode(0=sub, 1=token, 2=prefix)；0 表示非词尾。
        self.term: list[Category] | None = None


def _walk_trie(
    root: _TrieNode,
    elems: Sequence[Elem],
    start: int,
    visit: Callable[[int, list[Category], bool], None],
) -> None:
    """从 ``elems[start]`` 起沿字典树前进，跳过可跳过字符，并在每个词尾回调 ``visit``。"""
    node = root
    contiguous = True
    skipped = False
    for idx in range(start, len(elems)):
        elem = elems[idx]
        if elem.skip:
            skipped = True
            continue
        nxt = node.children.get(elem.r)
        if nxt is None:
            return
        if skipped:
            contiguous = False
        node = nxt
        if node.term is not None:
            visit(idx, node.term, contiguous)


def _token_bounded(elems: Sequence[Elem], start: int, end: int) -> bool:
    """整词边界校验：前后均不能紧挨 ASCII 字母或数字。"""
    if start > 0 and is_ascii_alnum(elems[start - 1].r):
        return False
    if end + 1 < len(elems) and is_ascii_alnum(elems[end + 1].r):
        return False
    return True


def _covered(spans: Sequence[tuple[int, int]], start: int, end: int) -> bool:
    for span_start, span_end in spans:
        if span_start <= start and end <= span_end:
            return True
    return False


class Matcher:
    """只读编译态词库匹配器，可被并发请求安全共享。"""

    __slots__ = ("_deny_root", "_allow_root", "_has_allow")

    def __init__(self, deny_root: _TrieNode, allow_root: _TrieNode, has_allow: bool) -> None:
        self._deny_root = deny_root
        self._allow_root = allow_root
        self._has_allow = has_allow

    def _allow_spans(self, elems: Sequence[Elem]) -> list[tuple[int, int]]:
        if not self._has_allow:
            return []
        spans: list[tuple[int, int]] = []
        allow_root = self._allow_root
        for start, elem in enumerate(elems):
            if elem.skip:
                continue
            _walk_trie(
                allow_root,
                elems,
                start,
                lambda end, _term, _contiguous, s=start: spans.append((s, end)),
            )
        return spans

    def allow_orig_spans(self, elems: Sequence[Elem]) -> list[OrigSpan]:
        """返回命中的全部白名单词对应的原文码点闭区间。"""
        if not elems or not self._has_allow:
            return []
        spans = self._allow_spans(elems)
        return [OrigSpan(start=elems[s].orig, end=elems[e].orig) for s, e in spans]

    def find(self, elems: Sequence[Elem]) -> list[Hit]:
        """返回折叠序列中的全部未豁免命中（按起点从前到后）。"""
        first_real = -1
        for idx, elem in enumerate(elems):
            if not elem.skip:
                first_real = idx
                break
        if first_real < 0:
            return []

        allowed = self._allow_spans(elems)
        hits: list[Hit] = []
        deny_root = self._deny_root
        for start in range(first_real, len(elems)):
            if elems[start].skip:
                continue

            def _on_term(end: int, term: list[Category], contiguous: bool, s: int = start) -> None:
                if allowed and _covered(allowed, s, end):
                    return
                reported: list[Category] = []

                def _emit(cat: Category) -> None:
                    if cat not in reported:
                        reported.append(cat)
                        hits.append(Hit(start=elems[s].orig, end=elems[end].orig, category=cat))

                sub_cat = term[Mode.SUB]
                if sub_cat != Category.UNSPECIFIED:
                    _emit(sub_cat)
                prefix_cat = term[Mode.PREFIX]
                if prefix_cat != Category.UNSPECIFIED and s == first_real:
                    _emit(prefix_cat)
                token_cat = term[Mode.TOKEN]
                if token_cat != Category.UNSPECIFIED and contiguous and _token_bounded(elems, s, end):
                    _emit(token_cat)

            _walk_trie(deny_root, elems, start, _on_term)
        return hits


def compile_matcher(lexicon: Lexicon | None) -> tuple[Matcher, CompileReport]:
    """将词库编译为只读 ``Matcher``；``lexicon`` 为 ``None`` 时仅加载内置防冒充词。"""
    deny_root = _TrieNode()
    allow_root = _TrieNode()
    deny_words = 0
    allow_words = 0
    dropped = 0
    duplicates = 0

    def _insert(root: _TrieNode, word: str, mode: Mode, category: Category) -> bool:
        nonlocal dropped, duplicates
        if mode not in _MODE_NAMES or category == Category.UNSPECIFIED or category not in _CATEGORY_NAMES:
            dropped += 1
            return False
        folded = fold_word(word)
        if not folded:
            dropped += 1
            return False
        node = root
        for ch in folded:
            nxt = node.children.get(ch)
            if nxt is None:
                nxt = _TrieNode()
                node.children[ch] = nxt
            node = nxt
        if node.term is None:
            node.term = [Category.UNSPECIFIED, Category.UNSPECIFIED, Category.UNSPECIFIED]
        if node.term[mode] != Category.UNSPECIFIED:
            duplicates += 1
            return False
        node.term[mode] = category
        return True

    for entry in BUILTIN_DENY:
        if _insert(deny_root, entry.word, entry.mode, entry.category):
            deny_words += 1

    if lexicon is not None:
        for entry in lexicon.deny:
            if _insert(deny_root, entry.word, entry.mode, entry.category):
                deny_words += 1
        for word in lexicon.allow:
            if _insert(allow_root, word, Mode.SUB, Category.OTHER):
                allow_words += 1

    report = CompileReport(
        deny_words=deny_words,
        allow_words=allow_words,
        dropped=dropped,
        duplicates=duplicates,
    )
    return Matcher(deny_root, allow_root, has_allow=allow_words > 0), report
