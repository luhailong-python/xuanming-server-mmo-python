"""内容安全本地判定引擎：维护当前词库快照，并按场景大类决定放行、打码或拒绝。

词库热更新采用不可变 ``Snapshot`` 整体替换，在途判定始终读取调用起始时的同一份快照。
本模块纯内存计算，不连外部依赖，不打日志。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import time

from .contact import contains_ad
from .textnorm import fold, is_mark
from .wordlist import (
    Category,
    CompileReport,
    Lexicon,
    Matcher,
    compile_matcher,
    orig_covered,
)


class SceneClass(IntEnum):
    """协议场景归并后的动作大类。"""

    NAME = 0
    NOTICE = 1
    CHAT = 2
    PRIVATE_LABEL = 3


class _Action(IntEnum):
    IGNORE = 0
    MASK = 1
    REJECT = 2


def _action_for(scene_class: SceneClass | int, category: Category) -> _Action:
    """全部场景×类别动作表；不认识的场景大类默认按 fail-closed 拒绝。"""
    if scene_class == SceneClass.CHAT:
        if category == Category.IMPERSONATION:
            return _Action.IGNORE
        if category == Category.ABUSE:
            return _Action.MASK
        return _Action.REJECT
    if scene_class == SceneClass.NOTICE:
        if category == Category.IMPERSONATION:
            return _Action.IGNORE
        return _Action.REJECT
    # SceneName / ScenePrivateLabel 以及任意未知场景大类一律拒绝
    return _Action.REJECT


class Verdict(IntEnum):
    """本地判定结局；数值与 ``content_guard.TextVerdict`` 一致。"""

    PASS = 1
    MASKED = 2
    REJECT = 3


@dataclass(frozen=True, slots=True)
class CheckResult:
    """单次本地判定结果。"""

    verdict: Verdict
    masked: str = ""
    category: Category = Category.UNSPECIFIED
    version: int = 0


@dataclass(frozen=True, slots=True)
class Snapshot:
    """当前生效的一份不可变词库快照。"""

    version: int
    matcher: Matcher
    report: CompileReport
    loaded_at: float


def _mask_span(orig: list[str], start: int, end: int) -> None:
    """把 ``orig[start..end]`` 及紧跟其后的组合记号（Mn / Me）一并替换为 ``*``。"""
    last = end
    limit = len(orig)
    while last + 1 < limit and is_mark(orig[last + 1]):
        last += 1
    for idx in range(start, min(last + 1, limit)):
        orig[idx] = "*"


class Engine:
    """内容安全本地判定引擎，支持词库快照原子热替换。"""

    def __init__(self) -> None:
        matcher, report = compile_matcher(None)
        self._current: Snapshot = Snapshot(
            version=0,
            matcher=matcher,
            report=report,
            loaded_at=time.time(),
        )

    def current(self) -> Snapshot:
        """返回当前生效的只读词库快照。"""
        return self._current

    def swap(self, version: int, lexicon: Lexicon | None) -> CompileReport:
        """先在快照外完成词库编译，再整体原子切换为新快照。"""
        matcher, report = compile_matcher(lexicon)
        self._current = Snapshot(
            version=version,
            matcher=matcher,
            report=report,
            loaded_at=time.time(),
        )
        return report

    def check(self, scene_class: SceneClass | int, text: str) -> CheckResult:
        """按场景大类判定一段文字；空串直接放行。"""
        snapshot = self._current
        if not text:
            return CheckResult(verdict=Verdict.PASS, version=snapshot.version)

        elems, orig = fold(text)
        hits = snapshot.matcher.find(elems)
        masked = False
        masked_category = Category.UNSPECIFIED
        for hit in hits:
            act = _action_for(scene_class, hit.category)
            if act == _Action.REJECT:
                return CheckResult(
                    verdict=Verdict.REJECT,
                    category=hit.category,
                    version=snapshot.version,
                )
            if act == _Action.MASK:
                if not masked:
                    masked = True
                    masked_category = hit.category
                _mask_span(orig, hit.start, hit.end)

        if scene_class != SceneClass.PRIVATE_LABEL:
            allow_spans = snapshot.matcher.allow_orig_spans(elems)
            if contains_ad(
                text,
                elems,
                lambda start_orig, end_orig: orig_covered(allow_spans, start_orig, end_orig),
            ):
                return CheckResult(
                    verdict=Verdict.REJECT,
                    category=Category.AD,
                    version=snapshot.version,
                )

        if masked:
            return CheckResult(
                verdict=Verdict.MASKED,
                masked="".join(orig),
                category=masked_category,
                version=snapshot.version,
            )
        return CheckResult(verdict=Verdict.PASS, version=snapshot.version)
