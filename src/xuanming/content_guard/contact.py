"""外链广告与变体联系方式启发式检测器。

与 Go 版 ``content_guard/internal/contact`` 保持一致：
- 外链 URL（``http://`` / ``https://`` / ``www.``）与常见广告顶级域名后缀正则检测；
- 跳过分隔符后的裸 11 位大陆手机号（``1[3-9]xxxxxxxxx``）检测；
- 24 个渠道线索词后向前扫描 28 个非跳过字符，识别阿拉伯数字、中文数字/谐音字及微信号形态；
- 全部命中点均支持按原文码点区间查询白名单豁免。

本模块纯内存计算，不连外部依赖，不打日志。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
import re

from .textnorm import Elem, is_ascii_alnum

# 24 个引流渠道线索词（已折叠形态，顺序与 Go 版保持一致）
CHANNEL_CUES: tuple[str, ...] = (
    "weixin",
    "weix",
    "微x",
    "wei信",
    "微信",
    "威信",
    "徽信",
    "v信",
    "加微",
    "加wx",
    "加vx",
    "加v",
    "wx",
    "vx",
    "qq群",
    "q群",
    "扣扣",
    "企鹅",
    "加q",
    "qq",
    "支付宝",
    "zfb",
    "闲鱼",
    "咸鱼",
)

_LOOKAHEAD = 28

# 两条正则均在折叠后的字符串上匹配（已转小写且折叠全角/带圈/同形字符，避免全角输入绕过）。
_DOMAIN_RE = re.compile(
    r"[a-z0-9-]{2,}\.(?:com|cn|net|org|xyz|top|vip|cc|tv|me|gg|io|club|shop|work|site)",
    re.ASCII,
)
_URL_RE = re.compile(r"(?:https?://(?:www\.)?|www\.)([a-z0-9.-]+)", re.ASCII)

# 渠道线索词后的中文数字与语音谐音字集合
_CN_DIGITS: frozenset[str] = frozenset("〇零洞一壹幺妖二贰两三叁四肆五伍六陆七柒拐八捌九玖")


def _allowed_folded(
    elems: Sequence[Elem],
    start: int,
    end: int,
    allow_check: Callable[[int, int], bool] | None,
) -> bool:
    """判断折叠序列闭开区间 ``[start, end)`` 对应的原文区间是否整段落在白名单内。"""
    if allow_check is None:
        return False
    return allow_check(elems[start].orig, elems[end - 1].orig)


def _has_unallowed_url(
    folded: str,
    elems: Sequence[Elem],
    allow_check: Callable[[int, int], bool] | None,
) -> bool:
    """检测主机名不在白名单里的网址；去除末尾句子标点（避免将 ``www...`` 误判为网址）。"""
    for match in _URL_RE.finditer(folded):
        start, end = match.span(1)
        while end > start and folded[end - 1] in (".", "-"):
            end -= 1
        if end > start and not _allowed_folded(elems, start, end, allow_check):
            return True
    return False


def _has_unallowed_domain(
    folded: str,
    elems: Sequence[Elem],
    allow_check: Callable[[int, int], bool] | None,
) -> bool:
    """检测不在白名单里的域名；显式检查右边界非 ASCII 字母数字，避免吃掉相邻域名或误伤 ``abc.community``。"""
    total = len(folded)
    for match in _DOMAIN_RE.finditer(folded):
        start, end = match.span()
        if end < total and is_ascii_alnum(folded[end]):
            continue
        if not _allowed_folded(elems, start, end, allow_check):
            return True
    return False


def _has_bare_phone_number(
    dense: Sequence[Elem],
    allow_check: Callable[[int, int], bool] | None,
) -> bool:
    run = 0
    total = len(dense)
    for idx, elem in enumerate(dense):
        if "0" <= elem.r <= "9":
            run += 1
            if run == 11:
                start = idx - 10
                if dense[start].r == "1" and "3" <= dense[start + 1].r <= "9":
                    next_is_digit = (idx + 1 < total) and ("0" <= dense[idx + 1].r <= "9")
                    if not next_is_digit and (
                        allow_check is None or not allow_check(dense[start].orig, elem.orig)
                    ):
                        return True
        else:
            run = 0
    return False


def _match_dense_cue(dense: Sequence[Elem], start: int, cue: str) -> bool:
    if start + len(cue) > len(dense):
        return False
    for offset, want in enumerate(cue):
        if dense[start + offset].r != want:
            return False
    return True


def _has_digit_in_window(dense: Sequence[Elem], start: int, end: int) -> bool:
    upper = min(end, len(dense) - 1)
    for idx in range(start, upper + 1):
        if "0" <= dense[idx].r <= "9":
            return True
    return False


def _has_cue_with_contact(
    dense: Sequence[Elem],
    allow_check: Callable[[int, int], bool] | None,
) -> bool:
    total = len(dense)
    for idx in range(total):
        cue_len = 0
        for cue in CHANNEL_CUES:
            if _match_dense_cue(dense, idx, cue):
                cue_len = len(cue)
                break
        if cue_len == 0:
            continue

        # 英文短线索（如 qq / wx / vx）前面紧挨 ASCII 字母或数字时视为普通单词后缀粘连，跳过。
        head_ch = dense[idx].r
        if (
            idx > 0
            and ord(head_ch) < 128
            and (("a" <= head_ch <= "z") or ("A" <= head_ch <= "Z"))
            and is_ascii_alnum(dense[idx - 1].r)
        ):
            continue

        digits = 0
        handle_chars = 0
        window_start = idx + cue_len
        window_end = min(total, window_start + _LOOKAHEAD)
        for j in range(window_start, window_end):
            ch = dense[j].r
            if "0" <= ch <= "9":
                digits += 1
                handle_chars += 1
            elif ch in _CN_DIGITS:
                digits += 1
            elif ("a" <= ch <= "z") or ch in ("_", "-"):
                handle_chars += 1
                digits = 0
            else:
                if digits >= 5:
                    break
                digits = 0
                handle_chars = 0

            if digits >= 5 or (handle_chars >= 6 and _has_digit_in_window(dense, window_start, j)):
                if allow_check is None or not allow_check(dense[idx].orig, dense[j].orig):
                    return True
                break
    return False


def contains_ad(
    text: str,
    elems: Sequence[Elem],
    allow_check: Callable[[int, int], bool] | None = None,
) -> bool:
    """判断文本是否包含未豁免的外链广告或变体联系方式（URL/域名在折叠后字符序列上匹配防全角/同形绕过）。"""
    del text
    if not elems:
        return False
    folded = "".join(elem.r for elem in elems)
    if _has_unallowed_url(folded, elems, allow_check) or _has_unallowed_domain(folded, elems, allow_check):
        return True

    dense = [elem for elem in elems if not elem.skip]
    if not dense:
        return False
    if _has_bare_phone_number(dense, allow_check):
        return True
    return _has_cue_with_contact(dense, allow_check)
