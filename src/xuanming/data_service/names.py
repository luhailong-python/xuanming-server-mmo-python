"""Player name rules (Go ``shared/playername``): NFKC, trim, structural 1..32 chars, charset, builtin word list."""

import unicodedata

RESERVE_OK, RESERVE_TAKEN, RESERVE_INVALID = 0, 1, 2
OK, EMPTY, INVALID, SENSITIVE = range(4)
MAX_RUNES = 32  # StructuralMaxRunes: the only length rule data_service applies

# Go strings.TrimSpace = unicode.IsSpace: ASCII whitespace, U+0085, U+00A0 and category Z.
_SPACE = "\t\n\v\f\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a" \
         "\u2028\u2029\u202f\u205f\u3000"
_SENSITIVE_SUBSTRINGS = ("管理员", "客服", "官方", "系统", "运营")
_SENSITIVE_PREFIXES = ("gm",)


def allowed(char: str) -> bool:
    code = ord(char)
    return (48 <= code <= 57 or 65 <= code <= 90 or 97 <= code <= 122 or code == 0x3007
            or 0x3400 <= code <= 0x4DBF or 0x4E00 <= code <= 0x9FFF)


def normalize(raw: str, *, min_runes: int = 1, max_runes: int = MAX_RUNES) -> tuple[str, str, int]:
    """Return ``(display, norm, verdict)``; display/norm are empty for EMPTY and INVALID."""
    text = unicodedata.normalize("NFKC", raw).strip(_SPACE)
    if not text:
        return "", "", EMPTY
    if not min_runes <= len(text) <= max_runes or not all(allowed(char) for char in text):
        return "", "", INVALID
    norm = text.lower()  # only ASCII and CJK survive the charset check, so str.lower == Go ToLower here
    if any(word in norm for word in _SENSITIVE_SUBSTRINGS) or norm.startswith(_SENSITIVE_PREFIXES):
        return text, norm, SENSITIVE
    return text, norm, OK
