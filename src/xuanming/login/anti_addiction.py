"""实名认证与未成年人防沉迷规则（对应 Go ``shared/antiaddiction`` + ``pkg/realname`` + ``svc/anti_addiction.go``）。

所有时间规则均使用固定东八区（UTC+8，北京时间），不依赖容器本地时区。
姓名与身份证号仅在核验调用中经手，不落盘、不进日志。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import secrets
import unicodedata
from dataclasses import dataclass

from . import keys, tips

logger = logging.getLogger(__name__)

# 固定东八区（中国无夏令时）
BEIJING = dt.timezone(dt.timedelta(hours=8))

ADULT_AGE = 18
PI_LENGTH = 38
PI_DIGITS = "0123456789abcdefghijklmnop"

# 国新出发〔2021〕14 号：未成年人仅可在可玩日 20:00–21:00 登录游戏
PLAY_START_HOUR = 20
PLAY_END_HOUR = 21

# GB 11643 18 位身份证校验权重与余数映射
_ID_WEIGHTS = (7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
_ID_CHECK_CODES = "10X98765432"

# NPPA 核验状态
AUTH_OK = 0
AUTH_PENDING = 1
AUTH_FAILED = 2

PENDING_TTL_S = 48 * 3600
ATTEMPTS_TTL_S = 25 * 3600

COUNT_ATTEMPT_SCRIPT = """
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return n
"""


class InvalidIdentityError(Exception):
    """国家系统明确判定姓名或身份证号不合法。"""


class NoRecordError(Exception):
    """国家系统查询无此 ai 记录（已过期或不存在）。"""


@dataclass(frozen=True, slots=True)
class SpendLimit:
    """未成年人分龄充值上限（单位：分）。"""

    unlimited: bool = False
    single_fen: int = 0
    daily_fen: int = 0
    monthly_fen: int = 0


@dataclass(frozen=True, slots=True)
class NppaAuthResult:
    status: int
    pi: str = ""


@dataclass(frozen=True, slots=True)
class AntiAddictionVerdict:
    """一次登录按防沉迷规则的判定结论（对应 Go ``svc.Verdict``）。"""

    status: object | None = None
    reject_tip: int = 0
    deadline_ms: int = 0
    report_pi: str = ""


# -- 纯规则函数（与 Go ``shared/antiaddiction`` 逐行等价）-------------------------------------------


def _date_of(year: int, month: int, day: int) -> dt.datetime | None:
    if year < 1900:
        return None
    try:
        return dt.datetime(year, month, day, 0, 0, 0, tzinfo=BEIJING)
    except ValueError:
        return None


def pi_birth_prefix(year: int, month: int, day: int) -> str:
    """将出生日期编码为 38 位国家系统 pi 的前 6 位 26 进制前缀。"""
    value = year * 10000 + month * 100 + day
    chars = ["0"] * 6
    for i in range(5, -1, -1):
        chars[i] = PI_DIGITS[value % 26]
        value //= 26
    return "".join(chars)


def birth_date(pi: str) -> dt.datetime | None:
    """从国家系统 38 位 pi 前 6 位解出出生日期（北京零点）；格式或日期非法返回 None。"""
    if len(pi) != PI_LENGTH:
        return None
    for ch in pi:
        if not (("0" <= ch <= "9") or ("a" <= ch <= "z")):
            return None
    value = 0
    for ch in pi[:6]:
        idx = PI_DIGITS.find(ch)
        if idx < 0:
            return None
        value = value * 26 + idx
    return _date_of(value // 10000, (value // 100) % 100, value % 100)


def valid_pi(pi: str) -> bool:
    return birth_date(pi) is not None


def age_at(birth: dt.datetime, now: dt.datetime) -> int:
    """按北京日期计算周岁：生日当天满一岁。"""
    bj = now.astimezone(BEIJING)
    age = bj.year - birth.year
    if (bj.month, bj.day) < (birth.month, birth.day):
        age -= 1
    return age


def spend_limit_at(age: int) -> SpendLimit:
    """返回周岁对应的充值上限：未满 8 岁禁充；8–16 岁 50/50/200 元；16–18 岁 100/100/400 元；18+ 不限。"""
    if age >= ADULT_AGE:
        return SpendLimit(unlimited=True)
    if age >= 16:
        return SpendLimit(single_fen=100_00, daily_fen=100_00, monthly_fen=400_00)
    if age >= 8:
        return SpendLimit(single_fen=50_00, daily_fen=50_00, monthly_fen=200_00)
    return SpendLimit()


class PlayCalendar:
    """未成年人可玩日日历：周五/周六/周日 + 运维配置的额外法定节假日（YYYY-MM-DD）。"""

    def __init__(self, extra_days: tuple[str, ...] | list[str] = ()) -> None:
        parsed: set[str] = set()
        for day in extra_days:
            try:
                value = dt.date.fromisoformat(day)
            except ValueError as error:
                raise ValueError(f"invalid extra play day {day!r}: want YYYY-MM-DD") from error
            if value.isoformat() != day:
                raise ValueError(f"invalid extra play day {day!r}: want YYYY-MM-DD")
            parsed.add(day)
        self._extra = frozenset(parsed)

    def is_play_day(self, bj_now: dt.datetime) -> bool:
        # Python weekday(): Monday=0 .. Friday=4, Saturday=5, Sunday=6
        if bj_now.weekday() in (4, 5, 6):
            return True
        return bj_now.date().isoformat() in self._extra

    def play_deadline(self, now: dt.datetime) -> dt.datetime | None:
        """返回未成年人本次可玩到的北京时间截止点；当前不在可玩时段则返回 None。"""
        bj = now.astimezone(BEIJING)
        if not self.is_play_day(bj):
            return None
        start = bj.replace(hour=PLAY_START_HOUR, minute=0, second=0, microsecond=0)
        end = bj.replace(hour=PLAY_END_HOUR, minute=0, second=0, microsecond=0)
        if bj < start or bj >= end:
            return None
        return end


def normalize_id_number(id_number: str) -> str:
    """去除首尾空白并将末尾校验码 x 规范为大写 X。"""
    return id_number.strip().upper()


def valid_id_number(id_number: str, now: dt.datetime) -> bool:
    """本地预检 18 位身份证号：长度、数字、GB 11643 校验码与不晚于当前的合法出生日期。"""
    if len(id_number) != 18:
        return False
    total = 0
    for i in range(17):
        ch = id_number[i]
        if not ("0" <= ch <= "9"):
            return False
        total += (ord(ch) - ord("0")) * _ID_WEIGHTS[i]
    if _ID_CHECK_CODES[total % 11] != id_number[17]:
        return False
    year = int(id_number[6:10])
    month = int(id_number[10:12])
    day = int(id_number[12:14])
    birth = _date_of(year, month, day)
    return birth is not None and birth <= now.astimezone(BEIJING)


def valid_name(name: str) -> bool:
    """本地预检姓名：去空白后 1–32 个字符，不含控制字符。"""
    cleaned = name.strip()
    if not cleaned or len(cleaned) > 32:
        return False
    return all(unicodedata.category(ch)[0] != "C" for ch in cleaned)


# -- 实名提交服务与沙箱核验器（对应 Go ``pkg/realname`` + ``svc/anti_addiction.go``）---------------


class SandboxNppaVerifier:
    """离线/沙箱实名核验器：根据合法身份证号中的出生日期生成符合规范的 38 位 pi。"""

    async def check(self, ai: str, name: str, id_number: str) -> NppaAuthResult:
        year = int(id_number[6:10])
        month = int(id_number[10:12])
        day = int(id_number[12:14])
        prefix = pi_birth_prefix(year, month, day)
        suffix = hashlib.sha256(id_number.encode("utf-8")).hexdigest()[:32]
        return NppaAuthResult(status=AUTH_OK, pi=prefix + suffix)

    async def query(self, ai: str) -> NppaAuthResult:
        raise NoRecordError("no pending record")


class RealNameService:
    """实名提交编排：本地预检 -> 额度/当日次数检查 -> 调核验器 -> 写账号目录（Go ``realname.Service``）。"""

    def __init__(
        self,
        redis,
        verifier,
        directory,
        *,
        daily_limit: int = 5,
        quota=None,
        clock=None,
    ) -> None:
        self.redis = redis
        self.verifier = verifier
        self.directory = directory
        self.daily_limit = daily_limit
        self.quota = quota
        self.clock = clock or (lambda: dt.datetime.now(BEIJING))

    async def _allow_quota(self, api: str) -> bool:
        if self.quota is None:
            return True
        try:
            return bool(await self.quota.allow(api))
        except Exception as error:
            logger.error("NPPA quota unavailable api=%s err=%s", api, error)
            return False

    async def submit(self, account: str, name: str, id_number: str) -> tuple[str, int]:
        """提交一次实名；成功返回 ``(pi, 0)``，失败返回 ``(stored_or_empty, tip_id)``。"""
        now = self.clock()
        cleaned_name = name.strip()
        norm_id = normalize_id_number(id_number)
        if not valid_name(cleaned_name) or not valid_id_number(norm_id, now):
            return "", tips.REAL_NAME_INVALID
        if not await self._allow_quota("check"):
            return "", tips.REAL_NAME_SERVICE_UNAVAILABLE
        day = now.astimezone(BEIJING).date().isoformat()
        try:
            attempts = int(await self.redis.eval(
                COUNT_ATTEMPT_SCRIPT, 1, keys.real_name_attempts_key(account, day), ATTEMPTS_TTL_S,
            ))
        except Exception as error:
            logger.error("Count real-name submit attempts failed account=%s err=%s", account, error)
            return "", tips.REAL_NAME_SERVICE_UNAVAILABLE
        if self.daily_limit > 0 and attempts > self.daily_limit:
            return "", tips.REAL_NAME_TOO_MANY_ATTEMPTS

        ai = secrets.token_hex(16)
        try:
            result = await self.verifier.check(ai, cleaned_name, norm_id)
        except InvalidIdentityError:
            return "", tips.REAL_NAME_INVALID
        except Exception as error:
            logger.error("NPPA check failed account=%s err=%s", account, error)
            return "", tips.REAL_NAME_SERVICE_UNAVAILABLE

        if result.status == AUTH_OK:
            return await self._bind(account, result.pi)
        if result.status == AUTH_PENDING:
            try:
                await self.redis.set(keys.real_name_pending_key(account), ai, ex=PENDING_TTL_S)
            except Exception as error:
                logger.error("Remember pending real-name ai failed account=%s err=%s", account, error)
            return "", tips.REAL_NAME_PENDING
        return "", tips.REAL_NAME_INVALID

    async def resolve_pending(self, account: str) -> str:
        """收尾认证中的实名提交；查到通过则绑定并返回 pi，否则返回空串。"""
        pending_key = keys.real_name_pending_key(account)
        try:
            raw = await self.redis.get(pending_key)
        except Exception as error:
            logger.error("Read pending real-name ai failed account=%s err=%s", account, error)
            return ""
        if not raw:
            return ""
        ai = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        if not await self._allow_quota("query"):
            return ""
        try:
            result = await self.verifier.query(ai)
        except NoRecordError:
            await self._forget_pending(pending_key)
            return ""
        except Exception as error:
            logger.error("NPPA query failed account=%s err=%s", account, error)
            return ""
        if result.status == AUTH_PENDING:
            return ""
        if result.status == AUTH_OK:
            pi, tip_id = await self._bind(account, result.pi)
            if tip_id == tips.REAL_NAME_SERVICE_UNAVAILABLE:
                return ""
            await self._forget_pending(pending_key)
            return pi
        await self._forget_pending(pending_key)
        return ""

    async def _bind(self, account: str, pi: str) -> tuple[str, int]:
        try:
            stored = await self.directory.set_real_name(account, pi)
        except Exception as error:
            logger.error("Bind real name to account directory failed account=%s err=%s", account, error)
            return "", tips.REAL_NAME_SERVICE_UNAVAILABLE
        if stored != pi:
            return stored, tips.REAL_NAME_ALREADY_BOUND
        return stored, 0

    async def _forget_pending(self, pending_key: str) -> None:
        try:
            await self.redis.delete(pending_key)
        except Exception as error:
            logger.error("Clear pending real-name ai failed err=%s", error)


class AntiAddictionService:
    """登录侧防沉迷聚合服务（对应 Go ``svc.AntiAddiction``）。"""

    def __init__(
        self,
        protocol,
        real_name: RealNameService,
        *,
        calendar: PlayCalendar | None = None,
        platform_managed: set[str] | frozenset[str] = frozenset(),
        clock=None,
    ) -> None:
        self.protocol = protocol
        self.real_name = real_name
        self.calendar = calendar if calendar is not None else PlayCalendar()
        self.platform_managed = frozenset(platform_managed)
        self.clock = clock or (lambda: dt.datetime.now(BEIJING))

    def now(self) -> dt.datetime:
        return self.clock()

    def self_managed(self, channel: str) -> bool:
        return channel not in self.platform_managed

    def evaluate(self, channel: str, pi: str, now: dt.datetime | None = None) -> AntiAddictionVerdict:
        """按登录渠道与账号 pi 判定防沉迷状态及是否拒绝绑定。"""
        now = self.now() if now is None else now
        status = self.protocol.message("loginpb.AntiAddictionStatus")
        self_managed = self.self_managed(channel)
        if not pi:
            if self_managed:
                status.real_name_required = True
                return AntiAddictionVerdict(status=status, reject_tip=tips.REAL_NAME_REQUIRED)
            return AntiAddictionVerdict(status=status)
        birth = birth_date(pi)
        if birth is None:
            return AntiAddictionVerdict(status=status, reject_tip=tips.REAL_NAME_SERVICE_UNAVAILABLE)
        report_pi = pi if self_managed else ""
        if age_at(birth, now) >= ADULT_AGE:
            return AntiAddictionVerdict(status=status, report_pi=report_pi)
        status.minor = True
        deadline = self.calendar.play_deadline(now)
        if deadline is None:
            return AntiAddictionVerdict(status=status, reject_tip=tips.MINOR_NOT_IN_PLAY_TIME, report_pi=report_pi)
        deadline_ms = int(deadline.timestamp() * 1000)
        status.play_deadline_ms = deadline_ms
        return AntiAddictionVerdict(status=status, deadline_ms=deadline_ms, report_pi=report_pi)
