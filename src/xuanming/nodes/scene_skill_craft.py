"""率土式分卷宗永久技能制书、手艺熟练度与主城宗师原子代打摊位系统。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import logging
import random
from collections.abc import Callable

from google.protobuf.message import DecodeError

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    BAG_FULL,
    BAG_INSUFFICIENT,
    BLOCKED,
    FEATURE_UNAVAILABLE,
    INVALID,
    UNAVAILABLE,
    bag_capacity,
    clone,
    held_item_count,
)
from .scene_mission import now_ms
from .scene_stats import try_auto_slot_newly_learned

logger = logging.getLogger(__name__)

_SERVICE = "SceneSkillClientPlayer"
_MAX_UINT64 = (1 << 64) - 1
_MAX_INT64 = (1 << 63) - 1
_MAX_UINT32 = (1 << 32) - 1

# 客户端幂等请求种类（与 C++ ClientRequestKind 严格对齐）。
_CLIENT_REQUEST_KIND_SKILL_SCROLL_CRAFT = 14
_CLIENT_REQUEST_KIND_LEARN_SKILL_BOOK = 15
_CLIENT_REQUEST_KIND_DISMANTLE_SKILL_BOOK = 16
_CLIENT_REQUEST_KIND_SEAL_SKILL_BOOK = 22
_CLIENT_REQUEST_KIND_UNSEAL_SKILL_BOOK = 23
_CLIENT_REQUEST_KIND_CLAIM_SCROLL_MILESTONE = 24
_CLIENT_REQUEST_KIND_BATCH_DISMANTLE_SKILL_BOOKS = 25
# 26..28 悬赏求书旧路径已于 2026-11-27 退役，改由 trade 求购单（ClientPlayerJubaozhai 582..584）承载。

# 物品配置 ID（与 C++ equip_craft_quality_rules.h 对齐）。
SCROLL_FRAGMENT_ITEM_ID = 3406
RARE_SCROLL_PAGE_ITEM_ID = 3407
RARE_SCROLL_INK_ITEM_ID = 3408
SEALED_SKILL_BOOK_ITEM_ID = 3414

# 手艺熟练度档位常量。
MASTERY_APPRENTICE = 1
MASTERY_ARTISAN = 2
MASTERY_MASTER = 3
ARTISAN_EXP_THRESHOLD = 20
MASTER_EXP_THRESHOLD = 60

# 代打摊位手续费与税率。
PROXY_FEE_MIN_GOLD = 50
PROXY_FEE_MAX_GOLD = 100
PROXY_TAX_BPS = 500  # 5% 银两税销毁
SCROLL_BONUS_FRAGMENTS_COUNT = 2
MAX_RARE_SKILL_RATIO_BPS = 1000  # 稀世绝学硬上限 <= 10%

# V2 名匠口碑、招牌枚举与 6 档零属性雅号印款常量。
MAX_MASTER_SHOWCASE_TAG = 4
CRAFT_TITLE_MAX_ID = 6
CRAFT_TITLE_BAILIAN_SUPREME_REQ = 5
CRAFT_TITLE_TIANGONG_SUPREME_REQ = 20
CRAFT_TITLE_WANJUAN_RARE_BOOKS_REQ = 3
CRAFT_TITLE_WANJUAN_ORDERS_REQ = 15
HOT_RANK_TOP_LIMIT = 3
_CRAFT_TITLE_NAMES = {
    1: "铸器宗师",
    2: "百炼宗师",
    3: "天工神匠",
    4: "制卷宗师",
    5: "万卷书圣",
    6: "双绝天工",
}

# V4 典籍藏经阁卷宗收集里程碑位图与批量安全拆页常量。
SCROLL_MILESTONE_HALF_TIER = 1
SCROLL_MILESTONE_FULL_TIER = 2
SCROLL_MILESTONE_HALF_REWARD_FRAGMENTS = 20
SCROLL_MILESTONE_FULL_REWARD_FRAGMENTS = 40
MAX_BATCH_DISMANTLE_COUNT = 20

# 制书模式（与 CraftSkillBookRequest.CraftMode 一致）。
CRAFT_MODE_RANDOM_REGULAR = 1
CRAFT_MODE_TARGETED_REGULAR = 2
CRAFT_MODE_TARGETED_RARE = 3

# 错误码（与 C++ skill_error_tip.pb.h / equip_error_tip.pb.h 严格对齐）。
SKILL_NOT_IN_CLASS = 7007
SKILL_GOLD_NOT_ENOUGH = 7012
SKILL_UPGRADE_BUSY = 7013
SKILL_SCROLL_INVALID = 7014
SKILL_BOOK_NOT_FOUND = 7015
SKILL_BOOK_NOT_DISMANTLABLE = 7016
SKILL_CRAFT_MATERIAL_NOT_ENOUGH = 7017
SKILL_CRAFT_REVISION_CHANGED = 7018
SKILL_CRAFT_BOOK_REQUIRED = 7019
SKILL_ALREADY_BOOK_UNLOCKED = 7020
SKILL_PROXY_STALL_REQUIRES_MASTER = 7021
SKILL_PROXY_FEE_INVALID = 7022
EQUIP_PROXY_MASTER_UNAVAILABLE = 28030
INVALID_TABLE_ID = 1002
CURRENCY_GOLD = 0  # currency.values 下标 0 为银两（与 SceneAssets 统一）

# 权威卷宗技能清单兜底（当 Skill 表尚未包含 scroll_id 字段时启用，与 C++ skill.json 严格一致）。
# 元素格式：(skill_table_id, caster_mask)，caster_mask: 1=人物, 2=宝宝, 3=通用。
_DEFAULT_SCROLL_SKILLS: dict[int, tuple[tuple[int, int], ...]] = {
    1: (
        (10002, 3), (10003, 1), (10006, 3), (10008, 1), (10010, 3), (10012, 1),
        (10014, 3), (10015, 1), (10016, 3), (10018, 2), (10019, 3), (10020, 1),
        (10021, 1), (10022, 1), (10023, 1), (10024, 1), (10025, 3), (10027, 3),
        (10029, 1), (10030, 1), (10031, 1), (10033, 3), (10034, 3), (10035, 1),
        (10036, 1), (10038, 1), (10039, 1), (10040, 1), (11005, 2), (11006, 2),
        (11007, 2), (11008, 2), (11011, 2), (11012, 2), (11013, 2), (11014, 2),
        (11015, 2), (12001, 1), (12002, 1), (12003, 1), (12004, 3), (12005, 1),
        (12006, 3), (12007, 1), (12008, 1), (12014, 2), (12015, 2), (12016, 2),
        (12017, 2), (12018, 2), (13001, 2), (13005, 2), (13006, 2), (14001, 1),
        (14002, 1), (14003, 2),
    ),
    2: (
        (10026, 3), (10041, 1), (11009, 2), (11016, 2), (12009, 1), (12019, 1),
    ),
    3: (
        (12020, 1), (12021, 1), (12022, 1),
    ),
}


def mastery_tier_for_exp(exp: int) -> int:
    if exp >= MASTER_EXP_THRESHOLD:
        return MASTERY_MASTER
    if exp >= ARTISAN_EXP_THRESHOLD:
        return MASTERY_ARTISAN
    return MASTERY_APPRENTICE


def mastery_next_exp(exp: int) -> int:
    if exp >= MASTER_EXP_THRESHOLD:
        return 0
    if exp >= ARTISAN_EXP_THRESHOLD:
        return MASTER_EXP_THRESHOLD
    return ARTISAN_EXP_THRESHOLD


def mastery_tier_name(tier: int) -> str:
    if tier == MASTERY_MASTER:
        return "宗师"
    if tier == MASTERY_ARTISAN:
        return "巧匠"
    return "学徒"


def forge_top_quality_rate_bps(tier: int) -> int:
    if tier == MASTERY_MASTER:
        return 1000
    if tier == MASTERY_ARTISAN:
        return 700
    return 500


def scroll_bonus_fragment_rate_bps(tier: int) -> int:
    if tier == MASTERY_MASTER:
        return 1000
    if tier == MASTERY_ARTISAN:
        return 500
    return 0


def valid_proxy_fee(gross_fee: int) -> bool:
    return PROXY_FEE_MIN_GOLD <= gross_fee <= PROXY_FEE_MAX_GOLD


def proxy_tax_gold(gross_fee: int) -> int:
    return 0 if gross_fee == 0 else (gross_fee * PROXY_TAX_BPS + 9999) // 10000


def proxy_net_gold(gross_fee: int) -> int:
    tax = proxy_tax_gold(gross_fee)
    return gross_fee - tax if gross_fee > tax else 0


def _sat_add_u32(a: int, b: int) -> int:
    return min(_MAX_UINT32, int(a) + int(b))


def _sat_add_u64(a: int, b: int) -> int:
    return min(_MAX_UINT64, int(a) + int(b))


def craft_title_name(title_id: int) -> str:
    return _CRAFT_TITLE_NAMES.get(int(title_id), "")


def is_craft_title_unlocked(
    title_id: int,
    forge_exp: int,
    scroll_exp: int,
    supreme_forged_count: int,
    rare_books_crafted_count: int,
    proxy_orders_completed: int,
) -> bool:
    forge_master = mastery_tier_for_exp(forge_exp) >= MASTERY_MASTER
    scroll_master = mastery_tier_for_exp(scroll_exp) >= MASTERY_MASTER
    tid = int(title_id)
    if tid == 1:
        return forge_master
    if tid == 2:
        return forge_master and supreme_forged_count >= CRAFT_TITLE_BAILIAN_SUPREME_REQ
    if tid == 3:
        return forge_master and supreme_forged_count >= CRAFT_TITLE_TIANGONG_SUPREME_REQ
    if tid == 4:
        return scroll_master
    if tid == 5:
        return scroll_master and (
            rare_books_crafted_count >= CRAFT_TITLE_WANJUAN_RARE_BOOKS_REQ
            or proxy_orders_completed >= CRAFT_TITLE_WANJUAN_ORDERS_REQ
        )
    if tid == 6:
        return forge_master and scroll_master
    return False


def unlocked_craft_title_ids(
    forge_exp: int,
    scroll_exp: int,
    supreme_forged_count: int,
    rare_books_crafted_count: int,
    proxy_orders_completed: int,
) -> list[int]:
    return [
        tid for tid in range(1, CRAFT_TITLE_MAX_ID + 1)
        if is_craft_title_unlocked(
            tid, forge_exp, scroll_exp, supreme_forged_count, rare_books_crafted_count, proxy_orders_completed,
        )
    ]


def unlocked_craft_title_mask(
    forge_exp: int,
    scroll_exp: int,
    supreme_forged_count: int,
    rare_books_crafted_count: int,
    proxy_orders_completed: int,
) -> int:
    mask = 0
    for tid in unlocked_craft_title_ids(
        forge_exp, scroll_exp, supreme_forged_count, rare_books_crafted_count, proxy_orders_completed,
    ):
        mask |= 1 << (tid - 1)
    return mask


def active_craft_title_id(craft) -> int:
    tid = int(craft.equipped_craft_title_id)
    if tid == 0:
        return 0
    return tid if is_craft_title_unlocked(
        tid,
        int(craft.forge_mastery_exp),
        int(craft.scroll_mastery_exp),
        int(craft.supreme_forged_count),
        int(craft.rare_books_crafted_count),
        int(craft.proxy_orders_completed),
    ) else 0


def format_master_signature(equipped_title_id: int, display_name: str) -> str:
    title = craft_title_name(equipped_title_id)
    return f"{title}·{display_name}" if title else display_name


def hot_rank_score(recent_24h_orders: int, recent_24h_supreme: int) -> int:
    return int(recent_24h_supreme) * 3 + int(recent_24h_orders)


def scroll_half_threshold(total_skills: int) -> int:
    return 0 if total_skills <= 0 else (int(total_skills) + 1) // 2


def scroll_milestone_bit_index(scroll_id: int, tier: int) -> int:
    if scroll_id <= 0 or tier not in (SCROLL_MILESTONE_HALF_TIER, SCROLL_MILESTONE_FULL_TIER):
        return 0
    return (int(scroll_id) - 1) * 2 + (int(tier) - 1)


def scroll_milestone_reward_fragments(tier: int) -> int:
    if tier == SCROLL_MILESTONE_HALF_TIER:
        return SCROLL_MILESTONE_HALF_REWARD_FRAGMENTS
    if tier == SCROLL_MILESTONE_FULL_TIER:
        return SCROLL_MILESTONE_FULL_REWARD_FRAGMENTS
    return 0


def has_bit(bits: bytes, bit_index: int) -> bool:
    byte_idx = bit_index // 8
    if byte_idx >= len(bits):
        return False
    return bool(bits[byte_idx] & (1 << (bit_index % 8)))


def set_bit(bits: bytes, bit_index: int) -> bytes:
    """置位指定 bit，低位在前、保留未知高位 bit 并去尾 0（对齐 bit-storage-and-table-ids.md）。"""
    byte_idx = bit_index // 8
    buf = bytearray(bits)
    if byte_idx >= len(buf):
        buf.extend(b"\x00" * (byte_idx + 1 - len(buf)))
    buf[byte_idx] |= 1 << (bit_index % 8)
    while buf and buf[-1] == 0:
        buf.pop()
    return bytes(buf)


def apply_reputation_to_craft(
    craft,
    orders_delta: int,
    supreme_delta: int,
    rare_delta: int,
    net_gold_delta: int,
    buyer_name: str,
) -> None:
    """将代打口碑与未读营业战报原子累积到 PlayerCraftComp。"""
    before_mask = unlocked_craft_title_mask(
        int(craft.forge_mastery_exp),
        int(craft.scroll_mastery_exp),
        int(craft.supreme_forged_count),
        int(craft.rare_books_crafted_count),
        int(craft.proxy_orders_completed),
    )
    craft.proxy_orders_completed = _sat_add_u32(craft.proxy_orders_completed, orders_delta)
    craft.supreme_forged_count = _sat_add_u32(craft.supreme_forged_count, supreme_delta)
    craft.rare_books_crafted_count = _sat_add_u32(craft.rare_books_crafted_count, rare_delta)
    after_mask = unlocked_craft_title_mask(
        int(craft.forge_mastery_exp),
        int(craft.scroll_mastery_exp),
        int(craft.supreme_forged_count),
        int(craft.rare_books_crafted_count),
        int(craft.proxy_orders_completed),
    )
    newly_unlocked = after_mask & ~before_mask

    craft.unread_proxy_digest_orders = _sat_add_u32(craft.unread_proxy_digest_orders, orders_delta)
    craft.unread_proxy_digest_supreme = _sat_add_u32(craft.unread_proxy_digest_supreme, supreme_delta)
    craft.unread_proxy_digest_rare = _sat_add_u32(craft.unread_proxy_digest_rare, rare_delta)
    craft.unread_proxy_digest_net_gold = _sat_add_u64(craft.unread_proxy_digest_net_gold, net_gold_delta)
    craft.unread_proxy_digest_new_titles_mask = int(craft.unread_proxy_digest_new_titles_mask) | newly_unlocked
    if buyer_name:
        craft.unread_proxy_digest_last_buyer = buyer_name


@dataclass(slots=True)
class PendingProxyReputation:
    orders: int = 0
    supreme: int = 0
    rare: int = 0
    net_gold: int = 0
    last_buyer: str = ""

    @property
    def is_empty(self) -> bool:
        return not (self.orders or self.supreme or self.rare or self.net_gold)


@dataclass(slots=True)
class HotProxyCounters:
    """热榜计数：一个小时桶里的，或近 24 小时各桶之和。"""

    orders: int = 0
    supreme: int = 0


# ── 名匠代打共享摊位目录与口碑台账：按区控制 Redis 的哈希，键、字段与取走脚本和 C++ player_craft.cpp 逐字节一致，
#    混跑时两版读写同一份（设计见 docs/design/craft-proxy-shared-ledger-redis.md）。
PROXY_DIRECTORY_REFRESH_INTERVAL_SEC = 5.0
# 西市热榜按 UTC 小时分桶：近 24 小时 = 当前小时与前 23 个小时的桶之和；桶每写一次续 25 小时过期，出窗后自然消失。
PROXY_HOT_WINDOW_HOURS = 24
_PROXY_HOT_BUCKET_TTL_SEC = 25 * 3600
_MS_PER_HOUR = 3_600_000
# 取走 = 同一段脚本里先 HMGET 再 HDEL：两个节点同时取同一名宗师，只有先执行的一方拿到值；热榜的 :24o / :24s 不取。
TAKE_PROXY_REPUTATION_LUA = (
    "local k=KEYS[1] local p=ARGV[1] "
    "local v=redis.call('HMGET',k,p..':o',p..':s',p..':r',p..':g',p..':b') "
    "redis.call('HDEL',k,p..':o',p..':s',p..':r',p..':g',p..':b') "
    "return v"
)
# 近 24 小时热榜 = KEYS 里的小时桶按字段求和（只读，同槽一次执行读完），应答同 HGETALL 的字段、值交替；与 C++ kSumProxyHotLua 同文。
SUM_PROXY_HOT_LUA = (
    "local acc={} "
    "for i=1,#KEYS do "
    "local kv=redis.call('HGETALL',KEYS[i]) "
    "for j=1,#kv,2 do local n=tonumber(kv[j+1]) if n then acc[kv[j]]=(acc[kv[j]] or 0)+n end end "
    "end "
    "local out={} "
    "for f,n in pairs(acc) do out[#out+1]=f out[#out+1]=string.format('%d',n) end "
    "return out"
)
_PENDING_FIELD_BY_SUFFIX = {b"o": "orders", b"s": "supreme", b"r": "rare", b"g": "net_gold"}
_HOT_FIELD_BY_SUFFIX = {b"o": "orders", b"s": "supreme"}


def proxy_stall_directory_key(zone_id: int) -> str:
    """按区共享摊位目录：field = 宗师 player_id 十进制，value = CraftProxyStallInfo 二进制。"""
    return f"craft_proxy_stall:{{ctl:{zone_id}}}"


def proxy_reputation_ledger_key(zone_id: int) -> str:
    """按区口碑待回账：<id>:o/s/r/g/b；与摊位目录、热榜桶同一 hash tag。"""
    return f"craft_proxy_reputation:{{ctl:{zone_id}}}"


def proxy_hot_bucket_key(zone_id: int, hour_index: int) -> str:
    """西市热榜一个小时的桶：<id>:o 单数、<id>:s 极品数；hour_index = UTC 秒 // 3600。"""
    return f"craft_proxy_hot:{{ctl:{zone_id}}}:{hour_index}"


def proxy_hot_window_bucket_keys(zone_id: int, newest_hour: int) -> list[str]:
    """窗口里的 24 个桶键，从 newest_hour 往前排（同 C++ ProxyHotWindowBucketKeys）。"""
    return [proxy_hot_bucket_key(zone_id, newest_hour - back) for back in range(min(PROXY_HOT_WINDOW_HOURS, newest_hour + 1))]


def _redis_failure(exc: BaseException) -> bool:
    # 不在模块顶层 import redis：本地单进程模式不装它。
    return isinstance(exc, OSError) or type(exc).__module__.startswith("redis")


def _ledger_count(raw) -> int:
    """台账计数按非负整数读：缺失、负数（旧版回账多扣出来的）、非数字按 0，超出 int64 封顶（同 C++ ParseNonNegativeInt64）。"""
    if not isinstance(raw, bytes) or not raw.isdigit():
        return 0
    return min(int(raw), _MAX_INT64)


def _parse_taken_reputation(reply) -> PendingProxyReputation | None:
    """取走脚本的应答是 5 项数组，缺的字段为 None。形状不对 = 结果未知（可能已取走也可能没有），返回 None。"""
    if (
        not isinstance(reply, (list, tuple))
        or len(reply) != 5
        or any(value is not None and not isinstance(value, bytes) for value in reply)
    ):
        return None
    orders, supreme, rare, net_gold, buyer = reply
    return PendingProxyReputation(
        orders=min(_ledger_count(orders), _MAX_UINT32),
        supreme=min(_ledger_count(supreme), _MAX_UINT32),
        rare=min(_ledger_count(rare), _MAX_UINT32),
        net_gold=_ledger_count(net_gold),
        last_buyer=buyer.decode("utf-8", "replace") if buyer else "",
    )


def _split_master_field(field, value) -> tuple[int, bytes] | None:
    """台账与热榜桶的字段都是 <宗师 player_id>:<后缀>（同 C++ ParseReputationField）；认不出的返回 None。"""
    if not isinstance(field, bytes) or not isinstance(value, bytes):
        return None
    master_text, sep, suffix = field.partition(b":")
    master_id = int(master_text) if master_text.isdigit() else 0
    if not sep or not suffix or not 0 < master_id <= _MAX_UINT64:
        return None
    return master_id, suffix


def _parse_reputation_ledger(raw: dict) -> dict[int, PendingProxyReputation]:
    """把待回账整表解析成镜像（同 C++ OnProxyReputationReply）；认不出的字段跳过，全 0 的待回账不留。"""
    pending: dict[int, PendingProxyReputation] = {}
    for field, value in raw.items():
        parsed = _split_master_field(field, value)
        if parsed is None:
            continue
        master_id, suffix = parsed
        if suffix == b"b":
            pending.setdefault(master_id, PendingProxyReputation()).last_buyer = value.decode("utf-8", "replace")
            continue
        count = _ledger_count(value)
        if count and suffix in _PENDING_FIELD_BY_SUFFIX:
            attr = _PENDING_FIELD_BY_SUFFIX[suffix]
            entry = pending.setdefault(master_id, PendingProxyReputation())
            setattr(entry, attr, count if attr == "net_gold" else min(count, _MAX_UINT32))
    return {mid: entry for mid, entry in pending.items() if not entry.is_empty}


def _parse_hot_totals(reply) -> dict[int, HotProxyCounters] | None:
    """求和脚本的应答：<id>:o / <id>:s 与和交替（同 C++ ParseProxyHotTotals）。形状不对 = 结果未知，返回 None。"""
    if not isinstance(reply, (list, tuple)) or len(reply) % 2:
        return None
    return _parse_hot_bucket(dict(zip(reply[::2], reply[1::2])))


def _parse_hot_bucket(raw: dict) -> dict[int, HotProxyCounters]:
    """把热榜桶（或求和脚本的应答）解析成 {宗师: 计数}，字段口径同 C++ ParseProxyHotTotals。"""
    bucket: dict[int, HotProxyCounters] = {}
    for field, value in raw.items():
        parsed = _split_master_field(field, value)
        if parsed is None:
            continue
        master_id, suffix = parsed
        count = _ledger_count(value)
        if count and suffix in _HOT_FIELD_BY_SUFFIX:
            setattr(bucket.setdefault(master_id, HotProxyCounters()), _HOT_FIELD_BY_SUFFIX[suffix], min(count, _MAX_UINT32))
    return bucket


def _iter_online_players(scene):
    """遍历本节点全部在线玩家，一人一次：locations 按 player_id 索引到所在世界（按世界遍历会是在线人数的平方）。"""
    for pid, world in scene.locations.items():
        target = world.players.get(pid)
        if pid and target is not None:
            yield pid, target


def _share_active_team(scene, player_a_id: int, player_b_id: int) -> bool:
    if not player_a_id or not player_b_id or player_a_id == player_b_id:
        return False
    team = getattr(scene, "team", None)
    if team is not None and hasattr(team, "team_id"):
        ta = int(team.team_id(player_a_id))
        tb = int(team.team_id(player_b_id))
        return ta != 0 and ta == tb
    return False


class CraftProxyStallDirectory:
    """同区跨节点/离线名匠代打共享摊位目录与口碑/24h 热榜台账（r2-craft#4）。

    权威数据在按区控制 Redis 的两个哈希里，本对象只持本节点镜像：摊位与热榜供西市列表读，待回账镜像只说明
    「谁可能有待回账」，入账数额以原子取走的为准。没配控制 Redis（本地单进程模式）时镜像就是台账本身。
    """

    def __init__(self, protocol, *, control_redis=None, zone_id: int = 0, clock_ms: Callable[[], int] = now_ms) -> None:
        self.protocol = protocol
        self._redis = control_redis
        self._zone_id = zone_id
        self._clock_ms = clock_ms
        self._stall_key = proxy_stall_directory_key(zone_id)
        self._ledger_key = proxy_reputation_ledger_key(zone_id)
        self._stalls: dict[int, object] = {}
        self._pending_reputation: dict[int, PendingProxyReputation] = {}
        # 热榜镜像 = 上次整表刷新读回的近 24 小时之和 + 之后本节点按 UTC 小时记下的成交（本地单进程模式只有后者，
        # 窗口靠它按小时滚动）。下次刷新用桶和整体接替；hot_window() 求和时丢掉比窗口更早的本地桶。
        self._hot_refreshed_totals: dict[int, HotProxyCounters] = {}
        self._hot_local_buckets: dict[int, dict[int, HotProxyCounters]] = {}

    @staticmethod
    def _display_name(data, player_id: int) -> str:
        name = getattr(data.profile_component, "name", "") if data.HasField("profile_component") else ""
        return name or f"宗师#{player_id}"

    def build_stall_info(self, player_id: int, data):
        if not player_id or not data.HasField("craft_component"):
            return None
        craft = data.craft_component
        if not craft.proxy_stall_open:
            return None
        f_tier = mastery_tier_for_exp(int(craft.forge_mastery_exp))
        s_tier = mastery_tier_for_exp(int(craft.scroll_mastery_exp))
        forge_ready = f_tier >= MASTERY_MASTER and valid_proxy_fee(int(craft.proxy_forge_fee_gold))
        scroll_ready = s_tier >= MASTERY_MASTER and valid_proxy_fee(int(craft.proxy_scroll_fee_gold))
        if not forge_ready and not scroll_ready:
            return None
        tag = int(craft.master_showcase_tag)
        return self.protocol.message(
            "CraftProxyStallInfo",
            master_player_id=player_id,
            master_name=self._display_name(data, player_id),
            forge_mastery_tier=f_tier,
            scroll_mastery_tier=s_tier,
            top_quality_rate_bps=forge_top_quality_rate_bps(f_tier),
            scroll_bonus_rate_bps=scroll_bonus_fragment_rate_bps(s_tier),
            proxy_forge_fee_gold=int(craft.proxy_forge_fee_gold) if forge_ready else 0,
            proxy_scroll_fee_gold=int(craft.proxy_scroll_fee_gold) if scroll_ready else 0,
            proxy_orders_completed=int(craft.proxy_orders_completed),
            supreme_forged_count=int(craft.supreme_forged_count),
            rare_books_crafted_count=int(craft.rare_books_crafted_count),
            master_showcase_tag=tag if tag <= MAX_MASTER_SHOWCASE_TAG else 0,
            equipped_craft_title_id=active_craft_title_id(craft),
            team_free_proxy=bool(craft.team_free_proxy),
            viewer_gets_free_proxy=False,
        )

    @staticmethod
    def is_valid_stall(info) -> bool:
        if info is None or not info.master_player_id:
            return False
        forge_ready = (
            info.forge_mastery_tier >= MASTERY_MASTER
            and valid_proxy_fee(int(info.proxy_forge_fee_gold))
        )
        scroll_ready = (
            info.scroll_mastery_tier >= MASTERY_MASTER
            and valid_proxy_fee(int(info.proxy_scroll_fee_gold))
        )
        return forge_ready or scroll_ready

    async def sync_player(self, player_id: int, data) -> None:
        """按存档重建本节点镜像里这名宗师的摊位，并写穿到按区目录（同 C++ SyncPlayerProxyStall：营业 HSET，下架 HDEL）。"""
        if not player_id:
            return
        info = self.build_stall_info(player_id, data)
        if info is not None:
            self._stalls[player_id] = info
            if self._redis is not None:
                await self._guarded_write(
                    "publish stall", player_id,
                    self._redis.hset(self._stall_key, str(player_id), info.SerializeToString()),
                )
            return
        removed = self._stalls.pop(player_id, None) is not None
        closed = data.HasField("craft_component") and not data.craft_component.proxy_stall_open
        if (removed or closed) and self._redis is not None:
            await self._guarded_write("remove stall", player_id, self._redis.hdel(self._stall_key, str(player_id)))

    def find(self, master_player_id: int, *, scene=None):
        """买家下单时解析宗师摊位（同 C++ ResolveSharedProxyStall）：宗师在本节点在线以他的存档为准，否则查镜像。"""
        target = _find_online_player_in_scene(scene, master_player_id) if scene is not None else None
        if target is None:
            info = self._stalls.get(master_player_id)
            return info if self.is_valid_stall(info) else None
        if not scene.mutable(target):
            return None
        info = self.build_stall_info(master_player_id, scene.rows.assemble(target.snapshot))
        if info is None:
            self._stalls.pop(master_player_id, None)
        else:
            self._stalls[master_player_id] = info
        return info

    async def record_order(
        self, master_player_id: int, supreme_delta: int, rare_delta: int, net_gold_earned: int, buyer_name: str,
    ) -> None:
        """记一笔代打成交：待回账 HINCRBY / HSET 进按区台账，热榜 HINCRBY 进当前小时的桶；本节点镜像同步累加。"""
        if not master_player_id:
            return
        hour = self._clock_ms() // _MS_PER_HOUR
        hot = self._hot_local_buckets.setdefault(hour, {}).setdefault(master_player_id, HotProxyCounters())
        hot.orders = _sat_add_u32(hot.orders, 1)
        hot.supreme = _sat_add_u32(hot.supreme, supreme_delta)
        pending = self._pending_reputation.setdefault(master_player_id, PendingProxyReputation())
        pending.orders = _sat_add_u32(pending.orders, 1)
        pending.supreme = _sat_add_u32(pending.supreme, supreme_delta)
        pending.rare = _sat_add_u32(pending.rare, rare_delta)
        pending.net_gold = _sat_add_u64(pending.net_gold, net_gold_earned)
        if buyer_name:
            pending.last_buyer = buyer_name
        if self._redis is None:
            return
        pipe = self._redis.pipeline(transaction=False)
        self._queue_ledger_increments(
            pipe, master_player_id, {":o": 1, ":s": supreme_delta, ":r": rare_delta, ":g": net_gold_earned}, buyer_name,
        )
        hot_key = proxy_hot_bucket_key(self._zone_id, hour)
        pipe.hincrby(hot_key, f"{master_player_id}:o", 1)
        if supreme_delta > 0:
            pipe.hincrby(hot_key, f"{master_player_id}:s", supreme_delta)
        pipe.expire(hot_key, _PROXY_HOT_BUCKET_TTL_SEC)
        await self._guarded_write("record reputation", master_player_id, pipe.execute())

    def has_pending(self, master_player_id: int) -> bool:
        """本节点镜像里这名宗师可能有待回账（只是提示，数额以取走的为准）。"""
        pending = self._pending_reputation.get(master_player_id)
        if pending is not None and pending.is_empty:
            del self._pending_reputation[master_player_id]
            return False
        return pending is not None

    async def take_pending(self, master_player_id: int) -> PendingProxyReputation | None:
        """从台账原子取走一名宗师的待回账；None = 结果未知（断连、应答畸形），调用方什么都不入账。只在宗师自己的串行任务里调用。"""
        if self._redis is None:
            # 本地单进程：镜像就是台账，宗师锁已串行化他的回账；读出即算取走，落盘后再由 settle_taken 扣掉。
            return replace(self._pending_reputation.get(master_player_id) or PendingProxyReputation())
        try:
            reply = await self._redis.eval(TAKE_PROXY_REPUTATION_LUA, 1, self._ledger_key, str(master_player_id))
        except Exception as exc:
            if not _redis_failure(exc):
                raise
            logger.warning(
                "Craft proxy reputation take failed on scene control Redis; nothing applied, the pending delta is "
                "either still in Redis or lost (display-only counters) master_id=%s error=%s", master_player_id, exc,
            )
            return None
        taken = _parse_taken_reputation(reply)
        if taken is None:
            logger.warning(
                "Craft proxy reputation take reply malformed; nothing applied master_id=%s reply=%r",
                master_player_id, reply,
            )
        return taken

    async def give_back(self, master_player_id: int, taken: PendingProxyReputation) -> None:
        """取走的数额确定没写进存档：原样 HINCRBY 还回台账，下一次再取（本地单进程模式镜像没扣，不用还）。"""
        if self._redis is None:
            return
        pipe = self._redis.pipeline(transaction=False)
        self._queue_ledger_increments(
            pipe, master_player_id,
            {":o": taken.orders, ":s": taken.supreme, ":r": taken.rare, ":g": taken.net_gold},
            taken.last_buyer,
        )
        await self._guarded_write("give back reputation", master_player_id, pipe.execute())

    def settle_taken(self, master_player_id: int, taken: PendingProxyReputation) -> None:
        """取走的数额已归宗师存档：镜像扣掉这部分，取走之后本节点又记下的增量留着。"""
        cur = self._pending_reputation.get(master_player_id)
        if cur is None:
            return
        cur.orders = max(0, cur.orders - taken.orders)
        cur.supreme = max(0, cur.supreme - taken.supreme)
        cur.rare = max(0, cur.rare - taken.rare)
        cur.net_gold = max(0, cur.net_gold - taken.net_gold)
        if cur.is_empty:
            del self._pending_reputation[master_player_id]

    def forget_pending(self, master_player_id: int) -> None:
        """取到全 0：别的节点已回过账，或镜像过时；作废这条提示，下一次刷新以台账为准。"""
        self._pending_reputation.pop(master_player_id, None)

    async def refresh(self, scene) -> None:
        """整表读回按区目录与待回账、近 24 小时热榜桶之和，覆盖本节点镜像（同 C++ RequestProxyDirectoryRefresh +
        RefreshProxyHotRanking），再给本节点在线且镜像里可能有待回账的宗师投回账任务。Redis 故障只告警，保留镜像。"""
        if self._redis is None:
            return
        hot_keys = proxy_hot_window_bucket_keys(self._zone_id, self._clock_ms() // _MS_PER_HOUR)
        pipe = self._redis.pipeline(transaction=False)
        pipe.hgetall(self._stall_key)
        pipe.hgetall(self._ledger_key)
        pipe.eval(SUM_PROXY_HOT_LUA, len(hot_keys), *hot_keys)
        try:
            stalls_raw, ledger_raw, hot_reply = await pipe.execute()
        except Exception as exc:
            if not _redis_failure(exc):
                raise
            logger.warning("Craft proxy directory refresh failed; keeping cached stalls and counters error=%s", exc)
            return
        self._stalls = self._parse_stalls(stalls_raw)
        self._pending_reputation = _parse_reputation_ledger(ledger_raw)
        hot_totals = _parse_hot_totals(hot_reply)
        if hot_totals is None:
            logger.warning("Unexpected hot ranking reply shape; keeping cached counters reply=%r", hot_reply)
        else:
            self._hot_refreshed_totals = hot_totals
            self._hot_local_buckets.clear()
        for master_id in tuple(self._pending_reputation):
            if _find_online_player_in_scene(scene, master_id) is not None:
                _schedule_proxy_reputation_drain(scene, master_id)

    async def refresh_loop(self, scene) -> None:
        """每 5 秒刷新一次本节点镜像（同 C++ StartProxyStallDirectory 的定时器），启动时先刷一次。

        这是 runtime 的后台任务，任务结束会被当成节点故障停机：展示用的目录出了意料之外的错，只记日志、下一轮再刷。"""
        logger.info(
            "Craft proxy directory refresh started interval_sec=%s", PROXY_DIRECTORY_REFRESH_INTERVAL_SEC,
        )
        while True:
            try:
                await self.refresh(scene)
            except Exception:
                logger.exception("Craft proxy directory refresh raised unexpectedly; retrying next round")
            await asyncio.sleep(PROXY_DIRECTORY_REFRESH_INTERVAL_SEC)

    def hot_window(self) -> dict[int, HotProxyCounters]:
        """近 24 小时热榜 = 上次刷新读回的桶和 + 之后本节点记下、仍在窗口里的小时桶；比窗口更早的本地桶丢掉。"""
        oldest = self._clock_ms() // _MS_PER_HOUR - PROXY_HOT_WINDOW_HOURS + 1
        for stale_hour in [hour for hour in self._hot_local_buckets if hour < oldest]:
            del self._hot_local_buckets[stale_hour]
        totals = {mid: replace(counts) for mid, counts in self._hot_refreshed_totals.items()}
        for bucket in self._hot_local_buckets.values():
            for master_id, counts in bucket.items():
                total = totals.setdefault(master_id, HotProxyCounters())
                total.orders = _sat_add_u32(total.orders, counts.orders)
                total.supreme = _sat_add_u32(total.supreme, counts.supreme)
        return totals

    def _parse_stalls(self, raw: dict) -> dict[int, object]:
        stalls: dict[int, object] = {}
        for value in raw.values():
            try:
                info = self.protocol.parse("CraftProxyStallInfo", value)
            except (DecodeError, TypeError):
                continue
            if self.is_valid_stall(info):
                info.viewer_gets_free_proxy = False
                stalls[int(info.master_player_id)] = info
        return stalls

    def _queue_ledger_increments(self, pipe, master_player_id: int, increments: dict[str, int], buyer_name: str) -> None:
        for suffix, delta in increments.items():
            if 0 < delta <= _MAX_INT64:
                pipe.hincrby(self._ledger_key, f"{master_player_id}{suffix}", delta)
        if buyer_name:
            pipe.hset(self._ledger_key, f"{master_player_id}:b", buyer_name.encode())

    @staticmethod
    async def _guarded_write(action: str, master_player_id: int, command) -> None:
        # 写失败只告警（同 C++）：这些是展示用计数，不能让已提交的订单或已落盘的开摊失败。
        try:
            await command
        except Exception as exc:
            if not _redis_failure(exc):
                raise
            logger.warning(
                "Craft proxy %s failed on scene control Redis master_id=%s error=%s", action, master_player_id, exc,
            )

    def list_stalls(self, *, scene=None, viewer_player_id: int = 0) -> list:
        merged: dict[int, object] = {}
        for pid, info in self._stalls.items():
            if self.is_valid_stall(info):
                merged[pid] = clone(info)
        if scene is not None:
            for pid, target in _iter_online_players(scene):
                if not scene.mutable(target):
                    merged.pop(pid, None)
                    continue
                data = scene.rows.assemble(target.snapshot)
                live = self.build_stall_info(pid, data)
                if live is not None:
                    merged[pid] = live
                else:
                    merged.pop(pid, None)
        rows = []
        hot_window = self.hot_window()
        for pid in sorted(merged):
            stall = merged[pid]
            if not self.is_valid_stall(stall):
                continue
            pending = self._pending_reputation.get(pid)
            if pending is not None:
                stall.proxy_orders_completed = _sat_add_u32(stall.proxy_orders_completed, pending.orders)
                stall.supreme_forged_count = _sat_add_u32(stall.supreme_forged_count, pending.supreme)
                stall.rare_books_crafted_count = _sat_add_u32(stall.rare_books_crafted_count, pending.rare)
            hot = hot_window.get(pid)
            stall.recent_24h_orders = hot.orders if hot is not None else 0
            stall.recent_24h_supreme = hot.supreme if hot is not None else 0
            stall.hot_rank = 0
            stall.viewer_gets_free_proxy = bool(
                scene is not None
                and stall.team_free_proxy
                and _share_active_team(scene, viewer_player_id, pid)
            )
            rows.append(stall)

        # 全区西市「极品热榜」Top 3 评选（与 C++ ListProxyStalls 完全一致）。
        eligible = [
            idx for idx, s in enumerate(rows)
            if s.recent_24h_orders > 0 or s.proxy_orders_completed > 0 or s.supreme_forged_count > 0
        ]
        eligible.sort(
            key=lambda idx: (
                -hot_rank_score(rows[idx].recent_24h_orders, rows[idx].recent_24h_supreme),
                -int(rows[idx].recent_24h_supreme),
                -int(rows[idx].supreme_forged_count),
                -int(rows[idx].proxy_orders_completed),
                int(rows[idx].master_player_id),
            )
        )
        for rank, idx in enumerate(eligible[:HOT_RANK_TOP_LIMIT], start=1):
            rows[idx].hot_rank = rank
        return rows


def get_proxy_stall_directory(scene) -> CraftProxyStallDirectory:
    """取挂在 scene 上的共享摊位目录：运行时按控制 Redis 装好；单测与直接建的场景按需建本节点内存版。"""
    directory = getattr(scene, "craft_proxy_directory", None)
    if directory is None:
        directory = CraftProxyStallDirectory(scene.protocol)
        scene.craft_proxy_directory = directory
    return directory


def _find_online_player_in_scene(scene, player_id: int):
    world = scene.locations.get(player_id)
    return None if world is None else world.players.get(player_id)


def _schedule_proxy_reputation_drain(scene, master_player_id: int) -> None:
    """向宗师自己的串行任务投一次回账；执行时按 player_id 重新找人，已离开就什么都不做，数额留在台账。"""

    async def drain_master() -> None:
        target = _find_online_player_in_scene(scene, master_player_id)
        if target is not None:
            await drain_pending_proxy_reputation(scene, target)

    scene.schedule_player_task(master_player_id, drain_master)


async def record_proxy_order_reputation(
    scene,
    master_player_id: int,
    supreme_delta: int = 0,
    rare_delta: int = 0,
    net_gold_earned: int = 0,
    buyer_name: str = "",
) -> None:
    """买家的任务里记一笔名匠代打/代制口碑与热榜：只写按区台账，不碰宗师存档、不拿宗师的锁（防 A↔B 互购死锁
    与跨玩家写）；宗师在本节点在线时，投他自己的串行任务去取走入账。"""
    if not master_player_id:
        return
    await get_proxy_stall_directory(scene).record_order(
        master_player_id, supreme_delta, rare_delta, net_gold_earned, buyer_name,
    )
    if _find_online_player_in_scene(scene, master_player_id) is not None:
        _schedule_proxy_reputation_drain(scene, master_player_id)


async def drain_pending_proxy_reputation(scene, player) -> bool:
    """在宗师自己的串行任务内回账（同 C++ DrainPendingProxyReputation + ApplyTakenProxyReputation）：先从台账原子
    取走，再记进存档落盘；确定没写进去就原样还回，取走结果未知只告警、什么都不记。回 True = 已入账并落盘。"""
    if player is None or not player.player_id or not scene.mutable(player):
        return False
    directory = getattr(scene, "craft_proxy_directory", None)
    master_id = player.player_id
    # 有待确认的存档时这次提交必被拒：先不取，等它落定后的下一次回账。
    if directory is None or player.pending_save is not None or not directory.has_pending(master_id):
        return False
    taken = await directory.take_pending(master_id)
    if taken is None:
        return False
    if taken.is_empty:
        directory.forget_pending(master_id)
        return False
    if not scene.mutable(player) or player.pending_save is not None:
        await directory.give_back(master_id, taken)
        logger.info(
            "Master is no longer writable here; taken reputation given back master_id=%s orders=%s",
            master_id, taken.orders,
        )
        return False
    staged = clone(scene.rows.assemble(player.snapshot))
    apply_reputation_to_craft(
        staged.craft_component, taken.orders, taken.supreme, taken.rare, taken.net_gold, taken.last_buyer,
    )
    rejected = False
    try:
        await scene.commit_player_data(player, staged)
    except Exception as exc:
        # 待存盘行没留下 = 确定没写进去；留下了 = 结果未知，场景会在下次落盘时重试这批行。
        rejected = player.pending_save is None
        logger.warning(
            "Failed to persist drained proxy reputation master_id=%s orders=%s given_back=%s error=%r",
            master_id, taken.orders, rejected, exc,
        )
        if rejected:
            await directory.give_back(master_id, taken)
        return False
    finally:
        if not rejected:
            # 已落盘，或这批已在待确认存档里等重试（含任务被取消）：镜像扣掉、绝不还回，否则重试成功后会记两次。
            directory.settle_taken(master_id, taken)
    await directory.sync_player(master_id, staged)
    logger.info(
        "Drained offline proxy reputation master_id=%s orders=%s supreme=%s rare=%s net_gold=%s",
        master_id, taken.orders, taken.supreme, taken.rare, taken.net_gold,
    )
    return True


class SceneSkillCraft:
    """Scene 分卷宗永久技能制书、研习、拆页与主城宗师代打摊位模块。"""

    def __init__(
        self,
        scene,
        tables,
        assets=None,
        *,
        clock=now_ms,
        roll_fn: Callable[[int], int] | None = None,
        craft_unlock_gate: bool = False,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        self._roll_fn = roll_fn
        self._craft_unlock_gate = bool(craft_unlock_gate)
        self._skill_has_scroll = (
            "Skill" in tables.tables
            and len(tables["Skill"]) > 0
            and hasattr(tables["Skill"].rows()[0], "scroll_id")
        )

    def register(self) -> None:
        """向 SceneNode 注册全部 11 条技能制书、封卷、里程碑、批量拆页与代打摊位 RPC（560..563 已退役迁 trade）。"""
        self.scene.register(_SERVICE, "GetSkillCraftingState", self.get_state)
        self.scene.register(_SERVICE, "CraftSkillBook", self.craft_skill_book, mutating=True)
        self.scene.register(_SERVICE, "LearnSkillBook", self.learn_skill_book, mutating=True)
        self.scene.register(_SERVICE, "DismantleSkillBook", self.dismantle_skill_book, mutating=True)
        self.scene.register(_SERVICE, "SealSkillBook", self.seal_skill_book, mutating=True)
        self.scene.register(_SERVICE, "UnsealSkillBook", self.unseal_skill_book, mutating=True)
        self.scene.register(_SERVICE, "SetCraftProxyStall", self.set_proxy_stall, mutating=True)
        self.scene.register(_SERVICE, "ListCraftProxyStalls", self.list_proxy_stalls)
        self.scene.register(_SERVICE, "ClaimScrollCollectionMilestone", self.claim_scroll_collection_milestone, mutating=True)
        self.scene.register(_SERVICE, "BatchDismantleSkillBooks", self.batch_dismantle_skill_books, mutating=True)
        self.scene.register(_SERVICE, "AckCraftProxyDigest", self.ack_proxy_digest, mutating=True)

    # ── 配置与校验辅助 ──

    def _roll_below(self, bound: int) -> int:
        if bound <= 1:
            return 0
        if self._roll_fn is not None:
            return int(self._roll_fn(bound)) % bound
        return random.randrange(bound)

    def _scrolls(self) -> list:
        if "SkillScrollCraft" not in self.tables.tables:
            return []
        rows = [row for row in self.tables["SkillScrollCraft"].rows() if row.enabled]
        rows.sort(key=lambda r: r.id)
        return rows

    def _scroll_by_id(self, scroll_id: int):
        if "SkillScrollCraft" not in self.tables.tables:
            return None
        row = self.tables["SkillScrollCraft"].get(scroll_id)
        return row if row is not None and row.enabled else None

    def _collect_scroll_skills(self, scroll_id: int) -> list[int]:
        if self._skill_has_scroll:
            ids = [
                row.id for row in self.tables["Skill"].rows()
                if getattr(row, "scroll_id", 0) == scroll_id
                and getattr(row, "season_id", 0) == 0
                and getattr(row, "battle_model", 1) == 1
            ]
            ids.sort()
            return ids
        return sorted(sid for sid, _ in _DEFAULT_SCROLL_SKILLS.get(scroll_id, ()))

    def _skill_caster_mask(self, skill_id: int) -> int:
        if self._skill_has_scroll:
            row = self.tables["Skill"].get(skill_id)
            if row is None or getattr(row, "season_id", 0) != 0:
                return 0
            return int(getattr(row, "caster_mask", 1))
        for entries in _DEFAULT_SCROLL_SKILLS.values():
            for sid, mask in entries:
                if sid == skill_id:
                    return mask
        return 0

    def validate_tables(self) -> bool:
        """校验 SkillScrollCraft 配方与稀世绝学占比 <= 10% 硬红线。"""
        scrolls = self._scrolls()
        if not scrolls:
            return False
        total_skills = 0
        rare_skills = 0
        for row in scrolls:
            if row.id == 0 or row.rarity_tier > 1:
                return False
            pool = self._collect_scroll_skills(row.id)
            if not pool:
                return False
            if row.rarity_tier == 0:
                if (
                    not row.fragment_item_id
                    or not row.random_fragment_count
                    or row.targeted_fragment_count < row.random_fragment_count
                    or not row.dismantle_fragment_count
                    or row.dismantle_fragment_count > row.random_fragment_count
                ):
                    return False
            else:
                if (
                    not row.rare_scroll_item_id
                    or not row.rare_scroll_count
                    or not row.rare_ink_item_id
                    or not row.rare_ink_count
                ):
                    return False
            total_skills += len(pool)
            if row.rarity_tier == 1:
                rare_skills += len(pool)
        if total_skills == 0:
            return False
        return rare_skills * 10_000 <= total_skills * MAX_RARE_SKILL_RATIO_BPS

    def _writable_error(self, player) -> int:
        if not self.scene.mutable(player):
            return SKILL_UPGRADE_BUSY
        battle = getattr(self.scene, "battle", None)
        if battle is not None and battle.frozen(player.player_id):
            return SKILL_UPGRADE_BUSY
        return 0

    @staticmethod
    def _display_name(data, player_id: int) -> str:
        name = getattr(data.profile_component, "name", "") if data.HasField("profile_component") else ""
        return name or f"宗师#{player_id}"

    @staticmethod
    def _buyer_display_name(data, player_id: int) -> str:
        name = getattr(data.profile_component, "name", "") if data.HasField("profile_component") else ""
        return name or f"侠士#{player_id}"

    async def _record_proxy_order_reputation(
        self,
        master_player_id: int,
        supreme_delta: int,
        rare_delta: int,
        net_gold_earned: int,
        buyer_name: str,
    ) -> None:
        await record_proxy_order_reputation(
            self.scene,
            master_player_id,
            supreme_delta=supreme_delta,
            rare_delta=rare_delta,
            net_gold_earned=net_gold_earned,
            buyer_name=buyer_name,
        )

    async def _drain_pending_proxy_reputation(self, player) -> bool:
        return await drain_pending_proxy_reputation(self.scene, player)

    # ── 背包材料与货币辅助（兼容 Item 表未含 3406/3407/3408 时走同口径原子逻辑） ──

    @staticmethod
    def _gold_balance(data) -> int:
        return int(data.currency.values[CURRENCY_GOLD]) if len(data.currency.values) > CURRENCY_GOLD else 0

    @staticmethod
    def _deduct_gold(data, amount: int) -> bool:
        if amount == 0:
            return True
        if len(data.currency.values) <= CURRENCY_GOLD or data.currency.values[CURRENCY_GOLD] < amount:
            return False
        data.currency.values[CURRENCY_GOLD] -= amount
        return True

    def _owned_count(self, data, config_id: int) -> int:
        total = 0
        for entry in data.bag_component.items:
            if entry.bag_type != 0:
                continue
            item = self.assets.item(entry)
            if item.config_id == config_id:
                total += item.size
        return total

    def _consume_materials(self, data, counts: dict[int, int], *, tradable_only: bool = False) -> tuple[int, bool]:
        """原子扣减所需材料，返回 (err, consumed_any_bound)。"""
        if not counts:
            return 0, False
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        if not self.assets._usable(entries, bag_capacity(bag, 0)):
            return FEATURE_UNAVAILABLE, False
        consumed_bound = False
        for cid, need in sorted(counts.items()):
            if need <= 0:
                continue
            held = held_item_count(data, cid)
            known = sum(
                item.size for _, item in entries
                if item.config_id == cid and self.assets.countable(item)
                and (not tradable_only or not item.bound)
            )
            if known < need or (held and need > max(0, known - held)):
                return SKILL_CRAFT_MATERIAL_NOT_ENOUGH, False
            # 优先扣非绑定，再扣绑定；任一扣到绑定即标 consumed_bound=True（防洗白）。
            ordered = sorted(
                (
                    (e, it) for e, it in entries
                    if it.config_id == cid and self.assets.countable(it)
                    and (not tradable_only or not it.bound)
                ),
                key=lambda pair: (1 if pair[1].bound else 0, pair[1].acquire_seq, pair[1].item_id),
            )
            remaining = need
            for entry, item in ordered:
                if remaining <= 0:
                    break
                take = min(remaining, item.size)
                if take > 0:
                    if item.bound:
                        consumed_bound = True
                    item.size -= take
                    remaining -= take
                    self.assets.write_entry(entry, item)
            if remaining > 0:
                return SKILL_CRAFT_MATERIAL_NOT_ENOUGH, False
        self.assets._drop_empty(bag.items, 0)
        data.bag_component.CopyFrom(bag)
        return 0, consumed_bound

    async def _grant_stack(self, data, config_id: int, count: int, *, bound: bool) -> int:
        """向主背包发放可堆叠材料（保留 bound 绑定属性）。"""
        if count <= 0:
            return 0
        if self.assets.item_gain_blocked(data, config_id):
            return BLOCKED
        row = self.tables["Item"].get(config_id)
        max_stack = row.max_stack_size if row is not None and row.max_stack_size > 0 else 999
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        cap = bag_capacity(bag, 0)
        if not self.assets._usable(entries, cap):
            return UNAVAILABLE
        occupied = {entry.pos for entry, _ in entries}
        free_slots = (slot for slot in range(cap) if slot not in occupied)
        remaining = count
        for entry, item in sorted(entries, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
            if item.config_id == config_id and item.bound == bound and self.assets._mergeable(item, max_stack):
                added = min(remaining, max_stack - item.size)
                if added > 0:
                    item.size += added
                    remaining -= added
                    self.assets.write_entry(entry, item)
        plan = []
        while remaining > 0:
            slot = next(free_slots, None)
            if slot is None:
                return BAG_FULL
            stack = min(remaining, max_stack)
            plan.append((slot, stack))
            remaining -= stack
        if plan and self.assets.allocate_item_id is None:
            return UNAVAILABLE
        ids = self.assets.all_ids(bag)
        watermark = max((item.acquire_seq for _, item in entries), default=0)
        for slot, stack in plan:
            try:
                guid = await self.assets.allocate_item_id()
            except (OSError, RuntimeError, ValueError):
                return UNAVAILABLE
            if type(guid) is not int or not 1 <= guid <= _MAX_UINT64 or guid in ids:
                return UNAVAILABLE
            ids.add(guid)
            watermark += 1
            item = self.protocol.message(
                "ItemComp", item_id=guid, config_id=config_id, size=stack, bound=bound, acquire_seq=watermark,
            )
            self.assets.write_entry(bag.items.add(pos=slot, bag_type=0), item)
        self.assets._order(bag.items)
        data.bag_component.CopyFrom(bag)
        return 0

    # ── 快照组装 ──

    def _fill_mastery(self, craft, forge_out, scroll_out) -> None:
        f_exp = int(craft.forge_mastery_exp)
        f_tier = mastery_tier_for_exp(f_exp)
        forge_out.tier = f_tier
        forge_out.tier_name = mastery_tier_name(f_tier)
        forge_out.exp = f_exp
        forge_out.next_tier_exp = mastery_next_exp(f_exp)
        forge_out.bonus_rate_bps = forge_top_quality_rate_bps(f_tier)

        s_exp = int(craft.scroll_mastery_exp)
        s_tier = mastery_tier_for_exp(s_exp)
        scroll_out.tier = s_tier
        scroll_out.tier_name = mastery_tier_name(s_tier)
        scroll_out.exp = s_exp
        scroll_out.next_tier_exp = mastery_next_exp(s_exp)
        scroll_out.bonus_rate_bps = scroll_bonus_fragment_rate_bps(s_tier)

    def _fill_state(self, data, out) -> None:
        out.Clear()
        craft = data.craft_component
        out.scroll_craft_revision = craft.scroll_craft_revision
        self._fill_mastery(craft, out.forge_mastery, out.scroll_mastery)
        for row in self._scrolls():
            entry = out.scrolls.add(
                scroll_id=row.id,
                name=row.name,
                description=row.description,
                rarity_tier=row.rarity_tier,
                fragment_item_id=row.fragment_item_id,
                random_fragment_count=row.random_fragment_count,
                random_gold_cost=row.random_gold_cost,
                targeted_fragment_count=row.targeted_fragment_count,
                targeted_gold_cost=row.targeted_gold_cost,
                dismantle_fragment_count=row.dismantle_fragment_count,
                rare_scroll_item_id=row.rare_scroll_item_id,
                rare_scroll_count=row.rare_scroll_count,
                rare_ink_item_id=row.rare_ink_item_id,
                rare_ink_count=row.rare_ink_count,
                rare_gold_cost=row.rare_gold_cost,
            )
            entry.skill_table_ids.extend(self._collect_scroll_skills(row.id))
        for book in sorted(craft.skill_books, key=lambda b: b.book_id):
            out.skill_books.add().CopyFrom(book)
        out.book_unlocked_skill_ids.extend(sorted(set(craft.book_unlocked_skill_ids)))
        out.gold_balance = self._gold_balance(data)
        out.fragment_owned = self._owned_count(data, SCROLL_FRAGMENT_ITEM_ID)
        out.rare_scroll_owned = self._owned_count(data, RARE_SCROLL_PAGE_ITEM_ID)
        out.rare_ink_owned = self._owned_count(data, RARE_SCROLL_INK_ITEM_ID)
        out.proxy_stall_open = bool(craft.proxy_stall_open)
        out.proxy_forge_fee_gold = craft.proxy_forge_fee_gold
        out.proxy_scroll_fee_gold = craft.proxy_scroll_fee_gold
        out.craft_unlock_gate_enabled = self._craft_unlock_gate
        out.proxy_orders_completed = int(craft.proxy_orders_completed)
        out.supreme_forged_count = int(craft.supreme_forged_count)
        out.rare_books_crafted_count = int(craft.rare_books_crafted_count)
        out.master_showcase_tag = int(craft.master_showcase_tag)
        out.equipped_craft_title_id = active_craft_title_id(craft)
        out.unlocked_craft_title_ids.extend(
            unlocked_craft_title_ids(
                int(craft.forge_mastery_exp),
                int(craft.scroll_mastery_exp),
                int(craft.supreme_forged_count),
                int(craft.rare_books_crafted_count),
                int(craft.proxy_orders_completed),
            )
        )
        out.team_free_proxy = bool(craft.team_free_proxy)
        out.claimed_scroll_milestone_bits = bytes(craft.claimed_scroll_milestone_bits)
        if (
            int(craft.unread_proxy_digest_orders) > 0
            or int(craft.unread_proxy_digest_net_gold) > 0
            or int(craft.unread_proxy_digest_new_titles_mask) != 0
        ):
            digest = out.unread_proxy_digest
            digest.orders_completed = int(craft.unread_proxy_digest_orders)
            digest.supreme_forged_count = int(craft.unread_proxy_digest_supreme)
            digest.rare_books_crafted_count = int(craft.unread_proxy_digest_rare)
            digest.net_gold_earned = int(craft.unread_proxy_digest_net_gold)
            digest.last_buyer_name = craft.unread_proxy_digest_last_buyer
            digest.newly_unlocked_title_mask = int(craft.unread_proxy_digest_new_titles_mask)

    def _finish(self, response, data, error: int = 0):
        if error:
            response.error_message.id = error
        self._fill_state(data, response.state)
        return response

    # ── 客户端 RPC ──

    async def get_state(self, _world, player, _route, _request):
        """查询技能制书卷宗目录、未学技能书架、熟练度与代打摊位状态。"""
        response = self.protocol.message("GetSkillCraftingStateResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        await self._drain_pending_proxy_reputation(player)
        data = self.scene.rows.assemble(player.snapshot)
        return self._finish(response, data)

    async def craft_skill_book(self, _world, player, _route, request):
        """分卷宗随机/定向/稀世制书，支持主城宗师代制与 client_request_id 幂等。"""
        response = self.protocol.message(
            "CraftSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_SKILL_SCROLL_CRAFT)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.bonus_fragments_granted = prior.scroll_bonus_fragments
            found = next((b for b in data.craft_component.skill_books if b.book_id == prior.crafted_skill_book_id), None)
            if found is not None:
                response.crafted_book.CopyFrom(found)
            else:
                response.crafted_book.book_id = prior.crafted_skill_book_id
                response.crafted_book.skill_table_id = prior.crafted_skill_table_id
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if craft.scroll_craft_revision == _MAX_UINT64:
            return self._finish(response, data, UNAVAILABLE)
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        scroll = self._scroll_by_id(request.scroll_id)
        if scroll is None:
            return self._finish(response, data, SKILL_SCROLL_INVALID)
        pool = self._collect_scroll_skills(scroll.id)
        if not pool:
            return self._finish(response, data, SKILL_SCROLL_INVALID)

        consumed: dict[int, int] = {}
        mode = request.mode
        if mode == CRAFT_MODE_RANDOM_REGULAR:
            if scroll.rarity_tier != 0 or request.target_skill_table_id != 0:
                return self._finish(response, data, SKILL_SCROLL_INVALID)
            chosen_skill_id = pool[self._roll_below(len(pool))]
            consumed[scroll.fragment_item_id] = scroll.random_fragment_count
            base_gold = int(scroll.random_gold_cost)
        elif mode == CRAFT_MODE_TARGETED_REGULAR:
            if scroll.rarity_tier != 0 or request.target_skill_table_id not in pool:
                return self._finish(response, data, SKILL_SCROLL_INVALID)
            chosen_skill_id = int(request.target_skill_table_id)
            consumed[scroll.fragment_item_id] = scroll.targeted_fragment_count
            base_gold = int(scroll.targeted_gold_cost)
        elif mode == CRAFT_MODE_TARGETED_RARE:
            if scroll.rarity_tier != 1 or request.target_skill_table_id not in pool:
                return self._finish(response, data, SKILL_SCROLL_INVALID)
            chosen_skill_id = int(request.target_skill_table_id)
            consumed[scroll.rare_scroll_item_id] = scroll.rare_scroll_count
            consumed[scroll.rare_ink_item_id] = scroll.rare_ink_count
            base_gold = int(scroll.rare_gold_cost)
        else:
            return self._finish(response, data, INVALID)

        proxy_master_id = int(request.proxy_master_player_id)
        proxy_fee_gold = 0
        proxy_net_fee = 0
        proxy_master_name = ""
        effective_tier = mastery_tier_for_exp(int(craft.scroll_mastery_exp))
        outbox = getattr(self.scene, "mail_outbox", None)
        if proxy_master_id != 0:
            if proxy_master_id == player.player_id:
                return self._finish(response, data, EQUIP_PROXY_MASTER_UNAVAILABLE)
            stall = get_proxy_stall_directory(self.scene).find(proxy_master_id, scene=self.scene)
            if (
                stall is None
                or stall.scroll_mastery_tier < MASTERY_MASTER
                or not valid_proxy_fee(int(stall.proxy_scroll_fee_gold))
            ):
                return self._finish(response, data, EQUIP_PROXY_MASTER_UNAVAILABLE)
            team_free = bool(stall.team_free_proxy) and _share_active_team(self.scene, player.player_id, proxy_master_id)
            proxy_fee_gold = 0 if team_free else int(stall.proxy_scroll_fee_gold)
            proxy_net_fee = proxy_net_gold(proxy_fee_gold)
            if proxy_net_fee > 0 and (
                outbox is None
                or not outbox.can_accept(data)
                or (hasattr(self.assets, "currency_globally_blocked") and self.assets.currency_globally_blocked(CURRENCY_GOLD))
            ):
                return self._finish(response, data, EQUIP_PROXY_MASTER_UNAVAILABLE)
            base_master_name = stall.master_name or f"宗师#{proxy_master_id}"
            proxy_master_name = format_master_signature(int(stall.equipped_craft_title_id), base_master_name)
            effective_tier = MASTERY_MASTER

        # 防洗白硬隔离：代工费(proxy_fee_gold)永远只扣流通银两；仅稀世绝学定向制书的系统手续费允许优先抵扣绑定银两。
        allow_bind_for_base_fee = mode == CRAFT_MODE_TARGETED_RARE
        strict_gold = proxy_fee_gold + (0 if allow_bind_for_base_fee else base_gold)
        prefer_bind_gold = base_gold if allow_bind_for_base_fee else 0
        if strict_gold > 0 or prefer_bind_gold > 0:
            if hasattr(self.assets, "can_afford_split_gold"):
                if not self.assets.can_afford_split_gold(data, strict_gold, prefer_bind_gold):
                    return self._finish(response, data, SKILL_GOLD_NOT_ENOUGH)
            elif self._gold_balance(data) < strict_gold + prefer_bind_gold:
                return self._finish(response, data, SKILL_GOLD_NOT_ENOUGH)

        # 常规卷宗可交易残页充足时优先纯耗可交易残页，避免返还的少量绑定残页误染毒后续可交易技能书。
        prefer_tradable_only = (
            mode != CRAFT_MODE_TARGETED_RARE
            and hasattr(self.assets, "removable_tradable_count")
            and self.assets.removable_tradable_count(data, scroll.fragment_item_id) >= consumed[scroll.fragment_item_id]
        )

        staged = clone(data)
        consume_err, consumed_bound = self._consume_materials(staged, consumed, tradable_only=prefer_tradable_only)
        if consume_err:
            return self._finish(response, data, consume_err)

        if strict_gold > 0 or prefer_bind_gold > 0:
            if hasattr(self.assets, "deduct_split_gold"):
                if self.assets.deduct_split_gold(staged, strict_gold, prefer_bind_gold):
                    return self._finish(response, data, SKILL_GOLD_NOT_ENOUGH)
            elif not self._deduct_gold(staged, strict_gold + prefer_bind_gold):
                return self._finish(response, data, SKILL_GOLD_NOT_ENOUGH)

        bonus_fragments = 0
        if scroll.rarity_tier == 0:
            bonus_rate = scroll_bonus_fragment_rate_bps(effective_tier)
            if bonus_rate > 0 and self._roll_below(10_000) < bonus_rate:
                grant_err = await self._grant_stack(
                    staged, scroll.fragment_item_id, SCROLL_BONUS_FRAGMENTS_COUNT, bound=True,
                )
                if grant_err == 0:
                    bonus_fragments = SCROLL_BONUS_FRAGMENTS_COUNT

        sc = staged.craft_component
        book_id = sc.next_book_seq or 1
        new_book = sc.skill_books.add(
            book_id=book_id,
            skill_table_id=chosen_skill_id,
            scroll_id=scroll.id,
            rarity_tier=scroll.rarity_tier,
            bound=True if mode == CRAFT_MODE_TARGETED_RARE else consumed_bound,
        )
        if effective_tier == MASTERY_MASTER:
            signer_id = proxy_master_id if proxy_master_id else player.player_id
            new_book.crafter_player_id = signer_id
            new_book.crafter_name = (
                proxy_master_name
                if proxy_master_id
                else format_master_signature(active_craft_title_id(sc), self._display_name(staged, signer_id))
            )
        sc.next_book_seq = book_id + 1
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev
        if proxy_master_id == 0:
            if sc.scroll_mastery_exp < _MAX_UINT32:
                sc.scroll_mastery_exp += 1
            if mode == CRAFT_MODE_TARGETED_RARE and sc.rare_books_crafted_count < _MAX_UINT32:
                sc.rare_books_crafted_count += 1

        if proxy_master_id != 0 and proxy_net_fee > 0:
            prepared_outbox = await outbox.prepare_proxy_fee(
                staged,
                recipient_player_id=proxy_master_id,
                net_gold=proxy_net_fee,
                source_id=request.client_request_id,
                buyer_name=self._display_name(staged, player.player_id),
            )
            if not prepared_outbox:
                return self._finish(response, data, EQUIP_PROXY_MASTER_UNAVAILABLE)

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_SKILL_SCROLL_CRAFT,
            fingerprint,
            crafted_skill_book_id=book_id,
            crafted_skill_table_id=chosen_skill_id,
            scroll_craft_revision=next_rev,
            scroll_bonus_fragments=bonus_fragments,
        )

        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        if proxy_master_id != 0:
            await self._record_proxy_order_reputation(
                proxy_master_id,
                0,
                1 if mode == CRAFT_MODE_TARGETED_RARE else 0,
                proxy_net_fee,
                self._buyer_display_name(staged, player.player_id),
            )

        logger.info(
            "Skill book crafted player_id=%s book_id=%s skill_id=%s scroll_id=%s bound=%s bonus=%s",
            player.player_id, book_id, chosen_skill_id, scroll.id, new_book.bound, bonus_fragments,
        )
        response.crafted_book.CopyFrom(new_book)
        response.bonus_fragments_granted = bonus_fragments
        return self._finish(response, staged)

    async def learn_skill_book(self, _world, player, _route, request):
        """研习未学技能书：永久解锁对应技能（绝不顶掉已有技能），支持人物与宝宝。"""
        response = self.protocol.message(
            "LearnSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.book_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_LEARN_SKILL_BOOK)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.learned_skill_table_id = prior.crafted_skill_table_id
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        book_idx = next((i for i, b in enumerate(craft.skill_books) if b.book_id == request.book_id), -1)
        if book_idx < 0:
            return self._finish(response, data, SKILL_BOOK_NOT_FOUND)
        skill_id = int(craft.skill_books[book_idx].skill_table_id)
        caster_mask = self._skill_caster_mask(skill_id)
        if caster_mask == 0:
            return self._finish(response, data, INVALID_TABLE_ID)
        required_mask = 1 if request.pet_id == 0 else 2
        if (caster_mask & required_mask) == 0:
            return self._finish(response, data, SKILL_NOT_IN_CLASS)
        if skill_id in craft.book_unlocked_skill_ids:
            return self._finish(response, data, SKILL_ALREADY_BOOK_UNLOCKED)

        staged = clone(data)
        if request.pet_id != 0:
            pet = next((p for p in staged.pet_component.pets if p.pet_id == request.pet_id), None)
            if pet is None:
                return self._finish(response, data, INVALID)
            if skill_id not in pet.skill_table_ids:
                pet.skill_table_ids.append(skill_id)
            if pet.skill_ranks.get(skill_id, 0) == 0:
                pet.skill_ranks[skill_id] = 1
        else:
            existing = next(
                (s for s in staged.skill_list.skill_list if s.skill_table_id == skill_id), None,
            )
            newly_learned = existing is None or existing.rank == 0
            if existing is None:
                staged.skill_list.skill_list.add(skill_table_id=skill_id, rank=1)
            elif existing.rank == 0:
                existing.rank = 1
            if newly_learned:
                skill_row = self.tables["Skill"].get(skill_id)
                if skill_row is not None:
                    try_auto_slot_newly_learned(self.tables, staged.skill_list, skill_row)

        sc = staged.craft_component
        del sc.skill_books[book_idx]
        sc.book_unlocked_skill_ids.append(skill_id)
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_LEARN_SKILL_BOOK,
            fingerprint,
            crafted_skill_book_id=request.book_id,
            crafted_skill_table_id=skill_id,
            scroll_craft_revision=next_rev,
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Skill book learned player_id=%s book_id=%s skill_id=%s pet_id=%s",
            player.player_id, request.book_id, skill_id, request.pet_id,
        )
        response.learned_skill_table_id = skill_id
        return self._finish(response, staged)

    async def dismantle_skill_book(self, _world, player, _route, request):
        """拆解多余未学常规技能书，固定返还 80%（16 张）同绑定属性悟道残页。"""
        response = self.protocol.message(
            "DismantleSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.book_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_DISMANTLE_SKILL_BOOK)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.refunded_fragments = prior.scroll_bonus_fragments
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        book_idx = next((i for i, b in enumerate(craft.skill_books) if b.book_id == request.book_id), -1)
        if book_idx < 0:
            return self._finish(response, data, SKILL_BOOK_NOT_FOUND)
        book = craft.skill_books[book_idx]
        if book.rarity_tier != 0:
            return self._finish(response, data, SKILL_BOOK_NOT_DISMANTLABLE)
        scroll = self._scroll_by_id(book.scroll_id)
        if scroll is None or scroll.dismantle_fragment_count == 0:
            return self._finish(response, data, SKILL_BOOK_NOT_DISMANTLABLE)

        staged = clone(data)
        grant_err = await self._grant_stack(
            staged, scroll.fragment_item_id, int(scroll.dismantle_fragment_count), bound=bool(book.bound),
        )
        if grant_err:
            return self._finish(response, data, grant_err)

        sc = staged.craft_component
        del sc.skill_books[book_idx]
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_DISMANTLE_SKILL_BOOK,
            fingerprint,
            crafted_skill_book_id=book.book_id,
            crafted_skill_table_id=book.skill_table_id,
            scroll_craft_revision=next_rev,
            scroll_bonus_fragments=int(scroll.dismantle_fragment_count),
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Skill book dismantled player_id=%s book_id=%s refunded=%s",
            player.player_id, book.book_id, scroll.dismantle_fragment_count,
        )
        response.refunded_fragments = int(scroll.dismantle_fragment_count)
        return self._finish(response, staged)

    async def seal_skill_book(self, _world, player, _route, request):
        """将书架中未绑定的常规技能书封卷入包（3414 封卷道籍），供西市流通。"""
        response = self.protocol.message(
            "SealSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.book_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_SEAL_SKILL_BOOK)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.item_id = int(prior.sealed_book_item_id)
            response.skill_table_id = int(prior.crafted_skill_table_id)
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if craft.scroll_craft_revision == _MAX_UINT64:
            return self._finish(response, data, UNAVAILABLE)
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        book_idx = next((i for i, b in enumerate(craft.skill_books) if b.book_id == request.book_id), -1)
        if book_idx < 0:
            return self._finish(response, data, SKILL_BOOK_NOT_FOUND)
        book = craft.skill_books[book_idx]
        # 防洗白硬隔离：仅未绑定常规技能书允许封卷入包流通；稀世绝学或绑定书一律拒绝。
        if (
            book.bound
            or book.rarity_tier != 0
            or not book.skill_table_id
            or self.tables["Skill"].get(book.skill_table_id) is None
        ):
            return self._finish(response, data, INVALID)
        sealed_row = self.tables["Item"].get(SEALED_SKILL_BOOK_ITEM_ID)
        if sealed_row is None or sealed_row.max_stack_size != 1 or sealed_row.equip_kind != 0:
            return self._finish(response, data, INVALID_TABLE_ID)

        if self.assets.allocate_item_id is None:
            return self._finish(response, data, UNAVAILABLE)
        staged = clone(data)
        try:
            created_guid = await self.assets.allocate_item_id()
        except (OSError, RuntimeError, ValueError):
            return self._finish(response, data, UNAVAILABLE)
        if type(created_guid) is not int or not 1 <= created_guid <= _MAX_UINT64:
            return self._finish(response, data, UNAVAILABLE)

        sealed_item = self.protocol.message(
            "ItemComp",
            item_id=created_guid,
            config_id=SEALED_SKILL_BOOK_ITEM_ID,
            size=1,
            bound=False,
        )
        sealed_meta = sealed_item.sealed_skill_book
        sealed_meta.skill_table_id = int(book.skill_table_id)
        sealed_meta.scroll_id = int(book.scroll_id)
        sealed_meta.rarity_tier = 0
        if book.crafter_name:
            sealed_meta.crafter_name = book.crafter_name
        if book.crafter_player_id:
            sealed_meta.crafter_player_id = int(book.crafter_player_id)

        add_err = await self.assets.add_items(staged, {}, instances=(sealed_item,))
        if add_err:
            return self._finish(response, data, add_err)

        sc = staged.craft_component
        del sc.skill_books[book_idx]
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_SEAL_SKILL_BOOK,
            fingerprint,
            crafted_skill_book_id=book.book_id,
            crafted_skill_table_id=book.skill_table_id,
            scroll_craft_revision=next_rev,
            sealed_book_item_id=created_guid,
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Skill book sealed player_id=%s book_id=%s item_id=%s skill_id=%s",
            player.player_id, book.book_id, created_guid, book.skill_table_id,
        )
        response.item_id = created_guid
        response.skill_table_id = int(book.skill_table_id)
        return self._finish(response, staged)

    async def unseal_skill_book(self, _world, player, _route, request):
        """将主背包中的 3414 封卷道籍开卷入书架，严格继承原道具绑定状态（防洗白）。"""
        response = self.protocol.message(
            "UnsealSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.item_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_UNSEAL_SKILL_BOOK)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            found = next((b for b in data.craft_component.skill_books if b.book_id == prior.crafted_skill_book_id), None)
            if found is not None:
                response.unsealed_book.CopyFrom(found)
            else:
                response.unsealed_book.book_id = prior.crafted_skill_book_id
                response.unsealed_book.skill_table_id = prior.crafted_skill_table_id
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if craft.scroll_craft_revision == _MAX_UINT64 or craft.next_book_seq == _MAX_UINT64:
            return self._finish(response, data, UNAVAILABLE)
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        target_entry_idx = -1
        target_item = None
        for idx, entry in enumerate(data.bag_component.items):
            if entry.bag_type != 0:
                continue
            inst = self.assets.item(entry)
            if inst.item_id == request.item_id:
                target_entry_idx = idx
                target_item = inst
                break
        if target_item is None:
            return self._finish(response, data, SKILL_BOOK_NOT_FOUND)
        if (
            target_item.config_id != SEALED_SKILL_BOOK_ITEM_ID
            or target_item.size != 1
            or not target_item.HasField("sealed_skill_book")
        ):
            return self._finish(response, data, INVALID)
        sealed = target_item.sealed_skill_book
        if (
            not sealed.skill_table_id
            or sealed.rarity_tier != 0
            or self.tables["Skill"].get(sealed.skill_table_id) is None
        ):
            return self._finish(response, data, INVALID)
        if not self.assets.countable(target_item) or held_item_count(data, SEALED_SKILL_BOOK_ITEM_ID):
            return self._finish(response, data, FEATURE_UNAVAILABLE)

        staged = clone(data)
        del staged.bag_component.items[target_entry_idx]
        sc = staged.craft_component
        book_id = sc.next_book_seq or 1
        new_book = sc.skill_books.add(
            book_id=book_id,
            skill_table_id=int(sealed.skill_table_id),
            scroll_id=int(sealed.scroll_id),
            rarity_tier=0,
            bound=bool(target_item.bound),
        )
        if sealed.crafter_name:
            new_book.crafter_name = sealed.crafter_name
        if sealed.crafter_player_id:
            new_book.crafter_player_id = int(sealed.crafter_player_id)
        sc.next_book_seq = book_id + 1
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_UNSEAL_SKILL_BOOK,
            fingerprint,
            crafted_skill_book_id=book_id,
            crafted_skill_table_id=int(sealed.skill_table_id),
            scroll_craft_revision=next_rev,
            sealed_book_item_id=request.item_id,
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Skill book unsealed player_id=%s item_id=%s book_id=%s skill_id=%s bound=%s",
            player.player_id, request.item_id, book_id, sealed.skill_table_id, new_book.bound,
        )
        response.unsealed_book.CopyFrom(new_book)
        return self._finish(response, staged)

    async def set_proxy_stall(self, _world, player, _route, request):
        """宗师开启或关闭主城原子代打/代制摊位，配置固定手工费（50..100）、招牌、雅号与同队免费。"""
        response = self.protocol.message("SetCraftProxyStallResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        await self._drain_pending_proxy_reputation(player)
        data = self.scene.rows.assemble(player.snapshot)
        craft = data.craft_component
        if int(request.master_showcase_tag) > MAX_MASTER_SHOWCASE_TAG:
            return self._finish(response, data, INVALID)
        if int(request.equipped_craft_title_id) != 0 and not is_craft_title_unlocked(
            int(request.equipped_craft_title_id),
            int(craft.forge_mastery_exp),
            int(craft.scroll_mastery_exp),
            int(craft.supreme_forged_count),
            int(craft.rare_books_crafted_count),
            int(craft.proxy_orders_completed),
        ):
            return self._finish(response, data, INVALID)

        forge_master = mastery_tier_for_exp(int(craft.forge_mastery_exp)) >= MASTERY_MASTER
        scroll_master = mastery_tier_for_exp(int(craft.scroll_mastery_exp)) >= MASTERY_MASTER

        staged = clone(data)
        sc = staged.craft_component
        if not request.open:
            sc.proxy_stall_open = False
            if forge_master or scroll_master:
                sc.master_showcase_tag = int(request.master_showcase_tag)
                sc.equipped_craft_title_id = int(request.equipped_craft_title_id)
                sc.team_free_proxy = bool(request.team_free_proxy)
            try:
                await self.scene.commit_player_data(player, staged)
            except (StoreRejected, StoreUnavailable, OSError):
                return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)
            await get_proxy_stall_directory(self.scene).sync_player(player.player_id, staged)
            return self._finish(response, staged)

        if not forge_master and not scroll_master:
            return self._finish(response, data, SKILL_PROXY_STALL_REQUIRES_MASTER)

        forge_fee = int(request.proxy_forge_fee_gold)
        scroll_fee = int(request.proxy_scroll_fee_gold)
        if forge_master and not valid_proxy_fee(forge_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)
        if not forge_master and forge_fee != 0 and not valid_proxy_fee(forge_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)
        if scroll_master and not valid_proxy_fee(scroll_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)
        if not scroll_master and scroll_fee != 0 and not valid_proxy_fee(scroll_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)

        sc.proxy_stall_open = True
        sc.proxy_forge_fee_gold = forge_fee if forge_master else 0
        sc.proxy_scroll_fee_gold = scroll_fee if scroll_master else 0
        sc.master_showcase_tag = int(request.master_showcase_tag)
        sc.equipped_craft_title_id = int(request.equipped_craft_title_id)
        sc.team_free_proxy = bool(request.team_free_proxy)
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)
        await get_proxy_stall_directory(self.scene).sync_player(player.player_id, staged)
        return self._finish(response, staged)

    def list_proxy_stalls(self, _world, player, _route, _request):
        """列出同区跨节点/离线营业中的宗师代打摊位（含口碑、同队免费标记与 24h 极品热榜 Top 3）。"""
        response = self.protocol.message("ListCraftProxyStallsResponse")
        viewer_id = player.player_id if player is not None else 0
        for info in get_proxy_stall_directory(self.scene).list_stalls(scene=self.scene, viewer_player_id=viewer_id):
            response.stalls.add().CopyFrom(info)
        return response

    async def claim_scroll_collection_milestone(self, _world, player, _route, request):
        """558 ClaimScrollCollectionMilestone：领取典籍藏经阁卷宗收集里程碑绑定残页（小成 20 / 圆满 40）。"""
        response = self.protocol.message(
            "ClaimScrollCollectionMilestoneResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if (
            not request.client_request_id
            or not request.scroll_id
            or request.milestone_tier not in (SCROLL_MILESTONE_HALF_TIER, SCROLL_MILESTONE_FULL_TIER)
        ):
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(
            ledger, request, _CLIENT_REQUEST_KIND_CLAIM_SCROLL_MILESTONE,
        )
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.rewarded_fragments = int(prior.scroll_bonus_fragments)
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if craft.scroll_craft_revision == _MAX_UINT64:
            return self._finish(response, data, UNAVAILABLE)
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        scroll = self._scroll_by_id(int(request.scroll_id))
        if scroll is None:
            return self._finish(response, data, SKILL_SCROLL_INVALID)
        pool = self._collect_scroll_skills(scroll.id)
        if not pool:
            return self._finish(response, data, SKILL_SCROLL_INVALID)

        unlocked = set(craft.book_unlocked_skill_ids)
        unlocked_in_scroll = sum(1 for sid in pool if sid in unlocked)
        required = (
            scroll_half_threshold(len(pool))
            if request.milestone_tier == SCROLL_MILESTONE_HALF_TIER
            else len(pool)
        )
        if unlocked_in_scroll < required:
            return self._finish(response, data, INVALID)

        bit_idx = scroll_milestone_bit_index(scroll.id, int(request.milestone_tier))
        if has_bit(bytes(craft.claimed_scroll_milestone_bits), bit_idx):
            return self._finish(response, data, INVALID)

        reward_count = scroll_milestone_reward_fragments(int(request.milestone_tier))
        frag_item_id = int(scroll.fragment_item_id) if scroll.fragment_item_id else SCROLL_FRAGMENT_ITEM_ID

        staged = clone(data)
        grant_err = await self._grant_stack(staged, frag_item_id, reward_count, bound=True)
        if grant_err:
            return self._finish(response, data, grant_err)

        sc = staged.craft_component
        sc.claimed_scroll_milestone_bits = set_bit(bytes(sc.claimed_scroll_milestone_bits), bit_idx)
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_CLAIM_SCROLL_MILESTONE,
            fingerprint,
            scroll_craft_revision=next_rev,
            scroll_bonus_fragments=reward_count,
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Scroll collection milestone claimed player_id=%s scroll_id=%s tier=%s fragments=%s",
            player.player_id, scroll.id, request.milestone_tier, reward_count,
        )
        response.rewarded_fragments = reward_count
        return self._finish(response, staged)

    async def batch_dismantle_skill_books(self, _world, player, _route, request):
        """559 BatchDismantleSkillBooks：一次拆解 1..20 本未学常规书（严禁稀世绝学），返还绑定残页。"""
        response = self.protocol.message(
            "BatchDismantleSkillBooksResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if (
            not request.client_request_id
            or not request.book_ids
            or len(request.book_ids) > MAX_BATCH_DISMANTLE_COUNT
        ):
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(
            ledger, request, _CLIENT_REQUEST_KIND_BATCH_DISMANTLE_SKILL_BOOKS,
        )
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.dismantled_count = int(prior.crafted_skill_table_id)
            response.refunded_fragments = int(prior.scroll_bonus_fragments)
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if craft.scroll_craft_revision == _MAX_UINT64:
            return self._finish(response, data, UNAVAILABLE)
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        by_book_id = {int(b.book_id): b for b in craft.skill_books}
        unique_book_ids: set[int] = set()
        refund_by_item: dict[int, int] = {}
        total_refund = 0
        for raw_id in request.book_ids:
            book_id = int(raw_id)
            if not book_id or book_id in unique_book_ids:
                return self._finish(response, data, INVALID)
            unique_book_ids.add(book_id)
            book = by_book_id.get(book_id)
            if book is None:
                return self._finish(response, data, SKILL_BOOK_NOT_FOUND)
            if book.rarity_tier != 0:
                return self._finish(response, data, SKILL_BOOK_NOT_DISMANTLABLE)
            scroll = self._scroll_by_id(int(book.scroll_id))
            if scroll is None or not scroll.dismantle_fragment_count:
                return self._finish(response, data, SKILL_BOOK_NOT_DISMANTLABLE)
            frag_id = int(scroll.fragment_item_id) if scroll.fragment_item_id else SCROLL_FRAGMENT_ITEM_ID
            refund_by_item[frag_id] = refund_by_item.get(frag_id, 0) + int(scroll.dismantle_fragment_count)
            total_refund += int(scroll.dismantle_fragment_count)

        staged = clone(data)
        for item_id, count in sorted(refund_by_item.items()):
            grant_err = await self._grant_stack(staged, item_id, count, bound=True)
            if grant_err:
                return self._finish(response, data, grant_err)

        sc = staged.craft_component
        kept = [b for b in sc.skill_books if int(b.book_id) not in unique_book_ids]
        del sc.skill_books[:]
        for b in kept:
            sc.skill_books.add().CopyFrom(b)
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        dismantled_count = len(unique_book_ids)
        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_BATCH_DISMANTLE_SKILL_BOOKS,
            fingerprint,
            crafted_skill_table_id=dismantled_count,
            scroll_craft_revision=next_rev,
            scroll_bonus_fragments=total_refund,
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Skill books batch dismantled player_id=%s count=%s refunded=%s",
            player.player_id, dismantled_count, total_refund,
        )
        response.dismantled_count = dismantled_count
        response.refunded_fragments = total_refund
        return self._finish(response, staged)

    async def ack_proxy_digest(self, _world, player, _route, _request):
        """573 AckCraftProxyDigest：先回账离线代打口碑，再幂等清零未读营业战报。"""
        response = self.protocol.message("AckCraftProxyDigestResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        await self._drain_pending_proxy_reputation(player)
        data = self.scene.rows.assemble(player.snapshot)
        craft = data.craft_component
        had_digest = (
            int(craft.unread_proxy_digest_orders) > 0
            or int(craft.unread_proxy_digest_supreme) > 0
            or int(craft.unread_proxy_digest_rare) > 0
            or int(craft.unread_proxy_digest_net_gold) > 0
            or bool(craft.unread_proxy_digest_last_buyer)
            or int(craft.unread_proxy_digest_new_titles_mask) != 0
        )
        if not had_digest:
            return self._finish(response, data)

        staged = clone(data)
        sc = staged.craft_component
        sc.unread_proxy_digest_orders = 0
        sc.unread_proxy_digest_supreme = 0
        sc.unread_proxy_digest_rare = 0
        sc.unread_proxy_digest_net_gold = 0
        sc.unread_proxy_digest_last_buyer = ""
        sc.unread_proxy_digest_new_titles_mask = 0
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)
        logger.info("Craft proxy digest acked player_id=%s", player.player_id)
        return self._finish(response, staged)
