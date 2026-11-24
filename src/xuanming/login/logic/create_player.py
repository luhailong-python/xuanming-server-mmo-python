"""CreatePlayer (Go ``createplayerlogic.go`` + ``shared/playername``)."""

import asyncio
import logging
import secrets
import unicodedata
from functools import cache

from ...data import ConfigTables
from .. import keys, login_session, session_fence, tips
from ..directory import RESERVE_INVALID, RESERVE_OK, RESERVE_TAKEN, Conflict, DirectoryError, Full, Invalid, \
    NotCommitted, status_name
from ..locker import RedisLock
from . import tip

logger = logging.getLogger(__name__)

_VALID_APPEARANCES = frozenset((
    "", "00_reference_topright_boy", "01_ice_sword_girl", "02_fire_talisman_boy", "03_lotus_healer_girl",
    "04_mountain_guardian_boy", "05_celestial_musician_girl", "08_alchemy_prodigy_boy", "09_bamboo_archer_girl",
    "10_crimson_spear_girl", "15_water_dragon_scholar_boy", "17_ghost_script_calligrapher_boy",
))
_RETIRED_APPEARANCES = frozenset(("07_moon_shadow_assassin_girl", "14_short_hair_snow_summoner_girl",
                                  "20_star_formation_master_girl"))
_REMOVED_APPEARANCE = "06_thunder_caster_boy"
_SENSITIVE_SUBSTRINGS = ("管理员", "客服", "官方", "系统", "运营")
_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
_DELAYED_RELEASE = 10
_TEXT_SCENE_ROLE_NAME = 1
_TEXT_VERDICT_PASS = 1
_CONTENT_GUARD_PATH = "/content_guard.ContentGuard/CheckText"
_CONTENT_GUARD_TIMEOUT = 1.5


def _allowed(char: str) -> bool:
    code = ord(char)
    return ("0" <= char <= "9" or "A" <= char <= "Z" or "a" <= char <= "z" or code == 0x3007
            or 0x3400 <= code <= 0x4DBF or 0x4E00 <= code <= 0x9FFF)


def normalize_name(raw: str, min_chars: int, max_chars: int) -> tuple[str, str]:
    """Return (display, verdict) with verdict in ok/empty/invalid/sensitive, as Go playername.Normalize."""
    name = unicodedata.normalize("NFKC", raw).strip()
    if not name:
        return "", "empty"
    if not min_chars <= len(name) <= max_chars or not all(map(_allowed, name)):
        return "", "invalid"
    norm = name.lower()
    if any(word in norm for word in _SENSITIVE_SUBSTRINGS) or norm.startswith("gm"):
        return name, "sensitive"
    return name, "ok"


def generate_name(prefix: str, suffix_len: int) -> str:
    return prefix + "".join(secrets.choice(_ALPHABET) for _ in range(suffix_len))


@cache
def _tables() -> ConfigTables:
    # shortcut: tables are read once per process; Go re-reads per request to follow hot reloads.
    return ConfigTables()


async def run(ctx, request, details):
    protocol, config = ctx.protocol, ctx.config
    response = protocol.message("loginpb.CreatePlayerResponse")

    def fail(tip_id, *parameters):
        response.error_message.CopyFrom(tip(protocol, tip_id, *parameters))
        return response

    conn = session_fence.connection_of(details) if details is not None and details.session_id else None
    if conn is None:
        return fail(tips.SESSION_ID_NOT_FOUND)
    try:
        account = await login_session.get_account(ctx.login_redis, conn)
    except Exception:
        logger.exception("CreatePlayer 读取登录态失败")
        account = None
    if account is None:
        return fail(tips.SESSION_NOT_FOUND)
    if config.create_role_closed:
        logger.info("CreatePlayer rejected: zone=%d create_role_closed account=%s", config.zone_id, account)
        return fail(tips.ZONE_CREATE_ROLE_CLOSED)
    lock = RedisLock(ctx.login_redis, keys.account_create_lock_key(account), config.account_lock_ttl_ms)
    try:
        locked = await lock.acquire()
    except Exception:
        locked = False
    if not locked:
        return fail(tips.IN_PROGRESS)
    try:
        return await _create_locked(ctx, request, response, fail, account, lock)
    finally:
        try:
            async with asyncio.timeout(1):
                await lock.release()
        except Exception:
            logger.exception("CreatePlayer 锁释放失败 account=%s", account)


async def _check_role_name_content(ctx, requested: str, account: str) -> int:
    """将玩家自定义角色名送内容安全服务核验；通过返回 0，否则返回提示 tip 码（fail-closed）。"""
    if not requested or ctx.content_guard_call is None:
        return 0
    req = ctx.protocol.message(
        "content_guard.CheckTextRequest",
        scene=_TEXT_SCENE_ROLE_NAME,
        text=requested,
        player_id=0,
        account=account,
    )
    try:
        raw = await ctx.content_guard_call(
            _CONTENT_GUARD_PATH, req.SerializeToString(), timeout=_CONTENT_GUARD_TIMEOUT,
        )
        reply = ctx.protocol.parse("content_guard.CheckTextResponse", raw)
    except Exception as error:
        logger.error("CreatePlayer rejected: content guard unavailable account=%s err=%s", account, error)
        return tips.ROLE_NAME_CHECK_UNAVAILABLE
    if int(reply.verdict) == _TEXT_VERDICT_PASS:
        return 0
    logger.info("CreatePlayer rejected: role name refused by content guard verdict=%d account=%s",
                int(reply.verdict), account)
    return tips.ROLE_NAME_SENSITIVE


async def _create_locked(ctx, request, response, fail, account, lock):
    protocol, config = ctx.protocol, ctx.config
    try:
        players, _pi, lifecycle = await ctx.data.read_account_with_lifecycle(account, False)
    except DirectoryError:
        return fail(tips.DATA_SERIALIZE_FAILED)
    if int(lifecycle.deletion_state) != 0:
        return fail(tips.ACCOUNT_DELETION_PENDING)
    tables = _tables()
    class_id = request.class_id
    if class_id == 0:
        rows = tables["Class"].rows()
        class_id = rows[0].id if rows else 0
    elif tables["Class"].get(class_id) is None:
        return fail(tips.UNKNOWN_ERROR)
    gender = request.gender or 1
    if gender > 2:
        return fail(tips.UNKNOWN_ERROR)
    appearance = request.appearance_id
    retired = appearance in _RETIRED_APPEARANCES
    if appearance not in _VALID_APPEARANCES and not retired:
        return fail(tips.UNKNOWN_ERROR)
    rule = tables["RoleNameRule"].get(1)
    if rule is None:
        return fail(tips.DATA_SERIALIZE_FAILED)
    display, verdict = normalize_name(request.name, rule.min_chars, rule.max_chars)
    if retired:
        # Only a lost-response retry of an already committed role may name a retired appearance.
        if verdict == "ok" and any(p.player_id and p.name == display and p.class_id == class_id
                                   and p.gender == gender and p.appearance_id == appearance for p in players):
            return _with_roles(protocol, response, players)
        return fail(tips.UNKNOWN_ERROR)
    if verdict == "sensitive":
        return fail(tips.ROLE_NAME_SENSITIVE)
    if verdict == "invalid":
        return fail(tips.ROLE_NAME_INVALID, str(rule.min_chars), str(rule.max_chars))
    if verdict == "ok":
        content_tip = await _check_role_name_content(ctx, display, account)
        if content_tip:
            return fail(content_tip)
    try:
        player_id = await ctx.data.mint_player_id()
    except Exception:
        logger.exception("发号失败，拒绝建角 account=%s", account)
        return fail(tips.DATA_SERIALIZE_FAILED)

    names = [display] if verdict == "ok" else None
    attempts = 1 if names else rule.max_generate_attempts
    name = ""
    for _ in range(attempts):
        candidate = names[0] if names else generate_name(rule.generated_prefix, rule.generated_suffix_len)
        outcome, owner = await _reserve(ctx, account, player_id, candidate)
        if outcome == RESERVE_OK:
            name = candidate
            break
        if outcome == RESERVE_TAKEN and names:
            owned = next((p for p in players if p.player_id == owner), None) if owner else None
            if (owned is not None and owned.class_id == class_id and owned.gender == gender
                    and owned.appearance_id == appearance):
                return _with_roles(protocol, response, players)  # lost-response retry of a committed role
            return fail(tips.ROLE_NAME_TAKEN)
        if outcome == RESERVE_INVALID and names:
            return fail(tips.ROLE_NAME_INVALID, str(rule.min_chars), str(rule.max_chars))
        if outcome not in (None, RESERVE_TAKEN, RESERVE_INVALID):
            # An unknown result might mean "reserved": release this abandoned id's name once.
            await _release(ctx, account, player_id, candidate, uncertain=False)
        if outcome != RESERVE_TAKEN:
            return fail(tips.DATA_SERIALIZE_FAILED)
    if not name:
        return fail(tips.DATA_SERIALIZE_FAILED)

    target_zone = ctx.resolve_target_zone(config.zone_id)
    try:
        await ctx.data.register_player_zone(player_id, target_zone, config.storage_id_for(player_id))
    except Exception:
        logger.exception("CreatePlayer register_player_zone failed player_id=%s zone=%d", player_id, target_zone)
        await _release(ctx, account, player_id, name, uncertain=False)
        return fail(tips.DATA_SERIALIZE_FAILED)
    role = protocol.message("AccountSimplePlayer", player_id=player_id, class_id=class_id, gender=gender,
                            zone_id=target_zone, name=name, appearance_id=appearance)
    try:
        held = await lock.still_held()
    except Exception:
        held = False
    if not held:
        await _release(ctx, account, player_id, name, uncertain=False)
        return fail(tips.IN_PROGRESS)
    try:
        players = await ctx.data.create_role(account, role)
    except (Full, Conflict, Invalid, NotCommitted) as error:
        await _release(ctx, account, player_id, name, uncertain=False)
        return fail(tips.ACCOUNT_PLAYER_FULL if isinstance(error, Full) else tips.DATA_SERIALIZE_FAILED)
    except Exception as error:
        # The commit may have happened: only an exact read-back of this player id proves success.
        try:
            async with asyncio.timeout(1):
                stored = await ctx.data.lookup_role(player_id)
                if stored.account != account or stored.role != role:
                    raise Conflict("read-back mismatch")
                players = await ctx.data.read_account(account, False)
        except Exception as read_error:
            logger.error("[player-name] orphan reservation player_id=%d name=%r account=%s: write=%s read=%s",
                         player_id, name, account, error, read_error)
            return fail(tips.DATA_SERIALIZE_FAILED)
    logger.info("角色创建成功 account=%s player_id=%d name=%r", account, player_id, name)
    return _with_roles(protocol, response, players)


def _with_roles(protocol, response, players):
    response.players.extend(protocol.message("loginpb.AccountSimplePlayerWrapper", player=p)
                            for p in players if p.appearance_id != _REMOVED_APPEARANCE)
    return response


async def _reserve(ctx, account: str, player_id: int, name: str) -> tuple[int | None, int]:
    """One reservation, retried once when the outcome is unknown; unknown leftovers are released."""
    error = None
    for _ in range(2):
        try:
            return await ctx.data.reserve_name(player_id, name)
        except Exception as caught:
            error = caught
            if status_name(caught) == "FAILED_PRECONDITION":
                # This id already holds another name: an id reuse incident, never release.
                logger.error("[player-name] player_id=%d already holds a different name", player_id)
                return None, 0
    logger.error("[player-name] reserve outcome unknown player_id=%d name=%r: %s", player_id, name, error)
    await _release(ctx, account, player_id, name, uncertain=True)
    return None, 0


async def _release(ctx, account: str, player_id: int, name: str, *, uncertain: bool) -> None:
    async def release():
        try:
            await ctx.data.release_name(player_id, name)
            return True
        except Exception as error:
            logger.error("[player-name] orphan reservation player_id=%d name=%r account=%s: %s",
                         player_id, name, account, error)
            return False

    await release()
    if uncertain:
        # The first release may have raced an in-flight INSERT; release again later.
        async def later():
            await asyncio.sleep(_DELAYED_RELEASE)
            await release()

        ctx.submit(later())
