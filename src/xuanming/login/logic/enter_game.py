"""EnterGame: synchronous admission, then the background enter chain (Go ``entergamelogic.go``)."""

import asyncio
import contextvars
import logging
from dataclasses import dataclass

from .. import gate_commands, keys, login_session, session_fence, tips
from ..cellroute import CellRouteError, check_cell_route_ownership
from ..directory import REMOVED_APPEARANCE, Conflict, DirectoryError, Missing
from ..locker import RedisLock
from ..session_fence import Outcome
from ..sessions import Decision, SessionChanged, decide, mint_login_generation, set_idempotency
from . import tip

logger = logging.getLogger(__name__)
_COMMIT_ATTEMPTS = 3
_COMPENSATION_TIMEOUT = 10
_PROBE_TIMEOUT = 2

# 后台入场链显式绑定的 player_id 日志上下文字段（对齐 Go entergamelogic.go tracing.Detach + player_id）。
ENTER_GAME_PLAYER_ID: contextvars.ContextVar[int] = contextvars.ContextVar("enter_game_player_id", default=0)


def detach_enter_context(player_id: int) -> contextvars.Context:
    """白名单剥离触发请求的会话元数据/鉴权上下文，仅显式附带 player_id 供后台入场链使用。"""
    detached = contextvars.Context()
    detached.run(ENTER_GAME_PLAYER_ID.set, int(player_id))
    return detached


@dataclass(frozen=True, slots=True)
class EnterState:
    player_id: int
    session_id: int
    gate_id: str
    gate_instance_id: str
    incarnation: int
    account: str
    request_id: str
    ticket_target_zone_id: int
    conn: str
    fence_gen: int
    owner_revision: int


async def run(ctx, request, details):
    protocol, config = ctx.protocol, ctx.config
    response = protocol.message("loginpb.EnterGameResponse")

    def fail(tip_id):
        response.error_message.CopyFrom(tip(protocol, tip_id))
        return response

    conn = session_fence.connection_of(details) if details is not None and details.session_id else None
    if conn is None:
        return fail(tips.SESSION_ID_NOT_FOUND)
    if details.ticket_player_id and details.ticket_player_id != request.player_id:
        return fail(tips.ENTER_GAME_GUID)
    try:
        check_cell_route_ownership(ctx.cell_router, config.cell_route, int(request.player_id))
    except CellRouteError:
        return fail(tips.ENTER_GAME_GUID)
    try:
        account = await login_session.get_account(ctx.login_redis, conn)
    except Exception:
        logger.exception("EnterGame 读取登录态失败")
        account = None
    if account is None:
        return fail(tips.SESSION_NOT_FOUND)
    try:
        outcome, fence_gen = await session_fence.check_enter_game(ctx.login_redis, conn, request.player_id)
    except Exception:
        logger.exception("EnterGame 栅栏检查失败 player=%d", request.player_id)
        return fail(tips.REDIS_ERROR)
    if outcome is Outcome.CLOSED:
        return fail(tips.SESSION_NOT_FOUND)
    if outcome is Outcome.OTHER_PLAYER:
        return fail(tips.IN_PROGRESS)

    lock = RedisLock(ctx.player_redis, keys.player_locker_key(request.player_id), config.player_lock_ttl_ms)
    try:
        if not await lock.acquire():
            return fail(tips.IN_PROGRESS)
    except Exception:
        logger.exception("EnterGame 玩家锁失败 player=%d", request.player_id)
        return fail(tips.REDIS_ERROR)
    handed_off = False
    try:
        try:
            owned = await ctx.data.lookup_role(request.player_id)
        except DirectoryError as error:
            return fail(tips.ENTER_GAME_GUID if isinstance(error, Missing) else tips.DATA_SERIALIZE_FAILED)
        if owned.account != account or owned.role.appearance_id == REMOVED_APPEARANCE:
            return fail(tips.ENTER_GAME_GUID)
        if owned.trade_listing_id:
            return fail(tips.IN_PROGRESS)
        if getattr(owned, "account_deletion_state", 0):
            return fail(tips.ACCOUNT_DELETION_PENDING)
        state = EnterState(
            player_id=request.player_id, session_id=details.session_id, gate_id=str(details.gate_node_id),
            gate_instance_id=details.gate_instance_id, incarnation=details.session_incarnation, account=account,
            request_id=request.request_id, ticket_target_zone_id=details.ticket_target_zone_id, conn=conn,
            fence_gen=fence_gen, owner_revision=owned.owner_revision,
        )
        detached = detach_enter_context(state.player_id)

        async def _run_detached() -> None:
            inner = asyncio.create_task(_chain(ctx, state, lock), context=detached)
            try:
                await inner
            except asyncio.CancelledError:
                inner.cancel()
                await asyncio.gather(inner, return_exceptions=True)
                raise

        if not ctx.submit(_run_detached()):
            return fail(tips.IN_PROGRESS)
        handed_off = True
    finally:
        if not handed_off:
            await _release(lock, request.player_id)
    # Accepted, not entered: failures later reach the client as tip 3023 through the gate.
    response.ClearField("error_message")
    response.player_id = request.player_id
    response.owner_revision = owned.owner_revision
    return response


async def _release(lock, player_id: int) -> None:
    try:
        async with asyncio.timeout(3):
            if not await lock.release():
                logger.info("玩家锁已不归本链持有 player=%d", player_id)
    except Exception:
        logger.exception("释放玩家锁失败 player=%d", player_id)


async def _chain(ctx, state: EnterState, lock) -> None:
    bind_sent = [False]
    failed = False
    lost = []
    chain = asyncio.current_task()

    def on_lost():
        lost.append(True)
        chain.cancel()

    heartbeat = asyncio.create_task(lock.heartbeat(on_lost))
    try:
        async with asyncio.timeout(ctx.config.enter_chain_budget):
            await _apply(ctx, state, bind_sent)
    except asyncio.CancelledError:
        if not lost:
            raise
        chain.uncancel()
        logger.error("EnterGame 链中途丢失玩家锁 player=%d", state.player_id)
        failed = True
    except Exception:
        logger.exception("EnterGame 链失败 player=%d", state.player_id)
        failed = True
    finally:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        await _release(lock, state.player_id)
    if failed:
        await _notify_failed(ctx, state, bind_sent[0])


async def _notify_failed(ctx, state: EnterState, bound: bool) -> None:
    """Best effort: tell the client to stop waiting for NotifyEnterScene."""
    if not state.gate_instance_id or state.gate_id in ("", "0"):
        logger.error("EnterGame 失败提示无处可推 player=%d", state.player_id)
        return
    try:
        built = gate_commands.push_tip(ctx.protocol, state.gate_id, state.gate_instance_id, state.session_id,
                                       state.player_id, tips.ENTER_SCENE_FAILED, bound=bound)
        await ctx.send_gate(built, state.player_id)
    except Exception:
        logger.exception("EnterGame 失败提示推送失败 player=%d", state.player_id)


async def _recheck_role(ctx, state: EnterState) -> None:
    owned = await ctx.data.lookup_role(state.player_id)
    if (owned.account != state.account or not state.owner_revision
            or owned.owner_revision != state.owner_revision or owned.trade_listing_id):
        raise Conflict("角色归属已变化")


async def _apply(ctx, state: EnterState, bind_sent: list) -> None:
    protocol = ctx.protocol
    await _recheck_role(ctx, state)
    existing, decision, generation, version = await _commit_session(ctx, state)
    await _bind_fence(ctx, state)
    event = protocol.message(
        "contracts.kafka.BindSessionEvent", session_id=state.session_id, player_id=state.player_id,
        session_version=version, enter_gs_type=decision.enter_gs_type, login_generation=generation,
        authenticated_account=state.account, session_incarnation=state.incarnation,
        owner_revision=state.owner_revision,
    )
    await ctx.send_gate(gate_commands.bind_session(protocol, state.gate_id, state.gate_instance_id, event),
                        state.player_id)
    bind_sent[0] = True
    own_zone = ctx.config.zone_id
    if state.ticket_target_zone_id == own_zone:
        zone = own_zone  # 跨区重定向第二条腿，钉住本区
    elif decision in (Decision.RECONNECT, Decision.REPLACE):
        zone = 0  # scene_manager 跟随玩家当前定位
    elif ctx.config.redirect_on_enter_enabled and (existing is None or not getattr(existing, "scene_id", 0)):
        try:
            home = await ctx.data.get_player_home_zone(state.player_id)
        except Exception:
            logger.error("EnterGame home_zone lookup failed; keeping own zone player=%d zone=%d",
                         state.player_id, own_zone, exc_info=True)
            home = 0
        zone = home if (home and home != own_zone) else own_zone
    else:
        zone = own_zone
    outcome = await session_fence.recheck(ctx.login_redis, state.conn, state.fence_gen)
    if outcome is not Outcome.OK:
        raise RuntimeError(f"session fence {outcome.value} before EnterScene")
    try:
        await _recheck_role(ctx, state)
    except Exception:
        await _compensate(ctx, state, place_lease=False)
        raise
    request = protocol.message(
        "scene_manager.EnterSceneRequest", player_id=state.player_id, scene_id=0, session_id=state.session_id,
        request_id=state.request_id, gate_id=state.gate_id, gate_instance_id=state.gate_instance_id,
        gate_zone_id=own_zone, zone_id=zone, login_generation=generation, owner_revision=state.owner_revision,
    )
    raw = await ctx.scene_manager_call("/scene_manager.SceneManager/EnterScene", request.SerializeToString(),
                                       timeout=ctx.config.scene_manager_timeout)
    reply = protocol.parse("scene_manager.EnterSceneResponse", raw)
    if reply.error_code:
        raise RuntimeError(f"scene_manager rejected EnterScene: code={reply.error_code} msg={reply.error_message}")
    if reply.HasField("redirect"):
        return  # scene_manager already sent the player to the other zone's gate
    try:
        await set_idempotency(ctx.player_redis, state.player_id, state.request_id)
    except Exception:
        logger.exception("写入登录幂等键失败 player=%d", state.player_id)


async def _commit_session(ctx, state: EnterState):
    locator = ctx.locator
    for attempt in range(1, _COMMIT_ATTEMPTS + 1):
        existing = await locator.get(state.player_id)
        decision = decide(existing, state.account)
        generation = await mint_login_generation(ctx.player_redis, state.player_id,
                                                 existing.login_generation if existing is not None else 0)
        try:
            if decision is Decision.RECONNECT:
                updated = await locator.reconnect(
                    player_id=state.player_id, session_id=state.session_id, gate_id=state.gate_id,
                    gate_instance_id=state.gate_instance_id, incarnation=state.incarnation, account=state.account,
                    request_id=state.request_id, login_generation=generation)
                return existing, decision, generation, updated.session_version
            session = locator.online_session(
                existing, player_id=state.player_id, session_id=state.session_id, gate_id=state.gate_id,
                gate_instance_id=state.gate_instance_id, incarnation=state.incarnation, account=state.account,
                request_id=state.request_id, login_generation=generation)
            try:
                await locator.set(session)
            except SessionChanged:
                raise
            except Exception:
                if decision is Decision.REPLACE and await _committed_despite_error(ctx, state, generation):
                    await _kick_replaced(ctx, existing, state, generation)
                    await _compensate(ctx, state, place_lease=True)
                raise
            if decision is Decision.REPLACE:
                await _kick_replaced(ctx, existing, state, generation)
            return existing, decision, generation, session.session_version
        except SessionChanged:
            if attempt == _COMMIT_ATTEMPTS:
                raise
            logger.info("会话记录在读写之间变化，重做提交 player=%d attempt=%d", state.player_id, attempt)


async def _committed_despite_error(ctx, state: EnterState, generation: int) -> bool:
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT):
            record = await ctx.locator.get(state.player_id)
    except Exception:
        return False
    return record is not None and generation and record.session_id == state.session_id \
        and record.login_generation == generation


async def _kick_replaced(ctx, existing, state: EnterState, generation: int) -> None:
    """Kick only after our session commit; never kick our own connection."""
    if existing is None or not existing.gate_id or not existing.gate_instance_id or not state.gate_instance_id:
        return
    if existing.session_id == state.session_id and existing.gate_instance_id == state.gate_instance_id:
        return
    try:
        built = gate_commands.kick_player(ctx.protocol, existing.gate_id, existing.gate_instance_id,
                                          existing.session_id, state.player_id, generation)
        await ctx.send_gate(built, state.player_id)
    except Exception:
        logger.exception("踢旧会话失败 player=%d", state.player_id)


async def _bind_fence(ctx, state: EnterState) -> None:
    try:
        outcome = await session_fence.bind_player(ctx.login_redis, state.conn, state.player_id, state.fence_gen)
    except Exception:
        await _compensate(ctx, state, place_lease=True)
        raise
    if outcome is Outcome.OK:
        return
    await _compensate(ctx, state, place_lease=outcome is Outcome.CLOSED)
    raise RuntimeError(f"session fence {outcome.value} when binding player {state.player_id}")


async def _compensate(ctx, state: EnterState, *, place_lease: bool) -> None:
    """Undo a session written by this chain: disconnect lease (closed) or delete it (left/other player)."""
    async def run():
        if place_lease:
            await disconnecting_with_retry(ctx, state.player_id, state.session_id, state.gate_instance_id,
                                           state.incarnation)
        else:
            try:
                await ctx.locator.delete(state.player_id, state.session_id, state.gate_instance_id)
            except Exception:
                logger.exception("删除会话失败 player=%d", state.player_id)

    try:
        async with asyncio.timeout(_COMPENSATION_TIMEOUT):
            await asyncio.shield(run())
    except Exception:
        logger.exception("补偿超时 player=%d", state.player_id)


async def disconnecting_with_retry(ctx, player_id, session_id, gate_instance_id, incarnation) -> None:
    delay = 0.2
    for attempt in range(1, 5):
        try:
            await ctx.locator.set_disconnecting(player_id, session_id, gate_instance_id, incarnation)
            return
        except Exception:
            logger.exception("SetDisconnecting 第 %d/4 次失败 player=%d", attempt, player_id)
        if attempt < 4:
            await asyncio.sleep(delay)
            delay *= 2
    logger.critical("CRITICAL: session stuck ONLINE player=%d session=%d gate_instance=%r incarnation=%d",
                    player_id, session_id, gate_instance_id, incarnation)
