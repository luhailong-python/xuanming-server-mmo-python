"""Scene 角色改名系统：校验规则、预留改名卡、内容安全与全服目录落定（对齐 C++ player_rename.cpp）。

关键不变量：
- 受理客户端 ``RenamePlayer`` 前先完成可写性、规则、冷却、并发与改名卡校验，随后在同一笔存档中原子写入
  ``item_holds``（kind=2）与 ``profile_component.rename_op``，持久化确认后才启动异步远端流程；
- 异步阶段按序执行可选 ``ContentGuard.CheckText`` 与 ``DataService.RenameRole``（本地无目录模式直接落定）；
  成功时释放预留并扣改名卡、更新 ``name`` / ``renamed_ms`` 并推送 ``NotifyPlayerRenamed``；
  被拒或不可逆失败时仅释放预留、清除 ``rename_op``、落盘并推送失败原因，绝不误扣改名卡；
- 进程重启或玩家重载后调用 ``resume_pending(player)`` 可按既有 ``seq`` 幂等续跑未完结的 ``rename_op``。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import grpc

from ..data import StoreRejected, StoreUnavailable
from ..data.asset_inventory import (
    BAG_INSUFFICIENT,
    FEATURE_UNAVAILABLE,
    INVALID,
    INVALID_DATA,
    UNAVAILABLE,
    bag_capacity,
    clone,
    held_item_count,
    release_item_hold,
)
from ..data_service import names
from ..grpc_runtime import sign_service_call

logger = logging.getLogger(__name__)

_SERVICE = "SceneClientPlayerCommon"
_NOTIFY_PLAYER_RENAMED = "SceneClientPlayerCommonNotifyPlayerRenamed"
DATA_SERVICE_RENAME_PATH = "/data_service.DataService/RenameRole"
CONTENT_GUARD_CHECK_TEXT_PATH = "/content_guard.ContentGuard/CheckText"

# 对应 C++ login_error_tip.pb.h 与 common_error_tip.pb.h 提示码。
ROLE_NAME_INVALID = 2032
ROLE_NAME_TAKEN = 2033
ROLE_NAME_SENSITIVE = 2034
ROLE_NAME_CHECK_UNAVAILABLE = 2050
ROLE_RENAME_COOLDOWN = 2051
ROLE_RENAME_NO_CARD = 2052
ROLE_RENAME_IN_PROGRESS = 2053
ROLE_RENAME_FROZEN = 2054
ROLE_RENAME_SAME_NAME = 2055
ROLE_RENAME_EXPIRED = 2056

# 对应 C++ item_hold.h / player_rename_rules.h 与 content_guard.proto 常量。
ITEM_HOLD_KIND_RENAME = 2
MAX_ITEM_HOLDS = 8
HOLD_REVIEW_AFTER_MS = 10 * 60 * 1000
STRUCTURAL_MAX_RUNES = 32
MAX_DURATION_SECONDS = 365 * 86_400
TEXT_SCENE_ROLE_NAME = 1
TEXT_VERDICT_PASS = 1

# 配表尚未同步改名列时的缺省值（与 C++ generated/tables/rolenamerule.json 保持一致）。
DEFAULT_RENAME_COOLDOWN_SECONDS = 604_800
DEFAULT_OLD_NAME_HOLD_SECONDS = 604_800
DEFAULT_RENAME_ITEM_ID = 3401
DEFAULT_RENAME_ITEM_COUNT = 1

# 对应 proto/data_service/data_service.proto::RenameRoleResult。
RENAME_ROLE_UNSPECIFIED = 0
RENAME_ROLE_OK = 1
RENAME_ROLE_TAKEN = 2
RENAME_ROLE_INVALID = 3
RENAME_ROLE_COOLDOWN = 4
RENAME_ROLE_FROZEN = 5
RENAME_ROLE_STALE_SEQ = 6
RENAME_ROLE_SAME_NAME = 7
RENAME_ROLE_NOT_FOUND = 8
RENAME_ROLE_EXPIRED = 9
RENAME_ROLE_BUSY = 10

_TERMINAL_RESULTS = frozenset({
    RENAME_ROLE_OK,
    RENAME_ROLE_TAKEN,
    RENAME_ROLE_INVALID,
    RENAME_ROLE_COOLDOWN,
    RENAME_ROLE_FROZEN,
    RENAME_ROLE_STALE_SEQ,
    RENAME_ROLE_SAME_NAME,
    RENAME_ROLE_NOT_FOUND,
    RENAME_ROLE_EXPIRED,
})


def is_transient_result(result: int) -> bool:
    """判定 DataService.RenameRole 枚举是否属于未终局结果（对齐 C++ player_rename::IsTransientResult）。

    仅明确列举的终局取值才允许落结局；BUSY、UNSPECIFIED 以及未知新枚举一律视为瞬时结果，
    保留 rename_op 与改名卡预留供后续按同一 seq 重试，绝不误释放改名卡。
    """
    return int(result) not in _TERMINAL_RESULTS


_TIP_BY_RESULT = {
    RENAME_ROLE_TAKEN: ROLE_NAME_TAKEN,
    RENAME_ROLE_INVALID: ROLE_NAME_INVALID,
    RENAME_ROLE_COOLDOWN: ROLE_RENAME_COOLDOWN,
    RENAME_ROLE_FROZEN: ROLE_RENAME_FROZEN,
    RENAME_ROLE_SAME_NAME: ROLE_RENAME_SAME_NAME,
    RENAME_ROLE_EXPIRED: ROLE_RENAME_EXPIRED,
    RENAME_ROLE_STALE_SEQ: UNAVAILABLE,
    RENAME_ROLE_NOT_FOUND: UNAVAILABLE,
}

# 明确未提交的 gRPC 错误码可直接回滚释放预留；其余网络/超时故障保留 rename_op 供幂等续跑。
_DEFINITE_NON_COMMIT_GRPC_CODES = frozenset({
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.FAILED_PRECONDITION,
    grpc.StatusCode.UNIMPLEMENTED,
    grpc.StatusCode.NOT_FOUND,
})


@dataclass(frozen=True, slots=True)
class RenameRule:
    """RoleNameRule 改名规则快照。"""

    min_chars: int
    max_chars: int
    cooldown_seconds: int
    old_name_hold_seconds: int
    rename_item_id: int
    rename_item_count: int


def rule_problem(rule: RenameRule) -> str | None:
    """校验 RoleNameRule 改名参数；合法返回 ``None``。"""
    if not 1 <= rule.min_chars <= rule.max_chars <= STRUCTURAL_MAX_RUNES:
        return "min_chars/max_chars out of range 1..32"
    if not 0 <= rule.cooldown_seconds <= MAX_DURATION_SECONDS:
        return "rename_cooldown_seconds out of range"
    if not 0 <= rule.old_name_hold_seconds <= MAX_DURATION_SECONDS:
        return "old_name_hold_seconds out of range"
    if rule.rename_item_id <= 0:
        return "rename_item_id must be non-zero"
    if rule.rename_item_count <= 0:
        return "rename_item_count must be >= 1"
    return None


def next_allowed_ms(renamed_ms: int, cooldown_seconds: int) -> int:
    """计算最早可再次改名的 Unix 毫秒时刻；无冷却或未改过返回 0。"""
    if not renamed_ms or not cooldown_seconds:
        return 0
    return int(renamed_ms) + int(cooldown_seconds) * 1000


def _grpc_status_code(error: Exception):
    code_fn = getattr(error, "code", None)
    return code_fn() if callable(code_fn) else None


class SceneRename:
    """Scene 角色改名处理器，负责受理校验、改名卡预留、远端落定与断线重启续跑。"""

    def __init__(
        self,
        scene,
        tables,
        assets,
        *,
        data_service_call=None,
        directory_secret: bytes = b"",
        content_guard_call=None,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self._data_service_call = data_service_call
        self._directory_secret = (
            directory_secret.encode("utf-8") if isinstance(directory_secret, str) else bytes(directory_secret)
        )
        self._content_guard_call = content_guard_call
        self.now_ms = now_ms or (lambda: time.time_ns() // 1_000_000)
        tx_enum = self.protocol.pool.FindEnumTypeByName("TransactionType")
        rename_tx = (
            tx_enum.values_by_name.get("TX_ROLE_RENAME")
            or tx_enum.values_by_name.get("TX_RENAME")
            or tx_enum.values_by_name["TX_ITEM_DESTROY"]
        )
        self._rename_tx = rename_tx.number
        self._tasks: dict[int, tuple[int, asyncio.Task]] = {}
        self._local_hold_seq = 0

    def register(self) -> None:
        """向 SceneNode 注册 ``SceneClientPlayerCommon.RenamePlayer`` 写入口。"""
        self.scene.register(_SERVICE, "RenamePlayer", self.rename_player, mutating=True)

    def current_rule(self) -> tuple[RenameRule | None, str | None]:
        """读取当前 ``RoleNameRule`` 首行并校验，缺改名列时回落到标准缺省值。"""
        try:
            table = self.tables["RoleNameRule"]
        except KeyError:
            return None, "RoleNameRule table is missing"
        rows = table.rows()
        if not rows:
            return None, "RoleNameRule table is empty"
        row = rows[0]
        rule = RenameRule(
            min_chars=int(row.min_chars),
            max_chars=int(row.max_chars),
            cooldown_seconds=(
                int(row.rename_cooldown_seconds)
                if hasattr(row, "rename_cooldown_seconds")
                else DEFAULT_RENAME_COOLDOWN_SECONDS
            ),
            old_name_hold_seconds=(
                int(row.old_name_hold_seconds)
                if hasattr(row, "old_name_hold_seconds")
                else DEFAULT_OLD_NAME_HOLD_SECONDS
            ),
            rename_item_id=(
                int(row.rename_item_id)
                if hasattr(row, "rename_item_id")
                else DEFAULT_RENAME_ITEM_ID
            ),
            rename_item_count=(
                int(row.rename_item_count)
                if hasattr(row, "rename_item_count")
                else DEFAULT_RENAME_ITEM_COUNT
            ),
        )
        problem = rule_problem(rule)
        if problem is not None:
            return None, problem
        return rule, None

    def _check_mutability(self, player) -> int:
        if self.scene.mutable(player):
            return 0
        if not player.snapshot.owner_epoch:
            return UNAVAILABLE
        return ROLE_RENAME_FROZEN

    def _reject_sync(self, tip: int):
        response = self.protocol.message("RenamePlayerResponse")
        response.error_message.id = tip
        return response

    def _online_player(self, player_id: int):
        world = self.scene.locations.get(player_id)
        return None if world is None else world.players.get(player_id)

    def _push_notify(self, player, *, name: str = "", next_rename_time: int = 0, tip: int = 0) -> None:
        notify = self.protocol.message(
            "PlayerRenamedNotify",
            name=name,
            next_rename_time=int(next_rename_time),
        )
        if tip:
            notify.error_message.id = tip
        self.scene._push(player, _NOTIFY_PLAYER_RENAMED, notify)

    async def _allocate_hold_id(self, data) -> int:
        existing = {hold.hold_id for hold in data.item_holds.holds}
        if self.assets.allocate_item_id is not None:
            try:
                hold_id = int(await self.assets.allocate_item_id())
            except (OSError, RuntimeError, ValueError):
                return 0
            return hold_id if hold_id > 0 and hold_id not in existing else 0
        self._local_hold_seq += 1
        while self._local_hold_seq in existing:
            self._local_hold_seq += 1
        return self._local_hold_seq

    def _place_rename_hold(self, data, hold_id: int, item_id: int, count: int, now_val: int) -> int:
        holds = data.item_holds.holds
        if (
            not hold_id
            or not item_id
            or count <= 0
            or len(holds) >= MAX_ITEM_HOLDS
            or any(hold.hold_id == hold_id for hold in holds)
        ):
            return INVALID
        if self.assets.removable_count(data, item_id) < count:
            return BAG_INSUFFICIENT
        hold = holds.add(
            hold_id=hold_id,
            kind=ITEM_HOLD_KIND_RENAME,
            created_at_ms=now_val,
            review_after_ms=now_val + HOLD_REVIEW_AFTER_MS,
        )
        hold.lines.add(config_id=item_id, count=count)
        return 0

    def _consume_card(self, data, card_item_id: int, count: int, *, except_hold_id: int) -> int:
        """从人物背包扣除改名卡；Item 配表尚未同步该模板 ID 时按同口径原子扣减。"""
        if not card_item_id or count <= 0:
            return 0
        if self.tables["Item"].get(card_item_id) is not None:
            return self.assets.consume_items(data, {card_item_id: count}, except_hold_id=except_hold_id)
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        if not self.assets._usable(entries, bag_capacity(bag, 0)):
            return INVALID_DATA
        held = held_item_count(data, card_item_id, except_hold_id=except_hold_id)
        known = sum(
            item.size for _, item in entries
            if item.config_id == card_item_id and self.assets.countable(item)
        )
        if known < count or (held and count > max(0, known - held)):
            return BAG_INSUFFICIENT
        taken, version_skew = self.assets._drain(entries, card_item_id, count)
        if taken != count and not version_skew:
            return BAG_INSUFFICIENT
        if version_skew:
            return FEATURE_UNAVAILABLE
        self.assets._drop_empty(bag.items, 0)
        data.bag_component.CopyFrom(bag)
        return 0

    async def rename_player(self, _world, player, _route, request):
        """受理客户端角色改名请求：校验 -> 预留改名卡并落盘 ``rename_op`` -> 派发异步完成任务。"""
        if mut_tip := self._check_mutability(player):
            return self._reject_sync(mut_tip)

        rule, problem = self.current_rule()
        if rule is None:
            logger.error("Player rename refused: invalid RoleNameRule (%s) player_id=%d", problem, player.player_id)
            return self._reject_sync(UNAVAILABLE)

        data = self.scene.rows.assemble(player.snapshot)
        profile = data.profile_component
        active = self._tasks.get(player.player_id)
        if profile.HasField("rename_op") or (active is not None and not active[1].done()):
            return self._reject_sync(ROLE_RENAME_IN_PROGRESS)

        display, norm, verdict = names.normalize(
            request.name, min_runes=rule.min_chars, max_runes=rule.max_chars,
        )
        if verdict in (names.EMPTY, names.INVALID):
            return self._reject_sync(ROLE_NAME_INVALID)
        if verdict == names.SENSITIVE and self._content_guard_call is None and self._data_service_call is None:
            return self._reject_sync(ROLE_NAME_SENSITIVE)

        current_name = profile.name
        if display == current_name or (current_name and norm == current_name.lower()):
            return self._reject_sync(ROLE_RENAME_SAME_NAME)

        now_val = int(self.now_ms())
        allow_at = next_allowed_ms(int(profile.renamed_ms), rule.cooldown_seconds)
        if allow_at and now_val < allow_at:
            return self._reject_sync(ROLE_RENAME_COOLDOWN)

        if self.assets.removable_count(data, rule.rename_item_id) < rule.rename_item_count:
            return self._reject_sync(ROLE_RENAME_NO_CARD)

        hold_id = await self._allocate_hold_id(data)
        if not hold_id:
            logger.error("Player rename refused: hold_id allocation failed player_id=%d", player.player_id)
            return self._reject_sync(UNAVAILABLE)

        # 分配 hold_id 期间可能发生事件循环让出，落盘前再次复核玩家状态与快照。
        if mut_tip := self._check_mutability(player):
            return self._reject_sync(mut_tip)
        data = self.scene.rows.assemble(player.snapshot)
        profile = data.profile_component
        if profile.HasField("rename_op"):
            return self._reject_sync(ROLE_RENAME_IN_PROGRESS)

        hold_err = self._place_rename_hold(data, hold_id, rule.rename_item_id, rule.rename_item_count, now_val)
        if hold_err == BAG_INSUFFICIENT:
            return self._reject_sync(ROLE_RENAME_NO_CARD)
        if hold_err:
            return self._reject_sync(UNAVAILABLE)

        next_seq = int(profile.rename_seq) + 1
        profile.rename_seq = next_seq
        op = profile.rename_op
        op.seq = next_seq
        op.name = display
        op.hold_id = hold_id
        op.card_item_id = rule.rename_item_id
        op.card_count = rule.rename_item_count
        op.started_ms = now_val

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("Player rename stage save failed player_id=%d seq=%d", player.player_id, next_seq, exc_info=True)
            return self._reject_sync(UNAVAILABLE)

        logger.info("Player rename staged player_id=%d seq=%d hold_id=%d", player.player_id, next_seq, hold_id)
        self._launch(player.player_id, next_seq)
        return self.protocol.message("RenamePlayerResponse")

    def _launch(self, player_id: int, seq: int) -> asyncio.Task:
        existing = self._tasks.get(player_id)
        if existing is not None and not existing[1].done():
            return existing[1]
        task = asyncio.create_task(self._complete_rename(player_id, seq), name=f"rename-{player_id}-{seq}")
        self._tasks[player_id] = (seq, task)

        def _cleanup(done_task: asyncio.Task) -> None:
            current = self._tasks.get(player_id)
            if current is not None and current[1] is done_task:
                self._tasks.pop(player_id, None)

        task.add_done_callback(_cleanup)
        return task

    def release_orphan_rename_holds(self, data, player_id: int) -> list[int]:
        """无在途 rename_op 却残留改名卡预留时收集并释放，防止改名卡永久锁死（对齐 C++ ReleaseOrphanRenameHolds）。"""
        orphans = [
            int(hold.hold_id)
            for hold in data.item_holds.holds
            if int(hold.kind) == ITEM_HOLD_KIND_RENAME and int(hold.hold_id)
        ]
        for hold_id in orphans:
            logger.error(
                "metric=player_rename_orphan_hold player_id=%d hold_id=%d "
                "a rename card hold has no pending rename; releasing it",
                player_id, hold_id,
            )
            release_item_hold(data, hold_id)
        return orphans

    async def _release_orphan_holds_and_save(self, player_id: int) -> None:
        async with self.scene._lock(player_id):
            player = self._online_player(player_id)
            if player is None or not self.scene.mutable(player):
                return
            data = self.scene.rows.assemble(player.snapshot)
            if data.profile_component.HasField("rename_op"):
                return
            orphans = self.release_orphan_rename_holds(data, player_id)
            if not orphans:
                return
            try:
                await self.scene.commit_player_data(player, data)
            except (StoreRejected, StoreUnavailable, OSError):
                logger.warning("Orphan rename hold cleanup save failed player_id=%d", player_id, exc_info=True)

    def resume_pending(self, player) -> asyncio.Task | None:
        """重启或角色重载后恢复已持久化的在途 ``rename_op``，按同一 ``seq`` 幂等续跑；无在途改名时清理孤立预留。"""
        data = self.scene.rows.assemble(player.snapshot)
        profile = data.profile_component
        if not profile.HasField("rename_op"):
            has_orphan = any(
                int(hold.kind) == ITEM_HOLD_KIND_RENAME and int(hold.hold_id)
                for hold in data.item_holds.holds
            )
            if has_orphan and self.scene.mutable(player):
                return asyncio.create_task(
                    self._release_orphan_holds_and_save(player.player_id),
                    name=f"rename-orphan-{player.player_id}",
                )
            return None
        seq = int(profile.rename_op.seq)
        if not seq:
            return None
        logger.info("Resuming persisted player rename player_id=%d seq=%d", player.player_id, seq)
        return self._launch(player.player_id, seq)

    def departed(self, player_id: int) -> None:
        """玩家离场时取消内存中未完成的改名协程，不清除已落盘的 ``rename_op``。"""
        entry = self._tasks.pop(player_id, None)
        if entry is not None and not entry[1].done():
            entry[1].cancel()

    async def close(self) -> None:
        """停机时取消并等待全部在途改名任务收尾，保留持久化 ``rename_op`` 供下次重载恢复。"""
        tasks = [task for _, task in self._tasks.values()]
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _complete_rename(self, player_id: int, seq: int) -> None:
        """执行异步内容安全复检与全服名字目录登记，再加锁原子提交结果。"""
        async with self.scene._lock(player_id):
            player = self._online_player(player_id)
            if player is None:
                return
            if not self.scene.mutable(player):
                logger.info(
                    "Player rename send deferred while player is not mutable player_id=%d seq=%d",
                    player_id, seq,
                )
                return
            data = self.scene.rows.assemble(player.snapshot)
            profile = data.profile_component
            if not profile.HasField("rename_op") or int(profile.rename_op.seq) != seq:
                return
            candidate_name = profile.rename_op.name

        rule, problem = self.current_rule()
        if rule is None:
            logger.error("Player rename completion aborted: invalid RoleNameRule (%s) player_id=%d", problem, player_id)
            await self._abort_rename(player_id, seq, UNAVAILABLE)
            return

        # 1. 可选内容安全检测（fail-closed，不向日志打印玩家正文）。
        if self._content_guard_call is not None:
            guard_req = self.protocol.message(
                "content_guard.CheckTextRequest",
                scene=TEXT_SCENE_ROLE_NAME,
                text=candidate_name,
                player_id=player_id,
            )
            try:
                raw = await self._content_guard_call(CONTENT_GUARD_CHECK_TEXT_PATH, guard_req.SerializeToString())
                guard_resp = self.protocol.parse("content_guard.CheckTextResponse", raw)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("ContentGuard check failed for player rename player_id=%d seq=%d", player_id, seq, exc_info=True)
                await self._abort_rename(player_id, seq, ROLE_NAME_CHECK_UNAVAILABLE)
                return
            if int(guard_resp.verdict) != TEXT_VERDICT_PASS:
                await self._abort_rename(player_id, seq, ROLE_NAME_SENSITIVE)
                return

        # 2. 全服账号目录登记（或本地无目录模式直接落定）。
        if self._data_service_call is None:
            display, _norm, verdict = names.normalize(
                candidate_name, min_runes=rule.min_chars, max_runes=rule.max_chars,
            )
            if verdict == names.SENSITIVE:
                await self._abort_rename(player_id, seq, ROLE_NAME_SENSITIVE)
                return
            if verdict != names.OK:
                await self._abort_rename(player_id, seq, ROLE_NAME_INVALID)
                return
            await self._commit_success(player_id, seq, display, int(self.now_ms()), rule.cooldown_seconds)
            return

        req = self.protocol.message(
            "data_service.RenameRoleRequest",
            player_id=player_id,
            seq=seq,
            name=candidate_name,
            cooldown_seconds=rule.cooldown_seconds,
            hold_seconds=rule.old_name_hold_seconds,
            min_chars=rule.min_chars,
            max_chars=rule.max_chars,
        )
        if self._directory_secret:
            sign_service_call(DATA_SERVICE_RENAME_PATH, req, "scene", int(self.now_ms()), self._directory_secret)

        try:
            raw = await self._data_service_call(DATA_SERVICE_RENAME_PATH, req.SerializeToString(deterministic=True))
            resp = self.protocol.parse("data_service.RenameRoleResponse", raw)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            code = _grpc_status_code(error)
            if code in _DEFINITE_NON_COMMIT_GRPC_CODES:
                logger.warning(
                    "DataService RenameRole rejected with non-commit gRPC code=%s player_id=%d seq=%d",
                    code, player_id, seq,
                )
                await self._abort_rename(player_id, seq, UNAVAILABLE)
                return
            logger.warning(
                "DataService RenameRole transient failure, keeping rename_op for replay player_id=%d seq=%d",
                player_id, seq, exc_info=True,
            )
            return

        result = int(resp.result)
        if is_transient_result(result):
            logger.warning(
                "DataService RenameRole returned transient or unknown result=%d, keeping rename_op player_id=%d seq=%d",
                result, player_id, seq,
            )
            return

        if result == RENAME_ROLE_OK:
            final_name = resp.name or candidate_name
            renamed_ms = int(resp.renamed_ms) or int(self.now_ms())
            await self._commit_success(player_id, seq, final_name, renamed_ms, rule.cooldown_seconds)
            return

        tip = _TIP_BY_RESULT.get(result, UNAVAILABLE)
        sync_seq = int(resp.current_seq) if result == RENAME_ROLE_STALE_SEQ else 0
        next_sec = int(resp.next_allowed_ms) // 1000 if result == RENAME_ROLE_COOLDOWN else 0
        await self._abort_rename(player_id, seq, tip, next_rename_time=next_sec, sync_seq=sync_seq)

    async def _commit_success(
        self,
        player_id: int,
        seq: int,
        final_name: str,
        renamed_ms: int,
        cooldown_seconds: int,
    ) -> None:
        async with self.scene._lock(player_id):
            player = self._online_player(player_id)
            if player is None:
                return
            if not self.scene.mutable(player):
                logger.info(
                    "RenameRole answered while player is not mutable, outcome kept for retry player_id=%d seq=%d",
                    player_id, seq,
                )
                return
            try:
                await self.scene.flush_player_rows(player)
            except (StoreRejected, StoreUnavailable, OSError):
                logger.warning("Player rename flush failed before commit player_id=%d seq=%d", player_id, seq, exc_info=True)
                return

            data = self.scene.rows.assemble(player.snapshot)
            profile = data.profile_component
            if not profile.HasField("rename_op") or int(profile.rename_op.seq) != seq:
                return

            hold_id = int(profile.rename_op.hold_id)
            card_item_id = int(profile.rename_op.card_item_id)
            card_count = int(profile.rename_op.card_count)

            before = clone(data) if self.assets.audit_enabled else None
            if card_item_id and card_count:
                consume_err = self._consume_card(data, card_item_id, card_count, except_hold_id=hold_id)
                if consume_err:
                    # 角色在 data_service 已改名但改名卡未扣成：保留 rename_op 与预留供重发同一 seq 重试，绝不白改。
                    logger.error(
                        "metric=player_rename_card_not_consumed player_id=%d seq=%d hold_id=%d err=%d "
                        "role already renamed in data_service but rename card could not be consumed; keeping pending",
                        player_id, seq, hold_id, consume_err,
                    )
                    return
            release_item_hold(data, hold_id)

            profile.name = final_name
            profile.renamed_ms = renamed_ms
            profile.ClearField("rename_op")

            audit = (
                self.assets.make_audit_callback(player, before, data, self._rename_tx, correlation=seq)
                if before is not None
                else None
            )
            try:
                await self.scene.commit_player_data(player, data, on_committed=audit)
            except (StoreRejected, StoreUnavailable, OSError):
                logger.warning("Player rename final save failed player_id=%d seq=%d", player_id, seq, exc_info=True)
                return

            next_sec = next_allowed_ms(renamed_ms, cooldown_seconds) // 1000
            self._push_notify(player, name=final_name, next_rename_time=next_sec)
            logger.info("Player rename committed player_id=%d seq=%d", player_id, seq)

    async def _abort_rename(
        self,
        player_id: int,
        seq: int,
        tip: int,
        *,
        next_rename_time: int = 0,
        sync_seq: int = 0,
    ) -> None:
        async with self.scene._lock(player_id):
            player = self._online_player(player_id)
            if player is None:
                return
            if not self.scene.mutable(player):
                logger.info(
                    "RenameRole abort deferred while player is not mutable player_id=%d seq=%d",
                    player_id, seq,
                )
                return
            try:
                await self.scene.flush_player_rows(player)
            except (StoreRejected, StoreUnavailable, OSError):
                logger.warning("Player rename flush failed before abort player_id=%d seq=%d", player_id, seq, exc_info=True)
                return

            data = self.scene.rows.assemble(player.snapshot)
            profile = data.profile_component
            if not profile.HasField("rename_op") or int(profile.rename_op.seq) != seq:
                return

            hold_id = int(profile.rename_op.hold_id)
            if sync_seq > int(profile.rename_seq):
                profile.rename_seq = sync_seq
            release_item_hold(data, hold_id)
            profile.ClearField("rename_op")

            try:
                await self.scene.commit_player_data(player, data)
            except (StoreRejected, StoreUnavailable, OSError):
                logger.warning("Player rename abort save failed player_id=%d seq=%d", player_id, seq, exc_info=True)
                return

            self._push_notify(player, tip=tip, next_rename_time=next_rename_time)
            logger.info("Player rename rejected player_id=%d seq=%d tip=%d", player_id, seq, tip)
