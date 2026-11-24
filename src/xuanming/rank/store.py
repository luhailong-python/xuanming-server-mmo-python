"""rank 存储抽象接口、共享校验/合并规则与内存双胞胎实现（MemoryRankStore）。"""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Awaitable, Callable
from typing import Protocol as TypingProtocol, runtime_checkable

from google.protobuf.message import Message

from ..grpc_runtime import service_digest
from ..protocol import Protocol
from .errors import (
    EntriesDriftError,
    RankClosedError,
    RankConflictError,
    RankCursorError,
    RankDeniedError,
    RankInvalidError,
    RankNotFoundError,
)
from .model import (
    BoardRow,
    DEFAULT_AROUND_RADIUS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_TOP_N,
    INT64_MAX,
    INT64_MIN,
    KNOWN_CURRENCY_TYPES,
    MAX_AROUND_RADIUS,
    MAX_BATCH_ENTRIES,
    MAX_PAGE_SIZE,
    MAX_REWARD_MAIL_WINDOW_MS,
    MAX_REWARD_TIERS,
    MAX_SOURCE_SHARDS,
    MAX_TOP_N,
    RANK_EXACT_LIMIT,
    RANK_GEN_OPEN,
    RANK_GEN_SEALED,
    REWARD_ABANDONED,
    REWARD_DELIVERED,
    REWARD_PENDING,
    RewardRow,
    SUBMIT_INCREMENT,
    SUBMIT_SET_IF_HIGHER,
    ScoreRow,
    ScoreWrite,
    ShardRow,
    UINT64_MAX,
    apply_rank_or_beyond,
    checked_increment_int64,
    order_key,
    page_window_for,
    position_around,
    shard_to_decrement,
    valid_board_key,
    valid_lookup_board_key,
    validate_attachments,
    validate_reward_mail_window_ms,
)

IdGenerator = Callable[[], Awaitable[int]]
ItemExistsFn = Callable[[int], bool]


def default_monotonic_id_generator(start: int = 1) -> IdGenerator:
    """进程内单调递增发号器（用于单测或 MemoryRankStore）。"""
    current = start - 1

    async def _next() -> int:
        nonlocal current
        current += 1
        return current

    return _next


@runtime_checkable
class RewardStore(TypingProtocol):
    """奖励投递工作器对 ``rank_mail_reward`` 表的全部依赖接口。"""

    async def due_rewards(self, now_ms: int, limit: int) -> list[int]: ...

    async def claim_reward(self, reward_id: int, now_ms: int, until_ms: int, token: int) -> RewardRow | None: ...

    async def settle_reward(self, reward: RewardRow, mail_id: int, next_ms: int, now_ms: int) -> None: ...

    async def abandon_reward(self, reward: RewardRow, now_ms: int) -> None: ...

    async def defer_reward(self, reward: RewardRow, next_ms: int, now_ms: int) -> None: ...


@runtime_checkable
class RankStore(RewardStore, TypingProtocol):
    """排行榜服务存储接口（SqlRankStore 与 MemoryRankStore 共同实现）。"""

    rank_limit: int

    def configure_reward_mail_window(self, window_ms: int) -> None: ...

    async def open_generation(self, caller: str, request: Message) -> Message: ...

    async def submit_scores(self, caller: str, request: Message) -> Message: ...

    async def freeze_generation(self, caller: str, request: Message) -> Message: ...

    async def remove_entry(self, caller: str, request: Message, *, hide: bool = False) -> None: ...

    async def get_page(self, player_id: int, request: Message) -> Message: ...

    async def get_around(self, player_id: int, request: Message) -> Message: ...


# -- 共享请求预检与纯内存合并逻辑（SqlRankStore 与 MemoryRankStore 复用） -------------------------------


def validate_open_request(caller: str, request: Message | None) -> str:
    """校验 OpenRankGeneration 请求并返回请求载荷摘要。"""
    if (
        request is None
        or not request.HasField("board")
        or not valid_board_key(request.board)
        or caller != request.producer
        or request.source_epoch == 0
        or request.request_id == 0
    ):
        raise RankInvalidError("invalid OpenRankGeneration request")
    return service_digest(request)


def validate_submit_request(request: Message | None) -> tuple[list[Message], str]:
    """校验 SubmitRankScores 请求，返回按 ``entity_id`` 升序去重检验后的条目列表及载荷摘要。"""
    if (
        request is None
        or request.board_id == 0
        or request.generation == 0
        or request.source_epoch == 0
        or request.source_cursor == 0
        or request.request_id == 0
        or request.source_shard >= MAX_SOURCE_SHARDS
        or not (1 <= len(request.entries) <= MAX_BATCH_ENTRIES)
        or not (SUBMIT_SET_IF_HIGHER <= request.mode <= SUBMIT_INCREMENT)
    ):
        raise RankInvalidError("invalid SubmitRankScores request")

    entries = list(request.entries)
    for entry in entries:
        if (
            entry.entity_id == 0
            or entry.achieved_ms == 0
            or entry.score < INT64_MIN
            or entry.score > INT64_MAX
        ):
            raise RankInvalidError("invalid RankScoreInput entry")
    entries.sort(key=lambda item: int(item.entity_id))
    for idx in range(1, len(entries)):
        if entries[idx - 1].entity_id == entries[idx].entity_id:
            raise RankInvalidError(f"duplicate entity_id in batch: {entries[idx].entity_id}")
    return entries, service_digest(request)


def merge_score_batch(
    *,
    mode: int,
    ascending: int,
    entries: list[Message],
    existing: dict[int, ScoreRow],
    removed: set[int],
) -> tuple[list[ScoreWrite], int]:
    """按 Go ``Repo.Submit`` 规则合并整批分数，返回待写回列表与本批新上榜实体数。"""
    writes: list[ScoreWrite] = []
    inserted = 0
    for entry in entries:
        entity_id = int(entry.entity_id)
        if entity_id in removed:
            continue
        old = existing.get(entity_id)
        found = old is not None
        score_version = int(entry.score_version)
        if found and score_version != 0 and score_version <= old.score_version:
            continue

        next_score = int(entry.score)
        if mode == SUBMIT_INCREMENT:
            base_score = old.score if found else 0
            next_score = checked_increment_int64(base_score, next_score)
        if mode == SUBMIT_SET_IF_HIGHER and found:
            if (ascending == 0 and next_score <= old.score) or (ascending != 0 and next_score >= old.score):
                continue

        achieved_ms = old.achieved_ms if found else 0
        if not found or next_score != old.score or score_version != 0:
            achieved_ms = int(entry.achieved_ms)
        writes.append(
            ScoreWrite(
                entity_id=entity_id,
                score=next_score,
                achieved_ms=achieved_ms,
                score_version=score_version,
                existed=found,
            )
        )
        if not found:
            inserted += 1
    return writes, inserted


def validate_freeze_request(
    request: Message | None,
    item_exists: ItemExistsFn | None,
    *,
    known_currencies: frozenset[int] = KNOWN_CURRENCY_TYPES,
) -> tuple[int, str]:
    """校验 FreezeRankGeneration 请求与全部奖励档位，返回 ``(top_n, digest)``。"""
    if (
        request is None
        or request.board_id == 0
        or request.generation == 0
        or request.source_epoch == 0
        or request.request_id == 0
        or len(request.rewards) > MAX_REWARD_TIERS
    ):
        raise RankInvalidError("invalid FreezeRankGeneration request")
    top_n = int(request.top_n) if request.top_n else DEFAULT_TOP_N
    if top_n > MAX_TOP_N:
        raise RankInvalidError(f"top_n exceeds {MAX_TOP_N}")

    tiers = list(request.rewards)
    for tier in tiers:
        if (
            tier.first_rank == 0
            or tier.last_rank < tier.first_rank
            or tier.last_rank > MAX_TOP_N
            or tier.last_rank > top_n
            or not tier.HasField("attachments")
        ):
            raise RankInvalidError("invalid reward tier range or missing attachments")
        validate_attachments(tier.attachments, item_exists, known_currencies=known_currencies)

    for i, left in enumerate(tiers):
        for j in range(i + 1, len(tiers)):
            right = tiers[j]
            if left.first_rank <= right.last_rank and right.first_rank <= left.last_rank:
                raise RankInvalidError("overlapping reward tiers")

    return top_n, service_digest(request)


def validate_remove_request(request: Message | None, *, hide: bool) -> str:
    """校验 RemoveRankEntry / DeleteRankGeneration 请求并返回带操作前缀的摘要。"""
    if (
        request is None
        or request.board_id == 0
        or request.generation == 0
        or request.request_id == 0
        or (hide and request.entity_id != 0)
        or (not hide and request.entity_id == 0)
    ):
        raise RankInvalidError("invalid RemoveRankEntry/DeleteRankGeneration request")
    prefix = "hide:" if hide else "remove:"
    return prefix + service_digest(request)


def normalize_page_request(player_id: int, request: Message | None) -> tuple[int, int]:
    """校验并钳制 GetRankPage 的 ``(page, page_size)``。"""
    if request is None or player_id == 0 or not request.HasField("board") or not valid_lookup_board_key(request.board):
        raise RankInvalidError("invalid GetRankPage request")
    size = int(request.page_size) if request.page_size else DEFAULT_PAGE_SIZE
    size = min(size, MAX_PAGE_SIZE)
    page = int(request.page) if request.page else 1
    return page, size


def normalize_around_request(player_id: int, request: Message | None) -> int:
    """校验并钳制 GetRankAround 的 ``radius``。"""
    if request is None or player_id == 0 or not request.HasField("board") or not valid_lookup_board_key(request.board):
        raise RankInvalidError("invalid GetRankAround request")
    radius = int(request.radius) if request.radius else DEFAULT_AROUND_RADIUS
    return min(radius, MAX_AROUND_RADIUS)


def fill_board_key(target: Message, board: BoardRow) -> None:
    """将 ``BoardRow`` 的逻辑榜键写入 proto ``RankBoardKey``。"""
    target.board_type = board.board_type
    target.scope = board.scope
    target.scope_id = board.scope_id
    target.period = board.period
    target.generation = board.generation


def known_currencies_from_protocol(protocol: Protocol) -> frozenset[int]:
    """从编译期 ``schema.pb`` 的 ``common.base.CurrencyType`` 读取权威币种取值表。"""
    try:
        enum_desc = protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        return frozenset(int(num) for num in enum_desc.values_by_number)
    except KeyError:
        return KNOWN_CURRENCY_TYPES


# -- MemoryRankStore（确定性内存双胞胎，完整复刻 SQL 事务语义） ----------------------------------------


class MemoryRankStore:
    """单进程确定性内存存储，与 ``SqlRankStore`` 保持 1:1 业务语义对齐。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        next_board_id: IdGenerator | None = None,
        next_settlement_id: IdGenerator | None = None,
        next_reward_id: IdGenerator | None = None,
        item_exists: ItemExistsFn | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        rank_limit: int = RANK_EXACT_LIMIT,
    ) -> None:
        self.protocol = protocol
        self._next_board_id = next_board_id or default_monotonic_id_generator(1)
        self._next_settlement_id = next_settlement_id or default_monotonic_id_generator(1)
        self._next_reward_id = next_reward_id or default_monotonic_id_generator(1)
        self._item_exists = item_exists
        self._clock_ms = clock_ms
        self._known_currencies = known_currencies_from_protocol(protocol)
        self._reward_mail_window_ms = MAX_REWARD_MAIL_WINDOW_MS
        self.rank_limit = rank_limit
        self._lock = asyncio.Lock()

        # 9 张表的内存镜像
        self.boards: dict[int, BoardRow] = {}
        self._board_by_unique: dict[tuple[int, int, int, str, int], int] = {}
        self.shards: dict[tuple[int, int, int], ShardRow] = {}
        # (board_id, generation) -> {entity_id: (score, order_key, achieved_ms, source_cursor, score_version)}
        self.scores: dict[tuple[int, int], dict[int, tuple[int, int, int, int, int]]] = {}
        # (board_id, generation, source_shard, source_cursor) -> (request_id, request_hash, committed_ms)
        self.receipts: dict[tuple[int, int, int, int], tuple[int, str, int]] = {}
        # settlement_id -> dict
        self.settlements: dict[int, dict] = {}
        # (settlement_id, rank) -> (entity_id, score, achieved_ms)
        self.snapshots: dict[tuple[int, int], tuple[int, int, int]] = {}
        # reward_id -> RewardRow
        self.rewards: dict[int, RewardRow] = {}
        # (board_id, generation, request_id) -> (request_hash, created_ms)
        self.mutations: dict[tuple[int, int, int], tuple[str, int]] = {}
        # (board_id, generation) -> {entity_id: removed_ms}
        self.removed_entries: dict[tuple[int, int], dict[int, int]] = {}

    def configure_reward_mail_window(self, window_ms: int) -> None:
        self._reward_mail_window_ms = validate_reward_mail_window_ms(window_ms)

    async def open_generation(self, caller: str, request: Message) -> Message:
        digest = validate_open_request(caller, request)
        board_id = await self._next_board_id()
        key = request.board
        unique_key = (int(key.board_type), int(key.scope), int(key.scope_id), key.period, int(key.generation))
        async with self._lock:
            existing_id = self._board_by_unique.get(unique_key)
            if existing_id is not None:
                existing = self.boards[existing_id]
                if existing.open_request_hash != digest or existing.deleted_ms != 0:
                    raise RankConflictError("open rank generation conflict")
                return self.protocol.message(
                    "rankpb.OpenRankGenerationResponse",
                    board_id=existing.board_id,
                    duplicated=True,
                )
            now = self._clock_ms()
            row = BoardRow(
                board_id=board_id,
                board_type=int(key.board_type),
                scope=int(key.scope),
                scope_id=int(key.scope_id),
                period=key.period,
                generation=int(key.generation),
                producer=caller,
                source_epoch=int(request.source_epoch),
                ascending=1 if request.ascending else 0,
                state=RANK_GEN_OPEN,
                settlement_id=0,
                open_request_id=int(request.request_id),
                open_request_hash=digest,
                created_ms=now,
                sealed_ms=0,
                deleted_ms=0,
            )
            self.boards[board_id] = row
            self._board_by_unique[unique_key] = board_id
            return self.protocol.message("rankpb.OpenRankGenerationResponse", board_id=board_id)

    async def submit_scores(self, caller: str, request: Message) -> Message:
        entries, digest = validate_submit_request(request)
        board_id = int(request.board_id)
        generation = int(request.generation)
        source_shard = int(request.source_shard)
        source_cursor = int(request.source_cursor)

        async with self._lock:
            board = self.boards.get(board_id)
            if board is None:
                raise RankNotFoundError("rank board not found")
            if board.producer != caller:
                raise RankDeniedError("caller is not board producer")
            if (
                board.generation != generation
                or board.source_epoch != int(request.source_epoch)
                or board.deleted_ms != 0
            ):
                raise RankConflictError("board generation/epoch/deleted conflict")

            shard_key = (board_id, generation, source_shard)
            shard = self.shards.get(shard_key)
            committed = shard.committed_cursor if shard is not None else 0
            if source_cursor <= committed:
                receipt = self.receipts.get((board_id, generation, source_shard, source_cursor))
                if receipt is None or receipt[1] != digest:
                    raise RankConflictError("submit replay hash mismatch")
                return self.protocol.message(
                    "rankpb.SubmitRankScoresResponse",
                    committed_cursor=committed,
                    duplicated=True,
                )
            if board.state != RANK_GEN_OPEN:
                raise RankClosedError("rank generation is closed")
            if committed == UINT64_MAX or source_cursor != committed + 1:
                raise RankCursorError("source cursor gap")

            bg = (board_id, generation)
            removed_map = self.removed_entries.get(bg, {})
            removed_ids = set(removed_map.keys())
            score_map = self.scores.setdefault(bg, {})
            existing: dict[int, ScoreRow] = {}
            for entry in entries:
                eid = int(entry.entity_id)
                if eid in removed_ids:
                    continue
                cur = score_map.get(eid)
                if cur is not None:
                    existing[eid] = ScoreRow(score=cur[0], achieved_ms=cur[2], score_version=cur[4])

            writes, inserted = merge_score_batch(
                mode=int(request.mode),
                ascending=board.ascending,
                entries=entries,
                existing=existing,
                removed=removed_ids,
            )

            for w in writes:
                score_map[w.entity_id] = (
                    w.score,
                    order_key(board.ascending, w.score),
                    w.achieved_ms,
                    source_cursor,
                    w.score_version,
                )

            self.receipts[(board_id, generation, source_shard, source_cursor)] = (
                int(request.request_id),
                digest,
                self._clock_ms(),
            )
            if shard is None:
                self.shards[shard_key] = ShardRow(
                    source_shard=source_shard,
                    committed_cursor=source_cursor,
                    entries=inserted,
                )
            else:
                shard.committed_cursor = source_cursor
                shard.entries += inserted

            return self.protocol.message(
                "rankpb.SubmitRankScoresResponse",
                committed_cursor=source_cursor,
                duplicated=False,
            )

    async def freeze_generation(self, caller: str, request: Message) -> Message:
        top_n, digest = validate_freeze_request(
            request,
            self._item_exists,
            known_currencies=self._known_currencies,
        )
        settlement_id = await self._next_settlement_id()
        reward_ids = [await self._next_reward_id() for _ in range(top_n)] if request.rewards else []
        board_id = int(request.board_id)
        generation = int(request.generation)

        async with self._lock:
            board = self.boards.get(board_id)
            if board is None:
                raise RankNotFoundError("rank board not found")
            if board.producer != caller:
                raise RankDeniedError("caller is not board producer")
            if (
                board.generation != generation
                or board.source_epoch != int(request.source_epoch)
                or board.deleted_ms != 0
            ):
                raise RankConflictError("board generation/epoch/deleted conflict")
            if board.settlement_id != 0:
                existing_settlement = self.settlements[board.settlement_id]
                if existing_settlement["request_hash"] != digest:
                    raise RankConflictError("freeze replay hash mismatch")
                return self.protocol.message(
                    "rankpb.FreezeRankGenerationResponse",
                    settlement_id=board.settlement_id,
                    winner_count=existing_settlement["winner_count"],
                    duplicated=True,
                )
            if board.state != RANK_GEN_OPEN:
                raise RankClosedError("rank generation is closed")

            shards = self._sorted_shards(board_id, generation)
            final_cursor = sum(s.committed_cursor for s in shards)
            if final_cursor != int(request.expected_source_cursor):
                raise RankCursorError("committed cursor total does not match expected_source_cursor")

            ordered = self._ordered_scores(board_id, generation)[:top_n]
            now = self._clock_ms()
            self.settlements[settlement_id] = {
                "settlement_id": settlement_id,
                "board_id": board_id,
                "generation": generation,
                "source_epoch": board.source_epoch,
                "final_cursor": final_cursor,
                "request_id": int(request.request_id),
                "request_hash": digest,
                "winner_count": len(ordered),
                "sealed_ms": now,
            }
            for idx, (entity_id, score, _okey, achieved_ms) in enumerate(ordered):
                rank_num = idx + 1
                self.snapshots[(settlement_id, rank_num)] = (entity_id, score, achieved_ms)
                for tier in request.rewards:
                    if int(tier.first_rank) <= rank_num <= int(tier.last_rank):
                        payload = tier.attachments.SerializeToString(deterministic=True)
                        expire_ms = now + self._reward_mail_window_ms
                        rid = reward_ids[idx]
                        self.rewards[rid] = RewardRow(
                            reward_id=rid,
                            settlement_id=settlement_id,
                            player_id=entity_id,
                            rank=rank_num,
                            attachments=payload,
                            status=REWARD_PENDING,
                            mail_id=0,
                            next_attempt_ms=now,
                            lease_until_ms=0,
                            lease_token=0,
                            attempts=0,
                            created_ms=now,
                            updated_ms=now,
                            expire_ms=expire_ms,
                        )
            board.state = RANK_GEN_SEALED
            board.settlement_id = settlement_id
            board.sealed_ms = now
            return self.protocol.message(
                "rankpb.FreezeRankGenerationResponse",
                settlement_id=settlement_id,
                winner_count=len(ordered),
                duplicated=False,
            )

    async def remove_entry(self, caller: str, request: Message, *, hide: bool = False) -> None:
        digest = validate_remove_request(request, hide=hide)
        board_id = int(request.board_id)
        generation = int(request.generation)
        request_id = int(request.request_id)

        async with self._lock:
            board = self.boards.get(board_id)
            if board is None:
                raise RankNotFoundError("rank board not found")
            if board.producer != caller:
                raise RankDeniedError("caller is not board producer")
            if board.generation != generation:
                raise RankConflictError("board generation mismatch")

            mut_key = (board_id, generation, request_id)
            existing = self.mutations.get(mut_key)
            if existing is not None:
                if existing[0] != digest:
                    raise RankConflictError("mutation request_id hash conflict")
                return

            if not hide and (board.state != RANK_GEN_OPEN or board.deleted_ms != 0):
                raise RankClosedError("rank generation is closed")

            shards = self._sorted_shards(board_id, generation)
            now = self._clock_ms()
            if hide:
                board.deleted_ms = now
            else:
                entity_id = int(request.entity_id)
                bg = (board_id, generation)
                self.removed_entries.setdefault(bg, {}).setdefault(entity_id, now)
                score_map = self.scores.get(bg)
                if score_map is not None and entity_id in score_map:
                    dec_shard = shard_to_decrement(shards)
                    if dec_shard is None:
                        raise EntriesDriftError("rank entry count drift on remove")
                    del score_map[entity_id]
                    self.shards[(board_id, generation, dec_shard)].entries -= 1

            self.mutations[mut_key] = (digest, now)

    async def get_page(self, player_id: int, request: Message) -> Message:
        page, size = normalize_page_request(player_id, request)
        async with self._lock:
            board = self._lookup_board(request.board)
            total = sum(s.entries for s in self._sorted_shards(board.board_id, board.generation))
            window = page_window_for(total, page, size, self.rank_limit)
            resp = self.protocol.message(
                "rankpb.GetRankPageResponse",
                state=board.state,
                total_count=total,
                page=window.page,
                page_size=size,
                page_count=window.pages,
                rank_limit=self.rank_limit,
            )
            fill_board_key(resp.board, board)

            ordered = self._ordered_scores(board.board_id, board.generation)
            page_rows = ordered[window.offset : window.offset + window.rows]
            self_found = False
            for idx, (entity_id, score, _okey, achieved_ms) in enumerate(page_rows):
                entry = resp.entries.add()
                entry.rank = window.offset + idx + 1
                entry.entity_id = entity_id
                entry.score = score
                entry.achieved_ms = achieved_ms
                entry.beyond_rank_limit = False
                if entity_id == player_id:
                    resp.self.CopyFrom(entry)
                    self_found = True

            if not self_found:
                score_map = self.scores.get((board.board_id, board.generation), {})
                mine = score_map.get(player_id)
                if mine is not None:
                    my_score, my_okey, my_achieved, _, _ = mine
                    ahead = self._count_ahead(ordered, (my_okey, my_achieved, player_id), self.rank_limit)
                    capped = ahead >= self.rank_limit
                    resp.self.entity_id = player_id
                    resp.self.score = my_score
                    resp.self.achieved_ms = my_achieved
                    apply_rank_or_beyond(resp.self, position_around(ahead, capped, 0), self.rank_limit)

            return resp

    async def get_around(self, player_id: int, request: Message) -> Message:
        radius = normalize_around_request(player_id, request)
        async with self._lock:
            board = self._lookup_board(request.board)
            resp = self.protocol.message("rankpb.GetRankAroundResponse", rank_limit=self.rank_limit)
            fill_board_key(resp.board, board)

            score_map = self.scores.get((board.board_id, board.generation), {})
            mine = score_map.get(player_id)
            if mine is None:
                return resp

            ordered = self._ordered_scores(board.board_id, board.generation)
            my_score, my_okey, my_achieved, _, _ = mine
            my_tuple = (my_okey, my_achieved, player_id)
            count_cap = self.rank_limit + radius
            ahead = self._count_ahead(ordered, my_tuple, count_cap)
            capped = ahead >= count_cap

            # 找出精确下标
            my_idx = next(i for i, row in enumerate(ordered) if row[0] == player_id)
            before = ordered[max(0, my_idx - radius) : my_idx]
            after = ordered[my_idx + 1 : my_idx + 1 + radius]

            for idx, (entity_id, score, _okey, achieved_ms) in enumerate(before):
                delta = idx - len(before)
                entry = resp.entries.add()
                entry.entity_id = entity_id
                entry.score = score
                entry.achieved_ms = achieved_ms
                apply_rank_or_beyond(entry, position_around(ahead, capped, delta), self.rank_limit)

            self_entry = resp.entries.add()
            self_entry.entity_id = player_id
            self_entry.score = my_score
            self_entry.achieved_ms = my_achieved
            apply_rank_or_beyond(self_entry, position_around(ahead, capped, 0), self.rank_limit)
            resp.self.CopyFrom(self_entry)

            for idx, (entity_id, score, _okey, achieved_ms) in enumerate(after):
                entry = resp.entries.add()
                entry.entity_id = entity_id
                entry.score = score
                entry.achieved_ms = achieved_ms
                apply_rank_or_beyond(entry, position_around(ahead, capped, idx + 1), self.rank_limit)

            return resp

    # -- RewardStore 实现 -----------------------------------------------------------------------------

    async def due_rewards(self, now_ms: int, limit: int) -> list[int]:
        if limit <= 0 or limit > 1000:
            raise RankInvalidError("invalid due_rewards limit")
        async with self._lock:
            due = [
                row
                for row in self.rewards.values()
                if row.status == REWARD_PENDING and row.next_attempt_ms <= now_ms and row.lease_until_ms < now_ms
            ]
            due.sort(key=lambda r: (r.next_attempt_ms, r.reward_id))
            return [row.reward_id for row in due[:limit]]

    async def claim_reward(self, reward_id: int, now_ms: int, until_ms: int, token: int) -> RewardRow | None:
        async with self._lock:
            row = self.rewards.get(reward_id)
            if row is None or row.status != REWARD_PENDING or row.lease_until_ms >= now_ms:
                return None
            row.lease_until_ms = until_ms
            row.lease_token = token
            return copy.copy(row)

    async def settle_reward(self, reward: RewardRow, mail_id: int, next_ms: int, now_ms: int) -> None:
        state = REWARD_DELIVERED if mail_id != 0 else REWARD_PENDING
        await self._settle(reward, state, mail_id, next_ms, now_ms, counted_attempts=1)

    async def abandon_reward(self, reward: RewardRow, now_ms: int) -> None:
        await self._settle(reward, REWARD_ABANDONED, 0, now_ms, now_ms, counted_attempts=1)

    async def defer_reward(self, reward: RewardRow, next_ms: int, now_ms: int) -> None:
        await self._settle(reward, REWARD_PENDING, 0, next_ms, now_ms, counted_attempts=0)

    async def _settle(
        self,
        reward: RewardRow,
        state: int,
        mail_id: int,
        next_ms: int,
        now_ms: int,
        *,
        counted_attempts: int,
    ) -> None:
        async with self._lock:
            row = self.rewards.get(reward.reward_id)
            if row is None or row.status != REWARD_PENDING or row.lease_token != reward.lease_token:
                raise RankConflictError("reward lease lost or already settled")
            row.status = state
            row.mail_id = mail_id
            row.next_attempt_ms = next_ms
            row.lease_until_ms = 0
            row.attempts += counted_attempts
            row.updated_ms = now_ms

    # -- 内部辅助 -------------------------------------------------------------------------------------

    def _sorted_shards(self, board_id: int, generation: int) -> list[ShardRow]:
        matched = [
            row
            for (bid, gen, _shard), row in self.shards.items()
            if bid == board_id and gen == generation
        ]
        matched.sort(key=lambda r: r.source_shard)
        return matched

    def _ordered_scores(self, board_id: int, generation: int) -> list[tuple[int, int, int, int]]:
        score_map = self.scores.get((board_id, generation), {})
        rows = [
            (entity_id, val[0], val[1], val[2])
            for entity_id, val in score_map.items()
        ]
        # 严格全序：(order_key ASC, achieved_ms ASC, entity_id ASC)
        rows.sort(key=lambda r: (r[2], r[3], r[0]))
        return rows

    @staticmethod
    def _count_ahead(
        ordered: list[tuple[int, int, int, int]],
        target_tuple: tuple[int, int, int],
        cap: int,
    ) -> int:
        count = 0
        for entity_id, _score, okey, achieved_ms in ordered:
            if (okey, achieved_ms, entity_id) < target_tuple:
                count += 1
                if count >= cap:
                    return cap
            else:
                break
        return count

    def _lookup_board(self, key: Message) -> BoardRow:
        candidates = [
            b
            for b in self.boards.values()
            if b.deleted_ms == 0
            and b.board_type == int(key.board_type)
            and b.scope == int(key.scope)
            and b.scope_id == int(key.scope_id)
            and (not key.period or b.period == key.period)
            and (key.generation == 0 or b.generation == int(key.generation))
        ]
        if not candidates:
            raise RankNotFoundError("rank board not found")
        if not key.period:
            candidates.sort(key=lambda b: (b.created_ms, b.board_id), reverse=True)
        else:
            candidates.sort(key=lambda b: b.generation, reverse=True)
        return candidates[0]
