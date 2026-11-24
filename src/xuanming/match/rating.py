"""Elo 评分计算、幂等落账与排行榜发件箱投递器（RankOutboxPublisher）。

与 Go ``go/match/internal/ratingrule/ratingrule.go``、``internal/authority/rating.go``、
``internal/authority/rank_publisher.go`` 及 ``internal/logic/rating.go`` 保持语义逐项一致。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from google.protobuf.message import Message

from ..grpc_runtime import sign_service_call
from ..protocol import Protocol
from .model import MODE_1V1, MODE_5V5, MODE_PVP_TEAM

logger = logging.getLogger(__name__)

# -- 评分与投榜常量 -----------------------------------------------------------------------------------

DEFAULT_RATING_CENTI = 150_000
ELO_K_CENTI = 3200  # K = 32 × 100
RANK_SOURCE_SHARDS = 64

# 计分模式 -> RankBoardType（与 proto/rank/rank.proto 严格对齐）
RANK_BOARD_TYPES: dict[int, int] = {
    MODE_1V1: 1,       # RANK_BOARD_TYPE_MATCH_1V1
    MODE_5V5: 2,       # RANK_BOARD_TYPE_MATCH_5V5
    MODE_PVP_TEAM: 3,  # RANK_BOARD_TYPE_MATCH_PVP_TEAM
}

RANK_SCOPE_GLOBAL = 1
RANK_SUBMIT_SET = 2

OPEN_RANK_GENERATION_PATH = "/rankpb.RankAdmin/OpenRankGeneration"
SUBMIT_RANK_SCORES_PATH = "/rankpb.RankAdmin/SubmitRankScores"
REMOVE_RANK_ENTRY_PATH = "/rankpb.RankAdmin/RemoveRankEntry"

# rank 服务移除条目时的可忽略终态码（榜不存在或已封榜）
_RANK_ERR_BOARD_NOT_FOUND = 21000
_RANK_ERR_GENERATION_CLOSED = 21001

# eBattleOutcome
_BATTLE_OUTCOME_SIDE_A_WIN = 1
_BATTLE_OUTCOME_SIDE_B_WIN = 2
_BATTLE_OUTCOME_DRAW = 3

RATING_OUTCOME_APPLIED = "applied"
RATING_OUTCOME_DUPLICATE = "duplicate"
RATING_OUTCOME_IGNORED = "ignored"


# -- 不可变数据模型 -----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RatingChange:
    """单名玩家在一场计分局中的评分变化快照。"""

    player_id: int
    rating_before_centi: int
    rating_after_centi: int
    delta_centi: int
    games_after: int
    achieved_ms: int


@dataclass(frozen=True, slots=True)
class RatingApplyResult:
    """单场对局 Elo 落账的权威结果。"""

    outcome: str  # applied | duplicate | ignored
    battle_id: int
    mode: int
    shard: int = 0
    source_cursor: int = 0
    unpublished: bool = False
    changes: tuple[RatingChange, ...] = ()


@dataclass(frozen=True, slots=True)
class RankSourceRow:
    """``match_rank_source`` 表行快照（一个计分模式的一片来源游标状态）。"""

    source_epoch: int
    mode: int
    shard: int
    produced_cursor: int
    committed_cursor: int
    board_id: int
    stopped: bool

    @property
    def backlog(self) -> int:
        return max(0, self.produced_cursor - self.committed_cursor)


@dataclass(frozen=True, slots=True)
class RankPublishStats:
    """单轮排行榜发件箱投递统计。"""

    boards_opened: int = 0
    batches_submitted: int = 0
    batches_duplicated: int = 0
    removals_processed: int = 0
    errors: int = 0


# -- 纯计算与规则函数 ---------------------------------------------------------------------------------


def is_rated_mode(mode: int) -> bool:
    """报告该匹配模式是否参与 Elo 评分与投榜（1V1 / 5V5 / PVP_TEAM）。"""
    return int(mode) in RANK_BOARD_TYPES


def rank_board_type(mode: int) -> int | None:
    """返回计分模式对应的 ``RankBoardType``，非计分模式返回 ``None``。"""
    return RANK_BOARD_TYPES.get(int(mode))


def rank_source_shard(battle_id: int) -> int:
    """按对局 ID 对 64 取模定位投榜来源片。"""
    return int(battle_id) % RANK_SOURCE_SHARDS


def round_half_away_from_zero(value: float) -> int:
    """与 Go ``math.Round`` 一致的四舍五入（远离零取整）。"""
    if value >= 0.0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


def compute_elo_delta(team_a_ratings: list[int], team_b_ratings: list[int], score_a: float) -> int:
    """按队伍平均分计算 A 方 Elo 分差（单位：百分之一分，K=32）。"""
    if not team_a_ratings or not team_b_ratings:
        raise ValueError("team ratings must be non-empty")
    avg_a = sum(r / 100.0 for r in team_a_ratings) / len(team_a_ratings)
    avg_b = sum(r / 100.0 for r in team_b_ratings) / len(team_b_ratings)
    expected_a = 1.0 / (1.0 + math.pow(10.0, (avg_b - avg_a) / 400.0))
    return round_half_away_from_zero(ELO_K_CENTI * (score_a - expected_a))


def _side_has_active_unit(settlements, team_index: int) -> bool:
    for settlement in settlements:
        if int(settlement.player_team_index) != team_index:
            continue
        if not bool(settlement.is_dead) and not bool(settlement.fled):
            return True
        for pet in settlement.pets:
            if not bool(pet.is_dead):
                return True
    return False


def rated_score_a(event: Message, round_limit: int = 0) -> tuple[float, str, bool]:
    """将 ``BattleResultEvent`` 换算为 A 方（team 0）Elo 得分：(score_a, round_cap_basis, is_rated)。"""
    outcome = int(event.outcome)
    if outcome == _BATTLE_OUTCOME_SIDE_A_WIN:
        return 1.0, "", True
    if outcome == _BATTLE_OUTCOME_DRAW:
        return 0.5, "", True
    if outcome != _BATTLE_OUTCOME_SIDE_B_WIN:
        return 0.0, "", False
    if int(event.settlement_format_version) == 1:
        if _side_has_active_unit(event.settlements, 0):
            return 0.5, "survivors", True
        return 0.0, "", True
    if round_limit > 0 and int(event.total_rounds) >= round_limit:
        return 0.5, "round_limit", True
    return 0.0, "", True


def split_result_teams(event: Message) -> tuple[list[int], list[int], bool]:
    """从 ``BattleResultEvent`` 拆分恰好两支非空队伍（team_index 0 与 1）。"""
    if len(event.teams) != 2:
        return [], [], False
    team_a: list[int] = []
    team_b: list[int] = []
    for team in event.teams:
        tid = int(team.team_index)
        if tid == 0:
            team_a.extend(int(pid) for pid in team.player_ids)
        elif tid == 1:
            team_b.extend(int(pid) for pid in team.player_ids)
        else:
            return [], [], False
    if not team_a or not team_b:
        return [], [], False
    return team_a, team_b, True


async def apply_battle_result(
    store,
    protocol: Protocol,
    event: Message,
    *,
    source_epoch: int = 1,
    generation: int = 1,
    round_limit: int = 0,
    eligible_for_rank: set[int] | None = None,
) -> RatingApplyResult:
    """消费一条 ``BattleResultEvent`` 并幂等落账 Elo 与投榜发件箱。

    - 非计分模式、未知胜负或队伍形态异常直接返回 ``ignored``；
    - 相同 ``battle_id`` 与确定性载荷摘要重放返回 ``duplicate``（不重复加分或追加发件箱）；
    - 相同 ``battle_id`` 但载荷摘要不一致抛 ``BattleOutcomeOriginalError``。
    """
    battle_id = int(event.battle_id)
    mode = int(event.match_mode)
    if battle_id == 0 or not is_rated_mode(mode):
        return RatingApplyResult(outcome=RATING_OUTCOME_IGNORED, battle_id=battle_id, mode=mode)

    score_a, round_cap, rated = rated_score_a(event, round_limit=round_limit)
    if not rated:
        logger.info("rating battle result ignored battle_id=%d mode=%d outcome=%d", battle_id, mode, int(event.outcome))
        return RatingApplyResult(outcome=RATING_OUTCOME_IGNORED, battle_id=battle_id, mode=mode)

    team_a, team_b, ok = split_result_teams(event)
    if not ok:
        logger.error("rating invalid team structure battle_id=%d mode=%d teams=%d", battle_id, mode, len(event.teams))
        return RatingApplyResult(outcome=RATING_OUTCOME_IGNORED, battle_id=battle_id, mode=mode)

    if round_cap:
        logger.info(
            "rating round limit reached; settling as draw battle_id=%d mode=%d basis=%s rounds=%d",
            battle_id,
            mode,
            round_cap,
            int(event.total_rounds),
        )

    raw_bytes = event.SerializeToString(deterministic=True)
    digest = hashlib.sha256(raw_bytes).digest()
    return await store.apply_rating(
        battle_id=battle_id,
        payload_digest=digest,
        mode=mode,
        team_a=team_a,
        team_b=team_b,
        score_a=score_a,
        source_epoch=source_epoch,
        generation=generation,
        eligible_for_rank=eligible_for_rank,
    )


# -- 排行榜发件箱投递器（RankOutboxPublisher）---------------------------------------------------------


class RankOutboxPublisher:
    """从 ``match_rank_outbox`` 按片顺序投递至 ``rankpb.RankAdmin``，并处理删角摘榜队列。

    所有出站请求均以调用方 ``"match"`` 及 ``RANK_PRODUCER_SECRET_MATCH`` 签名；
    投递失败或超时绝不推进本地 ``committed_cursor``，下次轮询重签同批原件幂等重投。
    """

    def __init__(
        self,
        protocol: Protocol,
        store,
        *,
        rank_call: Callable[[str, bytes], Awaitable[bytes]] | None,
        rank_secret: bytes,
        source_epoch: int | None = None,
        period: str = "all",
        generation: int = 1,
        per_shard_limit: int = 4,
        removal_page_limit: int = 16,
        interval_sec: float = 2.0,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._rank_call = rank_call
        self._rank_secret = rank_secret
        self._source_epoch = source_epoch
        self._period = period
        self._generation = generation
        self._per_shard_limit = per_shard_limit
        self._removal_page_limit = removal_page_limit
        self._interval_sec = interval_sec
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    async def _ensure_board(
        self,
        source_epoch: int,
        mode: int,
        shards: list[RankSourceRow],
    ) -> tuple[int, bool]:
        """确保 ``(source_epoch, mode)`` 已在 rank 服务打开榜并绑定到全部来源片，返回 ``(board_id, opened_now)``。"""
        board_type = rank_board_type(mode)
        if board_type is None:
            raise ValueError(f"mode {mode} is not a rated mode")

        board_id = 0
        unbound = False
        for row in shards:
            if row.source_epoch != source_epoch or row.mode != mode:
                continue
            if row.board_id == 0:
                unbound = True
            elif board_id == 0:
                board_id = row.board_id
            elif board_id != row.board_id:
                raise ValueError(f"inconsistent rank board_id across shards for epoch={source_epoch} mode={mode}")

        opened_now = False
        if board_id == 0:
            generation = self._generation if source_epoch == 1 else source_epoch
            period = self._period if source_epoch == 1 else f"s{source_epoch}"
            request_id = board_type if source_epoch == 1 else ((source_epoch << 16) | board_type)
            req = self.protocol.message(
                "rankpb.OpenRankGenerationRequest",
                producer="match",
                source_epoch=source_epoch,
                ascending=False,
                request_id=request_id,
            )
            req.board.board_type = board_type
            req.board.scope = RANK_SCOPE_GLOBAL
            req.board.period = period
            req.board.generation = generation
            sign_service_call(OPEN_RANK_GENERATION_PATH, req, "match", int(self._clock_ms()), self._rank_secret)
            raw = await self._rank_call(OPEN_RANK_GENERATION_PATH, req.SerializeToString(deterministic=True))
            resp = self.protocol.parse("rankpb.OpenRankGenerationResponse", raw)
            if int(resp.error_message.id) != 0 or int(resp.board_id) == 0:
                raise RuntimeError(
                    f"OpenRankGeneration rejected mode={mode} epoch={source_epoch} tip={int(resp.error_message.id)}"
                )
            board_id = int(resp.board_id)
            opened_now = not bool(resp.duplicated)

        if unbound:
            await self.store.bind_rank_board(source_epoch, mode, board_id)
        return board_id, opened_now

    async def _publish_shard(self, board_id: int, row: RankSourceRow) -> tuple[int, int]:
        """按片内游标顺序投递至多 ``per_shard_limit`` 批，返回 ``(submitted, duplicated)``。"""
        committed = row.committed_cursor
        submitted = 0
        duplicated = 0
        sent = 0
        while committed < row.produced_cursor and sent < self._per_shard_limit:
            next_cursor = committed + 1
            body = await self.store.get_rank_outbox_entry(row.source_epoch, row.mode, row.shard, next_cursor)
            if body is None:
                raise RuntimeError(
                    f"missing match_rank_outbox entry epoch={row.source_epoch} mode={row.mode} "
                    f"shard={row.shard} cursor={next_cursor}"
                )
            req = self.protocol.parse("rankpb.SubmitRankScoresRequest", body)
            req.ClearField("auth")
            req.board_id = board_id
            sign_service_call(SUBMIT_RANK_SCORES_PATH, req, "match", int(self._clock_ms()), self._rank_secret)
            raw = await self._rank_call(SUBMIT_RANK_SCORES_PATH, req.SerializeToString(deterministic=True))
            resp = self.protocol.parse("rankpb.SubmitRankScoresResponse", raw)
            reached = int(resp.committed_cursor)
            tip = int(resp.error_message.id)
            if tip != 0 or reached < int(req.source_cursor) or (reached > int(req.source_cursor) and not resp.duplicated):
                raise RuntimeError(
                    f"SubmitRankScores did not confirm contiguous cursor board_id={board_id} "
                    f"shard={row.shard} want={int(req.source_cursor)} got={reached} tip={tip}"
                )
            await self.store.advance_rank_committed_cursor(row.source_epoch, row.mode, row.shard, reached)
            if resp.duplicated:
                duplicated += 1
            else:
                submitted += 1
            committed = reached
            sent += 1
        return submitted, duplicated

    async def _publish_removals(self, boards_by_mode: dict[int, int]) -> tuple[int, int]:
        """投递删角产生的待摘榜记录，确认后删除本地行。"""
        processed = 0
        errors = 0
        for mode, player_id in await self.store.list_rank_removals(limit=self._removal_page_limit):
            board_id = boards_by_mode.get(mode, 0)
            if board_id == 0:
                continue
            try:
                req = self.protocol.message(
                    "rankpb.RemoveRankEntryRequest",
                    board_id=board_id,
                    generation=self._generation,
                    entity_id=player_id,
                    request_id=player_id,
                )
                sign_service_call(REMOVE_RANK_ENTRY_PATH, req, "match", int(self._clock_ms()), self._rank_secret)
                raw = await self._rank_call(REMOVE_RANK_ENTRY_PATH, req.SerializeToString(deterministic=True))
                resp = self.protocol.parse("rankpb.RankAdminResponse", raw)
                tip = int(resp.error_message.id)
                if tip not in (0, _RANK_ERR_GENERATION_CLOSED, _RANK_ERR_BOARD_NOT_FOUND):
                    raise RuntimeError(f"RemoveRankEntry rejected mode={mode} player={player_id} tip={tip}")
                await self.store.delete_rank_removal(mode, player_id)
                processed += 1
            except Exception:
                logger.warning(
                    "rank publisher removal failed mode=%d player_id=%d",
                    mode,
                    player_id,
                    exc_info=True,
                )
                errors += 1
        return processed, errors

    async def run_once(self) -> RankPublishStats:
        """执行一轮榜绑定、积压分片顺发与删角摘榜。"""
        if self._rank_call is None or len(self._rank_secret) < 32:
            return RankPublishStats()

        shards = await self.store.list_rank_sources(source_epoch=self._source_epoch)
        boards_opened = 0
        batches_submitted = 0
        batches_duplicated = 0
        errors = 0

        boards: dict[tuple[int, int], int] = {}
        latest_board_by_mode: dict[int, int] = {}
        for row in shards:
            key = (row.source_epoch, row.mode)
            if key in boards:
                continue
            try:
                bid, opened = await self._ensure_board(row.source_epoch, row.mode, shards)
                boards[key] = bid
                latest_board_by_mode[row.mode] = bid
                if opened:
                    boards_opened += 1
            except Exception:
                logger.warning(
                    "rank publisher bind board failed epoch=%d mode=%d",
                    row.source_epoch,
                    row.mode,
                    exc_info=True,
                )
                boards[key] = 0
                errors += 1

        # 积压越多的片越先投，避免高号片饥饿（与 Go orderRankShardsByBacklog 一致）
        ordered = sorted(shards, key=lambda r: (-r.backlog, r.source_epoch, r.mode, r.shard))
        for row in ordered:
            if row.backlog <= 0:
                continue
            board_id = boards.get((row.source_epoch, row.mode), 0)
            if board_id == 0:
                continue
            try:
                sub, dup = await self._publish_shard(board_id, row)
                batches_submitted += sub
                batches_duplicated += dup
            except Exception:
                logger.warning(
                    "rank publisher shard submit failed epoch=%d mode=%d shard=%d",
                    row.source_epoch,
                    row.mode,
                    row.shard,
                    exc_info=True,
                )
                errors += 1

        removals_processed, removal_errors = await self._publish_removals(latest_board_by_mode)
        errors += removal_errors
        return RankPublishStats(
            boards_opened=boards_opened,
            batches_submitted=batches_submitted,
            batches_duplicated=batches_duplicated,
            removals_processed=removals_processed,
            errors=errors,
        )

    async def run_forever(self, stop: asyncio.Event) -> None:
        """后台循环持续投递排行榜发件箱直至收到停止信号。"""
        while not stop.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.error("rank publisher round crashed", exc_info=True)
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._interval_sec)
            except asyncio.TimeoutError:
                continue
