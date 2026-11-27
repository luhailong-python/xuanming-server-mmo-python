"""万象残局坊 V1（UGC 回合解谜工坊，RPC 575..581）。

严格对齐 C++ / Go 权威契约：
- 规则校验与确定性种子：`go/match/internal/puzzle/spec.go` 与 `cpp/libs/services/battle/system/tactical_puzzle_rules.h`；
- 权威存储与结算语义：`go/match/internal/authority/tactical_puzzle.go`；
- 7 条客户端 RPC：`go/match/internal/logic/tactical_puzzle_logic.go`。
"""

from __future__ import annotations

import asyncio
import logging
import time
import unicodedata
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from google.protobuf.message import DecodeError, Message

from ..protocol import Protocol
from .errors import (
    ERR_MATCH_INTERNAL,
    ERR_MATCH_PUZZLE_CANNOT_LIKE_OWN,
    ERR_MATCH_PUZZLE_CLEAR_REQUIRED_TO_LIKE,
    ERR_MATCH_PUZZLE_LIMIT_REACHED,
    ERR_MATCH_PUZZLE_NOT_FOUND,
    ERR_MATCH_PUZZLE_NOT_OWNER,
    ERR_MATCH_PUZZLE_NOT_PUBLISHED,
    ERR_MATCH_PUZZLE_SOLUTION_REPLAY_NOT_FOUND,
    ERR_MATCH_PUZZLE_SPEC_INVALID,
    ERR_MATCH_PUZZLE_TEXT_REJECTED,
    MatchBusinessError,
    set_tip,
)
from .model import CallerIdentity, is_valid_uuid
from .store import MatchStore

logger = logging.getLogger(__name__)

# -- 残局白名单常量（与 Go puzzle/spec.go 及 C++ tactical_puzzle_rules.h 逐项同源）----------------

WORKSHOP_DUNGEON_ID = 314
WORKSHOP_PLAYMODE_ID = 475
DIRECT_INTENT_KIND_TACTICAL_PUZZLE = 7

MAX_PUZZLES_PER_PLAYER = 10
DEFAULT_LIST_LIMIT = 20
MAX_LIST_LIMIT = 30
LIST_FETCH_CAP = 200

MIN_TITLE_RUNES = 4
MAX_TITLE_RUNES = 20
MAX_HINT_RUNES = 40

MAX_TAG_BIT_COUNT = 6
MAX_TAG_MASK = (1 << MAX_TAG_BIT_COUNT) - 1
MAX_TAGS_PER_ITEM = 2

MIN_ROUND_LIMIT = 1
MAX_ROUND_LIMIT = 6

MIN_CHAR_PRESET_ID = 101
MAX_CHAR_PRESET_ID = 103
MIN_PET_PRESET_ID = 201
MAX_PET_PRESET_ID = 203
MAX_FORMATION_ID = 5

MIN_PLAYER_HP_PCT = 15
MAX_PLAYER_HP_PCT = 100
MIN_PLAYER_MP_PCT = 20
MAX_PLAYER_MP_PCT = 100
MAX_PLAYER_SHIELD_PCT = 50
MAX_PLAYER_DEBUFF_KIND = 3

MIN_ENEMIES = 1
MAX_ENEMIES = 3
MIN_MONSTER_ID = 311
MAX_MONSTER_ID = 316
MIN_ENEMY_HP_PCT = 10
MAX_ENEMY_HP_PCT = 150
MAX_ENEMY_SHIELD_PCT = 60
MIN_SPEED_DELTA_PCT = -30
MAX_SPEED_DELTA_PCT = 30
MAX_ENEMY_BUFF_KIND = 4
MAX_LEYLINE_ELEMENT = 5

_PRESET_SKILL_WHITELIST: dict[int, frozenset[int]] = {
    101: frozenset((10002, 10039, 10025, 10035)),
    102: frozenset((10006, 10020, 10034, 10041)),
    103: frozenset((10010, 10033, 10040, 10026)),
    201: frozenset((10002, 10018, 10016, 13005)),
    202: frozenset((10006, 10019, 10034, 13006)),
    203: frozenset((10014, 10010, 11006, 11005)),
}

STATE_UNSPECIFIED = 0
STATE_DRAFT = 1
STATE_PUBLISHED = 2

TAB_UNSPECIFIED = 0
TAB_HOT = 1
TAB_NEWEST = 2
TAB_HARDEST = 3
TAB_MY_CREATED = 4
TAB_MY_CLEARED = 5

_OUTCOME_SIDE_A_WIN = 1
_MASK64 = (1 << 64) - 1


# -- 纯校验与确定性派生工具函数 ----------------------------------------------------------------------


def is_allowed_preset_skill(char_preset_id: int, pet_preset_id: int, skill_id: int) -> bool:
    """判定 skill_id 是否属于所选公平人物/灵宠预设的合法技能白名单。"""
    sid = int(skill_id)
    if sid <= 0:
        return False
    if sid in _PRESET_SKILL_WHITELIST.get(int(char_preset_id), ()):
        return True
    pet = int(pet_preset_id)
    return pet != 0 and sid in _PRESET_SKILL_WHITELIST.get(pet, ())


def validate_tactical_puzzle_spec(spec: Message | None) -> bool:
    """强校验残局题面全部白名单参数，与 Go ValidateSpec 及 C++ IsValidTacticalPuzzleSpec 逐项同源。"""
    if spec is None:
        return False
    if not (MIN_ROUND_LIMIT <= int(spec.round_limit) <= MAX_ROUND_LIMIT):
        return False
    if not (MIN_CHAR_PRESET_ID <= int(spec.character_preset_id) <= MAX_CHAR_PRESET_ID):
        return False
    pet = int(spec.pet_preset_id)
    if pet != 0 and not (MIN_PET_PRESET_ID <= pet <= MAX_PET_PRESET_ID):
        return False
    if int(spec.formation_id) > MAX_FORMATION_ID:
        return False
    if not (MIN_PLAYER_HP_PCT <= int(spec.player_initial_hp_pct) <= MAX_PLAYER_HP_PCT):
        return False
    if not (MIN_PLAYER_MP_PCT <= int(spec.player_initial_mp_pct) <= MAX_PLAYER_MP_PCT):
        return False
    if int(spec.player_initial_shield_pct) > MAX_PLAYER_SHIELD_PCT:
        return False
    if int(spec.player_initial_debuff_kind) > MAX_PLAYER_DEBUFF_KIND:
        return False
    leyline = int(getattr(spec, "leyline_element", 0))
    if not (0 <= leyline <= MAX_LEYLINE_ELEMENT):
        return False
    banned_skill_id = int(getattr(spec, "banned_skill_id", 0))
    if banned_skill_id != 0 and not is_allowed_preset_skill(int(spec.character_preset_id), pet, banned_skill_id):
        return False
    enemies = list(spec.enemies)
    if not (MIN_ENEMIES <= len(enemies) <= MAX_ENEMIES):
        return False
    for enemy in enemies:
        if enemy is None:
            return False
        if not (MIN_MONSTER_ID <= int(enemy.monster_table_id) <= MAX_MONSTER_ID):
            return False
        if not (MIN_ENEMY_HP_PCT <= int(enemy.initial_hp_pct) <= MAX_ENEMY_HP_PCT):
            return False
        if int(enemy.initial_shield_pct) > MAX_ENEMY_SHIELD_PCT:
            return False
        if not (MIN_SPEED_DELTA_PCT <= int(enemy.speed_delta_pct) <= MAX_SPEED_DELTA_PCT):
            return False
        if int(enemy.initial_buff_kind) > MAX_ENEMY_BUFF_KIND:
            return False
    return True


def validate_tag_mask(mask: int) -> bool:
    """校验标签掩码仅含合法 6 位且至多选 2 项。"""
    m = int(mask)
    if m < 0 or (m & ~MAX_TAG_MASK) != 0:
        return False
    return m.bit_count() <= MAX_TAGS_PER_ITEM


def _has_control_rune(text: str) -> bool:
    return any(unicodedata.category(ch) == "Cc" for ch in text)


def normalize_and_validate_text(title: str, hint: str) -> tuple[str, str] | None:
    """清理首尾空白并拒绝控制字符与超长/过短文本；非法时返回 None。"""
    t = (title or "").strip()
    h = (hint or "").strip()
    if _has_control_rune(t) or _has_control_rune(h):
        return None
    if not (MIN_TITLE_RUNES <= len(t) <= MAX_TITLE_RUNES):
        return None
    if len(h) > MAX_HINT_RUNES:
        return None
    return t, h


def deterministic_puzzle_seed(puzzle_id: int, spec_revision: int) -> int:
    """按 (puzzle_id, spec_revision) 确定性派生非零 64 位战斗 RNG 种子（SplitMix64，与 Go 逐位一致）。"""
    x = (
        (int(puzzle_id) & _MASK64)
        ^ (((int(spec_revision) & _MASK64) * 0x9E3779B97F4A7C15) & _MASK64)
        ^ 0xA0761D6478BD642F
    )
    x = (x + 0x9E3779B97F4A7C15) & _MASK64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & _MASK64
    x = (x ^ (x >> 31)) & _MASK64
    return x if x != 0 else 1


def derive_honor(published: int, cleared: int, likes: int) -> tuple[int, str]:
    """派生纯展示零属性荣誉档位与称谓。"""
    if cleared >= 30 or (published >= 3 and likes >= 20):
        return 4, "奇局宗师"
    if cleared >= 15 or (published >= 1 and likes >= 5):
        return 3, "破局国手"
    if cleared >= 5 or published >= 1:
        return 2, "妙手破局"
    if cleared >= 1:
        return 1, "初试棋局"
    return 0, ""


# -- 存储记录结构与管理器 ---------------------------------------------------------------------------


@dataclass(slots=True)
class _PuzzleRow:
    puzzle_id: int
    author_player_id: int
    author_name: str
    state: int
    spec_revision: int
    title: str
    hint_text: str
    tag_mask: int
    spec_payload: bytes
    verified_battle_id: int = 0
    author_best_rounds: int = 0
    attempt_count: int = 0
    clear_count: int = 0
    global_best_rounds: int = 0
    like_count: int = 0
    created_ms: int = 0
    published_ms: int = 0
    updated_ms: int = 0


@dataclass(slots=True)
class _ClearRow:
    puzzle_id: int
    player_id: int
    attempts: int = 0
    cleared: bool = False
    best_rounds: int = 0
    liked: bool = False
    first_cleared_ms: int = 0
    updated_ms: int = 0


class TacticalPuzzleManager:
    """万象残局坊 V1 管理器（覆盖 575..581 七条 RPC 与终局自证/破局结算回调）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        *,
        battle_launcher: Callable[[CallerIdentity, Message, str], Awaitable[int]] | None = None,
        author_name_lookup: Callable[[int], Awaitable[str]] | None = None,
        content_guard: Callable[[int, str], tuple[bool, bool]] | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.battle_launcher = battle_launcher
        self.author_name_lookup = author_name_lookup
        self.content_guard = content_guard
        self._clock_ms = clock_ms
        self._lock = asyncio.Lock()
        self._puzzles: dict[int, _PuzzleRow] = {}
        self._clears: dict[tuple[int, int], _ClearRow] = {}
        self._settled_battles: set[int] = set()

    def _now(self) -> int:
        return max(1, int(self._clock_ms()))

    def _adjust_report_ref_count(self, battle_id: int, delta: int) -> bool:
        """若底层存储为 MemoryMatchStore，同步增减标准解回放的 ref_count 以钉住或释放正文。"""
        if battle_id <= 0:
            return False
        state = getattr(self.store, "_state", None)
        if state is None:
            return False
        rep = state.reports.get(battle_id)
        if rep is None or not rep.envelope:
            return False
        if delta > 0:
            rep.ref_count += delta
        elif delta < 0:
            rep.ref_count = max(0, rep.ref_count + delta)
            if rep.ref_count == 0:
                state.reports.pop(battle_id, None)
        return True

    def _has_stored_report(self, battle_id: int) -> bool:
        state = getattr(self.store, "_state", None)
        if state is not None:
            rep = state.reports.get(battle_id)
            return rep is not None and bool(rep.envelope)
        return False

    def _check_text_guard(self, player_id: int, text: str) -> int:
        if not text or self.content_guard is None:
            return 0
        passed, unavailable = self.content_guard(player_id, text)
        if unavailable:
            return ERR_MATCH_INTERNAL
        if not passed:
            return ERR_MATCH_PUZZLE_TEXT_REJECTED
        return 0

    def _build_summary_view(self, rec: _PuzzleRow, clear_row: _ClearRow | None) -> Message:
        spec = self.protocol.message("TacticalPuzzleSpec")
        if rec.spec_payload:
            spec.ParseFromString(rec.spec_payload)
        view = self.protocol.message(
            "match.TacticalPuzzleSummaryView",
            puzzle_id=rec.puzzle_id,
            author_player_id=rec.author_player_id,
            author_name=rec.author_name,
            state=rec.state,
            spec_revision=rec.spec_revision,
            title=rec.title,
            hint_text=rec.hint_text,
            tag_mask=rec.tag_mask,
            author_best_rounds=rec.author_best_rounds,
            attempt_count=rec.attempt_count,
            clear_count=rec.clear_count,
            global_best_rounds=rec.global_best_rounds,
            like_count=rec.like_count,
            has_solution_replay=rec.verified_battle_id != 0,
            published_ms=rec.published_ms,
            updated_ms=rec.updated_ms,
        )
        view.spec.CopyFrom(spec)
        if clear_row is not None:
            view.self_cleared = clear_row.cleared
            view.self_best_rounds = clear_row.best_rounds
            view.self_liked = clear_row.liked
        return view

    def _build_profile_locked(self, player_id: int) -> Message:
        published = 0
        draft = 0
        likes_received = 0
        for rec in self._puzzles.values():
            if rec.author_player_id != player_id:
                continue
            if rec.state == STATE_PUBLISHED:
                published += 1
                likes_received += rec.like_count
            else:
                draft += 1
        cleared = sum(
            1 for (_, pid), row in self._clears.items() if pid == player_id and row.cleared
        )
        tier, title = derive_honor(published, cleared, likes_received)
        return self.protocol.message(
            "match.TacticalPuzzleProfileView",
            published_count=published,
            draft_count=draft,
            total_cleared_count=cleared,
            total_likes_received=likes_received,
            honor_tier=tier,
            honor_title=title,
        )

    # -- 575 ListTacticalPuzzles ---------------------------------------------------------------------

    async def list_puzzles(self, caller: CallerIdentity, request: Message) -> Message:
        """575 ListTacticalPuzzles：按 5 大页签与可选标签掩码分页列出残局及个人荣誉档案。"""
        tab = int(request.tab) or TAB_HOT
        limit = int(request.limit) or DEFAULT_LIST_LIMIT
        if limit > MAX_LIST_LIMIT:
            limit = MAX_LIST_LIMIT
        offset = max(0, int(request.offset))
        tag_filter = int(request.tag_filter_mask) & MAX_TAG_MASK

        resp = self.protocol.message("match.ListTacticalPuzzlesResponse")
        async with self._lock:
            if tab == TAB_MY_CREATED:
                rows = [r for r in self._puzzles.values() if r.author_player_id == caller.player_id]
                rows.sort(key=lambda r: (r.updated_ms, r.puzzle_id), reverse=True)
            elif tab == TAB_MY_CLEARED:
                cleared_pairs = [
                    (self._puzzles[pid], c.updated_ms)
                    for (pid, uid), c in self._clears.items()
                    if uid == caller.player_id and c.cleared and pid in self._puzzles
                ]
                cleared_pairs.sort(key=lambda item: (item[1], item[0].puzzle_id), reverse=True)
                rows = [item[0] for item in cleared_pairs]
            elif tab == TAB_NEWEST:
                rows = [r for r in self._puzzles.values() if r.state == STATE_PUBLISHED]
                rows.sort(key=lambda r: (r.published_ms, r.puzzle_id), reverse=True)
            else:
                # HOT & HARDEST 基线先取已发布题
                rows = [r for r in self._puzzles.values() if r.state == STATE_PUBLISHED]
                rows.sort(key=lambda r: (r.like_count, r.clear_count, r.puzzle_id), reverse=True)

            rows = rows[:LIST_FETCH_CAP]
            if tag_filter:
                rows = [r for r in rows if (r.tag_mask & tag_filter) != 0]

            if tab == TAB_HARDEST:
                # 已尝试的排前面，按通关率 clear/attempt 升序，同率按 attempt 降序、puzzle_id 降序
                from functools import cmp_to_key

                def _cmp_hardest(a: _PuzzleRow, b: _PuzzleRow) -> int:
                    a_tried, b_tried = a.attempt_count > 0, b.attempt_count > 0
                    if a_tried != b_tried:
                        return -1 if a_tried else 1
                    lhs = a.clear_count * max(b.attempt_count, 1)
                    rhs = b.clear_count * max(a.attempt_count, 1)
                    if lhs != rhs:
                        return -1 if lhs < rhs else 1
                    if a.attempt_count != b.attempt_count:
                        return -1 if a.attempt_count > b.attempt_count else 1
                    if a.puzzle_id != b.puzzle_id:
                        return -1 if a.puzzle_id > b.puzzle_id else 1
                    return 0

                rows.sort(key=cmp_to_key(_cmp_hardest))

            total_count = len(rows)
            page = rows[offset : offset + limit]
            for rec in page:
                clear_row = self._clears.get((rec.puzzle_id, caller.player_id))
                resp.puzzles.add().CopyFrom(self._build_summary_view(rec, clear_row))
            resp.total_count = total_count
            resp.profile.CopyFrom(self._build_profile_locked(caller.player_id))
        return resp

    # -- 576 GetTacticalPuzzle -----------------------------------------------------------------------

    async def get_puzzle(self, caller: CallerIdentity, request: Message) -> Message:
        """576 GetTacticalPuzzle：只读查询单题详情（草稿仅作者本人可见）。"""
        resp = self.protocol.message("match.GetTacticalPuzzleResponse")
        puzzle_id = int(request.puzzle_id)
        if puzzle_id <= 0:
            return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在或暂不可查看")
        async with self._lock:
            rec = self._puzzles.get(puzzle_id)
            if rec is None or (rec.state != STATE_PUBLISHED and rec.author_player_id != caller.player_id):
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在或暂不可查看")
            clear_row = self._clears.get((puzzle_id, caller.player_id))
            resp.puzzle.CopyFrom(self._build_summary_view(rec, clear_row))
        return resp

    # -- 577 SaveTacticalPuzzleDraft -----------------------------------------------------------------

    async def save_draft(self, caller: CallerIdentity, request: Message) -> Message:
        """577 SaveTacticalPuzzleDraft：新建或编辑本人残局草稿（编辑已发布题重置为草稿并递增版本）。"""
        resp = self.protocol.message("match.SaveTacticalPuzzleDraftResponse")
        text_pair = normalize_and_validate_text(request.title, request.hint_text)
        if text_pair is None:
            return set_tip(resp, ERR_MATCH_PUZZLE_TEXT_REJECTED, "标题需为 4～20 个合法字符，思路提示不超过 40 字")
        title, hint = text_pair
        tag_mask = int(request.tag_mask)
        if not validate_tag_mask(tag_mask):
            return set_tip(resp, ERR_MATCH_PUZZLE_SPEC_INVALID, "标签数量超出限制")
        if not request.HasField("spec") or not validate_tactical_puzzle_spec(request.spec):
            return set_tip(resp, ERR_MATCH_PUZZLE_SPEC_INVALID, "残局配置超出允许范围")

        guard_err = self._check_text_guard(caller.player_id, title)
        if guard_err:
            return set_tip(resp, guard_err, "标题未通过内容安全审核")
        guard_err = self._check_text_guard(caller.player_id, hint)
        if guard_err:
            return set_tip(resp, guard_err, "思路提示未通过内容安全审核")

        author_name = ""
        if self.author_name_lookup is not None:
            try:
                author_name = await self.author_name_lookup(caller.player_id)
            except Exception:
                logger.warning("tactical puzzle author name lookup failed player_id=%d", caller.player_id, exc_info=True)
        if not author_name:
            author_name = f"道友{caller.player_id}"

        spec_bytes = request.spec.SerializeToString(deterministic=True)
        puzzle_id = int(request.puzzle_id)
        now_ms = self._now()

        async with self._lock:
            if puzzle_id == 0:
                owned_count = sum(1 for r in self._puzzles.values() if r.author_player_id == caller.player_id)
                if owned_count >= MAX_PUZZLES_PER_PLAYER:
                    return set_tip(resp, ERR_MATCH_PUZZLE_LIMIT_REACHED, "每人最多保留 10 道残局")
                new_id = await self.store.next_id()
                rec = _PuzzleRow(
                    puzzle_id=new_id,
                    author_player_id=caller.player_id,
                    author_name=author_name,
                    state=STATE_DRAFT,
                    spec_revision=1,
                    title=title,
                    hint_text=hint,
                    tag_mask=tag_mask,
                    spec_payload=spec_bytes,
                    created_ms=now_ms,
                    updated_ms=now_ms,
                )
                self._puzzles[new_id] = rec
            else:
                rec = self._puzzles.get(puzzle_id)
                if rec is None:
                    return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
                if rec.author_player_id != caller.player_id:
                    return set_tip(resp, ERR_MATCH_PUZZLE_NOT_OWNER, "仅作者本人可修改该残局")
                old_verified = rec.verified_battle_id
                rec.author_name = author_name
                rec.state = STATE_DRAFT
                rec.spec_revision += 1
                rec.title = title
                rec.hint_text = hint
                rec.tag_mask = tag_mask
                rec.spec_payload = spec_bytes
                rec.verified_battle_id = 0
                rec.author_best_rounds = 0
                rec.updated_ms = now_ms
                if old_verified:
                    self._adjust_report_ref_count(old_verified, -1)
            resp.puzzle.CopyFrom(self._build_summary_view(rec, None))
        return resp

    # -- 578 DeleteTacticalPuzzle --------------------------------------------------------------------

    async def delete_puzzle(self, caller: CallerIdentity, request: Message) -> Message:
        """578 DeleteTacticalPuzzle：删除本人残局，清理通关记录并释放标准解录像引用。"""
        resp = self.protocol.message("match.DeleteTacticalPuzzleResponse")
        puzzle_id = int(request.puzzle_id)
        if puzzle_id <= 0:
            return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
        async with self._lock:
            rec = self._puzzles.get(puzzle_id)
            if rec is None:
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
            if rec.author_player_id != caller.player_id:
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_OWNER, "仅作者本人可删除该残局")
            old_verified = rec.verified_battle_id
            self._puzzles.pop(puzzle_id, None)
            for key in [k for k in self._clears if k[0] == puzzle_id]:
                self._clears.pop(key, None)
            if old_verified:
                self._adjust_report_ref_count(old_verified, -1)
        return resp

    # -- 579 StartTacticalPuzzleBattle ---------------------------------------------------------------

    async def start_battle(self, caller: CallerIdentity, request: Message) -> Message:
        """579 StartTacticalPuzzleBattle：校验开战资格、冻结残局上下文并触发统一开局管线。"""
        resp = self.protocol.message("match.StartTacticalPuzzleBattleResponse")
        puzzle_id = int(request.puzzle_id)
        is_author_verify = bool(request.is_author_verify)
        if puzzle_id <= 0:
            return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
        if self.battle_launcher is None:
            return set_tip(resp, ERR_MATCH_INTERNAL, "残局开战暂不可用")

        async with self._lock:
            rec = self._puzzles.get(puzzle_id)
            if rec is None:
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
            if is_author_verify:
                if rec.author_player_id != caller.player_id:
                    return set_tip(resp, ERR_MATCH_PUZZLE_NOT_OWNER, "仅作者本人可发起自证验题")
            else:
                if rec.state != STATE_PUBLISHED:
                    return set_tip(resp, ERR_MATCH_PUZZLE_NOT_PUBLISHED, "该残局尚未发布")
            spec = self.protocol.message("TacticalPuzzleSpec")
            try:
                spec.ParseFromString(rec.spec_payload)
            except DecodeError:
                return set_tip(resp, ERR_MATCH_INTERNAL, "残局配置损坏")
            if not validate_tactical_puzzle_spec(spec):
                return set_tip(resp, ERR_MATCH_PUZZLE_SPEC_INVALID, "残局配置超出允许范围")

            pctx = self.protocol.message(
                "TacticalPuzzleBattleContext",
                puzzle_id=rec.puzzle_id,
                spec_revision=rec.spec_revision,
                is_author_verify=is_author_verify,
                puzzle_title=rec.title,
                author_player_id=rec.author_player_id,
                author_name=rec.author_name,
            )
            pctx.spec.CopyFrom(spec)

        req_id = request.request_id if is_valid_uuid(request.request_id) else str(uuid.uuid4())
        try:
            battle_id = await self.battle_launcher(caller, pctx, req_id)
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)
        except Exception:
            logger.error(
                "tactical puzzle launch failed player_id=%d puzzle_id=%d",
                caller.player_id, puzzle_id, exc_info=True,
            )
            return set_tip(resp, ERR_MATCH_INTERNAL, "残局开战暂不可用")

        resp.battle_id = int(battle_id)
        resp.spec_revision = int(pctx.spec_revision)
        return resp

    # -- 580 LikeTacticalPuzzle ----------------------------------------------------------------------

    async def like_puzzle(self, caller: CallerIdentity, request: Message) -> Message:
        """580 LikeTacticalPuzzle：仅允许已通关该题的非作者玩家幂等点赞。"""
        resp = self.protocol.message("match.LikeTacticalPuzzleResponse")
        puzzle_id = int(request.puzzle_id)
        if puzzle_id <= 0:
            return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
        now_ms = self._now()
        async with self._lock:
            rec = self._puzzles.get(puzzle_id)
            if rec is None:
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
            if rec.state != STATE_PUBLISHED:
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_PUBLISHED, "该残局尚未发布")
            if rec.author_player_id == caller.player_id:
                return set_tip(resp, ERR_MATCH_PUZZLE_CANNOT_LIKE_OWN, "不能给自己的残局点赞")
            clear_row = self._clears.get((puzzle_id, caller.player_id))
            if clear_row is None or not clear_row.cleared:
                return set_tip(resp, ERR_MATCH_PUZZLE_CLEAR_REQUIRED_TO_LIKE, "通关该残局后方可点赞")
            if not clear_row.liked:
                clear_row.liked = True
                clear_row.updated_ms = now_ms
                rec.like_count += 1
                rec.updated_ms = now_ms
            resp.like_count = rec.like_count
            resp.self_liked = True
        return resp

    # -- 581 GetTacticalPuzzleSolutionReplay ---------------------------------------------------------

    async def get_solution_replay(self, caller: CallerIdentity, request: Message) -> Message:
        """581 GetTacticalPuzzleSolutionReplay：仅凭 puzzle_id 读取作者钉住的标准解回放。"""
        resp = self.protocol.message("match.GetTacticalPuzzleSolutionReplayResponse")
        puzzle_id = int(request.puzzle_id)
        if puzzle_id <= 0:
            return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
        async with self._lock:
            rec = self._puzzles.get(puzzle_id)
            if rec is None:
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_FOUND, "残局不存在")
            if rec.state != STATE_PUBLISHED and rec.author_player_id != caller.player_id:
                return set_tip(resp, ERR_MATCH_PUZZLE_NOT_PUBLISHED, "该残局尚未发布")
            verified_battle_id = rec.verified_battle_id
        if verified_battle_id <= 0:
            return set_tip(resp, ERR_MATCH_PUZZLE_SOLUTION_REPLAY_NOT_FOUND, "标准解录像暂不可用")
        try:
            env = await self.store.get_battle_report(caller.player_id, verified_battle_id, allow_public=True)
        except MatchBusinessError:
            return set_tip(resp, ERR_MATCH_PUZZLE_SOLUTION_REPLAY_NOT_FOUND, "标准解录像暂不可用")
        resp.replay.CopyFrom(env)
        return resp

    # -- 终局结算回调与删角清理 ----------------------------------------------------------------------

    async def on_battle_completed(
        self,
        battle_id: int,
        formation: Message,
        event: Message,
        *,
        has_replay: bool = False,
        now_ms: int | None = None,
    ) -> None:
        """终局提交后原子结算作者自证发布（钉住标准解回放 ref_count+1）与挑战者破局统计。"""
        if formation is None or event is None:
            return
        raw_ctx = bytes(getattr(formation, "tactical_puzzle_context", b""))
        if not raw_ctx:
            return
        try:
            pctx = self.protocol.parse("TacticalPuzzleBattleContext", raw_ctx)
        except DecodeError:
            return
        puzzle_id = int(pctx.puzzle_id)
        if puzzle_id <= 0:
            return

        settlement = next((s for s in event.settlements if int(s.player_id) != 0), None)
        if settlement is None:
            return
        player_id = int(settlement.player_id)
        rounds = int(event.total_rounds)
        won = (
            int(event.outcome) == _OUTCOME_SIDE_A_WIN
            and int(settlement.player_team_index) == 0
            and not bool(settlement.fled)
            and rounds > 0
        )
        stamp = self._now() if now_ms is None else int(now_ms)

        async with self._lock:
            if battle_id in self._settled_battles:
                return
            rec = self._puzzles.get(puzzle_id)
            if rec is None:
                return
            self._settled_battles.add(battle_id)

            if bool(pctx.is_author_verify):
                if not won or player_id != rec.author_player_id or int(pctx.spec_revision) != rec.spec_revision:
                    return
                report_ready = has_replay or self._has_stored_report(battle_id)
                old_verified = rec.verified_battle_id
                new_verified = old_verified
                if report_ready:
                    new_verified = int(battle_id)
                    if new_verified != old_verified:
                        self._adjust_report_ref_count(new_verified, +1)
                if rec.author_best_rounds == 0 or rounds < rec.author_best_rounds:
                    rec.author_best_rounds = rounds
                if rec.global_best_rounds == 0 or rounds < rec.global_best_rounds:
                    rec.global_best_rounds = rounds
                if rec.published_ms == 0:
                    rec.published_ms = stamp
                rec.state = STATE_PUBLISHED
                rec.verified_battle_id = new_verified
                rec.updated_ms = stamp
                if report_ready and old_verified and old_verified != new_verified:
                    self._adjust_report_ref_count(old_verified, -1)
                return

            # 普通挑战者攻关：仅统计已发布残局
            if rec.state != STATE_PUBLISHED:
                return
            key = (puzzle_id, player_id)
            row = self._clears.get(key)
            if row is None:
                row = _ClearRow(puzzle_id=puzzle_id, player_id=player_id)
                self._clears[key] = row
            row.attempts += 1
            newly_cleared = False
            if won:
                if not row.cleared:
                    newly_cleared = True
                    row.cleared = True
                    row.first_cleared_ms = stamp
                if row.best_rounds == 0 or rounds < row.best_rounds:
                    row.best_rounds = rounds
            row.updated_ms = stamp

            rec.attempt_count += 1
            if newly_cleared:
                rec.clear_count += 1
            if won and (rec.global_best_rounds == 0 or rounds < rec.global_best_rounds):
                rec.global_best_rounds = rounds
            rec.updated_ms = stamp

    async def purge_player(self, player_id: int) -> None:
        """删角清理：移除该角色创作的全部残局与个人破局记录，并释放钉住的标准解回放。"""
        async with self._lock:
            owned_ids = [pid for pid, rec in self._puzzles.items() if rec.author_player_id == player_id]
            for pid in owned_ids:
                rec = self._puzzles.pop(pid, None)
                for key in [k for k in self._clears if k[0] == pid]:
                    self._clears.pop(key, None)
                if rec is not None and rec.verified_battle_id:
                    self._adjust_report_ref_count(rec.verified_battle_id, -1)
            for key in [k for k in self._clears if k[1] == player_id]:
                self._clears.pop(key, None)
