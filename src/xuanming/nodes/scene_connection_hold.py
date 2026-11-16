"""断线保留和挂机月卡。存盘、释放与入场归 Scene 的生命周期处理。"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .scene import Player

logger = logging.getLogger(__name__)

LOGIN_RECONNECT_LEASE = 30
HOLD_WINDOW = 25
SWEEP_INTERVAL = 1
RELEASE_MARGIN = 4
HOSTING_EXIT_LEAD = SWEEP_INTERVAL + RELEASE_MARGIN
HOSTING_UNCONFIRMED_GRACE = 30
_MAX_U64 = (1 << 64) - 1
_INVALID_SESSION = (1 << 32) - 1

assert HOLD_WINDOW + HOSTING_EXIT_LEAD <= LOGIN_RECONNECT_LEASE


class HoldDecision(Enum):
    HOLD = auto()
    KEEP_HOLDING = auto()
    EXIT_NOW = auto()


class HoldVerdict(Enum):
    KEEP = auto()
    PROBE = auto()
    EXIT = auto()


def session_bound(session_id: int) -> bool:
    return session_id not in (0, _INVALID_SESSION)


def decide_hold(*, exiting: bool, handoff_in_flight: bool, evacuating: bool,
                bound: bool, already_held: bool) -> HoldDecision:
    if exiting:
        return HoldDecision.EXIT_NOW
    if already_held:
        return HoldDecision.KEEP_HOLDING
    if handoff_in_flight or evacuating or not bound:
        return HoldDecision.EXIT_NOW
    return HoldDecision.HOLD


def is_resumed_by(incoming: int, incoming_generation: int, lost: int, lost_generation: int) -> bool:
    return session_bound(incoming) and (incoming != lost or incoming_generation > lost_generation)


def afk_pass_key(player_id: int) -> str:
    return f"player:afk_pass:{{{player_id}}}"


def parse_afk_pass(value: bytes | None, now_unix: int) -> int:
    """返回有效月卡的 Unix 秒，否则为 0。Redis 值只接受无符号 ASCII 十进制。"""
    if value is None or not value.isdigit():
        return 0
    significant = value.lstrip(b"0")
    if not significant or len(significant) > 20:
        return 0
    expires = int(significant)
    return expires if now_unix + HOSTING_EXIT_LEAD < expires <= _MAX_U64 else 0


def judge_hold(
    held_for: float, pass_expires_at: int | None, now_unix: int, in_flight: bool, hold_seconds: float = HOLD_WINDOW,
) -> HoldVerdict:
    # None 代表未查明，0 代表确认无法托管，正值代表上一次查到的到期时刻。
    if pass_expires_at:
        if now_unix + HOSTING_EXIT_LEAD < pass_expires_at:
            return HoldVerdict.KEEP
        if now_unix >= pass_expires_at + HOSTING_UNCONFIRMED_GRACE:
            return HoldVerdict.EXIT
        return HoldVerdict.KEEP if in_flight else HoldVerdict.PROBE
    if held_for >= hold_seconds:
        return HoldVerdict.EXIT
    return HoldVerdict.PROBE if pass_expires_at is None and not in_flight else HoldVerdict.KEEP


@dataclass(slots=True)
class _Hold:
    player: Player
    lost_at: float
    session_id: int
    generation: int
    pass_expires_at: int | None
    probe: asyncio.Task[None] | None = None
    hosting_started: bool = False


class SceneConnectionHolds:
    def __init__(
        self, read_pass: Callable[[int], Awaitable[bytes | None]] | None = None,
        on_hosting: Callable[[Player], None] | None = None, *, hold_seconds: float = HOLD_WINDOW,
        clock: Callable[[], float] = time.monotonic, unix_clock: Callable[[], float] = time.time,
    ) -> None:
        self.read_pass, self.on_hosting = read_pass, on_hosting
        self.hold_seconds, self.clock, self.unix_clock = hold_seconds, clock, unix_clock
        self._holds: dict[int, _Hold] = {}
        self._probes: set[asyncio.Task[None]] = set()
        self._next_sweep = 0.0
        self._closed = False

    def _get(self, player: Player) -> _Hold | None:
        hold = self._holds.get(player.player_id)
        return hold if hold is not None and hold.player is player else None

    def held(self, player: Player) -> bool:
        return self._get(player) is not None

    def hosted(self, player: Player) -> bool:
        hold = self._get(player)
        return hold is not None and hold.hosting_started

    def placement_eligible(self, player: Player) -> bool:
        hold = self._get(player)
        return bool(hold is not None and hold.hosting_started and hold.pass_expires_at
                    and int(self.unix_clock()) + HOSTING_EXIT_LEAD < hold.pass_expires_at
                    and session_bound(hold.session_id) and hold.generation
                    and (player.session_id, player.generation) == (hold.session_id, hold.generation))

    def placement_expiry(self, player: Player) -> int:
        hold = self._get(player)
        return int(hold.pass_expires_at) if hold is not None and self.placement_eligible(player) else 0

    def start(self, player: Player) -> bool:
        if self._closed or not player.player_id or not session_bound(player.session_id):
            return False
        previous = self._holds.get(player.player_id)
        if previous is not None:
            if previous.player is player:
                return False
            self.forget(previous.player)
        hold = _Hold(player, self.clock(), player.session_id, player.generation, None if self.read_pass else 0)
        self._holds[player.player_id] = hold
        self._probe(hold)
        return True

    def resumable(self, player: Player, session_id: int, generation: int) -> bool:
        hold = self._get(player)
        return hold is not None and is_resumed_by(session_id, generation, hold.session_id, hold.generation)

    def forget(self, player: Player) -> None:
        hold = self._get(player)
        if hold is not None:
            del self._holds[player.player_id]
            if hold.probe is not None:
                hold.probe.cancel()

    def sweep(self, now: float | None = None) -> None:
        now = self.clock() if now is None else now
        if self._closed or now < self._next_sweep:
            return
        self._next_sweep = now + SWEEP_INTERVAL
        now_unix = int(self.unix_clock())
        for hold in self._holds.values():
            if self._verdict(hold, now, now_unix) is HoldVerdict.PROBE:
                self._probe(hold)

    def expired(self, player: Player, now: float | None = None) -> bool:
        hold = self._get(player)
        now = self.clock() if now is None else now
        return hold is not None and self._verdict(hold, now, int(self.unix_clock())) is HoldVerdict.EXIT

    def _verdict(self, hold: _Hold, now: float, now_unix: int) -> HoldVerdict:
        return judge_hold(
            now - hold.lost_at, hold.pass_expires_at, now_unix, hold.probe is not None, self.hold_seconds,
        )

    def _probe(self, hold: _Hold) -> None:
        if self.read_pass is not None and hold.probe is None:
            task = asyncio.create_task(self._fetch(hold, self.read_pass))
            hold.probe = task
            self._probes.add(task)
            task.add_done_callback(self._probes.discard)

    async def _fetch(self, hold: _Hold, read_pass: Callable[[int], Awaitable[bytes | None]]) -> None:
        player_id = hold.player.player_id
        try:
            value = await read_pass(player_id)
            if value is not None and not isinstance(value, bytes):
                raise TypeError("AFK pass Redis response is not bytes")
        except Exception:
            logger.warning("AFK pass probe failed player_id=%s", player_id, exc_info=True)
            return
        finally:
            hold.probe = None
        if self._closed or self._holds.get(player_id) is not hold:
            return
        hold.pass_expires_at = parse_afk_pass(value, int(self.unix_clock()))
        if hold.pass_expires_at and not hold.hosting_started:
            hold.hosting_started = True
            if self.on_hosting is not None:
                try:
                    self.on_hosting(hold.player)
                except Exception:
                    logger.exception("AFK hosting start hook failed player_id=%s", player_id)

    async def close(self) -> None:
        self._closed = True
        self._holds.clear()
        tasks = tuple(self._probes)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
