"""Bounded observer-view battle replay, using the original Match wire envelope."""

from __future__ import annotations

import logging
import zlib

from google.protobuf.message import DecodeError, EncodeError, Message


_LOG = logging.getLogger(__name__)
FORMAT_VERSION = 1
GZIP_CODEC = 1
MAX_RAW_BYTES = 1 << 20
MAX_PAYLOAD_BYTES = 512 << 10
RECORD_BUDGET_BYTES = MAX_RAW_BYTES - (16 << 10)


def _strip_live(state: Message) -> None:
    state.ClearField("action_deadline_ms")
    state.ClearField("pending_actor_ids")
    state.ClearField("self_items")
    state.ClearField("surrender_vote")
    if "prep_state" in state.DESCRIPTOR.fields_by_name:
        state.ClearField("prep_state")
    for actor in state.actors:
        actor.ClearField("combat")
        actor.ClearField("skill_cooldown_rounds")
        actor.ClearField("support_effects")
        actor.ClearField("season_seal_state")


class BattleReplayRecorder:
    def __init__(self, protocol, request: Message, fingerprint: str, start: Message) -> None:
        self.protocol = protocol
        self._replay = protocol.message(
            "BattleReplay", battle_id=request.battle_id, match_mode=request.match_mode,
            battle_config_id=request.battle_config_id, started_at_ms=request.created_at_ms,
            table_fingerprint=fingerprint, start_state=start,
        )
        _strip_live(self._replay.start_state)
        self._recorded_bytes = self._replay.start_state.ByteSize()

    def add_round(self, turn: Message) -> None:
        replay = self._replay
        if replay is None or replay.truncated:
            return
        frame = self.protocol.message("TurnResultS2C")
        frame.CopyFrom(turn)
        _strip_live(frame.state)
        size = frame.ByteSize()
        if self._recorded_bytes + size > RECORD_BUDGET_BYTES:
            replay.truncated = True
            _LOG.warning("battle replay truncated battle=%s rounds=%s", replay.battle_id, len(replay.rounds))
        else:
            replay.rounds.append(frame)
            self._recorded_bytes += size

    def mark_deadline_draw(self, state: Message) -> None:
        if self._replay is not None:
            self._replay.final_state.CopyFrom(state)
            _strip_live(self._replay.final_state)
            self._replay.end_reason = 2

    def freeze(self, outcome: int, total_rounds: int, finished_at_ms: int) -> Message | None:
        replay = self._replay
        if replay is None:
            return None
        self._replay = None
        self._recorded_bytes = 0
        replay.outcome = outcome
        replay.total_rounds = total_rounds
        replay.finished_at_ms = finished_at_ms
        replay.end_reason = replay.end_reason or 1
        if not 0 < replay.ByteSize() <= MAX_RAW_BYTES:
            _LOG.warning("battle replay exceeds raw limit battle=%s", replay.battle_id)
            return None
        try:
            raw = replay.SerializeToString(deterministic=True)
            zipper = zlib.compressobj(level=1, wbits=31)
            compressed = zipper.compress(raw) + zipper.flush()
        except (zlib.error, EncodeError):
            _LOG.warning("battle replay could not be encoded battle=%s", replay.battle_id, exc_info=True)
            return None
        if len(compressed) > MAX_PAYLOAD_BYTES:
            _LOG.warning("battle replay exceeds compressed limit battle=%s", replay.battle_id)
            return None
        return self.protocol.message("BattleReplayEnvelope", format_version=FORMAT_VERSION, codec=GZIP_CODEC,
                                     raw_bytes=len(raw), payload=compressed)


def decode_replay(protocol, envelope: Message) -> Message:
    if (envelope.format_version != FORMAT_VERSION or envelope.codec != GZIP_CODEC
            or not 0 < envelope.raw_bytes <= MAX_RAW_BYTES
            or not 0 < len(envelope.payload) <= MAX_PAYLOAD_BYTES):
        raise ValueError("invalid battle replay envelope")
    try:
        reader = zlib.decompressobj(wbits=31)
        raw = reader.decompress(envelope.payload, envelope.raw_bytes + 1)
        if not reader.eof or reader.unused_data or len(raw) != envelope.raw_bytes:
            raise ValueError("invalid battle replay size or gzip stream")
        return protocol.parse("BattleReplay", raw)
    except (zlib.error, DecodeError) as error:
        raise ValueError("invalid battle replay payload") from error
