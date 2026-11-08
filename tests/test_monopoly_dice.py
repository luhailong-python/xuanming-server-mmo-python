import asyncio
from itertools import count

from test_monopoly import TOKEN, _enter, _gameplay, _settings
from xuanming.data import SqlitePlayerStore
from xuanming.data.player_rows import PlayerRowKind
from xuanming.nodes import monopoly
from xuanming.runtime import Runtime


START_MS = 1_800_000_000_000
REFILL_MS = 5 * 60 * 1000
DICE_CAP = 20

# Captured through TCP from the board version after 20 double-six rolls and before timed refill.
LEGACY_WALLET = bytes.fromhex("42060a04d00f0000ca011982f10415081410061806300140644864520518d00f20155801")
LEGACY_ROLL = bytes.fromhex("081410061806300140644864520518d00f20155801")


def _wire(message):
    return message.SerializeToString(deterministic=True)


async def _board(client, calls, remaining, next_at):
    reply = await _gameplay(client, next(calls), "GetBoard")
    assert not reply.error_message.id
    assert (reply.dice_cap, reply.dice_refill_interval_ms) == (DICE_CAP, REFILL_MS)
    assert (reply.state.dice_remaining, reply.state.next_dice_at_ms) == (remaining, next_at)
    return reply.state


async def _roll(client, calls, seq):
    return await _gameplay(client, next(calls), "RollDice", board_id=1, roll_seq=seq)


def test_dice_refill_after_offline_restart_does_not_bank_at_cap(tmp_path, monkeypatch):
    now = [START_MS]
    draws = []

    def draw(upper):
        draws.append(upper)
        return 5

    monkeypatch.setattr(monopoly, "_now_ms", lambda: now[0])
    monkeypatch.setattr(monopoly, "randbelow", draw)

    async def exercise():
        settings = _settings(tmp_path)
        first_deadline = START_MS + REFILL_MS
        calls = count(3)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                await _board(client, calls, 20, 0)
                for seq in range(1, 21):
                    rolled = await _roll(client, calls, seq)
                    assert not rolled.error_message.id and rolled.durable and not rolled.replayed
                    state = rolled.result.state
                    assert (state.dice_remaining, state.next_roll_seq, state.next_dice_at_ms) == (
                        20 - seq, seq + 1, first_deadline,
                    )
                original = _wire(rolled.result)
                now[0] = first_deadline - 1
                stopped = await _roll(client, calls, 21)
                assert stopped.error_message.id == 1006 and not stopped.durable and not stopped.HasField("result")
                assert len(draws) == 40
                assert (await _board(client, calls, 0, first_deadline)).next_roll_seq == 21

        now[0] = START_MS + 2 * REFILL_MS + REFILL_MS // 2
        third_deadline = START_MS + 3 * REFILL_MS
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_id):
                assert restored_id == player_id
                restored = await _board(client, calls, 2, third_deadline)
                assert (restored.position, restored.coins, restored.next_roll_seq) == (0, 2000, 21)
                assert await _board(client, calls, 2, third_deadline) == restored

                replay = await _roll(client, calls, 20)
                assert not replay.error_message.id and replay.durable and replay.replayed
                assert _wire(replay.result) == original and len(draws) == 40
                assert await _board(client, calls, 2, third_deadline) == restored

                used = await _roll(client, calls, 21)
                assert not used.error_message.id and used.durable and not used.replayed
                assert (used.result.state.dice_remaining, used.result.state.next_dice_at_ms) == (1, third_deadline)
                now[0] = third_deadline
                await _board(client, calls, 2, third_deadline + REFILL_MS)

                now[0] = START_MS + 90 * REFILL_MS
                await _board(client, calls, 20, 0)
                assert (await _board(client, calls, 20, 0)).next_roll_seq == 22

                now[0] = START_MS + 100 * REFILL_MS
                from_cap = await _roll(client, calls, 22)
                assert not from_cap.error_message.id and from_cap.durable and not from_cap.replayed
                fresh_deadline = now[0] + REFILL_MS
                assert (from_cap.result.state.dice_remaining, from_cap.result.state.next_dice_at_ms) == (19, fresh_deadline)

                now[0] = START_MS + 99 * REFILL_MS
                await _board(client, calls, 19, fresh_deadline)
                now[0] = fresh_deadline - 1
                await _board(client, calls, 19, fresh_deadline)
                now[0] = fresh_deadline
                assert (await _board(client, calls, 20, 0)).next_roll_seq == 23

                now[0] = fresh_deadline - REFILL_MS
                after_rollback = await _roll(client, calls, 23)
                assert not after_rollback.error_message.id and after_rollback.durable
                assert (after_rollback.result.state.dice_remaining, after_rollback.result.state.next_dice_at_ms) == (
                    19, fresh_deadline + REFILL_MS,
                )
        assert draws == [6] * 46

    asyncio.run(exercise())


def test_legacy_exhausted_dice_start_a_timer_without_rewriting_the_receipt(tmp_path, monkeypatch):
    now = [START_MS]
    monkeypatch.setattr(monopoly, "_now_ms", lambda: now[0])
    monkeypatch.setattr(monopoly, "randbelow", lambda _: 5)

    async def exercise():
        settings = _settings(tmp_path)
        store = SqlitePlayerStore(settings.database)
        snapshot = await store.load_and_claim(1, b"legacy-monopoly-dice")
        snapshot = await store.commit(snapshot, {PlayerRowKind.WALLET: LEGACY_WALLET}, force=True)
        await store.release(snapshot)

        calls = count(3)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, _):
                blocked = await _roll(client, calls, 21)
                assert blocked.error_message.id == 1006 and not blocked.durable and not blocked.HasField("result")
                state = await _board(client, calls, 0, START_MS + REFILL_MS)
                assert (state.coins, state.next_roll_seq) == (2000, 21)
                replay = await _roll(client, calls, 20)
                assert not replay.error_message.id and replay.durable and replay.replayed
                assert _wire(replay.result) == LEGACY_ROLL

        now[0] += REFILL_MS
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, _):
                # The roll reconciles the persisted timer even without a preceding GetBoard request.
                used = await _roll(client, calls, 21)
                assert not used.error_message.id and used.durable and not used.replayed
                state = await _board(client, calls, 0, START_MS + 2 * REFILL_MS)
                assert (state.coins, state.next_roll_seq) == (2100, 22)
                assert state == used.result.state

    asyncio.run(exercise())
