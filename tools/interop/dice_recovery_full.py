"""Validate Monopoly recovery across Python portal login and native MySQL saves."""

import time
from unittest.mock import patch

from board_full import wire
from data_full import read_native_player
from landmark_full import relogin, rpc
from xuanming.nodes import monopoly


CAP = 20
INTERVAL_MS = 300_000


def expect_board(board, *, remaining, next_at, next_roll, coins):
    assert not board.error_message.id and board.board_id == 1
    assert (board.dice_cap, board.dice_refill_interval_ms) == (CAP, INTERVAL_MS)
    state = board.state
    assert (state.dice_remaining, state.next_dice_at_ms, state.next_roll_seq, state.coins) == (
        remaining, next_at, next_roll, coins,
    )


async def check_sql(runtime, player_id, receipt, *, remaining, anchor, coins):
    snapshot = await read_native_player(player_id)
    saved = runtime.scene.rows.assemble(snapshot)
    ledger = saved.client_request_ledger
    dice = ledger.monopoly_dice
    assert (dice.dice_remaining, dice.anchor_ms) == (remaining, anchor)
    assert wire(ledger.monopoly_last_roll) == wire(receipt)
    assert (saved.currency.values[0] if saved.currency.values else 0) == coins
    return snapshot


async def replay(client, call_id, receipt):
    result = await rpc(client, call_id, "RollDice", board_id=1, roll_seq=receipt.roll_seq)
    assert not result.error_message.id and result.durable and result.replayed
    assert wire(result.result) == wire(receipt)


async def verify_recovery(client, runtime, player_id, receipt, *, extra_coins=0):
    state = receipt.state
    expected_coins = state.coins + extra_coins
    board = await rpc(client, 91, "GetBoard")
    expect_board(board, remaining=CAP - 1, next_at=state.next_dice_at_ms,
                 next_roll=state.next_roll_seq, coins=expected_coins)
    await replay(client, 92, receipt)
    assert await rpc(client, 93, "GetBoard") == board
    return await check_sql(runtime, player_id, receipt, remaining=CAP - 1,
                           anchor=state.next_dice_at_ms - INTERVAL_MS, coins=expected_coins)


async def exercise_recovery(stack, session, source, gate, runtime, ingress, report):
    initial_time = time.time_ns() // 1_000_000
    gameplay_time = initial_time

    def set_gameplay_time(value):
        nonlocal gameplay_time
        gameplay_time = value

    # Patch only the Monopoly module clock in this runner. Native authentication,
    # ownership fences, gRPC deadlines, Kafka and MySQL all retain wall time.
    stack.enter_context(patch.object(monopoly, "_now_ms", lambda: gameplay_time))
    player_id, client = session.player_id, session.client
    board = await rpc(client, 40, "GetBoard")
    expect_board(board, remaining=CAP, next_at=0, next_roll=1, coins=0)
    first_deadline = initial_time + INTERVAL_MS

    for roll_seq in (1, 2):
        roll = await rpc(client, 40 + roll_seq, "RollDice", board_id=1, roll_seq=roll_seq)
        assert not roll.error_message.id and roll.durable and not roll.replayed
        receipt = roll.result
        assert receipt.roll_seq == roll_seq and receipt.state.next_roll_seq == roll_seq + 1
        assert (receipt.state.dice_remaining, receipt.state.next_dice_at_ms) == (CAP - roll_seq, first_deadline)
    coins = receipt.state.coins
    spent_save = await check_sql(runtime, player_id, receipt, remaining=CAP - 2, anchor=initial_time, coins=coins)

    set_gameplay_time(first_deadline - 1)
    expect_board(await rpc(client, 44, "GetBoard"), remaining=CAP - 2, next_at=first_deadline,
                 next_roll=3, coins=coins)
    session, reentered = await relogin(
        stack, session, source, gate, runtime, ingress, leave_call_id=45,
        after_logout=lambda: set_gameplay_time(first_deadline),
    )
    client = session.client
    board = await rpc(client, 46, "GetBoard")
    expect_board(board, remaining=CAP - 1, next_at=first_deadline + INTERVAL_MS, next_roll=3, coins=coins)
    recovered_save = await check_sql(runtime, player_id, receipt, remaining=CAP - 1, anchor=first_deadline, coins=coins)
    assert not recovered_save.released and recovered_save.owner_epoch == reentered.snapshot.owner_epoch
    assert recovered_save.save_seq > spent_save.save_seq
    await replay(client, 47, receipt)
    assert await rpc(client, 48, "GetBoard") == board

    cap_time = first_deadline + INTERVAL_MS
    set_gameplay_time(cap_time)
    expect_board(await rpc(client, 49, "GetBoard"), remaining=CAP, next_at=0, next_roll=3, coins=coins)
    capped_save = await check_sql(runtime, player_id, receipt, remaining=CAP, anchor=cap_time, coins=coins)
    assert capped_save.save_seq > recovered_save.save_seq

    now = cap_time + 3 * INTERVAL_MS
    set_gameplay_time(now)
    roll = await rpc(client, 50, "RollDice", board_id=1, roll_seq=3)
    assert not roll.error_message.id and roll.durable and not roll.replayed
    fresh = roll.result
    assert fresh.state.coins == coins + fresh.coins_credited
    assert fresh.state.next_roll_seq == 4 and fresh.state.next_dice_at_ms == now + INTERVAL_MS
    fresh_save = await verify_recovery(client, runtime, player_id, fresh)
    assert fresh_save.save_seq > capped_save.save_seq

    report.update(dice_cap=CAP, refill_interval_ms=INTERVAL_MS, depleted_to=CAP - 2,
                  recovered_after_relogin=CAP - 1, capped_at=CAP, after_later_roll=CAP - 1,
                  first_deadline_ms=first_deadline, new_deadline_ms=fresh.state.next_dice_at_ms,
                  original_ticket_relogin=True, exact_replay_verified=True, no_banking_verified=True,
                  mysql_save_seq=[spent_save.save_seq, recovered_save.save_seq, capped_save.save_seq,
                                  fresh_save.save_seq], accelerated_gameplay_clock=True)
    print("PASS Monopoly 5-minute recovery through original logout and portal re-login; "
          "MySQL retained exact timers, original replay and no banked dice.", flush=True)
    return session, reentered, fresh
