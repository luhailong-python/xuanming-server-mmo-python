"""Native Go DB acceptance of the Python-only Monopoly landmark rules."""

import asyncio
from dataclasses import replace

from data_full import read_native_player
from login_full import authenticate, connect_assigned_gate, enter_game
from scene_full import entry


LANDMARK = 1
COSTS = (30, 60)


async def rpc(client, call_id, method, **fields):
    protocol = client.protocol
    message_id = protocol.message_ids[f"MonopolyClientPlayer{method}"]
    request = protocol.message(protocol.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


def landmark(board):
    return next(item for item in board.landmarks if item.landmark_id == LANDMARK)


async def sql_landmark(runtime, player_id, level, coins, receipt):
    snapshot = await read_native_player(player_id)
    saved = runtime.scene.rows.assemble(snapshot)
    state = saved.client_request_ledger.monopoly_landmarks
    assert state.levels[LANDMARK] == level
    assert saved.currency.values[0] == coins
    assert state.last_upgrade.SerializeToString(deterministic=True) == receipt.SerializeToString(deterministic=True)
    return snapshot


async def verify_reentry(client, runtime, player_id, receipt, *, extra_coins=0):
    expected_balance = receipt.coins_after + extra_coins
    board = await rpc(client, 71, "GetBoard")
    assert landmark(board).level == receipt.level
    assert landmark(board).next_cost == 120
    assert board.next_upgrade_seq == receipt.next_upgrade_seq
    assert board.state.coins == expected_balance
    replay = await rpc(client, 72, "UpgradeLandmark", landmark_id=LANDMARK,
                       expected_level=receipt.previous_level, upgrade_seq=receipt.upgrade_seq)
    assert not replay.error_message.id and replay.durable and replay.replayed
    assert replay.result.SerializeToString(deterministic=True) == receipt.SerializeToString(deterministic=True)
    after = await rpc(client, 73, "GetBoard")
    assert after == board
    return await sql_landmark(runtime, player_id, receipt.level, expected_balance, receipt)


async def relogin(stack, session, source, gate, runtime, ingress, *, leave_call_id, after_logout=None):
    client, player_id = session.client, session.player_id
    protocol = client.protocol
    await client.rpc(protocol.message_ids["ClientPlayerLoginLeaveGame"], leave_call_id,
                     protocol.message("loginpb.LeaveGameRequest"))
    async with asyncio.timeout(18):
        while player_id in runtime.scene.locations or not (await read_native_player(player_id)).released:
            await asyncio.sleep(0.1)
    client.writer.close()
    await client.writer.wait_closed()
    if after_logout is not None:
        after_logout()

    endpoint = gate.gate.info.client_endpoint
    new_client = await stack.enter_async_context(connect_assigned_gate(protocol, endpoint, session.account,
                                                                       access_token=session.access_token, ingress=ingress))
    account = await authenticate(new_client, session.access_token)
    assert any(item.player.player_id == player_id for item in account.players)
    await enter_game(new_client, player_id)
    reentered = await entry(new_client, runtime, player_id)
    assert reentered is not source and reentered.session_id != source.session_id
    assert reentered.snapshot.owner_epoch >= source.snapshot.owner_epoch
    return replace(session, client=new_client), reentered


async def exercise_landmarks(stack, session, source, gate, runtime, ingress, report):
    """Earn gold, build twice, logout, obtain a fresh ticket, and read the original records."""
    client, player_id = session.client, session.player_id
    board = await rpc(client, 40, "GetBoard")
    assert board.state.coins == 0
    assert landmark(board).level == 0 and landmark(board).next_cost == COSTS[0]
    rolls = []
    for index in range(12):
        if board.state.coins >= sum(COSTS):
            break
        roll = await rpc(client, 41 + index * 2, "RollDice", roll_seq=board.state.next_roll_seq)
        assert not roll.error_message.id and roll.durable and not roll.replayed
        assert roll.result.state.coins == board.state.coins + roll.result.coins_credited
        rolls.append(roll.result)
        board = await rpc(client, 42 + index * 2, "GetBoard")
        assert board.state.coins == roll.result.state.coins
    assert rolls and board.state.coins >= sum(COSTS)
    initial_coins = board.state.coins
    initial_sql = await read_native_player(player_id)
    assert runtime.scene.rows.assemble(initial_sql).currency.values[0] == initial_coins

    sequences = []
    debited = 0
    receipt = None
    for expected_level, cost in enumerate(COSTS):
        assert landmark(board).level == expected_level and landmark(board).next_cost == cost
        upgraded = await rpc(client, 65 + expected_level * 2, "UpgradeLandmark", landmark_id=LANDMARK,
                             expected_level=expected_level, upgrade_seq=board.next_upgrade_seq)
        assert not upgraded.error_message.id and upgraded.durable and not upgraded.replayed
        receipt = upgraded.result
        debited += cost
        assert receipt.upgrade_seq == board.next_upgrade_seq
        assert (receipt.landmark_id, receipt.previous_level, receipt.level) == (LANDMARK, expected_level, expected_level + 1)
        assert receipt.coins_spent == cost and receipt.coins_after == initial_coins - debited
        assert receipt.next_upgrade_seq == receipt.upgrade_seq + 1
        snapshot = await sql_landmark(runtime, player_id, receipt.level, receipt.coins_after, receipt)
        assert snapshot.save_seq > (sequences[-1] if sequences else initial_sql.save_seq)
        sequences.append(snapshot.save_seq)
        board = await rpc(client, 66 + expected_level * 2, "GetBoard")
        assert board.state.coins == receipt.coins_after and board.next_upgrade_seq == receipt.next_upgrade_seq

    session, reentered = await relogin(stack, session, source, gate, runtime, ingress, leave_call_id=69)
    snapshot = await verify_reentry(session.client, runtime, player_id, receipt)
    assert not snapshot.released and snapshot.owner_epoch == reentered.snapshot.owner_epoch

    report.update(landmark_id=LANDMARK, earned_rolls=len(rolls), coins_earned=initial_coins,
                  levels=[1, 2], debits=list(COSTS), final_coins=receipt.coins_after,
                  mysql_save_seq=sequences, explicit_logout_release=True, original_ticket_relogin=True,
                  persisted_replay_verified=True)
    print(f"PASS Monopoly landmark {LANDMARK} levels 1 and 2 from {len(rolls)} live rolls, "
          f"{debited} gold debited exactly once and recovered after fresh Go Login and MySQL claim.", flush=True)
    return session, reentered, receipt
