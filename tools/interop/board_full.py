"""Accept Monopoly board completion through native login and MySQL storage."""

from itertools import count

from data_full import read_native_player
from landmark_full import relogin, rpc


LANDMARK_COSTS = {1: (30, 60, 120), 2: (40, 80, 160), 3: (50, 100, 200)}
BOARD_REWARD = 300
DICE_PER_BOARD = 20


def wire(message):
    return message.SerializeToString(deterministic=True)


async def native_state(runtime, player_id):
    snapshot = await read_native_player(player_id)
    data = runtime.scene.rows.assemble(snapshot)
    return snapshot, data.client_request_ledger.monopoly_landmarks, data.currency.values[0]


async def sql_completion(runtime, player_id, receipt, expected_balance):
    snapshot, progress, wallet = await native_state(runtime, player_id)
    assert progress.board_id == receipt.completion.next_board_id and not progress.levels
    assert wire(progress.last_completion) == wire(receipt.completion) and wire(progress.last_upgrade) == wire(receipt)
    assert wallet == expected_balance
    return snapshot


async def verify_completion(client, runtime, player_id, expected, *, extra_coins=0):
    receipt, roll_sequence = expected
    completion = receipt.completion
    coins = receipt.coins_after + extra_coins
    board = await rpc(client, 241, "GetBoard")
    assert board.board_id == completion.next_board_id == 2
    assert wire(board.last_completion) == wire(completion)
    assert board.state.position == 0 and board.state.dice_remaining == DICE_PER_BOARD
    assert board.state.next_roll_seq == roll_sequence and board.next_upgrade_seq == receipt.next_upgrade_seq
    assert board.state.coins == coins
    assert {item.landmark_id: item.level for item in board.landmarks} == {key: 0 for key in LANDMARK_COSTS}

    replay = await rpc(client, 242, "UpgradeLandmark", board_id=receipt.board_id, landmark_id=receipt.landmark_id,
                       expected_level=receipt.previous_level, upgrade_seq=receipt.upgrade_seq)
    assert not replay.error_message.id and replay.durable and replay.replayed
    assert wire(replay.result) == wire(receipt)
    assert await rpc(client, 243, "GetBoard") == board
    return await sql_completion(runtime, player_id, receipt, coins)


async def exercise_board(stack, session, source, gate, runtime, ingress, report):
    client, player_id = session.client, session.player_id
    calls = count(100)
    board = await rpc(client, next(calls), "GetBoard")
    assert board.board_id == 1 and board.completion_reward == BOARD_REWARD
    assert board.state.coins == 0 and board.state.dice_remaining == DICE_PER_BOARD
    assert {item.landmark_id: item.level for item in board.landmarks} == {key: 0 for key in LANDMARK_COSTS}

    total_cost = sum(sum(costs) for costs in LANDMARK_COSTS.values())
    rolled = 0
    while board.state.coins < total_cost and board.state.dice_remaining:
        roll = await rpc(client, next(calls), "RollDice", board_id=board.board_id,
                         roll_seq=board.state.next_roll_seq)
        assert not roll.error_message.id and roll.durable and not roll.replayed
        assert roll.result.state.coins == board.state.coins + roll.result.coins_credited
        rolled += 1
        board = await rpc(client, next(calls), "GetBoard")
        assert board.state.coins == roll.result.state.coins
    earned = board.state.coins
    assert earned >= total_cost, f"legitimate dice funded {earned} gold, completion requires {total_cost}"
    first_save, _, wallet = await native_state(runtime, player_id)
    assert wallet == earned
    assert rolled > 0 and board.board_id == 1
    roll_sequence = board.state.next_roll_seq

    upgrades = [(landmark_id, level, cost) for landmark_id, costs in LANDMARK_COSTS.items()
                for level, cost in enumerate(costs)]
    receipt, levels, save_sequences = None, {}, []
    for index, (landmark_id, expected_level, cost) in enumerate(upgrades, 1):
        advertised = next(item for item in board.landmarks if item.landmark_id == landmark_id)
        assert (advertised.level, advertised.next_cost) == (expected_level, cost)
        final = index == len(upgrades)
        expected_balance = board.state.coins - cost + (BOARD_REWARD if final else 0)
        upgraded = await rpc(client, next(calls), "UpgradeLandmark", board_id=board.board_id, landmark_id=landmark_id,
                             expected_level=expected_level, upgrade_seq=board.next_upgrade_seq)
        assert not upgraded.error_message.id and upgraded.durable and not upgraded.replayed
        receipt = upgraded.result
        assert (receipt.board_id, receipt.landmark_id, receipt.previous_level, receipt.level) == (
            1, landmark_id, expected_level, expected_level + 1,
        )
        assert (receipt.coins_spent, receipt.coins_after) == (cost, expected_balance)
        assert receipt.upgrade_seq == board.next_upgrade_seq and receipt.next_upgrade_seq == receipt.upgrade_seq + 1
        levels[landmark_id] = receipt.level
        if final:
            assert (receipt.completion.board_id, receipt.completion.next_board_id) == (1, 2)
            assert receipt.completion.coin_reward == receipt.completion.coins_credited == BOARD_REWARD
            snapshot = await sql_completion(runtime, player_id, receipt, expected_balance)
            await verify_completion(client, runtime, player_id, (receipt, roll_sequence))
        else:
            assert not receipt.HasField("completion")
            snapshot, progress, wallet = await native_state(runtime, player_id)
            assert dict(progress.levels) == levels and wallet == expected_balance
            assert wire(progress.last_upgrade) == wire(receipt) and not progress.HasField("last_completion")
            board = await rpc(client, next(calls), "GetBoard")
            assert board.board_id == 1 and board.state.coins == expected_balance
        assert snapshot.save_seq > (save_sequences[-1] if save_sequences else first_save.save_seq)
        save_sequences.append(snapshot.save_seq)

    completion = receipt, roll_sequence
    assert receipt.coins_after == earned - total_cost + BOARD_REWARD
    session, reentered = await relogin(stack, session, source, gate, runtime, ingress, leave_call_id=245)
    snapshot = await verify_completion(session.client, runtime, player_id, completion)
    assert not snapshot.released and snapshot.owner_epoch == reentered.snapshot.owner_epoch
    report.update(board_id=1, next_board_id=2, native_rolls=rolled, coins_earned=earned, coins_spent=total_cost,
                  landmarks_completed=levels, completion_reward=BOARD_REWARD, coins_after=receipt.coins_after,
                  completion_mysql_save_seq=save_sequences[-1], relogin_mysql_save_seq=snapshot.save_seq,
                  explicit_logout_release=True, original_ticket_relogin=True, completion_replay_verified=True)
    print(f"PASS Monopoly board 1→2 after {rolled} genuine rolls, {total_cost} gold spent and {BOARD_REWARD} "
          "rewarded once, with original ticket re-login and MySQL recovery.", flush=True)
    return session, reentered, completion
