import asyncio
from itertools import count

from test_monopoly import TOKEN, _enter, _gameplay, _settings
from xuanming.data import SqlitePlayerStore
from xuanming.data.player_rows import PlayerRowKind
from xuanming.nodes import monopoly
from xuanming.runtime import Runtime


# Captured through TCP from the landmark-only version after maxing all three landmarks.
LEGACY_WALLET = bytes.fromhex(
    "42050a033c0000ca014982f1041e080510061806200b280b30013864406448c8015209080b100f1884072006"
    "8af104230a04080310030a04080210030a0408011003120f080910031802200328c801303c380a"
)
LEGACY_UPGRADE = bytes.fromhex("080910031802200328c801303c380a")


def _wire(message):
    return message.SerializeToString(deterministic=True)


def _levels(board):
    return {landmark.landmark_id: landmark.level for landmark in board.landmarks}


def _second_board(board):
    assert not board.error_message.id
    assert (board.board_id, board.board_name, board.completion_reward) == (2, "海港小镇", 450)
    assert _levels(board) == {1: 0, 2: 0, 3: 0}
    assert (board.state.position, board.state.dice_remaining, board.state.coins) == (0, 20, 360)
    assert (board.state.next_roll_seq, board.next_upgrade_seq) == (6, 10)
    assert board.HasField("last_completion")
    completion = board.last_completion
    assert (completion.board_id, completion.next_board_id, completion.coin_reward, completion.coins_credited) == (
        1, 2, 300, 300,
    )
    return _wire(completion)


def _terminal_board(board):
    assert not board.error_message.id and board.board_id == 2 and _levels(board) == {1: 3, 2: 3, 3: 3}
    assert (board.state.position, board.state.dice_remaining, board.state.coins) == (11, 16, 580)
    assert (board.state.next_roll_seq, board.next_upgrade_seq) == (10, 19)
    completion = board.last_completion
    assert (completion.board_id, completion.next_board_id, completion.coin_reward, completion.coins_credited) == (
        2, 0, 450, 450,
    )
    return _wire(completion)


async def _buy_upgrades(client, calls, board_id, seq, coins, prices):
    for landmark, start_level, costs in prices:
        for level, cost in enumerate(costs, start_level):
            upgraded = await _gameplay(client, next(calls), "UpgradeLandmark", board_id=board_id, landmark_id=landmark,
                                       upgrade_seq=seq, expected_level=level)
            coins -= cost
            assert not upgraded.error_message.id and upgraded.durable and not upgraded.replayed
            assert not upgraded.result.HasField("completion")
            assert (upgraded.result.board_id, upgraded.result.level, upgraded.result.coins_spent,
                    upgraded.result.coins_after, upgraded.result.next_upgrade_seq) == (
                board_id, level + 1, cost, coins, seq + 1,
            )
            seq += 1
    return seq, coins


async def _finish_first_board(client):
    calls = count(3)
    for seq, reward in enumerate((100, 200, 200, 200, 200), 1):
        rolled = await _gameplay(client, next(calls), "RollDice", board_id=1, roll_seq=seq)
        assert not rolled.error_message.id and rolled.durable and not rolled.replayed
        assert (rolled.result.to_position, rolled.result.coins_credited) == (11, reward)

    assert await _buy_upgrades(client, calls, 1, 1, 900, (
        (1, 0, (30, 60, 120)), (2, 0, (40, 80, 160)), (3, 0, (50, 100)),
    )) == (9, 260)

    before = await _gameplay(client, next(calls), "GetBoard")
    assert before.board_id == 1 and before.completion_reward == 300
    assert not before.HasField("last_completion") and _levels(before) == {1: 3, 2: 3, 3: 2}
    assert (before.state.position, before.state.dice_remaining, before.state.coins) == (11, 15, 260)
    assert (before.state.next_roll_seq, before.next_upgrade_seq) == (6, 9)

    request = {"board_id": 1, "landmark_id": 3, "expected_level": 2, "upgrade_seq": 9}
    final = await _gameplay(client, next(calls), "UpgradeLandmark", **request)
    assert not final.error_message.id and final.durable and not final.replayed
    result = final.result
    assert (result.board_id, result.landmark_id, result.previous_level, result.level) == (1, 3, 2, 3)
    assert (result.coins_spent, result.coins_after, result.next_upgrade_seq) == (200, 360, 10)
    assert result.HasField("completion")

    replay = await _gameplay(client, next(calls), "UpgradeLandmark", **request)
    assert not replay.error_message.id and replay.durable and replay.replayed and replay.result == result
    wrong_board = await _gameplay(client, next(calls), "UpgradeLandmark", **(request | {"board_id": 2}))
    assert wrong_board.error_message.id == 1005 and not wrong_board.durable
    stale_roll = await _gameplay(client, next(calls), "RollDice", board_id=1, roll_seq=6)
    assert stale_roll.error_message.id == 1005 and not stale_roll.durable

    board = await _gameplay(client, next(calls), "GetBoard")
    assert _second_board(board) == _wire(result.completion)
    assert [(tile.kind, tile.coin_reward) for tile in board.tiles] != [
        (tile.kind, tile.coin_reward) for tile in before.tiles
    ]
    return request, _wire(result), _wire(result.completion)


async def _finish_last_board(client):
    calls = count(9)
    for seq, reward in enumerate((120, 220, 220), 7):
        roll = await _gameplay(client, next(calls), "RollDice", board_id=2, roll_seq=seq)
        assert not roll.error_message.id and roll.durable and not roll.replayed
        assert (roll.result.to_position, roll.result.coins_credited) == (11, reward)

    assert await _buy_upgrades(client, calls, 2, 11, 940, (
        (1, 1, (60, 120)), (2, 0, (40, 80, 160)), (3, 0, (50, 100)),
    )) == (18, 330)
    request = {"board_id": 2, "landmark_id": 3, "expected_level": 2, "upgrade_seq": 18}
    final = await _gameplay(client, next(calls), "UpgradeLandmark", **request)
    assert not final.error_message.id and final.durable and not final.replayed
    assert (final.result.coins_spent, final.result.coins_after) == (200, 580)
    capped = await _gameplay(client, next(calls), "UpgradeLandmark", board_id=2, landmark_id=3,
                             upgrade_seq=19, expected_level=3)
    assert capped.error_message.id == 1006 and not capped.durable
    assert _terminal_board(await _gameplay(client, next(calls), "GetBoard")) == _wire(final.result.completion)
    return request, _wire(final.result)


def test_full_landmarks_reward_and_unlock_the_next_board_over_tcp(tmp_path, monkeypatch):
    # Land and collect laps on tile 11 of each board, with an initial tile 2 roll on board 2.
    draws = iter((4, 5) + (5, 5) * 4 + (0, 0, 3, 4) + (5, 5) * 2)
    monkeypatch.setattr(monopoly, "randbelow", lambda _: next(draws))

    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                request, receipt, completion = await _finish_first_board(client)

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_id):
                assert restored_id == player_id
                board = await _gameplay(client, 3, "GetBoard")
                assert _second_board(board) == completion
                replay = await _gameplay(client, 4, "UpgradeLandmark", **request)
                assert not replay.error_message.id and replay.durable and replay.replayed
                assert _wire(replay.result) == receipt

                p = client.protocol
                wallet = await client.rpc(p.message_ids["SceneCurrencyClientPlayerGetCurrencyList"], 5,
                                          p.message("GetCurrencyListRequest"))
                assert not wallet.error_message.id and wallet.currency.values[0] == 360

                rolled = await _gameplay(client, 6, "RollDice", board_id=2, roll_seq=6)
                assert not rolled.error_message.id and rolled.durable and not rolled.replayed
                expected_reward = next(tile.coin_reward for tile in board.tiles if tile.position == 2)
                assert expected_reward == 50
                assert (rolled.result.from_position, rolled.result.to_position, rolled.result.coins_credited) == (
                    0, 2, expected_reward,
                )
                assert (rolled.result.state.dice_remaining, rolled.result.state.next_roll_seq) == (19, 7)

                lighthouse = next(landmark for landmark in board.landmarks if landmark.landmark_id == 1)
                assert (lighthouse.name, lighthouse.next_cost) == ("灯塔", 30)
                built = await _gameplay(client, 7, "UpgradeLandmark", board_id=2, landmark_id=1,
                                        upgrade_seq=10, expected_level=0)
                assert not built.error_message.id and built.durable and not built.replayed
                assert (built.result.board_id, built.result.level, built.result.coins_spent, built.result.coins_after) == (
                    2, 1, 30, 380,
                )
                assert not built.result.HasField("completion")
                progressed = await _gameplay(client, 8, "GetBoard")
                assert progressed.board_id == 2 and _levels(progressed) == {1: 1, 2: 0, 3: 0}
                assert _wire(progressed.last_completion) == completion
                final_request, final_receipt = await _finish_last_board(client)

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_id):
                assert restored_id == player_id
                replay = await _gameplay(client, 3, "UpgradeLandmark", **final_request)
                assert not replay.error_message.id and replay.durable and replay.replayed
                assert _wire(replay.result) == final_receipt
                assert _terminal_board(await _gameplay(client, 4, "GetBoard")) == _wire(replay.result.completion)

    asyncio.run(exercise())


def test_legacy_max_landmarks_complete_once_and_preserve_the_old_receipt(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        store = SqlitePlayerStore(settings.database)
        snapshot = await store.load_and_claim(1, b"legacy-monopoly-boards")
        snapshot = await store.commit(snapshot, {PlayerRowKind.WALLET: LEGACY_WALLET}, force=True)
        await store.release(snapshot)

        completion = None
        for _ in range(2):
            async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
                async with _enter(runtime) as (client, player_id):
                    assert player_id == 1
                    observed = _second_board(await _gameplay(client, 3, "GetBoard"))
                    if completion is not None:
                        assert observed == completion
                    completion = observed
                    replay = await _gameplay(client, 4, "UpgradeLandmark", landmark_id=3, upgrade_seq=9, expected_level=2)
                    assert not replay.error_message.id and replay.durable and replay.replayed
                    assert _wire(replay.result) == LEGACY_UPGRADE
                    assert _second_board(await _gameplay(client, 5, "GetBoard")) == observed

    asyncio.run(exercise())
