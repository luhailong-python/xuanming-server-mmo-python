import asyncio

from test_monopoly import TOKEN, _enter, _gameplay, _settings
from xuanming.data import SqlitePlayerStore
from xuanming.data.player_rows import PlayerRowKind
from xuanming.nodes import monopoly
from xuanming.runtime import Runtime


# Wallet row and receipt captured through TCP from the roll-only version after a (1, 1) roll.
LEGACY_WALLET = bytes.fromhex("42050a03230000ca011a82f1041608011001180128023823482352080802101318232002")
LEGACY_ROLL = bytes.fromhex("08011001180128023823482352080802101318232002")


async def _upgrade(client, call_id, sequence, expected_level):
    return await _gameplay(client, call_id, "UpgradeLandmark", landmark_id=1,
                           expected_level=expected_level, upgrade_seq=sequence)


async def _board(client, call_id, *, level, coins, next_cost, upgrade_seq):
    board = await _gameplay(client, call_id, "GetBoard")
    landmark = next(item for item in board.landmarks if item.landmark_id == 1)
    assert landmark.name and landmark.max_level == 3
    assert (landmark.level, landmark.next_cost, board.state.coins, board.next_upgrade_seq) == (
        level, next_cost, coins, upgrade_seq,
    )
    return board


def _receipt(reply, *, sequence, previous_level, spent, coins, replayed=False):
    assert not reply.error_message.id and reply.durable and reply.replayed == replayed
    result = reply.result
    assert (result.upgrade_seq, result.landmark_id, result.previous_level, result.level) == (
        sequence, 1, previous_level, previous_level + 1,
    )
    assert (result.coins_spent, result.coins_after, result.next_upgrade_seq) == (spent, coins, sequence + 1)
    return result.SerializeToString(deterministic=True)


def _rejected(reply, error):
    assert reply.error_message.id == error and not reply.durable and not reply.replayed and not reply.HasField("result")


def test_landmark_build_upgrade_and_reopen_over_tcp(tmp_path, monkeypatch):
    draws = iter((0, 0, 0, 0, 2, 2, 0, 0))
    monkeypatch.setattr(monopoly, "randbelow", lambda _: next(draws))

    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                await _board(client, 3, level=0, coins=0, next_cost=30, upgrade_seq=1)
                _rejected(await _upgrade(client, 4, 1, 0), 27000)
                _rejected(await _upgrade(client, 5, 2, 0), 1005)
                await _board(client, 6, level=0, coins=0, next_cost=30, upgrade_seq=1)

                roll = await _gameplay(client, 7, "RollDice", roll_seq=1)
                assert roll.durable and (roll.result.to_position, roll.result.coins_credited) == (2, 35)
                build = _receipt(await _upgrade(client, 8, 1, 0), sequence=1, previous_level=0, spent=30, coins=5)
                _rejected(await _upgrade(client, 9, 1, 1), 1005)
                await _board(client, 10, level=1, coins=5, next_cost=60, upgrade_seq=2)

                roll = await _gameplay(client, 11, "RollDice", roll_seq=2)
                assert roll.durable and (roll.result.to_position, roll.result.coins_credited) == (4, 40)
                replay = await _upgrade(client, 12, 1, 0)
                assert _receipt(replay, sequence=1, previous_level=0, spent=30, coins=5, replayed=True) == build
                _rejected(await _upgrade(client, 13, 2, 1), 27000)
                await _board(client, 14, level=1, coins=45, next_cost=60, upgrade_seq=2)

                roll = await _gameplay(client, 15, "RollDice", roll_seq=3)
                assert roll.durable and (roll.result.to_position, roll.result.coins_credited) == (10, 85)
                upgraded = _receipt(await _upgrade(client, 16, 2, 1), sequence=2, previous_level=1, spent=60, coins=70)
                _rejected(await _upgrade(client, 17, 1, 0), 1005)
                _rejected(await _upgrade(client, 18, 3, 1), 1005)
                await _board(client, 19, level=2, coins=70, next_cost=120, upgrade_seq=3)

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_player_id):
                assert restored_player_id == player_id
                board = await _board(client, 3, level=2, coins=70, next_cost=120, upgrade_seq=3)
                assert (board.state.position, board.state.dice_remaining, board.state.next_roll_seq) == (10, 17, 4)
                replay = await _upgrade(client, 4, 2, 1)
                assert _receipt(replay, sequence=2, previous_level=1, spent=60, coins=70, replayed=True) == upgraded
                p = client.protocol
                wallet = await client.rpc(p.message_ids["SceneCurrencyClientPlayerGetCurrencyList"], 5,
                                          p.message("GetCurrencyListRequest"))
                assert not wallet.error_message.id and wallet.currency.values[0] == 70

                roll = await _gameplay(client, 6, "RollDice", roll_seq=4)
                assert roll.durable and (roll.result.to_position, roll.result.coins_credited) == (0, 100)
                _receipt(await _upgrade(client, 7, 3, 2), sequence=3, previous_level=2, spent=120, coins=50)
                _rejected(await _upgrade(client, 8, 4, 3), 1006)
                await _board(client, 9, level=3, coins=50, next_cost=0, upgrade_seq=4)

    asyncio.run(exercise())


def test_landmarks_accept_a_legacy_roll_only_save(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        store = SqlitePlayerStore(settings.database)
        snapshot = await store.load_and_claim(1, b"legacy-monopoly-test")
        snapshot = await store.commit(snapshot, {PlayerRowKind.WALLET: LEGACY_WALLET}, force=True)
        await store.release(snapshot)

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                assert player_id == 1
                board = await _board(client, 3, level=0, coins=35, next_cost=30, upgrade_seq=1)
                assert (board.state.position, board.state.dice_remaining, board.state.next_roll_seq) == (2, 19, 2)
                replay = await _gameplay(client, 4, "RollDice", roll_seq=1)
                assert replay.durable and replay.replayed and not replay.error_message.id
                assert replay.result.SerializeToString(deterministic=True) == LEGACY_ROLL
                _receipt(await _upgrade(client, 5, 1, 0), sequence=1, previous_level=0, spent=30, coins=5)

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, _):
                board = await _board(client, 3, level=1, coins=5, next_cost=60, upgrade_seq=2)
                assert (board.state.position, board.state.dice_remaining, board.state.next_roll_seq) == (2, 19, 2)
                replay = await _gameplay(client, 4, "RollDice", roll_seq=1)
                assert replay.durable and replay.replayed and not replay.error_message.id
                assert replay.result.SerializeToString(deterministic=True) == LEGACY_ROLL

    asyncio.run(exercise())
