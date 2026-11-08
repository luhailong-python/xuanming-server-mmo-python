import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

from xuanming.nodes import monopoly
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect


TOKEN = "monopoly-test-token"


def _settings(tmp_path):
    defaults = Settings()
    return replace(
        defaults, database=tmp_path / "players.sqlite3",
        gate=replace(defaults.gate, port=0), scene=replace(defaults.scene, port=0, grpc_port=0),
        battle=replace(defaults.battle, port=0, grpc_port=0),
    )


@asynccontextmanager
async def _enter(runtime, account="local"):
    p, gate = runtime.protocol, runtime.gate
    async with connect(p, gate.info.endpoint) as client:
        await client.send(gate.issue_ticket())
        assert (await client.receive("ClientTokenVerifyResponse")).success
        login = await client.rpc(p.message_ids["ClientPlayerLoginLogin"], 1, p.message(
            "loginpb.LoginRequest", account=account, password=TOKEN,
        ))
        assert not login.error_message.id
        player_id = login.players[0].player.player_id
        entered = await client.rpc(p.message_ids["ClientPlayerLoginEnterGame"], 2, p.message(
            "loginpb.EnterGameRequest", player_id=player_id, request_id="monopoly-test-enter",
        ))
        assert entered.player_id == player_id and not entered.error_message.id
        await client.content(p.message_ids["SceneSceneClientPlayerNotifyEnterScene"])
        yield client, player_id


async def _gameplay(client, call_id, method, **fields):
    p = client.protocol
    message_id = p.message_ids[f"MonopolyClientPlayer{method}"]
    request = p.message(p.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


def test_monopoly_tcp_rewards_and_latest_roll_survive_restart(tmp_path, monkeypatch):
    draws = iter((0, 0, 4, 3, 0, 0))
    calls = []

    def roll(upper):
        calls.append(upper)
        return next(draws)

    monkeypatch.setattr(monopoly, "randbelow", roll)

    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                board = await _gameplay(client, 3, "GetBoard")
                rewards = {tile.position: tile.coin_reward for tile in board.tiles}
                assert set(rewards) == set(range(12)) and len(board.tiles) == 12
                assert rewards[2] == 35 and board.pass_start_reward == 100
                assert (board.state.position, board.state.dice_remaining, board.state.next_roll_seq) == (0, 20, 1)

                future = await _gameplay(client, 4, "RollDice", roll_seq=2)
                assert future.error_message.id == 1005 and not future.durable and not calls
                assert (await _gameplay(client, 5, "GetBoard")).state == board.state

                first = await _gameplay(client, 6, "RollDice", roll_seq=1)
                assert not first.error_message.id and first.durable and not first.replayed
                result = first.result
                assert (result.roll_seq, result.die_one, result.die_two) == (1, 1, 1)
                assert (result.from_position, result.to_position, result.passed_start) == (0, 2, False)
                assert (result.landing_reward, result.pass_start_reward, result.coins_credited) == (35, 0, 35)
                assert (result.state.position, result.state.dice_remaining, result.state.next_roll_seq) == (2, 19, 2)
                assert result.state.coins == board.state.coins + 35

                same = await _gameplay(client, 7, "RollDice", roll_seq=1)
                assert not same.error_message.id and same.replayed and same.durable and same.result == result
                assert calls == [6, 6]

                second = await _gameplay(client, 8, "RollDice", roll_seq=2)
                assert not second.error_message.id and second.durable and second.result.to_position == 11
                wrapped = await _gameplay(client, 9, "RollDice", roll_seq=3)
                assert not wrapped.error_message.id and wrapped.durable and not wrapped.replayed
                result = wrapped.result
                assert (result.from_position, result.to_position, result.passed_start) == (11, 1, True)
                assert (result.landing_reward, result.pass_start_reward) == (rewards[1], 100)
                assert result.coins_credited == 100 + rewards[1]
                assert (result.state.position, result.state.dice_remaining, result.state.next_roll_seq) == (1, 17, 4)
                assert result.state.coins == second.result.state.coins + result.coins_credited
                assert (await _gameplay(client, 10, "GetBoard")).state == result.state

        saved_result = result.SerializeToString(deterministic=True)
        saved_state = result.state.SerializeToString(deterministic=True)
        starting_state = board.state.SerializeToString(deterministic=True)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_player_id):
                assert restored_player_id == player_id
                restored = await _gameplay(client, 3, "GetBoard")
                assert restored.state.SerializeToString(deterministic=True) == saved_state
                stale = await _gameplay(client, 4, "RollDice", roll_seq=1)
                assert stale.error_message.id == 1005 and not stale.durable
                replay = await _gameplay(client, 5, "RollDice", roll_seq=3)
                assert not replay.error_message.id and replay.replayed and replay.durable
                assert replay.result.SerializeToString(deterministic=True) == saved_result
                unchanged = await _gameplay(client, 6, "GetBoard")
                assert unchanged.state.SerializeToString(deterministic=True) == saved_state
                p = client.protocol
                wallet = await client.rpc(p.message_ids["SceneCurrencyClientPlayerGetCurrencyList"], 7,
                                          p.message("GetCurrencyListRequest"))
                assert not wallet.error_message.id and wallet.currency.values[0] == result.state.coins

            async with _enter(runtime, "second-monopoly-player") as (client, other_player_id):
                assert other_player_id != player_id
                fresh = await _gameplay(client, 3, "GetBoard")
                assert fresh.state.SerializeToString(deterministic=True) == starting_state
        assert calls == [6] * 6

    asyncio.run(exercise())


def test_monopoly_retries_a_committed_roll_after_losing_the_save_reply(tmp_path, monkeypatch):
    draws = []

    def roll(upper):
        draws.append(upper)
        return 0

    monkeypatch.setattr(monopoly, "randbelow", roll)

    async def exercise():
        async with Runtime(_settings(tmp_path), "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, _):
                before = (await _gameplay(client, 3, "GetBoard")).state
                store = runtime.scene.store
                write = store._write
                attempts = []

                def lose_first_reply(snapshot, changes, save_seq, release):
                    write(snapshot, changes, save_seq, release)
                    attempts.append((save_seq, dict(changes)))
                    if len(attempts) == 1:
                        raise OSError("lost the reply after committing the SQLite transaction")

                monkeypatch.setattr(store, "_write", lose_first_reply)
                p = client.protocol
                roll_id = p.message_ids["MonopolyClientPlayerRollDice"]
                request = p.message("monopoly.RollDiceRequest", roll_seq=1)
                await client.send(p.message("ClientRequest", id=4, message_id=roll_id, body=request.SerializeToString()))
                missed = await client.content(roll_id, 4)
                assert missed.error_message.id == 1003 and not missed.serialized_message
                assert len(attempts) == 1 and draws == [6, 6]

                recovered = await _gameplay(client, 5, "RollDice", roll_seq=1)
                assert not recovered.error_message.id and recovered.replayed and recovered.durable
                assert attempts == [attempts[0], attempts[0]] and draws == [6, 6]
                after = (await _gameplay(client, 6, "GetBoard")).state
                assert after == recovered.result.state
                assert (after.position, after.dice_remaining, after.next_roll_seq) == (2, 19, 2)
                assert after.coins == before.coins + 35

    asyncio.run(exercise())
