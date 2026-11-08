import asyncio
from itertools import count
from time import time_ns

from test_monopoly import TOKEN, _enter, _settings
from xuanming.data import PlayerRowCodec, SqlitePlayerStore
from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes import scene_pets
from xuanming.nodes.player_inbox import BATTLE
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime, grpc_address


async def _rpc(client, calls, method, *, service="ScenePetClientPlayer", **fields):
    protocol = client.protocol
    message_id = protocol.message_ids[service + method]
    return await client.rpc(message_id, next(calls), protocol.message(protocol.routes[message_id].request_type, **fields))


async def _prepare(runtime, player_id, battle_id):
    protocol = runtime.protocol
    now = time_ns() // 1_000_000
    request = protocol.message("PrepareBattleRequest", player_id=player_id, battle_id=battle_id,
                               deadline_ms=now + 60_000, prepare_deadline_ms=now + 30_000)
    endpoint = runtime.scene.info.grpc_endpoint
    peer = GrpcClient(grpc_address(endpoint.ip, endpoint.port))
    try:
        raw = await peer.call("/scene_node.SceneNodeGrpc/PrepareBattle", request.SerializeToString())
    finally:
        await peer.close()
    response = protocol.parse("PrepareBattleResponse", raw)
    assert not response.error_message.id
    return response.snapshot, request.deadline_ms


def test_pet_rpc_lifecycle_and_durable_battle_push(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_RUN_MODE", "test")
    monkeypatch.setenv("SCENE_RUN_MODE", "test")
    monkeypatch.setattr(scene_pets, "randbelow", lambda _: 0)

    async def exercise():
        settings = _settings(tmp_path)
        calls = count(3)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                empty = await _rpc(client, calls, "GetPetList")
                assert not empty.error_message.id and not empty.pets.pets and empty.pets.max_pets == 22
                assert (await _rpc(client, calls, "GmGrantPet", pet_table_id=0)).error_message.id == 26001

                granted = await _rpc(client, calls, "GmGrantPet", pet_table_id=1)
                assert not granted.error_message.id and granted.pet_id
                pet_id = granted.pet_id
                pet = granted.pets.pets[0]
                assert (pet.pet_id, pet.level, pet.name, pet.total_points, pet.remaining_points) == (pet_id, 1, "灵狐", 5, 5)
                assert list(pet.skill_table_ids) == [14] and pet.derived.health == pet.derived.max_health
                assert [item.aptitude for item in pet.dimensions] == [8000, 10_000, 7000, 11_000]

                auto = await _rpc(client, calls, "AutoAllocatePetPoints", pet_id=pet_id)
                assert not auto.error_message.id and dict(auto.suggested) == {401: 2, 402: 0, 403: 3, 404: 0}
                fresh = await _rpc(client, calls, "GetPetList")
                assert fresh.pets.pets[0].remaining_points == 5
                invested = await _rpc(client, calls, "AllocatePetPoints", pet_id=pet_id, allocated=auto.suggested)
                assert not invested.error_message.id and invested.pets.pets[0].remaining_points == 0
                assert invested.pets.pets[0].derived.max_health > pet.derived.max_health
                assert invested.pets.pets[0].derived.physical_attack > pet.derived.physical_attack
                decreased = await _rpc(client, calls, "AllocatePetPoints", pet_id=pet_id, allocated={401: 1})
                assert decreased.error_message.id == 26010
                reset = await _rpc(client, calls, "ResetPetPoints", pet_id=pet_id)
                assert not reset.error_message.id and reset.pets.pets[0].derived == pet.derived

                assert (await _rpc(client, calls, "RenamePet", pet_id=pet_id, name="　")).error_message.id == 26007
                assert (await _rpc(client, calls, "RenamePet", pet_id=pet_id, name="小灵狐")).error_message.id == 26013
                funded = await _rpc(client, calls, "GmAddCurrency", service="SceneCurrencyClientPlayer",
                                    currency_type=0, amount=250)
                assert not funded.error_message.id
                named = await _rpc(client, calls, "RenamePet", pet_id=pet_id, name="小灵狐")
                assert not named.error_message.id and named.pets.pets[0].name == "小灵狐"
                assert (await _rpc(client, calls, "RenamePet", pet_id=pet_id, name="小灵狐")).error_message.id == 26015

                summoned = await _rpc(client, calls, "SummonPet", pet_id=pet_id)
                assert not summoned.error_message.id and summoned.pets.active_pet_id == pet_id
                assert (await _rpc(client, calls, "SummonPet", pet_id=pet_id)).error_message.id == 26004
                recalled = await _rpc(client, calls, "RecallPet")
                assert not recalled.error_message.id and not recalled.pets.active_pet_id
                assert (await _rpc(client, calls, "RecallPet")).error_message.id == 26005
                assert not (await _rpc(client, calls, "SummonPet", pet_id=pet_id)).error_message.id

                snapshot, deadline = await _prepare(runtime, player_id, 42)
                assert len(snapshot.pets) == 1 and snapshot.pets[0].pet_id == pet_id
                assert list(snapshot.pets[0].skill_table_ids) == [14]
                assert (await _rpc(client, calls, "RecallPet")).error_message.id == 26008
                protocol = client.protocol
                await runtime.scene.battle.confirm(protocol.message("BattleConfirmedEvent", player_id=player_id,
                                                                   battle_id=42, deadline_ms=deadline))
                result = protocol.message("BattleSettlementData", player_id=player_id, battle_id=42, outcome=1,
                                          health=snapshot.max_health, total_rounds=1,
                                          pets=[{"pet_id": pet_id, "health": 7, "mana": 3}])
                await runtime.local_inbox.enqueue([result])
                runtime.scene.inbox.wake(player_id, BATTLE)
                message_id = protocol.message_ids["ScenePetClientPlayerNotifyPetListChanged"]
                push = protocol.parse("PetListChangedS2C", (await client.content(message_id)).serialized_message)
                assert push.pets.active_pet_id == pet_id
                assert (push.pets.pets[0].derived.health, push.pets.pets[0].derived.mana) == (7, 3)

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, restored_id):
                assert restored_id == player_id
                saved = await _rpc(client, calls, "GetPetList")
                assert saved.pets.active_pet_id == pet_id and saved.pets.pets[0].name == "小灵狐"
                assert (saved.pets.pets[0].derived.health, saved.pets.pets[0].derived.mana) == (7, 3)
                wallet = await _rpc(client, calls, "GetCurrencyList", service="SceneCurrencyClientPlayer")
                assert wallet.currency.values[0] == 50

    asyncio.run(exercise())


def test_loaded_pet_keeps_unknown_points_and_owner_level_fence(tmp_path, monkeypatch):
    monkeypatch.setenv("GATE_RUN_MODE", "test")
    monkeypatch.delenv("SCENE_RUN_MODE", raising=False)

    async def exercise():
        settings = _settings(tmp_path)
        protocol = Protocol()
        data = protocol.message("player_database", player_id=1, persist_schema_gen=1)
        data.level_component.level = 1
        data.currency.values.append(500)
        data.pet_component.active_pet_id = 101
        data.pet_component.pets.add(pet_id=101, pet_table_id=1, level=40, health=23, mana=19,
                                    allocated={401: 1, 9999: 300})
        store = SqlitePlayerStore(settings.database)
        snapshot = await store.load_and_claim(1, b"pet-fixture")
        snapshot = await store.commit(snapshot, PlayerRowCodec(protocol).project(data), force=True)
        await store.release(snapshot)

        calls = count(3)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime) as (client, player_id):
                assert (await _rpc(client, calls, "GmGrantPet", pet_table_id=1)).error_message.id == 1006
                loaded = await _rpc(client, calls, "GetPetList")
                assert loaded.pets.active_pet_id == 101
                assert (loaded.pets.pets[0].level, loaded.pets.pets[0].remaining_points) == (40, 0)
                snapshot, _ = await _prepare(runtime, player_id, 43)
                assert not snapshot.pets
                await runtime.scene.battle.cancel(client.protocol.message("CancelBattlePrepareRequest", player_id=player_id,
                                                                           battle_id=43))
                assert not (await _rpc(client, calls, "RecallPet")).error_message.id
                assert (await _rpc(client, calls, "SummonPet", pet_id=101)).error_message.id == 26003
                assert (await _rpc(client, calls, "AllocatePetPoints", pet_id=101, allocated={9999: 301})).error_message.id == 26012
                reset = await _rpc(client, calls, "ResetPetPoints", pet_id=101)
                assert not reset.error_message.id and reset.pets.pets[0].remaining_points == 0
                role = runtime.scene.locations[player_id].players[player_id]
                saved = runtime.scene.rows.assemble(role.snapshot)
                assert dict(saved.pet_component.pets[0].allocated) == {9999: 300}
                assert saved.currency.values[0] == 200

    asyncio.run(exercise())
