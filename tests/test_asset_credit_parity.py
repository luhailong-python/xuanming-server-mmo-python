import asyncio

import pytest
from google.protobuf.unknown_fields import UnknownFieldSet

from test_scene_assets import _request, _scene, _sign
from xuanming.data.asset_inventory import FEATURE_UNAVAILABLE
from xuanming.data.asset_ledger import Outcome, find_stream


def _unknown(message):
    # Field 99, varint value 1, outside all current asset-operation messages.
    message.MergeFromString(b"\x98\x06\x01")


def _player_data(scene):
    return scene.rows.assemble(scene.locations[1].players[1].snapshot)


def _assert_no_receipt(scene):
    data = _player_data(scene)
    assert find_stream(data.asset_op_ledger, 4) is None
    assert data.currency.values[0] == 100
    assert len(data.bag_component.items) == 1


@pytest.mark.parametrize("kind", ["item_grant", "item_instance", "pet_instance"])
def test_unknown_asset_config_retries_without_receipt(tmp_path, kind):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        try:
            unknown_id = 0xFFFFFFFF
            assert assets.tables["Item"].get(unknown_id) is None
            assert assets.tables["Pet"].get(unknown_id) is None
            if kind == "item_grant":
                bundle = {"items": [{"config_id": unknown_id, "count": 1}]}
            elif kind == "item_instance":
                item = p.message("ItemComp", item_id=700, config_id=unknown_id, size=1)
                bundle = {"item_instances": [{"item_uuid": 700, "config_id": unknown_id,
                                               "stack_size": 1, "instance_proto": item.SerializeToString()}]}
            else:
                pet = p.message("PetInstance", pet_id=700, pet_table_id=unknown_id, level=1, name="pet")
                bundle = {"pet_instance": {"pet_id": 700, "pet_table_id": unknown_id, "level": 1,
                                             "name": "pet", "instance_proto": pet.SerializeToString()}}
            request = _request(p, 4, 1, 1, **bundle)
            response = await assets.credit(_sign("credit", request, "trade"))
            assert response.outcome == Outcome.RETRY
            assert response.reason.id == FEATURE_UNAVAILABLE and not response.durable
            _assert_no_receipt(scene)

            # The same sequence can be applied after the incompatible payload is resolved.
            retry = _request(p, 4, 1, 1, currencies=[{"currency_type": 0, "amount": 1}])
            applied = await assets.credit(_sign("credit", retry, "trade"))
            assert applied.outcome == Outcome.APPLIED and applied.durable
        finally:
            await scene.close()
            await assets.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("location", ["request", "bundle", "currency"])
def test_unknown_request_fields_retry_without_receipt(tmp_path, location):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        try:
            request = _request(p, 4, 1, 1, currencies=[{"currency_type": 0, "amount": 1}])
            target = {"request": request, "bundle": request.bundle,
                      "currency": request.bundle.currencies[0]}[location]
            _unknown(target)
            response = await assets.credit(_sign("credit", request, "trade"))
            assert response.outcome == Outcome.RETRY and not response.durable
            _assert_no_receipt(scene)
            clean = _request(p, 4, 1, 1, currencies=[{"currency_type": 0, "amount": 1}])
            assert (await assets.credit(_sign("credit", clean, "trade"))).outcome == Outcome.APPLIED
        finally:
            await scene.close()
            await assets.close()

    asyncio.run(exercise())


def test_invalid_bundle_is_rejected_before_unknown_field_check(tmp_path):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        try:
            request = _request(p, 4, 1, 1, currencies=[{"currency_type": 0, "amount": 0}])
            _unknown(request.bundle)
            response = await assets.credit(_sign("credit", request, "trade"))
            assert response.outcome == Outcome.REJECTED and response.durable
            assert response.reason.id == 27004
        finally:
            await scene.close()
            await assets.close()

    asyncio.run(exercise())


def test_unknown_fields_are_checked_before_unknown_config(tmp_path):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        try:
            request = _request(p, 4, 1, 1, items=[{"config_id": 0xFFFFFFFF, "count": 1}])
            _unknown(request.bundle)
            class TablesNotRead:
                def __getitem__(self, _key):
                    raise AssertionError("unknown request fields must be checked before configuration")
            assets.tables = TablesNotRead()
            response = await assets.credit(_sign("credit", request, "trade"))
            assert response.outcome == Outcome.RETRY and response.reason.id == FEATURE_UNAVAILABLE
            _assert_no_receipt(scene)
        finally:
            await scene.close()
            await assets.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("kind", ["item", "pet"])
def test_transfer_preserves_unknown_fields_inside_instance_proto(tmp_path, kind):
    async def exercise():
        p, scene, assets, _ = await _scene(tmp_path)
        try:
            if kind == "item":
                row = next(row for row in assets.tables["Item"].rows() if row.max_stack_size == 1)
                instance = p.message("ItemComp", item_id=700, config_id=row.id, size=1)
                _unknown(instance)
                bundle = {"item_instances": [{"item_uuid": 700, "config_id": row.id, "stack_size": 1,
                                               "instance_proto": instance.SerializeToString()}]}
            else:
                row = next(iter(assets.tables["Pet"].rows()))
                instance = p.message("PetInstance", pet_id=700, pet_table_id=row.id, level=1, name="pet")
                _unknown(instance)
                bundle = {"pet_instance": {"pet_id": 700, "pet_table_id": row.id, "level": 1,
                                             "name": "pet", "instance_proto": instance.SerializeToString()}}
            request = _request(p, 4, 1, 1, **bundle)
            response = await assets.credit(_sign("credit", request, "trade"))
            assert response.outcome == Outcome.APPLIED and response.durable
            data = _player_data(scene)
            if kind == "item":
                stored = next(assets.item(entry) for entry in data.bag_component.items
                              if assets.item(entry).item_id == 700)
            else:
                stored = next(pet for pet in data.pet_component.pets if pet.pet_id == 700)
            assert any(field.field_number == 99 and field.data == 1 for field in UnknownFieldSet(stored))
        finally:
            await scene.close()
            await assets.close()

    asyncio.run(exercise())
