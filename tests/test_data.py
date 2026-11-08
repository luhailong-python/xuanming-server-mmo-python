import asyncio
import hashlib
import hmac
import shutil

import pytest

from xuanming.data import (
    ConfigTableError,
    ConfigTableRegistry,
    ConfigTables,
    DataServicePlayerStore,
    PlayerRowCodec,
    PlayerRowKind,
    PlayerSnapshot,
    SaveInDoubt,
    SqlitePlayerStore,
    StoreRejected,
)
from xuanming.protocol import Protocol


def test_config_tables_keep_source_indices_and_encoding_parity(tmp_path):
    text = ConfigTables()
    binary = ConfigTables(fmt="pb")
    assert text.content_digest == binary.content_digest
    assert len(text.tables) == 49
    for name, table in text.tables.items():
        assert table.rows() == binary[name].rows()

    # 这里的字符串键在源表中重复，C++ emplace 只保留首行。
    row = text["TestMultiKey"].get("aa", field="string_key")
    assert row.id == 1
    assert [item.id for item in text.resolve_foreign("TestMultiKey", row, "test_refs")] == [1, 2]
    row.id = 999
    assert [item.id for item in text["TestMultiKey"].find_all(1)] == [1]
    assert text["Test"].get(999) is None

    staging = tmp_path / "tables"
    shutil.copytree(text.directory, staging)
    (staging / "test.json").write_text("{}", encoding="utf-8")
    registry = ConfigTableRegistry(text)
    with pytest.raises(ConfigTableError):
        asyncio.run(registry.reload(staging))
    assert registry.snapshot is text


def test_player_rows_use_wire_groups_and_reject_unknown_new_fields():
    protocol = Protocol()
    codec = PlayerRowCodec(protocol)
    player = protocol.message("player_database", player_id=42, persist_schema_gen=1)
    player.level_component.level = 12
    player.currency.values.append(15)
    player.transform.location.x = 19.0
    rows = codec.project(player)
    assert protocol.parse("player_database", rows[PlayerRowKind.LOCATION]).transform.location.x == 19.0
    assert protocol.parse("player_database", rows[PlayerRowKind.WALLET]).currency.values == [15]
    snapshot = PlayerSnapshot(42, b"scene-1", 1, 0, rows)
    assert codec.assemble(snapshot) == player

    retired_field = b"\x52\x00"  # 退役的 10 号字段。
    assert codec.assemble(
        PlayerSnapshot(42, b"scene-1", 1, 0, {**rows, PlayerRowKind.PROFILE: retired_field})
    ) == player
    future_field = b"\x98\x06\x01"
    with pytest.raises(ValueError):
        codec.assemble(
            PlayerSnapshot(42, b"scene-1", 1, 0, {**rows, PlayerRowKind.PROFILE: future_field})
        )


def test_sqlite_snapshot_is_durable_and_old_writer_cannot_save(tmp_path):
    async def exercise():
        db = tmp_path / "data" / "player.sqlite3"
        store = SqlitePlayerStore(db)
        missing = SqlitePlayerStore(db, auto_create=False)
        with pytest.raises(StoreRejected, match="LOAD_AND_CLAIM_NO_OWNER"):
            await missing.load_and_claim(100, b"scene-1")
        player_id = (1 << 64) - 1
        snapshot = await store.load_and_claim(player_id, b"scene-1", 90)
        wallet = {PlayerRowKind.WALLET: b"new-wallet"}
        first = await store.commit(snapshot, wallet)
        assert first.save_seq == 1
        assert await store.commit(first, wallet) is first
        with pytest.raises(StoreRejected, match="SAVE_PLAYER_OLDER_SEQ"):
            await store.commit(snapshot, wallet)

        location = {**wallet, PlayerRowKind.LOCATION: b"new-location"}
        assert await store.commit(first, location, periodic=True) is first
        second = await store.commit(first, location, release=True)
        assert second.released
        assert second.rows == location
        reconnected = await store.commit(second, location)
        assert reconnected.released is False

        other_store = SqlitePlayerStore(db, auto_create=False)
        with pytest.raises(StoreRejected, match="LOAD_AND_CLAIM_NOT_HOLDER"):
            await other_store.load_and_claim(player_id, b"scene-2", 90)
        latest = await other_store.load_and_claim(player_id, b"scene-2", 91)
        assert latest.rows == location
        with pytest.raises(StoreRejected, match="SAVE_PLAYER_STALE_OWNER"):
            await store.commit(reconnected, location, force=True)

    asyncio.run(exercise())


def test_remote_saves_replay_same_sequence_and_sign_every_request():
    async def exercise():
        protocol = Protocol()
        secret = b"0123456789abcdef0123456789abcdef"
        saves = []
        available = False

        async def call(path, body):
            method = path.rsplit("/", 1)[1]
            request = protocol.parse(f"playerstore.{method}Request", body)
            auth = request.auth
            sent, signature = auth.timestamp_ms, auth.signature_hex
            assert auth.caller == "scene"
            request.ClearField("auth")
            digest = hashlib.sha256(request.SerializeToString(deterministic=True)).hexdigest()
            text = f"v1\n{path}\nscene\n{sent}\n{digest}".encode()
            assert signature == hmac.digest(secret, text, "sha256").hex()
            if method == "LoadAndClaim":
                response = protocol.message(
                    "playerstore.LoadAndClaimResponse", result=1, owner_epoch=9,
                    storage_id=12, save_seq=100, schema_gen=1,
                )
            else:
                saves.append(request)
                if not available:
                    raise OSError("模拟提交后的回包丢失")
                response = protocol.message("playerstore.SavePlayerResponse", result=2)
            return response.SerializeToString()

        store = DataServicePlayerStore(protocol, call, secret, retry_budget=0.03)
        original = await store.load_and_claim(42, b"scene-1", 9)
        rows = {PlayerRowKind.WALLET: b"payload"}
        with pytest.raises(SaveInDoubt):
            await store.commit(original, rows)
        with pytest.raises(SaveInDoubt):
            await store.load_and_claim(42, b"scene-1", 9)
        with pytest.raises(SaveInDoubt):
            await store.commit(original, {PlayerRowKind.WALLET: b"different"})
        available = True
        saved = await store.commit(original, rows)
        assert saves[0] == saves[1]
        assert saves[1].storage_id == 12
        assert saves[1].rows[0].kind == 7
        assert saved.save_seq == saves[1].save_seq
        assert saved.rows == rows

    asyncio.run(exercise())

@pytest.mark.parametrize("kind", [PlayerRowKind.WALLET, 1, 2, 5])
def test_remote_claim_accepts_empty_current_row_and_rejects_retired_layout(kind):
    async def exercise():
        protocol = Protocol()

        async def call(path, body):
            assert path == "/playerstore.PlayerStore/LoadAndClaim"
            return protocol.message(
                "playerstore.LoadAndClaimResponse", result=1, owner_epoch=9,
                storage_id=12, save_seq=100, schema_gen=1, rows=[{"kind": kind, "payload": b""}],
            ).SerializeToString()

        store = DataServicePlayerStore(protocol, call, b"scene-key" * 4)
        if kind == PlayerRowKind.WALLET:
            snapshot = await store.load_and_claim(42, b"scene-1", 9)
            assert snapshot.rows == {kind: b""}
            assert not PlayerRowCodec(protocol).assemble(snapshot).currency.values
        else:
            with pytest.raises(ValueError, match=f"玩家数据行无效: {kind}"):
                await store.load_and_claim(42, b"scene-1", 9)

    asyncio.run(exercise())


def test_detached_release_checks_existing_holder_and_uses_empty_remote_request(tmp_path):
    import sqlite3

    async def exercise():
        db = tmp_path / "players.sqlite3"
        local = SqlitePlayerStore(db)
        snapshot = await local.load_and_claim(1, b"scene", 15)
        await local.commit(snapshot, {PlayerRowKind.WALLET: b"keep"})
        assert await local.release_detached(1, b"another", 15)
        assert await local.release_detached(1, b"scene", 14)
        with sqlite3.connect(db) as conn:
            assert conn.execute("SELECT released FROM player_owner").fetchone()[0] == 0
        assert await local.release_detached(1, b"scene", 15)
        with sqlite3.connect(db) as conn:
            assert conn.execute("SELECT released FROM player_owner").fetchone()[0] == 1
            assert conn.execute("SELECT payload FROM player_row").fetchone()[0] == b"keep"

        protocol = Protocol()
        requests = []

        async def call(path, body):
            requests.append(protocol.parse("playerstore.SavePlayerRequest", body))
            assert path == "/playerstore.PlayerStore/SavePlayer"
            if len(requests) == 1:
                return protocol.message("playerstore.SavePlayerResponse", result=6).SerializeToString()
            raise OSError("lost reply")

        remote = DataServicePlayerStore(protocol, call, b"scene-key" * 4)
        assert await remote.release_detached(1, b"scene", 15)
        assert not await remote.release_detached(1, b"scene", 15)
        assert len(requests) == 2
        assert requests[0].storage_id == 0 and not requests[0].rows
        assert requests[0].release and requests[0].owner_epoch == 15
        assert requests[0].instance_id == b"scene" and requests[0].auth.caller == "scene"

    asyncio.run(exercise())
