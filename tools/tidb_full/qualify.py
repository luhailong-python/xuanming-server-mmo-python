"""Exercise SELECT-only pessimistic locking on this disposable TiDB instance."""

import asyncio
import json
import secrets

import aiomysql


async def qualify(port, run_id, output, assert_owner):
    assert_owner()
    schema = "qualification_" + run_id[:16]
    user = "qual_" + run_id[:16]
    password = secrets.token_hex(24)
    root = await aiomysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True,
                                  connect_timeout=3)
    reader = contender = None
    try:
        async with root.cursor() as cursor:
            await cursor.execute(f"CREATE DATABASE `{schema}`")
            await cursor.execute(f"CREATE TABLE `{schema}`.writer_slot (id INT PRIMARY KEY, value INT NOT NULL)")
            await cursor.execute(f"INSERT INTO `{schema}`.writer_slot VALUES (1, 0)")
            await cursor.execute(f"CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'")
            await cursor.execute(f"GRANT SELECT ON `{schema}`.writer_slot TO '{user}'@'%'")
        reader = await aiomysql.connect(host="127.0.0.1", port=port, user=user, password=password,
                                        db=schema, autocommit=False, connect_timeout=3)
        contender = await aiomysql.connect(host="127.0.0.1", port=port, user="root", db=schema,
                                           autocommit=False, connect_timeout=3)
        for conn in (reader, contender):
            async with conn.cursor() as cursor:
                await cursor.execute("SET SESSION tidb_txn_mode='pessimistic'")
                await cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        await reader.begin()
        async with reader.cursor() as cursor:
            await cursor.execute("SELECT value FROM writer_slot WHERE id=1 FOR UPDATE")
            assert (await cursor.fetchone())[0] == 0
        await contender.begin()

        async def update():
            async with contender.cursor() as cursor:
                await cursor.execute("UPDATE writer_slot SET value=1 WHERE id=1")
            await contender.commit()

        task = asyncio.create_task(update())
        try:
            done, _ = await asyncio.wait({task}, timeout=.5)
            if done:
                await task
                raise RuntimeError("TiDB SELECT-only row lock did not block a competing writer")
            await reader.rollback()
            await asyncio.wait_for(task, timeout=10)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        async with root.cursor() as cursor:
            await cursor.execute(f"SELECT value FROM `{schema}`.writer_slot WHERE id=1")
            assert (await cursor.fetchone())[0] == 1
        # A local qualification records actual observed lock semantics, not an
        # operator-issued production I3 certification.
        output.write_text(json.dumps({"format": "LOCAL_DEV_LOCK_QUALIFICATION_V1", "backend": "tidb",
                                      "lockPrimitive": "SELECT_ONLY", "run_id": run_id,
                                      "select_only_blocked_writer": True, "writer_released_after_rollback": True,
                                      "scope": "isolated integration instance; not production I3 certification"},
                                     sort_keys=True), encoding="utf-8")
    finally:
        if reader is not None: reader.close()
        if contender is not None: contender.close()
        try:
            assert_owner()
            async with root.cursor() as cursor:
                await cursor.execute(f"DROP DATABASE IF EXISTS `{schema}`")
                await cursor.execute(f"DROP USER IF EXISTS '{user}'@'%'")
        finally:
            root.close()
