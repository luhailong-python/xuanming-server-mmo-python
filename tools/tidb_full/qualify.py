"""Exercise SELECT-only pessimistic locking on this disposable TiDB instance."""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import secrets

import aiomysql


async def _observe_lock_wait(root, schema, task, *, timeout=10):
    """Require TiDB's own lock-wait row, rather than inferring it from elapsed time."""
    async with asyncio.timeout(timeout):
        while True:
            if task.done():
                await task
                raise RuntimeError("competing writer completed before a lock wait was observed")
            async with root.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM information_schema.DATA_LOCK_WAITS WHERE KEY_INFO LIKE %s",
                                     ("%" + schema + "%",))
                row = await cursor.fetchone()
            if row and row[0] > 0:
                return
            await asyncio.sleep(.05)


async def qualify(port, run_id, output, assert_owner, *, identity):
    assert_owner()
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise ValueError("qualification run ID must be 32 lowercase hexadecimal characters")
    if (not isinstance(identity, dict) or identity.get("run_id") != run_id
            or not re.fullmatch(r"xuanming-(?:tidb|ha)-[0-9a-f]{16}", identity.get("project", ""))
            or not re.fullmatch(r"[0-9a-f]{64}", identity.get("container", ""))
            or identity.get("sql_port") != port):
        raise ValueError("qualification requires the verified isolated-container identity")
    binding_bytes = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    binding_sha = hashlib.sha256(binding_bytes).hexdigest()
    source = Path(os.environ["XUANMING_IT_SOURCE"]).resolve(strict=True)
    source_text = (source / "go/shared/authoritysql/catalog.go").read_text(encoding="utf-8")
    matches = re.findall(r"const catalogWriterSlotBootstrapDDL = `([^`]+)`", source_text)
    if len(matches) != 1:
        raise RuntimeError("cannot identify the authoritative writer_slot DDL")
    ddl = matches[0]
    schema = "qualification_" + run_id[:16]
    user = "qual_" + run_id[:16]
    touch_user = "touch_" + run_id[:16]
    password = secrets.token_hex(24)
    touch_password = secrets.token_hex(24)
    writer_key = secrets.token_hex(16)
    root = await aiomysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True,
                                  connect_timeout=3)
    reader = toucher = contender = None
    schema_created = user_created = touch_user_created = False
    try:
        async with root.cursor() as cursor:
            await cursor.execute(f"CREATE DATABASE `{schema}`")
            schema_created = True
            await cursor.execute(f"USE `{schema}`")
            await cursor.execute(ddl)
            await cursor.executemany("INSERT INTO writer_slot (writer_key,slot,generation,writer_epoch,state,lock_touch) "
                                     "VALUES (UNHEX(%s),%s,%s,1,1,0)",
                                     [(writer_key, slot, "0" * 32) for slot in range(64)])
            await cursor.execute(f"CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'")
            user_created = True
            await cursor.execute(f"GRANT SELECT ON `{schema}`.writer_slot TO '{user}'@'%'")
            await cursor.execute(f"CREATE USER '{touch_user}'@'%' IDENTIFIED BY '{touch_password}'")
            touch_user_created = True
            await cursor.execute(f"GRANT SELECT, UPDATE (lock_touch) ON `{schema}`.writer_slot TO '{touch_user}'@'%'")
        reader = await aiomysql.connect(host="127.0.0.1", port=port, user=user, password=password,
                                        db=schema, autocommit=False, connect_timeout=3)
        contender = await aiomysql.connect(host="127.0.0.1", port=port, user="root", db=schema,
                                           autocommit=False, connect_timeout=3)
        toucher = await aiomysql.connect(host="127.0.0.1", port=port, user=touch_user, password=touch_password,
                                         db=schema, autocommit=False, connect_timeout=3)
        for conn in (reader, toucher, contender):
            async with conn.cursor() as cursor:
                for statement in ("SET SESSION tidb_snapshot=''", "SET SESSION tidb_read_consistency='strict'",
                                  "SET SESSION tidb_txn_mode='pessimistic'", "SET SESSION tidb_read_staleness=0",
                                  "SET SESSION tidb_low_resolution_tso=OFF", "SET SESSION tidb_replica_read='leader'",
                                  "SET SESSION tidb_constraint_check_in_place_pessimistic=ON"):
                    await cursor.execute(statement)
        # Both principals must fail to modify a non-lock column. LOCK_TOUCH
        # has only the one additional column privilege needed by its gate.
        for principal in (reader, toucher):
            try:
                async with principal.cursor() as cursor:
                    await cursor.execute("UPDATE writer_slot SET state=2 WHERE writer_key=UNHEX(%s) AND slot=0", (writer_key,))
            except aiomysql.Error as exc:
                if not exc.args or exc.args[0] not in (1142, 1143):
                    raise
            else:
                raise RuntimeError("lock principal unexpectedly has UPDATE privilege on state")
            finally:
                await principal.rollback()
        round_number = 0
        for isolation in ("READ COMMITTED", "REPEATABLE READ"):
            for conn in (reader, toucher, contender):
                async with conn.cursor() as cursor:
                    await cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL " + isolation)
                    await cursor.execute("SELECT @@tidb_txn_mode,@@transaction_isolation,@@tidb_read_staleness,"
                                         "@@tidb_low_resolution_tso,@@tidb_replica_read,@@tidb_read_consistency,@@tidb_snapshot")
                    values = await cursor.fetchone()
                    normalized = (values[0], values[1], int(values[2]), int(values[3]), *values[4:])
                    if normalized != ("pessimistic", isolation.replace(" ", "-"), 0, 0, "leader", "strict", ""):
                        raise RuntimeError(f"TiDB session does not match the qualification settings: {values!r}")
            for primitive, holder in (("SELECT_ONLY", reader), ("LOCK_TOUCH", toucher)):
                expected_epoch = 1 + round_number
                for slot in range(64):
                    await holder.begin()
                    async with holder.cursor() as cursor:
                        if primitive == "SELECT_ONLY":
                            await cursor.execute("SELECT generation,writer_epoch,state FROM writer_slot "
                                                 "WHERE writer_key=UNHEX(%s) AND slot=%s FOR UPDATE", (writer_key, slot))
                            if tuple(await cursor.fetchone()) != ("0" * 32, expected_epoch, 1):
                                raise RuntimeError("writer slot has an unexpected generation, epoch or state")
                        else:
                            await cursor.execute("UPDATE writer_slot SET lock_touch=1-lock_touch "
                                                 "WHERE writer_key=UNHEX(%s) AND slot=%s AND generation=%s "
                                                 "AND writer_epoch=%s AND state=1 AND lock_touch IN (0,1)",
                                                 (writer_key, slot, "0" * 32, expected_epoch))
                            if cursor.rowcount != 1:
                                raise RuntimeError("lock-touch did not acquire its writer slot")
                    await contender.begin()

                    async def update():
                        async with contender.cursor() as cursor:
                            await cursor.execute("UPDATE writer_slot SET writer_epoch=writer_epoch+1 WHERE writer_key=UNHEX(%s) AND slot=%s", (writer_key, slot))
                        await contender.commit()

                    task = asyncio.create_task(update())
                    try:
                        await _observe_lock_wait(root, schema, task)
                        await holder.rollback()
                        await asyncio.wait_for(task, timeout=10)
                    finally:
                        if not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                round_number += 1
        async with root.cursor() as cursor:
            await cursor.execute(f"SELECT COUNT(*) FROM `{schema}`.writer_slot "
                                 "WHERE writer_epoch=5 AND state=1 AND lock_touch=0")
            if (await cursor.fetchone())[0] != 64:
                raise RuntimeError("writer slot updates were not fully committed")
            await cursor.execute("SELECT VERSION(), TIDB_VERSION()")
            version = await cursor.fetchone()
        ddl_sha = hashlib.sha256(ddl.encode()).hexdigest()
        primitive_evidence = {}
        for primitive in ("SELECT_ONLY", "LOCK_TOUCH"):
            evidence = {"primitive": primitive, "slots_tested": 64,
                        "isolation_levels": ["READ COMMITTED", "REPEATABLE READ"],
                        "authoritative_ddl_sha256": ddl_sha, "physical_binding_sha256": binding_sha,
                        "lock_wait_observed": True, "writer_released_after_rollback": True}
            evidence["sha256"] = hashlib.sha256(json.dumps(evidence, sort_keys=True,
                                                          separators=(",", ":")).encode()).hexdigest()
            primitive_evidence[primitive] = evidence
        report = {"format": "LOCAL_DEV_LOCK_QUALIFICATION_V1", "backend": "tidb",
                                      "lockPrimitive": "SELECT_ONLY", "run_id": run_id,
                                      "select_only_blocked_writer": True, "writer_released_after_rollback": True,
                                      "update_denied_to_reader": True, "unrelated_update_denied_to_toucher": True,
                                      "lock_touch_rollback_preserved": True, "lock_wait_observed": True,
                                      "authoritative_ddl_sha256": ddl_sha,
                                      "physical_binding": identity, "physical_binding_sha256": binding_sha,
                                      "primitive_evidence": primitive_evidence,
                                      "slots_tested": 64, "isolation_levels": ["READ COMMITTED", "REPEATABLE READ"],
                                      "lock_primitives_tested": ["SELECT_ONLY", "LOCK_TOUCH"],
                                      "tidb_version": list(version),
                                      "scope": "isolated integration instance; not production I3 certification"}
    finally:
        if reader is not None: reader.close()
        if toucher is not None: toucher.close()
        if contender is not None: contender.close()
        try:
            assert_owner()
            async with root.cursor() as cursor:
                cleanup_errors = []
                if schema_created:
                    try:
                        await cursor.execute(f"DROP DATABASE `{schema}`")
                        await cursor.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=%s", (schema,))
                        if (await cursor.fetchone())[0] != 0:
                            raise RuntimeError("qualification schema cleanup could not be verified")
                    except Exception as exc:
                        cleanup_errors.append(exc)
                for name, created in ((user, user_created), (touch_user, touch_user_created)):
                    if created:
                        try:
                            await cursor.execute(f"DROP USER '{name}'@'%'")
                            await cursor.execute("SELECT COUNT(*) FROM mysql.user WHERE User=%s AND Host=%s", (name, "%"))
                            if (await cursor.fetchone())[0] != 0:
                                raise RuntimeError("qualification user cleanup could not be verified")
                        except Exception as exc:
                            cleanup_errors.append(exc)
                if cleanup_errors:
                    raise RuntimeError("qualification cleanup failed") from cleanup_errors[0]
        finally:
            root.close()
    report["cleanup"] = True
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, sort_keys=True)
