"""Adapt the isolated interoperability chain to a verified disposable TiDB."""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
import uuid

if not __debug__:
    raise RuntimeError("the integration must run with assertions enabled")

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "tools/interop"), str(ROOT / "tools/tidb_interop"), str(Path(__file__).parent)]
from safety import assert_exclusive_tidb
from qualify import qualify


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scene-drain", action="store_true", help="exercise same-zone Scene planned shutdown")
    return parser.parse_args(argv)


def smoke_options(args):
    return dict(monopoly_board=True, scene_landing=True, scene_team=True, scene_drain=args.scene_drain)


def main(argv=None):
    args = parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    import smoke_original as original

    port = int(os.environ["XUANMING_TIDB_FULL_PORT"])
    project = os.environ["XUANMING_TIDB_FULL_PROJECT"]
    container = os.environ["XUANMING_TIDB_FULL_CONTAINER"]
    run_id = os.environ["XUANMING_TIDB_FULL_RUN_ID"]
    assert_owner = lambda: assert_exclusive_tidb(port, project, container, run_id)
    assert_owner()
    original.ENV.update(MMORPG_SQL_BACKEND="tidb", MMORPG_SQL_REQUIRE_TIDB="true")

    def sql(query):
        assert_owner()
        result = subprocess.run([str(original.MYSQL / "bin/mysql.exe"), "--no-defaults", "--protocol=TCP",
                                 "--host=127.0.0.1", f"--port={port}", "--user=root"],
                                cwd=original.RUN, env=original.ENV, input=query.encode(),
                                capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW, timeout=30)
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace"))
        return result.stdout

    proof = original.RUN / "tidb-lock-qualification.json"

    async def bootstrap(_processes):
        version = await asyncio.to_thread(sql, "SELECT VERSION(), TIDB_VERSION();")
        if b"tidb" not in version.lower() or b"Store: tikv" not in version:
            raise RuntimeError("SQL endpoint is not isolated TiDB with TiKV storage")
        (original.RUN / "tidb-version.txt").write_bytes(version)
        check = await asyncio.to_thread(sql, "SET GLOBAL tidb_enable_check_constraint=ON; "
                                        "SELECT @@GLOBAL.tidb_enable_check_constraint;")
        if check.decode().splitlines()[-1].strip().upper() not in ("1", "ON"):
            raise RuntimeError("isolated TiDB did not enable CHECK constraint enforcement")
        await qualify(port, run_id, proof, assert_owner, identity={"run_id": run_id, "project": project,
                                                                   "container": container, "sql_port": port})
        ddl = (ROOT / "config/portal_schema.sql").read_text(encoding="utf-8")
        await asyncio.to_thread(sql, "CREATE DATABASE interop; USE interop;\n" + ddl +
                                "\nINSERT INTO zone_config(zone_id,name,recommended) VALUES(1,'zone-1',1);")

    # Install the backend before importing modules that bind these symbols.
    original.run_mysql, original.mysql = sql, bootstrap
    import data_full

    original_base_configs = data_full._base_configs

    def tidb_backend(value):
        result, count = re.subn(r"(?m)^([ \t]*Backend:)[ \t]*mysql[ \t]*$", r"\1 tidb", value)
        if count != 1:
            raise RuntimeError("unexpected SQL backend configuration")
        return result

    def base_configs():
        values = original_base_configs()
        return tuple(tidb_backend(value) for value in values)

    data_full._base_configs = base_configs

    async def migrate(processes):
        data_full.configure_data_env()
        schemas = (data_full.GLOBAL_DB, data_full.LEDGER_DB, data_full.ZONE_DB, data_full.PLAYER_DB)
        await asyncio.to_thread(sql, "\n".join(
            f"CREATE DATABASE `{name}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            for name in schemas))
        db, data = base_configs()
        env = original.ENV | {"MMORPG_SQL_SCHEMA_PRINCIPAL": "root@%"}
        db_config = data_full._config_file("db-migrate.yaml", db, user="root", password="", db=True)
        data_config = data_full._config_file("data-migrate.yaml", data, user="root", password="", db=False)
        processes.start("db-migrate-player", [data_full._current_exe("db_migrate"), "-f", db_config,
                                              "-command", "up", "-storage-id", data_full.STORAGE_ID], env=env)
        processes.start("data-migrate", [data_full._current_exe("data_service"), "-f", data_config, "-migrate"], env=env)
        await asyncio.gather(*(processes.finish(name, timeout=180) for name in ("db-migrate-player", "data-migrate")))
        return db, data

    data_full._migrate = migrate

    def register_writer(service, config_path, schema_privileges, lock_schemas, bind_sql, runtime_credentials):
        data_full._identifier(service)
        private = original.RUN / f"writer-private-{service}"
        private.mkdir()
        identity = data_full._command(["whoami.exe", "/user", "/fo", "csv", "/nh"], env={}).stdout
        sid = identity.decode().strip().split(",")[-1].strip('"')
        if not sid.startswith("S-1-"):
            raise RuntimeError("current Windows SID unavailable")
        data_full._checked(f"{service}-private-acl", ["icacls.exe", private, "/inheritance:r", "/grant:r",
                           f"*{sid}:(OI)(CI)F", "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F", "/Q"])
        inputs, outputs, consume = (private / name for name in ("input", "output", "consume"))
        for folder in (inputs, outputs, consume): folder.mkdir()
        registrar, registrar_password = f"it_{service}_reg", secrets.token_hex(24)
        username, password = runtime_credentials or (f"it_{service}_run", secrets.token_hex(24))
        data_full._identifier(username)
        if not re.fullmatch(r"[A-Za-z0-9_-]{24,128}", password):
            raise ValueError("invalid synthetic writer password")
        operator = (original.SOURCE / "deploy/sql/authority_operator_role.sql").read_text(encoding="utf-8")
        runtime = (original.SOURCE / "deploy/sql/authority_runtime_role.sql").read_text(encoding="utf-8")
        grants = [f"CREATE USER '{registrar}'@'%' IDENTIFIED BY '{registrar_password}';",
                  f"CREATE USER '{username}'@'%' IDENTIFIED BY '{password}';"]
        for schema, tables in schema_privileges.items():
            data_full._identifier(schema)
            grants.extend((data_full._grant_template(operator, schema, "{{REGISTRAR_ACCOUNT}}", registrar),
                           data_full._grant_template(runtime, schema, "{{RUNTIME_ACCOUNT}}", username)))
            for table, privileges in tables.items():
                if not set(privileges.split(",")) <= set(data_full.DML.split(",")):
                    raise ValueError("invalid SQL writer privilege")
                grants.append(f"GRANT {privileges} ON `{schema}`.`{data_full._identifier(table)}` TO '{username}'@'%';")
        sql("\n".join(grants))
        physical = data_full._sha(f"LOCAL_TIDB_AUTHORITY_V1\nendpoint=127.0.0.1:{port}\nrun={run_id}\n".encode())
        qualification = inputs / "lock-qualification.original"
        qualification.write_bytes(proof.read_bytes())
        config = (config_path.read_bytes() if bind_sql is None else
                  data_full._config_credentials(config_path.read_text(encoding="utf-8"), username, password,
                                                db=bind_sql == "db").encode())
        (inputs / "runtime.yaml").write_bytes(config)
        requests = []
        for schema in schema_privileges:
            generation = ""
            for _ in range(20):
                rows = sql(f"SELECT generation FROM `{schema}`.route_meta WHERE id=1;").decode().splitlines()
                if len(rows) == 2:
                    generation = rows[1]
                    break
                time.sleep(.1)
            if not re.fullmatch(r"[a-f0-9]{32}", generation):
                raise RuntimeError(f"missing native writer generation in {schema}")
            requests.append({"kind": "REGISTER", "writer": {"generation": generation,
                            "sqlPrincipal": data_full._b64(f"{username}@%".encode()), "state": "ACTIVE"},
                            "authoritySchema": schema, "protectedSchemas": [schema],
                            "deploymentDigest": data_full._b64(physical), "lockPrimitive": "SELECT_ONLY",
                            "lockQualificationSha256": data_full._b64(data_full._sha(qualification.read_bytes()))})
        roster = inputs / "roster.json"
        roster.write_text(json.dumps({"format": "SQL_WRITER_BOOT_ROSTER_V2", "entries": [{"requests": requests,
                          "runtimeConfigPath": "runtime.yaml", "runtimeConfigSha256": data_full._b64(data_full._sha(config))}]}),
                          encoding="utf-8")
        grant, pinned = outputs / "boot-grant.json", outputs / "runtime.yaml"
        env = {"MMORPG_SQL_BACKEND": "tidb", "MMORPG_SQL_REQUIRE_TIDB": "true",
               "MMORPG_SQL_REGISTRAR_DSN": f"{registrar}:{registrar_password}@tcp(127.0.0.1:{port})/{next(iter(schema_privileges))}?parseTime=true",
               "MMORPG_SQL_OPERATOR_PRINCIPAL": f"{registrar}@%", "MMORPG_SQL_PHYSICAL_AUTHORITY_SHA256": physical.hex()}
        data_full._checked(f"{service}-register-writer", [original.GO / "writer_authority.exe", "-command", "register",
                           "-pod-name", f"{service}-0", "-roster", roster, "-roster-sha256", data_full._sha(roster.read_bytes()).hex(),
                           "-input-root", inputs, "-lock-qualification", qualification, "-boot-output", grant,
                           "-deployment-id", f"interop-{service}-{run_id}", "-runtime-config-output", pinned], env=env)
        helper = Path(os.environ["XUANMING_IT_PRIVATE_HELPER"])
        if data_full._sha(pinned.read_bytes()) != data_full._sha(config):
            raise RuntimeError("writer runtime config drift")
        return {"MMORPG_SQL_WRITER_GRANT_FILE": str(grant), "MMORPG_SQL_WRITER_GRANT_SHA256": data_full._sha(grant.read_bytes()).hex(),
                "MMORPG_SQL_WRITER_CONSUME_DIR": str(consume), "MMORPG_SQL_DEPLOYMENT_ID": f"interop-{service}-{run_id}",
                "MMORPG_SQL_PHYSICAL_AUTHORITY_SHA256": physical.hex(), "MMORPG_SQL_PRIVATE_FILE_HELPER": str(helper),
                "MMORPG_SQL_PRIVATE_FILE_HELPER_SHA256": data_full._sha(helper.read_bytes()).hex(),
                "MMORPG_SQL_USERNAME": username, "MMORPG_SQL_PASSWORD": password}, pinned

    data_full._register_writer = register_writer
    import battle_full
    import login_full

    def seed_login_credentials():
        # TiDB metadata visibility requires a table-level grant. Use Login's
        # authoritative two-column credential schema, so the grant exposes no
        # extra PlayerStore fields and its startup shape guard remains active.
        schema, principal = "interop_login_credentials", "interop_login_auth"
        password, encoded = secrets.token_hex(24), login_full._argon2_hash()
        ddl = (original.SOURCE / "go/login/model/password_auth_schema.sql").read_text(encoding="utf-8")
        try:
            sql(f"CREATE DATABASE `{schema}`; USE `{schema}`;\n{ddl}\n"
                f"INSERT INTO user_accounts(account,password) VALUES ('{login_full.ACCOUNT}','{encoded}'),"
                f"('{login_full.TEAM_ACCOUNT}','{encoded}');\n"
                f"CREATE USER '{principal}'@'%' IDENTIFIED BY '{password}';\n"
                f"GRANT SELECT ON `{schema}`.user_accounts TO '{principal}'@'%';\n"
                "INSERT INTO interop.zone_config(zone_id,name,recommended) VALUES(2,'zone-2',0);")
        except RuntimeError as exc:
            raise RuntimeError(str(exc).replace(encoded, "[redacted]").replace(password, "[redacted]")) from None
        return f"{principal}:{password}@tcp(127.0.0.1:{port})/{schema}?parseTime=true"

    login_full._seed_credentials = seed_login_credentials
    old_battle_config, old_login_config = battle_full._config, login_full._login_configs
    def battle_config(*args):
        return tidb_backend(old_battle_config(*args))
    def login_config():
        router, zones = old_login_config()
        for _, path in zones:
            value = path.read_text(encoding="utf-8")
            path.write_text(tidb_backend(value), encoding="utf-8")
        return router, zones
    battle_full._config, login_full._login_configs = battle_config, login_config

    import smoke_full
    old_configure = smoke_full.configure
    def configure():
        old_configure()
        original.PORT["mysql"] = port
    smoke_full.configure = configure
    logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
    asyncio.run(smoke_full.main(False, "python", False, False, **smoke_options(args)))
    print(f"TiDB full-chain artifacts: {original.RUN}", flush=True)


if __name__ == "__main__":
    main()
