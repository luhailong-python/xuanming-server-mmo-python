"""Isolated native Go DataService and PlayerStore peers for the Python smoke."""

import asyncio
from base64 import b64encode
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import uuid

from smoke_original import ENV, GO, PORT, RUN, SOURCE, endpoint, run_mysql, wait_etcd
from xuanming.cluster import EtcdDirectory, PLAYER_STORE
from xuanming.data.player_store import PlayerSnapshot
from xuanming.protocol import Protocol

GLOBAL_DB = "mmorpg_global"
LEDGER_DB = "mmorpg_global_ledger"
ZONE_DB = "zone_1_db"
PLAYER_DB = "player_store_1000000_db"
STORAGE_ID = 1_000_000
DATA_SERVICE = 26
NATIVE_DATA_BIN = GO
KAFKA_TOPICS = {"transaction_log_topic_g1": 6, "player_snapshot_topic_g1": 3}
DML = "SELECT,INSERT,UPDATE,DELETE"
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")


def configure_data_env():
    for name in (
        "MMORPG_PLAYER_STORE_SECRET_SCENE",
        "MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN",
        "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE",
        "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE_MANAGER",
        "MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE",
        "MMORPG_ROLE_FENCE_SECRET_DB",
        "MMORPG_ASSET_OP_SECRET_DATA_SERVICE",
    ):
        ENV.setdefault(name, secrets.token_hex(32))
    ENV["DB_ALLOWED_DATABASES"] = PLAYER_DB
    ENV["MMORPG_DB_PLACEMENT_REQUIRED"] = "false"


def _sha(value):
    return hashlib.sha256(value).digest()


def _b64(value):
    return b64encode(value).decode("ascii")


def _identifier(value):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid scratch SQL identifier: {value}")
    return value


def _current_exe(service):
    executable = NATIVE_DATA_BIN / f"{service}.exe"
    if not executable.is_file():
        raise RuntimeError(f"build the current native {service} into {NATIVE_DATA_BIN}; "
                           "the installed November 5 binaries use retired PlayerStore rows")
    return executable


def _command(args, *, env=None):
    return subprocess.run(
        [str(arg) for arg in args], cwd=RUN, env=ENV | (env or {}),
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        timeout=50, creationflags=subprocess.CREATE_NO_WINDOW,
    )


def _checked(name, args, *, env=None):
    result = _command(args, env=env)
    log = RUN / f"{name}.log"
    log.write_bytes(result.stdout)
    if result.returncode:
        raise RuntimeError(f"{name} failed: {log}")
    return result.stdout


def _grant_template(template, schema, placeholder, username):
    return "\n".join(
        line.replace("{{AUTHORITY_SCHEMA}}", f"`{schema}`")
        .replace(placeholder, f"'{username}'@'%'")
        for line in template.splitlines() if line.startswith("GRANT ") and placeholder in line
    )


def _register_writer(service, config_path, schema_privileges, lock_schemas, bind_sql, runtime_credentials):
    # One identity for every logical schema in a given service boot; each schema has its own gate.
    _identifier(service)
    root = RUN / f"writer-private-{service}"
    root.mkdir()
    identity = _command(["whoami.exe", "/user", "/fo", "csv", "/nh"], env={}).stdout
    sid = identity.decode().strip().split(",")[-1].strip('"')
    if not sid.startswith("S-1-"):
        raise RuntimeError("current Windows SID unavailable")
    _checked(f"{service}-private-acl", ["icacls.exe", root, "/inheritance:r", "/grant:r", f"*{sid}:(OI)(CI)F",
                                        "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F", "/Q"])
    input_dir, output_dir, consume_dir = (root / name for name in ("input", "output", "consume"))
    for directory in (input_dir, output_dir, consume_dir):
        directory.mkdir()

    operator_template = (SOURCE / "deploy/sql/authority_operator_role.sql").read_text(encoding="utf-8")
    runtime_template = (SOURCE / "deploy/sql/authority_runtime_role.sql").read_text(encoding="utf-8")
    registrar, registrar_password = f"it_{service}_reg", secrets.token_hex(24)
    username, password = runtime_credentials or (f"it_{service}_run", secrets.token_hex(24))
    _identifier(username)
    if not re.fullmatch(r"[A-Za-z0-9_-]{24,128}", password):
        raise ValueError("scratch writer password must be an unquoted URL-safe synthetic secret")
    grants = [f"CREATE USER '{registrar}'@'%' IDENTIFIED BY '{registrar_password}';",
              f"CREATE USER '{username}'@'%' IDENTIFIED BY '{password}';"]
    for schema, tables in schema_privileges.items():
        _identifier(schema)
        grants.extend((
            _grant_template(operator_template, schema, "{{REGISTRAR_ACCOUNT}}", registrar),
            _grant_template(runtime_template, schema, "{{RUNTIME_ACCOUNT}}", username),
            f"GRANT UPDATE (lock_touch) ON `{schema}`.`writer_slot` TO '{username}'@'%';",
        ))
        if schema in lock_schemas:
            # MySQL needs one extra privilege for source route_meta FOR UPDATE probes.
            grants.append(f"GRANT LOCK TABLES ON `{schema}`.* TO '{username}'@'%';")
        for table, privileges in tables.items():
            if not set(privileges.split(",")) <= set(DML.split(",")):
                raise ValueError(f"unsupported runtime privilege set for {schema}.{table}")
            grants.append(f"GRANT {privileges} ON `{schema}`.`{_identifier(table)}` TO '{username}'@'%';")
    run_mysql("\n".join(grants))

    # The digest labels this one local MySQL authority in all Go boot grants.
    physical = _sha(f"LOCAL_SQL_AUTHORITY_V1\nendpoint={endpoint('mysql')}\nrun={RUN.name}\n".encode())
    qualification = input_dir / "lock-qualification.original"
    qualification.write_text(json.dumps({"format": "LOCAL_DEV_LOCK_QUALIFICATION_V1", "backend": "mysql",
                                         "lockPrimitive": "LOCK_TOUCH", "scope": "Loopback development database only"}),
                             encoding="utf-8")
    writer_key, instance = uuid.uuid4().bytes, uuid.uuid4().bytes
    deployment = f"interop-{service}-{uuid.uuid4().hex}"
    config = (config_path.read_bytes() if bind_sql is None else
              _config_credentials(config_path.read_text(encoding="utf-8"), username, password,
                                  db=bind_sql == "db").encode())
    (input_dir / "runtime.yaml").write_bytes(config)
    requests = []
    for schema in schema_privileges:
        generation = ""
        for _ in range(10):
            rows = run_mysql(f"SELECT generation FROM `{schema}`.route_meta WHERE id=1;").decode().splitlines()
            if len(rows) == 2:
                generation = rows[1]
                break
            time.sleep(0.1)
        if not re.fullmatch(r"[a-f0-9]{32}", generation):
            raise RuntimeError(f"missing native writer generation in {schema}")
        requests.append({
            "kind": "REGISTER",
            "writer": {"generation": generation, "sqlPrincipal": _b64(f"{username}@%".encode()),
                       "state": "ACTIVE"},
            "authoritySchema": schema, "protectedSchemas": [schema], "deploymentDigest": _b64(physical),
            "lockPrimitive": "LOCK_TOUCH",
            "lockQualificationSha256": _b64(_sha(qualification.read_bytes())),
        })
    roster = input_dir / "roster.json"
    roster.write_text(json.dumps({"format": "SQL_WRITER_BOOT_ROSTER_V2", "entries": [
        {"requests": requests, "runtimeConfigPath": "runtime.yaml", "runtimeConfigSha256": _b64(_sha(config))},
    ]}), encoding="utf-8")
    grant, pinned_config = output_dir / "boot-grant.json", output_dir / "runtime.yaml"
    registrar_env = {
        "MMORPG_SQL_BACKEND": "mysql",
        "MMORPG_SQL_REGISTRAR_DSN": (f"{registrar}:{registrar_password}@tcp({endpoint('mysql')})/"
                                     f"{next(iter(schema_privileges))}?parseTime=true"),
        "MMORPG_SQL_OPERATOR_PRINCIPAL": f"{registrar}@%",
        "MMORPG_SQL_PHYSICAL_AUTHORITY_SHA256": physical.hex(),
    }
    _checked(f"{service}-register-writer", [
        GO / "writer_authority.exe", "-command", "register", "-pod-name", f"{service}-0", "-roster", roster,
        "-roster-sha256", _sha(roster.read_bytes()).hex(), "-input-root", input_dir,
        "-lock-qualification", qualification, "-boot-output", grant,
        "-deployment-id", deployment, "-runtime-config-output", pinned_config,
    ], env=registrar_env)

    helper = Path(os.environ["XUANMING_IT_PRIVATE_HELPER"])
    if not helper.is_file():
        raise RuntimeError("installed trusted private control helper unavailable")
    if _sha(pinned_config.read_bytes()) != _sha(config):
        raise RuntimeError(f"pinned writer runtime config drift for {service}")
    return {
        "MMORPG_SQL_WRITER_GRANT_FILE": str(grant), "MMORPG_SQL_WRITER_GRANT_SHA256": _sha(grant.read_bytes()).hex(),
        "MMORPG_SQL_WRITER_CONSUME_DIR": str(consume_dir), "MMORPG_SQL_DEPLOYMENT_ID": deployment,
        "MMORPG_SQL_PHYSICAL_AUTHORITY_SHA256": physical.hex(), "MMORPG_SQL_PRIVATE_FILE_HELPER": str(helper),
        "MMORPG_SQL_PRIVATE_FILE_HELPER_SHA256": _sha(helper.read_bytes()).hex(),
        "MMORPG_SQL_USERNAME": username, "MMORPG_SQL_PASSWORD": password,
    }, pinned_config


async def register_writer(service, config_path, schema_privileges, *, lock_schemas=(), bind_sql=None,
                          runtime_credentials=None):
    return await asyncio.to_thread(_register_writer, service, config_path, schema_privileges, lock_schemas,
                                   bind_sql, runtime_credentials)


def _base_configs():
    db = (SOURCE / "go/db/etc/db.yaml").read_text(encoding="utf-8")
    for old, new in (
        ("0.0.0.0:6000", endpoint("db")), ("127.0.0.1:2379", endpoint("etcd")),
        ("127.0.0.1:3306", endpoint("mysql")), ("Backend: tidb", "Backend: mysql"),
        ("../../generated/data/mysql_database_table_list.json",
         (SOURCE / "generated/data/mysql_database_table_list.json").as_posix()),
        ('MetricsListenAddr: ":9160"', 'MetricsListenAddr: ""'),
        ("AllowlistEnforcement: warn", "AllowlistEnforcement: strict"),
    ):
        db = db.replace(old, new)
    data = (SOURCE / "go/data_service/etc/data_service.yaml").read_text(encoding="utf-8")
    for old, new in (
        ("0.0.0.0:9000", endpoint("data")), ("127.0.0.1:2379", endpoint("etcd")),
        ("127.0.0.1:6379", endpoint("redis")), ("127.0.0.1:9092", endpoint("kafka")),
        ("127.0.0.1:3306", endpoint("mysql")), ("Backend: tidb", "Backend: mysql"),
        ('MetricsListenAddr: ":9260"', 'MetricsListenAddr: ""'), ("  AutoMigrate: true", "  AutoMigrate: false"),
    ):
        data = data.replace(old, new)
    for key, old in (("match", "127.0.0.1:50500"), ("inbox", "127.0.0.1:50950")):
        if key in PORT:
            data = data.replace(old, endpoint(key))
    # Only the peers participating in this smoke are in its isolated network.
    # No role transfer/delete is attempted without the other required participants.
    data = re.sub(r"(?m)^    - Name: (guild|trade|mail|friend)\n      Endpoints: \[[^\n]*\]\n", "", data)
    return db, data


def _config_credentials(body, user, password, *, db):
    body = re.sub(r'(?m)^( +User:).+$', lambda match: f'{match[1]} "{user}"', body)
    if db:
        return re.sub(r'(?m)^( +Passwd:).+$', lambda match: f'{match[1]} "{password}"', body)
    else:
        return re.sub(r'(?m)^(SnapshotMySQL:\n(?:(?!\S).+\n)*?  Password:) "[^\n]*"$',
                      lambda match: f'{match[1]} "{password}"', body)


def _config_file(name, body, *, user, password, db):
    path = RUN / name
    path.write_text(_config_credentials(body, user, password, db=db), encoding="utf-8")
    return path


async def _migrate(processes):
    configure_data_env()
    schemas = (GLOBAL_DB, LEDGER_DB, ZONE_DB, PLAYER_DB)
    await asyncio.to_thread(run_mysql, "\n".join(
        f"CREATE DATABASE `{schema}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        for schema in schemas
    ))
    db, data = _base_configs()
    migration_env = ENV | {"MMORPG_SQL_SCHEMA_PRINCIPAL": "root@localhost"}
    db_migration = _config_file("db-migrate.yaml", db, user="root", password="", db=True)
    data_migration = _config_file("data-migrate.yaml", data, user="root", password="", db=False)
    processes.start("db-migrate-player", [_current_exe("db_migrate"), "-f", db_migration, "-command", "up",
                                          "-storage-id", STORAGE_ID], env=migration_env)
    processes.start("data-migrate", [_current_exe("data_service"), "-f", data_migration, "-migrate"], env=migration_env)
    await asyncio.gather(*(processes.finish(name) for name in ("db-migrate-player", "data-migrate")))
    return db, data


async def start_data(processes):
    """Run native migrations, allocate DB writer identities, boot, and wait for discovery."""
    db, data = await _migrate(processes)
    db_tables = dict.fromkeys(json.loads((SOURCE / "generated/data/mysql_database_table_list.json")
                                        .read_text(encoding="utf-8"))["messages"], DML)
    data_directory_tables = dict.fromkeys((
        "player_name", "player_name_owner", "directory_shard", "account_directory", "account_role", "account_role_ref",
        "account_role_trade", "account_role_transfer_receipt", "account_role_admission", "account_role_trade_holder",
        "account_role_removal", "player_route", "zone_merged_into",
    ), DML)
    data_ledger_tables = dict.fromkeys((
        "transaction_log", "player_snapshot", "rollback_audit_log", "gm_recall_batch", "gm_recall_op", "gm_recall_seq",
        "gm_recall_source", "role_fence",
    ), DML)
    data_storage_tables = {"player_store": DML, "player_centre_database": "SELECT", "user_accounts": "SELECT"}
    db_cfg = _config_file("db.yaml", db, user="it_db_run", password="", db=True)
    data_cfg = _config_file("data.yaml", data, user="it_data_run", password="", db=False)
    db_writer, data_writer = await asyncio.gather(
        register_writer("db", db_cfg, {PLAYER_DB: db_tables}, bind_sql="db"),
        register_writer("data", data_cfg, {GLOBAL_DB: data_directory_tables, LEDGER_DB: data_ledger_tables,
                                           PLAYER_DB: data_storage_tables},
                        lock_schemas=(GLOBAL_DB, PLAYER_DB), bind_sql="data"),
    )
    data_env, data_path = data_writer
    db_env, db_path = db_writer
    processes.start("data", [_current_exe("data_service"), "-f", data_path], env=ENV | data_env)
    directory = EtcdDirectory(Protocol(), endpoint("etcd"))
    try:
        await processes.ready("data")
        data_info = await wait_etcd(directory, DATA_SERVICE, 1)
        assert (data_info.grpc_endpoint.ip, data_info.grpc_endpoint.port) == ("127.0.0.1", PORT["data"])
        processes.start("db", [_current_exe("db"), "-f", db_path], env=ENV | db_env)
        await processes.ready("db")
        db_info = await wait_etcd(directory, PLAYER_STORE, 1)
        assert (db_info.grpc_endpoint.ip, db_info.grpc_endpoint.port) == ("127.0.0.1", PORT["db"])
    finally:
        await directory.close()


async def read_native_player(player_id):
    """Inspect the isolated SQL rows without taking ownership or calling PlayerStore."""
    if type(player_id) is not int or not 1 <= player_id < 1 << 64:
        raise ValueError("invalid uint64 player id")
    # Older Go Login uses the zone schema; current Login can use a regional store.
    # Take the rows from the catalog-selected, allowlisted schema in one statement.
    selects = (f"""
        SELECT route.storage_id, p.kind, p.owner_epoch, p.save_seq, p.schema_gen, p.released,
               HEX(COALESCE(p.holder_instance, '')), HEX(COALESCE(p.payload, ''))
        FROM `{GLOBAL_DB}`.player_route route JOIN `{schema}`.player_store p USING (player_id)
        WHERE route.player_id={player_id} AND route.storage_id={storage_id}
    """ for storage_id, schema in ((STORAGE_ID, PLAYER_DB),))
    query = " UNION ALL ".join(selects) + " ORDER BY kind;"
    raw = await asyncio.to_thread(run_mysql, query)
    found = [line.split("\t") for line in raw.decode().splitlines()[1:]]
    if not found or found[0][1] != "0":
        raise AssertionError(f"no native SQL ownership row for player {player_id}")
    storage_id, _, epoch, seq, schema_gen, released, holder, _ = found[0]
    return PlayerSnapshot(
        player_id=player_id, owner=bytes.fromhex(holder), owner_epoch=int(epoch), save_seq=int(seq),
        rows={int(row[1]): bytes.fromhex(row[7]) for row in found[1:]}, storage_id=int(storage_id),
        schema_gen=int(schema_gen), released=released == "1",
    )
