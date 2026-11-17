"""Native Go Login and Router with the Python HTTP portal."""

import asyncio
import base64
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import time
import uuid

import grpc

from smoke_original import ENV, GO, PORT, RUN, SOURCE, endpoint, http, original_configs, run_mysql, wait_etcd
from xuanming.cluster import EtcdDirectory, ROUTER
from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes.battle_control import sign_service_call
from xuanming.protocol import Protocol
from xuanming.smoke import Client, connect


ACCOUNT = f"native_interop_{uuid.uuid4().hex[:12]}"
TEAM_ACCOUNT = f"native_team_{uuid.uuid4().hex[:12]}"
PASSWORD = secrets.token_urlsafe(32)
DATA_SERVICE = "/data_service.DataService/"


@dataclass(frozen=True)
class LoginSession:
    client: Client
    account: str
    access_token: str
    player_id: int
    owner_revision: int


def _argon2_hash():
    work = RUN / "credential-tool"
    work.mkdir()
    module = work / "crypto"
    module.mkdir()
    go = Path(os.environ["XUANMING_IT_GO"])
    module_cache = subprocess.run([str(go), "env", "GOMODCACHE"], cwd=SOURCE / "go/login", env=ENV,
                                  capture_output=True, text=True, check=True,
                                  creationflags=subprocess.CREATE_NO_WINDOW).stdout.strip()
    crypto = Path(module_cache) / "golang.org/x/crypto@v0.46.0"
    for package in ("argon2", "blake2b"):
        shutil.copytree(crypto / package, module / package)
    (module / "go.mod").write_text("module golang.org/x/crypto\n\ngo 1.24.0\n", encoding="utf-8")
    (work / "go.mod").write_text(
        "module interopseed\n\ngo 1.24.0\n\nrequire golang.org/x/crypto v0.46.0\n"
        "replace golang.org/x/crypto => ./crypto\n", encoding="utf-8",
    )
    provider = (SOURCE / "go/login/internal/logic/pkg/auth/password_provider.go").read_text(encoding="utf-8")
    declarations = []
    for pattern in (r"^\s*(passwordMaxBytes\s*=.*)$", r"^\s*(defaultArgon\w+\s*=.*)$",
                    r"^(type argon2idParams struct \{[\s\S]+?^\})$",
                    r"^(var defaultArgon2idParams = argon2idParams\{[\s\S]+?^\})$",
                    r"^(func HashPassword\([\s\S]+?^\})$",
                    r"^(func hashPasswordWithParams\([\s\S]+?^\})$"):
        matches = re.findall(pattern, provider, re.MULTILINE)
        if not matches:
            raise RuntimeError("could not extract native password hash implementation")
        if matches[0].startswith(("passwordMaxBytes", "defaultArgon")):
            declarations.append("const (\n" + "\n".join(matches) + "\n)")
        else:
            declarations.extend(matches)
    (work / "main.go").write_text(r'''package main

import (
    "crypto/rand"
    "encoding/base64"
    "fmt"
    "io"
    "os"

    "golang.org/x/crypto/argon2"
)

func main() {
    password, err := io.ReadAll(io.LimitReader(os.Stdin, 1025))
    if err != nil {
        panic(err)
    }
    encoded, err := HashPassword(string(password))
    if err != nil {
        panic(err)
    }
    fmt.Print(encoded)
}
''' + "\n\n".join(declarations) + "\n", encoding="utf-8")
    command = [str(go), "run", "-tags=purego", "."]
    go_env = ENV | {"GOWORK": "off", "GOENV": "off", "GOTOOLCHAIN": "local", "GOPROXY": "off",
                    "GOSUMDB": "off", "GOTELEMETRY": "off", "CGO_ENABLED": "0",
                    "GOCACHE": str(RUN.parent / "credential-go-cache"),
                    "GOMODCACHE": str(work / "modcache"), "GOPATH": str(work / "gopath")}
    result = subprocess.run(command, cwd=work, env=go_env, input=PASSWORD.encode(), capture_output=True,
                            creationflags=subprocess.CREATE_NO_WINDOW, timeout=100)
    if result.returncode:
        raise RuntimeError(f"isolated Go password seed failed: {result.stderr.decode(errors='replace')}")
    value = result.stdout.decode("ascii")
    if not re.fullmatch(r"\$argon2id\$v=19\$m=65536,t=3,p=2\$[A-Za-z0-9+/]+\$[A-Za-z0-9+/]+", value):
        raise ValueError("Go password seed output is not the original Argon2id PHC format")
    return value


def _seed_credentials():
    schema = "interop_login_credentials"
    principal = "interop_login_auth"
    auth_password = secrets.token_hex(24)
    encoded = _argon2_hash()
    original_schema = (SOURCE / "go/db/model/mysql_database_table.sql").read_text(encoding="utf-8")
    ddl = re.search(
        r"^CREATE TABLE IF NOT EXISTS user_accounts \([\s\S]+?;[ \t]*$",
        original_schema,
        re.MULTILINE,
    )
    if not ddl:
        raise RuntimeError("original credential table DDL missing")
    try:
        run_mysql(f"CREATE DATABASE {schema}; USE {schema};\n{ddl.group()}\n"
                  f"INSERT INTO user_accounts(account,password) VALUES ('{ACCOUNT}','{encoded}'),('{TEAM_ACCOUNT}','{encoded}');\n"
                  f"CREATE USER '{principal}'@'%' IDENTIFIED BY '{auth_password}';\n"
                  f"GRANT SELECT (account,password) ON {schema}.user_accounts TO '{principal}'@'%';\n"
                  "INSERT INTO interop.zone_config(zone_id,name,recommended) VALUES(2,'zone-2',0);")
    except RuntimeError as exc:
        raise RuntimeError(str(exc).replace(encoded, "[redacted]").replace(auth_password, "[redacted]")) from None
    return f"{principal}:{auth_password}@tcp({endpoint('mysql')})/{schema}?parseTime=true"


def _login_configs():
    router, first = original_configs()
    contents = first.read_text(encoding="utf-8")
    expected = "\nPasswordAuth:\n  Enabled: false"
    if contents.count(expected) != 1 or "DevPasswordAuth:\n  Enabled: false" not in contents:
        raise RuntimeError("original Login password configuration layout changed")
    contents = contents.replace(expected, "\nPasswordAuth:\n  Enabled: true", 1)
    contents = contents.replace("Mode: pro", "Mode: dev", 1)
    contents = contents.replace("  Backend: tidb\n", "  Backend: mysql\n", 1)
    contents = contents.replace("  Database: mmorpg\n", "  Database: interop_login_credentials\n", 1)
    if "manager2" in PORT:
        key = "Key: scenemanagerservice.rpc"
        if contents.count(key) != 1:
            raise RuntimeError("original Login SceneManager discovery configuration layout changed")
        contents = contents.replace(key, key + ".zone1", 1)
    first.write_text(contents, encoding="utf-8")
    zones = [("login", first)]
    if "login2" in PORT:
        second = RUN / "login2.yaml"
        second.write_text(contents.replace(endpoint("login"), endpoint("login2"))
                          .replace("  ZoneId: 1", "  ZoneId: 2", 1)
                          .replace("Key: scenemanagerservice.rpc.zone1", "Key: scenemanagerservice.rpc.zone2", 1)
                          .replace(f"Port: {PORT['metrics']}", f"Port: {PORT['metrics2']}")
                          .replace((RUN / "snowflake").as_posix(), (RUN / "snowflake-zone2").as_posix()),
                          encoding="utf-8")
        zones.append(("login2", second))
    return router, zones


async def start_login(processes, *, ingress="python"):
    """Start native peers after isolated MySQL, Kafka, Redis, and etcd are ready."""
    if ingress != "python":
        raise ValueError(f"unsupported native login ingress: {ingress}")
    router, zones = _login_configs()
    processes.start("router", [GO / "client_rpc_router.exe", "-f", router])
    dsn = await asyncio.to_thread(_seed_credentials)
    login_env = {key: value for key, value in ENV.items() if not key.startswith("MMORPG_SQL_")}
    login_env["LOGIN_PASSWORD_AUTH_DSN"] = dsn
    login_env.pop("MMORPG_INTERNAL_AUTH_SIGNING_KEY", None)
    for name, config in zones:
        processes.start(name, [GO / "login.exe", "-loginService", config], env=login_env)
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(processes.ready("router"))
        for name, _ in zones:
            tasks.create_task(processes.ready(name))
    if ingress == "python":
        from python_gateway import start_portal
        processes.portal_runner = await start_portal()
    protocol = Protocol()
    kinds = protocol.pool.FindEnumTypeByName("common.base.eNodeType").values_by_name
    directory = EtcdDirectory(protocol, endpoint("etcd"))
    try:
        router = await wait_etcd(directory, ROUTER, 1)
        if router.grpc_endpoint.port != PORT["router"]:
            raise RuntimeError("unexpected original Router registration")
        for zone, (name, _) in enumerate(zones, start=1):
            login = await wait_etcd(directory, kinds["LoginNodeService"].number, zone)
            if login.grpc_endpoint.port != PORT[name]:
                raise RuntimeError(f"unexpected original Login registration in zone {zone}")
    finally:
        await directory.close()


async def _directory_call(protocol, client, method, request_type, response_type, **fields):
    request = protocol.message(f"data_service.{request_type}", **fields)
    path = DATA_SERVICE + method
    sign_service_call(path, request, "login", time.time_ns() // 1_000_000,
                      ENV["MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN"].encode())
    return protocol.parse(f"data_service.{response_type}", await client.call(path, request.SerializeToString()))


async def _read_account(protocol, client, account):
    return await _directory_call(protocol, client, "ReadAccountDirectory", "ReadAccountDirectoryRequest",
                                 "AccountDirectoryResponse", account=account, create_if_missing=False)


async def _rpc(client, method, call_id, request_type, **fields):
    protocol = client.protocol
    response = await client.rpc(protocol.message_ids[f"ClientPlayerLogin{method}"], call_id,
                                protocol.message(f"loginpb.{request_type}", **fields))
    if response.error_message.id:
        raise RuntimeError(f"original Go {method} returned application tip {response.error_message.id}")
    return response


async def authenticate(client, access_token, *, call_id=1):
    return await _rpc(client, "Login", call_id, "LoginRequest", auth_type="access_token", auth_token=access_token)


async def enter_game(client, player_id, *, call_id=3):
    response = await _rpc(client, "EnterGame", call_id, "EnterGameRequest", player_id=player_id,
                          request_id=f"native-interop-{uuid.uuid4().hex}")
    if response.player_id != player_id:
        raise RuntimeError("Go EnterGame ACK player did not match the requested role")
    return response


@asynccontextmanager
async def connect_assigned_gate(protocol, gate_endpoint, account, *, access_token="", zone=1, ingress="python", login=None):
    """Request an original admission ticket and verify it on a new TCP connection."""
    fields = {"zone_id": zone, "access_token": access_token, "device_id": "native-interop"}
    if ingress == "python":
        admission = await asyncio.to_thread(http, "/api/assign-gate", fields)
        if admission.get("code") == 429 and admission.get("error") == "ACCOUNT_COOLDOWN":
            # The native Gateway's default per-account window is five seconds.
            await asyncio.sleep(5.1)
            admission = await asyncio.to_thread(http, "/api/assign-gate", fields)
        if admission.get("code") != 0:
            raise RuntimeError(f"{ingress} assign gate failed: code={admission.get('code')} error={admission.get('error')}")
        assigned_endpoint = (admission["gate_ip"], admission["gate_port"])
        payload = base64.b64decode(admission["token_payload"], validate=True)
        signature = base64.b64decode(admission["token_signature"], validate=True)
    else:
        raise ValueError(f"unsupported native login ingress: {ingress}")
    if assigned_endpoint != (gate_endpoint.ip, gate_endpoint.port):
        raise RuntimeError("Go Login chose an unexpected Gate endpoint")
    ticket = protocol.message("ClientTokenVerifyRequest", payload=payload, signature=signature)
    async with connect(protocol, gate_endpoint) as client:
        await client.send(ticket)
        if not (await client.receive("ClientTokenVerifyResponse")).success:
            raise RuntimeError("Python Gate rejected native Login ticket")
        yield client


@asynccontextmanager
async def exercise_login(protocol, gate_endpoint, *, zone=1, ingress="python", account=ACCOUNT):
    """Use native password auth and native signed TCP role creation."""
    if ingress != "python":
        raise ValueError(f"unsupported native login ingress: {ingress}")
    data = GrpcClient(endpoint("data"), timeout=8)
    try:
        try:
            await _read_account(protocol, data, account)
        except grpc.aio.AioRpcError as exc:
            if exc.code() != grpc.StatusCode.NOT_FOUND:
                raise
        else:
            raise RuntimeError("fresh fixture account already present in authoritative directory")

        body = {"zone_id": zone, "account": account, "password": PASSWORD, "device_id": "native-interop"}
        authorized = await asyncio.to_thread(http, "/api/login", body)
        if authorized.get("code") != 0:
            raise RuntimeError(f"{ingress} password Login failed: code={authorized.get('code')} message={authorized.get('message')}")
        access_token, refresh_token = authorized.get("access_token"), authorized.get("refresh_token")
        if not access_token or not refresh_token:
            raise RuntimeError("native Go password Login returned no access or refresh token")
        if (await _read_account(protocol, data, account)).players.players:
            raise RuntimeError("new SQL account unexpectedly has pre-existing roles")
        origin = f"{ingress} /api/login"
        print(f"PASS {origin} verified the native SQL Argon2id credential and Go DataService created the account.",
              flush=True)

        async with connect_assigned_gate(protocol, gate_endpoint, account, access_token=access_token, zone=zone, ingress=ingress) as client:
            await authenticate(client, access_token)
            print("PASS Python Gate accepted the native ticket and native Go Login accepted the access token.",
                  flush=True)
            name = f"Native{secrets.token_hex(2)}"
            response = await _rpc(client, "CreatePlayer", 2, "CreatePlayerRequest", name=name, gender=1)
            created = next((role.player for role in response.players if role.player.name == name), None)
            if not created or not created.player_id or created.zone_id != zone:
                raise RuntimeError("native CreatePlayer returned no matching role")
            owned = await _directory_call(protocol, data, "LookupAccountRole", "LookupAccountRoleRequest",
                                          "AccountRoleOwnership", player_id=created.player_id)
            if owned.account != account or owned.role != created or not owned.owner_revision:
                raise RuntimeError("native DataService did not return persisted role ownership")
            print("PASS original Go Login created the role and native DataService confirmed its SQL owner revision.", flush=True)
            yield LoginSession(client, account, access_token, created.player_id, owned.owner_revision)
    finally:
        await data.close()
