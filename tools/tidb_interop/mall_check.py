import asyncio
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import time
import uuid
from types import SimpleNamespace

from aiohttp import ClientSession, web
from xuanming.portal_mall_admin import register_routes
from ownership import assert_owned_tidb

ROOT = Path.cwd()
FLAGS = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
PRIVATE_ROOT = None


def local_environment():
    return {key: value for key, value in os.environ.items()
            if not key.startswith(('MMORPG_', 'MATCH_IT_', 'COMMERCE_'))}


def clean_private(root):
    if root is None or not root.exists():
        return
    resolved = root.resolve()
    allowed = (Path(os.environ['USERPROFILE']) / '.mmorpg/sql-boot-local').resolve()
    if root.is_symlink() or resolved.parent != allowed or not re.fullmatch(r'codex-mall-[0-9a-f]{32}', resolved.name):
        raise RuntimeError('private output is outside the intended directory; refusing cleanup')
    shutil.rmtree(resolved)


def command(args, *, env=None, data=None):
    result = subprocess.run([str(arg) for arg in args], input=data, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, env=env, timeout=120, creationflags=FLAGS)
    if result.returncode:
        raise RuntimeError(f'{Path(args[0]).name} failed with status {result.returncode}')
    return result.stdout


def sql(text):
    return command([MYSQL, '--protocol=TCP', '--host=127.0.0.1', f'--port={PORT}', '--user=root',
                    '--batch', '--skip-column-names'], data=text.encode())


def sha(data): return hashlib.sha256(data).digest()
def b64(data): return base64.b64encode(data).decode()


def setup(port, mysql, work, cpp, source):
    global PRIVATE_ROOT
    nonce = uuid.uuid4().hex
    schema, registrar, runtime = 'mallit_' + nonce[:12], 'mreg_' + nonce[:12], 'mrun_' + nonce[:12]
    adminpw, runpw = secrets.token_hex(24), secrets.token_hex(24)
    root = Path(os.environ['USERPROFILE']) / '.mmorpg/sql-boot-local' / ('codex-mall-' + nonce)
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir()
    PRIVATE_ROOT = root
    sid = command(['whoami.exe', '/user', '/fo', 'csv', '/nh']).decode().strip().split(',')[-1].strip('"')
    command(['icacls.exe', root, '/inheritance:r', '/grant:r', f'*{sid}:(OI)(CI)F',
             '*S-1-5-18:(OI)(CI)F', '*S-1-5-32-544:(OI)(CI)F', '/Q'])
    inputs, outputs = root / 'input', root / 'output'
    inputs.mkdir(); outputs.mkdir()
    sql(f'CREATE DATABASE `{schema}` CHARACTER SET utf8mb4; USE `{schema}`;\n' +
        (SOURCE / 'src/main/resources/db/schema.sql').read_text(encoding='utf-8'))
    base = local_environment()
    base.update(MMORPG_SQL_BACKEND='tidb', MMORPG_SQL_SCHEMA_DSN=f'root@tcp(127.0.0.1:{PORT})/{schema}?parseTime=true',
                MMORPG_SQL_SCHEMA_PRINCIPAL='root@%')
    command([EXE, '-command', 'prepare-schema', '-schema', schema], env=base)
    generation = sql(f'SELECT generation FROM `{schema}`.route_meta WHERE id=1;').decode().strip()
    operator = (CPP / 'deploy/sql/authority_operator_role.sql').read_text(encoding='utf-8')
    runtime_grants = (CPP / 'deploy/sql/authority_runtime_role.sql').read_text(encoding='utf-8')
    statements = [f"CREATE USER '{registrar}'@'%' IDENTIFIED BY '{adminpw}';",
                  f"CREATE USER '{runtime}'@'%' IDENTIFIED BY '{runpw}';"]
    for line in operator.splitlines():
        if '{{REGISTRAR_ACCOUNT}}' in line and not line.lstrip().startswith('--'):
            statements.append(line.replace('{{AUTHORITY_SCHEMA}}', schema).replace('{{REGISTRAR_ACCOUNT}}', f"'{registrar}'@'%'") )
    for line in runtime_grants.splitlines():
        if '{{RUNTIME_ACCOUNT}}' in line and not line.lstrip().startswith('--'):
            statements.append(line.replace('{{AUTHORITY_SCHEMA}}', schema).replace('{{RUNTIME_ACCOUNT}}', f"'{runtime}'@'%'") )
    tables = sql(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}';").decode().splitlines()
    authority_tables = {'route_meta','writer_registry','writer_slot','writer_lifecycle_receipt','sql_writer_drain_run',
                        'sql_writer_freeze_ticket','sql_writer_maintenance_receipt','db_move_sentinel'}
    for table in tables:
        if table not in authority_tables:
            statements.append(f"GRANT SELECT,INSERT,UPDATE,DELETE ON `{schema}`.`{table}` TO '{runtime}'@'%';")
    sql('\n'.join(statements))
    physical = sha(f'LOCAL_TIDB_FIXTURE_V1:{nonce}:127.0.0.1:{PORT}'.encode())
    qualification = inputs / 'lock-qualification.original'
    qualification.write_text(json.dumps({'format':'LOCAL_DEV_LOCK_QUALIFICATION_V1', 'backend':'tidb',
                                         'lockPrimitive':'SELECT_ONLY', 'scope':'isolated local TiDB only'}), encoding='utf-8')
    deployment = 'interop-mall-' + nonce
    config = b'commerce:\n  run-mode: prod\n  require-tidb: true\n'
    (inputs / 'runtime.yaml').write_bytes(config)
    request = {'kind':'REGISTER', 'writer':{'generation':generation, 'sqlPrincipal':b64(f'{runtime}@%'.encode()),
               'state':'ACTIVE'}, 'authoritySchema':schema, 'protectedSchemas':[schema],
               'deploymentDigest':b64(physical), 'lockPrimitive':'SELECT_ONLY',
               'lockQualificationSha256':b64(sha(qualification.read_bytes()))}
    roster = inputs / 'roster.json'
    roster.write_text(json.dumps({'format':'SQL_WRITER_BOOT_ROSTER_V2','entries':[
        {'requests':[request], 'runtimeConfigPath':'runtime.yaml', 'runtimeConfigSha256':b64(sha(config))}]}), encoding='utf-8')
    grant = outputs / 'boot-grant.json'
    env = local_environment()
    env.update(MMORPG_SQL_BACKEND='tidb', MMORPG_SQL_REGISTRAR_DSN=f'{registrar}:{adminpw}@tcp(127.0.0.1:{PORT})/{schema}?parseTime=true',
               MMORPG_SQL_OPERATOR_PRINCIPAL=f'{registrar}@%', MMORPG_SQL_PHYSICAL_AUTHORITY_SHA256=physical.hex())
    command([EXE, '-command','register', '-pod-name','mall-0', '-roster',roster, '-roster-sha256',sha(roster.read_bytes()).hex(),
             '-input-root',inputs, '-lock-qualification',qualification, '-boot-output',grant,
             '-deployment-id',deployment, '-runtime-config-output', outputs / 'runtime.yaml'], env=env)
    return schema, runtime, runpw, deployment, physical.hex(), grant


async def verify(upstream, secret):
    admin_key = secrets.token_hex(32)
    deps = SimpleNamespace(settings=SimpleNamespace(admin_api_key=admin_key, mall_admin_base_url=upstream,
                           mall_admin_secret=secret, mall_admin_subject='mall-admin'))
    app = web.Application(); register_routes(app, deps)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0); await site.start()
    port = site._server.sockets[0].getsockname()[1]
    data = {'requestId': str(uuid.uuid4()), 'skuId':811, 'stock':5,
            'terms': {'productId':81, 'skuCode':'tidb-811', 'priceMinor':1250, 'currency':'CNY',
                      'published':True, 'itemConfigId':10, 'countPerUnit':1, 'title':'test'}}
    try:
        async with ClientSession(base_url=f'http://127.0.0.1:{port}') as client:
            response = await client.post('/admin/mall/skus/create', json=data)
            assert response.status == 401
            response = await client.post('/admin/mall/skus/create', headers={'X-Admin-Key': admin_key}, json=data)
            body = await response.text()
            assert response.status == 200, f'create returned HTTP {response.status}'
            first = json.loads(body)
            response = await client.post('/admin/mall/skus/create', headers={'X-Admin-Key': admin_key}, json=data)
            assert response.status == 200 and (await response.json()) == first
            response = await client.post('/admin/mall/skus/list', headers={'X-Admin-Key': admin_key},
                                         json={'afterSkuId':0, 'limit':10})
            assert response.status == 200 and any(item['skuId'] == 811 for item in (await response.json())['skus'])
    finally:
        await runner.cleanup()


def main(args):
    global WORK, SOURCE, CPP, EXE, MYSQL, PORT
    WORK, CPP, MYSQL, PORT = args.work, args.cpp_snapshot, args.mysql, args.port
    SOURCE = CPP / 'java/mall_game'
    EXE = WORK / 'writer_authority.exe'
    assert_owned_tidb(PORT, args.project, args.container, args.run_id)
    if args.network != args.project + '_default':
        raise RuntimeError('mall must use this run’s isolated TiDB network')
    try:
        schema, user, password, deployment, physical, grant = setup(PORT, MYSQL, WORK, CPP, SOURCE)
    except BaseException:
        clean_private(PRIVATE_ROOT)
        raise
    secret = secrets.token_hex(32)
    jar = SOURCE / 'target/mall-game-0.1.0-SNAPSHOT.jar'
    name = 'codex-tidb-mall-' + uuid.uuid4().hex[:12]
    env = {'COMMERCE_BIND_ADDRESS':'0.0.0.0', 'COMMERCE_PORT':'18120', 'COMMERCE_RUN_MODE':'prod',
           'COMMERCE_REQUIRE_TIDB':'true', 'MMORPG_SQL_REQUIRE_TIDB':'true', 'MMORPG_RUN_MODE':'prod',
           'COMMERCE_JDBC_URL':f'jdbc:mysql://tidb:4000/{schema}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC',
           'MMORPG_SQL_USERNAME':user, 'MMORPG_SQL_PASSWORD':password, 'COMMERCE_ADMIN_SECRET':secret,
           'COMMERCE_GATEWAY_SECRET':secrets.token_hex(32), 'COMMERCE_PAYMENT_SECRET':secrets.token_hex(32),
           'COMMERCE_ALLOWED_PAYMENT_CHANNELS':'online', 'MMORPG_SQL_WRITER_GRANT_FILE':'/private/grants/boot.json',
           'MMORPG_SQL_WRITER_GRANT_SHA256':sha(grant.read_bytes()).hex(), 'MMORPG_SQL_WRITER_CONSUME_DIR':'/private/consume',
           'MMORPG_SQL_DEPLOYMENT_ID':deployment, 'MMORPG_SQL_PHYSICAL_AUTHORITY_SHA256':physical}
    argv = ['docker','run','-d','--name',name,'--network',args.network,
            '--label',f'io.xuanming.interop.run={args.run_id}',
            '--memory','1536m','-p','127.0.0.1::18120', '--mount',f'type=bind,source={jar},target=/app.jar,readonly',
            '--mount',f'type=bind,source={grant},target=/incoming/grant.json,readonly']
    for key in env: argv.extend(['-e', key])
    argv += [args.java_image, 'sh','-c',
             'mkdir -p /private/grants /private/consume; chmod 700 /private/grants /private/consume; '
             'cp /incoming/grant.json /private/grants/boot.json; chmod 600 /private/grants/boot.json; '
             'exec java -Xmx768m -jar /app.jar']
    try:
        container_id = command(argv, env=local_environment() | env).decode().strip()
    except BaseException:
        inspected = subprocess.run(['docker', 'inspect', name], capture_output=True, timeout=15, creationflags=FLAGS)
        if inspected.returncode == 0:
            obj = json.loads(inspected.stdout)[0]
            if (obj['Config'].get('Labels') or {}).get('io.xuanming.interop.run') == args.run_id:
                command(['docker', 'rm', '-f', obj['Id']])
        clean_private(PRIVATE_ROOT)
        raise
    success = False
    try:
        binding = command(['docker','port',name,'18120/tcp']).decode().strip()
        port = int(binding.rsplit(':', 1)[-1])
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            state = command(['docker','inspect','--format','{{.State.Running}}',name]).decode().strip()
            if state != 'true':
                raise RuntimeError('isolated production mall exited; see mall-container.log')
            logs = command(['docker','logs',name])
            if b'Started MallGameApplication' in logs:
                break
            time.sleep(1)
        else: raise RuntimeError('isolated production mall readiness timeout')
        asyncio.run(verify(f'http://127.0.0.1:{port}', secret))
        version = sql('SELECT VERSION();').decode().strip()
        result = {'ok':True,'tidb_version':version,'mall_run_mode':'prod','require_tidb':True,'backend':'tidb/tikv',
                  'python_proxy_signature_verified':True,'unauthorized_rejected':True,
                  'sku_create_list_and_idempotence':True}
        success = True
    finally:
        try:
            current = command(['docker','inspect','--format','{{.Id}}',name]).decode().strip()
            if current != container_id:
                raise RuntimeError('mall container identity changed; refusing cleanup')
            try:
                logs = command(['docker','logs',container_id])
                for value in env.values():
                    if len(value) >= 24:
                        logs = logs.replace(value.encode(), b'[redacted]')
                (WORK / 'mall-container.log').write_bytes(logs)
            finally:
                command(['docker','rm','-f',container_id])
        finally:
            clean_private(PRIVATE_ROOT)
        if success:
            result['private_artifacts_cleaned'] = True
            result['container_cleaned'] = True
            (WORK / 'mall-acceptance.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
            print(json.dumps(result))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Production Mall over disposable local TiDB')
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--cpp-snapshot', type=Path, required=True)
    parser.add_argument('--mysql', type=Path, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--network', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--container', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--java-image', required=True)
    try:
        main(parser.parse_args())
    finally:
        clean_private(PRIVATE_ROOT)
