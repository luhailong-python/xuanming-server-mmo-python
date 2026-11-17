import asyncio
import hashlib
import hmac
import json
from urllib.parse import parse_qs, urlsplit

from aiohttp import ClientSession

from python_gateway import ADMIN_KEY, CDN_SECRET, OBSERVED_METADATA
from smoke_original import endpoint, run_mysql
from xuanming.grpc_runtime import GrpcClient
from xuanming.protocol import Protocol

async def verify():
    result = {}
    base = f"http://{endpoint('gateway')}"
    headers = {'X-Admin-Key': ADMIN_KEY}
    async with ClientSession(base_url=base) as client:
        async with client.get('/admin/announcements') as response:
            assert response.status == 401
        async with client.post('/api/hotfix-check', json={'platform': 'windows', 'client_version': '1.0'}) as response:
            item = await response.json()
            assert response.status == 200 and item['need_update'] and item['force_update']
        async with client.post('/api/cdn-sign', json={'resource_path': '/probe/file.dat'}) as response:
            item = await response.json()
            assert response.status == 200
            url = urlsplit(item['signed_url'])
            query = parse_qs(url.query)
            assert url.path == '/probe/file.dat' and int(query['expire'][0]) == item['expire_at']
            assert hmac.compare_digest(query['sign'][0], hmac.new(CDN_SECRET.encode(),
                                      f"/probe/file.dat|{item['expire_at']}".encode(), hashlib.sha256).hexdigest())
        async with client.post('/admin/announcements', headers=headers,
                               json={'title': 'isolated portal check', 'content': 'hello', 'type': 'notice'}) as response:
            assert response.status == 200, await response.text()
            identifier = (await response.json())['id']
        try:
            sql = await asyncio.to_thread(run_mysql, f"SELECT title FROM interop.announcement WHERE id={identifier};")
            assert b'isolated portal check' in sql
            async with client.get('/api/announcement') as response:
                assert response.status == 200
                assert any(item['id'] == identifier for item in (await response.json())['items'])
        finally:
            async with client.delete(f'/admin/announcements/{identifier}', headers=headers) as response:
                assert response.status == 204
        async with client.get('/api/server-list') as response:
            assert response.status == 200
            assert await response.json() is not None
        zone = {'zone_id': 9, 'name': 'isolated zone', 'manual_status': 0, 'capacity': 20,
                'maintenance_msg': '', 'open_time': None, 'recommended': False, 'sort_order': 9}
        async with client.post('/admin/zones', headers=headers, json=zone) as response:
            assert response.status == 200, await response.text()
        try:
            async with client.get('/admin/zones/9', headers=headers) as response:
                assert response.status == 200
                etag = response.headers['ETag']
            zone['name'] = 'updated isolated zone'
            async with client.put('/admin/zones/9', headers=headers | {'If-Match': etag}, json=zone) as response:
                assert response.status == 200, await response.text()
            async with client.put('/admin/zones/9', headers=headers | {'If-Match': etag}, json=zone) as response:
                assert response.status == 412
            async with client.post('/admin/whitelist', headers=headers,
                                   json={'zone_id': 9, 'account_id': 12345, 'note': 'isolated'}) as response:
                assert response.status == 200, await response.text()
            sql = await asyncio.to_thread(run_mysql, 'SELECT name FROM interop.zone_config WHERE zone_id=9;'
                                          'SELECT note FROM interop.zone_whitelist WHERE zone_id=9 AND account_id=12345;')
            assert b'updated isolated zone' in sql and b'isolated' in sql
            async with client.get('/admin/whitelist/9', headers=headers) as response:
                assert response.status == 200 and len(await response.json()) == 1
            async with client.delete('/admin/whitelist/9/12345', headers=headers) as response:
                assert response.status == 204
        finally:
            async with client.delete('/admin/zones/9', headers=headers) as response:
                assert response.status == 204
        result.update(unauthorized_rejected=True, hotfix_http=True, cdn_hmac=True, announcement_mysql=True,
                      zone_etag_mysql=True, whitelist_mysql=True, server_list_http=True)
        async with client.put('/admin/killswitch', headers=headers,
                              json={'target': 'interop.NoSuchService/NoSuchMethod', 'deny': True,
                                    'reason': 'isolated portal check'}) as response:
            assert response.status == 200, await response.text()
            rule = await response.json()
        try:
            async with client.get('/admin/killswitch', headers=headers) as response:
                assert response.status == 200
                assert any(item['key'] == rule['key'] and item['deny'] for item in await response.json())
            protocol = Protocol()
            etcd = GrpcClient(endpoint('etcd'), timeout=3)
            try:
                req = protocol.message('etcdserverpb.RangeRequest', key=('/mmorpg/killswitch/' + rule['key']).encode())
                data = await etcd.call('/etcdserverpb.KV/Range', req.SerializeToString())
                assert protocol.parse('etcdserverpb.RangeResponse', data).count == 1
            finally:
                await etcd.close()
        finally:
            async with client.delete('/admin/killswitch', headers=headers, params={'key': rule['key']}) as response:
                assert response.status == 200 and (await response.json())['deleted'] == 1
        result['killswitch_etcd'] = True
        etcd = GrpcClient(endpoint('etcd'), timeout=3)
        protocol = Protocol()
        try:
            observed_health = {}
            async with asyncio.timeout(12):
                for zone_id in (1, 2):
                    policy_key = f'/mmorpg/zone_policy/zone/{zone_id}'.encode()
                    prefix = f'/mmorpg/zone_health/zone/{zone_id}/publisher/'.encode()
                    end = prefix[:-1] + bytes((prefix[-1] + 1,))
                    while True:
                        policy_req = protocol.message('etcdserverpb.RangeRequest', key=policy_key)
                        health_req = protocol.message('etcdserverpb.RangeRequest', key=prefix, range_end=end)
                        policy = protocol.parse('etcdserverpb.RangeResponse', await etcd.call(
                            '/etcdserverpb.KV/Range', policy_req.SerializeToString()))
                        health = protocol.parse('etcdserverpb.RangeResponse', await etcd.call(
                            '/etcdserverpb.KV/Range', health_req.SerializeToString()))
                        if policy.kvs and health.kvs:
                            contents = json.loads(policy.kvs[0].value)
                            assert contents['createRoleClosed'] is False, contents
                            assert json.loads(health.kvs[0].value)['zoneId'] == zone_id
                            observed_health[zone_id] = (bytes(health.kvs[0].key), health.kvs[0].lease,
                                                        health.kvs[0].mod_revision)
                            break
                        await asyncio.sleep(0.1)
            # Past the original 15 s TTL, the same lease and revision must still
            # exist: no rewrite for unchanged load, repeated keepalive succeeded.
            await asyncio.sleep(17)
            for zone_id, (key, lease, revision) in observed_health.items():
                req = protocol.message('etcdserverpb.RangeRequest', key=key)
                value = protocol.parse('etcdserverpb.RangeResponse', await etcd.call(
                    '/etcdserverpb.KV/Range', req.SerializeToString()))
                assert len(value.kvs) == 1 and value.kvs[0].lease == lease and lease > 0
                assert value.kvs[0].mod_revision == revision, (zone_id, revision, value.kvs[0].mod_revision)
        finally:
            await etcd.close()
        result.update(policy_etcd=True, health_etcd=True, health_lease_renewed=True,
                      health_stable_load_coalesced=True)
    print('PASS Python portal public APIs, MySQL admin CRUD and etcd health/policy/kill-switch', flush=True)
    return result

async def verify_capacity(deps, session):
    from xuanming.portal_zone_policy import check_capacity, publish_zone
    base = f"http://{endpoint('gateway')}"
    headers = {'X-Admin-Key': ADMIN_KEY}
    async with ClientSession(base_url=base) as client:
        async with client.post('/admin/zones/1/maintenance', headers=headers,
                               json={'maintenance_msg': 'interop maintenance'}) as response:
            assert response.status == 200, await response.text()
        try:
            # Admission caches the last SQL status for one second, just like
            # the native gateway. Check the externally visible result after it expires.
            await asyncio.sleep(1.2)
            async with client.post('/api/assign-gate', json={'zone_id': 1, 'access_token': session.access_token,
                                                              'device_id': 'native-interop'}) as response:
                denied = await response.json()
                assert denied.get('code') == 503 and denied.get('error') == 'zone_maintenance', denied
        finally:
            async with client.post('/admin/zones/1/open', headers=headers) as response:
                assert response.status == 200, await response.text()
        await asyncio.sleep(1.2)
        async with client.post('/api/assign-gate', json={'zone_id': 1, 'access_token': session.access_token,
                                                          'device_id': 'native-interop'}) as response:
            admitted = await response.json()
            assert admitted.get('code') == 0, admitted
        async with client.put('/admin/zones/1/role-capacity', headers=headers,
                              json={'role_capacity': 1}) as response:
            assert response.status == 200, await response.text()
        try:
            outcome = await check_capacity(deps, 1)
            assert outcome in ('closed', 'already_closed'), outcome
            await publish_zone(deps, 1)
            # Native Login polls policy every 2 s; wait before attempting a second role.
            await asyncio.sleep(3)
            p = session.client.protocol
            reply = await session.client.rpc(p.message_ids['ClientPlayerLoginCreatePlayer'], 599,
                                             p.message('loginpb.CreatePlayerRequest', name='CapacityBlock', gender=1))
            assert reply.error_message.id, reply
        finally:
            async with client.put('/admin/zones/1/role-capacity', headers=headers,
                                  json={'role_capacity': 0}) as response:
                assert response.status == 200
            async with client.post('/admin/zones/1/create-role/open', headers=headers) as response:
                assert response.status == 200
            await publish_zone(deps, 1)
            # Native Login polls every two seconds; let the reopened policy converge
            # before the next fixture account attempts character creation.
            await asyncio.sleep(3)
    print('PASS MySQL role-cap guard published policy and native Login blocked role creation', flush=True)
    return {'guard_triggered': True, 'native_create_blocked': True, 'policy_reopened': True}


async def verify_observability(portal):
    import re
    traces = [(path, metadata.get('traceparent')) for path, metadata in OBSERVED_METADATA
              if path in ('/loginpb.ClientPlayerLogin/Login', '/loginpb.LoginPreGate/AssignGate')]
    assert {path for path, trace in traces if trace} == {
        '/loginpb.ClientPlayerLogin/Login', '/loginpb.LoginPreGate/AssignGate'}
    assert all(re.fullmatch(r'00-[0-9a-f]{32}-[0-9a-f]{16}-01', trace) for _, trace in traces)
    assert all('11111111111111111111111111111111' not in trace for _, trace in traces)
    async with ClientSession(base_url=f"http://127.0.0.1:{portal.management_port}") as client:
        for path in ('/actuator/health/liveness', '/actuator/health/readiness', '/actuator/health'):
            async with client.get(path) as response:
                assert response.status == 200 and (await response.json())['status'] == 'UP'
        async with client.get('/actuator/prometheus') as response:
            assert response.status == 200
            output = await response.text()
            assert 'portal_' in output
    print('PASS real Go Login accepted portal trace metadata; management health and metrics are live', flush=True)
    return {'real_login_trace_metadata': True, 'external_trace_replaced': True, 'health_metrics_http': True}
