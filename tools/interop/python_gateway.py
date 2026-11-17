from contextlib import AsyncExitStack
import asyncio
import secrets
from aiohttp import web
from smoke_original import PORT, RUN, endpoint
from xuanming.portal import create_app, load_settings, open_deps
from xuanming.portal_observability import create_management_app
from xuanming import portal_login

ADMIN_KEY = secrets.token_urlsafe(32)
CDN_SECRET = secrets.token_urlsafe(32)
OBSERVED_METADATA = []
_original_client = portal_login.GrpcClient

class RecordingGrpcClient(_original_client):
    async def call(self, path, data, *, metadata=()):
        OBSERVED_METADATA.append((path, dict(metadata)))
        return await super().call(path, data, metadata=metadata)

class PortalRun:
    def __init__(self, stack, deps, management_port):
        self.stack, self.deps, self.management_port = stack, deps, management_port

    async def aclose(self):
        try:
            await self.stack.aclose()
        finally:
            portal_login.GrpcClient = _original_client

async def _verify_health_removed(deps):
    for zone in deps.settings.home_zone_ids:
        prefix = f"/mmorpg/zone_health/zone/{zone}/publisher/".encode()
        end = prefix[:-1] + bytes((prefix[-1] + 1,))
        result = await deps.directory._rpc("KV", "Range", key=prefix, range_end=end)
        assert not result.kvs, f"health key for {zone} survived portal cleanup"

async def start_portal():
    config = RUN / 'python-portal.toml'
    config.write_text(f'''[portal]
host = "127.0.0.1"
port = {PORT["gateway"]}
mysql_host = "127.0.0.1"
mysql_port = {PORT["mysql"]}
mysql_user = "root"
mysql_database = "interop"
redis_url = "redis://127.0.0.1:{PORT["redis"]}/0"
etcd = "{endpoint('etcd')}"
cdn_base_url = "https://cdn.example.invalid"
cdn_sign_ttl_seconds = 3600
[portal.login_endpoints]
"1" = "{endpoint('login')}"
"2" = "{endpoint('login2')}"
[portal.hotfix_platforms.windows]
latest_version = "2.0"
min_version = "1.5"
patch_url = "https://updates.example.invalid/patch"
''', encoding='utf-8')
    settings = load_settings(config)
    settings.admin_api_key = ADMIN_KEY
    settings.cdn_signing_secret = CDN_SECRET
    settings.mysql_password = ''
    settings.home_zone_ids = [1, 2]
    stack = AsyncExitStack()
    portal_login.GrpcClient = RecordingGrpcClient
    try:
        deps = await stack.enter_async_context(open_deps(settings))
        stack.push_async_callback(_verify_health_removed, deps)
        runner = web.AppRunner(create_app(deps), access_log=None)
        await runner.setup()
        stack.push_async_callback(runner.cleanup)
        await web.TCPSite(runner, '127.0.0.1', PORT['gateway']).start()
        management = web.AppRunner(create_management_app(deps), access_log=None)
        await management.setup()
        stack.push_async_callback(management.cleanup)
        site = web.TCPSite(management, '127.0.0.1', 0)
        await site.start()
        deps.metrics.ready = True
        management_port = site._server.sockets[0].getsockname()[1]
        return PortalRun(stack, deps, management_port)
    except BaseException:
        await stack.aclose()
        portal_login.GrpcClient = _original_client
        raise
