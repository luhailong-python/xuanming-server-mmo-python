import asyncio
import json
from types import SimpleNamespace

from xuanming.portal_zone_policy import check_capacity, publish_zone
from xuanming.protocol import Protocol


class Db:
    def __init__(self):
        self.row = (100, b"\x00")
        self.capacity = (3, b"\x00")
        self.closed = False
        self.writes = []

    def acquire(self):
        return self

    def cursor(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def execute(self, sql, args):
        self.sql = sql
        if sql.startswith("UPDATE"):
            self.writes.append(args)
            self.rowcount = int(not self.closed and self.capacity[0] == args[1])
            self.closed |= bool(self.rowcount)

    async def fetchone(self):
        return self.capacity if "role_capacity" in self.sql else self.row

    async def commit(self):
        pass

    async def rollback(self):
        pass


class Directory:
    def __init__(self):
        self.current = b""
        self.writes = []

    async def _rpc(self, service, method, **kwargs):
        assert service == "KV"
        if method == "Range":
            return SimpleNamespace(kvs=[SimpleNamespace(value=self.current)] if self.current else [])
        self.current = kwargs["value"]
        self.writes.append(kwargs)


class Rpc:
    def __init__(self, protocol):
        self.protocol = protocol
        self.roles = 3

    async def call(self, zone, service, method, request, response):
        assert zone == 1 and service == "LoginPreGate" and method == "GetZoneRoleCount"
        assert request.zone_id == 1
        return self.protocol.message("loginpb.ZoneRoleCountResponse", roles=self.roles)


def test_policy_publish_is_idempotent_and_capacity_close_is_conditional():
    async def run():
        protocol = Protocol()
        db, directory, rpc = Db(), Directory(), Rpc(protocol)
        deps = SimpleNamespace(protocol=protocol, db=db, directory=directory, rpc=rpc)
        assert await publish_zone(deps, 1) == "written"
        assert json.loads(directory.current) == {"capacity": 100, "createRoleClosed": False}
        assert await publish_zone(deps, 1) == "unchanged" and len(directory.writes) == 1
        rpc.roles = 2
        assert await check_capacity(deps, 1) == "below" and not db.writes
        rpc.roles = 3
        assert await check_capacity(deps, 1) == "closed" and db.writes == [(1, 3)]
        assert await check_capacity(deps, 1) == "raced"
        db.capacity = (0, False)
        assert await check_capacity(deps, 1) == "no_cap"
        db.capacity = (3, b"\x01")
        assert await check_capacity(deps, 1) == "already_closed"

    asyncio.run(run())
