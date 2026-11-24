"""In-memory twin of ``data_service.sql_store.SqlStore`` for tests (same store interface, same outcomes).

Like ``login_fakes``: it follows the Go/SQL store semantics line by line, but real SQL byte behaviour is only
exercised by the opt-in MySQL tests in ``test_data_service.py``.
"""

from xuanming.data_service import names
from xuanming.data_service.errors import (Conflict, Full, Invalid, Missing, NameConflict, NameInUse,
                                          NameOutsideWindow, NotCommitted, ZoneConflict)
from xuanming.data_service.model import Ownership, valid_account


class MemoryStore:
    def __init__(self, protocol, *, max_roles: int = 5, storage_ids=(1_000_000,)) -> None:
        self.role_type = protocol.message_class("AccountSimplePlayer")
        self.max_roles, self.storage_ids = max_roles, set(storage_ids)
        self.accounts, self.refs, self.roles, self.routes_ = set(), {}, {}, {}
        self.merged, self.names, self.owners, self.admissions, self.next_id = {}, {}, {}, {}, 1
        self.fail_create = None  # exception raised before the create commit point
        self.fail = set()        # method names that raise an unknown (UNAVAILABLE) error

    def _check(self, method: str) -> None:
        if method in self.fail:
            raise RuntimeError(f"{method} injected failure")

    # -- account directory ---------------------------------------------------------------------------
    def _list(self, account):
        return [self.roles[pid]["role"] for pid in sorted(self.refs.get(account, ()))
                if pid in self.roles and self.roles[pid]["account"] == account]

    async def read_account(self, account, create):
        self._check("read_account")
        if not valid_account(account):
            raise Invalid("account")
        if account not in self.accounts:
            if not create:
                raise Missing(account)
            self.accounts.add(account)
        return self._list(account)

    async def create_role(self, account, role):
        self._check("create_role")
        if not valid_account(account) or not role.player_id:
            raise Invalid("role")
        old = self.roles.get(role.player_id)
        if old is not None:
            if not old["account"] or old["account"] != account or self.role_type.FromString(old["role"]) != role:
                raise Conflict("replay mismatch")
            return self._list(account)
        route = self.routes_.get(role.player_id)
        if not role.zone_id or route is None:
            raise NotCommitted("no route")
        if route[0] != role.zone_id or route[1] not in self.storage_ids:
            raise Conflict("route mismatch")
        self.accounts.add(account)
        refs = self.refs.setdefault(account, set())
        if role.player_id not in refs and len(refs) >= self.max_roles:
            raise Full(account)
        if self.fail_create is not None:
            raise self.fail_create
        refs.add(role.player_id)
        self.roles[role.player_id] = dict(account=account, role=role.SerializeToString(), rev=1, initial=True)
        for row in self.names.values():  # best-effort bind after the commit point
            if row["pid"] == role.player_id and row["name"] == role.name:
                row["bound"] = 1
        return self._list(account)

    async def lookup_role(self, player_id):
        self._check("lookup_role")
        row = self.roles.get(player_id)
        if row is None or not row["account"]:
            raise Missing(player_id)
        return Ownership(row["account"], row["role"], row["rev"], 0, 0, row["initial"])

    # -- admissions ----------------------------------------------------------------------------------
    async def register_admission(self, identity, digest):
        role = self.roles.get(identity.player_id)
        if role is None:
            raise Missing(identity.player_id)
        if role["rev"] != identity.owner_revision:
            raise Conflict("revision")
        key = identity.player_id, identity.scene_instance_id
        row = self.admissions.get(key)
        if row is None:
            self.admissions[key] = dict(highest=identity.generation, closed=0, identity=identity, digest=digest,
                                        floor=(0, 0), close_digest=b"")
            return 0
        if identity.generation <= row["closed"] or identity.generation < row["highest"]:
            raise Conflict("stale generation")
        if identity.generation == row["highest"]:
            if row["digest"] != digest:
                raise Conflict("digest")
            return row["closed"]
        old = row["identity"]
        if (old.scene_node_id, old.zone_id) != (identity.scene_node_id, identity.zone_id):
            raise Conflict("moved")
        row.update(highest=identity.generation, identity=identity, digest=digest)
        return row["closed"]

    async def close_admissions(self, request, digest):
        role = self.roles.get(request.player_id)
        if role is None:
            raise Missing(request.player_id)
        if role["rev"] > request.owner_revision:
            return request.through_generation
        if role["rev"] != request.owner_revision:
            raise Conflict("revision")
        row = self.admissions.get((request.player_id, request.scene_instance_id))
        if row is None:
            raise Missing("admission")
        if request.through_generation > row["highest"] or request.through_generation < row["closed"]:
            raise Conflict("range")
        if request.through_generation == row["closed"]:
            if row["close_digest"] != digest:
                raise Conflict("close digest")
        else:
            row["floor"] = max(row["floor"], (request.required_owner_epoch, request.required_save_seq))
            row.update(closed=request.through_generation, close_digest=digest)
        return request.through_generation

    # -- routing -------------------------------------------------------------------------------------
    async def register_zone(self, player_id, home, storage):
        self._check("register_zone")
        row = self.routes_.setdefault(player_id, [home, storage])
        if row[0] != home:
            raise ZoneConflict(row[0])

    async def routes(self, player_ids):
        self._check("routes")
        return {pid: self.routes_[pid][0] for pid in player_ids if pid in self.routes_}

    async def merged_zones(self):
        self._check("merged_zones")
        return dict(self.merged)

    async def route_detail(self, player_id):
        self._check("route_detail")
        row = self.routes_.get(player_id)
        return (int(row[0]), int(row[1])) if row is not None else None

    async def count_zone_roles(self, zones):
        self._check("count_zone_roles")
        wanted = set(zones)
        return sum(1 for home, _ in self.routes_.values() if home in wanted)

    async def role_fence_state(self, player_id):
        self._check("role_fence_state")
        role = self.roles.get(player_id)
        if role is None:
            return None
        route = self.routes_.get(player_id)
        return {
            "player_id": player_id,
            "account": role["account"],
            "owner_revision": role["rev"],
            "freeze_token": role.get("freeze_token", 0),
            "version": role.get("fence_version", 1),
            "home_zone": route[0] if route is not None else 0,
            "freeze_final": bool(role.get("freeze_final", False)),
        }

    async def verify_admission(self, identity):
        self._check("verify_admission")
        role = self.roles.get(identity.player_id)
        if role is None or not role["account"] or role["rev"] != identity.owner_revision:
            return False
        row = self.admissions.get((identity.player_id, identity.scene_instance_id))
        if row is None:
            return False
        if identity.generation != row["highest"] or identity.generation <= row["closed"]:
            return False
        return row["identity"] == identity

    async def read_store_row(self, player_id, kind):
        self._check("read_store_row")
        if player_id not in self.roles:
            return False, b""
        payloads = getattr(self, "store_rows", {})
        return True, payloads.get((player_id, kind), b"")

    # -- names ---------------------------------------------------------------------------------------
    async def reserve_name(self, player_id, name, norm, now_ms):
        self._check("reserve_name")
        owned = self.owners.get(player_id)
        if owned is not None and owned[0] != norm:
            raise NameConflict(f"player {player_id} already holds {owned[0]!r}")
        row = self.names.get(norm)
        if row is not None and row["pid"] != player_id:
            return names.RESERVE_TAKEN, row["pid"]
        if row is None:
            self.names[norm] = dict(pid=player_id, name=name, created=now_ms, bound=0)
            self.owners[player_id] = (norm, name)
        return names.RESERVE_OK, 0

    async def release_name(self, player_id, norm, min_created_ms):
        row = self.names.get(norm)
        if row is None or row["pid"] != player_id:
            return
        if row["bound"]:
            raise NameInUse("bound")
        if row["created"] < min_created_ms:
            raise NameOutsideWindow("window")
        if player_id in self.roles and self.roles[player_id]["account"]:
            raise NameInUse("committed")
        route = self.routes_.get(player_id)
        if player_id not in self.roles and route is not None:  # sealRoleTx: tombstone, the id never creates again
            self.roles[player_id] = dict(account="", role=self.role_type(player_id=player_id, zone_id=route[0])
                                         .SerializeToString(), rev=1, initial=False)
        del self.names[norm]
        self.owners.pop(player_id, None)

    async def player_names(self, player_ids):
        return {pid: self.owners[pid][1] for pid in player_ids if pid in self.owners}

    # -- id segments ---------------------------------------------------------------------------------
    async def allocate(self, step):
        self._check("allocate")
        lo, self.next_id = self.next_id, self.next_id + step
        return lo, self.next_id
