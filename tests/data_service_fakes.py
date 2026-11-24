"""``data_service.sql_store.SqlStore`` 的内存测试替身（接口与业务语义逐项对齐）。

所有单测默认跑在 ``MemoryStore`` 上；设置 ``XUANMING_TEST_MYSQL=host:port:user:password`` 时，
相同场景同时对真库 ``SqlStore`` 执行。
"""

import hashlib
import time

from xuanming.data_service import names
from xuanming.data_service.errors import (
    Conflict,
    Full,
    Invalid,
    Missing,
    NameConflict,
    NameInUse,
    NameOutsideWindow,
    NotCommitted,
    RecallBatchExists,
    RecallBatchNotFound,
    RecallOpNotFound,
    RecallSourceConsumed,
    RetryLater,
    ZoneConflict,
)
from xuanming.data_service.model import (
    AccountDirectoryRead,
    DELETION_DELETING,
    DELETION_LEASE_MS,
    DELETION_NONE,
    DELETION_OUTCOME_BLOCKED,
    DELETION_OUTCOME_NOT_FOUND,
    DELETION_OUTCOME_OK,
    DELETION_OUTCOME_TOO_LATE,
    DELETION_PENDING,
    DELETION_SOURCE_ADMIN,
    DELETION_SOURCE_USER,
    EVIDENCE_INITIAL_COMMIT,
    EVIDENCE_SCENE,
    INITIAL_OWNER_EPOCH,
    INITIAL_SAVE_SEQ,
    MAX_APPEARANCE_ID_BYTES,
    MAX_DELETION_CLAIM,
    MAX_DELETION_COOLING_OFF_MS,
    MAX_ROLE_BYTES,
    MAX_TRADE_HOLDERS,
    MAX_UINT64,
    RECALL_BATCH_FINISHED,
    RECALL_BATCH_RUNNING,
    RECALL_OP_ABORTED,
    RECALL_OP_CANCELLED,
    RECALL_OP_PENDING,
    RECALL_OP_PLANNED,
    RECALL_OP_REJECTED,
    RECALL_OP_ROLE_GONE,
    RENAME_COOLDOWN,
    RENAME_FROZEN,
    RENAME_INVALID,
    RENAME_NOT_FOUND,
    RENAME_OK,
    RENAME_SAME_NAME,
    RENAME_STALE_SEQ,
    RENAME_STATE_SETTLED,
    RENAME_TAKEN,
    SCHEMA_GEN,
    TRADE_CANCELLED,
    TRADE_CANCEL_PENDING,
    TRADE_FREEZING,
    TRADE_FROZEN,
    TRADE_PREPARING,
    TRADE_TRANSFERRED,
    Ownership,
    save_floor_after,
    valid_account,
    valid_pi,
    valid_save_floor,
    valid_trade_seal,
    valid_trade_tuple,
)
from xuanming.data_service.recall import RECALL_STREAM_GM_RECALL, RECALL_TX_TYPE
from xuanming.data_service.sql_trade import digest_player_store_rows

_DEAD_HOLDER_ACK_DIGEST = hashlib.sha256(b"account-role-dead-holder-v1").digest()


class MemoryStore:
    def __init__(self, protocol, *, max_roles: int = 5, storage_ids=(1_000_000,),
                 clock_ms=lambda: time.time_ns() // 1_000_000) -> None:
        self.protocol = protocol
        self.role_type = protocol.message_class("AccountSimplePlayer")
        self.database_type = protocol.message_class("player_database")
        self.max_roles, self.storage_ids, self.clock_ms = max_roles, set(storage_ids), clock_ms
        self.accounts: set[str] = set()
        self.account_meta: dict[str, dict] = {}
        self.deletion_receipts: dict[bytes, dict] = {}
        self.channel_identities: dict[str, tuple[str, str, int]] = {}
        self.refs: dict[str, set[int]] = {}
        self.roles: dict[int, dict] = {}
        self.routes_: dict[int, list[int]] = {}
        self.merged: dict[int, int] = {}
        self.names: dict[str, dict] = {}
        self.owners: dict[int, tuple[str, str]] = {}
        self.admissions: dict[tuple[int, str], dict] = {}
        self.trades: dict[int, dict] = {}
        self.trade_holders: dict[int, dict[str, dict]] = {}
        self.transfers: dict[int, dict] = {}
        self.renames: dict[int, dict] = {}
        self.store_rows: dict[tuple[int, int], bytes] = {}
        self.store_released: dict[int, bool] = {}
        self.store_holders: dict[int, list] = {}  # player_id -> [owner_epoch, holder_bytes, claim_seq, save_seq]
        self.snapshots: list[dict] = []
        self.tx_logs: list[dict] = []
        self.recall_batches: dict[int, dict] = {}
        self.recall_ops: dict[int, dict] = {}
        self.recall_sources: dict[int, dict] = {}
        self.audit_logs: list[dict] = []
        self.next_id = 1
        self.fail_create = None  # 在建角提交点之前注入异常
        self.fail: set[str] = set()  # 按方法名注入未知故障（UNAVAILABLE）

    def _check(self, method: str) -> None:
        if method in self.fail:
            raise RuntimeError(f"{method} injected failure")

    def _ensure_account(self, account: str) -> int:
        if account in self.accounts:
            return 0
        self.accounts.add(account)
        self.account_meta[account] = {
            "created_ms": self.clock_ms(),
            "deletion_state": DELETION_NONE,
            "deletion_requested_ms": 0,
            "deletion_execute_after_ms": 0,
            "deletion_lease_until_ms": 0,
            "deletion_source": 0,
            "real_name_pi": "",
        }
        receipt = self.deletion_receipts.get(hashlib.sha256(account.encode("utf-8")).digest())
        return receipt["deleted_ms"] if receipt else 0

    def _meta(self, account: str) -> dict:
        if account not in self.account_meta:
            self._ensure_account(account)
        return self.account_meta[account]

    # -- account directory ---------------------------------------------------------------------------
    def _list(self, account: str) -> list[bytes]:
        return [self.roles[pid]["role"] for pid in sorted(self.refs.get(account, ()))
                if pid in self.roles and self.roles[pid]["account"] == account]

    async def read_account_with_lifecycle(self, account: str, create: bool) -> AccountDirectoryRead:
        self._check("read_account")
        if not valid_account(account):
            raise Invalid("account")
        previous_deleted_ms = 0
        if account not in self.accounts:
            if not create:
                raise Missing(account)
            previous_deleted_ms = self._ensure_account(account)
        meta = self._meta(account)
        return AccountDirectoryRead(
            roles=self._list(account),
            deletion_state=meta["deletion_state"],
            deletion_execute_after_ms=meta["deletion_execute_after_ms"],
            previous_deleted_ms=previous_deleted_ms,
            real_name_pi=meta["real_name_pi"],
        )

    async def read_account(self, account: str, create: bool) -> list[bytes]:
        return (await self.read_account_with_lifecycle(account, create)).roles

    async def account_deletion_state(self, account: str) -> int:
        self._check("account_deletion_state")
        if not valid_account(account):
            raise Invalid("account")
        if account not in self.accounts:
            return DELETION_NONE
        return self._meta(account)["deletion_state"]

    async def create_role(self, account: str, role) -> list[bytes]:
        self._check("create_role")
        if not valid_account(account) or not role.player_id or role.ByteSize() > MAX_ROLE_BYTES:
            raise Invalid("role")
        old = self.roles.get(role.player_id)
        if old is not None:
            if (not old["account"] or old.get("trade_listing_id", 0) or old.get("prepare_token", 0)
                    or old["account"] != account or self.role_type.FromString(old["role"]) != role):
                raise Conflict("replay mismatch")
            return self._list(account)
        route = self.routes_.get(role.player_id)
        if not role.zone_id or route is None:
            raise NotCommitted("no route")
        if route[0] != role.zone_id or route[1] not in self.storage_ids:
            raise Conflict("route mismatch")
        self._ensure_account(account)
        meta = self._meta(account)
        refs = self.refs.setdefault(account, set())
        if role.player_id not in refs and (len(refs) >= self.max_roles or meta["deletion_state"] != DELETION_NONE):
            raise Full(account)
        if self.fail_create is not None:
            raise self.fail_create
        refs.add(role.player_id)
        self.roles[role.player_id] = dict(
            account=account, role=role.SerializeToString(), rev=1, initial=True,
            trade_listing_id=0, prepare_token=0, fence_version=1,
        )
        # 写入建角初始 player_store 写权行与资料行
        db_msg = self.database_type(
            uint32_pb_component={"class": role.class_id},
            profile_component={"name": role.name, "appearance_id": role.appearance_id, "gender": role.gender},
        )
        self.store_holders[role.player_id] = [INITIAL_OWNER_EPOCH, b"", 0, INITIAL_SAVE_SEQ]
        self.store_released[role.player_id] = True
        self.store_rows[(role.player_id, 0)] = b""
        self.store_rows[(role.player_id, 4)] = db_msg.SerializeToString(deterministic=True)
        for row in self.names.values():
            if row["pid"] == role.player_id and row["name"] == role.name:
                row["bound"] = 1
        return self._list(account)

    async def lookup_role(self, player_id: int) -> Ownership:
        self._check("lookup_role")
        if not player_id:
            raise Invalid("player_id")
        row = self.roles.get(player_id)
        if row is None or not row["account"]:
            raise Missing(player_id)
        return Ownership(row["account"], row["role"], row["rev"], row.get("trade_listing_id", 0), 0, row["initial"])

    async def repair_role(self, account: str, role, expected_owner_revision: int) -> list[bytes]:
        self._check("repair_role")
        if (not valid_account(account) or not role.player_id or role.ByteSize() > MAX_ROLE_BYTES
                or not expected_owner_revision):
            raise Invalid("repair")
        row = self.roles.get(role.player_id)
        if row is None:
            raise Missing(role.player_id)
        if (row["account"] != account or row["rev"] != expected_owner_revision
                or row.get("trade_listing_id", 0) or row.get("prepare_token", 0)):
            raise Conflict("ownership changed or frozen")
        merged = self.role_type.FromString(row["role"])
        for attr in ("class_id", "gender", "appearance_id", "name"):
            val = getattr(role, attr)
            if val:
                cur = getattr(merged, attr)
                if cur and cur != val:
                    raise Conflict(f"{attr} mismatch")
                setattr(merged, attr, val)
        row["role"] = merged.SerializeToString()
        return self._list(account)

    async def remove_roles(self, account: str, roles: list[tuple[int, int]]) -> None:
        self._check("remove_roles")
        if not valid_account(account) or not roles or len(roles) > 100:
            raise Invalid("remove")
        seen: set[int] = set()
        for pid, rev in roles:
            if not pid or not rev or pid in seen:
                raise Invalid("remove item")
            seen.add(pid)
        for pid, rev in sorted(roles, key=lambda x: x[0]):
            row = self.roles.get(pid)
            if row is None:
                continue
            if not row["account"]:
                self.refs.get(account, set()).discard(pid)
                continue
            if (row["account"] != account or row["rev"] != rev
                    or row.get("trade_listing_id", 0) or row.get("prepare_token", 0)):
                raise Conflict("ownership changed or frozen")
            for (a_pid, _inst), adm in self.admissions.items():
                if a_pid == pid and adm["closed"] < adm["highest"]:
                    raise Conflict("open admission exists")
            old_name = self.role_type.FromString(row["role"]).name
            row["account"] = ""
            row["rev"] += 1
            row["trade_listing_id"] = 0
            row["prepare_token"] = 0
            row["fence_version"] = row.get("fence_version", 1) + 1
            self.refs.get(account, set()).discard(pid)
            if old_name:
                _, norm, _ = names.normalize(old_name)
                if norm and self.names.get(norm, {}).get("pid") == pid:
                    del self.names[norm]
                self.owners.pop(pid, None)

    # -- real name -----------------------------------------------------------------------------------
    async def set_real_name(self, account: str, pi: str) -> str:
        self._check("set_real_name")
        if not valid_account(account) or not valid_pi(pi):
            raise Invalid("real_name")
        if account not in self.accounts:
            raise Missing(account)
        meta = self._meta(account)
        if not meta["real_name_pi"]:
            meta["real_name_pi"] = pi
        return meta["real_name_pi"]

    async def reset_real_name(self, account: str) -> bool:
        self._check("reset_real_name")
        if not valid_account(account):
            raise Invalid("account")
        if account not in self.accounts:
            raise Missing(account)
        meta = self._meta(account)
        had = bool(meta["real_name_pi"])
        meta["real_name_pi"] = ""
        return had

    async def get_real_name_of_player(self, player_id: int) -> tuple[bool, str]:
        self._check("get_real_name_of_player")
        if not player_id:
            raise Invalid("player_id")
        row = self.roles.get(player_id)
        if row is None or not row["account"] or row["account"] not in self.accounts:
            return False, ""
        return True, self._meta(row["account"])["real_name_pi"]

    # -- channel identity ----------------------------------------------------------------------------
    async def upsert_channel_identity(self, account: str, channel: str, open_id: str) -> None:
        self._check("upsert_channel_identity")
        if not valid_account(account) or not channel or len(channel) > 32 or len(open_id) > 191:
            raise Invalid("channel identity")
        self.channel_identities[account] = (channel, open_id, self.clock_ms())

    async def player_channel_identity(self, player_id: int) -> dict | None:
        self._check("player_channel_identity")
        row = self.roles.get(player_id)
        if row is None or not row["account"]:
            return None
        acct = row["account"]
        created_ms = self.account_meta.get(acct, {}).get("created_ms", 1)
        ch, oid, _ = self.channel_identities.get(acct, ("", "", 0))
        return {
            "account": acct,
            "channel": ch,
            "open_id": oid,
            "account_created_ms": created_ms,
            "player_level": 0,
        }

    # -- account deletion ----------------------------------------------------------------------------
    async def request_deletion(self, account: str, source: int, cooling_off_ms: int) -> dict:
        self._check("request_deletion")
        if (not valid_account(account) or source not in (DELETION_SOURCE_USER, DELETION_SOURCE_ADMIN)
                or not 0 <= cooling_off_ms <= MAX_DELETION_COOLING_OFF_MS):
            raise Invalid("request_deletion")
        if account not in self.accounts:
            return {"outcome": DELETION_OUTCOME_NOT_FOUND, "lifecycle": None, "blockers": []}
        meta = self._meta(account)
        if meta["deletion_state"] != DELETION_NONE:
            return {
                "outcome": DELETION_OUTCOME_OK,
                "lifecycle": {
                    "deletion_state": meta["deletion_state"],
                    "deletion_execute_after_ms": meta["deletion_execute_after_ms"],
                },
                "blockers": [],
            }
        blockers: list[tuple[int, str]] = []
        for pid in sorted(self.refs.get(account, ())):
            role = self.roles.get(pid)
            if role is None or role["account"] != account:
                continue
            if role.get("trade_listing_id", 0):
                blockers.append((pid, "role_listed"))
            elif role.get("prepare_token", 0):
                blockers.append((pid, "role_preparing"))
        if blockers:
            return {
                "outcome": DELETION_OUTCOME_BLOCKED,
                "lifecycle": {"deletion_state": 0, "deletion_execute_after_ms": 0},
                "blockers": blockers,
            }
        now = self.clock_ms()
        meta["deletion_state"] = DELETION_PENDING
        meta["deletion_requested_ms"] = now
        meta["deletion_execute_after_ms"] = now + cooling_off_ms
        meta["deletion_lease_until_ms"] = 0
        meta["deletion_source"] = source
        return {
            "outcome": DELETION_OUTCOME_OK,
            "lifecycle": {"deletion_state": DELETION_PENDING, "deletion_execute_after_ms": now + cooling_off_ms},
            "blockers": [],
        }

    async def cancel_deletion(self, account: str) -> dict:
        self._check("cancel_deletion")
        if not valid_account(account):
            raise Invalid("account")
        if account not in self.accounts:
            return {"outcome": DELETION_OUTCOME_NOT_FOUND, "lifecycle": None, "blockers": []}
        meta = self._meta(account)
        outcome = DELETION_OUTCOME_OK
        if meta["deletion_state"] == DELETION_PENDING:
            meta["deletion_state"] = DELETION_NONE
            meta["deletion_requested_ms"] = 0
            meta["deletion_execute_after_ms"] = 0
            meta["deletion_lease_until_ms"] = 0
            meta["deletion_source"] = 0
        elif meta["deletion_state"] == DELETION_DELETING:
            outcome = DELETION_OUTCOME_TOO_LATE
        return {
            "outcome": outcome,
            "lifecycle": {
                "deletion_state": meta["deletion_state"],
                "deletion_execute_after_ms": meta["deletion_execute_after_ms"],
            },
            "blockers": [],
        }

    async def claim_deletions(self, limit: int) -> list[str]:
        self._check("claim_deletions")
        if not 1 <= limit <= MAX_DELETION_CLAIM:
            raise Invalid("limit")
        now = self.clock_ms()
        due = [
            (meta["deletion_execute_after_ms"], acct)
            for acct, meta in self.account_meta.items()
            if acct in self.accounts
            and meta["deletion_state"] in (DELETION_PENDING, DELETION_DELETING)
            and meta["deletion_execute_after_ms"] <= now
            and meta["deletion_lease_until_ms"] <= now
        ]
        due.sort()
        claimed: list[str] = []
        for _exec_after, acct in due[:limit]:
            meta = self.account_meta[acct]
            meta["deletion_state"] = DELETION_DELETING
            meta["deletion_lease_until_ms"] = now + DELETION_LEASE_MS
            claimed.append(acct)
        return claimed

    async def finish_deletion(self, account: str) -> tuple[bool, int]:
        self._check("finish_deletion")
        if not valid_account(account):
            raise Invalid("account")
        if account not in self.accounts:
            return True, 0
        meta = self._meta(account)
        if meta["deletion_state"] != DELETION_DELETING:
            raise Conflict("account is not in deleting state")
        # 清理陈旧 ref
        refs = self.refs.get(account, set())
        stale = [pid for pid in refs if pid not in self.roles or self.roles[pid]["account"] != account]
        for pid in stale:
            refs.discard(pid)
        if refs:
            return False, len(refs)
        now = self.clock_ms()
        digest = hashlib.sha256(account.encode("utf-8")).digest()
        self.deletion_receipts[digest] = {
            "requested_ms": meta["deletion_requested_ms"],
            "deleted_ms": now,
            "source": meta["deletion_source"],
        }
        self.accounts.discard(account)
        self.account_meta.pop(account, None)
        self.refs.pop(account, None)
        return True, 0

    # -- role trade & drain ack ----------------------------------------------------------------------
    def _build_mem_trade_receipt(self, trade: dict) -> dict:
        holders = [
            {"seal": h["seal"], "drained": h["drained"]}
            for _inst, h in sorted(self.trade_holders.get(trade["listing_id"], {}).items())
        ]
        return {
            "listing_id": trade["listing_id"],
            "player_id": trade["player_id"],
            "seller_account": trade["seller_account"],
            "seller_owner_revision": trade["seller_owner_revision"],
            "state": trade["state"],
            "freeze_evidence": trade["freeze_evidence"],
            "holders": holders,
        }

    async def begin_role_trade(self, request, digest: bytes) -> dict:
        self._check("begin_role_trade")
        if not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision):
            raise Invalid("trade tuple")
        prior = self.trades.get(request.listing_id)
        if prior is not None:
            if prior["request_digest"] != digest:
                raise Conflict("listing_id already used")
            return self._build_mem_trade_receipt(prior)
        role = self.roles.get(request.player_id)
        if role is None:
            raise Missing(request.player_id)
        if (role["account"] != request.seller_account or role["rev"] != request.expected_owner_revision
                or role.get("trade_listing_id", 0) or role.get("prepare_token", 0)):
            raise Conflict("ownership changed or already frozen")
        role["trade_listing_id"] = request.listing_id
        role["prepare_token"] = 0
        role["fence_version"] = role.get("fence_version", 1) + 1
        req_epoch, req_seq = 0, 0
        holders: dict[str, dict] = {}
        seal_type = self.protocol.message_class("common.base.RoleTradeSeal")
        for (a_pid, inst), adm in sorted(self.admissions.items()):
            if a_pid != request.player_id:
                continue
            if save_floor_after(adm["floor"][0], adm["floor"][1], req_epoch, req_seq):
                req_epoch, req_seq = adm["floor"]
            if adm["closed"] == adm["highest"]:
                continue
            ident = adm["identity"]
            if ident.owner_revision != request.expected_owner_revision or len(holders) >= MAX_TRADE_HOLDERS:
                raise Conflict("holder set invalid")
            holders[inst] = {
                "seal": seal_type(
                    listing_id=request.listing_id,
                    player_id=request.player_id,
                    owner_revision=request.expected_owner_revision,
                    scene_instance_id=inst,
                    through_generation=adm["highest"],
                    scene_node_id=ident.scene_node_id,
                    zone_id=ident.zone_id,
                ),
                "drained": False,
                "ack_digest": b"",
            }
        self.trade_holders[request.listing_id] = holders
        trade = {
            "listing_id": int(request.listing_id),
            "player_id": int(request.player_id),
            "seller_account": request.seller_account,
            "seller_owner_revision": int(request.expected_owner_revision),
            "request_digest": digest,
            "state": TRADE_FREEZING,
            "freeze_evidence": None,
            "required_owner_epoch": req_epoch,
            "required_save_seq": req_seq,
        }
        self.trades[request.listing_id] = trade
        return self._build_mem_trade_receipt(trade)

    async def confirm_role_trade_freeze(self, request) -> dict:
        self._check("confirm_role_trade_freeze")
        if not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision):
            raise Invalid("trade tuple")
        trade = self.trades.get(request.listing_id)
        if trade is None:
            raise Missing(request.listing_id)
        if (trade["player_id"] != request.player_id or trade["seller_account"] != request.seller_account
                or trade["seller_owner_revision"] != request.expected_owner_revision):
            raise Conflict("trade tuple mismatch")
        if trade["state"] == TRADE_PREPARING:
            raise RetryLater("preparing")
        if trade["state"] == TRADE_FROZEN:
            return self._build_mem_trade_receipt(trade)
        if trade["state"] != TRADE_FREEZING:
            raise Conflict("trade not freezing")
        role = self.roles.get(request.player_id)
        if (role is None or role["account"] != request.seller_account
                or role["rev"] != request.expected_owner_revision
                or role.get("trade_listing_id", 0) != request.listing_id):
            raise Conflict("ownership mismatch")
        holders = self.trade_holders.get(request.listing_id, {})
        if any(not h["drained"] for h in holders.values()):
            raise Conflict("holders not drained")
        holder_row = self.store_holders.get(request.player_id)
        if holder_row is None:
            raise Missing(request.player_id)
        owner_epoch, holder_inst, _claim_seq, save_seq = holder_row
        if save_floor_after(trade["required_owner_epoch"], trade["required_save_seq"], owner_epoch, save_seq):
            raise Conflict("store behind required floor")
        kind = EVIDENCE_SCENE
        if owner_epoch == INITIAL_OWNER_EPOCH and save_seq == INITIAL_SAVE_SEQ:
            if trade["required_owner_epoch"] != 0 or trade["required_save_seq"] != 0 or holders or not role["initial"] or role["rev"] != 1:
                raise Conflict("initial commit evidence invalid")
            kind = EVIDENCE_INITIAL_COMMIT
        route = self.routes_.get(request.player_id, [1, 1_000_000])
        store_rows = sorted(
            (k, owner_epoch, save_seq, SCHEMA_GEN, int(self.store_released.get(request.player_id, True)) if k == 0 else 0,
             holder_inst if k == 0 else b"", payload)
            for (pid, k), payload in self.store_rows.items() if pid == request.player_id
        )
        if not store_rows:
            store_rows = [(0, owner_epoch, save_seq, SCHEMA_GEN, int(self.store_released.get(request.player_id, True)), holder_inst, b"")]
        digest = digest_player_store_rows(f"player_store_{route[1]}_db", store_rows)
        ev_type = self.protocol.message_class("data_service.AccountRoleFreezeEvidence")
        trade["freeze_evidence"] = ev_type(
            player_id=trade["player_id"],
            listing_id=trade["listing_id"],
            owner_revision=trade["seller_owner_revision"],
            owner_epoch=owner_epoch,
            save_seq=save_seq,
            persisted_digest=digest,
            kind=kind,
        )
        trade["state"] = TRADE_FROZEN
        return self._build_mem_trade_receipt(trade)

    async def cancel_role_trade(self, request, begin_digest: bytes) -> dict:
        self._check("cancel_role_trade")
        if not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision):
            raise Invalid("trade tuple")
        trade = self.trades.get(request.listing_id)
        if trade is None:
            role = self.roles.get(request.player_id)
            if role is None:
                raise Missing(request.player_id)
            if (role["account"] != request.seller_account or role["rev"] != request.expected_owner_revision
                    or role.get("trade_listing_id", 0) or role.get("prepare_token", 0)):
                raise Conflict("ownership changed or already frozen")
            trade = {
                "listing_id": int(request.listing_id),
                "player_id": int(request.player_id),
                "seller_account": request.seller_account,
                "seller_owner_revision": int(request.expected_owner_revision),
                "request_digest": begin_digest,
                "state": TRADE_CANCELLED,
                "freeze_evidence": None,
                "required_owner_epoch": 0,
                "required_save_seq": 0,
            }
            self.trades[request.listing_id] = trade
            self.trade_holders[request.listing_id] = {}
            return self._build_mem_trade_receipt(trade)
        if (trade["player_id"] != request.player_id or trade["seller_account"] != request.seller_account
                or trade["seller_owner_revision"] != request.expected_owner_revision):
            raise Conflict("trade tuple mismatch")
        if trade["state"] == TRADE_CANCELLED:
            return self._build_mem_trade_receipt(trade)
        if trade["state"] == TRADE_TRANSFERRED:
            raise Conflict("already transferred")
        role = self.roles.get(request.player_id)
        if role is None or role["account"] != request.seller_account or role["rev"] != request.expected_owner_revision:
            raise Conflict("ownership mismatch")
        holders = self.trade_holders.get(request.listing_id, {})
        if any(not h["drained"] for h in holders.values()):
            trade["state"] = TRADE_CANCEL_PENDING
            return self._build_mem_trade_receipt(trade)
        role["trade_listing_id"] = 0
        role["prepare_token"] = 0
        role["fence_version"] = role.get("fence_version", 1) + 1
        trade["state"] = TRADE_CANCELLED
        return self._build_mem_trade_receipt(trade)

    async def ack_role_trade_drain(self, request, digest: bytes) -> None:
        self._check("ack_role_trade_drain")
        seal = request.seal
        if not valid_trade_seal(seal) or not valid_save_floor(request.required_owner_epoch, request.required_save_seq):
            raise Invalid("drain ack")
        trade = self.trades.get(seal.listing_id)
        if trade is None:
            raise Missing(seal.listing_id)
        if trade["player_id"] != seal.player_id or trade["seller_owner_revision"] != seal.owner_revision:
            raise Conflict("seal mismatch")
        role = self.roles.get(seal.player_id)
        if role is None:
            raise Missing(seal.player_id)
        adm = self.admissions.get((seal.player_id, seal.scene_instance_id))
        if adm is None:
            raise Missing("admission")
        holder = self.trade_holders.get(seal.listing_id, {}).get(seal.scene_instance_id)
        if holder is None:
            raise Missing("trade holder")
        if holder["seal"] != seal:
            raise Conflict("seal mismatch")
        if holder["drained"]:
            if holder["ack_digest"] not in (digest, _DEAD_HOLDER_ACK_DIGEST):
                raise Conflict("ack digest mismatch")
            return
        if trade["state"] not in (TRADE_FREEZING, TRADE_CANCEL_PENDING):
            raise Conflict("trade not awaiting drain")
        if role["rev"] != seal.owner_revision or role.get("trade_listing_id", 0) != seal.listing_id:
            raise Conflict("ownership mismatch")
        epoch, seq = adm["floor"]
        if save_floor_after(request.required_owner_epoch, request.required_save_seq, epoch, seq):
            epoch, seq = int(request.required_owner_epoch), int(request.required_save_seq)
        adm["closed"] = adm["highest"]
        adm["floor"] = (epoch, seq)
        if save_floor_after(epoch, seq, trade["required_owner_epoch"], trade["required_save_seq"]):
            trade["required_owner_epoch"], trade["required_save_seq"] = epoch, seq
        holder["drained"] = True
        holder["ack_digest"] = digest

    async def transfer_role(self, request, digest: bytes) -> dict:
        self._check("transfer_role")
        if (not request.tx_id
                or not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision)
                or request.expected_owner_revision == MAX_UINT64
                or not valid_account(request.buyer_account)
                or request.buyer_account == request.seller_account):
            raise Invalid("transfer request")
        prior = self.transfers.get(request.tx_id)
        if prior is not None:
            if prior["request_digest"] != digest:
                raise Conflict("tx_id used by different request")
            return prior
        trade = self.trades.get(request.listing_id)
        if trade is None:
            raise Missing(request.listing_id)
        if (trade["player_id"] != request.player_id or trade["seller_account"] != request.seller_account
                or trade["seller_owner_revision"] != request.expected_owner_revision
                or trade["state"] != TRADE_FROZEN):
            raise Conflict("listing not frozen")
        self._ensure_account(request.buyer_account)
        buyer_meta = self._meta(request.buyer_account)
        buyer_refs = self.refs.setdefault(request.buyer_account, set())
        if request.player_id not in buyer_refs and (len(buyer_refs) >= self.max_roles or buyer_meta["deletion_state"] != DELETION_NONE):
            raise Full(request.buyer_account)
        role = self.roles.get(request.player_id)
        if (role is None or role["account"] != request.seller_account
                or role["rev"] != request.expected_owner_revision
                or role.get("trade_listing_id", 0) != request.listing_id):
            raise Conflict("ownership mismatch")
        now = self.clock_ms()
        buyer_rev = role["rev"] + 1
        role["account"] = request.buyer_account
        role["rev"] = buyer_rev
        role["trade_listing_id"] = 0
        role["fence_version"] = role.get("fence_version", 1) + 1
        trade["state"] = TRADE_TRANSFERRED
        self.refs.get(request.seller_account, set()).discard(request.player_id)
        buyer_refs.add(request.player_id)
        receipt = {
            "tx_id": int(request.tx_id),
            "request_digest": digest,
            "listing_id": int(request.listing_id),
            "player_id": int(request.player_id),
            "seller_account": request.seller_account,
            "buyer_account": request.buyer_account,
            "seller_owner_revision": int(request.expected_owner_revision),
            "buyer_owner_revision": buyer_rev,
            "created_ms": now,
            "role": self.role_type.FromString(role["role"]),
        }
        self.transfers[request.tx_id] = receipt
        return receipt

    async def check_role_slot(self, account: str) -> bool:
        self._check("check_role_slot")
        if not valid_account(account):
            raise Invalid("account")
        if account not in self.accounts:
            return False
        return len(self._list(account)) >= self.max_roles

    # -- role rename & appearance --------------------------------------------------------------------
    async def update_role_appearance(self, player_id: int, appearance_id: str) -> bool:
        self._check("update_role_appearance")
        if not player_id or not appearance_id or len(appearance_id.encode("utf-8")) > MAX_APPEARANCE_ID_BYTES:
            raise Invalid("appearance")
        row = self.roles.get(player_id)
        if row is None or not row["account"]:
            return False
        role = self.role_type.FromString(row["role"])
        if role.appearance_id == appearance_id:
            return False
        role.appearance_id = appearance_id
        row["role"] = role.SerializeToString()
        return True

    async def rename_role(
        self, player_id: int, seq: int, display: str, norm: str, *,
        invalid: bool, cooldown_ms: int, hold_ms: int, now_ms: int,
    ) -> dict:
        self._check("rename_role")
        if not player_id or not seq or cooldown_ms < 0 or hold_ms < 0:
            raise Invalid("rename")
        row = self.roles.get(player_id)
        if row is None or not row["account"]:
            return {"result": RENAME_NOT_FOUND, "current_seq": 0, "name": "", "renamed_ms": 0, "next_allowed_ms": 0}
        rec = self.renames.get(player_id)
        prev_renamed_ms = rec["renamed_ms"] if rec else 0
        if rec is not None:
            if rec["seq"] > seq or (rec["seq"] == seq and rec["new_norm"] != norm):
                return {"result": RENAME_STALE_SEQ, "current_seq": rec["seq"], "name": "", "renamed_ms": 0, "next_allowed_ms": 0}
            if rec["seq"] == seq:
                if rec["result"] == RENAME_OK:
                    return {"result": RENAME_OK, "current_seq": seq, "name": rec["new_name"],
                            "renamed_ms": rec["renamed_ms"], "next_allowed_ms": 0}
                nxt = (rec["renamed_ms"] + cooldown_ms) if rec["result"] == RENAME_COOLDOWN else 0
                return {"result": rec["result"], "current_seq": seq, "name": "", "renamed_ms": 0, "next_allowed_ms": nxt}
        role = self.role_type.FromString(row["role"])
        _, cur_norm, _ = names.normalize(role.name)
        verdict = 0
        next_allowed = 0
        if invalid:
            verdict = RENAME_INVALID
        elif row.get("trade_listing_id", 0) or row.get("prepare_token", 0):
            verdict = RENAME_FROZEN
        elif cur_norm == norm:
            verdict = RENAME_SAME_NAME
        elif prev_renamed_ms and now_ms < prev_renamed_ms + cooldown_ms:
            verdict = RENAME_COOLDOWN
            next_allowed = prev_renamed_ms + cooldown_ms
        if verdict:
            self.renames[player_id] = {
                "seq": seq, "state": 5, "result": verdict, "new_name": display, "new_norm": norm,
                "renamed_ms": prev_renamed_ms,
            }
            return {"result": verdict, "current_seq": seq, "name": "", "renamed_ms": 0, "next_allowed_ms": next_allowed}
        existing_name = self.names.get(norm)
        if existing_name is not None and existing_name["pid"] != player_id:
            held_until = existing_name.get("held_until_ms", 0)
            if held_until == 0 or held_until > now_ms:
                self.renames[player_id] = {
                    "seq": seq, "state": 5, "result": RENAME_TAKEN, "new_name": display, "new_norm": norm,
                    "renamed_ms": prev_renamed_ms,
                }
                return {"result": RENAME_TAKEN, "current_seq": seq, "name": "", "renamed_ms": 0, "next_allowed_ms": 0}
        self.names[norm] = dict(pid=player_id, name=display, created=now_ms, bound=1, held_until_ms=0)
        self.owners[player_id] = (norm, display)
        if cur_norm and cur_norm != norm and self.names.get(cur_norm, {}).get("pid") == player_id:
            self.names[cur_norm]["held_until_ms"] = max(1, now_ms + hold_ms)
        role.name = display
        row["role"] = role.SerializeToString()
        self.renames[player_id] = {
            "seq": seq, "state": RENAME_STATE_SETTLED, "result": RENAME_OK, "new_name": display,
            "new_norm": norm, "renamed_ms": now_ms,
        }
        return {"result": RENAME_OK, "current_seq": seq, "name": display, "renamed_ms": now_ms, "next_allowed_ms": 0}

    # -- player_store dead holder read / release -----------------------------------------------------
    async def read_player_store_holder(self, player_id: int) -> tuple[int, bytes, int] | None:
        self._check("read_player_store_holder")
        if not player_id:
            raise Invalid("player_id")
        if player_id not in self.routes_ or player_id not in self.store_holders:
            return None
        epoch, holder, claim_seq, _save_seq = self.store_holders[player_id]
        return int(epoch), bytes(holder), int(claim_seq)

    async def release_dead_player_store_holder(
        self, player_id: int, owner_epoch: int, seen: tuple[int, bytes, int] | None = None,
    ) -> None:
        self._check("release_dead_player_store_holder")
        if not player_id or not owner_epoch or owner_epoch == MAX_UINT64:
            raise Invalid("release_dead_holder")
        if player_id not in self.routes_ or player_id not in self.store_holders:
            return
        row = self.store_holders[player_id]
        cur = (int(row[0]), bytes(row[1]), int(row[2]))
        if cur[0] > owner_epoch:
            return
        if seen is not None and cur != seen:
            raise Conflict("player store holder changed since read")
        row[0] = owner_epoch + 1
        row[1] = b""
        self.store_released[player_id] = True

    # -- admissions ----------------------------------------------------------------------------------
    async def register_admission(self, identity, digest):
        role = self.roles.get(identity.player_id)
        if role is None:
            raise Missing(identity.player_id)
        if role["rev"] != identity.owner_revision or role.get("trade_listing_id", 0):
            raise Conflict("revision or frozen")
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
            if row["close_digest"] not in (digest, _DEAD_HOLDER_ACK_DIGEST):
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
        listing = role.get("trade_listing_id", 0)
        prepare = role.get("prepare_token", 0)
        return {
            "player_id": player_id,
            "account": role["account"],
            "owner_revision": role["rev"],
            "freeze_token": listing or prepare or role.get("freeze_token", 0),
            "version": role.get("fence_version", 1),
            "home_zone": route[0] if route is not None else 0,
            "freeze_final": bool(listing or role.get("freeze_final", False)),
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
        return True, self.store_rows.get((player_id, kind), b"")

    async def read_store_row_with_release(self, player_id, kind):
        self._check("read_store_row_with_release")
        if player_id not in self.roles:
            return False, False, b""
        released = self.store_released.get(player_id, True)
        return True, released, self.store_rows.get((player_id, kind), b"")

    async def player_displays(self, player_ids):
        self._check("player_displays")
        result = {}
        for pid in player_ids:
            row = self.roles.get(pid)
            if row is None or not row["account"]:
                continue
            role = self.role_type.FromString(row["role"])
            result[pid] = {
                "name": role.name,
                "level": 0,
                "class_id": int(role.class_id),
                "gender": int(role.gender),
                "appearance_id": role.appearance_id,
            }
        return result

    # -- snapshots, transaction log & batch recall ---------------------------------------------------
    async def list_player_snapshots(self, player_id: int, before_time: int, limit: int) -> list[dict]:
        self._check("list_player_snapshots")
        limit = limit or 20
        matched = [
            s for s in self.snapshots
            if s["player_id"] == player_id and s.get("source", 0) == 0
            and (before_time == 0 or s["created_at"] <= before_time)
        ]
        matched.sort(key=lambda s: s["created_at"], reverse=True)
        return [
            {
                "snapshot_id": s["snapshot_id"],
                "player_id": s["player_id"],
                "zone_id": s["zone_id"],
                "type": s["type"],
                "created_at": s["created_at"],
                "reason": s.get("reason", ""),
                "operator": s.get("operator", ""),
                "data_size_bytes": s.get("data_size_bytes", len(s.get("data", b""))),
            }
            for s in matched[:limit]
        ]

    async def query_transaction_log(
        self,
        *,
        player_id: int = 0,
        to_player_id: int = 0,
        time_start: int = 0,
        time_end: int = 0,
        tx_types: list[int] = (),
        item_config_id: int = 0,
        currency_type: int = 0,
        exact_currency: bool = False,
        correlation_ids: list[int] = (),
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        self._check("query_transaction_log")
        limit = min(limit or 100, 10_000)
        tx_set = set(tx_types)
        corr_set = set(correlation_ids)
        matched = []
        for r in self.tx_logs:
            if to_player_id > 0 and r["to_player"] != to_player_id:
                continue
            if player_id > 0 and r["from_player"] != player_id and r["to_player"] != player_id:
                continue
            if time_start > 0 and r["timestamp"] < time_start:
                continue
            if time_end > 0 and r["timestamp"] > time_end:
                continue
            if tx_set and r["tx_type"] not in tx_set:
                continue
            if corr_set and r["correlation_id"] not in corr_set:
                continue
            if item_config_id > 0 and r["item_config_id"] != item_config_id:
                continue
            if exact_currency and (r["item_config_id"] != 0 or r["currency_type"] != currency_type):
                continue
            if not exact_currency and currency_type > 0 and r["currency_type"] != currency_type:
                continue
            matched.append(dict(r))
        matched.sort(key=lambda r: r["timestamp"], reverse=True)
        return matched[offset:offset + limit], len(matched)

    async def read_recall_batch(self, request_id: int) -> dict:
        self._check("read_recall_batch")
        batch = self.recall_batches.get(request_id)
        if batch is None:
            raise RecallBatchNotFound(request_id)
        return dict(batch)

    async def list_recall_ops(self, request_id: int) -> list[dict]:
        self._check("list_recall_ops")
        ops = [dict(op) for op in self.recall_ops.values() if op["request_id"] == request_id]
        ops.sort(key=lambda op: op["op_id"])
        return ops

    async def consumed_recall_sources(self, tx_ids: list[int]) -> set[int]:
        self._check("consumed_recall_sources")
        return {tx_id for tx_id in tx_ids if tx_id in self.recall_sources}

    async def recall_source_state(self, tx_ids: list[int]) -> tuple[bool, bool]:
        self._check("recall_source_state")
        manual, batch = False, False
        for tx_id in tx_ids:
            row = self.recall_sources.get(tx_id)
            if row is not None:
                if row["op_id"] == 0:
                    manual = True
                else:
                    batch = True
        return manual, batch

    async def insert_recall_audit(self, target_time: int, affected: int, failed: int, reason: str,
                                  operator: str, now_ms: int) -> None:
        self.audit_logs.append(dict(
            target_time=target_time, affected=affected, failed=failed, reason=reason, operator=operator, now_ms=now_ms,
        ))

    async def create_recall_batch(self, batch: dict, ops: list[dict], now_ms: int) -> None:
        self._check("create_recall_batch")
        if batch["request_id"] in self.recall_batches:
            raise RecallBatchExists(batch["request_id"])
        for op in ops:
            for tx_id in op["source_tx_ids"]:
                if tx_id in self.recall_sources:
                    raise RecallSourceConsumed(tx_id)
        self.recall_batches[batch["request_id"]] = {
            **batch,
            "state": RECALL_BATCH_RUNNING,
            "open_ops": batch["op_count"],
            "ops_complete": 0,
            "ops_shortfall": 0,
            "ops_failed": 0,
            "amount_taken": 0,
            "amount_debt": 0,
            "amount_shortfall": 0,
            "cancelled_ms": 0,
            "created_ms": now_ms,
            "finished_ms": 0,
        }
        for op in ops:
            self.recall_ops[op["op_id"]] = {
                "op_id": op["op_id"],
                "request_id": batch["request_id"],
                "player_id": op["player_id"],
                "stream": RECALL_STREAM_GM_RECALL,
                "seq": op["op_id"],
                "stream_epoch": 0,
                "status": RECALL_OP_PLANNED,
                "payload": op["payload"],
                "tx_type": RECALL_TX_TYPE,
                "attempts": 0,
                "lease_until_ms": 0,
                "last_send_ms": 0,
                "last_reason": 0,
                "deadline_ms": 0,
                "currency_type": op["currency_type"],
                "item_config_id": op["item_config_id"],
                "requested": op["requested"],
                "taken": 0,
                "debt": 0,
                "shortfall": 0,
                "resolved_by": "",
                "resolve_reason": "",
            }
            for tx_id in op["source_tx_ids"]:
                self.recall_sources[tx_id] = {
                    "request_id": batch["request_id"], "player_id": op["player_id"], "op_id": op["op_id"],
                }

    async def cancel_recall_batch(self, request_id: int, operator: str, reason: str, now_ms: int) -> None:
        self._check("cancel_recall_batch")
        batch = self.recall_batches.get(request_id)
        if batch is None:
            raise RecallBatchNotFound(request_id)
        if batch["cancelled_ms"] != 0 or batch["state"] == RECALL_BATCH_FINISHED:
            return
        unsent, unsent_amount = 0, 0
        cancelled_ops: set[int] = set()
        for op in self.recall_ops.values():
            if op["request_id"] != request_id:
                continue
            if op["status"] == RECALL_OP_PLANNED:
                op["status"] = RECALL_OP_CANCELLED
                op["shortfall"] = op["requested"]
                unsent += 1
                unsent_amount += op["requested"]
                cancelled_ops.add(op["op_id"])
            elif op["status"] == RECALL_OP_PENDING:
                op["deadline_ms"] = now_ms
        for tx_id in [k for k, v in self.recall_sources.items() if v["op_id"] in cancelled_ops]:
            del self.recall_sources[tx_id]
        batch["cancelled_ms"] = now_ms
        if unsent > 0:
            batch["open_ops"] = max(0, batch["open_ops"] - unsent)
            batch["ops_failed"] += unsent
            batch["amount_shortfall"] += unsent_amount
            if batch["open_ops"] == 0:
                batch["state"] = RECALL_BATCH_FINISHED
                batch["finished_ms"] = now_ms

    async def register_recall_review_handled(
        self, player_id: int, correlation_id: int, tx_ids: list[int],
        operator: str, reason: str, detail: str, now_ms: int,
    ) -> bool:
        self._check("register_recall_review_handled")
        manual, batch = await self.recall_source_state(tx_ids)
        if batch:
            raise RecallSourceConsumed("batch consumed")
        if manual:
            return False
        audit_id, _ = await self.allocate(1)
        for tx_id in tx_ids:
            self.recall_sources[tx_id] = {"request_id": audit_id, "player_id": player_id, "op_id": 0}
        self.audit_logs.append(dict(
            audit_id=audit_id, player_id=player_id, correlation_id=correlation_id,
            operator=operator, reason=reason, detail=detail, now_ms=now_ms,
        ))
        return True

    async def read_recall_op_state(self, op_id: int) -> dict:
        self._check("read_recall_op_state")
        op = self.recall_ops.get(op_id)
        if op is None:
            raise RecallOpNotFound(op_id)
        return dict(op)

    async def resolve_recall_op_manually(
        self, op_id: int, final_status: int, operator: str, reason: str,
        witness_lease_ms: int, witness_last_send_ms: int, now_ms: int,
    ) -> bool:
        self._check("resolve_recall_op_manually")
        op = self.recall_ops.get(op_id)
        if op is None:
            raise RecallOpNotFound(op_id)
        if op["status"] != RECALL_OP_PENDING:
            return False
        if op["lease_until_ms"] != witness_lease_ms or op["last_send_ms"] != witness_last_send_ms:
            return False
        if final_status in (RECALL_OP_REJECTED, RECALL_OP_ABORTED, RECALL_OP_ROLE_GONE):
            for tx_id in [k for k, v in self.recall_sources.items() if v["op_id"] == op_id]:
                del self.recall_sources[tx_id]
        op["status"] = final_status
        op["lease_until_ms"] = 0
        op["taken"] = 0
        op["debt"] = 0
        op["shortfall"] = op["requested"]
        op["resolved_by"] = operator
        op["resolve_reason"] = reason
        batch = self.recall_batches[op["request_id"]]
        batch["open_ops"] = max(0, batch["open_ops"] - 1)
        batch["ops_failed"] += 1
        batch["amount_shortfall"] += op["requested"]
        if batch["open_ops"] == 0:
            batch["state"] = RECALL_BATCH_FINISHED
            batch["finished_ms"] = now_ms
        return True

    # -- names ---------------------------------------------------------------------------------------
    async def reserve_name(self, player_id, name, norm, now_ms):
        self._check("reserve_name")
        owned = self.owners.get(player_id)
        if owned is not None and owned[0] != norm:
            raise NameConflict(f"player {player_id} already holds {owned[0]!r}")
        row = self.names.get(norm)
        if row is not None and row["pid"] != player_id:
            held_until = row.get("held_until_ms", 0)
            if held_until == 0 or held_until > now_ms:
                return names.RESERVE_TAKEN, row["pid"]
            self.names[norm] = dict(pid=player_id, name=name, created=now_ms, bound=0, held_until_ms=0)
            self.owners[player_id] = (norm, name)
            return names.RESERVE_OK, 0
        if row is None:
            self.names[norm] = dict(pid=player_id, name=name, created=now_ms, bound=0, held_until_ms=0)
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
        if player_id not in self.roles and route is not None:  # sealRoleTx: 写墓碑，永不再建角
            self.roles[player_id] = dict(
                account="", role=self.role_type(player_id=player_id, zone_id=route[0]).SerializeToString(),
                rev=1, initial=False, trade_listing_id=0, prepare_token=0, fence_version=1,
            )
        del self.names[norm]
        self.owners.pop(player_id, None)

    async def player_names(self, player_ids):
        return {pid: self.owners[pid][1] for pid in player_ids if pid in self.owners}

    # -- id segments ---------------------------------------------------------------------------------
    async def allocate(self, step):
        self._check("allocate")
        lo, self.next_id = self.next_id, self.next_id + step
        return lo, self.next_id
