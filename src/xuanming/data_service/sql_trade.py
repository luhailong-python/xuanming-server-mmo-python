"""角色交易冻结、排空 ACK 与过户的 SQL 存储混入（对齐 Go ``account_role_trade_store.go`` / ``account_role_store_proof.go``）。"""

import hashlib
import logging
import random
import struct

from .errors import Conflict, Full, Invalid, Missing, RetryLater
from .model import (
    EVIDENCE_INITIAL_COMMIT,
    EVIDENCE_SCENE,
    INITIAL_OWNER_EPOCH,
    INITIAL_SAVE_SEQ,
    MAX_ROLE_BYTES,
    MAX_TRADE_HOLDERS,
    MAX_UINT64,
    SCHEMA_GEN,
    TRADE_CANCELLED,
    TRADE_CANCEL_PENDING,
    TRADE_FREEZING,
    TRADE_FROZEN,
    TRADE_PREPARING,
    TRADE_TRANSFERRED,
    no_actor_freeze_evidence,
    save_floor_after,
    valid_account,
    valid_freeze_evidence,
    valid_save_floor,
    valid_trade_seal,
    valid_trade_tuple,
)

logger = logging.getLogger(__name__)

_DB_NOW_MS = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)"
_OWNERSHIP_COLUMNS = "account,role_proto,owner_revision,trade_listing_id,prepare_token"
_LOCK_OWNERSHIP = f"SELECT {_OWNERSHIP_COLUMNS} FROM account_role WHERE player_id=%s FOR UPDATE"
_TRADE_COLUMNS = (
    "player_id,seller_account,seller_owner_revision,request_digest,state,freeze_evidence,"
    "created_ms,updated_ms,required_owner_epoch,required_save_seq"
)
_TRANSFER_COLUMNS = (
    "request_digest,listing_id,player_id,seller_account,buyer_account,"
    "seller_owner_revision,buyer_owner_revision,created_ms,role_proto"
)
_DEAD_HOLDER_ACK_DIGEST = hashlib.sha256(b"account-role-dead-holder-v1").digest()
_PLAYER_STORE_PROOF_MAX_BYTES = 16 << 20
_REF_PENDING, _REF_ACTIVE = 1, 2
_MAX_REF_ROWS = 100


def _write_proof_part(digest, part: bytes) -> None:
    digest.update(struct.pack(">Q", len(part)))
    digest.update(part)


def digest_player_store_rows(schema: str, rows: list[tuple]) -> bytes:
    """按 Go account_role_store_proof.go 口径计算玩家存档行的 SHA-256 证据摘要。"""
    digest = hashlib.sha256()
    _write_proof_part(digest, b"account-role-store-proof-v1")
    _write_proof_part(digest, schema.encode("utf-8"))
    for kind, epoch, seq, schema_gen, released, holder, payload in rows:
        head = struct.pack(">IQQII4x", int(kind) & 0xFFFFFFFF, int(epoch), int(seq), int(schema_gen), int(released))
        _write_proof_part(digest, head)
        _write_proof_part(digest, bytes(holder))
        _write_proof_part(digest, bytes(payload))
    return digest.digest()


def _matches_trade(row: dict, player_id: int, seller: str, revision: int) -> bool:
    return (row["player_id"] == player_id and row["seller_account"] == seller
            and row["seller_owner_revision"] == revision)


class _SqlTradeMixin:
    """挂在 SqlStore 上的角色交易冻结、排空确认、名额检查与过户 SQL 实现。"""

    def _decode_trade_row(self, listing_id: int, raw_row) -> dict:
        (player_id, seller_b, rev, req_digest, state, evidence_b,
         created_ms, updated_ms, req_epoch, req_seq) = raw_row
        seller = bytes(seller_b).decode("utf-8")
        req_digest = bytes(req_digest)
        evidence_b = bytes(evidence_b or b"")
        if (not valid_trade_tuple(listing_id, player_id, seller, rev) or len(req_digest) != 32
                or not TRADE_FREEZING <= state <= TRADE_PREPARING
                or not valid_save_floor(req_epoch, req_seq)):
            raise RuntimeError("corrupt account_role_trade row")
        return {
            "listing_id": int(listing_id),
            "player_id": int(player_id),
            "seller_account": seller,
            "seller_owner_revision": int(rev),
            "request_digest": req_digest,
            "state": int(state),
            "freeze_evidence": evidence_b,
            "created_ms": int(created_ms),
            "updated_ms": int(updated_ms),
            "required_owner_epoch": int(req_epoch),
            "required_save_seq": int(req_seq),
        }

    async def _lock_trade(self, cursor, listing_id: int) -> dict:
        await cursor.execute(
            f"SELECT {_TRADE_COLUMNS} FROM account_role_trade WHERE listing_id=%s FOR UPDATE",
            (listing_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise Missing(listing_id)
        return self._decode_trade_row(listing_id, row)

    async def _read_trade(self, listing_id: int) -> dict | None:
        _, rows = await self._one(
            f"SELECT {_TRADE_COLUMNS} FROM account_role_trade WHERE listing_id=%s",
            (listing_id,),
        )
        return self._decode_trade_row(listing_id, rows[0]) if rows else None

    async def _read_trade_holders(self, cursor, listing_id: int) -> list[dict]:
        await cursor.execute(
            "SELECT scene_instance_id,seal_proto,drained FROM account_role_trade_holder "
            "WHERE listing_id=%s ORDER BY scene_instance_id LIMIT 257 FOR UPDATE",
            (listing_id,),
        )
        seal_type = self.protocol.message_class("common.base.RoleTradeSeal")
        out: list[dict] = []
        for instance_b, raw_seal, drained in await cursor.fetchall():
            instance = bytes(instance_b).decode("utf-8")
            raw_seal = bytes(raw_seal)
            seal = seal_type.FromString(raw_seal)
            if (len(raw_seal) > 512 or not valid_trade_seal(seal) or seal.scene_instance_id != instance
                    or seal.listing_id != listing_id or drained > 1 or len(out) >= MAX_TRADE_HOLDERS):
                raise RuntimeError("corrupt account_role_trade_holder row")
            out.append({"seal": seal, "drained": drained == 1})
        return out

    def _build_trade_receipt(self, trade: dict, holders: list[dict]) -> dict:
        ev_type = self.protocol.message_class("data_service.AccountRoleFreezeEvidence")
        ev = None
        if trade["freeze_evidence"]:
            if len(trade["freeze_evidence"]) > 1024:
                raise RuntimeError("freeze evidence too large")
            ev = ev_type.FromString(trade["freeze_evidence"])
            if (not valid_freeze_evidence(ev) or ev.player_id != trade["player_id"]
                    or ev.listing_id != trade["listing_id"]
                    or ev.owner_revision != trade["seller_owner_revision"]
                    or save_floor_after(trade["required_owner_epoch"], trade["required_save_seq"],
                                        ev.owner_epoch, ev.save_seq)):
                raise RuntimeError("corrupt freeze evidence")
        if trade["state"] in (TRADE_FROZEN, TRADE_TRANSFERRED) and ev is None:
            raise RuntimeError("missing freeze evidence on frozen/transferred trade")
        for h in holders:
            if (h["seal"].player_id != trade["player_id"]
                    or h["seal"].owner_revision != trade["seller_owner_revision"]
                    or (trade["state"] in (TRADE_FROZEN, TRADE_TRANSFERRED, TRADE_CANCELLED) and not h["drained"])):
                raise RuntimeError("trade holder state mismatch")
        if no_actor_freeze_evidence(ev) and holders:
            raise RuntimeError("no-actor freeze evidence cannot carry holders")
        return {
            "listing_id": trade["listing_id"],
            "player_id": trade["player_id"],
            "seller_account": trade["seller_account"],
            "seller_owner_revision": trade["seller_owner_revision"],
            "state": trade["state"],
            "freeze_evidence": ev,
            "holders": holders,
        }

    async def _snapshot_trade_holders(self, cursor, trade: dict) -> None:
        await cursor.execute(
            "SELECT scene_instance_id,highest_generation,closed_through,identity_proto,request_digest,"
            "required_owner_epoch,required_save_seq,close_digest,updated_ms "
            "FROM account_role_admission WHERE player_id=%s ORDER BY scene_instance_id FOR UPDATE",
            (trade["player_id"],),
        )
        identity_type = self.protocol.message_class("common.base.RoleAdmissionIdentity")
        seal_type = self.protocol.message_class("common.base.RoleTradeSeal")
        seals = []
        req_epoch, req_seq = trade["required_owner_epoch"], trade["required_save_seq"]
        for (inst_b, highest, closed, raw_id, _req_d, a_epoch, a_seq, _close_d, _upd) in await cursor.fetchall():
            inst = bytes(inst_b).decode("utf-8")
            ident = identity_type.FromString(bytes(raw_id))
            if save_floor_after(a_epoch, a_seq, req_epoch, req_seq):
                req_epoch, req_seq = int(a_epoch), int(a_seq)
            if closed == highest:
                continue
            if ident.owner_revision != trade["seller_owner_revision"] or len(seals) >= MAX_TRADE_HOLDERS:
                raise Conflict("holder set invalid or exceeds cap")
            seals.append(seal_type(
                listing_id=trade["listing_id"],
                player_id=trade["player_id"],
                owner_revision=trade["seller_owner_revision"],
                scene_instance_id=inst,
                through_generation=int(highest),
                scene_node_id=ident.scene_node_id,
                zone_id=ident.zone_id,
            ))
        for seal in seals:
            raw_seal = seal.SerializeToString()
            await cursor.execute(
                "INSERT INTO account_role_trade_holder(listing_id,scene_instance_id,seal_proto,drained,ack_digest,updated_ms) "
                f"VALUES (%s,%s,UNHEX(%s),0,'',{_DB_NOW_MS})",
                (trade["listing_id"], seal.scene_instance_id, raw_seal.hex()),
            )
        trade["required_owner_epoch"] = req_epoch
        trade["required_save_seq"] = req_seq
        await cursor.execute(
            "UPDATE account_role_trade SET required_owner_epoch=%s,required_save_seq=%s WHERE listing_id=%s",
            (req_epoch, req_seq, trade["listing_id"]),
        )

    async def begin_role_trade(self, request, digest: bytes) -> dict:
        """发起角色寄售冻结（单库版直接校验无未完成改名并进入 FREEZING + 快照 holder）。"""
        if not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision):
            raise Invalid("trade tuple")

        async def work(cursor):
            await cursor.execute(
                "INSERT INTO account_role_trade(listing_id,player_id,seller_account,seller_owner_revision,"
                "request_digest,state,freeze_evidence,created_ms,updated_ms) "
                f"VALUES (%s,%s,%s,%s,UNHEX(%s),%s,'',{_DB_NOW_MS},{_DB_NOW_MS}) "
                "ON DUPLICATE KEY UPDATE listing_id=listing_id",
                (request.listing_id, request.player_id, request.seller_account,
                 request.expected_owner_revision, digest.hex(), TRADE_FREEZING),
            )
            inserted = cursor.rowcount == 1
            trade = await self._lock_trade(cursor, request.listing_id)
            if not inserted:
                if trade["request_digest"] != digest:
                    raise Conflict("listing_id already used by different request")
                holders = await self._read_trade_holders(cursor, request.listing_id)
                return self._build_trade_receipt(trade, holders)
            await cursor.execute(_LOCK_OWNERSHIP, (request.player_id,))
            role_row = await cursor.fetchone()
            if role_row is None:
                raise Missing(request.player_id)
            owned = self._decode(request.player_id, *role_row, self.role_type)
            if (owned.account != request.seller_account or owned.owner_revision != request.expected_owner_revision
                    or owned.trade_listing_id != 0 or owned.prepare_token != 0):
                raise Conflict("role ownership changed or already frozen")
            if await self._rename_open_tx(cursor, request.player_id):
                raise RetryLater("role rename in progress")
            await cursor.execute(
                f"UPDATE account_role SET trade_listing_id=%s,prepare_token=0,fence_version=fence_version+1,"
                f"updated_ms={_DB_NOW_MS} WHERE player_id=%s",
                (request.listing_id, request.player_id),
            )
            await self._snapshot_trade_holders(cursor, trade)
            holders = await self._read_trade_holders(cursor, request.listing_id)
            return self._build_trade_receipt(trade, holders)

        return await self._tx(work)

    async def _read_store_freeze_evidence(self, trade: dict, holder_count: int):
        """事务外强读 player_store 所有行并按 Go decideFrozenEvidence 构造证据。"""
        route = await self.route_detail(trade["player_id"])
        if route is None:
            raise Missing(trade["player_id"])
        storage_id = route[1]
        pool = self.storage_pools.get(storage_id)
        if pool is None:
            raise RetryLater(f"storage pool unavailable: {storage_id}")
        _, rows = await self._one(
            "SELECT kind,owner_epoch,save_seq,schema_gen,released,holder_instance,payload "
            "FROM player_store WHERE player_id=%s ORDER BY kind",
            (trade["player_id"],),
            pool=pool,
        )
        if not rows:
            raise Missing(trade["player_id"])
        total_bytes = sum(len(r[5] or b"") + len(r[6] or b"") for r in rows)
        if total_bytes > _PLAYER_STORE_PROOF_MAX_BYTES:
            raise RuntimeError("player_store proof exceeds budget")
        if int(rows[0][0]) != 0:
            raise RuntimeError("player_store missing owner row")
        owner_epoch, save_seq, schema_gen = int(rows[0][1]), int(rows[0][2]), int(rows[0][3])
        if not valid_save_floor(owner_epoch, save_seq):
            raise RuntimeError("invalid player_store save floor")
        if schema_gen > SCHEMA_GEN:
            raise RuntimeError(f"player_store schema_gen ahead: {schema_gen}")
        if save_floor_after(trade["required_owner_epoch"], trade["required_save_seq"], owner_epoch, save_seq):
            raise Conflict("player_store behind required save floor")
        kind = EVIDENCE_SCENE
        if owner_epoch == INITIAL_OWNER_EPOCH and save_seq == INITIAL_SAVE_SEQ:
            is_initial = all(int(r[1]) == INITIAL_OWNER_EPOCH and int(r[2]) == INITIAL_SAVE_SEQ for r in rows)
            if trade["required_owner_epoch"] != 0 or trade["required_save_seq"] != 0 or holder_count != 0 or not is_initial:
                raise Conflict("initial commit evidence mismatch")
            kind = EVIDENCE_INITIAL_COMMIT
        schema_name = f"player_store_{storage_id}_db"
        digest = digest_player_store_rows(schema_name, rows)
        ev_type = self.protocol.message_class("data_service.AccountRoleFreezeEvidence")
        return ev_type(
            player_id=trade["player_id"],
            listing_id=trade["listing_id"],
            owner_revision=trade["seller_owner_revision"],
            owner_epoch=owner_epoch,
            save_seq=save_seq,
            persisted_digest=digest,
            kind=kind,
        )

    async def confirm_role_trade_freeze(self, request) -> dict:
        """核验全部 holder 已排空与 player_store 证据后推进到 FROZEN。"""
        if not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision):
            raise Invalid("trade tuple")
        pre_trade = await self._read_trade(request.listing_id)
        if pre_trade is None:
            raise Missing(request.listing_id)
        if not _matches_trade(pre_trade, request.player_id, request.seller_account, request.expected_owner_revision):
            raise Conflict("trade tuple mismatch")
        if pre_trade["state"] == TRADE_PREPARING:
            raise RetryLater("trade still preparing")
        evidence = None
        pre_holders = 0
        if pre_trade["state"] == TRADE_FREEZING:
            async def check_holders(cursor):
                t = await self._lock_trade(cursor, request.listing_id)
                holders = await self._read_trade_holders(cursor, request.listing_id)
                for h in holders:
                    if not h["drained"]:
                        raise Conflict("trade holders not drained")
                return t, len(holders)

            pre_trade, pre_holders = await self._tx(check_holders)
            evidence = await self._read_store_freeze_evidence(pre_trade, pre_holders)
        elif pre_trade["state"] != TRADE_FROZEN:
            raise Conflict("trade not freezing or frozen")

        async def commit(cursor):
            trade = await self._lock_trade(cursor, request.listing_id)
            if not _matches_trade(trade, request.player_id, request.seller_account, request.expected_owner_revision):
                raise Conflict("trade tuple mismatch")
            if trade["state"] == TRADE_PREPARING:
                raise RetryLater("trade still preparing")
            if trade["state"] == TRADE_FROZEN:
                holders = await self._read_trade_holders(cursor, request.listing_id)
                return self._build_trade_receipt(trade, holders)
            if trade["state"] != TRADE_FREEZING:
                raise Conflict("trade state mismatch")
            if evidence is None:
                raise RuntimeError("trade state regressed from FROZEN to FREEZING")
            await cursor.execute(_LOCK_OWNERSHIP, (request.player_id,))
            role_row = await cursor.fetchone()
            if role_row is None:
                raise Missing(request.player_id)
            owned = self._decode(request.player_id, *role_row, self.role_type)
            if (owned.account != request.seller_account or owned.owner_revision != request.expected_owner_revision
                    or owned.trade_listing_id != request.listing_id):
                raise Conflict("ownership changed before freeze confirm")
            holders = await self._read_trade_holders(cursor, request.listing_id)
            if any(not h["drained"] for h in holders):
                raise Conflict("trade holders not drained")
            if (len(holders) != pre_holders or not valid_freeze_evidence(evidence)
                    or save_floor_after(trade["required_owner_epoch"], trade["required_save_seq"],
                                        evidence.owner_epoch, evidence.save_seq)):
                raise RetryLater("freeze evidence stale")
            if evidence.kind == EVIDENCE_INITIAL_COMMIT:
                if trade["required_owner_epoch"] != 0 or trade["required_save_seq"] != 0 or holders:
                    raise Conflict("initial commit evidence invalid with holders")
                await cursor.execute(
                    "SELECT initial_player_commit,owner_revision FROM account_role WHERE player_id=%s FOR UPDATE",
                    (request.player_id,),
                )
                marker, rev = await cursor.fetchone()
                if marker != 1 or rev != 1 or trade["seller_owner_revision"] != 1:
                    raise Conflict("initial_player_commit mismatch")
            raw_ev = evidence.SerializeToString()
            await cursor.execute(
                f"UPDATE account_role_trade SET state=%s,freeze_evidence=UNHEX(%s),updated_ms={_DB_NOW_MS} "
                "WHERE listing_id=%s",
                (TRADE_FROZEN, raw_ev.hex(), request.listing_id),
            )
            trade["state"] = TRADE_FROZEN
            trade["freeze_evidence"] = raw_ev
            return self._build_trade_receipt(trade, holders)

        return await self._tx(commit)

    async def cancel_role_trade(self, request, begin_digest: bytes) -> dict:
        """取消角色寄售；未排空则进 CANCEL_PENDING，全排空后解锁角色并进入 CANCELLED。"""
        if not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision):
            raise Invalid("trade tuple")

        async def work(cursor):
            await cursor.execute(
                "INSERT INTO account_role_trade(listing_id,player_id,seller_account,seller_owner_revision,"
                "request_digest,state,freeze_evidence,created_ms,updated_ms) "
                f"VALUES (%s,%s,%s,%s,UNHEX(%s),%s,'',{_DB_NOW_MS},{_DB_NOW_MS}) "
                "ON DUPLICATE KEY UPDATE listing_id=listing_id",
                (request.listing_id, request.player_id, request.seller_account,
                 request.expected_owner_revision, begin_digest.hex(), TRADE_CANCELLED),
            )
            inserted = cursor.rowcount == 1
            trade = await self._lock_trade(cursor, request.listing_id)
            if inserted:
                await cursor.execute(_LOCK_OWNERSHIP, (request.player_id,))
                role_row = await cursor.fetchone()
                if role_row is None:
                    raise Missing(request.player_id)
                owned = self._decode(request.player_id, *role_row, self.role_type)
                if (owned.account != request.seller_account or owned.owner_revision != request.expected_owner_revision
                        or owned.trade_listing_id != 0 or owned.prepare_token != 0):
                    raise Conflict("role ownership changed or already frozen")
                return self._build_trade_receipt(trade, [])
            if not _matches_trade(trade, request.player_id, request.seller_account, request.expected_owner_revision):
                raise Conflict("trade tuple mismatch")
            if trade["state"] == TRADE_CANCELLED:
                holders = await self._read_trade_holders(cursor, request.listing_id)
                return self._build_trade_receipt(trade, holders)
            if trade["state"] == TRADE_TRANSFERRED:
                raise Conflict("trade already transferred")
            await cursor.execute(_LOCK_OWNERSHIP, (request.player_id,))
            role_row = await cursor.fetchone()
            if role_row is None:
                raise Missing(request.player_id)
            owned = self._decode(request.player_id, *role_row, self.role_type)
            held = owned.prepare_token if trade["state"] == TRADE_PREPARING else owned.trade_listing_id
            if (owned.account != request.seller_account or owned.owner_revision != request.expected_owner_revision
                    or held != request.listing_id):
                raise Conflict("role not locked by this listing")
            holders = await self._read_trade_holders(cursor, request.listing_id)
            if any(not h["drained"] for h in holders):
                await cursor.execute(
                    f"UPDATE account_role_trade SET state=%s,updated_ms={_DB_NOW_MS} WHERE listing_id=%s",
                    (TRADE_CANCEL_PENDING, request.listing_id),
                )
                trade["state"] = TRADE_CANCEL_PENDING
                return self._build_trade_receipt(trade, holders)
            await cursor.execute(
                f"UPDATE account_role SET trade_listing_id=0,prepare_token=0,fence_version=fence_version+1,"
                f"updated_ms={_DB_NOW_MS} WHERE player_id=%s",
                (request.player_id,),
            )
            await cursor.execute(
                f"UPDATE account_role_trade SET state=%s,updated_ms={_DB_NOW_MS} WHERE listing_id=%s",
                (TRADE_CANCELLED, request.listing_id),
            )
            trade["state"] = TRADE_CANCELLED
            return self._build_trade_receipt(trade, holders)

        return await self._tx(work)

    async def ack_role_trade_drain(self, request, digest: bytes) -> None:
        """scene 确认冻结期间本地实体与迟到回调已排空，关闭对应入场登记并置 holder drained=1。"""
        seal = request.seal
        if not valid_trade_seal(seal) or not valid_save_floor(request.required_owner_epoch, request.required_save_seq):
            raise Invalid("drain ack")

        async def work(cursor):
            trade = await self._lock_trade(cursor, seal.listing_id)
            if trade["player_id"] != seal.player_id or trade["seller_owner_revision"] != seal.owner_revision:
                raise Conflict("seal mismatch with trade")
            await cursor.execute(_LOCK_OWNERSHIP, (seal.player_id,))
            role_row = await cursor.fetchone()
            if role_row is None:
                raise Missing(seal.player_id)
            owned = self._decode(seal.player_id, *role_row, self.role_type)
            identity_type = self.protocol.message_class("common.base.RoleAdmissionIdentity")
            admission = await self._read_admission(cursor, identity_type, seal.player_id, seal.scene_instance_id)
            if admission is None:
                raise Missing("admission")
            await cursor.execute(
                "SELECT seal_proto,drained,ack_digest FROM account_role_trade_holder "
                "WHERE listing_id=%s AND scene_instance_id=%s FOR UPDATE",
                (seal.listing_id, seal.scene_instance_id),
            )
            h_row = await cursor.fetchone()
            if h_row is None:
                raise Missing("trade holder")
            raw_seal, drained, prior_digest = bytes(h_row[0]), int(h_row[1]), bytes(h_row[2] or b"")
            seal_type = self.protocol.message_class("common.base.RoleTradeSeal")
            if len(raw_seal) > 512 or seal_type.FromString(raw_seal) != seal or drained > 1:
                raise Conflict("trade holder seal mismatch")
            if drained == 1:
                if prior_digest not in (digest, _DEAD_HOLDER_ACK_DIGEST):
                    raise Conflict("ack digest mismatch")
                return
            if trade["state"] not in (TRADE_FREEZING, TRADE_CANCEL_PENDING):
                raise Conflict("trade not awaiting drain")
            if owned.owner_revision != seal.owner_revision or owned.trade_listing_id != seal.listing_id:
                raise Conflict("role ownership changed")
            ident = admission["identity"]
            if (admission["highest"] != seal.through_generation or ident.owner_revision != seal.owner_revision
                    or ident.scene_node_id != seal.scene_node_id or ident.zone_id != seal.zone_id):
                raise Conflict("admission identity mismatch")
            epoch, seq = admission["epoch"], admission["seq"]
            if save_floor_after(request.required_owner_epoch, request.required_save_seq, epoch, seq):
                epoch, seq = int(request.required_owner_epoch), int(request.required_save_seq)
            await cursor.execute(
                f"UPDATE account_role_admission SET closed_through=highest_generation,required_owner_epoch=%s,"
                f"required_save_seq=%s,updated_ms={_DB_NOW_MS} WHERE player_id=%s AND scene_instance_id=%s",
                (epoch, seq, seal.player_id, seal.scene_instance_id),
            )
            if save_floor_after(epoch, seq, trade["required_owner_epoch"], trade["required_save_seq"]):
                await cursor.execute(
                    f"UPDATE account_role_trade SET required_owner_epoch=%s,required_save_seq=%s,"
                    f"updated_ms={_DB_NOW_MS} WHERE listing_id=%s",
                    (epoch, seq, seal.listing_id),
                )
            await cursor.execute(
                f"UPDATE account_role_trade_holder SET drained=1,ack_digest=UNHEX(%s),updated_ms={_DB_NOW_MS} "
                "WHERE listing_id=%s AND scene_instance_id=%s",
                (digest.hex(), seal.listing_id, seal.scene_instance_id),
            )

        await self._tx(work)

    def _decode_transfer_row(self, tx_id: int, raw_row) -> dict:
        (req_digest, listing_id, player_id, seller_b, buyer_b,
         seller_rev, buyer_rev, created_ms, role_b) = raw_row
        seller = bytes(seller_b).decode("utf-8")
        buyer = bytes(buyer_b).decode("utf-8")
        req_digest = bytes(req_digest)
        role_b = bytes(role_b)
        role = self.role_type.FromString(role_b)
        if (len(req_digest) != 32 or not valid_trade_tuple(listing_id, player_id, seller, seller_rev)
                or not valid_account(buyer) or buyer == seller or buyer_rev != seller_rev + 1
                or len(role_b) > MAX_ROLE_BYTES or role.player_id != player_id):
            raise RuntimeError("corrupt account_role_transfer_receipt row")
        return {
            "tx_id": int(tx_id),
            "request_digest": req_digest,
            "listing_id": int(listing_id),
            "player_id": int(player_id),
            "seller_account": seller,
            "buyer_account": buyer,
            "seller_owner_revision": int(seller_rev),
            "buyer_owner_revision": int(buyer_rev),
            "created_ms": int(created_ms),
            "role": role,
        }

    async def transfer_role(self, request, digest: bytes) -> dict:
        """角色过户三步（买家预留 ref -> 提交点换归属/写收据/置 TRANSFERRED -> 激活买家 ref 并清卖家 ref）。"""
        if (not request.tx_id
                or not valid_trade_tuple(request.listing_id, request.player_id, request.seller_account, request.expected_owner_revision)
                or request.expected_owner_revision == MAX_UINT64
                or not valid_account(request.buyer_account)
                or request.buyer_account == request.seller_account):
            raise Invalid("transfer request")

        # 重放先读收据
        _, prior_rows = await self._one(
            f"SELECT {_TRANSFER_COLUMNS} FROM account_role_transfer_receipt WHERE tx_id=%s",
            (request.tx_id,),
        )
        if prior_rows:
            prior = self._decode_transfer_row(request.tx_id, prior_rows[0])
            if prior["request_digest"] != digest:
                raise Conflict("tx_id already used by different transfer")
            await self._finish_transfer_refs(prior)
            return prior

        listing = await self._read_trade(request.listing_id)
        if listing is None:
            raise Missing(request.listing_id)
        if (not _matches_trade(listing, request.player_id, request.seller_account, request.expected_owner_revision)
                or listing["state"] != TRADE_FROZEN):
            raise Conflict("listing not frozen or tuple mismatch")

        # 第 1 步：买家账号预留 PENDING ref 并核角色上限与注销状态
        nonce = random.getrandbits(63)
        async def reserve_buyer(cursor):
            deletion = await self._lock_directory(cursor, request.buyer_account, True)
            await cursor.execute(
                "SELECT player_id FROM account_role_ref WHERE account=%s ORDER BY player_id LIMIT %s",
                (request.buyer_account, _MAX_REF_ROWS + 1),
            )
            refs = [int(r[0]) for r in await cursor.fetchall()]
            if request.player_id not in refs and (len(refs) >= self.max_roles or deletion != 0):
                raise Full(request.buyer_account)
            await cursor.execute(
                "INSERT INTO account_role_ref(account,player_id,state,tx_id,listing_id,home_zone,nonce,created_ms) "
                f"VALUES (%s,%s,%s,%s,%s,0,%s,{_DB_NOW_MS}) ON DUPLICATE KEY UPDATE state=VALUES(state),"
                "tx_id=VALUES(tx_id),listing_id=VALUES(listing_id),nonce=VALUES(nonce),created_ms=VALUES(created_ms),"
                "held_ms=0,held_reason=''",
                (request.buyer_account, request.player_id, _REF_PENDING, request.tx_id, request.listing_id, nonce),
            )

        await self._tx(reserve_buyer)

        # 第 2 步：提交点
        async def commit_transfer(cursor):
            await cursor.execute(f"SELECT {_DB_NOW_MS}")
            now_ms = int((await cursor.fetchone())[0])
            await cursor.execute(
                "INSERT INTO account_role_transfer_receipt(tx_id,request_digest,listing_id,player_id,"
                "seller_account,buyer_account,seller_owner_revision,buyer_owner_revision,created_ms,role_proto) "
                "VALUES (%s,UNHEX(%s),%s,%s,%s,%s,%s,%s,%s,'') ON DUPLICATE KEY UPDATE tx_id=tx_id",
                (request.tx_id, digest.hex(), request.listing_id, request.player_id,
                 request.seller_account, request.buyer_account,
                 request.expected_owner_revision, request.expected_owner_revision + 1, now_ms),
            )
            inserted = cursor.rowcount == 1
            await cursor.execute(
                f"SELECT {_TRANSFER_COLUMNS} FROM account_role_transfer_receipt WHERE tx_id=%s FOR UPDATE",
                (request.tx_id,),
            )
            prior_row = await cursor.fetchone()
            if not inserted:
                prior = self._decode_transfer_row(request.tx_id, prior_row)
                if prior["request_digest"] != digest:
                    raise Conflict("tx_id already used by different transfer")
                return prior
            trade = await self._lock_trade(cursor, request.listing_id)
            if (not _matches_trade(trade, request.player_id, request.seller_account, request.expected_owner_revision)
                    or trade["state"] != TRADE_FROZEN):
                raise Conflict("listing not frozen")
            await cursor.execute(_LOCK_OWNERSHIP, (request.player_id,))
            role_row = await cursor.fetchone()
            if role_row is None:
                raise Missing(request.player_id)
            owned = self._decode(request.player_id, *role_row, self.role_type)
            if (owned.account != request.seller_account or owned.owner_revision != request.expected_owner_revision
                    or owned.trade_listing_id != request.listing_id):
                raise Conflict("role ownership mismatch")
            buyer_rev = owned.owner_revision + 1
            await cursor.execute(
                "UPDATE account_role SET account=%s,owner_revision=%s,trade_listing_id=0,"
                "fence_version=fence_version+1,updated_ms=%s WHERE player_id=%s",
                (request.buyer_account, buyer_rev, now_ms, request.player_id),
            )
            await cursor.execute(
                "UPDATE account_role_trade SET state=%s,updated_ms=%s WHERE listing_id=%s",
                (TRADE_TRANSFERRED, now_ms, request.listing_id),
            )
            await cursor.execute(
                "UPDATE account_role_transfer_receipt SET role_proto=UNHEX(%s) WHERE tx_id=%s",
                (owned.role.hex(), request.tx_id),
            )
            return {
                "tx_id": int(request.tx_id),
                "request_digest": digest,
                "listing_id": int(request.listing_id),
                "player_id": int(request.player_id),
                "seller_account": request.seller_account,
                "buyer_account": request.buyer_account,
                "seller_owner_revision": int(request.expected_owner_revision),
                "buyer_owner_revision": int(buyer_rev),
                "created_ms": now_ms,
                "role": self.role_type.FromString(owned.role),
            }

        try:
            receipt = await self._tx(commit_transfer)
        except (Conflict, Missing):
            await self._release_own_ref(request.buyer_account, (request.player_id, 0, nonce))
            raise
        await self._finish_transfer_refs(receipt)
        return receipt

    async def _finish_transfer_refs(self, receipt: dict) -> None:
        owned = await self._read_owner(receipt["player_id"])
        if owned is not None and owned.account == receipt["buyer_account"]:
            await self._one(
                "INSERT INTO account_role_ref(account,player_id,state,tx_id,listing_id,home_zone,nonce,created_ms) "
                f"VALUES (%s,%s,%s,0,0,0,%s,{_DB_NOW_MS}) ON DUPLICATE KEY UPDATE "
                "state=IF(tx_id IN (0,%s),VALUES(state),state),listing_id=IF(tx_id IN (0,%s),0,listing_id),"
                "nonce=IF(tx_id IN (0,%s),VALUES(nonce),nonce),held_ms=IF(tx_id IN (0,%s),0,held_ms),"
                "held_reason=IF(tx_id IN (0,%s),'',held_reason),tx_id=IF(tx_id IN (0,%s),0,tx_id)",
                (receipt["buyer_account"], receipt["player_id"], _REF_ACTIVE, random.getrandbits(63),
                 *[receipt["tx_id"]] * 6),
            )
        await self._one(
            "DELETE FROM account_role_ref WHERE account=%s AND player_id=%s",
            (receipt["seller_account"], receipt["player_id"]),
        )

    async def check_role_slot(self, account: str) -> bool:
        """只读检查账号当前存活角色数是否已满（不计陈旧 ref）。"""
        if not valid_account(account):
            raise Invalid("account")
        exists, refs = await self._read_account(account)
        if not exists:
            return False
        roles = await self._roles_of_refs(account, refs)
        return len(roles) >= self.max_roles
