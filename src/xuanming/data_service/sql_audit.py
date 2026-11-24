"""快照元数据、交易流水查询与 GM 批量回收的 SQL 存储混入（对齐 Go ``snapshot_store.go`` / ``transaction_log_store.go`` / ``recall_store.go``）。"""

import logging
import pymysql

from .errors import RecallBatchExists, RecallBatchNotFound, RecallOpNotFound, RecallSourceConsumed
from .model import (
    RECALL_BATCH_FINISHED,
    RECALL_BATCH_RUNNING,
    RECALL_OP_ABORTED,
    RECALL_OP_APPLIED,
    RECALL_OP_CANCELLED,
    RECALL_OP_PENDING,
    RECALL_OP_PLANNED,
    RECALL_OP_REJECTED,
    RECALL_OP_ROLE_GONE,
)
from .recall import RECALL_AUDIT_TYPE, RECALL_STREAM_GM_RECALL, RECALL_TX_TYPE

logger = logging.getLogger(__name__)

_DUP_ENTRY = 1062
_SOURCE_CHUNK = 500
_BATCH_COLUMNS = (
    "request_id,request_digest,plan_digest,recall_kind,item_config_id,currency_type,"
    "time_start,time_end,operator,reason,peer_addr,state,player_count,op_count,open_ops,"
    "source_rows,ops_complete,ops_shortfall,ops_failed,amount_requested,amount_taken,"
    "amount_debt,amount_shortfall,cancelled_ms,created_ms,finished_ms"
)


def _marks(count: int) -> str:
    return ",".join(["%s"] * count)


def _chunks(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


class _SqlAuditMixin:
    """挂在 SqlStore 上的快照、流水与批量回收 SQL 实现。"""

    # -- snapshots -----------------------------------------------------------------------------------
    async def list_player_snapshots(self, player_id: int, before_time: int, limit: int) -> list[dict]:
        """查询玩家 source=0 的快照元数据（按 created_at 倒序，不拉大 blob）。"""
        limit = limit or 20
        if before_time > 0:
            sql = (
                "SELECT id, player_id, zone_id, snapshot_type, created_at, reason, operator, COALESCE(LENGTH(data),0) "
                "FROM player_snapshot WHERE player_id=%s AND created_at<=%s AND source=0 "
                "ORDER BY created_at DESC LIMIT %s"
            )
            args = (player_id, before_time, limit)
        else:
            sql = (
                "SELECT id, player_id, zone_id, snapshot_type, created_at, reason, operator, COALESCE(LENGTH(data),0) "
                "FROM player_snapshot WHERE player_id=%s AND source=0 "
                "ORDER BY created_at DESC LIMIT %s"
            )
            args = (player_id, limit)
        _, rows = await self._one(sql, args)
        return [
            {
                "snapshot_id": int(r[0]),
                "player_id": int(r[1]),
                "zone_id": int(r[2]),
                "type": int(r[3]),
                "created_at": int(r[4]),
                "reason": str(r[5]),
                "operator": str(r[6]),
                "data_size_bytes": int(r[7]),
            }
            for r in rows
        ]

    # -- transaction log -----------------------------------------------------------------------------
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
        """按过滤条件查询 transaction_log 并返回 (行列表, 总命中数)。"""
        if player_id > 0 and to_player_id > 0:
            raise ValueError("player_id and to_player_id are mutually exclusive")
        limit = min(limit or 100, 10_000)
        conds: list[str] = []
        args: list = []
        if to_player_id > 0:
            conds.append("to_player=%s")
            args.append(to_player_id)
        elif player_id > 0:
            conds.append("(from_player=%s OR to_player=%s)")
            args.extend([player_id, player_id])
        if time_start > 0:
            conds.append("timestamp_sec>=%s")
            args.append(time_start)
        if time_end > 0:
            conds.append("timestamp_sec<=%s")
            args.append(time_end)
        if tx_types:
            conds.append(f"tx_type IN ({_marks(len(tx_types))})")
            args.extend(tx_types)
        if correlation_ids:
            conds.append(f"correlation_id IN ({_marks(len(correlation_ids))})")
            args.extend(correlation_ids)
        if item_config_id > 0:
            conds.append("item_config_id=%s")
            args.append(item_config_id)
        if exact_currency:
            conds.extend(["item_config_id=0", "currency_type=%s"])
            args.append(currency_type)
        elif currency_type > 0:
            conds.append("currency_type=%s")
            args.append(currency_type)

        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        _, cnt_rows = await self._one(f"SELECT COUNT(*) FROM transaction_log{where}", args)
        total = int(cnt_rows[0][0]) if cnt_rows else 0
        _, rows = await self._one(
            "SELECT tx_id, timestamp_sec, tx_type, from_player, to_player, item_uuid, item_config_id, "
            "item_quantity, currency_type, currency_delta, balance_before, balance_after, correlation_id, extra "
            f"FROM transaction_log{where} ORDER BY timestamp_sec DESC LIMIT %s OFFSET %s",
            [*args, limit, offset],
        )
        out = [
            {
                "tx_id": int(r[0]),
                "timestamp": int(r[1]),
                "tx_type": int(r[2]),
                "from_player": int(r[3]),
                "to_player": int(r[4]),
                "item_uuid": int(r[5]),
                "item_config_id": int(r[6]),
                "item_quantity": int(r[7]),
                "currency_type": int(r[8]),
                "currency_delta": int(r[9]),
                "balance_before": int(r[10]),
                "balance_after": int(r[11]),
                "correlation_id": int(r[12]),
                "extra": str(r[13]),
            }
            for r in rows
        ]
        return out, total

    # -- batch recall --------------------------------------------------------------------------------
    @staticmethod
    def _row_to_batch(r) -> dict:
        return {
            "request_id": int(r[0]),
            "request_digest": bytes(r[1]),
            "plan_digest": bytes(r[2]),
            "kind": int(r[3]),
            "item_config_id": int(r[4]),
            "currency_type": int(r[5]),
            "time_start": int(r[6]),
            "time_end": int(r[7]),
            "operator": str(r[8]),
            "reason": str(r[9]),
            "peer_addr": str(r[10]),
            "state": int(r[11]),
            "player_count": int(r[12]),
            "op_count": int(r[13]),
            "open_ops": int(r[14]),
            "source_rows": int(r[15]),
            "ops_complete": int(r[16]),
            "ops_shortfall": int(r[17]),
            "ops_failed": int(r[18]),
            "amount_requested": int(r[19]),
            "amount_taken": int(r[20]),
            "amount_debt": int(r[21]),
            "amount_shortfall": int(r[22]),
            "cancelled_ms": int(r[23]),
            "created_ms": int(r[24]),
            "finished_ms": int(r[25]),
        }

    async def read_recall_batch(self, request_id: int) -> dict:
        _, rows = await self._one(f"SELECT {_BATCH_COLUMNS} FROM gm_recall_batch WHERE request_id=%s", (request_id,))
        if not rows:
            raise RecallBatchNotFound(request_id)
        return self._row_to_batch(rows[0])

    async def list_recall_ops(self, request_id: int) -> list[dict]:
        _, rows = await self._one(
            "SELECT op_id,player_id,status,currency_type,item_config_id,requested,taken,debt,shortfall,"
            "last_reason,attempts FROM gm_recall_op WHERE request_id=%s ORDER BY op_id",
            (request_id,),
        )
        return [
            {
                "op_id": int(r[0]),
                "player_id": int(r[1]),
                "status": int(r[2]),
                "currency_type": int(r[3]),
                "item_config_id": int(r[4]),
                "requested": int(r[5]),
                "taken": int(r[6]),
                "debt": int(r[7]),
                "shortfall": int(r[8]),
                "last_reason": int(r[9]),
                "attempts": int(r[10]),
            }
            for r in rows
        ]

    async def consumed_recall_sources(self, tx_ids: list[int]) -> set[int]:
        consumed: set[int] = set()
        for chunk in _chunks(list(dict.fromkeys(tx_ids)), _SOURCE_CHUNK):
            _, rows = await self._one(
                f"SELECT tx_id FROM gm_recall_source WHERE tx_id IN ({_marks(len(chunk))})", chunk,
            )
            consumed.update(int(r[0]) for r in rows)
        return consumed

    async def recall_source_state(self, tx_ids: list[int]) -> tuple[bool, bool]:
        """返回 (manual, batch) 标记，分别指示是否有流水被人工登记(op_id=0)或批次消费(op_id!=0)。"""
        manual, batch = False, False
        for chunk in _chunks(list(dict.fromkeys(tx_ids)), _SOURCE_CHUNK):
            _, rows = await self._one(
                f"SELECT op_id FROM gm_recall_source WHERE tx_id IN ({_marks(len(chunk))})", chunk,
            )
            for (op_id,) in rows:
                if int(op_id) == 0:
                    manual = True
                else:
                    batch = True
        return manual, batch

    async def insert_recall_audit(self, target_time: int, affected: int, failed: int, reason: str,
                                  operator: str, now_ms: int) -> None:
        audit_id, _ = await self.allocate(1)
        await self._one(
            "INSERT INTO rollback_audit_log(id,player_id,zone_id,rollback_type,snapshot_id_used,"
            "pre_rollback_snapshot_id,target_time,players_affected,players_failed,orphans_cleaned,"
            "reason,operator,created_at) VALUES (%s,0,0,%s,0,0,%s,%s,%s,0,%s,%s,%s)",
            (audit_id, RECALL_AUDIT_TYPE, target_time, affected, failed, reason, operator, now_ms // 1000),
        )

    async def create_recall_batch(self, batch: dict, ops: list[dict], now_ms: int) -> None:
        """原子冻结批次、全部指令行、消费来源流水与受理审计。"""
        audit_id, _ = await self.allocate(1)

        async def work(cursor):
            try:
                await cursor.execute(
                    "INSERT INTO gm_recall_batch(request_id,request_digest,plan_digest,recall_kind,item_config_id,"
                    "currency_type,time_start,time_end,operator,reason,peer_addr,state,player_count,op_count,"
                    "open_ops,source_rows,ops_complete,ops_shortfall,ops_failed,amount_requested,amount_taken,"
                    "amount_debt,amount_shortfall,cancelled_ms,created_ms,updated_ms,finished_ms) "
                    "VALUES (%s,UNHEX(%s),UNHEX(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,0,0,%s,0,0,0,0,%s,%s,0)",
                    (batch["request_id"], batch["request_digest"].hex(), batch["plan_digest"].hex(),
                     batch["kind"], batch["item_config_id"], batch["currency_type"],
                     batch["time_start"], batch["time_end"], batch["operator"], batch["reason"], batch["peer_addr"],
                     RECALL_BATCH_RUNNING, batch["player_count"], batch["op_count"], batch["op_count"],
                     batch["source_rows"], batch["amount_requested"], now_ms, now_ms),
                )
            except pymysql.err.IntegrityError as error:
                if error.args[0] == _DUP_ENTRY:
                    raise RecallBatchExists(batch["request_id"]) from error
                raise
            for op in ops:
                await cursor.execute(
                    "INSERT INTO gm_recall_op(op_id,request_id,player_id,stream,stream_epoch,seq,status,payload,"
                    "tx_type,attempts,next_attempt_ms,lease_until_ms,lease_token,last_outcome,last_reason,durable,"
                    "deadline_ms,created_ms,updated_ms,last_send_ms,recall_kind,currency_type,item_config_id,"
                    "requested,taken,debt,shortfall,resolved_by,resolve_reason) "
                    "VALUES (%s,%s,%s,%s,0,%s,%s,UNHEX(%s),%s,0,%s,0,0,0,0,0,0,%s,%s,0,%s,%s,%s,%s,0,0,0,'','')",
                    (op["op_id"], batch["request_id"], op["player_id"], RECALL_STREAM_GM_RECALL, op["op_id"],
                     RECALL_OP_PLANNED, op["payload"].hex(), RECALL_TX_TYPE, now_ms, now_ms, now_ms,
                     op["kind"], op["currency_type"], op["item_config_id"], op["requested"]),
                )
                for tx_id in op["source_tx_ids"]:
                    try:
                        await cursor.execute(
                            "INSERT INTO gm_recall_source(tx_id,request_id,player_id,op_id,created_ms) "
                            "VALUES (%s,%s,%s,%s,%s)",
                            (tx_id, batch["request_id"], op["player_id"], op["op_id"], now_ms),
                        )
                    except pymysql.err.IntegrityError as error:
                        if error.args[0] == _DUP_ENTRY:
                            raise RecallSourceConsumed(tx_id) from error
                        raise
            reason = (
                f"batch_recall ACCEPTED request_id={batch['request_id']} plan={batch['plan_digest'].hex()} "
                f"ops={batch['op_count']} players={batch['player_count']}: {batch['reason']}"
            )
            await cursor.execute(
                "INSERT INTO rollback_audit_log(id,player_id,zone_id,rollback_type,snapshot_id_used,"
                "pre_rollback_snapshot_id,target_time,players_affected,players_failed,orphans_cleaned,"
                "reason,operator,created_at) VALUES (%s,0,0,%s,0,0,%s,0,0,0,%s,%s,%s)",
                (audit_id, RECALL_AUDIT_TYPE, batch["time_start"], reason, batch["operator"], now_ms // 1000),
            )

        await self._tx(work)

    async def cancel_recall_batch(self, request_id: int, operator: str, reason: str, now_ms: int) -> None:
        """取消批次：PLANNED 直接置 CANCELLED 并释放来源流水，PENDING 设置 deadline_ms=now_ms。"""
        cancel_audit, _ = await self.allocate(1)
        finish_audit, _ = await self.allocate(1)

        async def work(cursor):
            await cursor.execute(
                f"SELECT {_BATCH_COLUMNS} FROM gm_recall_batch WHERE request_id=%s FOR UPDATE",
                (request_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RecallBatchNotFound(request_id)
            batch = self._row_to_batch(row)
            if batch["cancelled_ms"] != 0 or batch["state"] == RECALL_BATCH_FINISHED:
                return
            await cursor.execute(
                "UPDATE gm_recall_op SET status=%s,shortfall=requested,updated_ms=%s "
                "WHERE request_id=%s AND status=%s",
                (RECALL_OP_CANCELLED, now_ms, request_id, RECALL_OP_PLANNED),
            )
            unsent = cursor.rowcount
            await cursor.execute(
                "SELECT COALESCE(SUM(requested),0) FROM gm_recall_op WHERE request_id=%s AND status=%s",
                (request_id, RECALL_OP_CANCELLED),
            )
            unsent_amount = int((await cursor.fetchone())[0] or 0)
            await cursor.execute(
                "DELETE FROM gm_recall_source WHERE request_id=%s AND op_id IN "
                "(SELECT op_id FROM gm_recall_op WHERE request_id=%s AND status=%s)",
                (request_id, request_id, RECALL_OP_CANCELLED),
            )
            await cursor.execute(
                "UPDATE gm_recall_op SET deadline_ms=%s,updated_ms=%s WHERE request_id=%s AND status=%s",
                (now_ms, now_ms, request_id, RECALL_OP_PENDING),
            )
            await cursor.execute(
                "UPDATE gm_recall_batch SET cancelled_ms=%s,updated_ms=%s WHERE request_id=%s",
                (now_ms, now_ms, request_id),
            )
            audit_reason = f"batch_recall CANCELLED request_id={request_id} unsent={unsent}: {reason}"
            await cursor.execute(
                "INSERT INTO rollback_audit_log(id,player_id,zone_id,rollback_type,snapshot_id_used,"
                "pre_rollback_snapshot_id,target_time,players_affected,players_failed,orphans_cleaned,"
                "reason,operator,created_at) VALUES (%s,0,0,%s,0,0,%s,0,%s,0,%s,%s,%s)",
                (cancel_audit, RECALL_AUDIT_TYPE, batch["time_start"], unsent, audit_reason, operator, now_ms // 1000),
            )
            if unsent > 0:
                new_open = max(0, batch["open_ops"] - unsent)
                new_state = RECALL_BATCH_FINISHED if new_open == 0 else batch["state"]
                finished_ms = now_ms if new_open == 0 else batch["finished_ms"]
                await cursor.execute(
                    "UPDATE gm_recall_batch SET open_ops=%s,state=%s,finished_ms=%s,updated_ms=%s,"
                    "ops_failed=ops_failed+%s,amount_shortfall=amount_shortfall+%s WHERE request_id=%s",
                    (new_open, new_state, finished_ms, now_ms, unsent, unsent_amount, request_id),
                )
                if new_open == 0:
                    fin_reason = f"batch_recall FINISHED request_id={request_id} ops={batch['op_count']}: {batch['reason']}"
                    await cursor.execute(
                        "INSERT INTO rollback_audit_log(id,player_id,zone_id,rollback_type,snapshot_id_used,"
                        "pre_rollback_snapshot_id,target_time,players_affected,players_failed,orphans_cleaned,"
                        "reason,operator,created_at) VALUES (%s,0,0,%s,0,0,%s,0,%s,0,%s,%s,%s)",
                        (finish_audit, RECALL_AUDIT_TYPE, batch["time_start"], unsent, fin_reason,
                         batch["operator"], now_ms // 1000),
                    )

        await self._tx(work)

    async def register_recall_review_handled(
        self, player_id: int, correlation_id: int, tx_ids: list[int],
        operator: str, reason: str, detail: str, now_ms: int,
    ) -> bool:
        """把 needs_review 指令的全部流水按 op_id=0 写进 gm_recall_source 并记审计；已登记返回 False。"""
        audit_id, _ = await self.allocate(1)

        async def work(cursor):
            manual, batch = False, False
            for chunk in _chunks(list(dict.fromkeys(tx_ids)), _SOURCE_CHUNK):
                await cursor.execute(
                    f"SELECT op_id FROM gm_recall_source WHERE tx_id IN ({_marks(len(chunk))})", chunk,
                )
                for (op_id,) in await cursor.fetchall():
                    if int(op_id) == 0:
                        manual = True
                    else:
                        batch = True
            if batch:
                raise RecallSourceConsumed("consumed by recall batch")
            if manual:
                return False
            for tx_id in tx_ids:
                try:
                    await cursor.execute(
                        "INSERT INTO gm_recall_source(tx_id,request_id,player_id,op_id,created_ms) "
                        "VALUES (%s,%s,%s,0,%s)",
                        (tx_id, audit_id, player_id, now_ms),
                    )
                except pymysql.err.IntegrityError as error:
                    if error.args[0] == _DUP_ENTRY:
                        raise RecallSourceConsumed(tx_id) from error
                    raise
            audit_reason = (
                f"batch_recall REVIEW_HANDLED player={player_id} correlation={correlation_id} "
                f"sources={len(tx_ids)} {detail}: {reason}"
            )
            await cursor.execute(
                "INSERT INTO rollback_audit_log(id,player_id,zone_id,rollback_type,snapshot_id_used,"
                "pre_rollback_snapshot_id,target_time,players_affected,players_failed,orphans_cleaned,"
                "reason,operator,created_at) VALUES (%s,0,0,%s,0,0,0,0,0,0,%s,%s,%s)",
                (audit_id, RECALL_AUDIT_TYPE, audit_reason, operator, now_ms // 1000),
            )
            return True

        return await self._tx(work)

    async def read_recall_op_state(self, op_id: int) -> dict:
        _, rows = await self._one(
            "SELECT op_id,request_id,player_id,stream,seq,stream_epoch,status,requested,"
            "lease_until_ms,last_send_ms,last_reason FROM gm_recall_op WHERE op_id=%s",
            (op_id,),
        )
        if not rows:
            raise RecallOpNotFound(op_id)
        r = rows[0]
        return {
            "op_id": int(r[0]),
            "request_id": int(r[1]),
            "player_id": int(r[2]),
            "stream": int(r[3]),
            "seq": int(r[4]),
            "stream_epoch": int(r[5]),
            "status": int(r[6]),
            "requested": int(r[7]),
            "lease_until_ms": int(r[8]),
            "last_send_ms": int(r[9]),
            "last_reason": int(r[10]),
        }

    async def resolve_recall_op_manually(
        self, op_id: int, final_status: int, operator: str, reason: str,
        witness_lease_ms: int, witness_last_send_ms: int, now_ms: int,
    ) -> bool:
        """带安静见证原子终结 PENDING 回收指令（对齐 Go ResolveManuallyIfQuiet）。"""
        audit_id, _ = await self.allocate(1)

        async def work(cursor):
            await cursor.execute("SELECT request_id FROM gm_recall_op WHERE op_id=%s", (op_id,))
            req_row = await cursor.fetchone()
            if req_row is None:
                raise RecallOpNotFound(op_id)
            request_id = int(req_row[0])
            await cursor.execute(
                f"SELECT {_BATCH_COLUMNS} FROM gm_recall_batch WHERE request_id=%s FOR UPDATE",
                (request_id,),
            )
            batch = self._row_to_batch(await cursor.fetchone())
            await cursor.execute(
                "SELECT status,requested,lease_until_ms,last_send_ms FROM gm_recall_op WHERE op_id=%s FOR UPDATE",
                (op_id,),
            )
            status, requested, lease_until, last_send = await cursor.fetchone()
            if int(status) != RECALL_OP_PENDING:
                return False
            if int(lease_until) != witness_lease_ms or int(last_send) != witness_last_send_ms:
                return False
            if final_status in (RECALL_OP_REJECTED, RECALL_OP_ABORTED, RECALL_OP_ROLE_GONE):
                await cursor.execute("DELETE FROM gm_recall_source WHERE op_id=%s", (op_id,))
            await cursor.execute(
                "UPDATE gm_recall_op SET status=%s,lease_until_ms=0,updated_ms=%s,"
                "taken=0,debt=0,shortfall=%s,resolved_by=%s,resolve_reason=%s WHERE op_id=%s",
                (final_status, now_ms, requested, operator, reason, op_id),
            )
            new_open = max(0, batch["open_ops"] - 1)
            new_state = RECALL_BATCH_FINISHED if new_open == 0 else batch["state"]
            finished_ms = now_ms if new_open == 0 else batch["finished_ms"]
            await cursor.execute(
                "UPDATE gm_recall_batch SET open_ops=%s,state=%s,finished_ms=%s,updated_ms=%s,"
                "ops_failed=ops_failed+1,amount_shortfall=amount_shortfall+%s WHERE request_id=%s",
                (new_open, new_state, finished_ms, now_ms, requested, request_id),
            )
            if new_open == 0:
                fin_reason = f"batch_recall FINISHED request_id={request_id} ops={batch['op_count']}: {batch['reason']}"
                await cursor.execute(
                    "INSERT INTO rollback_audit_log(id,player_id,zone_id,rollback_type,snapshot_id_used,"
                    "pre_rollback_snapshot_id,target_time,players_affected,players_failed,orphans_cleaned,"
                    "reason,operator,created_at) VALUES (%s,0,0,%s,0,0,%s,0,1,0,%s,%s,%s)",
                    (audit_id, RECALL_AUDIT_TYPE, batch["time_start"], fin_reason, batch["operator"], now_ms // 1000),
                )
            return True

        return await self._tx(work)
