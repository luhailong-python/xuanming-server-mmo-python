"""Scene 收件箱消费者。只有已持久化的游标才可用于确认投递。"""

import asyncio
import logging
import sqlite3
import time
from contextlib import closing
from inspect import isawaitable
from itertools import count

from google.protobuf.message import DecodeError

from .battle_control import sign_service_call
from .battle_settlement import BAG_SPACE_DEFERRED, known_message

logger = logging.getLogger(__name__)
_SERVICE = "/player_inbox.PlayerInbox"
GM, BATTLE = 1, 2
APPLIED, REJECTED, SKIPPED, UNKNOWN = 1, 2, 3, 4
MAX_RECENT = 32


class InboxPeer:
    def __init__(self, protocol, call, node_id: int, secret: bytes):
        self.protocol, self.call, self.node_id, self.secret = protocol, call, node_id, secret

    async def _send(self, method, name, player, domain, correlation, **fields):
        identity = player.role_identity
        if identity is None or identity.owner_epoch != player.snapshot.owner_epoch:
            raise ValueError("角色未确认 SQL 入场身份")
        path = f"{_SERVICE}/{method}"
        req = self.protocol.message(
            f"player_inbox.{name}PlayerInboxRequest",
            player_id=player.player_id, domain=domain, owner_epoch=identity.owner_epoch,
            correlation_id=correlation, scene_node_id=self.node_id, admission=identity, **fields,
        )
        sign_service_call(path, req, "scene", time.time_ns() // 1_000_000, self.secret)
        raw = await self.call(path, req.SerializeToString(deterministic=True))
        return self.protocol.parse(f"player_inbox.{name}PlayerInboxResponse", raw)

    async def fetch(self, player, domain, correlation, cursor):
        return await self._send("FetchItems", "Fetch", player, domain, correlation,
                                consumer_decided_through=cursor, max_items=1 if domain == BATTLE else 16)

    async def ack(self, player, domain, correlation, reports):
        return await self._send("AckItems", "Ack", player, domain, correlation, decisions=reports)


class LocalInbox:
    """单机模式的持久终局入口，供本地 Battle 使用同一消费者。"""

    def __init__(self, protocol, database):
        self.protocol, self.database = protocol, database

    def _connect(self):
        connection = sqlite3.connect(self.database, timeout=10, isolation_level=None)
        try:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS local_battle_inbox (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    player TEXT NOT NULL,
                    battle TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    applied INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(player, battle)
                )
            """)
            return connection
        except BaseException:
            connection.close()
            raise

    async def enqueue(self, settlements) -> None:
        entries = [
            (str(item.player_id), str(item.battle_id), item.SerializeToString(deterministic=True))
            for item in settlements
        ]
        await asyncio.to_thread(self._enqueue, entries)

    def _enqueue(self, entries) -> None:
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            for player, battle, payload in entries:
                row = conn.execute(
                    "SELECT payload FROM local_battle_inbox WHERE player=? AND battle=?", (player, battle),
                ).fetchone()
                if row is not None and row[0] != payload:
                    raise ValueError("本地结算原件冲突")
                if row is None:
                    conn.execute("INSERT INTO local_battle_inbox(player,battle,payload) VALUES (?,?,?)",
                                 (player, battle, payload))

    async def fetch(self, player, domain, correlation, cursor):
        rows = await asyncio.to_thread(self._fetch, player.player_id) if domain == BATTLE else []
        reply = self.protocol.message("player_inbox.FetchPlayerInboxResponse",
                                      player_id=player.player_id, domain=domain, correlation_id=correlation, fence=3)
        for seq, battle, payload in rows[:1]:
            reply.items.add(seq=seq, request_id=int(battle), msg_type=4, payload=payload)
        reply.has_more = len(rows) > 1
        return reply

    def _fetch(self, player_id):
        with closing(self._connect()) as conn:
            return conn.execute("""
                SELECT seq, battle, payload FROM local_battle_inbox
                WHERE player=? AND applied=0 ORDER BY seq LIMIT 2
            """, (str(player_id),)).fetchall()

    async def ack(self, player, domain, correlation, reports):
        decisions = [(r.seq, str(r.request_id), r.outcome) for r in reports]
        results = await asyncio.to_thread(self._ack, player.player_id, decisions)
        reply = self.protocol.message("player_inbox.AckPlayerInboxResponse",
                                      player_id=player.player_id, domain=domain, correlation_id=correlation, fence=3)
        for seq, result, state in results:
            reply.results.add(seq=seq, result=result, state=state)
        return reply

    def _ack(self, player_id, decisions):
        results = []
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            for seq, battle, outcome in decisions:
                row = conn.execute(
                    "SELECT battle,applied FROM local_battle_inbox WHERE seq=? AND player=?", (seq, str(player_id)),
                ).fetchone()
                if row is None:
                    results.append((seq, 4, 0))
                elif row[0] != battle or outcome != APPLIED:
                    results.append((seq, 7, 2 if row[1] else 1))
                else:
                    if not row[1]:
                        conn.execute("UPDATE local_battle_inbox SET applied=1 WHERE seq=?", (seq,))
                    results.append((seq, 2 if row[1] else 1, 2))
        return results


class VersionSkew(ValueError):
    pass


class SceneInbox:
    def __init__(self, scene, peer, settlement, assets, *, poll_seconds=60, clock=time.monotonic):
        self.scene, self.protocol, self.peer = scene, scene.protocol, peer
        self.settlement, self.assets = settlement, assets
        self._poll_seconds, self._clock = poll_seconds, clock
        self._due = {}
        self._jobs = {}
        self._halted = set()
        self._bag_deferred = {}
        self._published = {}
        self._serial = count(1)
        self._stopping = False

    def landed(self, player):
        for domain in (BATTLE, GM):
            self.wake(player.player_id, domain)

    def departed(self, player):
        for domain in (BATTLE, GM):
            key = player.player_id, domain
            self._due.pop(key, None)
            self._halted.discard(key)
            self._bag_deferred.pop(key, None)
        self._published.pop(player.entity, None)

    def wake(self, player_id, domain):
        if domain in (GM, BATTLE) and player_id in self.scene.locations:
            self._due[(player_id, domain)] = self._clock()

    def on_bag_space_freed(self, player):
        if self._player(player.player_id) is not player:
            return
        for domain in (BATTLE, GM):
            key = player.player_id, domain
            if self._bag_deferred.get(key) is player and key not in self._halted:
                self._due[key] = self._clock()

    def tick(self):
        if self._stopping:
            return
        now = self._clock()
        for key, due in tuple(self._due.items()):
            if len(self._jobs) >= 16:
                return
            if due <= now and key not in self._jobs and key not in self._halted:
                self._due[key] = now + self._poll_seconds
                self._jobs[key] = asyncio.create_task(self._poll(*key))

    def _player(self, player_id):
        world = self.scene.locations.get(player_id)
        return world.players.get(player_id) if world is not None else None

    @staticmethod
    def _identity(player):
        return player.entity, player.session_id, player.generation, player.snapshot.owner_epoch

    def _available(self, player, domain):
        return player and self.scene.mutable(player) and not (
            domain == GM and self.scene.battle and self.scene.battle.frozen(player.player_id)
        )

    async def _poll(self, player_id, domain) -> None:
        key = player_id, domain
        self._bag_deferred.pop(key, None)
        identity = None
        try:
            async with self.scene._lock(player_id):
                player = self._player(player_id)
                if player is None:
                    self._due.pop(key, None)
                    return
                if not self._available(player, domain):
                    self._due[key] = self._clock() + 2
                    return
                await self.scene.flush_player_rows(player)
                cursor = self._cursor(self.scene.rows.assemble(player.snapshot), domain)
                through = cursor.decided_through_seq if cursor is not None else 0
                identity = self._identity(player)
            correlation = next(self._serial)
            fetched = await self.peer.fetch(player, domain, correlation, through)
            self._check_reply(fetched, player_id, domain, correlation)
            if not fetched.items:
                return
            if fetched.fence == 1 or len(fetched.items) > (1 if domain == BATTLE else 16) or any(
                a.seq >= b.seq for a, b in zip(fetched.items, fetched.items[1:])
            ):
                raise VersionSkew("收件箱回包序号无序")
            reports = []
            async with self.scene._lock(player_id):
                if self._player(player_id) is not player or self._identity(player) != identity or not self._available(player, domain):
                    return
                await self.scene.flush_player_rows(player)
                for item in fetched.items:
                    report = await self._consume(player, domain, item)
                    if report is None:
                        self._due[key] = self._clock() + 2
                        break
                    reports.append(report)
            if reports and self._player(player_id) is player and self._identity(player) == identity:
                correlation = next(self._serial)
                ack = await self.peer.ack(player, domain, correlation, reports)
                self._check_reply(ack, player_id, domain, correlation)
                if len(ack.results) != len(reports) or any(
                    result.seq != report.seq or result.result not in (1, 2) or result.state != report.outcome + 1
                    for result, report in zip(ack.results, reports)
                ):
                    raise VersionSkew("收件箱确认回包与持久账本不一致")
                if fetched.has_more:
                    self._due[key] = self._clock() + 1
        except VersionSkew as error:
            current = self._player(player_id)
            if current is not None and (identity is None or self._identity(current) == identity):
                self._halted.add(key)
                logger.error("收件箱域停用 player_id=%s domain=%s reason=%s", player_id, domain, error)
        except Exception:
            if player_id in self.scene.locations:
                self._due[key] = self._clock() + 5
            logger.warning("收件箱处理待重试 player_id=%s domain=%s", player_id, domain, exc_info=True)
        finally:
            self._jobs.pop(key, None)

    def _check_reply(self, reply, player_id, domain, correlation):
        if (reply.player_id, reply.domain, reply.correlation_id) != (player_id, domain, correlation):
            raise VersionSkew("收件箱回包身份不一致")
        if reply.error_message.id not in (0, 1000) or reply.fence not in (1, 2, 3, 4):
            raise RuntimeError("收件箱拒绝当前写权")

    def _cursor(self, data, domain):
        domains = {}
        for cursor in data.inbox_ledger.cursors:
            if not cursor.domain or cursor.domain in domains:
                raise VersionSkew("收件箱持久账本包含零值或重复域")
            if (
                len(cursor.recent) > MAX_RECENT
                or any(a.seq >= b.seq for a, b in zip(cursor.recent, cursor.recent[1:]))
                or any(not entry.seq or entry.seq > cursor.decided_through_seq
                       or entry.outcome not in (APPLIED, REJECTED, SKIPPED) for entry in cursor.recent)
            ):
                raise VersionSkew("收件箱持久账本不可解释")
            domains[cursor.domain] = cursor
        cursor = domains.get(domain)
        if cursor is not None and not known_message(cursor):
            raise VersionSkew("收件箱当前域包含未知字段")
        return cursor

    async def _consume(self, player, domain, item):
        key = player.player_id, domain
        self._bag_deferred.pop(key, None)
        if not item.seq or not item.request_id or domain == BATTLE and (item.msg_type != 4 or item.directive):
            raise VersionSkew("收件箱条目与域契约不符")
        data = self.scene.rows.assemble(player.snapshot)
        cursor = self._cursor(data, domain)
        settlement = None
        audits = []
        if domain == BATTLE:
            settlement = self._payload("BattleSettlementData", item, 16 << 10)
            if (settlement.battle_id != item.request_id or settlement.player_id != player.player_id
                    or settlement.outcome not in (1, 2, 3, 4)):
                raise VersionSkew("战斗结算原件与收件箱身份不符")
        if cursor is not None and item.seq <= cursor.decided_through_seq:
            saved = next((entry for entry in cursor.recent if entry.seq == item.seq), None)
            if saved is None:
                if domain == BATTLE:
                    raise VersionSkew("战斗结算必须回放原 APPLIED 结局")
                return self._report(item, UNKNOWN, 0)
            if ((saved.request_id, saved.msg_type) != (item.request_id, item.msg_type)
                    or domain == BATTLE and saved.outcome != APPLIED):
                raise VersionSkew("同一序号对应不同原件")
            if settlement is not None:
                await self._publish(player, settlement)
            return self._report(item, saved.outcome, saved.reason)
        if settlement is not None:
            duplicate = cursor is not None and any(
                entry.request_id == item.request_id and entry.outcome == APPLIED and entry.msg_type == 4
                for entry in cursor.recent
            )
            if not duplicate:
                if settlement.outcome != 4 and self.settlement.currency_version_skew(settlement, data):
                    raise VersionSkew("战斗结算包含未知币种或欠款字段")
                deferred = []
                if not await self.settlement.apply(player, data, settlement, audits=audits, defer_reasons=deferred):
                    if BAG_SPACE_DEFERRED in deferred:
                        self._bag_deferred[key] = player
                    return None
            outcome, reason = APPLIED, 0
        elif item.directive == 1:
            if item.void_reason not in (1, 2):
                raise VersionSkew("未知作废原因")
            outcome, reason = SKIPPED, item.void_reason
        elif item.directive == 0:
            outcome, reason = self._sanction(data, item)
        else:
            raise VersionSkew("未知收件箱指令")
        if cursor is None:
            cursor = data.inbox_ledger.cursors.add(domain=domain)
        cursor.decided_through_seq = item.seq
        cursor.recent.add(seq=item.seq, request_id=item.request_id, msg_type=item.msg_type,
                          outcome=outcome, reason=reason)
        if len(cursor.recent) > MAX_RECENT:
            del cursor.recent[:-MAX_RECENT]
        async def committed(snapshot):
            for audit in audits:
                result = audit(snapshot)
                if isawaitable(result):
                    await result

        await self.scene.commit_player_data(player, data, on_committed=committed if audits else None)
        if settlement is not None and not duplicate:
            await self._publish(player, settlement)
        return self._report(item, outcome, reason)

    def _payload(self, name, item, limit):
        try:
            if len(item.payload) <= limit:
                result = self.protocol.parse(name, item.payload)
                if known_message(result):
                    return result
        except DecodeError:
            pass
        raise VersionSkew(f"收件箱无法解析 {name}")

    def _sanction(self, data, item):
        if item.msg_type in (1, 2):
            payload = self._payload("player_inbox.GmCurrencyGainPayload", item, 64)
            if payload.currency_type not in self.assets.currency_types:
                raise VersionSkew("无法识别收件箱币种")
            values, key = data.currency.blocked_types, payload.currency_type
            active = item.msg_type == 1
        elif item.msg_type == 3:
            payload = self._payload("player_inbox.GmCurrencyDebtPayload", item, 1024)
            if payload.currency_type not in self.assets.currency_types:
                raise VersionSkew("无法识别欠款币种")
            if (len(payload.reason.encode()) > 256 or len(payload.gm_operator.encode()) > 64
                    or not 0 < payload.owe_amount < (1 << 63)):
                return REJECTED, 1005
            code = self.assets.attach_debt(data, payload.currency_type, payload.owe_amount,
                                           reason=payload.reason, operator=payload.gm_operator,
                                           expires_at=payload.expires_at)
            if code == 1006:
                raise VersionSkew("欠款带未知字段")
            return (REJECTED, code) if code else (APPLIED, 0)
        elif item.msg_type in (5, 6):
            payload = self._payload("player_inbox.GmItemGainPayload", item, 64)
            values, key = data.item_gain_block.item_config_ids, payload.item_config_id
            if not key:
                return REJECTED, 1005
            active = item.msg_type == 5
        else:
            raise VersionSkew("收件箱处置类型未注册")
        if active and key not in values:
            values.append(key)
            values.sort()
        elif not active and key in values:
            values.remove(key)
        return APPLIED, 0

    async def _publish(self, player, settlement):
        published = self._published.setdefault(player.entity, [])
        if settlement.battle_id not in published:
            await self.settlement.complete(player, settlement)
            published.append(settlement.battle_id)
            if len(published) > MAX_RECENT:
                del published[0]

    def _report(self, item, outcome, reason):
        return self.protocol.message("player_inbox.PlayerInboxDecisionReport",
                                     seq=item.seq, request_id=item.request_id, outcome=outcome, reason=reason)

    async def close(self):
        self._stopping = True
        await asyncio.gather(*tuple(self._jobs.values()), return_exceptions=True)
