"""Persist battle overflow before delivering idempotent personal mail."""

import asyncio
import logging
import os
import random
import time

import grpc

from ..data import PlayerRowKind
from ..grpc_runtime import GrpcClient
from .battle_control import sign_service_call

_LOG = logging.getLogger(__name__)
_PATH = "/mailpb.MailAdmin/SendPersonalMail"
_TITLE = "战斗奖励"
_BODY = "背包已满,本场战斗获得的物品改由邮件发放,请腾出背包空位后领取附件。"
_MAX_ENTRIES = 32
_MAX_LINES = 16
_MAX_ITEMS = 4096
_REJECTED = {1005, 17002, 17003, 17004}


def split_items(items):
    result, current, total = [], {}, 0
    for config, count in sorted(items.items()):
        if config <= 0 or count <= 0:
            continue
        while count:
            if len(current) == _MAX_LINES or total == _MAX_ITEMS:
                result.append(current)
                current, total = {}, 0
            take = min(count, _MAX_ITEMS - total)
            current[config] = take
            total += take
            count -= take
            if len(result) >= _MAX_ENTRIES:
                return None
    if current:
        result.append(current)
    return result


def valid_entry(entry):
    last = total = 0
    if not entry.request_id or not entry.source or not entry.title or not 1 <= len(entry.items) <= _MAX_LINES:
        return False
    for item in entry.items:
        if item.config_id <= last or not item.count:
            return False
        last = item.config_id
        total += item.count
        if total > _MAX_ITEMS:
            return False
    copy = type(entry)()
    copy.CopyFrom(entry)
    copy.DiscardUnknownFields()
    return copy.SerializeToString(deterministic=True) == entry.SerializeToString(deterministic=True)


class MailSender:
    def __init__(self, protocol, directory):
        self.protocol, self.directory = protocol, directory
        self.clients = {}

    @staticmethod
    def secret():
        value = os.getenv("MAIL_PRODUCER_SECRET_BATTLE", "").encode()
        return value if len(value.strip()) >= 32 else b""

    async def __call__(self, player_id, entry):
        secret = self.secret()
        if not secret or not valid_entry(entry):
            raise RuntimeError("mail producer key or outbox entry invalid")
        nodes = await self.directory.candidates(8, None)
        nodes = [node for node in nodes if node.node_type == 8 and node.protocol_type == 1
                 and node.node_uuid and node.grpc_endpoint.ip and 0 < node.grpc_endpoint.port < 65536]
        if not nodes:
            raise RuntimeError("no ready mail node")
        endpoint = random.choice(nodes).grpc_endpoint
        address = f"[{endpoint.ip}]:{endpoint.port}" if ":" in endpoint.ip else f"{endpoint.ip}:{endpoint.port}"
        client = self.clients.get(address)
        if client is None:
            client = self.clients[address] = GrpcClient(address, timeout=5, enable_retries=False)
        request = self.protocol.message("mailpb.SendMailRequest", sender_kind=5, request_id=entry.request_id,
                                        recipient_player_id=player_id, title=entry.title, body=entry.body)
        for item in entry.items:
            request.attachments.items.add(config_id=item.config_id, count=item.count)
        sign_service_call(_PATH, request, "battle", time.time_ns() // 1_000_000, secret)
        raw = await client.call(_PATH, request.SerializeToString())
        return self.protocol.parse("mailpb.SendMailResponse", raw)

    async def close(self):
        await asyncio.gather(*(client.close() for client in self.clients.values()), return_exceptions=True)
        self.clients.clear()


class SceneMailOutbox:
    def __init__(self, scene, allocate_id, sender, *, clock=time.monotonic):
        self.scene, self.allocate_id, self.sender, self.clock = scene, allocate_id, sender, clock
        self._jobs, self._due, self._failures = {}, {}, {}
        self._next_tick = 0.0
        self._closed = False

    async def prepare(self, data, battle_id, items):
        if not battle_id or self.allocate_id is None or not self.sender.secret():
            return False
        batches = split_items(items)
        if not batches or len(data.mail_outbox.entries) + len(batches) > _MAX_ENTRIES:
            return False
        ids = []
        try:
            for _ in batches:
                request_id = await self.allocate_id()
                if not 0 < request_id < (1 << 64) - 1 or request_id in ids:
                    return False
                ids.append(request_id)
        except Exception:
            _LOG.warning("mail outbox cannot allocate request id", exc_info=True)
            return False
        existing = {entry.request_id for entry in data.mail_outbox.entries}
        if any(request_id in existing for request_id in ids):
            return False
        stamp = time.time_ns() // 1_000_000
        for request_id, batch in zip(ids, batches):
            entry = data.mail_outbox.entries.add(request_id=request_id, source=1, source_id=battle_id,
                                                  title=_TITLE, body=_BODY, created_at_ms=stamp)
            for config, count in batch.items():
                entry.items.add(config_id=config, count=count)
        return True

    def tick(self):
        now = self.clock()
        if self._closed or now < self._next_tick:
            return
        self._next_tick = now + 1.0
        for world in tuple(self.scene.worlds.values()):
            for player in tuple(world.players.values()):
                key = player.player_id
                if len(self._jobs) >= 16:
                    return
                if key not in self._jobs and self._due.get(key, 0) <= now and self.scene.mutable(player):
                    # Only scan the committed snapshot: sending an unconfirmed
                    # settlement could duplicate rewards after a crash.
                    try:
                        row = player.snapshot.rows.get(PlayerRowKind.WALLET, b"")
                        if not self.scene.protocol.parse("player_database", row).mail_outbox.entries:
                            continue
                    except Exception:
                        _LOG.exception("cannot read mail outbox player=%s", key)
                        self._due[key] = now + 60
                        continue
                    task = asyncio.create_task(self._send_one(player))
                    self._jobs[key] = task
                    task.add_done_callback(lambda _, player_id=key: self._jobs.pop(player_id, None))

    def _current(self, player):
        world = self.scene.locations.get(player.player_id)
        return world is not None and world.players.get(player.player_id) is player and self.scene.mutable(player)

    def departed(self, player):
        self._due.pop(player.player_id, None)
        self._failures.pop(player.player_id, None)

    async def _send_one(self, player):
        key = player.player_id
        rejected = False
        try:
            async with self.scene._lock(key):
                if not self._current(player):
                    return
                await self.scene.flush_player_rows(player)
                data = self.scene.rows.assemble(player.snapshot)
                if not data.mail_outbox.entries:
                    self._due.pop(key, None)
                    self._failures.pop(key, None)
                    return
                entry = type(data.mail_outbox.entries[0])()
                entry.CopyFrom(data.mail_outbox.entries[0])
                if not valid_entry(entry):
                    raise ValueError("invalid persisted mail outbox entry")
            reply = await self.sender(key, entry)
            if reply.error_message.id not in (0, 1000) or not reply.mail_id:
                rejected = reply.error_message.id in _REJECTED
                raise RuntimeError(f"mail did not confirm delivery: {reply.error_message.id}")
            async with self.scene._lock(key):
                if not self._current(player):
                    return
                await self.scene.flush_player_rows(player)
                data = self.scene.rows.assemble(player.snapshot)
                raw = entry.SerializeToString(deterministic=True)
                for index, saved in enumerate(data.mail_outbox.entries):
                    if saved.request_id == entry.request_id and saved.SerializeToString(deterministic=True) == raw:
                        del data.mail_outbox.entries[index]
                        await self.scene.commit_player_data(player, data)
                        break
            self._failures.pop(key, None)
            self._due[key] = self.clock()
        except Exception as exc:
            if not self._current(player):
                return
            if isinstance(exc, grpc.RpcError) and exc.code() == grpc.StatusCode.UNAUTHENTICATED:
                rejected = True
            streak = min(self._failures.get(key, 0) + 1, 256)
            self._failures[key] = streak
            delay = min((30 if rejected else 1) * (1 << min(streak - 1, 8)), 300 if rejected else 60)
            self._due[key] = self.clock() + delay
            if streak == 1 or streak % 8 == 0:
                _LOG.warning("mail outbox retry player=%s streak=%s delay=%s", key, streak, delay, exc_info=True)

    async def close(self):
        self._closed = True
        tasks = tuple(self._jobs.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.sender.close()
