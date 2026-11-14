import asyncio
import logging

import grpc
from google.protobuf import json_format

from .grpc_runtime import GrpcClient
from .protocol import Protocol

logger = logging.getLogger(__name__)

GATE, SCENE, BATTLE, ROUTER, PLAYER_STORE = 4, 3, 28, 29, 31


def command_route(role: str, node_id: int, *, generation: int = 1, partitions: int = 256):
    if not role or min(node_id, generation, partitions) < 1:
        raise ValueError("非法命令 topic 配置")
    return f"{role}-cmd_g{generation}", node_id % partitions


class LocalDirectory:
    def __init__(self) -> None:
        self.nodes = {}

    async def lookup(self, node_type: int, zone: int, node_id: int):
        return self.nodes.get((node_type, zone, node_id))

    async def candidates(self, node_type: int, zone: int | None):
        return [info for (kind, region, _), info in self.nodes.items()
                if kind == node_type and (zone is None or region == zone)]

    def publish(self, info) -> None:
        self.nodes[info.node_type, info.zone_id, info.node_id] = info


class EtcdDirectory:
    def __init__(self, protocol: Protocol, endpoint: str, ttl: int = 15) -> None:
        self.protocol = protocol
        self.client = GrpcClient(endpoint, timeout=4)
        self.types = protocol.pool.FindEnumTypeByName("common.base.eNodeType").values_by_number
        self.ttl = ttl
        self.lease = 0
        self._lock = asyncio.Lock()
        self._closed = False

    def prefix(self, node_type: int, zone: int) -> bytes:
        return f"{self.types[node_type].name}.rpc/zone/{zone}/node_type/{node_type}/node_id/".encode()

    def _service_key(self, info) -> bytes:
        return self.prefix(info.node_type, info.zone_id) + str(info.node_id).encode()

    async def _rpc(self, service: str, method: str, **fields):
        name = f"etcdserverpb.{method}"
        request = self.protocol.message(f"{name}Request", **fields)
        data = await self.client.call(f"/etcdserverpb.{service}/{method}", request.SerializeToString())
        return self.protocol.parse(f"{name}Response", data)

    async def lookup(self, node_type: int, zone: int, node_id: int):
        key = self.prefix(node_type, zone) + str(node_id).encode()
        result = await self._rpc("KV", "Range", key=key)
        return self._info(result.kvs[0].value) if result.kvs else None

    async def candidates(self, node_type: int, zone: int | None):
        start = (f"{self.types[node_type].name}.rpc/zone/".encode()
                 if zone is None else self.prefix(node_type, zone))
        result = await self._rpc("KV", "Range", key=start, range_end=start[:-1] + b"0")
        nodes = []
        for item in result.kvs:
            try:
                info = self._info(item.value)
            except (UnicodeDecodeError, json_format.ParseError):
                logger.warning("跳过无法解析的 etcd 节点登记 type=%s", node_type)
                continue
            if (info.node_type == node_type and (zone is None or info.zone_id == zone)
                    and item.key == self._service_key(info)):
                nodes.append(info)
        return nodes

    def _info(self, data: bytes):
        return json_format.Parse(data.decode(), self.protocol.message("NodeInfo"), ignore_unknown_fields=True)

    @staticmethod
    def _serialized(info) -> bytes:
        # Go node watchers consume the protobuf JSON field names with encoding/json.
        return json_format.MessageToJson(info, indent=None).encode()

    async def register(self, info) -> None:
        allocation = (
            f"{self.types[info.node_type].name}.rpc/allocated/node_type/"
            f"{info.node_type}/node_id/{info.node_id}"
        ).encode()
        records = {
            allocation: info.node_uuid.encode(),
            self._service_key(info): self._serialized(info),
            f"/service/{info.endpoint.ip}/port/{info.endpoint.port}".encode(): b"",
        }
        async with self._lock:
            if self._closed or self.lease:
                raise RuntimeError("etcd 节点注册器已关闭或已登记")
            grant = await self._rpc("Lease", "LeaseGrant", TTL=self.ttl)
            lease = grant.ID
            if not lease or grant.error:
                raise RuntimeError("etcd 未分配节点租约")
            try:
                result = await self._rpc(
                    "KV", "Txn",
                    compare=[{"key": key, "version": 0} for key in records],
                    success=[
                        {"request_put": {"key": key, "value": value, "lease": lease}}
                        for key, value in records.items()
                    ],
                )
                if not result.succeeded:
                    raise RuntimeError("节点号、注册地址或端口已被占用")
            except BaseException:
                await self._revoke(lease)
                raise
            self.lease = lease

    async def maintain(self, info, *, load=None) -> None:
        lease = self.lease
        if not lease:
            raise RuntimeError("etcd 节点尚未登记")
        request = self.protocol.message("etcdserverpb.LeaseKeepAliveRequest", ID=lease)
        call = self.client.channel.stream_stream("/etcdserverpb.Lease/LeaseKeepAlive")
        last_load = info.player_count
        clock = asyncio.get_running_loop().time
        last_report = clock()
        report_interval = 10
        interval = self.ttl / 3 if load is None else min(self.ttl / 3, report_interval)

        async def ping():
            yield request.SerializeToString()

        while True:
            await asyncio.sleep(interval)
            keepalive = call(ping(), timeout=self.client.timeout)
            try:
                data = await keepalive.read()
                if data is grpc.aio.EOF:
                    raise RuntimeError("etcd 租约续期没有应答")
                reply = self.protocol.parse("etcdserverpb.LeaseKeepAliveResponse", data)
                if reply.ID != lease or reply.TTL <= 0:
                    raise RuntimeError("etcd 节点租约失效")
            finally:
                keepalive.cancel()
            if load is not None:
                current = load()
                # Go Login judges Gate liveness from revisions, even when the player count is unchanged.
                if current != last_load or clock() - last_report >= report_interval:
                    published = type(info)()
                    published.CopyFrom(info)
                    published.player_count = current
                    await self.update(published)
                    info.player_count = current
                    last_load = current
                    last_report = clock()

    async def update(self, info) -> None:
        async with self._lock:
            if not self.lease or self._closed:
                raise RuntimeError("etcd 节点没有可更新的登记")
            key, lease = self._service_key(info), self.lease
            result = await self._rpc(
                "KV", "Txn",
                compare=[{"key": key, "target": "LEASE", "lease": lease}],
                success=[{"request_put": {"key": key, "value": self._serialized(info), "lease": lease}}],
            )
            if not result.succeeded:
                raise RuntimeError("etcd 节点登记的租约已更换")

    async def _revoke(self, lease: int) -> None:
        try:
            await self._rpc("Lease", "LeaseRevoke", ID=lease)
        except grpc.aio.AioRpcError as exc:
            if exc.code() != grpc.StatusCode.NOT_FOUND:
                logger.warning("撤销节点租约失败，等待 etcd 过期回收", exc_info=True)

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            lease, self.lease = self.lease, 0
            try:
                if lease:
                    await self._revoke(lease)
            finally:
                await self.client.close()


class KafkaCommands:
    def __init__(self, brokers: str, *, partitions: int, generation: int) -> None:
        self.brokers = brokers
        self.partitions = partitions
        self.generation = generation
        self.producer = None
        self.consumer = None
        self.spectate_consumer = None
        self.world_chat_consumer = None

    def route(self, role: str, node_id: int):
        return command_route(role, node_id, generation=self.generation, partitions=self.partitions)

    async def start(self, role: str | None = None, node_id: int = 0) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

        self.producer = AIOKafkaProducer(bootstrap_servers=self.brokers, enable_idempotence=True)
        await self.producer.start()
        if role is not None:
            topic, partition = self.route(role, node_id)
            target = TopicPartition(topic, partition)
            self.consumer = AIOKafkaConsumer(bootstrap_servers=self.brokers, group_id=None)
            self.consumer.assign([target])
            await self.consumer.start()
            actual = self.consumer.partitions_for_topic(topic)
            if actual is None or len(actual) != self.partitions:
                raise RuntimeError(f"命令 topic {topic} 的分区数与配置不一致")
            await self.consumer.seek_to_end(target)

    async def consume(self, dispatch) -> None:
        async for record in self.consumer:
            if record.value is not None:
                await dispatch(record.value)

    async def start_world_chat(self) -> None:
        from aiokafka import AIOKafkaConsumer, TopicPartition

        topic = f"chat-world-bcast_g{self.generation}"
        partitions = set(range(4))
        targets = [TopicPartition(topic, index) for index in sorted(partitions)]
        self.world_chat_consumer = consumer = AIOKafkaConsumer(bootstrap_servers=self.brokers, group_id=None)
        consumer.assign(targets)
        await consumer.start()
        if consumer.partitions_for_topic(topic) != partitions:
            raise RuntimeError(f"世界频道 topic {topic} 必须有 4 个分区")
        await consumer.seek_to_end(*targets)

    async def consume_world_chat(self, dispatch) -> None:
        async for record in self.world_chat_consumer:
            if record.value is not None:
                await dispatch(record.value)

    async def start_spectate(self, gate_id: int) -> None:
        from aiokafka import AIOKafkaConsumer, TopicPartition

        relay = f"gate-spectate_g{self.generation}"
        broadcast = f"spectate-bcast_g{self.generation}"
        targets = [TopicPartition(relay, gate_id % self.partitions)]
        targets.extend(TopicPartition(broadcast, index) for index in range(16))
        self.spectate_consumer = AIOKafkaConsumer(bootstrap_servers=self.brokers, group_id=None)
        self.spectate_consumer.assign(targets)
        await self.spectate_consumer.start()
        for topic, count in ((relay, self.partitions), (broadcast, 16)):
            actual = self.spectate_consumer.partitions_for_topic(topic)
            if actual is None or len(actual) != count:
                raise RuntimeError(f"观战 topic {topic} 的分区数与配置不一致")
        await self.spectate_consumer.seek_to_end(*targets)

    async def consume_spectate(self, dispatch) -> None:
        async for record in self.spectate_consumer:
            if record.value is not None:
                await dispatch(record.topic, record.value)

    async def publish_spectate(self, channel: str, gate_id: int, battle_id: int, message) -> None:
        from .nodes.battle_spectate import spectate_kafka_route

        topic, partition, key = spectate_kafka_route(
            channel, gate_id, battle_id, self.generation, self.partitions,
        )
        await self.producer.send_and_wait(topic, message.SerializeToString(), key=key, partition=partition)

    async def publish_audit(self, entry, generation: int) -> None:
        await self.producer.send_and_wait(
            f"transaction_log_topic_g{generation}", entry.SerializeToString(),
            key=str(entry.from_player or entry.to_player).encode(),
        )

    async def publish_anomaly(self, player_id: int, payload: bytes) -> None:
        await self.producer.send_and_wait("anomaly_alert_topic", payload, key=str(player_id).encode())

    async def publish(self, role: str, node_id: int, message, *, key: bytes | None = None) -> None:
        topic, partition = self.route(role, node_id)
        await self.producer.send_and_wait(
            topic, message.SerializeToString(), key=key if key is not None else str(node_id).encode(),
            partition=partition,
        )

    async def close(self) -> None:
        if self.world_chat_consumer is not None:
            await self.world_chat_consumer.stop()
        if self.spectate_consumer is not None:
            await self.spectate_consumer.stop()
        if self.consumer is not None:
            await self.consumer.stop()
        if self.producer is not None:
            await self.producer.stop()
