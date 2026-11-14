import asyncio
import sys
from types import SimpleNamespace

from xuanming.cluster import KafkaCommands


def test_aiokafka_seek_to_end_none_return_is_supported_and_tombstones_are_skipped(monkeypatch):
    class TopicPartition:
        def __init__(self, topic, partition):
            self.topic, self.partition = topic, partition

    class Consumer:
        instances = []

        def __init__(self, **_kwargs):
            self.assigned = []
            self.records = []
            self.seeks = []
            self.started = False
            self.instances.append(self)

        def assign(self, targets):
            self.assigned = list(targets)

        async def start(self):
            self.started = True

        def partitions_for_topic(self, topic):
            return set(range(4 if topic.startswith("chat-") else 16 if topic.startswith("spectate-bcast") else 32))

        async def seek_to_end(self, *targets):
            self.seeks.append(targets)
            return None

        def __aiter__(self):
            async def records():
                for record in self.records:
                    yield record
            return records()

        async def stop(self):
            self.started = False

    class Producer:
        def __init__(self, **_kwargs):
            pass

        async def start(self):
            return None

        async def stop(self):
            return None

    monkeypatch.setitem(sys.modules, "aiokafka", SimpleNamespace(
        AIOKafkaConsumer=Consumer, AIOKafkaProducer=Producer, TopicPartition=TopicPartition,
    ))

    async def exercise():
        kafka = KafkaCommands("unused", partitions=32, generation=1)
        await kafka.start("gate", 7)
        await kafka.start_world_chat()
        await kafka.start_spectate(9)
        assert len(Consumer.instances) == 3
        assert all(consumer.started and len(consumer.seeks) == 1 for consumer in Consumer.instances)

        record = lambda value, topic="topic": SimpleNamespace(value=value, topic=topic)
        main_values, world_values, spectate_values = [], [], []
        async def append(target, value):
            target.append(value)

        async def append_spectate(topic, value):
            spectate_values.append((topic, value))

        kafka.consumer.records = [record(b"command"), record(None)]
        kafka.world_chat_consumer.records = [record(None), record(b"chat")]
        kafka.spectate_consumer.records = [record(b"frame", "spectate"), record(None, "spectate")]
        await kafka.consume(lambda value: append(main_values, value))
        await kafka.consume_world_chat(lambda value: append(world_values, value))
        await kafka.consume_spectate(append_spectate)
        assert main_values == [b"command"]
        assert world_values == [b"chat"]
        assert spectate_values == [("spectate", b"frame")]
        await kafka.close()

    asyncio.run(exercise())
