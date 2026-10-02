"""An in-memory Kafka for the streaming tests: just enough of kafka-python's
KafkaProducer / KafkaConsumer (group drain and assign/seek/poll) and TopicPartition.

One partition per topic. Messages keep their serialized bytes, keys and headers;
consumer groups keep committed offsets, so a second consume sees only new messages.

    broker = FakeBroker()
    monkeypatch.setitem(sys.modules, "kafka", broker.module())
"""
from __future__ import annotations

import json
import types
from collections import namedtuple

TopicPartition = namedtuple("TopicPartition", "topic partition")
Message = namedtuple("Message", "topic partition offset key value headers")


class FakeBroker:
    def __init__(self):
        self.log: dict[str, list[Message]] = {}
        self.committed: dict[tuple[str, str], int] = {}     # (group, topic) -> next offset
        self.commits = 0
        self.flushes: list[int] = []                         # messages in the log at each flush

    # -- inspection ----------------------------------------------------------
    def values(self, topic: str) -> list[dict]:
        return [json.loads(m.value) for m in self.log.get(topic, [])]

    def topics(self) -> set[str]:
        return {t for t, msgs in self.log.items() if msgs}

    def append(self, topic: str, value, key: bytes | None = None, headers=None) -> None:
        raw = value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")
        msgs = self.log.setdefault(topic, [])
        msgs.append(Message(topic, 0, len(msgs), key, raw, list(headers or [])))

    # -- kafka-python surface ------------------------------------------------
    def module(self):
        broker = self

        class KafkaProducer:
            instances: list = []

            def __init__(self, **kw):
                self.kw = kw
                self.ser = kw.get("value_serializer") or (lambda v: v)
                KafkaProducer.instances.append(self)

            def send(self, topic, value=None, key=None, headers=None):
                broker.append(topic, self.ser(value), key=key, headers=headers)

            def flush(self):
                broker.flushes.append(sum(len(m) for m in broker.log.values()))

            def close(self):
                pass

        class KafkaConsumer:
            def __init__(self, *topics, **kw):
                self.kw, self.group = kw, kw.get("group_id")
                self.de = kw.get("value_deserializer") or (lambda v: v)
                self.subscribed = list(topics)
                self.positions: dict[TopicPartition, int] = {}
                self.closed = False

            # group drain
            def __iter__(self):
                self._read = {}
                for t in self.subscribed:
                    start = broker.committed.get((self.group, t), 0)
                    msgs = broker.log.get(t, [])[start:]
                    self._read[t] = start + len(msgs)
                    for m in msgs:
                        yield m._replace(value=self.de(m.value))

            def commit(self):
                broker.commits += 1
                for t, nxt in getattr(self, "_read", {}).items():
                    broker.committed[(self.group, t)] = nxt

            # assign / seek / poll
            def partitions_for_topic(self, topic):
                return {0} if topic in broker.log else None

            def assign(self, tps):
                self.positions = {tp: 0 for tp in tps}

            def seek_to_beginning(self, *tps):
                for tp in tps:
                    self.positions[tp] = 0

            def end_offsets(self, tps):
                return {tp: len(broker.log.get(tp.topic, [])) for tp in tps}

            def position(self, tp):
                return self.positions[tp]

            def poll(self, timeout_ms=0):
                out = {}
                for tp, pos in self.positions.items():
                    msgs = broker.log.get(tp.topic, [])[pos:pos + 2]      # small batches
                    if msgs:
                        out[tp] = [m._replace(value=self.de(m.value)) for m in msgs]
                        self.positions[tp] = pos + len(msgs)
                return out

            def close(self):
                self.closed = True

        self.producer_cls = KafkaProducer
        return types.SimpleNamespace(KafkaProducer=KafkaProducer, KafkaConsumer=KafkaConsumer,
                                     TopicPartition=TopicPartition)
