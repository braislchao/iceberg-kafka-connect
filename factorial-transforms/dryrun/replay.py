"""Copy a contiguous offset window of a prod topic into the local Kafka, byte for byte.

Read-only on prod: the partition is assigned, never subscribed, and offsets are never
committed, so no consumer group appears on the prod cluster.

usage: replay.py <topic> <count> <start_back>   (window starts start_back records before the end)
prints: <topic> <first prod offset> <last prod offset>
"""
import sys

from confluent_kafka import Consumer, Producer, TopicPartition

topic, count, back = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
user, password = open("secrets/kafka.creds").read().split()

src = Consumer({
    "bootstrap.servers": "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": user,
    "sasl.password": password,
    "group.id": "never-committed",
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
})
low, high = src.get_watermark_offsets(TopicPartition(topic, 0), timeout=30)
start = max(low, high - back)
src.assign([TopicPartition(topic, 0, start)])

dst = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True})
first = prev = None
copied = 0
while copied < count:
    m = src.poll(30)
    if m is None:
        break
    if m.error():
        raise SystemExit(m.error())
    # local offsets only map onto prod offsets through cdc.offset.start if the window has no gaps
    if prev is not None and m.offset() != prev + 1:
        raise SystemExit(f"offset gap {prev} -> {m.offset()}, pick another window")
    first = m.offset() if first is None else first
    prev = m.offset()
    dst.produce(topic, value=m.value(), key=m.key(), headers=m.headers(), timestamp=m.timestamp()[1])
    copied += 1
dst.flush()
src.close()
print(topic, first, prev)
