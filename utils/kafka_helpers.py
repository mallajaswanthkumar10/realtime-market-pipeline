"""Shared Kafka producer/consumer factories.

Uses kafka-python (pure Python, no native deps) so the project installs
cleanly on any OS/Python version. For a production deployment targeting
very high throughput, swap this module's producer for `confluent-kafka`
(librdkafka-backed) and set `enable.idempotence=True` for broker-level
exactly-once-per-partition semantics - see docs/ARCHITECTURE.md for the
tradeoffs and the exact config.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from kafka import KafkaConsumer, KafkaProducer

from utils import config


def build_producer(bootstrap_servers: Optional[str] = None) -> KafkaProducer:
    """Producer tuned for durability over raw throughput:

    - acks="all": wait for every in-sync replica before ack'ing.
    - retries + linger_ms: absorb transient broker hiccups without
      dropping messages, batching small bursts for efficiency.
    - key-based partitioning (caller passes `key=`) makes per-key ordering
      and downstream idempotent dedup possible even without broker-level
      exactly-once support.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=10,
        linger_ms=20,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )


def build_consumer(
    topics: list[str],
    group_id: str,
    bootstrap_servers: Optional[str] = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
) -> KafkaConsumer:
    """Consumer with manual commit so a handler can commit only after a
    message is durably processed (at-least-once delivery). Combined with
    an idempotent write (e.g. upsert keyed by message id), this gives
    effectively-once processing downstream.
    """
    return KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
    )


def send_sync(producer: KafkaProducer, topic: str, value: Any, key: Optional[str] = None, timeout: int = 10) -> None:
    """Send and block for the broker ack - used where losing a message
    (e.g. an alert) is worse than the extra latency."""
    future = producer.send(topic, value=value, key=key)
    future.get(timeout=timeout)
