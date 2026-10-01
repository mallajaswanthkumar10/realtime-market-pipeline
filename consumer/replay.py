"""Replay utilities demonstrating the pipeline's reprocessing story.

Two distinct replay scenarios:

1. `replay_dlq()` - messages that failed delivery land on `alerts.dlq`
   with failure context attached. This replays them back onto the main
   `alerts` topic so alert_consumer.py gets another shot at them (e.g.
   after the downstream outage that caused the failures is resolved).

2. `replay_from_timestamp()` - because Kafka topics are configured with
   7-day retention (KAFKA_RETENTION_MS in utils/config.py /
   infra/create_topics.sh), raw events are never gone the moment they're
   consumed. This seeks a *new* consumer group to an arbitrary point in
   the last 7 days and reprocesses from there - e.g. to backfill the S3
   mart layer after fixing a bug in the streaming job.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from kafka import TopicPartition

from utils import config
from utils.kafka_helpers import build_consumer, build_producer, send_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("replay")


def replay_dlq(max_messages: int | None = None) -> int:
    consumer = build_consumer(
        topics=[config.TOPIC_ALERTS_DLQ],
        group_id="dlq-replay-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = build_producer()
    replayed = 0
    try:
        for message in consumer:
            alert = message.value
            alert.pop("dlq_reason", None)
            send_sync(producer, config.TOPIC_ALERTS, alert, key=alert.get("symbol"))
            consumer.commit()
            replayed += 1
            log.info("replayed DLQ message %s", alert.get("event_id"))
            if max_messages and replayed >= max_messages:
                break
    finally:
        consumer.close()
        producer.flush()
        producer.close()
    log.info("replayed %d DLQ messages", replayed)
    return replayed


def replay_from_timestamp(topic: str, start: datetime, group_id: str = "backfill-replay-group") -> None:
    consumer = build_consumer(topics=[topic], group_id=group_id, enable_auto_commit=False)
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or []]
    consumer.assign(partitions)

    ts_ms = int(start.timestamp() * 1000)
    offsets = consumer.offsets_for_times({tp: ts_ms for tp in partitions})
    for tp, offset_and_ts in offsets.items():
        if offset_and_ts is not None:
            consumer.seek(tp, offset_and_ts.offset)
        else:
            consumer.seek_to_end(tp)

    log.info("seeked %s to %s - starting replay (Ctrl+C to stop)", topic, start.isoformat())
    try:
        for message in consumer:
            log.info("replay: offset=%s key=%s", message.offset, message.key)
    except KeyboardInterrupt:
        log.info("stopping replay")
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay DLQ messages or backfill from a historical offset")
    sub = parser.add_subparsers(dest="mode", required=True)

    dlq_parser = sub.add_parser("dlq", help="replay alerts.dlq back onto alerts")
    dlq_parser.add_argument("--max", type=int, default=None)

    backfill_parser = sub.add_parser("backfill", help="reprocess raw events from a point in the last 7 days")
    backfill_parser.add_argument("--topic", default=config.TOPIC_STOCK_RAW)
    backfill_parser.add_argument("--since", required=True, help="ISO timestamp, e.g. 2026-09-28T00:00:00")

    args = parser.parse_args()
    if args.mode == "dlq":
        replay_dlq(max_messages=args.max)
    else:
        replay_from_timestamp(args.topic, datetime.fromisoformat(args.since))


if __name__ == "__main__":
    main()
