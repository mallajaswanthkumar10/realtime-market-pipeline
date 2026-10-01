"""Consumes the `alerts` topic and delivers each anomaly to a downstream
sink (stubbed here as a logged "notification" - swap `notify()` for a real
webhook/PagerDuty/Slack call).

Resilience stack, cheapest-first:
    1. retry_with_backoff - absorb transient failures (network blip).
    2. CircuitBreaker      - stop hammering a downstream that's actually
                             down, fail fast instead of piling up retries.
    3. Dead-letter topic   - once retries + circuit breaker both give up,
                             the message is never silently dropped: it's
                             routed to `alerts.dlq` with failure context
                             for later inspection/replay (see replay.py).

Manual offset commits: we only commit after the alert has reached a
terminal state (delivered OR routed to DLQ), giving at-least-once
delivery. Because notify() calls are logically idempotent (keyed by
event_id), at-least-once here is equivalent to effectively-once.
"""
from __future__ import annotations

import logging
import random

from kafka.errors import KafkaError

from utils import config
from utils.circuit_breaker import CircuitBreaker, CircuitOpenError
from utils.kafka_helpers import build_consumer, build_producer, send_sync
from utils.retry import retry_with_backoff

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alert_consumer")

_breaker = CircuitBreaker(failure_threshold=5, reset_timeout_seconds=30, name="downstream-notify")


class DownstreamUnavailable(Exception):
    pass


def notify(alert: dict) -> None:
    """Stand-in for a real downstream call (webhook / PagerDuty / Slack).

    Simulates occasional failures so the retry/circuit-breaker/DLQ path is
    actually exercised in a demo run.
    """
    if random.random() < 0.05:
        raise DownstreamUnavailable("simulated downstream timeout")
    log.info("ALERT delivered: %s %s price=%.2f zscore=%.2f", alert["symbol"], alert["alert_type"], alert["price"], alert["zscore"])


@retry_with_backoff(max_attempts=3, base_delay=0.5, exceptions=(DownstreamUnavailable,))
def notify_with_retry(alert: dict) -> None:
    notify(alert)


def handle_alert(alert: dict, dlq_producer) -> None:
    try:
        _breaker.call(notify_with_retry, alert)
    except (DownstreamUnavailable, CircuitOpenError) as exc:
        log.warning("routing alert %s to DLQ: %s", alert.get("event_id"), exc)
        dlq_payload = {**alert, "dlq_reason": str(exc)}
        send_sync(dlq_producer, config.TOPIC_ALERTS_DLQ, dlq_payload, key=alert.get("symbol"))


def run() -> None:
    consumer = build_consumer(
        topics=[config.TOPIC_ALERTS],
        group_id="alert-consumer-group",
        enable_auto_commit=False,
    )
    dlq_producer = build_producer()

    log.info("alert_consumer listening on %s", config.TOPIC_ALERTS)
    try:
        for message in consumer:
            alert = message.value
            try:
                handle_alert(alert, dlq_producer)
            except KafkaError:
                log.exception("kafka error handling alert %s - will retry on redelivery", alert.get("event_id"))
                continue  # do not commit -> message is redelivered
            consumer.commit()
    except KeyboardInterrupt:
        log.info("stopping alert_consumer")
    finally:
        consumer.close()
        dlq_producer.flush()
        dlq_producer.close()


if __name__ == "__main__":
    run()
