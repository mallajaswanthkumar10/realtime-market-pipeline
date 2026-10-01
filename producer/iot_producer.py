"""Simulates IoT sensor telemetry (temperature/humidity/vibration) and
publishes readings to Kafka. Mirrors stock_producer.py's shape so the same
streaming job pattern (windowed aggregation + anomaly detection) applies
to either domain - swap TOPIC_STOCK_RAW for TOPIC_IOT_RAW in the streaming
job to process this feed instead.

Usage:
    python -m producer.iot_producer --rate 50 --duration 0
"""
from __future__ import annotations

import argparse
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from utils import config
from utils.kafka_helpers import build_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("iot_producer")

SENSOR_IDS = [f"sensor-{i:03d}" for i in range(1, 11)]


def make_event(sensor_id: str, tick: int, spike_every: int) -> dict:
    is_spike = spike_every > 0 and tick % spike_every == 0
    temperature = random.gauss(22.0, 1.5) + (random.choice([-1, 1]) * 25 if is_spike else 0)
    return {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "symbol": sensor_id,  # unify field name with stock schema for shared downstream logic
        "price": round(temperature, 2),  # reuse "price" as the generic metric field
        "humidity": round(random.uniform(30, 70), 1),
        "vibration": round(random.uniform(0, 1), 3),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "iot_producer_sim",
    }


def run(rate_per_sec: float, duration_sec: int, spike_every: int) -> None:
    producer = build_producer()
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    start = time.time()
    sent = 0
    tick_by_sensor = {s: 0 for s in SENSOR_IDS}
    try:
        while True:
            if duration_sec > 0 and (time.time() - start) >= duration_sec:
                break
            sensor_id = random.choice(SENSOR_IDS)
            tick_by_sensor[sensor_id] += 1
            event = make_event(sensor_id, tick_by_sensor[sensor_id], spike_every)
            producer.send(config.TOPIC_IOT_RAW, value=event, key=sensor_id)
            sent += 1
            if sent % 500 == 0:
                log.info("sent %d readings", sent)
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        log.info("stopping producer")
    finally:
        producer.flush()
        producer.close()
        log.info("total readings sent: %d", sent)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate real-time IoT sensor readings onto Kafka")
    parser.add_argument("--rate", type=float, default=20.0)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--spike-every", type=int, default=40)
    args = parser.parse_args()
    run(args.rate, args.duration, args.spike_every)


if __name__ == "__main__":
    main()
