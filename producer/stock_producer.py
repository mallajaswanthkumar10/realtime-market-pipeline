"""Simulates a real-time stock price feed and publishes ticks to Kafka.

Each ticker follows an independent random walk. Every `--spike-every`
ticks we deliberately inject a large price jump so the downstream anomaly
detector (streaming/anomaly_detection.py) has something to catch - this
makes the pipeline demoable end-to-end without needing a live market feed.

Usage:
    python -m producer.stock_producer --rate 50 --duration 0
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
log = logging.getLogger("stock_producer")


class TickerState:
    def __init__(self, symbol: str, start_price: float) -> None:
        self.symbol = symbol
        self.price = start_price
        self.tick_count = 0

    def next_price(self, spike_every: int, spike_magnitude: float) -> float:
        self.tick_count += 1
        drift = random.gauss(0, 0.15)  # normal market noise
        if spike_every > 0 and self.tick_count % spike_every == 0:
            drift += random.choice([-1, 1]) * spike_magnitude
            log.info("injecting anomaly spike for %s", self.symbol)
        self.price = max(0.01, self.price + drift)
        return round(self.price, 2)


def make_event(state: TickerState, spike_every: int, spike_magnitude: float) -> dict:
    price = state.next_price(spike_every, spike_magnitude)
    return {
        "event_id": str(uuid.uuid4()),
        "symbol": state.symbol,
        "price": price,
        "volume": random.randint(1, 5000),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "stock_producer_sim",
    }


def run(rate_per_sec: float, duration_sec: int, spike_every: int, spike_magnitude: float) -> None:
    producer = build_producer()
    states = {sym: TickerState(sym, start_price=random.uniform(50, 800)) for sym in config.TICKERS}

    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    start = time.time()
    sent = 0
    try:
        while True:
            if duration_sec > 0 and (time.time() - start) >= duration_sec:
                break
            symbol = random.choice(config.TICKERS)
            event = make_event(states[symbol], spike_every, spike_magnitude)
            producer.send(config.TOPIC_STOCK_RAW, value=event, key=symbol)
            sent += 1
            if sent % 500 == 0:
                log.info("sent %d events", sent)
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        log.info("stopping producer")
    finally:
        producer.flush()
        producer.close()
        log.info("total events sent: %d", sent)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate real-time stock ticks onto Kafka")
    parser.add_argument("--rate", type=float, default=20.0, help="events/sec (0 = as fast as possible)")
    parser.add_argument("--duration", type=int, default=0, help="seconds to run (0 = run forever)")
    parser.add_argument("--spike-every", type=int, default=40, help="inject an anomaly every N ticks per symbol")
    parser.add_argument("--spike-magnitude", type=float, default=15.0, help="size of injected anomaly spikes")
    args = parser.parse_args()
    run(args.rate, args.duration, args.spike_every, args.spike_magnitude)


if __name__ == "__main__":
    main()
