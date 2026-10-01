"""Real-time Streamlit dashboard for the pipeline.

Rather than polling a REST API, this subscribes directly to the same
Kafka topics the pipeline uses (`stock.raw` for ticks, `alerts` for
anomalies) and redraws in place inside a single long-running script
execution - the standard "while True + st.empty() placeholders" pattern
for live Streamlit dashboards, so no extra autorefresh component/dep is
needed.

Run:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict, deque

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils import config
from utils.kafka_helpers import build_consumer

st.set_page_config(page_title="Real-Time Market Pipeline", layout="wide")
st.title("Real-Time Market Data Pipeline")
st.caption("Live stock ticks, 5-min windowed stats, and anomaly alerts streamed directly from Kafka.")

MAX_POINTS_PER_SYMBOL = 300
MAX_ALERTS = 50


@st.cache_resource
def get_consumers():
    group_suffix = uuid.uuid4().hex[:8]
    stock_consumer = build_consumer(
        topics=[config.TOPIC_STOCK_RAW],
        group_id=f"dashboard-stock-{group_suffix}",
        auto_offset_reset="latest",
    )
    alert_consumer = build_consumer(
        topics=[config.TOPIC_ALERTS],
        group_id=f"dashboard-alerts-{group_suffix}",
        auto_offset_reset="latest",
    )
    return stock_consumer, alert_consumer


def drain(consumer, limit: int = 500) -> list[dict]:
    records = []
    batches = consumer.poll(timeout_ms=200, max_records=limit)
    for _tp, messages in batches.items():
        for msg in messages:
            records.append(msg.value)
    return records


def main() -> None:
    stock_consumer, alert_consumer = get_consumers()

    price_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_POINTS_PER_SYMBOL))
    time_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_POINTS_PER_SYMBOL))
    alerts: deque = deque(maxlen=MAX_ALERTS)

    total_events = 0
    total_alerts = 0
    window_start = time.time()
    window_events = 0
    throughput = 0.0

    metric_cols = st.columns(4)
    chart_placeholder = st.empty()
    alerts_placeholder = st.empty()

    while True:
        stock_events = drain(stock_consumer)
        alert_events = drain(alert_consumer)

        for event in stock_events:
            symbol = event.get("symbol")
            price_history[symbol].append(event.get("price"))
            time_history[symbol].append(event.get("event_time"))
        total_events += len(stock_events)
        window_events += len(stock_events)

        for alert in alert_events:
            alerts.appendleft(alert)
        total_alerts += len(alert_events)

        elapsed = time.time() - window_start
        if elapsed >= 2:
            throughput = window_events / elapsed
            window_events = 0
            window_start = time.time()

        metric_cols[0].metric("Total events consumed", f"{total_events:,}")
        metric_cols[1].metric("Throughput (msg/s)", f"{throughput:,.1f}")
        metric_cols[2].metric("Total alerts", f"{total_alerts:,}")
        metric_cols[3].metric("Symbols tracked", len(price_history))

        with chart_placeholder.container():
            fig = go.Figure()
            for symbol, prices in price_history.items():
                fig.add_trace(
                    go.Scatter(
                        x=list(range(len(prices))),
                        y=list(prices),
                        mode="lines",
                        name=symbol,
                    )
                )
            fig.update_layout(
                title="Live price feed (most recent ticks)",
                xaxis_title="tick #",
                yaxis_title="price",
                height=450,
                legend_title="symbol",
            )
            st.plotly_chart(fig, use_container_width=True, key=f"chart-{total_events}")

        with alerts_placeholder.container():
            st.subheader("Recent anomaly alerts")
            if alerts:
                df = pd.DataFrame(list(alerts))
                st.dataframe(df, use_container_width=True, height=250)
            else:
                st.info("No anomalies detected yet.")

        time.sleep(config.DASHBOARD_REFRESH_SECONDS)


if __name__ == "__main__":
    main()
