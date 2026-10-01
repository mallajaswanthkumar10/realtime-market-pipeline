#!/usr/bin/env bash
# Creates all Kafka topics with explicit partitions + 7-day retention
# (enables the replay/backfill capability in consumer/replay.py).
# Run after `docker compose up -d`:
#   docker exec -it pipeline-kafka bash -c "$(cat infra/create_topics.sh)"
# or, from a host with kafka-topics.sh on PATH, point --bootstrap-server
# at localhost:9092 instead of running inside the container.

set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-6}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}" # 7 days

create_topic() {
  local topic="$1"
  local partitions="${2:-$PARTITIONS}"
  kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms="$RETENTION_MS"
  echo "ensured topic: $topic (partitions=$partitions, retention.ms=$RETENTION_MS)"
}

create_topic "stock.raw" 6
create_topic "iot.raw" 6
create_topic "alerts" 3
create_topic "alerts.dlq" 1

kafka-topics --bootstrap-server "$BROKER" --list
