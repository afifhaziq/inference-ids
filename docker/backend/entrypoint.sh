#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONFIG=/opt/kafka/config/kraft/server.properties
KAFKA_DATA_DIR=/var/lib/kafka/data

if [ ! -f "${KAFKA_DATA_DIR}/meta.properties" ]; then
    CLUSTER_ID=$(kafka-storage.sh random-uuid)
    kafka-storage.sh format -t "${CLUSTER_ID}" -c "${KAFKA_CONFIG}"
fi

kafka-server-start.sh "${KAFKA_CONFIG}" &
KAFKA_PID=$!
trap 'kill -TERM "${KAFKA_PID}" 2>/dev/null' EXIT

echo "Waiting for Kafka to accept connections on :9092..."
until nc -z localhost 9092; do
    sleep 1
done
echo "Kafka is up."

uv run python -m inference_ids --config config/default.yaml
