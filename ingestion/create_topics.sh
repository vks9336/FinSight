#!/usr/bin/env bash
# Creates the three FinSight topics (spec 6.1 and 7.2 R2).
#
#   txn-raw      3 partitions -- Spark Streaming fraud, Spark Streaming churn,
#                and the HDFS sink each run as an independent consumer group,
#                so the topic needs parallelism to serve them concurrently.
#   txn-flagged  1 partition  -- fraud output volume is low.
#   txn-churn    1 partition  -- churn alert volume is lower still.
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-finsight-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
KT="/opt/kafka/bin/kafka-topics.sh"

create() {
  local name="$1" parts="$2"
  echo "==> $name (${parts} partition(s))"
  docker exec "$KAFKA_CONTAINER" "$KT" \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor 1 \
    --config retention.ms=604800000
}

create txn-raw     3
create txn-flagged 1
create txn-churn   1

echo
echo "==> current topics"
docker exec "$KAFKA_CONTAINER" "$KT" --bootstrap-server "$BOOTSTRAP" --list

echo
echo "==> txn-raw detail"
docker exec "$KAFKA_CONTAINER" "$KT" --bootstrap-server "$BOOTSTRAP" \
  --describe --topic txn-raw
