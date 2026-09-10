#!/usr/bin/env bash
# Registers the HDFS 3 Sink connector and reports whether Parquet is landing.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG="$(dirname "$0")/connectors/hdfs3-sink.json"
NAME="finsight-hdfs-sink"

echo "==> waiting for Kafka Connect at $CONNECT_URL"
for _ in $(seq 1 60); do
  if curl -sf "$CONNECT_URL/connectors" > /dev/null; then break; fi
  sleep 3
done

echo "==> installed plugins"
curl -s "$CONNECT_URL/connector-plugins" | python3 -c \
  'import json,sys; [print("   ", p["class"]) for p in json.load(sys.stdin)]'

# Delete first so re-running picks up config edits instead of silently no-oping.
curl -s -X DELETE "$CONNECT_URL/connectors/$NAME" > /dev/null 2>&1 || true

echo "==> registering $NAME"
code=$(curl -s -o /tmp/connect_resp.json -w '%{http_code}' \
  -X POST -H 'Content-Type: application/json' \
  --data @"$CONFIG" "$CONNECT_URL/connectors")

if [ "$code" != "201" ] && [ "$code" != "200" ]; then
  echo "!! registration failed (HTTP $code)"
  cat /tmp/connect_resp.json
  echo
  echo "!! Fall back to the Spark sink, which writes the identical layout:"
  echo "     make hdfs-sink-spark"
  exit 1
fi

sleep 8
echo "==> status"
curl -s "$CONNECT_URL/connectors/$NAME/status" | python3 -m json.tool
