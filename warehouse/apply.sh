#!/usr/bin/env bash
# Applies the warehouse DDL through HiveServer2 via beeline.
#
#   ./warehouse/apply.sh              # both files, in order
#   ./warehouse/apply.sh hive_ddl.sql # just one
set -euo pipefail

HS2_CONTAINER="${HS2_CONTAINER:-finsight-hiveserver2}"
JDBC="${HIVE_JDBC:-jdbc:hive2://localhost:10000/default}"
HERE="$(cd "$(dirname "$0")" && pwd)"

FILES=("${@:-hive_ddl.sql hive_summary_mart.sql}")
if [ $# -eq 0 ]; then FILES=(hive_ddl.sql hive_summary_mart.sql); fi

echo "==> waiting for HiveServer2"
for _ in $(seq 1 60); do
  if docker exec "$HS2_CONTAINER" beeline -u "$JDBC" -e 'SHOW DATABASES;' >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

for f in "${FILES[@]}"; do
  echo
  echo "======================================================================"
  echo "==> applying $f"
  echo "======================================================================"
  docker cp "$HERE/$f" "$HS2_CONTAINER:/tmp/$f"
  docker exec "$HS2_CONTAINER" beeline -u "$JDBC" --silent=false -f "/tmp/$f"
done

echo
echo "==> done"
