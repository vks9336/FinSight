#!/usr/bin/env bash
# Dispatches to a specific HDFS daemon and runs it in the foreground so Docker
# owns the process lifecycle. Usage: entrypoint.sh {namenode|datanode}
set -euo pipefail

ROLE="${1:-}"

case "$ROLE" in
  namenode)
    # /hadoop/dfs/name is a named volume, so format only on very first boot.
    if [ ! -d /hadoop/dfs/name/current ]; then
      echo "[entrypoint] formatting namenode (first boot)"
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode
    ;;

  datanode)
    exec hdfs datanode
    ;;

  bootstrap)
    # Creates the FinSight directory tree once the namenode leaves safe mode.
    echo "[entrypoint] waiting for namenode to exit safe mode"
    hdfs dfsadmin -safemode wait
    for d in /finsight/raw/transactions \
             /finsight/processed/streaming_metrics \
             /finsight/processed/churn_alerts \
             /finsight/processed/risk_scores \
             /finsight/processed/daily_summary \
             /finsight/processed/clv_scores \
             /finsight/processed/customer_fraud_summary \
             /finsight/processed/compliance_summary \
             /finsight/processed/dormancy_report \
             /finsight/processed/flagged \
             /finsight/checkpoints \
             /finsight/exports \
             /user/hive/warehouse; do
      hdfs dfs -mkdir -p "$d"
    done
    hdfs dfs -chmod -R 777 /finsight /user/hive
    echo "[entrypoint] HDFS layout ready:"
    hdfs dfs -ls -R /finsight | head -40
    ;;

  *)
    exec "$@"
    ;;
esac
