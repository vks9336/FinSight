# Runbook

## Prerequisites

- Python 3.11+ (3.14 is what this was built against)
- Docker with at least 12GB of memory allocated

On Apple Silicon, Colima gives explicit control over VM memory:

```bash
brew install colima docker docker-compose
colima start --cpu 6 --memory 12 --disk 80
```

Docker Desktop works too — raise the memory limit under
Settings → Resources → Memory.

## First run

```bash
make setup      # venv and dependencies
make data       # demo dataset; add `make data-full` for the 6.3M-row CSV
```

## No-cluster path

Everything analytical works without Docker. This is also the spec's stated
Day 1 fallback ("switch to direct file load to HDFS").

```bash
make offline    # produce all exports straight from the CSV
make blend      # run both Alteryx-substitute workflows
make verify     # 35 KPI checks against the specification
make dashboards # http://localhost:8501
```

## Day 1 — Kafka to HDFS

```bash
make up-core        # namenode, datanode, HDFS directory tree
make up-ingest      # Kafka and Kafka Connect
make topics         # txn-raw (3 partitions), txn-flagged, txn-churn
```

Then, in two shells:

```bash
make produce        # replay the CSV at ~1000 msg/s
make connector      # register the HDFS 3 Sink
```

If the connector fails to register — most likely the Confluent evaluation
licence — use the Apache-licensed sink instead. It writes the same layout:

```bash
make hdfs-sink-spark
```

Confirm the landing:

```bash
docker exec finsight-namenode hdfs dfs -ls /finsight/raw/transactions | head
```

You should see `step=1/`, `step=2/`, … directories containing Parquet files.
The NameNode UI at http://localhost:9870 shows the same tree under
**Utilities → Browse the file system**.

## Day 2 — Spark

```bash
make up          # spark-master and spark-worker need the compute profile
```

The two streaming jobs are meant to run concurrently (spec 7.2 R1), so start
each in its own shell:

```bash
make stream-fraud
make stream-churn
```

Both appear in the Spark UI at http://localhost:8080 under their own
application names, `FinSight-Streaming-Fraud` and `FinSight-Streaming-Churn`.

Then the batch and SQL jobs:

```bash
make batch-risk
make batch-clv
make sql-all
```

## Day 3 — Databases

```bash
make up-warehouse
make up-nosql
make hive        # external table, fraud view, statistics, summary mart
make mongo       # import, indexes, segment validation
make neo4j       # graph load + money mule query
```

`make neo4j` exits non-zero if the fraud ring query surfaces fewer than five
suspicious accounts, which is the spec's Day 3 acceptance criterion.

## Day 4 — Blending

```bash
make blend
```

Produces `exports/customer_risk_blend.xlsx` (10,000 rows) and
`exports/transaction_summary.csv` (591 rows).

## Day 5 — Dashboards

```bash
make verify
make dashboards
```

See [05_demo_script.md](05_demo_script.md) for the walkthrough.

## Scheduling the nightly batch

`orchestration/crontab.example` runs the risk scoring and CLV jobs after market
close, then refreshes the summary mart. Install with:

```bash
crontab orchestration/crontab.example
```

## Troubleshooting

**NameNode will not start after a `make clean`.**
The format check keys on `/hadoop/dfs/name/current`. If the volume was deleted
but the container kept its old state, remove both:
`docker compose -f infra/docker-compose.yml down -v` then `make up-core`.

**Hive table reads as empty despite files on HDFS.**
Partitions on disk are invisible to the metastore until it is told about them.
Run `MSCK REPAIR TABLE finsight.transactions;`, which `make hive` does, but which
must be repeated whenever new `step=` directories appear.

**Spark cannot see the Hive metastore.**
Check that `spark.sql.hive.metastore.version` matches the running Hive image.
If the handshake still fails, every SQL job takes `--no-hive` and reads the same
Parquet directly:

```bash
docker exec finsight-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/finsight/spark/finsight_common.py \
  /opt/finsight/spark/spark_sql_jobs.py --mode all --no-hive
```

**Streaming job produces nothing.**
Both jobs default to `--starting latest`, so they only see messages produced
after they start. Either start them before `make produce`, or pass
`--starting earliest`.

**Churn job raises no alerts.**
It needs the HDFS history to compute per-customer baselines for signals S1 and
S2. Without it, only the two history-free signals can fire and two are required.
Land data first, then start the job.

**Producer reports `BufferError` or a falling rate.**
The broker is not keeping up. Lower the rate: `--rate 500`.

**Containers are killed with exit code 137.**
Out of memory. Bring up fewer profiles rather than the full `make up`, or raise
the Docker memory allocation.

**Dashboard shows "Required export is missing".**
Run `make offline` (no cluster) or the relevant Spark job.

## Resetting

```bash
make down     # stop containers, keep data
make clean    # stop containers and delete all volumes
rm -rf exports/*.csv exports/*.xlsx
```
