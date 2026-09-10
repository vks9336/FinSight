# Day 5 Demo Script

A 20-minute end-to-end walkthrough of the FinSight platform. Times are a guide.

## Before you start

```bash
make up            # all 12 containers
make verify        # confirm all 35 KPIs still hold
```

Have these open:

- http://localhost:9870 — HDFS NameNode
- http://localhost:8080 — Spark Master
- http://localhost:7474 — Neo4j Browser
- http://localhost:8501 — dashboards

Have four terminals ready: producer, fraud job, churn job, and a scratch shell.

---

## 1. The problem (2 min)

NovaCrest Bank processes over 2 million transactions a day and has three
problems: no real-time fraud detection, no unified customer view, and a
compliance process that takes 18 working days per quarter.

Show the raw input:

```bash
head -3 data/raw/demo/transactions.csv
```

Point out that this is the whole starting position — a flat file. Everything
that follows is built from it.

---

## 2. Ingestion: Kafka to HDFS (4 min)

Show the topics, and why `txn-raw` has three partitions:

```bash
docker exec finsight-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe --topic txn-raw
```

> Three partitions because three independent consumer groups read this topic:
> the fraud job, the churn job, and the HDFS sink.

Start the producer:

```bash
make produce
```

It reports a sustained rate — the spec asks for roughly 1,000 messages per
second. While it runs, show the data landing:

```bash
docker exec finsight-namenode hdfs dfs -ls /finsight/raw/transactions | head
```

The `step=N/` directories are the field partitioning from the sink. Open the
NameNode UI and browse the same tree to make the point visually.

---

## 3. Real-time processing (5 min)

This is the centrepiece: two independent streaming applications on one topic.

Terminal 2:

```bash
make stream-fraud
```

Terminal 3:

```bash
make stream-churn
```

Open the Spark Master UI. Both applications are listed under their own names,
`FinSight-Streaming-Fraud` and `FinSight-Streaming-Churn`, running
simultaneously — which is exactly what spec 7.2 R1 asks to be demonstrated.

The fraud job logs each micro-batch:

```
[fraud] batch 7: total=4,213 flagged=31 rate=0.74% | avg detection latency 840 ms
```

Two things to call out: the fraud rate metric is persisted to HDFS for
monitoring, and the detection latency is well inside the 2-second business SLA.

Show the flagged output arriving on its own topic:

```bash
docker exec finsight-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic txn-flagged --from-beginning --max-messages 3
```

The churn job is doing something different — it holds a rolling 24-step profile
per customer and fires when two or more behavioural signals trip together.

---

## 4. Batch and SQL (3 min)

```bash
make batch-risk
make batch-clv
```

Both jobs appear separately in the Spark UI with distinct application names, and
share no state or output path — a requirement in spec 7.4 R2 precisely so they
can be told apart here.

Then the three SQL modes from a single entry point:

```bash
make sql-all
```

The dormancy report is worth pausing on: 134 dormant accounts, 27 of them
severely dormant, each meeting three criteria — over 72 steps of inactivity, at
least five prior transactions, and a customer rather than merchant account.

---

## 5. Multi-model storage (3 min)

Hive first — the external table and the shared fraud view:

```bash
docker exec finsight-hiveserver2 beeline -u jdbc:hive2://localhost:10000/default \
  -e "SELECT COUNT(*) FROM finsight.vw_fraud_transactions;"
```

> One view definition of "confirmed fraud", used by both the Neo4j load and the
> Fraud Alert Board, so the two cannot drift apart.

MongoDB, validated at import time:

```bash
make mongo
```

The aggregation checks the segment distribution against the spec before the
blending workflow is allowed to run.

Then the graph, which is the most visual part of the demo:

```bash
make neo4j
```

It loads 499 accounts, 1,554 transactions and 3,108 edges, then runs the money
mule query. Switch to the Neo4j Browser and run:

```cypher
MATCH (sender:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(receiver:Account)
WITH receiver, collect(DISTINCT sender) AS senders
WHERE size(senders) > 3
RETURN receiver, senders LIMIT 5
```

Switch to graph view. The fan-in structure is immediately visible — many senders
converging on one account. That pattern is invisible in a per-transaction
tabular rule, which is the whole argument for holding this data as a graph.

---

## 6. Blending and dashboards (3 min)

```bash
make blend
```

The composite risk formula is the one the spec specifies:
`(fraud_rate_pct × 0.6) + (churn_probability × 0.4)`.

Then open the dashboards and walk the three pages:

**Fraud Alert Board** — fraud and risk team. Four KPIs, the fraud volume trend,
the type breakdown, and a live feed reading directly off `txn-flagged`. The
false positive rate is computed from ground truth rather than asserted: 159
flagged, 158 confirmed, one false positive, against a 62% legacy baseline.

**Customer 360** — relationship managers. The scatter is the useful one: risk
against churn, coloured by segment, with quadrant guides at the portfolio means.
The top-right cluster is the outreach list. Data from four previously
disconnected systems in one view.

**Risk & Compliance** — compliance officers. 1,554 transactions, $312M, 158
confirmed fraud at 10.2%, 134 dormant accounts. The audit table at the bottom is
the artefact that goes to regulators, and the transaction type filter at the top
is how officers drill in during review.

---

## 7. Close (1 min)

```bash
make verify
```

35 checks covering every figure the specification publishes — transaction
counts, fraud rates, total volume, dormancy tiers, segment distribution, the
composite risk formula — all asserted against the actual artefacts rather than
asserted in prose.

Three outcomes against the three original problems: fraud detected in under two
seconds instead of 24–48 hours, a unified customer view spanning four systems,
and a compliance report that regenerates on demand instead of taking 18 working
days.

---

## If something breaks

The dashboards read export files, so they work even if the cluster is down.
`make offline` regenerates every export straight from the CSV in about ten
seconds. See the troubleshooting section of [02_runbook.md](02_runbook.md).
