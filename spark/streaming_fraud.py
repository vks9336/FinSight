#!/usr/bin/env python3
"""Real-time fraud detection on the txn-raw stream (spec 7.1).

A transaction is flagged when all three conditions hold:

    1. type is TRANSFER or CASH_OUT
    2. amount exceeds $200,000
    3. the destination post-transaction balance is zero

which is the classic account-emptying pattern.

Flagged records go to the txn-flagged topic and are persisted to HDFS. Each
micro-batch also emits a fraud-rate metric to /finsight/processed/streaming_metrics/
(spec 7.1 R2), and the checkpoint at /finsight/checkpoints/fraud gives the
exactly-once semantics and mid-stream recovery required by R1.

    spark-submit /opt/finsight/spark/streaming_fraud.py
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from finsight_common import (
    CK_FRAUD, KAFKA_BOOTSTRAP, P_FLAGGED, P_STREAMING_METRICS, TOPIC_FLAGGED,
    TOPIC_RAW, build_session, read_kafka_stream,
)

FRAUD_AMOUNT_THRESHOLD = 200_000.0

FLAGGED_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
    "detectionLatencyMs", "flaggedAt", "fraudReason",
]


def fraud_condition():
    """The three-part rule from spec 7.1, as a single reusable predicate."""
    return (
        F.col("type").isin("TRANSFER", "CASH_OUT")
        & (F.col("amount") > FRAUD_AMOUNT_THRESHOLD)
        & (F.col("newbalanceDest") == 0)
    )


def process_batch(batch_df, batch_id: int) -> None:
    # The batch is scanned three times (count, Kafka write, Parquet write);
    # without caching, Spark would re-pull from Kafka for each action.
    batch_df.persist()
    try:
        total = batch_df.count()
        if total == 0:
            return

        flagged = (
            batch_df.filter(fraud_condition())
            .withColumn("flaggedAt", (F.unix_timestamp(F.current_timestamp()) * 1000).cast("long"))
            .withColumn("detectionLatencyMs", F.col("flaggedAt") - F.col("ingestedAt"))
            .withColumn(
                "fraudReason",
                F.concat_ws(
                    " + ",
                    F.concat(F.lit("type="), F.col("type")),
                    F.concat(F.lit("amount>"), F.lit(f"{FRAUD_AMOUNT_THRESHOLD:,.0f}")),
                    F.lit("newbalanceDest=0"),
                ),
            )
            .select(*FLAGGED_COLUMNS)
        )
        flagged.persist()
        flagged_count = flagged.count()

        if flagged_count:
            (flagged.select(
                F.col("nameOrig").cast("string").alias("key"),
                F.to_json(F.struct(*FLAGGED_COLUMNS)).alias("value"),
            )
             .write.format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("topic", TOPIC_FLAGGED)
             .save())

            flagged.write.mode("append").parquet(P_FLAGGED)

        rate = 100.0 * flagged_count / total

        # spec 7.1 R2: running fraud rate per micro-batch, persisted for monitoring.
        metrics = batch_df.sparkSession.createDataFrame(
            [(int(batch_id), int(total), int(flagged_count), float(rate))],
            "batchId long, totalCount long, flaggedCount long, fraudRatePct double",
        ).withColumn("recordedAt", F.current_timestamp())
        metrics.write.mode("append").parquet(P_STREAMING_METRICS)

        latency = ""
        if flagged_count:
            avg_ms = flagged.agg(F.avg("detectionLatencyMs")).first()[0]
            if avg_ms is not None:
                latency = f" | avg detection latency {avg_ms:,.0f} ms"

        print(f"[fraud] batch {batch_id}: total={total:,} flagged={flagged_count:,} "
              f"rate={rate:.2f}%{latency}", flush=True)

        flagged.unpersist()
    finally:
        batch_df.unpersist()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--starting", default="latest", choices=["earliest", "latest"])
    ap.add_argument("--trigger", default="5 seconds")
    args = ap.parse_args()

    spark = build_session("FinSight-Streaming-Fraud")
    stream = read_kafka_stream(spark, TOPIC_RAW, starting=args.starting)

    print(f"[fraud] consuming {TOPIC_RAW} -> {TOPIC_FLAGGED}")
    print(f"[fraud] rule: type IN (TRANSFER, CASH_OUT) "
          f"AND amount > {FRAUD_AMOUNT_THRESHOLD:,.0f} AND newbalanceDest = 0")

    (stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CK_FRAUD)
        .trigger(processingTime=args.trigger)
        .start()
        .awaitTermination())


if __name__ == "__main__":
    main()
