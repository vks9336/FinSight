#!/usr/bin/env python3
"""Real-time customer churn detection on the txn-raw stream (spec 7.2).

Where the fraud job scores individual transactions, this one maintains a rolling
behavioural profile per *customer* over a 24-step sliding window and raises an
alert when two or more of these signals fire together:

    S1  frequency collapse -- under 1 txn per 12 steps, for a customer whose
        historical rate was above 3 per 12 steps
    S2  spend collapse -- window average amount below 20% of all-time average
    S3  liquidation -- activity shifts exclusively to CASH_OUT, with no
        PAYMENT or DEBIT in the window
    S4  balance drain -- newbalanceOrig at zero or under $500 on two or more
        transactions

Alerts carry the customerId, the window bounds, and the triggering signals
(spec 7.2 R2), and are written to both the txn-churn topic and
/finsight/processed/churn_alerts/.

Runs concurrently with streaming_fraud.py against the same topic under a
separate consumer group, which is the point of spec 7.2 R1.

    spark-submit /opt/finsight/spark/streaming_churn.py
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from finsight_common import (
    CK_CHURN, KAFKA_BOOTSTRAP, P_CHURN_ALERTS, RAW_TRANSACTIONS, TOPIC_CHURN,
    TOPIC_RAW, build_session, read_kafka_stream,
)

# The dataset measures time in "steps" (1 step = 1 hour) rather than wall clock,
# so steps are projected onto a real timeline anchored here. That lets the 24-step
# requirement be expressed as a genuine 24-hour event-time window with a
# watermark, instead of being approximated with processing time.
STEP_EPOCH = os.environ.get("FINSIGHT_STEP_EPOCH", "2024-01-01 00:00:00")

WINDOW_STEPS = 24
SLIDE_STEPS = 1
FREQ_WINDOW_STEPS = 12
FREQ_COLLAPSE_PER_12 = 1.0     # S1 lower bound
FREQ_BASELINE_PER_12 = 3.0     # S1 historical qualifier
SPEND_COLLAPSE_RATIO = 0.20    # S2
LOW_BALANCE_THRESHOLD = 500.0  # S4
LOW_BALANCE_MIN_EVENTS = 2     # S4
MIN_SIGNALS = 2                # spec: "any two or more"

ALERT_COLUMNS = [
    "customerId", "windowStart", "windowEnd", "signals", "signalCount",
    "txnCount", "windowAvgAmount", "baselineAvgAmount", "baselineFreqPer12",
    "lowBalanceEvents", "cashOutOnly", "detectedAt",
]


def load_baseline(spark):
    """All-time per-customer averages, read once from the HDFS lake.

    Signals S1 and S2 are defined relative to a customer's history, which a
    stream alone cannot supply. This is a stream-static join: the baseline is
    broadcast to every executor, so it must stay small (one row per customer).
    """
    try:
        history = spark.read.parquet(RAW_TRANSACTIONS)
    except Exception as exc:  # noqa: BLE001
        print(f"[churn] no HDFS history at {RAW_TRANSACTIONS} ({exc}).")
        print("[churn] falling back to neutral baselines -- S1/S2 will not fire "
              "until the sink has landed data.")
        return spark.createDataFrame(
            [], "customerId string, baselineAvgAmount double, baselineFreqPer12 double"
        )

    span = history.agg(
        F.min("step").alias("minStep"), F.max("step").alias("maxStep")
    ).first()
    total_steps = max(1, (span["maxStep"] or 1) - (span["minStep"] or 0) + 1)

    return (
        history.groupBy(F.col("nameOrig").alias("customerId"))
        .agg(
            F.avg("amount").alias("baselineAvgAmount"),
            F.count("*").alias("historicalTxns"),
        )
        .withColumn(
            "baselineFreqPer12",
            F.col("historicalTxns") / F.lit(total_steps / FREQ_WINDOW_STEPS),
        )
        .select("customerId", "baselineAvgAmount", "baselineFreqPer12")
    )


def build_alerts(stream, baseline):
    """Window the stream per customer and evaluate the four churn signals."""
    events = (
        stream
        # step -> event time, so the 24-step requirement becomes a 24h window.
        .withColumn(
            "eventTime",
            (F.unix_timestamp(F.lit(STEP_EPOCH)) + F.col("step") * 3600).cast("timestamp"),
        )
        .withColumnRenamed("nameOrig", "customerId")
        .withWatermark("eventTime", "2 hours")
    )

    windowed = (
        events.groupBy(
            F.window(F.col("eventTime"), f"{WINDOW_STEPS} hours", f"{SLIDE_STEPS} hours"),
            F.col("customerId"),
        )
        .agg(
            F.count("*").alias("txnCount"),
            F.avg("amount").alias("windowAvgAmount"),
            F.collect_set("type").alias("txnTypes"),
            F.sum(
                F.when(F.col("newbalanceOrig") < LOW_BALANCE_THRESHOLD, 1).otherwise(0)
            ).alias("lowBalanceEvents"),
        )
        .select(
            F.col("customerId"),
            F.col("window.start").alias("windowStart"),
            F.col("window.end").alias("windowEnd"),
            "txnCount", "windowAvgAmount", "txnTypes", "lowBalanceEvents",
        )
    )

    # Left join keeps customers with no history; their baseline columns are null
    # and the null-safe comparisons below simply leave S1/S2 unfired.
    joined = windowed.join(F.broadcast(baseline), on="customerId", how="left")

    freq_per_12 = F.col("txnCount") / F.lit(WINDOW_STEPS / FREQ_WINDOW_STEPS)

    s1 = (freq_per_12 < F.lit(FREQ_COLLAPSE_PER_12)) & \
         (F.col("baselineFreqPer12") > F.lit(FREQ_BASELINE_PER_12))
    s2 = F.col("windowAvgAmount") < (F.col("baselineAvgAmount") * F.lit(SPEND_COLLAPSE_RATIO))
    s3 = F.array_contains(F.col("txnTypes"), "CASH_OUT") & \
         ~F.array_contains(F.col("txnTypes"), "PAYMENT") & \
         ~F.array_contains(F.col("txnTypes"), "DEBIT")
    s4 = F.col("lowBalanceEvents") >= F.lit(LOW_BALANCE_MIN_EVENTS)

    # coalesce guards the null baselines; a missing history must not count as a signal.
    named = [
        (F.coalesce(s1, F.lit(False)), "FREQUENCY_COLLAPSE"),
        (F.coalesce(s2, F.lit(False)), "SPEND_COLLAPSE"),
        (F.coalesce(s3, F.lit(False)), "CASH_OUT_LIQUIDATION"),
        (F.coalesce(s4, F.lit(False)), "BALANCE_DRAIN"),
    ]

    scored = joined.withColumn(
        "signals",
        F.array_compact(F.array(*[F.when(cond, F.lit(name)) for cond, name in named])),
    ).withColumn("signalCount", F.size(F.col("signals")))

    return (
        scored.filter(F.col("signalCount") >= MIN_SIGNALS)
        .withColumn("cashOutOnly", F.coalesce(s3, F.lit(False)))
        .withColumn("detectedAt", F.current_timestamp())
        .select(*ALERT_COLUMNS)
    )


def process_batch(batch_df, batch_id: int) -> None:
    batch_df.persist()
    try:
        n = batch_df.count()
        if n == 0:
            return

        (batch_df.select(
            F.col("customerId").cast("string").alias("key"),
            F.to_json(F.struct(*ALERT_COLUMNS)).alias("value"),
        )
         .write.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", TOPIC_CHURN)
         .save())

        (batch_df.withColumn("signals", F.concat_ws("|", F.col("signals")))
         .write.mode("append").parquet(P_CHURN_ALERTS))

        print(f"[churn] batch {batch_id}: {n:,} churn alert(s) raised", flush=True)
        batch_df.select("customerId", "windowStart", "windowEnd", "signals").show(5, truncate=False)
    finally:
        batch_df.unpersist()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--starting", default="latest", choices=["earliest", "latest"])
    ap.add_argument("--trigger", default="10 seconds")
    args = ap.parse_args()

    spark = build_session("FinSight-Streaming-Churn")

    baseline = load_baseline(spark).cache()
    print(f"[churn] baseline loaded for {baseline.count():,} customers")

    stream = read_kafka_stream(spark, TOPIC_RAW, starting=args.starting)
    alerts = build_alerts(stream, baseline)

    print(f"[churn] consuming {TOPIC_RAW} -> {TOPIC_CHURN} + {P_CHURN_ALERTS}")
    print(f"[churn] window={WINDOW_STEPS} steps slide={SLIDE_STEPS} "
          f"min signals={MIN_SIGNALS}")

    (alerts.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CK_CHURN)
        .trigger(processingTime=args.trigger)
        .start()
        .awaitTermination())


if __name__ == "__main__":
    main()
