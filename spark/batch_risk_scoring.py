#!/usr/bin/env python3
"""Nightly composite risk scoring over the HDFS transaction history (spec 7.3).

Computes a rolling 7-day (168-step) risk score per customer account from four
weighted factors, then assigns each customer to a risk tier (R1) and emits a
per-type/per-step daily summary that Alteryx Workflow 2 consumes (R2).

    Input   /finsight/raw/transactions/
    Output  /finsight/processed/risk_scores/
            /finsight/processed/daily_summary/

Scheduled after market close; see orchestration/crontab.example.

    spark-submit /opt/finsight/spark/batch_risk_scoring.py
"""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from finsight_common import (
    HIVE_DB, P_DAILY_SUMMARY, P_RISK_SCORES, RAW_TRANSACTIONS, build_session,
    read_transactions,
)

ROLLING_WINDOW_STEPS = 168   # 7 days at 1 step = 1 hour

# The spec names the four factors but not their weights. These are chosen so
# that behavioural breadth (how many distinct destinations an account reaches)
# and cash-out concentration together outweigh raw volume, which is the usual
# shape of a mule-detection score. They sum to 1.0.
W_FREQUENCY = 0.25
W_AVG_TRANSFER = 0.30
W_CASHOUT_RATIO = 0.25
W_UNIQUE_DESTS = 0.20

TIER_LOW_MAX = 0.25          # R1: Low < 0.25
TIER_MEDIUM_MAX = 0.60       # R1: Medium 0.25-0.60, High > 0.60


def minmax(col: str, alias: str):
    """Min-max normalise a column to [0, 1] across all customers.

    A degenerate column (every customer identical) would divide by zero, so the
    denominator is floored and the result collapses to 0 rather than NaN.
    """
    w = Window.partitionBy()
    lo = F.min(col).over(w)
    hi = F.max(col).over(w)
    span = hi - lo
    return F.when(span <= 0, F.lit(0.0)).otherwise((F.col(col) - lo) / span).alias(alias)


def compute_risk_scores(df):
    max_step = df.agg(F.max("step")).first()[0]
    window_start = max(0, (max_step or 0) - ROLLING_WINDOW_STEPS)
    recent = df.filter(F.col("step") > window_start)

    per_customer = (
        recent.groupBy(F.col("nameOrig").alias("customerId"))
        .agg(
            F.count("*").alias("txnCount"),
            F.avg(F.when(F.col("type") == "TRANSFER", F.col("amount"))).alias("avgTransferAmount"),
            F.sum(F.when(F.col("type") == "CASH_OUT", 1).otherwise(0)).alias("cashOutCount"),
            F.countDistinct("nameDest").alias("uniqueDestinations"),
            F.sum("amount").alias("totalAmount"),
            F.sum("isFraud").alias("fraudCount"),
        )
        # Customers with no TRANSFER activity have a null average, which must
        # read as "no transfer risk" rather than propagate a null through the
        # weighted sum and void the whole score.
        .withColumn("avgTransferAmount", F.coalesce(F.col("avgTransferAmount"), F.lit(0.0)))
        .withColumn("cashOutRatio", F.col("cashOutCount") / F.col("txnCount"))
    )

    normalised = per_customer.select(
        "*",
        minmax("txnCount", "nFrequency"),
        minmax("avgTransferAmount", "nAvgTransfer"),
        minmax("cashOutRatio", "nCashOutRatio"),
        minmax("uniqueDestinations", "nUniqueDests"),
    )

    scored = normalised.withColumn(
        "risk_score",
        F.round(
            F.col("nFrequency") * F.lit(W_FREQUENCY)
            + F.col("nAvgTransfer") * F.lit(W_AVG_TRANSFER)
            + F.col("nCashOutRatio") * F.lit(W_CASHOUT_RATIO)
            + F.col("nUniqueDests") * F.lit(W_UNIQUE_DESTS),
            4,
        ),
    ).withColumn(
        "risk_tier",
        F.when(F.col("risk_score") < TIER_LOW_MAX, "Low")
        .when(F.col("risk_score") <= TIER_MEDIUM_MAX, "Medium")
        .otherwise("High"),
    ).withColumn("windowStartStep", F.lit(window_start)) \
     .withColumn("windowEndStep", F.lit(max_step)) \
     .withColumn("scoredAt", F.current_timestamp())

    return scored.select(
        "customerId", "risk_score", "risk_tier", "txnCount", "totalAmount",
        "avgTransferAmount", "cashOutRatio", "uniqueDestinations", "fraudCount",
        "windowStartStep", "windowEndStep", "scoredAt",
    )


def compute_daily_summary(df):
    """spec 7.3 R2: volume, amount and fraud count by transaction type and step."""
    return (
        df.groupBy("type", "step")
        .agg(
            F.count("*").alias("txn_volume"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("isFraud").alias("fraud_count"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(100.0 * F.col("fraud_count") / F.col("txn_volume"), 4),
        )
        .orderBy("step", "type")
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=RAW_TRANSACTIONS)
    ap.add_argument("--refresh-hive", action="store_true",
                    help="repoint the Hive risk_scores table at the new output")
    args = ap.parse_args()

    # Distinct appName so this job is separable from the CLV job in the Spark UI.
    spark = build_session("FinSight-Batch-RiskScoring")

    df = read_transactions(spark, args.input).cache()
    print(f"[risk] read {df.count():,} transactions from {args.input}")

    scores = compute_risk_scores(df)
    scores.write.mode("overwrite").parquet(P_RISK_SCORES)
    print(f"[risk] wrote risk scores -> {P_RISK_SCORES}")

    tiers = scores.groupBy("risk_tier").count().collect()
    for row in sorted(tiers, key=lambda r: r["risk_tier"]):
        print(f"[risk]   {row['risk_tier']:<7} {row['count']:>8,}")

    summary = compute_daily_summary(df)
    summary.write.mode("overwrite").parquet(P_DAILY_SUMMARY)
    print(f"[risk] wrote daily summary -> {P_DAILY_SUMMARY}")

    if args.refresh_hive:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.risk_scores")
        spark.sql(f"CREATE EXTERNAL TABLE {HIVE_DB}.risk_scores "
                  f"USING PARQUET LOCATION '{P_RISK_SCORES}'")
        print(f"[risk] registered {HIVE_DB}.risk_scores")

    spark.stop()


if __name__ == "__main__":
    main()
