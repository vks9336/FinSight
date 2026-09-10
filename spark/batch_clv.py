#!/usr/bin/env python3
"""Customer Lifetime Value scoring (spec 7.4).

Where batch_risk_scoring.py measures threat, this job measures revenue
potential. Four weighted components, all derived from the transaction dataset
alone:

    Transaction Volume     30%  cumulative amount, normalised to the top spender
    Transaction Frequency  25%  transaction count, normalised to the most active
    Product Diversity      25%  distinct transaction types / 5
    Recency                20%  inverse steps since last activity; zero if the
                                customer has been silent for over 48 steps

Tiers: High Value > 0.70, Growth Potential 0.40-0.70, At Risk < 0.40.

Per R2 this job shares no state or output path with the risk scoring job and
declares its own appName, so both can run on the same cluster and be told apart
in the Spark UI. Per R1 the output is registered as finsight.customer_clv.

    spark-submit /opt/finsight/spark/batch_clv.py
"""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from finsight_common import (
    HIVE_DB, P_CLV_SCORES, RAW_TRANSACTIONS, build_session, read_transactions,
)

W_VOLUME = 0.30
W_FREQUENCY = 0.25
W_DIVERSITY = 0.25
W_RECENCY = 0.20

ALL_TXN_TYPES = 5            # PAYMENT, TRANSFER, CASH_IN, DEBIT, CASH_OUT
RECENCY_CUTOFF_STEPS = 48    # silent longer than this scores 0

TIER_HIGH_MIN = 0.70
TIER_GROWTH_MIN = 0.40


def normalise_to_max(col: str, alias: str):
    """Scale to [0, 1] against the dataset maximum, per the spec's wording
    ("normalised relative to the highest-spending account")."""
    hi = F.max(col).over(Window.partitionBy())
    return F.when(hi <= 0, F.lit(0.0)).otherwise(F.col(col) / hi).alias(alias)


def compute_clv(df):
    max_step = df.agg(F.max("step")).first()[0] or 0

    per_customer = (
        df.groupBy(F.col("nameOrig").alias("customerId"))
        .agg(
            F.sum("amount").alias("totalAmount"),
            F.count("*").alias("txnCount"),
            F.countDistinct("type").alias("distinctTypes"),
            F.max("step").alias("lastActiveStep"),
        )
        .withColumn("stepsSinceLastTxn", F.lit(max_step) - F.col("lastActiveStep"))
    )

    components = per_customer.select(
        "*",
        normalise_to_max("totalAmount", "volumeScore"),
        normalise_to_max("txnCount", "frequencyScore"),
    ).withColumn(
        "diversityScore", F.col("distinctTypes") / F.lit(float(ALL_TXN_TYPES))
    ).withColumn(
        # Inverse of steps since last activity, so recent activity scores higher.
        # The +1 keeps a customer active on the final step from dividing by zero.
        "recencyScore",
        F.when(F.col("stepsSinceLastTxn") > RECENCY_CUTOFF_STEPS, F.lit(0.0))
        .otherwise(
            (F.lit(float(RECENCY_CUTOFF_STEPS)) - F.col("stepsSinceLastTxn"))
            / F.lit(float(RECENCY_CUTOFF_STEPS))
        ),
    )

    return (
        components.withColumn(
            "clv_score",
            F.round(
                F.col("volumeScore") * F.lit(W_VOLUME)
                + F.col("frequencyScore") * F.lit(W_FREQUENCY)
                + F.col("diversityScore") * F.lit(W_DIVERSITY)
                + F.col("recencyScore") * F.lit(W_RECENCY),
                4,
            ),
        )
        .withColumn(
            "clv_tier",
            F.when(F.col("clv_score") > TIER_HIGH_MIN, "High Value")
            .when(F.col("clv_score") >= TIER_GROWTH_MIN, "Growth Potential")
            .otherwise("At Risk"),
        )
        .withColumn("scoredAt", F.current_timestamp())
        .select(
            "customerId", "clv_score", "clv_tier", "volumeScore", "frequencyScore",
            "diversityScore", "recencyScore", "totalAmount", "txnCount",
            "distinctTypes", "lastActiveStep", "stepsSinceLastTxn", "scoredAt",
        )
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=RAW_TRANSACTIONS)
    ap.add_argument("--skip-hive", action="store_true")
    args = ap.parse_args()

    spark = build_session("FinSight-Batch-CLV")

    df = read_transactions(spark, args.input)
    clv = compute_clv(df)
    clv.write.mode("overwrite").parquet(P_CLV_SCORES)
    print(f"[clv] wrote CLV scores -> {P_CLV_SCORES}")

    for row in clv.groupBy("clv_tier").count().collect():
        print(f"[clv]   {row['clv_tier']:<17} {row['count']:>8,}")

    if not args.skip_hive:
        # R1: a second external table so downstream Spark SQL can join CLV
        # without re-reading files in every job.
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
        spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.customer_clv")
        spark.sql(f"CREATE EXTERNAL TABLE {HIVE_DB}.customer_clv "
                  f"USING PARQUET LOCATION '{P_CLV_SCORES}'")
        print(f"[clv] registered {HIVE_DB}.customer_clv")

    spark.stop()


if __name__ == "__main__":
    main()
