#!/usr/bin/env python3
"""Spark SQL analytics over the Hive warehouse (spec 7.5 and 7.6).

Three independently triggerable modes behind one entry point, so the Day 5
walkthrough can run any of them without restarting the Spark session
(spec 7.5 R2, extended by 7.6 R2):

    --mode compliance   weekly compliance summary by transaction type
    --mode customer     per-nameOrig fraud summary (Alteryx Input A)
    --mode dormancy     account dormancy report with severity tiers
    --mode all          all three in sequence

    spark-submit /opt/finsight/spark/spark_sql_jobs.py --mode compliance
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from finsight_common import (
    EXPORTS, HIVE_DB, P_COMPLIANCE, P_CUSTOMER_FRAUD, P_DORMANCY,
    RAW_TRANSACTIONS, build_session, read_transactions,
)

COMPLIANCE_WINDOW_STEPS = 168   # 7 days, spec 7.5
DORMANCY_INACTIVITY_STEPS = 72  # spec 7.6
DORMANCY_SEVERE_STEPS = 120     # spec 7.6 R1
DORMANCY_MIN_HISTORY = 5        # spec 7.6


def register_source(spark, use_hive: bool) -> str:
    """Return the table name the queries should read from.

    Preference is the Hive external table, since the spec frames these as
    warehouse queries. If the metastore is unreachable the same Parquet files
    are registered as a temp view instead, so the SQL below is unchanged and
    the report still runs.
    """
    table = f"{HIVE_DB}.transactions"
    if use_hive:
        try:
            spark.sql(f"SELECT 1 FROM {table} LIMIT 1").collect()
            print(f"[sql] source: Hive table {table}")
            return table
        except Exception as exc:  # noqa: BLE001
            print(f"[sql] Hive table {table} unavailable ({type(exc).__name__}); "
                  f"falling back to Parquet at {RAW_TRANSACTIONS}")

    read_transactions(spark).createOrReplaceTempView("transactions_src")
    print(f"[sql] source: Parquet {RAW_TRANSACTIONS}")
    return "transactions_src"


def write_single_csv(df, target_path: str) -> None:
    """Write exactly one CSV file at `target_path`.

    Spark writes a directory of part files; the Alteryx workflow (spec 7.6)
    expects a single named file, so the part is moved into place via the
    Hadoop FileSystem API and the staging directory removed.
    """
    spark = df.sparkSession
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    staging = f"{target_path}__staging"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(staging)

    fs = Path(staging).getFileSystem(hconf)
    part = next(
        f.getPath() for f in fs.listStatus(Path(staging))
        if f.getPath().getName().startswith("part-") and f.getPath().getName().endswith(".csv")
    )
    dest = Path(target_path)
    if fs.exists(dest):
        fs.delete(dest, False)
    fs.rename(part, dest)
    fs.delete(Path(staging), True)
    print(f"[sql] wrote {target_path}")


# --------------------------------------------------------------------------
# spec 7.5 -- weekly compliance aggregation
# --------------------------------------------------------------------------

def run_compliance(spark, src: str):
    max_step = spark.sql(f"SELECT MAX(step) AS s FROM {src}").first()["s"] or 0
    window_start = max(0, max_step - COMPLIANCE_WINDOW_STEPS)

    df = spark.sql(f"""
        SELECT
            type                                              AS transaction_type,
            COUNT(*)                                          AS txn_count,
            ROUND(SUM(amount), 2)                             AS total_volume,
            ROUND(AVG(amount), 2)                             AS avg_amount,
            ROUND(MAX(amount), 2)                             AS max_amount,
            SUM(isFraud)                                      AS fraud_count,
            SUM(isFlaggedFraud)                               AS system_flagged_count,
            ROUND(SUM(CASE WHEN isFraud = 1 THEN amount ELSE 0 END), 2)
                                                              AS fraud_volume,
            ROUND(100.0 * SUM(isFraud) / COUNT(*), 4)         AS fraud_rate_pct,
            {window_start}                                    AS window_start_step,
            {max_step}                                        AS window_end_step
        FROM {src}
        WHERE step > {window_start} AND step <= {max_step}
        GROUP BY type
        ORDER BY total_volume DESC
    """)

    # Risk classification drives the colour coding on the compliance audit table.
    df = df.withColumn(
        "risk_classification",
        F.when(F.col("fraud_rate_pct") >= 10, "HIGH")
        .when(F.col("fraud_rate_pct") >= 2, "MEDIUM")
        .otherwise("LOW"),
    )

    df.write.mode("overwrite").parquet(P_COMPLIANCE)
    write_single_csv(df, f"{EXPORTS}/compliance_summary.csv")
    print(f"[sql] compliance summary -> {P_COMPLIANCE} (steps {window_start}-{max_step})")
    df.show(truncate=False)
    return df


# --------------------------------------------------------------------------
# spec 7.5 R1 -- customer-level fraud summary (Alteryx Input A)
# --------------------------------------------------------------------------

def run_customer(spark, src: str):
    df = spark.sql(f"""
        SELECT
            nameOrig                                     AS customerId,
            COUNT(*)                                     AS txn_count,
            ROUND(SUM(amount), 2)                        AS total_amount,
            ROUND(AVG(amount), 2)                        AS avg_amount,
            SUM(isFraud)                                 AS fraud_count,
            ROUND(100.0 * SUM(isFraud) / COUNT(*), 4)    AS fraud_rate_pct,
            COUNT(DISTINCT nameDest)                     AS unique_destinations,
            MAX(step)                                    AS last_active_step
        FROM {src}
        GROUP BY nameOrig
        ORDER BY fraud_count DESC, total_amount DESC
    """)

    df.write.mode("overwrite").parquet(P_CUSTOMER_FRAUD)
    write_single_csv(df, f"{EXPORTS}/customer_fraud_summary.csv")
    print(f"[sql] customer fraud summary -> {P_CUSTOMER_FRAUD} ({df.count():,} customers)")
    df.show(10, truncate=False)
    return df


# --------------------------------------------------------------------------
# spec 7.6 -- account dormancy report
# --------------------------------------------------------------------------

def run_dormancy(spark, src: str):
    max_step = spark.sql(f"SELECT MAX(step) AS s FROM {src}").first()["s"] or 0

    df = spark.sql(f"""
        WITH account_activity AS (
            SELECT
                nameOrig                  AS customerId,
                MAX(step)                 AS last_active_step,
                MIN(step)                 AS first_active_step,
                COUNT(*)                  AS txn_count,
                ROUND(SUM(amount), 2)     AS total_amount,
                ROUND(AVG(amount), 2)     AS avg_amount,
                SUM(isFraud)              AS fraud_count
            FROM {src}
            -- Merchant accounts run on different activity cycles and are
            -- excluded from dormancy classification (spec 7.6).
            WHERE nameOrig LIKE 'C%'
            GROUP BY nameOrig
        )
        SELECT
            customerId,
            last_active_step,
            first_active_step,
            txn_count,
            total_amount,
            avg_amount,
            fraud_count,
            {max_step}                          AS dataset_max_step,
            {max_step} - last_active_step       AS steps_inactive,
            CASE
                WHEN {max_step} - last_active_step > {DORMANCY_SEVERE_STEPS}
                    THEN 'Severely Dormant'
                ELSE 'Dormant'
            END                                 AS dormancy_severity
        FROM account_activity
        WHERE {max_step} - last_active_step > {DORMANCY_INACTIVITY_STEPS}
          -- Newly opened accounts lack the history to be judged dormant.
          AND txn_count >= {DORMANCY_MIN_HISTORY}
        ORDER BY steps_inactive DESC
    """)

    df.write.mode("overwrite").parquet(P_DORMANCY)
    # spec 7.6: CSV for direct consumption by the Alteryx Customer Risk Blend.
    write_single_csv(df, f"{EXPORTS}/dormancy_report.csv")

    counts = {r["dormancy_severity"]: r["n"] for r in
              df.groupBy("dormancy_severity").agg(F.count("*").alias("n")).collect()}
    total = sum(counts.values())
    print(f"[sql] dormancy report -> {P_DORMANCY}")
    print(f"[sql]   total dormant     {total:>6,}")
    print(f"[sql]   Dormant           {counts.get('Dormant', 0):>6,}")
    print(f"[sql]   Severely Dormant  {counts.get('Severely Dormant', 0):>6,}")
    return df


MODES = {
    "compliance": run_compliance,
    "customer": run_customer,
    "dormancy": run_dormancy,
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=[*MODES, "all"], required=True)
    ap.add_argument("--no-hive", action="store_true",
                    help="read Parquet directly instead of via the metastore")
    args = ap.parse_args()

    spark = build_session("FinSight-SparkSQL")
    src = register_source(spark, use_hive=not args.no_hive)

    modes = list(MODES) if args.mode == "all" else [args.mode]
    for mode in modes:
        print(f"\n{'=' * 70}\n[sql] mode: {mode}\n{'=' * 70}")
        MODES[mode](spark, src)

    spark.stop()


if __name__ == "__main__":
    main()
