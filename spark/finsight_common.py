"""Shared plumbing for every FinSight Spark job.

Centralises three things that would otherwise be copy-pasted across six jobs:
the HDFS path layout, the transaction schema, and Kafka envelope handling.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

# --------------------------------------------------------------------------
# Cluster coordinates -- overridable so jobs can run against a local Spark too
# --------------------------------------------------------------------------

HDFS = os.environ.get("FINSIGHT_HDFS", "hdfs://namenode:8020")
KAFKA_BOOTSTRAP = os.environ.get("FINSIGHT_KAFKA", "kafka:29092")

TOPIC_RAW = "txn-raw"
TOPIC_FLAGGED = "txn-flagged"
TOPIC_CHURN = "txn-churn"

# Paths are exactly as named in the specification (sections 6.3, 7.1-7.6).
RAW_TRANSACTIONS = f"{HDFS}/finsight/raw/transactions"
P_STREAMING_METRICS = f"{HDFS}/finsight/processed/streaming_metrics"
P_CHURN_ALERTS = f"{HDFS}/finsight/processed/churn_alerts"
P_RISK_SCORES = f"{HDFS}/finsight/processed/risk_scores"
P_DAILY_SUMMARY = f"{HDFS}/finsight/processed/daily_summary"
P_CLV_SCORES = f"{HDFS}/finsight/processed/clv_scores"
P_CUSTOMER_FRAUD = f"{HDFS}/finsight/processed/customer_fraud_summary"
P_COMPLIANCE = f"{HDFS}/finsight/processed/compliance_summary"
P_DORMANCY = f"{HDFS}/finsight/processed/dormancy_report"
P_FLAGGED = f"{HDFS}/finsight/processed/flagged"
EXPORTS = f"{HDFS}/finsight/exports"

CK_FRAUD = f"{HDFS}/finsight/checkpoints/fraud"
CK_CHURN = f"{HDFS}/finsight/checkpoints/churn"
CK_SINK = f"{HDFS}/finsight/checkpoints/hdfs_sink"

HIVE_DB = "finsight"

# --------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------

TXN_FIELDS = [
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("isFraud", IntegerType()),
    StructField("isFlaggedFraud", IntegerType()),
    StructField("ingestedAt", LongType()),
]

TXN_SCHEMA = StructType(TXN_FIELDS)

# The producer emits the Connect envelope {"schema":..., "payload":...} because
# the HDFS sink's ParquetFormat needs a typed record. Parsing against a schema
# that carries both the envelope and the flat fields lets one code path read
# either shape: whichever half is absent simply parses as null.
ENVELOPE_SCHEMA = StructType(TXN_FIELDS + [StructField("payload", TXN_SCHEMA)])

TXN_COLUMN_NAMES = [f.name for f in TXN_FIELDS]


def build_session(app_name: str, **conf) -> SparkSession:
    """A SparkSession with a distinct appName.

    Spec 7.4 R2 requires the batch jobs to be separately identifiable in the
    Spark UI, so every job passes its own name rather than sharing one.
    """
    builder = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k.replace("__", "."), v)
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, topic: str, starting: str = "latest"):
    """Structured Streaming source for `topic`, flattened to transaction columns."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        .load()
    )
    return _flatten(raw)


def read_kafka_batch(spark: SparkSession, topic: str):
    """Batch (non-streaming) read of a whole topic, used by the dashboards."""
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    return _flatten(raw)


def _flatten(raw):
    parsed = raw.select(
        F.col("timestamp").alias("kafkaTimestamp"),
        F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("j"),
    )
    cols = [
        F.coalesce(F.col(f"j.payload.{name}"), F.col(f"j.{name}")).alias(name)
        for name in TXN_COLUMN_NAMES
    ]
    return parsed.select(*cols, F.col("kafkaTimestamp")).filter(F.col("nameOrig").isNotNull())


def to_kafka_value(df, *columns):
    """Shape a DataFrame into Kafka's expected key/value pair."""
    return df.select(
        F.col(columns[0]).cast("string").alias("key"),
        F.to_json(F.struct(*[F.col(c) for c in columns])).alias("value"),
    )


def read_transactions(spark, path: str | None = None):
    """Read the HDFS Parquet lake, tolerating either landing layout.

    Kafka Connect writes step=N/ directories; the Spark sink writes the same.
    Either way `step` comes back as a partition column, so it is cast to int to
    match the schema the batch jobs expect.
    """
    df = spark.read.parquet(path or RAW_TRANSACTIONS)
    if "step" in df.columns:
        df = df.withColumn("step", F.col("step").cast("int"))
    return df
