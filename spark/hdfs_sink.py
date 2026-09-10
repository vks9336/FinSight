#!/usr/bin/env python3
"""Lands txn-raw into HDFS as step-partitioned Parquet (spec 6.3).

This is the Apache-licensed alternative to Confluent's HDFS 3 Sink Connector,
which is proprietary and time-limited. The output layout is byte-compatible --
/finsight/raw/transactions/step=<N>/*.parquet -- so the Hive external table and
every downstream job are indifferent to which sink produced the data.

    spark-submit /opt/finsight/spark/hdfs_sink.py --starting earliest
"""

from __future__ import annotations

import argparse

from finsight_common import (
    CK_SINK, RAW_TRANSACTIONS, TOPIC_RAW, build_session, read_kafka_stream,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--starting", default="earliest", choices=["earliest", "latest"])
    ap.add_argument("--trigger", default="30 seconds")
    ap.add_argument("--once", action="store_true", help="drain the topic then exit")
    args = ap.parse_args()

    spark = build_session("FinSight-HDFS-Sink")
    stream = read_kafka_stream(spark, TOPIC_RAW, starting=args.starting)

    writer = (
        stream.drop("kafkaTimestamp")
        .writeStream.format("parquet")
        .option("path", RAW_TRANSACTIONS)
        .option("checkpointLocation", CK_SINK)
        .partitionBy("step")
        .outputMode("append")
    )

    query = writer.trigger(availableNow=True) if args.once \
        else writer.trigger(processingTime=args.trigger)

    print(f"[hdfs-sink] {TOPIC_RAW} -> {RAW_TRANSACTIONS} (partitioned by step)")
    query.start().awaitTermination()


if __name__ == "__main__":
    main()
