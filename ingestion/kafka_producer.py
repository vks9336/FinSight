#!/usr/bin/env python3
"""Replays the NovaCrest transaction CSV into Kafka (spec 6.2).

Targets ~1,000 messages/second so the stream resembles NovaCrest's real load
without saturating a laptop broker.

Message shape
-------------
Connect's ParquetFormat cannot infer a schema from bare JSON, so by default
each message is wrapped in the Connect envelope:

    {"schema": {...}, "payload": {...}}

That is what makes the HDFS sink able to write typed Parquet. The Spark jobs
read through spark/finsight_common.py, which unwraps the envelope transparently,
so both consumers work off the same topic. Pass --flat to emit bare JSON when
running without Kafka Connect.

Records are keyed by nameOrig so every transaction for one customer lands on the
same partition. The churn job (spec 7.2) aggregates per customer, and this keeps
a customer's history from being split across partitions.
"""

from __future__ import annotations

import argparse
import csv
import json
import signal
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

# Connect schema envelope, mirroring the spec 4.1 attribute table.
VALUE_SCHEMA = {
    "type": "struct",
    "optional": False,
    "name": "finsight.transaction",
    "fields": [
        {"field": "step", "type": "int32", "optional": False},
        {"field": "type", "type": "string", "optional": False},
        {"field": "amount", "type": "double", "optional": False},
        {"field": "nameOrig", "type": "string", "optional": False},
        {"field": "oldbalanceOrg", "type": "double", "optional": False},
        {"field": "newbalanceOrig", "type": "double", "optional": False},
        {"field": "nameDest", "type": "string", "optional": False},
        {"field": "oldbalanceDest", "type": "double", "optional": False},
        {"field": "newbalanceDest", "type": "double", "optional": False},
        {"field": "isFraud", "type": "int32", "optional": False},
        {"field": "isFlaggedFraud", "type": "int32", "optional": False},
        {"field": "ingestedAt", "type": "int64", "optional": False},
    ],
}

_running = True


def _stop(signum, frame):  # noqa: ARG001
    global _running
    _running = False
    print("\n[producer] stop requested, flushing...", file=sys.stderr)


def build_payload(row: dict) -> dict:
    return {
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "nameOrig": row["nameOrig"],
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": row["nameDest"],
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFraud": int(row["isFraud"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"]),
        # Wall-clock ingestion time; the fraud job measures detection latency
        # against this to evidence the spec's 2-second SLA.
        "ingestedAt": int(time.time() * 1000),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    default_csv = Path(__file__).resolve().parents[1] / "data" / "raw" / "demo" / "transactions.csv"
    ap.add_argument("--csv", type=Path, default=default_csv)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="txn-raw")
    ap.add_argument("--rate", type=int, default=1000, help="messages per second (spec 6.2)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N messages (0 = whole file)")
    ap.add_argument("--loop", action="store_true", help="replay the file continuously")
    ap.add_argument("--flat", action="store_true",
                    help="emit bare JSON instead of the Connect schema envelope")
    args = ap.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}\nRun data/generator/generate_data.py first.")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "compression.type": "lz4",
        "acks": "1",
        "queue.buffering.max.messages": 200_000,
    })

    delivered = 0
    failed = 0

    def on_delivery(err, msg):  # noqa: ARG001
        nonlocal delivered, failed
        if err is None:
            delivered += 1
        else:
            failed += 1

    # Pace in short slices rather than sleeping per message; a per-message sleep
    # cannot hold 1,000/s because the syscall overhead dominates at that rate.
    slice_size = max(1, args.rate // 20)
    slice_seconds = slice_size / args.rate

    sent = 0
    started = time.monotonic()
    print(f"[producer] {args.csv} -> {args.bootstrap}/{args.topic} at ~{args.rate} msg/s")
    print(f"[producer] envelope={'off (flat JSON)' if args.flat else 'on (Connect schema)'}")

    try:
        while _running:
            with args.csv.open(newline="") as fh:
                reader = csv.DictReader(fh)
                slice_start = time.monotonic()
                in_slice = 0

                for row in reader:
                    if not _running or (args.limit and sent >= args.limit):
                        break

                    payload = build_payload(row)
                    value = payload if args.flat else {"schema": VALUE_SCHEMA, "payload": payload}

                    while True:
                        try:
                            producer.produce(
                                topic=args.topic,
                                key=payload["nameOrig"].encode(),
                                value=json.dumps(value).encode(),
                                callback=on_delivery,
                            )
                            break
                        except BufferError:
                            producer.poll(0.1)

                    sent += 1
                    in_slice += 1

                    if in_slice >= slice_size:
                        producer.poll(0)
                        elapsed = time.monotonic() - slice_start
                        if elapsed < slice_seconds:
                            time.sleep(slice_seconds - elapsed)
                        slice_start = time.monotonic()
                        in_slice = 0

                        if sent % (args.rate * 5) == 0:
                            rate = sent / (time.monotonic() - started)
                            print(f"[producer] sent={sent:,} delivered={delivered:,} "
                                  f"failed={failed:,} rate={rate:,.0f}/s")

            if not args.loop or (args.limit and sent >= args.limit):
                break
            print("[producer] end of file, looping")
    finally:
        producer.flush(30)
        wall = time.monotonic() - started
        print(f"\n[producer] done: sent={sent:,} delivered={delivered:,} failed={failed:,} "
              f"in {wall:,.1f}s ({sent / max(wall, 1e-9):,.0f} msg/s)")
        if failed:
            sys.exit(1)


if __name__ == "__main__":
    main()
