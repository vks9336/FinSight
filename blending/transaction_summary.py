#!/usr/bin/env python3
"""Alteryx Workflow 2 -- Transaction Summary (spec 9.2).

    Input      finsight.txn_summary_mart via the Hive connection (spec 8.2 R2)
    Filter     step between 1 and 168 -- the first 7 simulation days
    Aggregate  total volume, average amount and fraud count by type and step
    Output     exports/transaction_summary.csv

Spec 8.2 R2 supersedes the original 9.2 wording: the input is the pre-aggregated
Hive mart rather than the raw Spark Core CSV export, so that Alteryx and Power BI
report from identical metric definitions.

    python blending/transaction_summary.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

import sources

STEP_MIN = 1
STEP_MAX = 168


def load_input() -> tuple[pd.DataFrame, str]:
    """Prefer the Hive mart; fall back to the Spark Core daily summary.

    The mart is one row per customer per step and carries txn_types, which is
    what makes a by-type breakdown possible. The daily summary is already
    grouped by type and step, so it needs no explosion.
    """
    try:
        mart = sources.read_parquet_dir(
            "/user/hive/warehouse/finsight.db/txn_summary_mart"
        )
        print(f"    Hive mart: {len(mart):,} customer-step rows")
        return mart, "mart"
    except sources.SourceUnavailable as exc:
        print(f"    Hive mart unavailable ({exc})")

    summary = sources.read_parquet_dir("/finsight/processed/daily_summary",
                                       "daily_summary.csv")
    print(f"    Spark Core daily summary: {len(summary):,} rows")
    return summary, "daily_summary"


def from_mart(mart: pd.DataFrame) -> pd.DataFrame:
    """Explode the mart's comma-separated txn_types back to one row per type.

    The mart collapses a customer's types within a step into a single string, so
    a by-type aggregate has to unpack it. Amounts are divided evenly across the
    types present, which is the closest attribution the mart's grain supports.
    """
    df = mart.copy()
    df["txn_types"] = df["txn_types"].fillna("").astype(str)
    df["type_list"] = df["txn_types"].str.split(",")
    df["type_count"] = df["type_list"].apply(lambda x: max(1, len([t for t in x if t])))

    exploded = df.explode("type_list").rename(columns={"type_list": "transaction_type"})
    exploded = exploded[exploded["transaction_type"].astype(str).str.len() > 0]
    exploded["transaction_type"] = exploded["transaction_type"].str.strip()

    exploded["attributed_amount"] = exploded["total_amount"] / exploded["type_count"]
    exploded["attributed_txns"] = exploded["txn_count"] / exploded["type_count"]
    exploded["attributed_fraud"] = exploded["fraud_count"] / exploded["type_count"]

    return (
        exploded.groupby(["transaction_type", "step"])
        .agg(
            total_volume=("attributed_amount", "sum"),
            txn_count=("attributed_txns", "sum"),
            avg_amount=("avg_amount", "mean"),
            max_amount=("max_amount", "max"),
            fraud_count=("attributed_fraud", "sum"),
            customers=("customerId", "nunique"),
        )
        .reset_index()
    )


def from_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={"type": "transaction_type", "txn_volume": "txn_count"})
    keep = ["transaction_type", "step", "total_amount", "txn_count", "avg_amount", "fraud_count"]
    out = out[[c for c in keep if c in out.columns]]
    return out.rename(columns={"total_amount": "total_volume"})


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    default_out = Path(__file__).resolve().parents[1] / "exports" / "transaction_summary.csv"
    ap.add_argument("--out", type=Path, default=default_out)
    args = ap.parse_args()

    print("==> Input Tool: Hive txn_summary_mart (spec 8.2 R2)")
    raw, kind = load_input()

    print("==> Aggregate Tool: group by transaction type and step")
    agg = from_mart(raw) if kind == "mart" else from_daily_summary(raw)

    print(f"==> Filter Tool: {STEP_MIN} <= step <= {STEP_MAX}")
    before = len(agg)
    agg = agg[(agg["step"] >= STEP_MIN) & (agg["step"] <= STEP_MAX)]
    print(f"    {before:,} -> {len(agg):,} rows")

    agg["fraud_rate_pct"] = (
        100.0 * agg["fraud_count"] / agg["txn_count"].replace(0, pd.NA)
    ).fillna(0.0)

    for col in ["total_volume", "avg_amount", "fraud_rate_pct"]:
        if col in agg.columns:
            agg[col] = agg[col].astype(float).round(2)
    for col in ["txn_count", "fraud_count"]:
        agg[col] = agg[col].round().astype(int)

    agg = agg.sort_values(["step", "transaction_type"])

    args.out.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(args.out, index=False)
    print(f"\n==> wrote {args.out}  ({len(agg):,} rows)")

    by_type = (
        agg.groupby("transaction_type")
        .agg(
            steps=("step", "nunique"),
            txn_count=("txn_count", "sum"),
            total_volume=("total_volume", "sum"),
            fraud_count=("fraud_count", "sum"),
        )
        .reset_index()
        .sort_values("total_volume", ascending=False)
    )
    by_type["fraud_rate_pct"] = (
        100.0 * by_type["fraud_count"] / by_type["txn_count"]
    ).round(2)

    print("\n    Rollup by transaction type")
    print(by_type.to_string(index=False))
    print(f"\n    total volume ${agg['total_volume'].sum():,.2f} "
          f"| total fraud {int(agg['fraud_count'].sum()):,}")


if __name__ == "__main__":
    main()
