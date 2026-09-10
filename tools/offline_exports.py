#!/usr/bin/env python3
"""Reproduces the Spark job outputs locally with pandas.

Two purposes:

  1. The spec's Solo Completion Strategy names a fallback -- "if Day 1
     Kafka-to-HDFS setup runs over time, switch to direct file load". This is
     that path: it takes transactions.csv straight to the export files the
     blending and dashboard layers consume, with no cluster involved.

  2. It lets the downstream layers be verified before the cluster is up.

The aggregate definitions mirror spark/spark_sql_jobs.py and spark/batch_clv.py
exactly. If you change a definition there, change it here too -- these are the
two places the same metric is expressed.

    python tools/offline_exports.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

COMPLIANCE_WINDOW_STEPS = 168
DORMANCY_INACTIVITY_STEPS = 72
DORMANCY_SEVERE_STEPS = 120
DORMANCY_MIN_HISTORY = 5

CLV_W = {"volume": 0.30, "frequency": 0.25, "diversity": 0.25, "recency": 0.20}
CLV_RECENCY_CUTOFF = 48
ALL_TXN_TYPES = 5


def compliance_summary(df: pd.DataFrame) -> pd.DataFrame:
    max_step = int(df["step"].max())
    start = max(0, max_step - COMPLIANCE_WINDOW_STEPS)
    win = df[(df["step"] > start) & (df["step"] <= max_step)]

    out = (
        win.groupby("type")
        .agg(
            txn_count=("amount", "size"),
            total_volume=("amount", "sum"),
            avg_amount=("amount", "mean"),
            max_amount=("amount", "max"),
            fraud_count=("isFraud", "sum"),
            system_flagged_count=("isFlaggedFraud", "sum"),
        )
        .reset_index()
        .rename(columns={"type": "transaction_type"})
    )
    fraud_vol = win[win["isFraud"] == 1].groupby("type")["amount"].sum()
    out["fraud_volume"] = out["transaction_type"].map(fraud_vol).fillna(0.0)
    out["fraud_rate_pct"] = 100.0 * out["fraud_count"] / out["txn_count"]
    out["window_start_step"] = start
    out["window_end_step"] = max_step
    out["risk_classification"] = pd.cut(
        out["fraud_rate_pct"], bins=[-1, 2, 10, 1e9], labels=["LOW", "MEDIUM", "HIGH"]
    ).astype(str)
    for c in ["total_volume", "avg_amount", "max_amount", "fraud_volume"]:
        out[c] = out[c].round(2)
    out["fraud_rate_pct"] = out["fraud_rate_pct"].round(4)
    return out.sort_values("total_volume", ascending=False)


def customer_fraud_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("nameOrig")
        .agg(
            txn_count=("amount", "size"),
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
            fraud_count=("isFraud", "sum"),
            unique_destinations=("nameDest", "nunique"),
            last_active_step=("step", "max"),
        )
        .reset_index()
        .rename(columns={"nameOrig": "customerId"})
    )
    out["fraud_rate_pct"] = (100.0 * out["fraud_count"] / out["txn_count"]).round(4)
    out["total_amount"] = out["total_amount"].round(2)
    out["avg_amount"] = out["avg_amount"].round(2)
    return out.sort_values(["fraud_count", "total_amount"], ascending=False)


def dormancy_report(df: pd.DataFrame) -> pd.DataFrame:
    max_step = int(df["step"].max())
    cust = df[df["nameOrig"].str.startswith("C")]

    acct = (
        cust.groupby("nameOrig")
        .agg(
            last_active_step=("step", "max"),
            first_active_step=("step", "min"),
            txn_count=("amount", "size"),
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
            fraud_count=("isFraud", "sum"),
        )
        .reset_index()
        .rename(columns={"nameOrig": "customerId"})
    )
    acct["dataset_max_step"] = max_step
    acct["steps_inactive"] = max_step - acct["last_active_step"]

    out = acct[
        (acct["steps_inactive"] > DORMANCY_INACTIVITY_STEPS)
        & (acct["txn_count"] >= DORMANCY_MIN_HISTORY)
    ].copy()
    out["dormancy_severity"] = out["steps_inactive"].gt(DORMANCY_SEVERE_STEPS).map(
        {True: "Severely Dormant", False: "Dormant"}
    )
    out["total_amount"] = out["total_amount"].round(2)
    out["avg_amount"] = out["avg_amount"].round(2)
    return out.sort_values("steps_inactive", ascending=False)


def daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby(["type", "step"])
        .agg(
            txn_volume=("amount", "size"),
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
            fraud_count=("isFraud", "sum"),
        )
        .reset_index()
    )
    out["fraud_rate_pct"] = (100.0 * out["fraud_count"] / out["txn_volume"]).round(4)
    out["total_amount"] = out["total_amount"].round(2)
    out["avg_amount"] = out["avg_amount"].round(2)
    return out.sort_values(["step", "type"])


def clv_scores(df: pd.DataFrame) -> pd.DataFrame:
    max_step = int(df["step"].max())
    per = (
        df.groupby("nameOrig")
        .agg(
            totalAmount=("amount", "sum"),
            txnCount=("amount", "size"),
            distinctTypes=("type", "nunique"),
            lastActiveStep=("step", "max"),
        )
        .reset_index()
        .rename(columns={"nameOrig": "customerId"})
    )
    per["stepsSinceLastTxn"] = max_step - per["lastActiveStep"]
    per["volumeScore"] = per["totalAmount"] / per["totalAmount"].max()
    per["frequencyScore"] = per["txnCount"] / per["txnCount"].max()
    per["diversityScore"] = per["distinctTypes"] / ALL_TXN_TYPES
    per["recencyScore"] = (
        (CLV_RECENCY_CUTOFF - per["stepsSinceLastTxn"]) / CLV_RECENCY_CUTOFF
    ).where(per["stepsSinceLastTxn"] <= CLV_RECENCY_CUTOFF, 0.0)

    per["clv_score"] = (
        per["volumeScore"] * CLV_W["volume"]
        + per["frequencyScore"] * CLV_W["frequency"]
        + per["diversityScore"] * CLV_W["diversity"]
        + per["recencyScore"] * CLV_W["recency"]
    ).round(4)
    per["clv_tier"] = pd.cut(
        per["clv_score"],
        bins=[-1, 0.40, 0.70, 2],
        labels=["At Risk", "Growth Potential", "High Value"],
        right=False,
    ).astype(str)
    return per


def flagged_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """The spec 7.1 streaming rule, applied in batch."""
    mask = (
        df["type"].isin(["TRANSFER", "CASH_OUT"])
        & (df["amount"] > 200_000)
        & (df["newbalanceDest"] == 0)
    )
    out = df[mask].copy()
    out["fraudReason"] = "type=" + out["type"] + " + amount>200,000 + newbalanceDest=0"
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path, default=ROOT / "data" / "raw" / "demo" / "transactions.csv")
    ap.add_argument("--out", type=Path, default=ROOT / "exports")
    args = ap.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"{args.csv} not found; run data/generator/generate_data.py first.")

    args.out.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(args.csv)
    print(f"read {len(df):,} transactions from {args.csv}")

    artefacts = {
        "compliance_summary.csv": compliance_summary(df),
        "customer_fraud_summary.csv": customer_fraud_summary(df),
        "dormancy_report.csv": dormancy_report(df),
        "daily_summary.csv": daily_summary(df),
        "clv_scores.csv": clv_scores(df),
        "flagged_transactions.csv": flagged_transactions(df),
    }
    for name, frame in artefacts.items():
        path = args.out / name
        frame.to_csv(path, index=False)
        print(f"  wrote {name:<32} {len(frame):>8,} rows")

    dorm = artefacts["dormancy_report.csv"]
    print(f"\n  dormant {len(dorm):,} "
          f"(severe {int((dorm['dormancy_severity'] == 'Severely Dormant').sum()):,})")
    print(f"  flagged by streaming rule {len(artefacts['flagged_transactions.csv']):,}")
    print(f"  confirmed fraud {int(df['isFraud'].sum()):,} of {len(df):,} "
          f"({100.0 * df['isFraud'].sum() / len(df):.2f}%)")


if __name__ == "__main__":
    main()
