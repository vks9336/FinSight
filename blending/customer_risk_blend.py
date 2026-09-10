#!/usr/bin/env python3
"""Alteryx Workflow 1 -- Customer Risk Blend (spec 9.1).

Reproduces the drag-and-drop workflow tool for tool, so the .yxmd built from
docs/03_alteryx_build_guide.md produces the same output as this script.

    Input A   Spark SQL customer fraud summary   (spec 7.5 R1)
    Input B   MongoDB customer profiles          (spec 8.3)
    Input C   Spark Core CLV scores              (spec 7.4)
    Input D   Spark SQL dormancy report          (spec 7.6)

    Formula   composite_risk_score = (fraud_rate_pct * 0.6) + (churn_probability * 0.4)
    Output    exports/customer_risk_blend.xlsx

Inputs C and D are joins the spec calls for in sections 7.4 and 7.6 but does not
redraw in the section 9.1 tool list; they are left joins so the workflow still
completes if those jobs have not run.

    python blending/customer_risk_blend.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

import sources

# spec 9.1 Formula Tool
W_FRAUD_RATE = 0.6
W_CHURN = 0.4

OUTPUT_COLUMNS = [
    "customerId", "name", "age", "segment", "kyc_status", "preferred_channel",
    "product_count", "products", "account_opened",
    "txn_count", "total_amount", "avg_amount", "fraud_count", "fraud_rate_pct",
    "unique_destinations", "last_active_step",
    "risk_score", "churn_probability", "composite_risk_score", "risk_band",
    "clv_score", "clv_tier", "dormancy_severity", "steps_inactive", "is_dormant",
]


def band(score: pd.Series) -> pd.Series:
    """Bucket the composite score for the Power BI scatter plot legend."""
    return pd.cut(
        score,
        bins=[-float("inf"), 2.0, 6.0, float("inf")],
        labels=["Low", "Medium", "High"],
    ).astype(str)


def build(args) -> pd.DataFrame:
    print("==> Input A: customer fraud summary (Spark SQL)")
    fraud = sources.read_csv("/finsight/exports/customer_fraud_summary.csv",
                             "customer_fraud_summary.csv")
    print(f"    {len(fraud):,} rows")

    print("==> Input B: customer profiles (MongoDB)")
    customers = sources.read_customers()
    print(f"    {len(customers):,} rows")

    print("==> Input C: CLV scores (Spark Core)")
    try:
        clv = sources.read_parquet_dir("/finsight/processed/clv_scores",
                                       "clv_scores.csv")[
            ["customerId", "clv_score", "clv_tier"]
        ]
        print(f"    {len(clv):,} rows")
    except sources.SourceUnavailable as exc:
        print(f"    skipped ({exc})")
        clv = pd.DataFrame(columns=["customerId", "clv_score", "clv_tier"])

    print("==> Input D: dormancy report (Spark SQL)")
    try:
        dormancy = sources.read_csv("/finsight/exports/dormancy_report.csv",
                                    "dormancy_report.csv")[
            ["customerId", "dormancy_severity", "steps_inactive"]
        ]
        print(f"    {len(dormancy):,} rows")
    except sources.SourceUnavailable as exc:
        print(f"    skipped ({exc})")
        dormancy = pd.DataFrame(columns=["customerId", "dormancy_severity", "steps_inactive"])

    # ---- Join Tool: customer profiles are the spine ------------------------
    # Left join, not inner: the profile collection holds 10,000 customers while
    # only the ~400 that originated transactions appear in the fraud summary.
    # An inner join here would silently drop 96% of the portfolio and make the
    # Customer 360 segment counts wrong.
    print("==> Join Tool: profiles LEFT JOIN fraud summary on customerId")
    df = customers.merge(fraud, on="customerId", how="left")
    df = df.merge(clv, on="customerId", how="left")
    df = df.merge(dormancy, on="customerId", how="left")

    # ---- Data Cleansing Tool ----------------------------------------------
    for col, fill in [
        ("txn_count", 0), ("total_amount", 0.0), ("avg_amount", 0.0),
        ("fraud_count", 0), ("fraud_rate_pct", 0.0), ("unique_destinations", 0),
        ("last_active_step", 0), ("clv_score", 0.0),
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(fill)
    df["clv_tier"] = df.get("clv_tier", pd.Series(dtype=str)).fillna("No Activity")
    df["dormancy_severity"] = df["dormancy_severity"].fillna("Active")
    df["steps_inactive"] = pd.to_numeric(df["steps_inactive"], errors="coerce").fillna(0).astype(int)
    df["is_dormant"] = df["dormancy_severity"].ne("Active")

    # ---- Formula Tool (spec 9.1) ------------------------------------------
    # fraud_rate_pct is on a 0-100 scale and churn_probability on 0-1. The spec
    # combines them as written, so the result is intentionally not a 0-1 score;
    # risk_band below is what the dashboard uses for classification.
    print(f"==> Formula Tool: composite_risk_score = "
          f"(fraud_rate_pct * {W_FRAUD_RATE}) + (churn_probability * {W_CHURN})")
    df["composite_risk_score"] = (
        df["fraud_rate_pct"] * W_FRAUD_RATE
        + df["churn_probability"] * W_CHURN
    ).round(4)
    df["risk_band"] = band(df["composite_risk_score"])

    df["product_count"] = df["products"].apply(
        lambda p: len(p) if isinstance(p, (list, tuple)) else 0
    )
    df["products"] = df["products"].apply(
        lambda p: ", ".join(p) if isinstance(p, (list, tuple)) else ""
    )

    # ---- Select Tool -------------------------------------------------------
    present = [c for c in OUTPUT_COLUMNS if c in df.columns]
    return df[present].sort_values("composite_risk_score", ascending=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    default_out = Path(__file__).resolve().parents[1] / "exports" / "customer_risk_blend.xlsx"
    ap.add_argument("--out", type=Path, default=default_out)
    args = ap.parse_args()

    df = build(args)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    # Output Tool: XLSX for Power BI import (spec 9.1). A second sheet carries
    # the segment rollup so the Customer 360 page does not have to re-aggregate.
    segment_rollup = (
        df.groupby("segment")
        .agg(
            customers=("customerId", "count"),
            avg_risk_score=("risk_score", "mean"),
            avg_churn_probability=("churn_probability", "mean"),
            avg_composite_risk=("composite_risk_score", "mean"),
            avg_clv=("clv_score", "mean"),
            total_fraud=("fraud_count", "sum"),
            dormant_accounts=("is_dormant", "sum"),
        )
        .round(4)
        .reset_index()
        .sort_values("customers", ascending=False)
    )

    with pd.ExcelWriter(args.out, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="customer_risk", index=False)
        segment_rollup.to_excel(writer, sheet_name="segment_rollup", index=False)

    # Power BI reads XLSX; the dashboards and any CSV-based tooling read this.
    csv_out = args.out.with_suffix(".csv")
    df.to_csv(csv_out, index=False)

    print(f"\n==> wrote {args.out}  ({len(df):,} rows x {len(df.columns)} cols)")
    print(f"==> wrote {csv_out}")
    print("\n    Segment rollup")
    print(segment_rollup.to_string(index=False))
    print(f"\n    avg composite risk {df['composite_risk_score'].mean():.4f}")
    print(f"    dormant accounts   {int(df['is_dormant'].sum()):,}")


if __name__ == "__main__":
    main()
