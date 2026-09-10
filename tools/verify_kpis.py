#!/usr/bin/env python3
"""Checks every published FinSight figure against the specification.

The spec states a lot of concrete numbers -- 1,554 transactions, 158 fraud,
$312M, 134 dormant accounts, 10,000 customers averaging 2.6 products. This
asserts all of them against the actual artefacts, so a regression anywhere in
the generator or the aggregation logic fails loudly instead of quietly shifting
a dashboard tile.

    python tools/verify_kpis.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
EXPORTS = ROOT / "exports"

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
SKIP = "\033[33mSKIP\033[0m"


class Results:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def check(self, label: str, actual, expected, tol: float = 0.0, spec: str = "") -> None:
        if actual is None:
            print(f"  {SKIP}  {label:<40} (artefact missing)")
            self.skipped += 1
            return
        ok = (abs(float(actual) - float(expected)) <= tol
              if isinstance(expected, (int, float)) and not isinstance(expected, bool)
              else actual == expected)
        tag = PASS if ok else FAIL
        self.passed += ok
        self.failed += not ok
        shown = f"{actual:,.2f}" if isinstance(actual, float) else f"{actual:,}" \
            if isinstance(actual, int) else str(actual)
        want = f"{expected:,.2f}" if isinstance(expected, float) else f"{expected:,}" \
            if isinstance(expected, int) else str(expected)
        suffix = f"   [{spec}]" if spec else ""
        print(f"  {tag}  {label:<40} {shown:>16}  expected {want}{suffix}")

    def at_least(self, label: str, actual, minimum, spec: str = "") -> None:
        """For requirements the spec states as a floor rather than an exact value."""
        if actual is None:
            print(f"  {SKIP}  {label:<40} (artefact missing)")
            self.skipped += 1
            return
        ok = actual >= minimum
        self.passed += ok
        self.failed += not ok
        suffix = f"   [{spec}]" if spec else ""
        print(f"  {PASS if ok else FAIL}  {label:<40} {actual:>16,}  "
              f"expected >= {minimum:,}{suffix}")


def read(path: Path) -> pd.DataFrame | None:
    return pd.read_csv(path) if path.exists() else None


def main() -> int:
    r = Results()

    print("\nFinSight KPI verification")
    print("=" * 78)

    # ---- transactions ------------------------------------------------------
    print("\nTransaction dataset (spec 4.1, 4.3)")
    txns = read(RAW / "demo" / "transactions.csv")
    if txns is None:
        print(f"  {SKIP}  data/raw/demo/transactions.csv missing")
        r.skipped += 1
    else:
        accounts = pd.unique(pd.concat([txns["nameOrig"], txns["nameDest"]]))
        r.check("transaction count", len(txns), 1554, spec="4.3")
        r.check("distinct accounts", len(accounts), 499, spec="4.3")
        r.check("confirmed fraud", int(txns["isFraud"].sum()), 158, spec="page 3")
        r.check("total volume (USD)", float(txns["amount"].sum()), 312_000_000.00,
                tol=0.01, spec="page 3")
        r.check("fraud rate %", 100.0 * txns["isFraud"].sum() / len(txns), 10.2,
                tol=0.05, spec="page 3")
        r.check("max step", int(txns["step"].max()), 168, spec="9.2")

    # ---- graph -------------------------------------------------------------
    print("\nNeo4j graph (spec 4.3)")
    nodes = read(RAW / "neo4j" / "neo4j_accounts_nodes.csv")
    tnodes = read(RAW / "neo4j" / "neo4j_transaction_nodes.csv")
    sent = read(RAW / "neo4j" / "neo4j_sent_rels.csv")
    recv = read(RAW / "neo4j" / "neo4j_received_by_rels.csv")
    r.check("Account nodes", len(nodes) if nodes is not None else None, 499, spec="4.3")
    r.check("Transaction nodes", len(tnodes) if tnodes is not None else None, 1554, spec="4.3")
    if sent is not None and recv is not None:
        r.check("total relationship edges", len(sent) + len(recv), 3108, spec="4.3")
        # Each Transaction node has exactly one sender, so distinct inbound
        # senders per account is counted by walking the full
        # (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account) path.
        path = sent.merge(recv, left_on=":END_ID", right_on=":START_ID",
                          suffixes=("_s", "_r"))
        mules = path.groupby(":END_ID_r")[":START_ID_s"].nunique()
        r.at_least("accounts with >3 inbound senders", int((mules > 3).sum()), 5,
                   spec="8.4")

    # ---- customers ---------------------------------------------------------
    print("\nCustomer collection (spec 4.2, page 2)")
    cust_path = RAW / "novacrest_customers.json"
    if not cust_path.exists():
        print(f"  {SKIP}  novacrest_customers.json missing")
        r.skipped += 1
    else:
        docs = [json.loads(line) for line in cust_path.read_text().splitlines() if line]
        cust = pd.DataFrame(docs)
        r.check("customer documents", len(cust), 10_000, spec="4.2")
        r.check("distinct segments", cust["segment"].nunique(), 5, spec="8.3 R2")
        r.check("KYC verified (active)", int((cust["kyc_status"] == "verified").sum()),
                9_240, spec="page 2")
        r.check("avg risk score", float(cust["risk_score"].mean()), 0.28,
                tol=0.005, spec="page 2")
        r.check("avg churn probability", float(cust["churn_probability"].mean()), 0.224,
                tol=0.005, spec="page 2")
        r.check("avg products held", float(cust["products"].apply(len).mean()), 2.6,
                tol=0.05, spec="page 2")
        for segment, expected in [("Standard", 4000), ("Basic", 3000), ("Premium", 1800),
                                  ("Student", 900), ("Private Banking", 300)]:
            r.check(f"segment: {segment}", int((cust["segment"] == segment).sum()),
                    expected, spec="8.3 R2")

    # ---- dormancy ----------------------------------------------------------
    print("\nDormancy report (spec 7.6, page 3)")
    dorm = read(EXPORTS / "dormancy_report.csv")
    if dorm is None:
        print(f"  {SKIP}  exports/dormancy_report.csv missing "
              f"(run 'make sql-dormancy' or 'make offline')")
        r.skipped += 1
    else:
        severe = int((dorm["dormancy_severity"] == "Severely Dormant").sum())
        r.check("dormant accounts", len(dorm), 134, spec="page 3")
        r.check("severely dormant", severe, 27, spec="7.6 R1")
        r.check("min transaction history", int(dorm["txn_count"].min()), 5,
                spec="7.6 (>=5)")
        r.check("all customer accounts", bool(dorm["customerId"].str.startswith("C").all()),
                True, spec="7.6")

    # ---- compliance --------------------------------------------------------
    print("\nCompliance summary (spec 7.5, page 3)")
    comp = read(EXPORTS / "compliance_summary.csv")
    if comp is None:
        print(f"  {SKIP}  exports/compliance_summary.csv missing")
        r.skipped += 1
    else:
        r.check("transaction types covered", len(comp), 5, spec="page 3")
        r.check("compliance total volume", float(comp["total_volume"].sum()),
                312_000_000.00, tol=1.0, spec="page 3")
        r.check("compliance fraud count", int(comp["fraud_count"].sum()), 158,
                spec="page 3")

    # ---- streaming rule ----------------------------------------------------
    print("\nStreaming fraud rule (spec 7.1)")
    flagged = read(EXPORTS / "flagged_transactions.csv")
    if flagged is None:
        print(f"  {SKIP}  exports/flagged_transactions.csv missing")
        r.skipped += 1
    else:
        r.check("all flagged are TRANSFER/CASH_OUT",
                bool(flagged["type"].isin(["TRANSFER", "CASH_OUT"]).all()), True, spec="7.1")
        r.check("all flagged exceed $200,000",
                bool((flagged["amount"] > 200_000).all()), True, spec="7.1")
        r.check("all flagged have zero dest balance",
                bool((flagged["newbalanceDest"] == 0).all()), True, spec="7.1")
        fp = int((flagged["isFraud"] == 0).sum())
        print(f"        {len(flagged):,} flagged, {len(flagged) - fp:,} confirmed, "
              f"{fp:,} false positive ({100.0 * fp / len(flagged):.2f}%)")

    # ---- blend -------------------------------------------------------------
    print("\nAlteryx blend outputs (spec 9.1, 9.2)")
    blend = read(EXPORTS / "customer_risk_blend.csv")
    summary = read(EXPORTS / "transaction_summary.csv")
    if blend is None:
        print(f"  {SKIP}  exports/customer_risk_blend.csv missing (run 'make blend')")
        r.skipped += 1
    else:
        r.check("blend row count", len(blend), 10_000, spec="9.1")
        expected_composite = (blend["fraud_rate_pct"] * 0.6
                              + blend["churn_probability"] * 0.4).round(4)
        r.check("composite formula holds",
                bool((blend["composite_risk_score"] - expected_composite).abs().max() < 1e-6),
                True, spec="9.1")
    if summary is None:
        print(f"  {SKIP}  exports/transaction_summary.csv missing")
        r.skipped += 1
    else:
        r.check("summary within steps 1-168",
                bool(summary["step"].between(1, 168).all()), True, spec="9.2")
        r.check("summary fraud count", int(summary["fraud_count"].sum()), 158, spec="9.2")

    # ---- verdict -----------------------------------------------------------
    print("\n" + "=" * 78)
    total = r.passed + r.failed
    print(f"  {r.passed}/{total} checks passed"
          + (f", {r.failed} failed" if r.failed else "")
          + (f", {r.skipped} skipped" if r.skipped else ""))
    print()
    return 1 if r.failed else 0


if __name__ == "__main__":
    sys.exit(main())
