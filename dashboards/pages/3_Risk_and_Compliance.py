"""Page 3 — Risk & Compliance Report (spec 10, page 3).

Audience: Compliance Officers.
Source: Spark SQL compliance aggregation joined to the Hive transaction summary
mart and enriched by the Alteryx Transaction Summary workflow.

Replaces NovaCrest's 18-working-day manual quarterly process with a weekly
dashboard that is always current.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import data_access as da

st.set_page_config(page_title="Risk & Compliance", page_icon="📋", layout="wide")

FRAUD_RATE_TARGET = 8.0   # internal threshold the trend line is judged against

try:
    compliance = da.compliance()
    summary = da.transaction_summary()
    dormancy = da.dormancy()
except da.MissingExport as exc:
    da.missing_export_banner(exc)

da.page_header(
    "Risk & Compliance Report",
    "Compliance Officers",
    "Spark SQL → Hive → Alteryx Transaction Summary",
    badge="✔ AUTO-GENERATED",
)

# --------------------------------------------------------------------------
# Transaction type filter (spec 10, page 3: drill into specific categories)
# --------------------------------------------------------------------------

all_types = sorted(compliance["transaction_type"].unique())
selected = st.multiselect(
    "Transaction type filter", all_types, default=all_types,
    help="Compliance officers drill into specific categories during regulatory review.",
)
if not selected:
    st.warning("Select at least one transaction type.")
    st.stop()

comp = compliance[compliance["transaction_type"].isin(selected)]
summ = summary[summary["transaction_type"].isin(selected)]

# --------------------------------------------------------------------------
# KPI row
# --------------------------------------------------------------------------

total_txns = int(comp["txn_count"].sum())
total_volume = float(comp["total_volume"].sum())
total_fraud = int(comp["fraud_count"].sum())
fraud_rate = 100.0 * total_fraud / max(total_txns, 1)

severe = int((dormancy["dormancy_severity"] == "Severely Dormant").sum())
dormant_total = len(dormancy)

step_lo = int(comp["window_start_step"].min()) if "window_start_step" in comp else 0
step_hi = int(comp["window_end_step"].max()) if "window_end_step" in comp else 168

k1, k2, k3, k4 = st.columns(4)
k1.metric("TOTAL TRANSACTIONS", f"{total_txns:,}",
          f"Steps {step_lo + 1}–{step_hi} (7-day)", delta_color="off")
k2.metric("TOTAL VOLUME", f"${total_volume / 1e6:,.0f}M",
          "All types" if len(selected) == len(all_types) else f"{len(selected)} type(s)",
          delta_color="off")
k3.metric("CONFIRMED FRAUD", f"{total_fraud:,}",
          f"Fraud rate: {fraud_rate:.1f}%", delta_color="off")
k4.metric("DORMANT ACCOUNTS", f"{dormant_total:,}",
          f"{severe:,} severely dormant", delta_color="off")

st.divider()

# --------------------------------------------------------------------------
# Fraud rate trend vs target
# --------------------------------------------------------------------------

left, right = st.columns([3, 2])

with left:
    st.markdown("**Weekly Fraud Rate Trend vs Target**")
    st.caption(f"Target threshold {FRAUD_RATE_TARGET:.0f}%. Lets the team spot "
               "deterioration before it becomes a reportable breach.")

    trend = (
        summ.groupby("step")
        .agg(txn_count=("txn_count", "sum"), fraud_count=("fraud_count", "sum"))
        .reset_index()
    )
    trend["fraud_rate_pct"] = 100.0 * trend["fraud_count"] / trend["txn_count"].clip(lower=1)
    # Per-step rates on a 1,554-row slice are very noisy, so a rolling mean is
    # shown alongside; the raw series stays visible rather than being replaced.
    trend["rolling"] = trend["fraud_rate_pct"].rolling(12, min_periods=1).mean()

    fig = go.Figure()
    fig.add_scatter(x=trend["step"], y=trend["fraud_rate_pct"], mode="lines",
                    name="Fraud rate", line=dict(color=da.PALETTE["primary"], width=1),
                    opacity=0.45)
    fig.add_scatter(x=trend["step"], y=trend["rolling"], mode="lines",
                    name="12-step rolling mean",
                    line=dict(color=da.PALETTE["danger"], width=2.5))
    fig.add_hline(y=FRAUD_RATE_TARGET, line_dash="dash",
                  line_color=da.PALETTE["success"],
                  annotation_text=f"Target {FRAUD_RATE_TARGET:.0f}%",
                  annotation_position="top left")
    fig.update_layout(height=380, margin=dict(t=10, b=10, l=10, r=10),
                      xaxis_title="Step (1 step = 1 hour)",
                      yaxis_title="Fraud rate (%)",
                      legend=dict(orientation="h", y=1.12, x=0))
    st.plotly_chart(fig, width="stretch")

with right:
    st.markdown("**Transaction Volume by Type**")
    st.caption("Required for BSA large currency transaction reporting.")
    vol = comp.sort_values("total_volume", ascending=True)
    fig = px.bar(
        vol, x="total_volume", y="transaction_type", orientation="h",
        color="fraud_rate_pct", color_continuous_scale="RdYlGn_r",
        text=vol["total_volume"].map(lambda v: f"${v / 1e6:,.1f}M"),
        labels={"total_volume": "Volume (USD)", "transaction_type": "",
                "fraud_rate_pct": "Fraud %"},
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(height=380, margin=dict(t=10, b=10, l=10, r=60))
    st.plotly_chart(fig, width="stretch")

st.divider()

# --------------------------------------------------------------------------
# Dormancy breakdown (spec 7.6 R1)
# --------------------------------------------------------------------------

left, right = st.columns([2, 3])

with left:
    st.markdown("**Account Dormancy Breakdown**")
    st.caption("Severely dormant accounts (over 120 steps inactive) require escalation.")
    tiers = dormancy["dormancy_severity"].value_counts().reset_index()
    tiers.columns = ["severity", "accounts"]
    fig = px.bar(
        tiers, x="severity", y="accounts", text="accounts",
        color="severity",
        color_discrete_map={"Dormant": da.PALETTE["warning"],
                            "Severely Dormant": da.PALETTE["danger"]},
        labels={"severity": "", "accounts": "Accounts"},
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(height=330, margin=dict(t=10, b=10, l=10, r=10),
                      showlegend=False)
    st.plotly_chart(fig, width="stretch")

with right:
    st.markdown("**Longest-dormant accounts**")
    st.caption("Ordered by steps of inactivity; the escalation worklist.")
    cols = [c for c in ["customerId", "dormancy_severity", "steps_inactive",
                        "last_active_step", "txn_count", "total_amount"]
            if c in dormancy.columns]
    st.dataframe(
        dormancy.nlargest(25, "steps_inactive")[cols],
        width="stretch", hide_index=True, height=330,
        column_config={
            "total_amount": st.column_config.NumberColumn("Total amount", format="$%.2f"),
            "steps_inactive": st.column_config.NumberColumn("Steps inactive"),
        },
    )

st.divider()

# --------------------------------------------------------------------------
# Audit table -- the artefact submitted to regulators
# --------------------------------------------------------------------------

st.markdown("**Compliance Summary by Transaction Type**")
st.caption("This table is the primary audit artefact submitted to regulators.")

audit_cols = [c for c in
              ["transaction_type", "txn_count", "total_volume", "avg_amount",
               "max_amount", "fraud_count", "fraud_volume", "fraud_rate_pct",
               "system_flagged_count", "risk_classification"]
              if c in comp.columns]

st.dataframe(
    comp[audit_cols].sort_values("total_volume", ascending=False),
    width="stretch", hide_index=True,
    column_config={
        "transaction_type": "Type",
        "txn_count": st.column_config.NumberColumn("Count", format="%d"),
        "total_volume": st.column_config.NumberColumn("Volume", format="$%.2f"),
        "avg_amount": st.column_config.NumberColumn("Avg", format="$%.2f"),
        "max_amount": st.column_config.NumberColumn("Max", format="$%.2f"),
        "fraud_count": st.column_config.NumberColumn("Fraud", format="%d"),
        "fraud_volume": st.column_config.NumberColumn("Fraud volume", format="$%.2f"),
        "fraud_rate_pct": st.column_config.NumberColumn("Fraud rate", format="%.2f%%"),
        "system_flagged_count": st.column_config.NumberColumn("System flagged", format="%d"),
        "risk_classification": "Risk",
    },
)

st.caption(
    f"Window: steps {step_lo + 1}–{step_hi}  ·  "
    f"{total_txns:,} transactions  ·  ${total_volume:,.2f}  ·  "
    f"{total_fraud:,} confirmed fraud ({fraud_rate:.2f}%)  ·  "
    f"{dormant_total:,} dormant accounts ({severe:,} severe)"
)
