"""Page 1 — Fraud Alert Board (spec 10, page 1).

Audience: Fraud & Risk Team.
Source: txn-flagged Kafka topic plus the Alteryx output.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import data_access as da

st.set_page_config(page_title="Fraud Alert Board", page_icon="🚨", layout="wide")

try:
    flagged = da.flagged()
    txns = da.transactions()
except da.MissingExport as exc:
    da.missing_export_banner(exc)

live_df = da.live_topic(da.TOPIC_FLAGGED)
is_live = not live_df.empty

da.page_header(
    "Fraud Alert Board",
    "Fraud & Risk Team",
    "txn-flagged Kafka topic + Alteryx output",
    badge="● LIVE FEED" if is_live else "FILE MODE",
)

# --------------------------------------------------------------------------
# KPI row
# --------------------------------------------------------------------------

latest_day = int(flagged["day"].max())
prev_day = latest_day - 1

today = flagged[flagged["day"] == latest_day]
yesterday = flagged[flagged["day"] == prev_day]

# KPI 1 -- flagged today, with day-on-day delta
total_flagged_today = len(today)
dod = (
    100.0 * (len(today) - len(yesterday)) / len(yesterday)
    if len(yesterday) else 0.0
)

# KPI 2 -- fraud rate as a share of all transactions, week on week.
# The dataset covers exactly one 7-day window, so "this week" is the final 3.5
# days and the comparison half is the first: a within-dataset split, not a
# fabricated prior week.
midpoint = txns["step"].max() / 2
this_half = txns[txns["step"] > midpoint]
prev_half = txns[txns["step"] <= midpoint]
rate_now = 100.0 * this_half["isFraud"].sum() / max(len(this_half), 1)
rate_prev = 100.0 * prev_half["isFraud"].sum() / max(len(prev_half), 1)

overall_rate = 100.0 * txns["isFraud"].sum() / len(txns)

# KPI 3 -- monetary value flagged today vs the daily average
value_today = today["amount"].sum()
daily_avg_value = flagged.groupby("day")["amount"].sum().mean()

# KPI 4 -- false positive rate against the legacy 62% baseline
false_positives = int(flagged["is_false_positive"].sum())
fp_rate = 100.0 * false_positives / max(len(flagged), 1)

k1, k2, k3, k4 = st.columns(4)
k1.metric("TOTAL FLAGGED", f"{total_flagged_today:,}",
          f"{dod:+.0f}% vs yday", help=f"Day {latest_day} of {latest_day}")
k2.metric("FRAUD RATE", f"{overall_rate:.1f}%",
          f"{rate_now - rate_prev:+.1f}pp this week", delta_color="inverse")
k3.metric("TOTAL VALUE", f"${value_today / 1e6:,.1f}M",
          f"${(value_today - daily_avg_value) / 1e6:+,.1f}M vs avg")
k4.metric("FALSE POSITIVE", f"{fp_rate:.1f}%",
          f"{fp_rate - da.LEGACY_FALSE_POSITIVE_RATE:+.1f}pp vs legacy",
          delta_color="inverse",
          help=f"Legacy manual review baseline was "
               f"{da.LEGACY_FALSE_POSITIVE_RATE:.0f}%")

st.divider()

# --------------------------------------------------------------------------
# Charts
# --------------------------------------------------------------------------

left, right = st.columns([3, 2])

with left:
    st.markdown("**Daily Fraud Volume Trend** — flagged transactions per step")
    by_step = (
        flagged.groupby(["step", "type"])
        .agg(flagged_count=("amount", "size"), value=("amount", "sum"))
        .reset_index()
    )
    fig = px.line(
        by_step, x="step", y="flagged_count", color="type",
        markers=True,
        color_discrete_sequence=da.SEQUENCE,
        labels={"step": "Step (1 step = 1 hour)", "flagged_count": "Flagged", "type": ""},
    )
    fig.update_layout(height=340, margin=dict(t=10, b=10, l=10, r=10),
                      legend=dict(orientation="h", y=1.12, x=0))
    st.plotly_chart(fig, width="stretch")

with right:
    st.markdown("**Fraud Count by Type** — TRANSFER vs CASH_OUT")
    by_type = (
        flagged.groupby("type")
        .agg(flagged=("amount", "size"),
             confirmed=("isFraud", "sum"),
             value=("amount", "sum"))
        .reset_index()
        .sort_values("flagged", ascending=False)
    )
    fig = go.Figure()
    fig.add_bar(x=by_type["type"], y=by_type["flagged"], name="Flagged",
                marker_color=da.PALETTE["primary"])
    fig.add_bar(x=by_type["type"], y=by_type["confirmed"], name="Confirmed fraud",
                marker_color=da.PALETTE["danger"])
    fig.update_layout(height=340, barmode="group",
                      margin=dict(t=10, b=10, l=10, r=10),
                      legend=dict(orientation="h", y=1.12, x=0),
                      yaxis_title="Transactions")
    st.plotly_chart(fig, width="stretch")

st.divider()

# --------------------------------------------------------------------------
# Live feed
# --------------------------------------------------------------------------

st.markdown("**Live Flagged Transaction Feed**")

if is_live:
    st.caption(f"{len(live_df):,} records read from the `{da.TOPIC_FLAGGED}` topic.")
    feed = live_df.copy()
    if "detectionLatencyMs" in feed.columns:
        median_latency = pd.to_numeric(feed["detectionLatencyMs"], errors="coerce").median()
        if pd.notna(median_latency):
            within_sla = median_latency <= 2000
            st.caption(
                f"Median detection latency **{median_latency:,.0f} ms** — "
                f"{'within' if within_sla else 'outside'} the 2-second SLA."
            )
    feed = feed.sort_values("flaggedAt", ascending=False) if "flaggedAt" in feed else feed
else:
    st.caption(
        f"Kafka unavailable at `{da.KAFKA_BOOTSTRAP}`; showing the batch-equivalent "
        "output of the same rule from `exports/flagged_transactions.csv`."
    )
    feed = flagged.sort_values(["step", "amount"], ascending=[False, False])

display_cols = [c for c in
                ["step", "type", "amount", "nameOrig", "nameDest",
                 "newbalanceDest", "isFraud", "fraudReason", "detectionLatencyMs"]
                if c in feed.columns]

status = st.radio("Confirmation status", ["All", "Confirmed fraud", "False positive"],
                  horizontal=True, label_visibility="collapsed")
view = feed
if "isFraud" in feed.columns:
    if status == "Confirmed fraud":
        view = feed[feed["isFraud"] == 1]
    elif status == "False positive":
        view = feed[feed["isFraud"] == 0]

st.dataframe(
    view[display_cols].head(250),
    width="stretch", hide_index=True, height=380,
    column_config={
        "amount": st.column_config.NumberColumn("Amount", format="$%.2f"),
        "newbalanceDest": st.column_config.NumberColumn("Dest balance after", format="$%.2f"),
        "isFraud": st.column_config.CheckboxColumn("Confirmed"),
        "detectionLatencyMs": st.column_config.NumberColumn("Latency (ms)", format="%d"),
    },
)

st.caption(
    f"Rule (spec 7.1): type IN (TRANSFER, CASH_OUT) AND amount > $200,000 "
    f"AND newbalanceDest = 0  ·  {len(flagged):,} flagged, "
    f"{len(flagged) - false_positives:,} confirmed, {false_positives:,} false positive"
)
