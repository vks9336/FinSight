"""Page 2 — Customer 360 (spec 10, page 2).

Audience: Relationship Managers.
Source: Alteryx Customer Risk Blend, which joins the Hive compliance table to
the MongoDB profiles and appends the Spark Core CLV score.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import plotly.express as px
import streamlit as st

import data_access as da

st.set_page_config(page_title="Customer 360", page_icon="👥", layout="wide")

try:
    df = da.customer_blend()
except da.MissingExport as exc:
    da.missing_export_banner(exc)

da.page_header(
    "Customer 360 (Portfolio View)",
    "Relationship Managers",
    "Alteryx Customer Risk Blend",
    badge=f"{df['segment'].nunique()} SEGMENTS",
)

# --------------------------------------------------------------------------
# Filters
# --------------------------------------------------------------------------

with st.expander("Filters", expanded=False):
    c1, c2, c3 = st.columns(3)
    segments = c1.multiselect("Segment", da.SEGMENT_ORDER,
                              default=da.SEGMENT_ORDER)
    channels = c2.multiselect("Preferred channel",
                              sorted(df["preferred_channel"].dropna().unique()),
                              default=sorted(df["preferred_channel"].dropna().unique()))
    kyc = c3.multiselect("KYC status", sorted(df["kyc_status"].dropna().unique()),
                         default=sorted(df["kyc_status"].dropna().unique()))

view = df[
    df["segment"].isin(segments)
    & df["preferred_channel"].isin(channels)
    & df["kyc_status"].isin(kyc)
]

if view.empty:
    st.warning("No customers match the current filters.")
    st.stop()

# --------------------------------------------------------------------------
# KPI row
# --------------------------------------------------------------------------

total_customers = len(view)
active = int((view["kyc_status"] == "verified").sum())
avg_risk = view["risk_score"].mean()
avg_churn = view["churn_probability"].mean()
avg_products = view["product_count"].mean()

k1, k2, k3, k4 = st.columns(4)
k1.metric("TOTAL CUSTOMERS", f"{total_customers:,}", f"Active: {active:,}",
          delta_color="off")
k2.metric("AVG RISK SCORE", f"{avg_risk:.2f}")
k3.metric("AVG CHURN PROB", f"{avg_churn * 100:.1f}%")
k4.metric("AVG PRODUCTS HELD", f"{avg_products:.1f}")

st.divider()

# --------------------------------------------------------------------------
# Scatter + segment bars
# --------------------------------------------------------------------------

left, right = st.columns([3, 2])

with left:
    st.markdown("**Risk Score vs Churn Probability** — coloured by segment")
    st.caption("Top-right quadrant is the outreach priority: high risk and high churn.")
    fig = px.scatter(
        view, x="risk_score", y="churn_probability", color="segment",
        size="product_count", size_max=13, opacity=0.55,
        hover_data=["customerId", "clv_tier", "composite_risk_score"],
        category_orders={"segment": da.SEGMENT_ORDER},
        color_discrete_sequence=da.SEQUENCE,
        labels={"risk_score": "Risk score", "churn_probability": "Churn probability",
                "segment": ""},
    )
    # Quadrant guides at the portfolio means, so "high" is relative to this book.
    fig.add_vline(x=avg_risk, line_dash="dot", line_color=da.PALETTE["muted"])
    fig.add_hline(y=avg_churn, line_dash="dot", line_color=da.PALETTE["muted"])
    fig.update_layout(height=420, margin=dict(t=10, b=10, l=10, r=10),
                      legend=dict(orientation="h", y=1.1, x=0))
    st.plotly_chart(fig, width="stretch")

with right:
    st.markdown("**Customer Segment Distribution**")
    seg = (
        view.groupby("segment")
        .agg(customers=("customerId", "count"), avg_risk=("risk_score", "mean"))
        .reset_index()
        .sort_values("customers", ascending=True)
    )
    fig = px.bar(
        seg, x="customers", y="segment", orientation="h",
        color="avg_risk", color_continuous_scale="RdYlGn_r",
        text="customers",
        labels={"customers": "Customers", "segment": "", "avg_risk": "Avg risk"},
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig.update_layout(height=420, margin=dict(t=10, b=10, l=10, r=10),
                      coloraxis_colorbar=dict(title="Avg<br>risk"))
    st.plotly_chart(fig, width="stretch")

st.divider()

# --------------------------------------------------------------------------
# Heatmap + CLV donut
# --------------------------------------------------------------------------

left, right = st.columns([3, 2])

with left:
    st.markdown("**Churn Heatmap** — churn probability by segment and channel")
    st.caption("Identifies which segment/channel combinations carry the highest attrition risk.")
    heat = (
        view.pivot_table(index="segment", columns="preferred_channel",
                         values="churn_probability", aggfunc="mean")
        .reindex([s for s in da.SEGMENT_ORDER if s in view["segment"].unique()])
    )
    fig = px.imshow(
        heat, text_auto=".2f", aspect="auto",
        color_continuous_scale="RdYlGn_r",
        labels={"x": "Preferred channel", "y": "", "color": "Churn"},
    )
    fig.update_layout(height=380, margin=dict(t=10, b=10, l=10, r=10))
    st.plotly_chart(fig, width="stretch")

with right:
    st.markdown("**CLV Tier Breakdown**")
    st.caption("Guides where retention effort is worth spending.")
    tiers = view["clv_tier"].value_counts().reset_index()
    tiers.columns = ["clv_tier", "customers"]
    order = ["High Value", "Growth Potential", "At Risk", "No Activity"]
    tiers["order"] = tiers["clv_tier"].apply(
        lambda t: order.index(t) if t in order else len(order))
    tiers = tiers.sort_values("order")
    fig = px.pie(
        tiers, names="clv_tier", values="customers", hole=0.55,
        color="clv_tier",
        color_discrete_map={
            "High Value": da.PALETTE["success"],
            "Growth Potential": da.PALETTE["primary"],
            "At Risk": da.PALETTE["danger"],
            "No Activity": da.PALETTE["muted"],
        },
    )
    fig.update_traces(textinfo="label+percent")
    fig.update_layout(height=380, margin=dict(t=10, b=10, l=10, r=10),
                      showlegend=False)
    st.plotly_chart(fig, width="stretch")

st.divider()

# --------------------------------------------------------------------------
# Product holdings matrix + outreach list
# --------------------------------------------------------------------------

st.markdown("**Highest-priority customers** — by composite risk score")
st.caption("composite_risk_score = (fraud_rate_pct x 0.6) + (churn_probability x 0.4), per spec 9.1")

cols = [c for c in ["customerId", "name", "segment", "kyc_status", "preferred_channel",
                    "product_count", "risk_score", "churn_probability",
                    "composite_risk_score", "risk_band", "clv_score", "clv_tier",
                    "dormancy_severity", "fraud_count"] if c in view.columns]

st.dataframe(
    view.nlargest(50, "composite_risk_score")[cols],
    width="stretch", hide_index=True, height=360,
    column_config={
        "risk_score": st.column_config.ProgressColumn("Risk", min_value=0, max_value=1,
                                                      format="%.2f"),
        "churn_probability": st.column_config.ProgressColumn("Churn", min_value=0,
                                                             max_value=1, format="%.2f"),
        "clv_score": st.column_config.NumberColumn("CLV", format="%.3f"),
        "composite_risk_score": st.column_config.NumberColumn("Composite", format="%.3f"),
    },
)
