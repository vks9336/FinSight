"""FinSight dashboards — Power BI substitute.

Three report pages for three audiences, matching the .pbix described in spec
section 10. Run with:

    streamlit run dashboards/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st

import data_access as da

st.set_page_config(
    page_title="FinSight — NovaCrest Bank",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("## FinSight")
st.markdown("##### NovaCrest Bank — unified data platform")
st.divider()

st.markdown(
    """
Three report pages, each built for a different audience. Open them from the
sidebar.

| Page | Audience | Source |
|---|---|---|
| **Fraud Alert Board** | Fraud & Risk Team | `txn-flagged` Kafka topic + Alteryx output |
| **Customer 360** | Relationship Managers | Alteryx Customer Risk Blend |
| **Risk & Compliance** | Compliance Officers | Spark SQL → Hive → Alteryx Transaction Summary |
"""
)

st.divider()
st.markdown("#### Platform status")

cols = st.columns(4)

exports = [
    ("Flagged transactions", "flagged_transactions.csv"),
    ("Compliance summary", "compliance_summary.csv"),
    ("Customer risk blend", "customer_risk_blend.csv"),
    ("Dormancy report", "dormancy_report.csv"),
]
for col, (label, filename) in zip(cols, exports):
    present = (da.EXPORTS / filename).exists()
    col.metric(label, "ready" if present else "missing")
    col.caption(f"`exports/{filename}`")

st.markdown("")
live = da.kafka_available()
if live:
    st.success(f"Kafka reachable at `{da.KAFKA_BOOTSTRAP}` — the Fraud Alert "
               "Board will show the live feed.")
else:
    st.info(
        f"Kafka not reachable at `{da.KAFKA_BOOTSTRAP}`. Pages fall back to the "
        "export files, which is the expected state when the cluster is down."
    )

missing = [f for _, f in exports if not (da.EXPORTS / f).exists()]
if missing:
    st.warning(
        "Some exports are missing. Generate them with the Spark jobs, or "
        "offline with `python tools/offline_exports.py`."
    )

st.divider()
st.caption(
    "Metric definitions are shared with the Alteryx workflows in `blending/` so "
    "both reporting surfaces agree. See `docs/04_powerbi_build_guide.md` to "
    "rebuild these pages as a .pbix."
)
