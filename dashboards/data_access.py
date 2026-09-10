"""Shared data loading for the FinSight dashboards.

Every page reads from the same export files the Power BI report would import, so
the two reporting surfaces cannot disagree. The Fraud Alert Board additionally
tails the txn-flagged Kafka topic for its live feed.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd
import streamlit as st

# librdkafka logs a connection failure every ~50ms while a broker is down, which
# floods the console during file-mode demos. Routing its logs to a silenced
# logger keeps the fallback path quiet without hiding genuine Python errors.
_kafka_log = logging.getLogger("finsight.kafka")
_kafka_log.addHandler(logging.NullHandler())
_kafka_log.propagate = False

ROOT = Path(__file__).resolve().parents[1]
EXPORTS = ROOT / "exports"
DATA_RAW = ROOT / "data" / "raw"

KAFKA_BOOTSTRAP = os.environ.get("FINSIGHT_KAFKA_HOST", "localhost:9092")
TOPIC_FLAGGED = "txn-flagged"
TOPIC_CHURN = "txn-churn"

STEPS_PER_DAY = 24
LEGACY_FALSE_POSITIVE_RATE = 62.0   # spec 10, Fraud Alert Board KPI 4

TYPE_ORDER = ["TRANSFER", "CASH_OUT", "PAYMENT", "CASH_IN", "DEBIT"]
SEGMENT_ORDER = ["Standard", "Basic", "Premium", "Student", "Private Banking"]

PALETTE = {
    "primary": "#2563eb",
    "danger": "#dc2626",
    "warning": "#f59e0b",
    "success": "#16a34a",
    "muted": "#64748b",
}
SEQUENCE = ["#2563eb", "#dc2626", "#f59e0b", "#16a34a", "#8b5cf6", "#0891b2"]


class MissingExport(FileNotFoundError):
    pass


def _read(name: str) -> pd.DataFrame:
    path = EXPORTS / name
    if not path.exists():
        raise MissingExport(
            f"{name} not found in exports/.\n\n"
            "Generate it by running the Spark jobs, or offline with:\n"
            "    python tools/offline_exports.py"
        )
    return pd.read_csv(path)


@st.cache_data(ttl=30)
def transactions() -> pd.DataFrame:
    path = DATA_RAW / "demo" / "transactions.csv"
    if not path.exists():
        raise MissingExport("data/raw/demo/transactions.csv not found; "
                            "run data/generator/generate_data.py")
    df = pd.read_csv(path)
    df["day"] = ((df["step"] - 1) // STEPS_PER_DAY) + 1
    return df


@st.cache_data(ttl=30)
def flagged() -> pd.DataFrame:
    """Transactions caught by the streaming fraud rule (spec 7.1)."""
    df = _read("flagged_transactions.csv")
    df["day"] = ((df["step"] - 1) // STEPS_PER_DAY) + 1
    # A flagged record whose ground-truth label is 0 is a false positive; that
    # difference is the whole point of KPI 4.
    df["is_false_positive"] = df["isFraud"] == 0
    return df


@st.cache_data(ttl=30)
def compliance() -> pd.DataFrame:
    return _read("compliance_summary.csv")


@st.cache_data(ttl=30)
def daily_summary() -> pd.DataFrame:
    df = _read("daily_summary.csv")
    df["day"] = ((df["step"] - 1) // STEPS_PER_DAY) + 1
    return df


@st.cache_data(ttl=30)
def dormancy() -> pd.DataFrame:
    return _read("dormancy_report.csv")


@st.cache_data(ttl=30)
def customer_blend() -> pd.DataFrame:
    """Alteryx Customer Risk Blend output (spec 9.1)."""
    return _read("customer_risk_blend.csv")


@st.cache_data(ttl=30)
def transaction_summary() -> pd.DataFrame:
    """Alteryx Transaction Summary output (spec 9.2)."""
    return _read("transaction_summary.csv")


@st.cache_data(ttl=10)
def live_topic(topic: str, max_messages: int = 500) -> pd.DataFrame:
    """Drain a Kafka topic without blocking the page render.

    Returns an empty frame when the broker is unreachable so the dashboard
    degrades to its file-based view rather than erroring out.
    """
    try:
        from confluent_kafka import Consumer, KafkaException  # noqa: F401
    except ImportError:
        return pd.DataFrame()

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"finsight-dashboard-{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 6000,
            "socket.timeout.ms": 3000,
        },
        logger=_kafka_log,
    )
    records = []
    try:
        consumer.subscribe([topic])
        deadline = max_messages
        while len(records) < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                break
            try:
                records.append(json.loads(msg.value().decode()))
            except (ValueError, AttributeError):
                continue
    except Exception:  # noqa: BLE001
        return pd.DataFrame()
    finally:
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            pass

    return pd.DataFrame(records)


@st.cache_data(ttl=15)
def kafka_available() -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient(
            {"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 2000},
            logger=_kafka_log,
        )
        return bool(admin.list_topics(timeout=2).topics)
    except Exception:  # noqa: BLE001
        return False


def page_header(title: str, audience: str, source: str, badge: str | None = None) -> None:
    left, right = st.columns([4, 1])
    with left:
        st.markdown(f"### FinSight — {title}")
        st.caption(f"**Audience:** {audience}  ·  **Source:** {source}")
    if badge:
        with right:
            st.markdown(
                f"<div style='text-align:right;padding-top:18px;'>"
                f"<span style='background:{PALETTE['success']};color:white;"
                f"padding:4px 12px;border-radius:12px;font-size:12px;"
                f"font-weight:600;'>{badge}</span></div>",
                unsafe_allow_html=True,
            )
    st.divider()


def missing_export_banner(exc: MissingExport) -> None:
    st.error("Required export is missing")
    st.code(str(exc), language="text")
    st.stop()
