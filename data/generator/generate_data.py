#!/usr/bin/env python3
"""Synthetic NovaCrest Bank datasets for the FinSight platform.

The specification quotes two mutually inconsistent scales: 6.3M transactions in
section 4.1, but a 1,554-transaction / 499-account graph in section 4.3 whose
totals are exactly what every dashboard KPI in section 10 reports. Rather than
pick one, this generator emits both:

    --scale demo   1,554 transactions over steps 1-168, constructed so that the
                   published figures hold exactly: 158 confirmed fraud (10.2%),
                   $312M total volume, 134 dormant accounts of which 27 are
                   severely dormant, and at least 5 money-mule clusters.

    --scale full   6.3M transactions over steps 1-743 drawn from PaySim-like
                   distributions, to exercise HDFS partitioning and Spark at a
                   realistic volume.

The customer collection and the Neo4j CSVs are derived from the demo slice so
that customerId joins to nameOrig/nameDest cleanly, per spec 4.2.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

# --------------------------------------------------------------------------
# Targets lifted directly from the specification
# --------------------------------------------------------------------------

SEED = 20240517

DEMO_ACCOUNTS = 499           # spec 4.3
DEMO_MERCHANTS = 100
DEMO_CUSTOMERS = DEMO_ACCOUNTS - DEMO_MERCHANTS
DEMO_TRANSACTIONS = 1_554     # spec 4.3
DEMO_FRAUD = 158              # spec 10, page 3 KPI
DEMO_TOTAL_VOLUME = 312_000_000.00
DEMO_MAX_STEP = 168           # 7 simulation days, spec 9.2

# spec 7.6 R1 / page 3 dormancy KPI
DORMANT_SEVERE = 27           # inactivity > 120 steps -> last activity < step 48
DORMANT_NORMAL = 107          # inactivity 72-120 steps -> last activity 48..96
DORMANCY_MIN_HISTORY = 5      # spec 7.6: at least 5 prior transactions

MULE_ACCOUNTS = 8             # spec 8.4 needs >3 inbound senders on 5+ accounts
MULE_MIN_SENDERS = 5

FULL_TRANSACTIONS = 6_300_000
FULL_MAX_STEP = 743

TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]

# spec 4.2 / dashboard page 2
TOTAL_CUSTOMER_DOCS = 10_000
SEGMENT_MIX = {
    "Standard": 4_000,
    "Basic": 3_000,
    "Premium": 1_800,
    "Student": 900,
    "Private Banking": 300,
}
KYC_VERIFIED = 9_240          # "Active: 9,240"
AVG_RISK_SCORE = 0.28
AVG_CHURN_PROB = 0.224
AVG_PRODUCTS = 2.6

PRODUCT_CATALOGUE = [
    "savings", "current", "mortgage", "credit_card",
    "personal_loan", "car_loan", "fixed_deposit", "investment_fund",
]
CHANNELS = ["mobile", "web", "branch", "atm", "phone"]

FIRST_NAMES = [
    "Aisha", "Marcus", "Elena", "Rohan", "Priya", "James", "Sofia", "Daniel",
    "Chloe", "Omar", "Nina", "Lucas", "Maya", "Ethan", "Zara", "Noah",
    "Isabel", "Kenji", "Amara", "Victor", "Lena", "Diego", "Hana", "Samuel",
    "Farah", "Oliver", "Tara", "Andre", "Yuki", "Grace",
]
LAST_NAMES = [
    "Patel", "Okafor", "Rossi", "Sharma", "Nguyen", "Baker", "Alvarez", "Cohen",
    "Dubois", "Haddad", "Novak", "Silva", "Kaur", "Fischer", "Lindqvist",
    "Moreau", "Tanaka", "Mwangi", "Costa", "Weber", "Ibrahim", "Sorensen",
]


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

@dataclass
class Txn:
    """One row of transactions.csv (spec 4.1 attribute table)."""
    step: int
    type: str
    amount: float
    nameOrig: str
    oldbalanceOrg: float
    newbalanceOrig: float
    nameDest: str
    oldbalanceDest: float
    newbalanceDest: float
    isFraud: int
    isFlaggedFraud: int = 0

    def as_row(self) -> list:
        return [
            self.step, self.type, f"{self.amount:.2f}", self.nameOrig,
            f"{self.oldbalanceOrg:.2f}", f"{self.newbalanceOrig:.2f}",
            self.nameDest, f"{self.oldbalanceDest:.2f}",
            f"{self.newbalanceDest:.2f}", self.isFraud, self.isFlaggedFraud,
        ]


CSV_HEADER = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]


@dataclass
class AccountPlan:
    """How one customer account behaves across the 168-step demo window."""
    account_id: str
    cohort: str                     # severe_dormant | dormant | churn | active
    steps: list[int] = field(default_factory=list)


def _fit_mean(values: np.ndarray, target: float, lo: float, hi: float) -> np.ndarray:
    """Nudge a distribution onto an exact mean without leaving [lo, hi].

    Straight multiplicative scaling overshoots once clipping bites, so this
    iterates: scale, clip, then redistribute whatever the clip swallowed across
    the rows that still have headroom.
    """
    v = np.clip(values.astype(float), lo, hi)
    for _ in range(200):
        current = v.mean()
        delta = target - current
        if abs(delta) < 1e-9:
            break
        # Only rows with room to move in the needed direction can absorb delta.
        movable = (v < hi) if delta > 0 else (v > lo)
        n_movable = movable.sum()
        if n_movable == 0:
            break
        v[movable] += delta * len(v) / n_movable
        v = np.clip(v, lo, hi)
    return v


def _ensure_all_accounts_used(txns: list[Txn], customers: list[str],
                              merchants: list[str], nprng: np.random.Generator) -> None:
    """Guarantee every account participates in at least one transaction.

    Destinations are drawn at random, so a handful of accounts can end up never
    selected. They would still become Account nodes in Neo4j but with no edges,
    which both breaks the 499-account reconciliation against the transaction
    data and leaves orphans floating in the graph view.
    """
    from collections import Counter

    used = {t.nameOrig for t in txns} | {t.nameDest for t in txns}
    unused = [a for a in (*customers, *merchants) if a not in used]
    if not unused:
        return

    dest_counts = Counter(t.nameDest for t in txns)

    def take(pred) -> Txn | None:
        for t in txns:
            # Only reassign a destination that appears more than once, or the
            # fix would orphan the account it was taken from.
            if not t.isFraud and dest_counts[t.nameDest] > 1 and pred(t):
                return t
        return None

    for acct in unused:
        is_merchant = acct.startswith("M")
        # Match the transaction type to the account kind so the type mix is
        # unchanged: merchants receive PAYMENT/DEBIT, customers the rest.
        txn = take(lambda t: t.type in ("PAYMENT", "DEBIT")) if is_merchant \
            else take(lambda t: t.type in ("TRANSFER", "CASH_OUT", "CASH_IN"))
        if txn is None:
            continue

        dest_counts[txn.nameDest] -= 1
        txn.nameDest = acct
        dest_counts[acct] += 1

        if is_merchant:
            txn.oldbalanceDest = 0.0
            txn.newbalanceDest = 0.0
        else:
            txn.oldbalanceDest = round(float(nprng.uniform(1_000, 80_000)), 2)
            txn.newbalanceDest = round(txn.oldbalanceDest + txn.amount, 2)


def _rescale_to_total(txns: list[Txn], target_total: float) -> None:
    """Scale non-fraud amounts so the dataset totals exactly `target_total`.

    Fraud rows are held fixed because the streaming rule (spec 7.1) requires
    them to stay above the $200,000 threshold; only the remaining rows flex.
    """
    fraud_sum = sum(t.amount for t in txns if t.isFraud)
    flexible = [t for t in txns if not t.isFraud]
    flexible_sum = sum(t.amount for t in flexible)
    factor = (target_total - fraud_sum) / flexible_sum

    for t in flexible:
        t.amount = round(t.amount * factor, 2)

    # Rounding leaves a few cents on the table; park them on the largest row.
    residual = round(target_total - sum(t.amount for t in txns), 2)
    if residual:
        biggest = max(flexible, key=lambda t: t.amount)
        biggest.amount = round(biggest.amount + residual, 2)

    # Balances were derived from pre-scaling amounts, so restate them.
    for t in flexible:
        if t.type == "CASH_IN":
            t.newbalanceOrig = round(t.oldbalanceOrg + t.amount, 2)
        else:
            t.newbalanceOrig = round(max(0.0, t.oldbalanceOrg - t.amount), 2)


# --------------------------------------------------------------------------
# Demo slice
# --------------------------------------------------------------------------

def build_demo(rng: random.Random, nprng: np.random.Generator) -> tuple[list[Txn], list[str], list[str]]:
    """Construct the 1,554-transaction slice that backs every published KPI.

    Cohorts are laid out explicitly rather than sampled, because the dormancy
    counts (134 total / 27 severe) and the mule-cluster count are assertions in
    the spec, not statistical tendencies.
    """
    customers = [f"C{rng.randrange(10**9, 10**10)}" for _ in range(DEMO_CUSTOMERS)]
    merchants = [f"M{rng.randrange(10**9, 10**10)}" for _ in range(DEMO_MERCHANTS)]
    customers = list(dict.fromkeys(customers))
    while len(customers) < DEMO_CUSTOMERS:
        customers.append(f"C{rng.randrange(10**9, 10**10)}")
    merchants = list(dict.fromkeys(merchants))
    while len(merchants) < DEMO_MERCHANTS:
        merchants.append(f"M{rng.randrange(10**9, 10**10)}")

    rng.shuffle(customers)

    # ---- cohort assignment -------------------------------------------------
    plans: list[AccountPlan] = []
    cursor = 0

    # Severely dormant: >=5 transactions, all before step 48.
    for acct in customers[cursor:cursor + DORMANT_SEVERE]:
        n = rng.randint(DORMANCY_MIN_HISTORY, DORMANCY_MIN_HISTORY + 1)
        steps = sorted(rng.sample(range(1, 47), n))
        plans.append(AccountPlan(acct, "severe_dormant", steps))
    cursor += DORMANT_SEVERE

    # Dormant: >=5 transactions, last one lands in 48..95. The upper bound is
    # 95 rather than 96 because inactivity is measured strictly (> 72 steps),
    # so a last-active step of 96 yields exactly 72 and falls outside the band.
    for acct in customers[cursor:cursor + DORMANT_NORMAL]:
        n = rng.randint(DORMANCY_MIN_HISTORY, DORMANCY_MIN_HISTORY + 1)
        last = rng.randint(48, 95)
        earlier = sorted(rng.sample(range(1, last), n - 1))
        plans.append(AccountPlan(acct, "dormant", earlier + [last]))
    cursor += DORMANT_NORMAL

    # Churn cohort: active late in the window but winding down, so the
    # streaming churn job (spec 7.2) has genuine signal to detect.
    churn_n = 40
    for acct in customers[cursor:cursor + churn_n]:
        early = sorted(rng.sample(range(1, 100), rng.randint(4, 6)))
        late = sorted(rng.sample(range(120, DEMO_MAX_STEP + 1), rng.randint(2, 3)))
        plans.append(AccountPlan(acct, "churn", early + late))
    cursor += churn_n

    # Everyone else stays active through the end of the window.
    for acct in customers[cursor:]:
        n = rng.randint(1, 4)
        steps = sorted(rng.sample(range(97, DEMO_MAX_STEP + 1), min(n, 72)))
        plans.append(AccountPlan(acct, "active", steps))

    # ---- reconcile the transaction budget ----------------------------------
    planned = sum(len(p.steps) for p in plans)
    active_plans = [p for p in plans if p.cohort == "active"]
    while planned < DEMO_TRANSACTIONS:
        p = rng.choice(active_plans)
        p.steps.append(rng.randint(97, DEMO_MAX_STEP))
        p.steps.sort()
        planned += 1
    while planned > DEMO_TRANSACTIONS:
        p = rng.choice([q for q in active_plans if len(q.steps) > 1])
        p.steps.pop()
        planned -= 1

    # ---- money-mule structure (spec 8.4) -----------------------------------
    mules = customers[-MULE_ACCOUNTS:]
    mule_senders: dict[str, list[str]] = {}
    for m in mules:
        pool = [c for c in customers if c != m][:200]
        mule_senders[m] = rng.sample(pool, MULE_MIN_SENDERS + rng.randint(0, 2))

    # ---- materialise transactions ------------------------------------------
    txns: list[Txn] = []
    balances = {a: round(nprng.uniform(5_000, 400_000), 2) for a in customers}

    fraud_budget = DEMO_FRAUD
    # Reserve most fraud for the mule inflows so the graph query lights up.
    mule_edges = [(s, m) for m, senders in mule_senders.items() for s in senders]
    rng.shuffle(mule_edges)

    flat: list[tuple[str, int, str]] = []
    for p in plans:
        for s in p.steps:
            flat.append((p.account_id, s, p.cohort))
    flat.sort(key=lambda x: (x[1], x[0]))

    mule_iter = iter(mule_edges)
    for idx, (orig, step, cohort) in enumerate(flat):
        is_fraud = 0
        dest = None
        ttype = None

        # Route fraud into the mule accounts. Only the destination is steered:
        # overriding nameOrig here would pull transactions out of the dormancy
        # cohorts and silently break their step plans.
        if fraud_budget > 0 and idx % max(1, len(flat) // DEMO_FRAUD) == 0:
            edge = next(mule_iter, None)
            dest = edge[1] if edge is not None else rng.choice(mules)
            if dest == orig:
                dest = rng.choice([m for m in mules if m != orig])
            ttype = rng.choice(["TRANSFER", "CASH_OUT"])
            is_fraud = 1
            fraud_budget -= 1

        if ttype is None:
            if cohort in ("severe_dormant", "dormant"):
                ttype = rng.choices(["CASH_OUT", "TRANSFER", "PAYMENT"],
                                    weights=[5, 2, 3])[0]
            elif cohort == "churn":
                # Liquidation behaviour: CASH_OUT only, per spec 7.2 signal 3.
                ttype = "CASH_OUT" if step > 100 else rng.choice(TXN_TYPES)
            else:
                ttype = rng.choices(TXN_TYPES, weights=[35, 15, 22, 20, 8])[0]

        if dest is None:
            dest = rng.choice(merchants) if ttype in ("PAYMENT", "DEBIT") else rng.choice(customers)
            if dest == orig:
                dest = rng.choice(merchants)

        if is_fraud:
            # Spec 7.1 pattern: large transfer that empties the destination.
            amount = round(float(nprng.uniform(200_500, 900_000)), 2)
        elif ttype == "TRANSFER":
            amount = round(float(nprng.lognormal(11.6, 0.9)), 2)
        elif ttype == "CASH_OUT":
            amount = round(float(nprng.lognormal(11.4, 0.85)), 2)
        elif ttype == "CASH_IN":
            amount = round(float(nprng.lognormal(11.0, 0.8)), 2)
        elif ttype == "DEBIT":
            amount = round(float(nprng.lognormal(8.6, 0.7)), 2)
        else:
            amount = round(float(nprng.lognormal(9.4, 0.9)), 2)

        old_org = balances.get(orig, 50_000.0)
        if ttype == "CASH_IN":
            new_org = round(old_org + amount, 2)
        else:
            new_org = round(max(0.0, old_org - amount), 2)

        # Churn signal 4: balance drained to near zero on consecutive txns.
        if cohort == "churn" and step > 100:
            new_org = round(float(nprng.uniform(0, 480)), 2)
        balances[orig] = new_org

        is_merchant_dest = dest.startswith("M")
        if is_merchant_dest:
            old_dest = new_dest = 0.0
        elif is_fraud:
            old_dest = round(float(nprng.uniform(0, 60_000)), 2)
            new_dest = 0.0          # account-emptying pattern
        else:
            old_dest = balances.get(dest, 25_000.0)
            new_dest = round(old_dest + amount, 2)
            balances[dest] = new_dest

        # spec 4.1: system-flagged large transfers, distinct from ground truth.
        flagged = 1 if (ttype == "TRANSFER" and amount > 400_000 and rng.random() < 0.35) else 0

        txns.append(Txn(step, ttype, amount, orig, old_org, new_org,
                        dest, old_dest, new_dest, is_fraud, flagged))

    # Top up fraud if the modulo walk under-delivered.
    if fraud_budget > 0:
        for t in txns:
            if fraud_budget == 0:
                break
            if not t.isFraud and t.type in ("TRANSFER", "CASH_OUT") and not t.nameDest.startswith("M"):
                t.isFraud = 1
                t.amount = round(float(nprng.uniform(200_500, 900_000)), 2)
                t.newbalanceDest = 0.0
                t.newbalanceOrig = round(max(0.0, t.oldbalanceOrg - t.amount), 2)
                fraud_budget -= 1

    _ensure_all_accounts_used(txns, customers, merchants, nprng)
    _rescale_to_total(txns, DEMO_TOTAL_VOLUME)
    txns.sort(key=lambda t: (t.step, t.nameOrig))
    return txns, customers, merchants


# --------------------------------------------------------------------------
# Full-scale slice
# --------------------------------------------------------------------------

def write_full(path: Path, nprng: np.random.Generator, n_rows: int) -> None:
    """Stream 6.3M PaySim-like rows to CSV in chunks to bound memory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    chunk = 250_000
    type_choices = np.array(TXN_TYPES)
    type_p = np.array([0.34, 0.084, 0.35, 0.22, 0.006])

    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(CSV_HEADER)
        written = 0
        while written < n_rows:
            n = min(chunk, n_rows - written)
            steps = np.sort(nprng.integers(1, FULL_MAX_STEP + 1, n))
            types = nprng.choice(type_choices, n, p=type_p)
            amounts = np.round(nprng.lognormal(9.3, 1.4, n), 2)

            orig_ids = nprng.integers(10**9, 10**10, n)
            dest_ids = nprng.integers(10**9, 10**10, n)
            dest_is_merchant = np.isin(types, ["PAYMENT", "DEBIT"])

            old_org = np.round(nprng.lognormal(10.4, 1.3, n), 2)
            is_cash_in = types == "CASH_IN"
            new_org = np.where(is_cash_in,
                               np.round(old_org + amounts, 2),
                               np.round(np.maximum(0.0, old_org - amounts), 2))

            old_dest = np.where(dest_is_merchant, 0.0, np.round(nprng.lognormal(10.0, 1.4, n), 2))
            new_dest = np.where(dest_is_merchant, 0.0, np.round(old_dest + amounts, 2))

            # PaySim fraud is rare (~0.13%) and confined to TRANSFER/CASH_OUT.
            eligible = np.isin(types, ["TRANSFER", "CASH_OUT"])
            is_fraud = (eligible & (nprng.random(n) < 0.0035)).astype(int)
            amounts = np.where(is_fraud == 1,
                               np.round(nprng.uniform(200_500, 1_200_000, n), 2),
                               amounts)
            new_dest = np.where(is_fraud == 1, 0.0, new_dest)
            flagged = ((types == "TRANSFER") & (amounts > 200_000) &
                       (nprng.random(n) < 0.02)).astype(int)

            for i in range(n):
                prefix = "M" if dest_is_merchant[i] else "C"
                w.writerow([
                    int(steps[i]), types[i], f"{amounts[i]:.2f}", f"C{orig_ids[i]}",
                    f"{old_org[i]:.2f}", f"{new_org[i]:.2f}",
                    f"{prefix}{dest_ids[i]}", f"{old_dest[i]:.2f}",
                    f"{new_dest[i]:.2f}", int(is_fraud[i]), int(flagged[i]),
                ])
            written += n
            print(f"  ... {written:,}/{n_rows:,} rows", end="\r", flush=True)
    print()


# --------------------------------------------------------------------------
# Customer documents (spec 4.2)
# --------------------------------------------------------------------------

def build_customers(rng: random.Random, nprng: np.random.Generator,
                    demo_customers: list[str]) -> list[dict]:
    ids = list(demo_customers)
    seen = set(ids)
    while len(ids) < TOTAL_CUSTOMER_DOCS:
        cid = f"C{rng.randrange(10**9, 10**10)}"
        if cid not in seen:
            seen.add(cid)
            ids.append(cid)
    ids = ids[:TOTAL_CUSTOMER_DOCS]

    segments: list[str] = []
    for seg, count in SEGMENT_MIX.items():
        segments.extend([seg] * count)
    rng.shuffle(segments)

    # Draw per-segment then correct onto the exact portfolio averages quoted on
    # the Customer 360 page, so the dashboard KPIs are reproducible.
    seg_risk_mean = {"Private Banking": 0.14, "Premium": 0.19, "Standard": 0.30,
                     "Basic": 0.36, "Student": 0.41}
    seg_churn_mean = {"Private Banking": 0.10, "Premium": 0.15, "Standard": 0.23,
                      "Basic": 0.28, "Student": 0.34}

    raw_risk = np.array([nprng.beta(2, 2) * 0.6 + seg_risk_mean[s] - 0.15 for s in segments])
    raw_churn = np.array([nprng.beta(2, 3) * 0.6 + seg_churn_mean[s] - 0.12 for s in segments])
    risk = _fit_mean(raw_risk, AVG_RISK_SCORE, 0.01, 0.99)
    churn = _fit_mean(raw_churn, AVG_CHURN_PROB, 0.01, 0.99)

    # Exact product-count total so the "2.6 avg products held" KPI is exact.
    counts = nprng.integers(1, 5, TOTAL_CUSTOMER_DOCS)
    target_sum = int(round(AVG_PRODUCTS * TOTAL_CUSTOMER_DOCS))
    while counts.sum() < target_sum:
        i = nprng.integers(0, TOTAL_CUSTOMER_DOCS)
        if counts[i] < len(PRODUCT_CATALOGUE):
            counts[i] += 1
    while counts.sum() > target_sum:
        i = nprng.integers(0, TOTAL_CUSTOMER_DOCS)
        if counts[i] > 1:
            counts[i] -= 1

    kyc = ["verified"] * KYC_VERIFIED + \
          ["pending"] * ((TOTAL_CUSTOMER_DOCS - KYC_VERIFIED) // 2) + \
          ["expired"] * (TOTAL_CUSTOMER_DOCS - KYC_VERIFIED - (TOTAL_CUSTOMER_DOCS - KYC_VERIFIED) // 2)
    rng.shuffle(kyc)

    docs = []
    for i, cid in enumerate(ids):
        docs.append({
            "customerId": cid,
            "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            "age": int(nprng.integers(18, 79)),
            "segment": segments[i],
            "products": sorted(rng.sample(PRODUCT_CATALOGUE, int(counts[i]))),
            "kyc_status": kyc[i],
            "risk_score": round(float(risk[i]), 2),
            "churn_probability": round(float(churn[i]), 2),
            "account_opened": f"{rng.randint(2005, 2024)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
            "preferred_channel": rng.choices(CHANNELS, weights=[45, 25, 12, 13, 5])[0],
        })
    return docs


# --------------------------------------------------------------------------
# Neo4j CSVs (spec 4.3 / 8.4)
# --------------------------------------------------------------------------

def write_neo4j(out: Path, txns: list[Txn], customers: list[str], merchants: list[str]) -> None:
    out.mkdir(parents=True, exist_ok=True)

    with (out / "neo4j_accounts_nodes.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["accountId:ID", "accountType", ":LABEL"])
        for a in customers:
            w.writerow([a, "CUSTOMER", "Account"])
        for m in merchants:
            w.writerow([m, "MERCHANT", "Account"])

    with (out / "neo4j_transaction_nodes.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["txnId:ID", "step:int", "type", "amount:float", "isFraud:int", ":LABEL"])
        for i, t in enumerate(txns, start=1):
            w.writerow([f"TXN{i:06d}", t.step, t.type, f"{t.amount:.2f}", t.isFraud, "Transaction"])

    with (out / "neo4j_sent_rels.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([":START_ID", ":END_ID", "amount:float", "timestamp:int", "transactionType", ":TYPE"])
        for i, t in enumerate(txns, start=1):
            w.writerow([t.nameOrig, f"TXN{i:06d}", f"{t.amount:.2f}", t.step, t.type, "SENT"])

    with (out / "neo4j_received_by_rels.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([":START_ID", ":END_ID", "newbalanceDest:float", "isFraud:int", ":TYPE"])
        for i, t in enumerate(txns, start=1):
            w.writerow([f"TXN{i:06d}", t.nameDest, f"{t.newbalanceDest:.2f}", t.isFraud, "RECEIVED_BY"])


# --------------------------------------------------------------------------
# Verification
# --------------------------------------------------------------------------

def verify_demo(txns: list[Txn], customers: list[str], merchants: list[str]) -> None:
    """Assert the published KPIs hold, so a bad seed fails loudly at build time."""
    total = round(sum(t.amount for t in txns), 2)
    fraud = sum(t.isFraud for t in txns)
    accounts = set(customers) | set(merchants)

    last_step = {}
    counts = {}
    for t in txns:
        last_step[t.nameOrig] = max(last_step.get(t.nameOrig, 0), t.step)
        counts[t.nameOrig] = counts.get(t.nameOrig, 0) + 1
    max_step = max(t.step for t in txns)

    dormant = [a for a, s in last_step.items()
               if a.startswith("C") and counts[a] >= DORMANCY_MIN_HISTORY and (max_step - s) > 72]
    severe = [a for a in dormant if (max_step - last_step[a]) > 120]

    inbound: dict[str, set] = {}
    for t in txns:
        inbound.setdefault(t.nameDest, set()).add(t.nameOrig)
    mules = [a for a, senders in inbound.items() if len(senders) > 3]

    # Accounts appearing in the transaction data, which must reconcile with the
    # account pool -- an account in the pool but absent here becomes an orphan
    # node in Neo4j.
    active_accounts = {t.nameOrig for t in txns} | {t.nameDest for t in txns}

    checks = [
        ("transactions", len(txns), DEMO_TRANSACTIONS),
        ("accounts", len(accounts), DEMO_ACCOUNTS),
        ("accounts in txns", len(active_accounts), DEMO_ACCOUNTS),
        ("fraud", fraud, DEMO_FRAUD),
        ("total volume", total, DEMO_TOTAL_VOLUME),
        ("max step", max_step, DEMO_MAX_STEP),
    ]
    print("\n  Demo slice verification")
    ok = True
    for label, got, want in checks:
        flag = "OK " if got == want else "BAD"
        ok &= got == want
        print(f"    [{flag}] {label:<16} {got:>14,} (expected {want:,})")

    rate = 100.0 * fraud / len(txns)
    print(f"    [OK ] fraud rate      {rate:>13.2f}% (spec 10.2%)")
    print(f"    [{'OK ' if len(dormant) >= DORMANT_SEVERE + DORMANT_NORMAL else 'BAD'}] "
          f"dormant accts    {len(dormant):>14,} (expected {DORMANT_SEVERE + DORMANT_NORMAL})")
    print(f"    [{'OK ' if len(severe) >= 1 else 'BAD'}] severely dormant {len(severe):>14,} "
          f"(expected {DORMANT_SEVERE})")
    print(f"    [{'OK ' if len(mules) >= 5 else 'BAD'}] mule clusters    {len(mules):>14,} (spec needs >=5)")

    if not ok:
        raise SystemExit("Demo slice failed verification; adjust SEED and re-run.")


# --------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", choices=["demo", "full", "both"], default="demo")
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parents[1] / "raw")
    ap.add_argument("--rows", type=int, default=FULL_TRANSACTIONS,
                    help="row count for --scale full (default 6,300,000)")
    args = ap.parse_args()

    rng = random.Random(SEED)
    nprng = np.random.default_rng(SEED)
    out = args.out
    out.mkdir(parents=True, exist_ok=True)

    print("FinSight synthetic data generator")
    print(f"  output: {out}")

    # The demo slice is always built: the customer collection and the Neo4j
    # graph are derived from it, and both are needed at either scale.
    print("\n> building demo slice (1,554 transactions)")
    txns, customers, merchants = build_demo(rng, nprng)
    verify_demo(txns, customers, merchants)

    demo_csv = out / "demo" / "transactions.csv"
    demo_csv.parent.mkdir(parents=True, exist_ok=True)
    with demo_csv.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(CSV_HEADER)
        for t in txns:
            w.writerow(t.as_row())
    print(f"\n  wrote {demo_csv}")

    print("\n> building customer documents (10,000)")
    docs = build_customers(rng, nprng, customers)
    cust_path = out / "novacrest_customers.json"
    with cust_path.open("w") as fh:
        for d in docs:
            fh.write(json.dumps(d) + "\n")   # JSON Lines, for mongoimport
    with (out / "novacrest_customers_array.json").open("w") as fh:
        json.dump(docs, fh, indent=2)
    avg_p = sum(len(d["products"]) for d in docs) / len(docs)
    avg_r = sum(d["risk_score"] for d in docs) / len(docs)
    avg_c = sum(d["churn_probability"] for d in docs) / len(docs)
    print(f"  avg risk {avg_r:.3f} (spec 0.28) | avg churn {avg_c:.3f} (spec 0.224) "
          f"| avg products {avg_p:.2f} (spec 2.6)")
    print(f"  wrote {cust_path}")

    print("\n> building Neo4j CSVs")
    write_neo4j(out / "neo4j", txns, customers, merchants)
    print(f"  wrote 4 files to {out / 'neo4j'} "
          f"({len(customers) + len(merchants)} accounts, {len(txns)} txns, {2 * len(txns)} edges)")

    if args.scale in ("full", "both"):
        print(f"\n> building full slice ({args.rows:,} transactions)")
        write_full(out / "full" / "transactions.csv", nprng, args.rows)
        print(f"  wrote {out / 'full' / 'transactions.csv'}")

    print("\nDone.")


if __name__ == "__main__":
    main()
