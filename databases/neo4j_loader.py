#!/usr/bin/env python3
"""Loads the NovaCrest fraud knowledge graph into Neo4j (spec 8.4).

Implements the two-relationship model

    (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)

rather than a direct Account-to-Account edge. Making Transaction a first-class
node is what allows amount, isFraud and step to be queried along the traversal
path, which the fraud-ring and money-mule queries depend on.

Reads the four CSVs emitted by data/generator/generate_data.py:

    neo4j_accounts_nodes.csv        499 Account nodes
    neo4j_transaction_nodes.csv   1,554 Transaction nodes
    neo4j_sent_rels.csv           1,554 SENT edges
    neo4j_received_by_rels.csv    1,554 RECEIVED_BY edges

    python databases/neo4j_loader.py --run-fraud-ring
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path

from neo4j import GraphDatabase

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
USER = os.environ.get("NEO4J_USER", "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "finsight123")

BATCH = 1_000

CONSTRAINTS = [
    "CREATE CONSTRAINT account_id IF NOT EXISTS "
    "FOR (a:Account) REQUIRE a.accountId IS UNIQUE",
    "CREATE CONSTRAINT txn_id IF NOT EXISTS "
    "FOR (t:Transaction) REQUIRE t.txnId IS UNIQUE",
]

# Indexes on the properties the fraud queries filter by. Without these the ring
# query degrades to a full scan of all 1,554 transaction nodes per traversal.
INDEXES = [
    "CREATE INDEX txn_is_fraud IF NOT EXISTS FOR (t:Transaction) ON (t.isFraud)",
    "CREATE INDEX txn_step IF NOT EXISTS FOR (t:Transaction) ON (t.step)",
    "CREATE INDEX account_type IF NOT EXISTS FOR (a:Account) ON (a.accountType)",
]


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def batched(rows: list[dict], size: int = BATCH):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


class GraphLoader:
    def __init__(self, driver):
        self.driver = driver

    def run(self, query: str, **params):
        with self.driver.session() as s:
            return s.run(query, **params).data()

    def wipe(self) -> None:
        print("==> clearing existing graph")
        # Delete in chunks; DETACH DELETE over the whole graph in one
        # transaction can exhaust the default heap on a larger load.
        while True:
            deleted = self.run(
                "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS c"
            )[0]["c"]
            if deleted == 0:
                break

    def schema(self) -> None:
        print("==> applying constraints and indexes")
        for stmt in CONSTRAINTS + INDEXES:
            self.run(stmt)

    def load_accounts(self, rows: list[dict]) -> None:
        query = """
        UNWIND $rows AS row
        MERGE (a:Account {accountId: row.accountId})
        SET a.accountType = row.accountType
        """
        for chunk in batched(rows):
            self.run(query, rows=[
                {"accountId": r["accountId:ID"], "accountType": r["accountType"]}
                for r in chunk
            ])
        print(f"    {len(rows):,} Account nodes")

    def load_transactions(self, rows: list[dict]) -> None:
        query = """
        UNWIND $rows AS row
        MERGE (t:Transaction {txnId: row.txnId})
        SET t.step    = row.step,
            t.type    = row.type,
            t.amount  = row.amount,
            t.isFraud = row.isFraud
        """
        for chunk in batched(rows):
            self.run(query, rows=[
                {
                    "txnId": r["txnId:ID"],
                    "step": int(r["step:int"]),
                    "type": r["type"],
                    "amount": float(r["amount:float"]),
                    "isFraud": int(r["isFraud:int"]),
                }
                for r in chunk
            ])
        print(f"    {len(rows):,} Transaction nodes")

    def load_sent(self, rows: list[dict]) -> None:
        query = """
        UNWIND $rows AS row
        MATCH (a:Account {accountId: row.src})
        MATCH (t:Transaction {txnId: row.dst})
        MERGE (a)-[r:SENT]->(t)
        SET r.amount          = row.amount,
            r.timestamp       = row.timestamp,
            r.transactionType = row.transactionType
        """
        for chunk in batched(rows):
            self.run(query, rows=[
                {
                    "src": r[":START_ID"],
                    "dst": r[":END_ID"],
                    "amount": float(r["amount:float"]),
                    "timestamp": int(r["timestamp:int"]),
                    "transactionType": r["transactionType"],
                }
                for r in chunk
            ])
        print(f"    {len(rows):,} SENT edges")

    def load_received(self, rows: list[dict]) -> None:
        query = """
        UNWIND $rows AS row
        MATCH (t:Transaction {txnId: row.src})
        MATCH (a:Account {accountId: row.dst})
        MERGE (t)-[r:RECEIVED_BY]->(a)
        SET r.newbalanceDest = row.newbalanceDest,
            r.isFraud        = row.isFraud
        """
        for chunk in batched(rows):
            self.run(query, rows=[
                {
                    "src": r[":START_ID"],
                    "dst": r[":END_ID"],
                    "newbalanceDest": float(r["newbalanceDest:float"]),
                    "isFraud": int(r["isFraud:int"]),
                }
                for r in chunk
            ])
        print(f"    {len(rows):,} RECEIVED_BY edges")

    def summary(self) -> None:
        stats = self.run("""
            MATCH (a:Account) WITH count(a) AS accounts
            MATCH (t:Transaction) WITH accounts, count(t) AS txns
            MATCH ()-[s:SENT]->() WITH accounts, txns, count(s) AS sent
            MATCH ()-[r:RECEIVED_BY]->()
            RETURN accounts, txns, sent, count(r) AS received
        """)[0]
        print("\n==> graph summary")
        print(f"    Account nodes      {stats['accounts']:>7,}")
        print(f"    Transaction nodes  {stats['txns']:>7,}")
        print(f"    SENT edges         {stats['sent']:>7,}")
        print(f"    RECEIVED_BY edges  {stats['received']:>7,}")
        print(f"    total edges        {stats['sent'] + stats['received']:>7,}")


FRAUD_RING_QUERY = """
// Money mule detection: accounts receiving from more than three distinct
// senders. Fan-in of this shape is the primary structuring signal, and it is
// invisible to per-transaction tabular rules.
MATCH (sender:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(receiver:Account)
WITH receiver,
     collect(DISTINCT sender.accountId) AS senders,
     collect(t)                         AS txns
WHERE size(senders) > 3
RETURN receiver.accountId                                   AS muleAccount,
       receiver.accountType                                 AS accountType,
       size(senders)                                        AS distinctSenders,
       size(txns)                                           AS inboundTxns,
       round(reduce(s = 0.0, x IN txns | s + x.amount), 2)   AS totalInboundAmount,
       size([x IN txns WHERE x.isFraud = 1])                AS fraudulentInbound
ORDER BY fraudulentInbound DESC, distinctSenders DESC
LIMIT 20
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    default_dir = Path(__file__).resolve().parents[1] / "data" / "raw" / "neo4j"
    ap.add_argument("--csv-dir", type=Path, default=default_dir)
    ap.add_argument("--uri", default=URI)
    ap.add_argument("--keep", action="store_true", help="do not wipe before loading")
    ap.add_argument("--run-fraud-ring", action="store_true",
                    help="execute the mule detection query after loading")
    args = ap.parse_args()

    files = {
        "accounts": args.csv_dir / "neo4j_accounts_nodes.csv",
        "transactions": args.csv_dir / "neo4j_transaction_nodes.csv",
        "sent": args.csv_dir / "neo4j_sent_rels.csv",
        "received": args.csv_dir / "neo4j_received_by_rels.csv",
    }
    missing = [str(p) for p in files.values() if not p.exists()]
    if missing:
        raise SystemExit("Missing CSVs:\n  " + "\n  ".join(missing) +
                         "\nRun data/generator/generate_data.py first.")

    print(f"FinSight Neo4j loader -> {args.uri}")
    started = time.monotonic()

    with GraphDatabase.driver(args.uri, auth=(USER, PASSWORD)) as driver:
        driver.verify_connectivity()
        loader = GraphLoader(driver)

        if not args.keep:
            loader.wipe()
        loader.schema()

        print("==> loading nodes and relationships")
        loader.load_accounts(read_csv(files["accounts"]))
        loader.load_transactions(read_csv(files["transactions"]))
        loader.load_sent(read_csv(files["sent"]))
        loader.load_received(read_csv(files["received"]))
        loader.summary()

        print(f"\n==> loaded in {time.monotonic() - started:,.1f}s")

        if args.run_fraud_ring:
            print("\n==> fraud ring / money mule detection (spec 8.4)")
            rows = loader.run(FRAUD_RING_QUERY)
            if not rows:
                print("    no accounts with >3 distinct inbound senders")
                sys.exit(1)

            header = f"    {'account':<14}{'type':<10}{'senders':>8}{'inbound':>9}{'fraud':>7}{'amount':>16}"
            print(header)
            print("    " + "-" * (len(header) - 4))
            for r in rows:
                print(f"    {r['muleAccount']:<14}{r['accountType']:<10}"
                      f"{r['distinctSenders']:>8}{r['inboundTxns']:>9}"
                      f"{r['fraudulentInbound']:>7}{r['totalInboundAmount']:>16,.2f}")
            print(f"\n    {len(rows)} suspicious account(s) surfaced "
                  f"(spec requires at least 5)")
            if len(rows) < 5:
                sys.exit(1)


if __name__ == "__main__":
    main()
