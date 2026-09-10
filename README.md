# FinSight — NovaCrest Bank Data Platform

An end-to-end banking data platform built to the FinSight specification: ten
technologies spanning ingestion, distributed storage, real-time and batch
processing, multi-model databases, data blending and executive reporting.

```
Kafka → HDFS → Hive → Spark (Streaming / Core / SQL) → MongoDB + Neo4j → blending → dashboards
```

## Quick start

```bash
make setup      # venv + Python dependencies
make data       # generate the synthetic NovaCrest datasets
make offline    # produce every export without needing the cluster
make verify     # check all 35 published KPIs against the specification
make dashboards # serve the three report pages
```

That path needs no Docker and exercises the whole analytical surface. For the
real distributed stack:

```bash
make up         # bring up all 12 containers (needs ~12GB of Docker memory)
make day1       # Kafka topics + HDFS landing
make day3       # Hive tables, MongoDB import, Neo4j graph
```

`make help` lists every target.

## What is where

| Path | Contents |
|---|---|
| `infra/` | `docker-compose.yml` and the Hadoop, Hive, Spark and Connect images |
| `data/generator/` | Synthetic data generator for both scales |
| `ingestion/` | Topic creation, the Kafka producer, HDFS sink connector config |
| `spark/` | Two streaming jobs, two batch jobs, the three-mode SQL script |
| `warehouse/` | Hive DDL for the external table, view, statistics and summary mart |
| `databases/` | MongoDB import/validation and the Neo4j loader plus Cypher |
| `blending/` | Alteryx workflow substitutes in pandas |
| `dashboards/` | Three-page Streamlit app (the Power BI substitute) |
| `tools/` | KPI verification, dashboard render tests, offline export path |
| `docs/` | Architecture, runbook, Alteryx and Power BI build guides, demo script |

## Three things worth knowing up front

**HDFS on Apple Silicon.** Apache publishes no arm64 build of `apache/hadoop`
for any tag. `infra/hadoop/Dockerfile` assembles one from the binary tarball on
`eclipse-temurin:17-jre`; Hadoop is pure Java apart from optional native codecs,
so it runs natively without x86 emulation.

**The HDFS sink is dual-path.** Spec 6.3 requires Confluent's HDFS 3 Sink
Connector, which is proprietary and time-limited. It is configured in
`ingestion/connectors/hdfs3-sink.json`, and `spark/hdfs_sink.py` writes the
identical `step=N/` Parquet layout under Apache 2.0. Nothing downstream can tell
which one produced the data.

**Two dataset scales, deliberately.** The spec quotes 6.3M transactions in
section 4.1 but describes a 1,554-transaction graph in section 4.3, and every
dashboard KPI in section 10 is computed from that smaller slice. Both are
generated. The demo slice is constructed so the published figures hold exactly:

```
1,554 transactions · 499 accounts · 158 fraud (10.2%) · $312,000,000.00
134 dormant accounts (27 severe) · 10,000 customers · 2.6 avg products
```

`make verify` asserts all of them.

## Alteryx and Power BI

Both are Windows-only. `blending/` reproduces the two Alteryx workflows in
pandas and `dashboards/` reproduces the three Power BI pages in Streamlit; those
are what the pipeline runs. To build the real artefacts on Windows, follow
[docs/03_alteryx_build_guide.md](docs/03_alteryx_build_guide.md) and
[docs/04_powerbi_build_guide.md](docs/04_powerbi_build_guide.md), which map each
step onto the corresponding tool and give the DAX for every measure.

## Service endpoints

| Service | URL | Credentials |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8080 | — |
| Kafka Connect | http://localhost:8083 | — |
| HiveServer2 | http://localhost:10002 | `hive` |
| Neo4j Browser | http://localhost:7474 | `neo4j` / `finsight123` |
| MongoDB | `localhost:27017` | `finsight` / `finsight` |
| Dashboards | http://localhost:8501 | — |

## Documentation

- [Architecture](docs/01_architecture.md) — layers, data flow, design decisions
- [Runbook](docs/02_runbook.md) — day-by-day operation and troubleshooting
- [Alteryx build guide](docs/03_alteryx_build_guide.md)
- [Power BI build guide](docs/04_powerbi_build_guide.md)
- [Demo script](docs/05_demo_script.md) — the Day 5 walkthrough
# FinSight
