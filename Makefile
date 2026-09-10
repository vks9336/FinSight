# FinSight -- NovaCrest Bank data platform
#
# Day-by-day entry points mirroring the spec's 5-day execution plan:
#
#   make day1   Kafka topics, producer, HDFS landing
#   make day2   Spark streaming, batch and SQL jobs
#   make day3   Hive, MongoDB, Neo4j
#   make day4   Alteryx-substitute blending workflows
#   make day5   Dashboards and the full demo
#
# `make help` lists everything.

SHELL       := /bin/bash
PY          := .venv/bin/python
COMPOSE     := docker compose -f infra/docker-compose.yml
SPARK_EXEC  := docker exec finsight-spark-master
SUBMIT      := $(SPARK_EXEC) /opt/spark/bin/spark-submit \
                 --master spark://spark-master:7077 \
                 --py-files /opt/finsight/spark/finsight_common.py

.DEFAULT_GOAL := help
.PHONY: help setup data data-full up up-core up-ingest up-warehouse up-nosql down clean \
        topics produce produce-full connector hdfs-sink-spark \
        stream-fraud stream-churn batch-risk batch-clv \
        sql-compliance sql-customer sql-dormancy sql-all \
        hive mongo neo4j blend dashboards offline verify test \
        day1 day2 day3 day4 day5 status logs

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- setup ----

setup: ## Create the venv and install host dependencies
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

data: ## Generate the demo dataset (1,554 txns + 10k customers + Neo4j CSVs)
	$(PY) data/generator/generate_data.py --scale demo

data-full: ## Generate the 6.3M-row dataset as well
	$(PY) data/generator/generate_data.py --scale full

# ------------------------------------------------------------ containers ----

up-core: ## Start HDFS (namenode, datanode) and create the directory tree
	$(COMPOSE) --profile core up -d
	$(COMPOSE) --profile core run --rm hdfs-bootstrap

up-ingest: ## Start Kafka and Kafka Connect
	$(COMPOSE) --profile ingest up -d

up-warehouse: ## Start Postgres, Hive metastore and HiveServer2
	$(COMPOSE) --profile warehouse up -d

up-nosql: ## Start MongoDB and Neo4j
	$(COMPOSE) --profile nosql up -d

up: ## Start everything (needs ~12GB allocated to Docker)
	$(COMPOSE) --profile demo up -d
	$(COMPOSE) --profile core run --rm hdfs-bootstrap

down: ## Stop all containers
	$(COMPOSE) --profile demo down

clean: ## Stop containers and delete all volumes
	$(COMPOSE) --profile demo down -v

status: ## Show container status
	$(COMPOSE) ps

logs: ## Tail logs (make logs SVC=kafka)
	$(COMPOSE) logs -f $(SVC)

# -------------------------------------------------------------- ingestion ----

topics: ## Create txn-raw, txn-flagged and txn-churn
	bash ingestion/create_topics.sh

produce: ## Replay the demo CSV into txn-raw at ~1000 msg/s
	$(PY) ingestion/kafka_producer.py --csv data/raw/demo/transactions.csv

produce-full: ## Replay the 6.3M-row CSV (about 105 minutes)
	$(PY) ingestion/kafka_producer.py --csv data/raw/full/transactions.csv

connector: ## Register the Confluent HDFS 3 Sink connector
	bash ingestion/register_connector.sh

hdfs-sink-spark: ## Apache-licensed alternative to the HDFS sink connector
	$(SUBMIT) /opt/finsight/spark/hdfs_sink.py --once

# ----------------------------------------------------------------- spark ----

stream-fraud: ## Run the real-time fraud detection job (spec 7.1)
	$(SUBMIT) /opt/finsight/spark/streaming_fraud.py

stream-churn: ## Run the real-time churn detection job (spec 7.2)
	$(SUBMIT) /opt/finsight/spark/streaming_churn.py

batch-risk: ## Nightly composite risk scoring (spec 7.3)
	$(SUBMIT) /opt/finsight/spark/batch_risk_scoring.py

batch-clv: ## Customer lifetime value scoring (spec 7.4)
	$(SUBMIT) /opt/finsight/spark/batch_clv.py

sql-compliance: ## Weekly compliance summary (spec 7.5)
	$(SUBMIT) /opt/finsight/spark/spark_sql_jobs.py --mode compliance

sql-customer: ## Customer fraud summary (spec 7.5 R1)
	$(SUBMIT) /opt/finsight/spark/spark_sql_jobs.py --mode customer

sql-dormancy: ## Account dormancy report (spec 7.6)
	$(SUBMIT) /opt/finsight/spark/spark_sql_jobs.py --mode dormancy

sql-all: ## All three Spark SQL modes
	$(SUBMIT) /opt/finsight/spark/spark_sql_jobs.py --mode all

# ------------------------------------------------------------- databases ----

hive: ## Create the external table, view, stats and summary mart
	bash warehouse/apply.sh

mongo: ## Import customers, build indexes, validate the load
	bash databases/mongo_import.sh

neo4j: ## Load the fraud graph and run the money mule query
	$(PY) databases/neo4j_loader.py --run-fraud-ring

# -------------------------------------------------------------- blending ----

blend: ## Run both Alteryx-substitute workflows
	cd blending && ../$(PY) customer_risk_blend.py
	cd blending && ../$(PY) transaction_summary.py

offline: ## Produce all exports from CSV without the cluster (spec fallback)
	$(PY) tools/offline_exports.py

# ------------------------------------------------------------ dashboards ----

dashboards: ## Serve the three report pages
	$(PY) -m streamlit run dashboards/app.py

test: ## Render every dashboard page headlessly and fail on errors
	$(PY) tools/test_dashboards.py

verify: ## Check every published KPI against the specification
	$(PY) tools/verify_kpis.py

# ------------------------------------------------------------------ days ----

day1: up-core up-ingest topics ## Day 1: Kafka to HDFS pipeline
	@echo "Now run 'make produce' in one shell and 'make connector' in another."

day2: stream-fraud ## Day 2: Spark jobs
	@echo "Run stream-churn, batch-risk, batch-clv and sql-all as separate targets."

day3: up-warehouse up-nosql hive mongo neo4j ## Day 3: Hive, MongoDB, Neo4j

day4: blend ## Day 4: Alteryx blending workflows

day5: verify dashboards ## Day 5: verify KPIs then serve the dashboards
