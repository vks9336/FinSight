-- FinSight warehouse layer (spec 8.1)
--
-- The transactions table is deliberately EXTERNAL, not managed. Dropping a
-- managed table would delete the underlying HDFS Parquet -- the same files that
-- Spark, Kafka Connect and Alteryx all read concurrently. EXTERNAL keeps the
-- data lake authoritative and the catalogue disposable.

CREATE DATABASE IF NOT EXISTS finsight
  COMMENT 'NovaCrest Bank unified analytics warehouse'
  LOCATION 'hdfs://namenode:8020/user/hive/warehouse/finsight.db';

USE finsight;

-- ---------------------------------------------------------------------------
-- Raw transaction lake
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS finsight.transactions;

CREATE EXTERNAL TABLE finsight.transactions (
    `type`            STRING  COMMENT 'PAYMENT | TRANSFER | CASH_OUT | CASH_IN | DEBIT',
    `amount`          DOUBLE  COMMENT 'Transaction amount (USD)',
    `nameOrig`        STRING  COMMENT 'Originating account ID',
    `oldbalanceOrg`   DOUBLE  COMMENT 'Sender balance before',
    `newbalanceOrig`  DOUBLE  COMMENT 'Sender balance after',
    `nameDest`        STRING  COMMENT 'Destination account ID',
    `oldbalanceDest`  DOUBLE  COMMENT 'Recipient balance before',
    `newbalanceDest`  DOUBLE  COMMENT 'Recipient balance after',
    `isFraud`         INT     COMMENT 'Ground truth fraud label',
    `isFlaggedFraud`  INT     COMMENT 'System-flagged large transfer',
    `ingestedAt`      BIGINT  COMMENT 'Producer wall-clock ingestion time (epoch ms)'
)
COMMENT 'External table over the Kafka-landed Parquet lake'
-- step is the partition column, so it is declared here rather than above; the
-- HDFS sink writes step=<N>/ directories that map onto this.
PARTITIONED BY (`step` INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/finsight/raw/transactions';

-- Partitions already exist on disk from the sink, so the metastore has to be
-- told about them. Without this the table reads as empty.
MSCK REPAIR TABLE finsight.transactions;

-- ---------------------------------------------------------------------------
-- Confirmed-fraud view (spec 8.1 R1)
--
-- One shared definition of "confirmed fraud", consumed by the Neo4j graph load
-- and the Power BI Fraud Alert Board so the two cannot drift apart.
-- ---------------------------------------------------------------------------

DROP VIEW IF EXISTS finsight.vw_fraud_transactions;

CREATE VIEW finsight.vw_fraud_transactions AS
SELECT
    step, `type`, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
FROM finsight.transactions
WHERE isFraud = 1;

-- ---------------------------------------------------------------------------
-- Statistics (spec 8.1 R2)
--
-- Required before any Spark SQL runs against the warehouse: without table
-- stats the optimiser cannot size joins and falls back to sort-merge on
-- everything, including the small dimension joins.
-- ---------------------------------------------------------------------------

ANALYZE TABLE finsight.transactions COMPUTE STATISTICS;

SHOW TABLES IN finsight;
DESCRIBE FORMATTED finsight.transactions;
