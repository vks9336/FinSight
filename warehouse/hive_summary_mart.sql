-- Transaction summary mart (spec 8.2)
--
-- A curated layer over finsight.transactions holding one row per customer per
-- step, so the aggregations that Spark SQL, Alteryx and Power BI each re-run
-- during the demo are computed once rather than on every query.
--
-- Unlike the raw table this one is MANAGED: it is derived data, and dropping it
-- should reclaim the space. Refresh it whenever the Spark Core batch job
-- completes.

USE finsight;

DROP TABLE IF EXISTS finsight.txn_summary_mart;

CREATE TABLE finsight.txn_summary_mart (
    `customerId`    STRING  COMMENT 'nameOrig; join key to MongoDB and the CLV output',
    `step`          INT     COMMENT 'Time step of the aggregated window',
    `txn_count`     BIGINT  COMMENT 'Transactions initiated in this step',
    `total_amount`  DOUBLE  COMMENT 'Sum of transaction amounts',
    `avg_amount`    DOUBLE  COMMENT 'Mean transaction amount',
    `max_amount`    DOUBLE  COMMENT 'Largest single transaction; outlier detection in Power BI',
    `fraud_count`   BIGINT  COMMENT 'Transactions where isFraud = 1',
    `txn_types`     STRING  COMMENT 'Comma-separated distinct transaction types',
    `last_balance`  DOUBLE  COMMENT 'newbalanceOrig of the last transaction in the step'
)
COMMENT 'Pre-aggregated one-row-per-customer-per-step mart'
STORED AS PARQUET
-- Explicitly non-transactional: Hive 4 would otherwise create this as an ACID
-- table, which Spark cannot read, and Spark is the primary consumer here.
TBLPROPERTIES ('transactional' = 'false');

-- ---------------------------------------------------------------------------
-- Populate
--
-- last_balance needs the newbalanceOrig of the *latest* transaction within each
-- customer/step group, which no plain aggregate expresses. The inner query
-- ranks each group so the outer one can pick out row 1.
-- ---------------------------------------------------------------------------

INSERT OVERWRITE TABLE finsight.txn_summary_mart
SELECT
    t.nameOrig                                             AS customerId,
    t.step                                                 AS step,
    COUNT(*)                                               AS txn_count,
    ROUND(SUM(t.amount), 2)                                AS total_amount,
    ROUND(AVG(t.amount), 2)                                AS avg_amount,
    ROUND(MAX(t.amount), 2)                                AS max_amount,
    SUM(t.isFraud)                                         AS fraud_count,
    CONCAT_WS(',', COLLECT_SET(t.`type`))                  AS txn_types,
    MAX(CASE WHEN t.rn = 1 THEN t.newbalanceOrig END)      AS last_balance
FROM (
    SELECT
        nameOrig, step, amount, `type`, isFraud, newbalanceOrig,
        ROW_NUMBER() OVER (
            PARTITION BY nameOrig, step
            ORDER BY ingestedAt DESC, amount DESC
        ) AS rn
    FROM finsight.transactions
) t
GROUP BY t.nameOrig, t.step;

-- ---------------------------------------------------------------------------
-- Column-level statistics (spec 8.2 R1)
--
-- Table-level stats are not enough here: the compliance and dormancy queries
-- both range-scan `step`, and only column stats give the optimiser the
-- histogram it needs to estimate those predicates.
-- ---------------------------------------------------------------------------

ANALYZE TABLE finsight.txn_summary_mart COMPUTE STATISTICS FOR COLUMNS;

SELECT
    COUNT(*)                    AS mart_rows,
    COUNT(DISTINCT customerId)  AS customers,
    MIN(step)                   AS min_step,
    MAX(step)                   AS max_step,
    SUM(fraud_count)            AS total_fraud
FROM finsight.txn_summary_mart;
