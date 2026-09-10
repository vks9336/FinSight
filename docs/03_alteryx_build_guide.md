# Alteryx Build Guide

Alteryx Designer is Windows-only, so `blending/customer_risk_blend.py` and
`blending/transaction_summary.py` implement the two workflows in pandas and are
what the pipeline actually runs. This guide maps each of those steps onto the
Designer canvas so the `.yxmd` files can be rebuilt on a Windows machine and
produce identical output.

Verify a rebuild by diffing against the committed reference output:

```
exports/customer_risk_blend.csv     10,000 rows x 25 columns
exports/transaction_summary.csv        591 rows x 10 columns
```

## Before you start

Copy these files to the Windows machine. All are produced by the Spark jobs and
land in `exports/` (or on HDFS at `/finsight/exports/`).

| File | Produced by | Spec |
|---|---|---|
| `customer_fraud_summary.csv` | `spark_sql_jobs.py --mode customer` | 7.5 R1 |
| `dormancy_report.csv` | `spark_sql_jobs.py --mode dormancy` | 7.6 |
| `clv_scores.csv` | `batch_clv.py` | 7.4 |
| `daily_summary.csv` | `batch_risk_scoring.py` | 7.3 R2 |
| `novacrest_customers.json` | MongoDB export | 8.3 |

For the MongoDB input, either point the Input Data tool at the JSON file or use
the MongoDB ODBC driver against `novacrest.customers`.

---

## Workflow 1 — Customer Risk Blend

Save as `blending/customer_risk_blend.yxmd`.

### Canvas layout

```
[Input: novacrest_customers.json] ─┐
                                   ├─[Join: customerId]─┐
[Input: customer_fraud_summary]  ──┘  (Left outer)      │
                                                         ├─[Join: customerId]─┐
[Input: clv_scores.csv]  ────────────────────────────────┘  (Left outer)      │
                                                                               ├─[Join]─[Data Cleansing]─[Formula]─[Select]─[Sort]─[Output: .xlsx]
[Input: dormancy_report.csv] ──────────────────────────────────────────────────┘
```

### Tool-by-tool

**1. Input Data — customer profiles**
File `novacrest_customers.json`, format JSON Lines. This is the spine of the
workflow: all 10,000 customers must survive to the output.

**2. Input Data — fraud summary**
File `customer_fraud_summary.csv`. 399 rows, one per account that originated a
transaction.

**3. Join — profiles to fraud summary**
Join on `customerId = customerId`. Take the **L** and **J** outputs and union
them, or use a Join tool configured as a left outer join.

> This is the step most likely to be built wrong. An inner join looks correct in
> the preview but drops every customer with no transaction history — 9,601 of
> the 10,000. The Customer 360 segment counts would then be wrong by 96% and the
> "10,000 total customers" KPI would read 399.

**4. Join — attach CLV scores**
Left outer on `customerId`. Brings in `clv_score` and `clv_tier`.

**5. Join — attach dormancy**
Left outer on `customerId`. Brings in `dormancy_severity` and `steps_inactive`.

**6. Data Cleansing**
Replace nulls with 0 for: `txn_count`, `total_amount`, `avg_amount`,
`fraud_count`, `fraud_rate_pct`, `unique_destinations`, `last_active_step`,
`clv_score`, `steps_inactive`.

Then a Formula tool for the string defaults:

```
clv_tier          = IIF(IsNull([clv_tier]), "No Activity", [clv_tier])
dormancy_severity = IIF(IsNull([dormancy_severity]), "Active", [dormancy_severity])
is_dormant        = IIF([dormancy_severity] != "Active", 1, 0)
```

**7. Formula — composite risk score (spec 9.1)**

```
composite_risk_score = ([fraud_rate_pct] * 0.6) + ([churn_probability] * 0.4)
```

```
risk_band = IF [composite_risk_score] > 6.0 THEN "High"
            ELSEIF [composite_risk_score] > 2.0 THEN "Medium"
            ELSE "Low" ENDIF
```

> `fraud_rate_pct` runs 0–100 while `churn_probability` runs 0–1, so the
> weighted sum is dominated by the fraud term and is not itself a 0–1 score.
> That is what the spec asks for, so it is reproduced as written; `risk_band`
> is what the dashboard classifies on.

**8. Formula — product fields**

```
product_count = Length(Replace([products], ",", "")) > 0
                ? CountWords([products]) : 0
products      = ToString([products])
```

If the JSON `products` array comes in as a nested field, add a JSON Parse tool
before this and re-aggregate with a Summarize tool concatenating on `customerId`.

**9. Select**
Keep, in order: `customerId`, `name`, `age`, `segment`, `kyc_status`,
`preferred_channel`, `product_count`, `products`, `account_opened`, `txn_count`,
`total_amount`, `avg_amount`, `fraud_count`, `fraud_rate_pct`,
`unique_destinations`, `last_active_step`, `risk_score`, `churn_probability`,
`composite_risk_score`, `risk_band`, `clv_score`, `clv_tier`,
`dormancy_severity`, `steps_inactive`, `is_dormant`.

**10. Sort** — `composite_risk_score` descending.

**11. Output Data** — `exports/customer_risk_blend.xlsx`, sheet `customer_risk`.

**12. Second output — segment rollup**
Branch off the Select tool into a Summarize tool grouping by `segment`:

| Action | Field | Output name |
|---|---|---|
| Count | `customerId` | `customers` |
| Average | `risk_score` | `avg_risk_score` |
| Average | `churn_probability` | `avg_churn_probability` |
| Average | `composite_risk_score` | `avg_composite_risk` |
| Average | `clv_score` | `avg_clv` |
| Sum | `fraud_count` | `total_fraud` |
| Sum | `is_dormant` | `dormant_accounts` |

Output to the same workbook, sheet `segment_rollup`.

### Expected result

| segment | customers | avg_risk_score | total_fraud | dormant_accounts |
|---|---|---|---|---|
| Standard | 4,000 | 0.2754 | 66 | 53 |
| Basic | 3,000 | 0.3353 | 45 | 39 |
| Premium | 1,800 | 0.1719 | 33 | 29 |
| Student | 900 | 0.3820 | 9 | 8 |
| Private Banking | 300 | 0.1315 | 5 | 5 |

Totals: 10,000 customers, 158 fraud, 134 dormant.

---

## Workflow 2 — Transaction Summary

Save as `blending/transaction_summary.yxmd`.

### Canvas layout

```
[Input: Hive txn_summary_mart]─[Text to Columns]─[Formula]─[Summarize]─[Filter]─[Formula]─[Sort]─[Output: .csv]
```

### Tool-by-tool

**1. Input Data — Hive**
Per spec 8.2 R2 the source is the Hive mart, not the raw CSV export, so that
Alteryx and Power BI share metric definitions.

Configure a DSN with the Cloudera or Hortonworks Hive ODBC driver:

```
Host      localhost        (or the Docker host)
Port      10000
Database  finsight
Auth      User Name, user "hive", no password
```

Query: `SELECT * FROM finsight.txn_summary_mart`

If the ODBC driver is unavailable, fall back to `daily_summary.csv`, which is
already grouped by type and step — skip to step 5.

**2. Text to Columns**
The mart stores `txn_types` as a comma-separated string. Split field
`txn_types`, delimiter `,`, method **Split to rows**, output field
`transaction_type`.

**3. Formula — attribute metrics across the split types**
Splitting to rows duplicates each customer-step row once per type, so the
metrics have to be divided or the totals inflate:

```
type_count         = CountWords(Replace([txn_types], ",", " "))
attributed_amount  = [total_amount] / [type_count]
attributed_txns    = [txn_count]    / [type_count]
attributed_fraud   = [fraud_count]  / [type_count]
transaction_type   = Trim([transaction_type])
```

**4. Summarize** — group by `transaction_type` and `step`:

| Action | Field | Output name |
|---|---|---|
| Sum | `attributed_amount` | `total_volume` |
| Sum | `attributed_txns` | `txn_count` |
| Average | `avg_amount` | `avg_amount` |
| Max | `max_amount` | `max_amount` |
| Sum | `attributed_fraud` | `fraud_count` |
| CountDistinct | `customerId` | `customers` |

**5. Filter (spec 9.2)** — `[step] >= 1 AND [step] <= 168`. Take the **T** output.

**6. Formula — rates and rounding**

```
fraud_rate_pct = IIF([txn_count] = 0, 0, 100.0 * [fraud_count] / [txn_count])
total_volume   = Round([total_volume], 0.01)
avg_amount     = Round([avg_amount], 0.01)
fraud_rate_pct = Round([fraud_rate_pct], 0.01)
txn_count      = Round([txn_count], 1)
fraud_count    = Round([fraud_count], 1)
```

**7. Sort** — `step` ascending, then `transaction_type` ascending.

**8. Output Data** — `exports/transaction_summary.csv`, headers on.

### Expected result

591 rows. Rollup by type:

| transaction_type | txn_count | total_volume | fraud_count | fraud_rate_pct |
|---|---|---|---|---|
| CASH_OUT | 652 | 174,743,453.55 | 85 | 13.04 |
| TRANSFER | 331 | 109,987,005.31 | 73 | 22.05 |
| CASH_IN | 129 | 15,214,371.72 | 0 | 0.00 |
| PAYMENT | 371 | 11,388,923.25 | 0 | 0.00 |
| DEBIT | 71 | 666,246.17 | 0 | 0.00 |

Grand total volume must be exactly $312,000,000.00 with 158 fraud transactions.
If your total differs, the attribution in step 3 is the first place to look.
