# Power BI Build Guide

Power BI Desktop is Windows-only, so the three report pages are implemented as a
Streamlit app (`dashboards/`) which is what the pipeline actually serves. This
guide rebuilds the same three pages as a `.pbix` on Windows.

Target: one `FinSight.pbix` containing three report pages (spec section 10).

## Import the data

Copy `exports/` to the Windows machine and use **Get Data → Text/CSV** (or Excel
for the first one):

| Query name | File | Rows |
|---|---|---|
| `CustomerRisk` | `customer_risk_blend.xlsx`, sheet `customer_risk` | 10,000 |
| `SegmentRollup` | `customer_risk_blend.xlsx`, sheet `segment_rollup` | 5 |
| `TransactionSummary` | `transaction_summary.csv` | 591 |
| `Compliance` | `compliance_summary.csv` | 5 |
| `Dormancy` | `dormancy_report.csv` | 134 |
| `Flagged` | `flagged_transactions.csv` | 159 |

### Power Query steps

Applies to every query:

1. **Use First Row as Headers**
2. Set types explicitly — Power BI guesses `customerId` as a number on some
   locales, which then fails to join against the text keys in the other tables.
3. On `Flagged` and `TransactionSummary`, add a `Day` column:
   `Number.RoundDown(([step] - 1) / 24) + 1`

### Model relationships

Set these in **Model view**. All are single-direction, many-to-one:

```
Flagged[nameOrig]            *--1  CustomerRisk[customerId]
Dormancy[customerId]         *--1  CustomerRisk[customerId]
TransactionSummary[transaction_type] *--1 Compliance[transaction_type]
```

Mark `CustomerRisk` as the dimension side. Leave cross-filtering single unless a
visual specifically needs it — bidirectional filters on a 10,000-row dimension
will make the scatter plot sluggish.

---

## Page 1 — Fraud Alert Board

Audience: Fraud & Risk Team. Sources: `Flagged`, `TransactionSummary`.

### Measures

```dax
Total Flagged = COUNTROWS('Flagged')

Flagged Today =
VAR LatestDay = MAX('Flagged'[Day])
RETURN CALCULATE([Total Flagged], 'Flagged'[Day] = LatestDay)

Flagged Yesterday =
VAR LatestDay = MAX('Flagged'[Day])
RETURN CALCULATE([Total Flagged], 'Flagged'[Day] = LatestDay - 1)

Flagged DoD % =
DIVIDE([Flagged Today] - [Flagged Yesterday], [Flagged Yesterday])

Fraud Rate % =
DIVIDE(SUM('TransactionSummary'[fraud_count]),
       SUM('TransactionSummary'[txn_count])) * 100

Flagged Value Today =
VAR LatestDay = MAX('Flagged'[Day])
RETURN CALCULATE(SUM('Flagged'[amount]), 'Flagged'[Day] = LatestDay)

Flagged Value Daily Avg =
AVERAGEX(VALUES('Flagged'[Day]), CALCULATE(SUM('Flagged'[amount])))

-- KPI 4: a flagged record with isFraud = 0 is a false positive.
False Positives = CALCULATE(COUNTROWS('Flagged'), 'Flagged'[isFraud] = 0)

False Positive Rate % = DIVIDE([False Positives], [Total Flagged]) * 100

Legacy FP Baseline = 62.0

FP Improvement pp = [False Positive Rate %] - [Legacy FP Baseline]
```

### Visuals

| Visual | Type | Configuration |
|---|---|---|
| KPI 1 | Card | `[Flagged Today]`, subtitle `[Flagged DoD %]` formatted as percent |
| KPI 2 | Card | `[Fraud Rate %]`, one decimal |
| KPI 3 | Card | `[Flagged Value Today]`, custom format `$#,##0.0,,"M"` |
| KPI 4 | Card | `[False Positive Rate %]`, subtitle `[FP Improvement pp]` |
| Daily Fraud Volume Trend | Line chart | Axis `step`, Values `[Total Flagged]`, Legend `type` |
| Fraud Count by Type | Clustered bar | Axis `type`, Values `[Total Flagged]` and `SUM(isFraud)` |
| Live Flagged Feed | Table | `step`, `type`, `amount`, `nameOrig`, `nameDest`, `newbalanceDest`, `isFraud`, `fraudReason` |

Add a slicer on `isFraud` labelled "Confirmation status".

For the live-feed behaviour, set **File → Options → Data Load → Auto page
refresh** to 30 seconds. True streaming requires a Power BI streaming dataset
pushed from the `txn-flagged` topic, which is outside the scope of a `.pbix`
file; the Streamlit page reads the topic directly if you need that in the demo.

### Expected values

```
TOTAL FLAGGED    7        -74% vs yday    (final day of the window)
FRAUD RATE       10.2%
TOTAL VALUE      $3.8M
FALSE POSITIVE   0.6%     -61.4pp vs legacy
```

> The spec's sample layout shows 158 flagged and a 14.3% false positive rate.
> 158 is the *whole-window* flagged count, not the final day's; the card above
> is day-scoped, matching the "current day" wording of KPI 1. Use
> `[Total Flagged]` instead of `[Flagged Today]` if you want the 158 figure on
> the card. The false positive rate is computed from the data rather than
> asserted, and the rule turns out to be sharper than the spec's estimate:
> 159 flagged, 158 confirmed, 1 false positive.

---

## Page 2 — Customer 360

Audience: Relationship Managers. Source: `CustomerRisk`, `SegmentRollup`.

### Measures

```dax
Total Customers = COUNTROWS('CustomerRisk')

Active Customers =
CALCULATE(COUNTROWS('CustomerRisk'), 'CustomerRisk'[kyc_status] = "verified")

Avg Risk Score = AVERAGE('CustomerRisk'[risk_score])

Avg Churn Probability = AVERAGE('CustomerRisk'[churn_probability])

Avg Churn % = [Avg Churn Probability] * 100

Avg Products Held = AVERAGE('CustomerRisk'[product_count])

Avg Composite Risk = AVERAGE('CustomerRisk'[composite_risk_score])
```

### Visuals

| Visual | Type | Configuration |
|---|---|---|
| KPI row | 4 Cards | `[Total Customers]` (subtitle `[Active Customers]`), `[Avg Risk Score]`, `[Avg Churn %]`, `[Avg Products Held]` |
| Risk vs Churn | Scatter | X `risk_score`, Y `churn_probability`, Legend `segment`, Size `product_count`, Details `customerId` |
| Segment Distribution | Clustered bar | Axis `segment`, Values `[Total Customers]` |
| Churn Heatmap | Matrix | Rows `segment`, Columns `preferred_channel`, Values `[Avg Churn Probability]`, conditional background formatting red-to-green reversed |
| CLV Tier Breakdown | Donut | Legend `clv_tier`, Values `[Total Customers]` |
| Outreach list | Table | Top 50 by `composite_risk_score` |

On the scatter, set **Analytics → Average line** on both axes to reproduce the
quadrant guides. Cap the visual at 10,000 points under **Format → General →
Data volume**, otherwise Power BI samples and the high-risk cluster thins out.

Slicers: `segment`, `preferred_channel`, `kyc_status`.

### Expected values

```
TOTAL CUSTOMERS     10,000    Active: 9,240
AVG RISK SCORE      0.28
AVG CHURN PROB      22.4%
AVG PRODUCTS HELD   2.6
```

Segment counts: Standard 4,000 · Basic 3,000 · Premium 1,800 · Student 900 ·
Private Banking 300.

---

## Page 3 — Risk & Compliance Report

Audience: Compliance Officers. Sources: `Compliance`, `TransactionSummary`,
`Dormancy`.

### Measures

```dax
Total Transactions = SUM('Compliance'[txn_count])

Total Volume = SUM('Compliance'[total_volume])

Confirmed Fraud = SUM('Compliance'[fraud_count])

Compliance Fraud Rate % = DIVIDE([Confirmed Fraud], [Total Transactions]) * 100

Fraud Rate Target = 8.0

Dormant Accounts = COUNTROWS('Dormancy')

Severely Dormant =
CALCULATE(COUNTROWS('Dormancy'),
          'Dormancy'[dormancy_severity] = "Severely Dormant")

-- Per-step rates on a 1,554-row slice are noisy; the rolling mean is what the
-- trend line should be read against.
Fraud Rate Rolling 12 =
AVERAGEX(
    WINDOW(-11, REL, 0, REL,
           SUMMARIZE('TransactionSummary', 'TransactionSummary'[step]),
           ORDERBY('TransactionSummary'[step], ASC)),
    CALCULATE(DIVIDE(SUM('TransactionSummary'[fraud_count]),
                     SUM('TransactionSummary'[txn_count])) * 100)
)
```

### Visuals

| Visual | Type | Configuration |
|---|---|---|
| KPI row | 4 Cards | `[Total Transactions]`, `[Total Volume]` (`$#,##0,,"M"`), `[Confirmed Fraud]` (subtitle `[Compliance Fraud Rate %]`), `[Dormant Accounts]` (subtitle `[Severely Dormant]`) |
| Fraud Rate Trend | Line chart | Axis `step`, Values `[Compliance Fraud Rate %]` and `[Fraud Rate Rolling 12]`; add a constant line at `[Fraud Rate Target]` under Analytics |
| Volume by Type | Clustered bar | Axis `transaction_type`, Values `[Total Volume]`, data colours conditional on `fraud_rate_pct` |
| Dormancy Breakdown | Clustered column | Axis `dormancy_severity`, Values `[Dormant Accounts]` |
| Escalation worklist | Table | Top 25 `Dormancy` rows by `steps_inactive` |
| Compliance audit table | Table | All `Compliance` columns |

Add a **transaction type slicer** at the top of the page — spec section 10 calls
for compliance officers to drill into specific categories during regulatory
review.

### Expected values

```
TOTAL TRANSACTIONS   1,554     Steps 1-168 (7-day)
TOTAL VOLUME         $312M     All types
CONFIRMED FRAUD      158       Fraud rate: 10.2%
DORMANT ACCOUNTS     134       27 severely dormant
```

Audit table:

| Type | Count | Volume | Fraud | Fraud rate | Risk |
|---|---|---|---|---|---|
| CASH_OUT | 652 | $174,743,453.55 | 85 | 13.04% | HIGH |
| TRANSFER | 331 | $109,987,005.31 | 73 | 22.05% | HIGH |
| CASH_IN | 129 | $15,214,371.72 | 0 | 0.00% | LOW |
| PAYMENT | 371 | $11,388,923.25 | 0 | 0.00% | LOW |
| DEBIT | 71 | $666,246.17 | 0 | 0.00% | LOW |

---

## Theme

Save as `finsight-theme.json` and apply via **View → Themes → Browse**, to match
the Streamlit palette:

```json
{
  "name": "FinSight",
  "dataColors": ["#2563eb", "#dc2626", "#f59e0b", "#16a34a", "#8b5cf6", "#0891b2"],
  "background": "#ffffff",
  "foreground": "#0f172a",
  "tableAccent": "#2563eb"
}
```

## Checklist before delivery

- [ ] Three pages, named Fraud Alert Board, Customer 360, Risk & Compliance Report
- [ ] Every KPI matches the expected values above
- [ ] Transaction type slicer present on page 3
- [ ] Relationships single-direction, `CustomerRisk` on the one side
- [ ] Auto page refresh enabled on page 1
- [ ] Saved as `FinSight.pbix`
