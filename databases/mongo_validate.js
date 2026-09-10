// Import validation (spec 8.3 R2).
//
// Groups by segment and checks the distribution against the specification
// before the Day 4 Alteryx workflow is allowed to run. Exits non-zero on a
// mismatch so the Makefile halts rather than blending a bad load.

const coll = db.getSiblingDB(db.getName()).customers;

const EXPECTED = {
  Standard: 4000,
  Basic: 3000,
  Premium: 1800,
  Student: 900,
  "Private Banking": 300,
};
const EXPECTED_TOTAL = 10000;

const rows = coll
  .aggregate([
    { $group: { _id: "$segment", count: { $sum: 1 }, avgRisk: { $avg: "$risk_score" }, avgChurn: { $avg: "$churn_probability" } } },
    { $sort: { count: -1 } },
  ])
  .toArray();

print("");
print("Segment distribution");
print("  " + "segment".padEnd(18) + "count".padStart(8) + "expected".padStart(10) + "avg risk".padStart(11) + "avg churn".padStart(11));

let ok = true;
let total = 0;
for (const r of rows) {
  const expected = EXPECTED[r._id];
  const match = expected === r.count;
  if (!match) ok = false;
  total += r.count;
  print(
    "  " +
      String(r._id).padEnd(18) +
      String(r.count).padStart(8) +
      String(expected === undefined ? "-" : expected).padStart(10) +
      r.avgRisk.toFixed(3).padStart(11) +
      r.avgChurn.toFixed(3).padStart(11) +
      (match ? "" : "   <-- MISMATCH")
  );
}

const overall = coll
  .aggregate([
    { $group: { _id: null, avgRisk: { $avg: "$risk_score" }, avgChurn: { $avg: "$churn_probability" }, avgProducts: { $avg: { $size: "$products" } } } },
  ])
  .toArray()[0];

const verified = coll.countDocuments({ kyc_status: "verified" });

print("");
print("Portfolio KPIs (Power BI Customer 360)");
print("  total customers   " + String(total).padStart(8) + "   expected " + EXPECTED_TOTAL);
print("  KYC verified      " + String(verified).padStart(8) + "   expected 9240");
print("  avg risk score    " + overall.avgRisk.toFixed(3).padStart(8) + "   expected 0.280");
print("  avg churn prob    " + overall.avgChurn.toFixed(3).padStart(8) + "   expected 0.224");
print("  avg products held " + overall.avgProducts.toFixed(2).padStart(8) + "   expected 2.60");

if (total !== EXPECTED_TOTAL) ok = false;

print("");
if (ok) {
  print("VALIDATION PASSED -- safe to run the Alteryx workflow");
} else {
  print("VALIDATION FAILED -- re-run data/generator/generate_data.py and re-import");
  quit(1);
}
