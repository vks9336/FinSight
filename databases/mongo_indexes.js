// Indexes for the customer collection (spec 8.3 R1).
//
// The Alteryx blend joins on customerId and the Power BI Customer 360 page
// filters by segment. Without a compound index covering both, each of those
// becomes a collection scan across all 10,000 documents.

const coll = db.getSiblingDB(db.getName()).customers;

coll.createIndex(
  { customerId: 1, segment: 1 },
  { name: "idx_customerId_segment", unique: false }
);

// customerId alone is the join key and must be unique; a duplicate would
// silently fan out rows in the Alteryx join.
coll.createIndex({ customerId: 1 }, { name: "idx_customerId_unique", unique: true });

// Supports the churn heatmap, which groups by segment and preferred_channel.
coll.createIndex({ segment: 1, preferred_channel: 1 }, { name: "idx_segment_channel" });

print("Indexes on " + coll.getFullName() + ":");
coll.getIndexes().forEach((i) => print("  " + i.name + "  " + JSON.stringify(i.key)));
