// FinSight fraud graph queries (spec 8.4)
//
// Run in Neo4j Browser at http://localhost:7474 (neo4j / finsight123), or:
//   docker exec -i finsight-neo4j cypher-shell -u neo4j -p finsight123 \
//     < databases/cypher/fraud_ring.cypher

// ---------------------------------------------------------------------------
// Q1 -- Money mule detection (the primary spec 8.4 query)
//
// Accounts receiving from more than three distinct senders. This fan-in shape
// is the classic structuring signal and cannot be seen in a per-transaction
// tabular rule, which is the whole argument for holding this data as a graph.
// ---------------------------------------------------------------------------
MATCH (sender:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(receiver:Account)
WITH receiver,
     collect(DISTINCT sender.accountId) AS senders,
     collect(t)                         AS txns
WHERE size(senders) > 3
RETURN receiver.accountId                                  AS muleAccount,
       receiver.accountType                                AS accountType,
       size(senders)                                       AS distinctSenders,
       size(txns)                                          AS inboundTxns,
       round(reduce(s = 0.0, x IN txns | s + x.amount), 2)  AS totalInboundAmount,
       size([x IN txns WHERE x.isFraud = 1])               AS fraudulentInbound
ORDER BY fraudulentInbound DESC, distinctSenders DESC
LIMIT 20;

// ---------------------------------------------------------------------------
// Q2 -- Fraud rings: mules that share senders with each other
//
// Two mule accounts fed by the same set of senders are far more likely to be
// one coordinated ring than two unrelated incidents.
// ---------------------------------------------------------------------------
MATCH (s:Account)-[:SENT]->(t1:Transaction)-[:RECEIVED_BY]->(m1:Account)
MATCH (s)-[:SENT]->(t2:Transaction)-[:RECEIVED_BY]->(m2:Account)
WHERE m1.accountId < m2.accountId
  AND (t1.isFraud = 1 OR t2.isFraud = 1)
WITH m1, m2, collect(DISTINCT s.accountId) AS sharedSenders
WHERE size(sharedSenders) >= 2
RETURN m1.accountId    AS mule1,
       m2.accountId    AS mule2,
       size(sharedSenders) AS sharedSenderCount,
       sharedSenders[0..5] AS sampleSenders
ORDER BY sharedSenderCount DESC
LIMIT 15;

// ---------------------------------------------------------------------------
// Q3 -- Highest-value confirmed fraud paths, end to end
// ---------------------------------------------------------------------------
MATCH (orig:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(dest:Account)
WHERE t.isFraud = 1
RETURN orig.accountId  AS fromAccount,
       t.txnId         AS transaction,
       t.type          AS transactionType,
       t.amount        AS amount,
       t.step          AS step,
       dest.accountId  AS toAccount,
       dest.accountType AS destType
ORDER BY t.amount DESC
LIMIT 20;

// ---------------------------------------------------------------------------
// Q4 -- Two-hop layering: funds moving on through a mule to a second account
// ---------------------------------------------------------------------------
MATCH path = (a:Account)-[:SENT]->(:Transaction)-[:RECEIVED_BY]->
             (mule:Account)-[:SENT]->(:Transaction)-[:RECEIVED_BY]->(c:Account)
WHERE a.accountId <> c.accountId
WITH mule, count(DISTINCT a) AS inboundSources, count(DISTINCT c) AS onwardTargets
WHERE inboundSources > 2 AND onwardTargets > 1
RETURN mule.accountId AS layeringAccount,
       inboundSources,
       onwardTargets
ORDER BY inboundSources DESC, onwardTargets DESC
LIMIT 15;

// ---------------------------------------------------------------------------
// Q5 -- Graph inventory, to confirm the load matches spec 4.3
// ---------------------------------------------------------------------------
MATCH (a:Account)
WITH count(a) AS accounts
MATCH (t:Transaction)
WITH accounts, count(t) AS transactions, sum(t.isFraud) AS fraudTransactions
MATCH ()-[s:SENT]->()
WITH accounts, transactions, fraudTransactions, count(s) AS sentEdges
MATCH ()-[r:RECEIVED_BY]->()
RETURN accounts, transactions, fraudTransactions,
       sentEdges, count(r) AS receivedEdges,
       sentEdges + count(r) AS totalEdges;
