// ═══════════════════════════════════════════════════════════════
// SAP O2C — Advanced Analytical Queries
// Ready for Graph-RAG / LLM-generated Cypher
// ═══════════════════════════════════════════════════════════════


// ───────────────────────────────────────────────────────────────
// QUERY 1: Revenue by Product (Excluding Cancelled Invoices)
// Guardrail: filters billingDocumentIsCancelled + groups by currency
// ───────────────────────────────────────────────────────────────

MATCH (bd:BillingDocument)-[r:INVOICED_PRODUCT]->(p:Product)
WHERE bd.billingDocumentIsCancelled = false
  AND bd.billingDocumentType = 'F2'
RETURN
  p.material       AS productCode,
  p.description    AS productName,
  r.currency       AS currency,
  SUM(r.amount)    AS totalRevenue,
  SUM(r.qty)       AS totalQuantitySold,
  COUNT(DISTINCT bd.billingDocument) AS invoiceCount
ORDER BY totalRevenue DESC;


// ───────────────────────────────────────────────────────────────
// QUERY 2: Lead-Time Analysis (Sales Order → Billing)
// Traverses: ORDER → DELIVERY → BILLING
// ───────────────────────────────────────────────────────────────

MATCH (order:SalesDocument {sdDocumentCategory: 'ORDER'})
      -[:FULFILLED_BY]->(delivery:SalesDocument {sdDocumentCategory: 'DELIVERY'})
      -[:GENERATED_BILL]->(bd:BillingDocument)
WHERE bd.billingDocumentIsCancelled = false
  AND order.creationDate IS NOT NULL
  AND bd.billingDocumentDate IS NOT NULL
RETURN
  order.soldToParty AS customer,
  COUNT(*) AS documentPairs,
  AVG(duration.inDays(order.creationDate, bd.billingDocumentDate).days) AS avgLeadTimeDays,
  MIN(duration.inDays(order.creationDate, bd.billingDocumentDate).days) AS minLeadTimeDays,
  MAX(duration.inDays(order.creationDate, bd.billingDocumentDate).days) AS maxLeadTimeDays
ORDER BY avgLeadTimeDays DESC;


// ───────────────────────────────────────────────────────────────
// QUERY 3: Full Flow Trace (From a Sales Order to Products)
// Replace '740509' with your target sales order ID
// ───────────────────────────────────────────────────────────────

MATCH path = (c:Customer)-[:PLACED_ORDER]->(order:SalesDocument {documentId: '740509'})
              -[:FULFILLED_BY]->(delivery:SalesDocument)
              -[:GENERATED_BILL]->(bd:BillingDocument)
              -[r:INVOICED_PRODUCT]->(p:Product)
RETURN
  c.name                          AS customerName,
  order.documentId                AS salesOrder,
  order.creationDate              AS orderDate,
  delivery.documentId             AS deliveryNote,
  delivery.creationDate           AS deliveryDate,
  bd.billingDocument              AS invoice,
  bd.billingDocumentDate          AS invoiceDate,
  bd.billingDocumentIsCancelled   AS isCancelled,
  r.item                          AS lineItem,
  p.material                      AS productCode,
  p.description                   AS productName,
  r.qty                           AS quantity,
  r.amount                        AS lineAmount,
  r.currency                      AS currency;


// ───────────────────────────────────────────────────────────────
// QUERY 4: Cancellation Audit Report
// Shows original invoices, their reversal S1 docs, and time delta
// ───────────────────────────────────────────────────────────────

MATCH (reversal:BillingDocument {billingDocumentType: 'S1'})
      -[:REVERSED_BY]->(original:BillingDocument)
RETURN
  original.billingDocument       AS originalInvoice,
  original.billingDocumentDate   AS originalDate,
  original.totalNetAmount        AS originalAmount,
  reversal.billingDocument       AS reversalDocument,
  reversal.creationDate          AS reversalDate,
  reversal.totalNetAmount        AS reversalAmount,
  original.transactionCurrency   AS currency,
  original.soldToParty           AS customer,
  duration.inDays(original.billingDocumentDate, reversal.creationDate).days AS daysToCancel
ORDER BY daysToCancel DESC;


// ───────────────────────────────────────────────────────────────
// QUERY 5: Customer Portfolio Summary (360° View)
// ───────────────────────────────────────────────────────────────

MATCH (c:Customer)-[:PLACED_ORDER]->(order:SalesDocument {sdDocumentCategory: 'ORDER'})
OPTIONAL MATCH (order)-[:FULFILLED_BY]->(delivery:SalesDocument {sdDocumentCategory: 'DELIVERY'})
               -[:GENERATED_BILL]->(bd:BillingDocument {billingDocumentIsCancelled: false})
               -[r:INVOICED_PRODUCT]->(p:Product)
RETURN
  c.soldToParty                        AS customerId,
  c.name                               AS customerName,
  COUNT(DISTINCT order.documentId)     AS totalOrders,
  COUNT(DISTINCT delivery.documentId)  AS totalDeliveries,
  COUNT(DISTINCT bd.billingDocument)   AS totalInvoices,
  SUM(r.amount)                        AS totalRevenue,
  COLLECT(DISTINCT r.currency)[0]      AS currency,
  COUNT(DISTINCT p.material)           AS uniqueProducts
ORDER BY totalRevenue DESC;
