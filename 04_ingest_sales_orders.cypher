// ═══════════════════════════════════════════════════════════════
// STEP 4: SALES ORDERS — Load headers + create PLACED_ORDER
// SalesDocument node with sdDocumentCategory = 'ORDER'
// ═══════════════════════════════════════════════════════════════

// --- File: sales_order_headers/part-20251119-133429-440.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/sales_order_headers/part-20251119-133429-440.jsonl') YIELD value AS row
WITH row

// Create/merge the SalesDocument node
MERGE (sd:SalesDocument {documentId: row.salesOrder})
  ON CREATE SET
    sd.sdDocumentCategory    = 'ORDER',
    sd.documentType          = row.salesOrderType,
    sd.soldToParty           = row.soldToParty,
    sd.creationDate          = date(substring(row.creationDate, 0, 10)),
    sd.totalNetAmount        = toFloat(row.totalNetAmount),
    sd.transactionCurrency   = row.transactionCurrency,
    sd.deliveryStatus        = row.overallDeliveryStatus,
    sd.deliveryBlockReason   = row.deliveryBlockReason

// Link to Customer
MERGE (c:Customer {soldToParty: row.soldToParty})
MERGE (c)-[:PLACED_ORDER]->(sd);
