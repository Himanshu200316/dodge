// ═══════════════════════════════════════════════════════════════
// STEP 7: BILLING DOCUMENT HEADERS — Load all header files
// Both billing_document_headers and billing_document_cancellations
// share the same schema; MERGE deduplicates across them.
// ═══════════════════════════════════════════════════════════════

// --- billing_document_headers/part-20251119-133433-228.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/billing_document_headers/part-20251119-133433-228.jsonl') YIELD value AS row
WITH row
MERGE (bd:BillingDocument {billingDocument: row.billingDocument})
  ON CREATE SET
    bd.billingDocumentType       = row.billingDocumentType,
    bd.totalNetAmount            = toFloat(row.totalNetAmount),
    bd.transactionCurrency       = row.transactionCurrency,
    bd.billingDocumentDate       = date(substring(row.billingDocumentDate, 0, 10)),
    bd.billingDocumentIsCancelled = row.billingDocumentIsCancelled,
    bd.cancelledBillingDocument  = row.cancelledBillingDocument,
    bd.companyCode               = row.companyCode,
    bd.fiscalYear                = row.fiscalYear,
    bd.creationDate              = date(substring(row.creationDate, 0, 10)),
    bd.soldToParty               = row.soldToParty;

// --- billing_document_headers/part-20251119-133433-936.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/billing_document_headers/part-20251119-133433-936.jsonl') YIELD value AS row
WITH row
MERGE (bd:BillingDocument {billingDocument: row.billingDocument})
  ON CREATE SET
    bd.billingDocumentType       = row.billingDocumentType,
    bd.totalNetAmount            = toFloat(row.totalNetAmount),
    bd.transactionCurrency       = row.transactionCurrency,
    bd.billingDocumentDate       = date(substring(row.billingDocumentDate, 0, 10)),
    bd.billingDocumentIsCancelled = row.billingDocumentIsCancelled,
    bd.cancelledBillingDocument  = row.cancelledBillingDocument,
    bd.companyCode               = row.companyCode,
    bd.fiscalYear                = row.fiscalYear,
    bd.creationDate              = date(substring(row.creationDate, 0, 10)),
    bd.soldToParty               = row.soldToParty;

// --- billing_document_cancellations/part-20251119-133433-51.jsonl ---
// Same schema as headers. MERGE ensures no duplicates.
CALL apoc.load.jsonArray('file:///sap-o2c-data/billing_document_cancellations/part-20251119-133433-51.jsonl') YIELD value AS row
WITH row
MERGE (bd:BillingDocument {billingDocument: row.billingDocument})
  ON CREATE SET
    bd.billingDocumentType       = row.billingDocumentType,
    bd.totalNetAmount            = toFloat(row.totalNetAmount),
    bd.transactionCurrency       = row.transactionCurrency,
    bd.billingDocumentDate       = date(substring(row.billingDocumentDate, 0, 10)),
    bd.billingDocumentIsCancelled = row.billingDocumentIsCancelled,
    bd.cancelledBillingDocument  = row.cancelledBillingDocument,
    bd.companyCode               = row.companyCode,
    bd.fiscalYear                = row.fiscalYear,
    bd.creationDate              = date(substring(row.creationDate, 0, 10)),
    bd.soldToParty               = row.soldToParty;


// ═══════════════════════════════════════════════════════════════
// STEP 8: BILLING ITEMS — GENERATED_BILL + INVOICED_PRODUCT
// Line-item data stored as relationship properties (NO item nodes)
// ═══════════════════════════════════════════════════════════════

// --- billing_document_items/part-20251119-133432-233.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/billing_document_items/part-20251119-133432-233.jsonl') YIELD value AS row
WITH row
MERGE (bd:BillingDocument {billingDocument: row.billingDocument})
MERGE (sd:SalesDocument {documentId: row.referenceSdDocument})
MERGE (sd)-[:GENERATED_BILL]->(bd)
MERGE (p:Product {material: row.material})
MERGE (bd)-[r:INVOICED_PRODUCT {
  item: row.billingDocumentItem,
  material: row.material
}]->(p)
  ON CREATE SET
    r.qty      = toInteger(row.billingQuantity),
    r.amount   = toFloat(row.netAmount),
    r.currency = row.transactionCurrency,
    r.unit     = row.billingQuantityUnit;

// --- billing_document_items/part-20251119-133432-978.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/billing_document_items/part-20251119-133432-978.jsonl') YIELD value AS row
WITH row
MERGE (bd:BillingDocument {billingDocument: row.billingDocument})
MERGE (sd:SalesDocument {documentId: row.referenceSdDocument})
MERGE (sd)-[:GENERATED_BILL]->(bd)
MERGE (p:Product {material: row.material})
MERGE (bd)-[r:INVOICED_PRODUCT {
  item: row.billingDocumentItem,
  material: row.material
}]->(p)
  ON CREATE SET
    r.qty      = toInteger(row.billingQuantity),
    r.amount   = toFloat(row.netAmount),
    r.currency = row.transactionCurrency,
    r.unit     = row.billingQuantityUnit;


// ═══════════════════════════════════════════════════════════════
// STEP 9: REVERSAL LINKS — S1 documents that cancel F2 documents
// S1 doc has cancelledBillingDocument pointing to the original F2
// ═══════════════════════════════════════════════════════════════

MATCH (reversal:BillingDocument)
  WHERE reversal.billingDocumentType = 'S1'
    AND reversal.cancelledBillingDocument IS NOT NULL
    AND reversal.cancelledBillingDocument <> ''
MATCH (original:BillingDocument {billingDocument: reversal.cancelledBillingDocument})
MERGE (reversal)-[:REVERSED_BY]->(original);
