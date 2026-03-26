// ═══════════════════════════════════════════════════════════════
// STEP 5: DELIVERY HEADERS — Create SalesDocument (DELIVERY) nodes
// ═══════════════════════════════════════════════════════════════

// --- File: outbound_delivery_headers/part-20251119-133431-414.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/outbound_delivery_headers/part-20251119-133431-414.jsonl') YIELD value AS row
WITH row
MERGE (sd:SalesDocument {documentId: row.deliveryDocument})
  ON CREATE SET
    sd.sdDocumentCategory     = 'DELIVERY',
    sd.documentType           = 'DL',
    sd.creationDate           = date(substring(row.creationDate, 0, 10)),
    sd.shippingPoint          = row.shippingPoint,
    sd.goodsMovementStatus    = row.overallGoodsMovementStatus,
    sd.pickingStatus          = row.overallPickingStatus,
    sd.actualGoodsMovementDate = CASE
      WHEN row.actualGoodsMovementDate IS NOT NULL
      THEN date(substring(row.actualGoodsMovementDate, 0, 10))
      ELSE null
    END;


// ═══════════════════════════════════════════════════════════════
// STEP 6: DELIVERY ITEMS — Link ORDER → DELIVERY (FULFILLED_BY)
// referenceSdDocument on delivery items points to the Sales Order
// ═══════════════════════════════════════════════════════════════

// --- File: outbound_delivery_items/part-20251119-133431-439.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/outbound_delivery_items/part-20251119-133431-439.jsonl') YIELD value AS row
WITH row
MATCH (delivery:SalesDocument {documentId: row.deliveryDocument})
MERGE (order:SalesDocument {documentId: row.referenceSdDocument})
MERGE (order)-[:FULFILLED_BY]->(delivery);

// --- File: outbound_delivery_items/part-20251119-133431-626.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/outbound_delivery_items/part-20251119-133431-626.jsonl') YIELD value AS row
WITH row
MATCH (delivery:SalesDocument {documentId: row.deliveryDocument})
MERGE (order:SalesDocument {documentId: row.referenceSdDocument})
MERGE (order)-[:FULFILLED_BY]->(delivery);
