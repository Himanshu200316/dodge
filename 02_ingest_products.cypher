// ═══════════════════════════════════════════════════════════════
// STEP 1: PRODUCTS — Load from products/*.jsonl
// ═══════════════════════════════════════════════════════════════

// --- File: products/part-20251119-133438-390.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/products/part-20251119-133438-390.jsonl') YIELD value AS row
WITH row
MERGE (p:Product {material: row.product})
  ON CREATE SET
    p.productType    = row.productType,
    p.productGroup   = row.productGroup,
    p.baseUnit       = row.baseUnit,
    p.division       = row.division,
    p.grossWeight    = toFloat(row.grossWeight),
    p.netWeight      = toFloat(row.netWeight),
    p.weightUnit     = row.weightUnit,
    p.creationDate   = date(substring(row.creationDate, 0, 10))
  ON MATCH SET
    p.productType    = row.productType,
    p.productGroup   = row.productGroup;

// --- File: products/part-20251119-133438-730.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/products/part-20251119-133438-730.jsonl') YIELD value AS row
WITH row
MERGE (p:Product {material: row.product})
  ON CREATE SET
    p.productType    = row.productType,
    p.productGroup   = row.productGroup,
    p.baseUnit       = row.baseUnit,
    p.division       = row.division,
    p.grossWeight    = toFloat(row.grossWeight),
    p.netWeight      = toFloat(row.netWeight),
    p.weightUnit     = row.weightUnit,
    p.creationDate   = date(substring(row.creationDate, 0, 10))
  ON MATCH SET
    p.productType    = row.productType,
    p.productGroup   = row.productGroup;


// ═══════════════════════════════════════════════════════════════
// STEP 2: PRODUCT DESCRIPTIONS — Enrich Product nodes
// ═══════════════════════════════════════════════════════════════

// --- File: product_descriptions/part-20251119-133438-106.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/product_descriptions/part-20251119-133438-106.jsonl') YIELD value AS row
WITH row WHERE row.language = 'EN'
MATCH (p:Product {material: row.product})
SET p.description = row.productDescription;

// --- File: product_descriptions/part-20251119-133438-991.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/product_descriptions/part-20251119-133438-991.jsonl') YIELD value AS row
WITH row WHERE row.language = 'EN'
MATCH (p:Product {material: row.product})
SET p.description = row.productDescription;
