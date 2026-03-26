// ═══════════════════════════════════════════════════════════════
// STEP 3: CUSTOMERS — Load from business_partners/*.jsonl
// soldToParty = businessPartner = customer (interchangeable in dataset)
// ═══════════════════════════════════════════════════════════════

// --- File: business_partners/part-20251119-133435-168.jsonl ---
CALL apoc.load.jsonArray('file:///sap-o2c-data/business_partners/part-20251119-133435-168.jsonl') YIELD value AS row
WITH row
MERGE (c:Customer {soldToParty: row.businessPartner})
  ON CREATE SET
    c.name       = row.businessPartnerName,
    c.fullName   = row.businessPartnerFullName,
    c.category   = row.businessPartnerCategory,
    c.grouping   = row.businessPartnerGrouping,
    c.isBlocked  = row.businessPartnerIsBlocked;
