// ═══════════════════════════════════════════════════════════════
// SAP Order-to-Cash — Neo4j Schema Initialization
// Run this FIRST before any data ingestion
// ═══════════════════════════════════════════════════════════════


// ───────────────────────────────────────────────────────────────
// PRIMARY KEY CONSTRAINTS — Enforce uniqueness per Node Label
// ───────────────────────────────────────────────────────────────

CREATE CONSTRAINT customer_pk IF NOT EXISTS
  FOR (c:Customer) REQUIRE c.soldToParty IS UNIQUE;

CREATE CONSTRAINT sales_document_pk IF NOT EXISTS
  FOR (sd:SalesDocument) REQUIRE sd.documentId IS UNIQUE;

CREATE CONSTRAINT billing_document_pk IF NOT EXISTS
  FOR (bd:BillingDocument) REQUIRE bd.billingDocument IS UNIQUE;

CREATE CONSTRAINT product_pk IF NOT EXISTS
  FOR (p:Product) REQUIRE p.material IS UNIQUE;


// ───────────────────────────────────────────────────────────────
// SECONDARY INDEXES — Optimized for LLM-generated Cypher queries
// ───────────────────────────────────────────────────────────────

// Date-range queries on billing
CREATE INDEX idx_billing_date IF NOT EXISTS
  FOR (bd:BillingDocument) ON (bd.billingDocumentDate);

// Filter cancelled invoices
CREATE INDEX idx_billing_cancelled IF NOT EXISTS
  FOR (bd:BillingDocument) ON (bd.billingDocumentIsCancelled);

// Billing document type (F2 vs S1)
CREATE INDEX idx_billing_type IF NOT EXISTS
  FOR (bd:BillingDocument) ON (bd.billingDocumentType);

// Product search by material code
CREATE INDEX idx_product_material IF NOT EXISTS
  FOR (p:Product) ON (p.material);

// Product description for text search
CREATE INDEX idx_product_desc IF NOT EXISTS
  FOR (p:Product) ON (p.description);

// SalesDocument category filter (ORDER vs DELIVERY)
CREATE INDEX idx_sd_category IF NOT EXISTS
  FOR (sd:SalesDocument) ON (sd.sdDocumentCategory);

// SalesDocument creation date for lead-time analytics
CREATE INDEX idx_sd_creation_date IF NOT EXISTS
  FOR (sd:SalesDocument) ON (sd.creationDate);

// Customer name search
CREATE INDEX idx_customer_name IF NOT EXISTS
  FOR (c:Customer) ON (c.name);
