"""
═══════════════════════════════════════════════════════════════════════════════
SAP Order-to-Cash (O2C) → Neo4j Graph Ingestion Pipeline
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
  Reads JSONL files from the SAP O2C dataset and ingests them into a clean,
  deduplicated, query-optimized Neo4j graph using the official neo4j driver.

GRAPH MODEL:
  (Customer)-[:PLACED]->(SalesDocument:ORDER)
      -[:FLOWS_TO]->(SalesDocument:DELIVERY)
          -[:BILLED_AS]->(BillingDocument)
              -[:CONTAINS {item, qty, amount, currency}]->(Product)
  (BillingDocument)-[:REVERSED_BY]->(BillingDocument)

GUARANTEES:
  • Idempotent  — Safe to re-run. All node ops use MERGE on primary keys.
  • No CREATEs  — Only MERGE for nodes; MATCH + MERGE for relationships.
  • No APOC     — Pure neo4j driver + Python file I/O.
  • Batch-based — Processes rows in configurable batches (default 1000).

USAGE:
  pip install neo4j
  python ingest_o2c.py

═══════════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from neo4j import GraphDatabase

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "password"

# Path to the root of the SAP O2C dataset (contains subfolders like products/, etc.)
DATA_DIR = Path(r"C:\Users\Himanshu Negi\Desktop\Dodge\sap-order-to-cash-dataset\sap-o2c-data")

BATCH_SIZE = 1000  # Rows per UNWIND batch

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("O2C-Ingest")

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

def connect_to_neo4j() -> GraphDatabase.driver:
    """Establish a connection to Neo4j. Verifies connectivity on creation."""
    log.info(f"Connecting to Neo4j at {NEO4J_URI} ...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    log.info("✓ Connected to Neo4j successfully.")
    return driver

# ─────────────────────────────────────────────────────────────────────────────
# FILE I/O HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_jsonl(file_path: Path) -> list[dict]:
    """
    Read a JSONL file and return a list of parsed dicts.
    Skips blank lines and logs malformed JSON rows (does not abort).
    """
    rows = []
    skipped = 0
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                skipped += 1
                log.warning(f"  Malformed JSON at {file_path.name}:{line_num} — skipped.")
    if skipped:
        log.warning(f"  {skipped} malformed row(s) skipped in {file_path.name}")
    return rows


def get_jsonl_files(subfolder: str) -> list[Path]:
    """Return sorted list of .jsonl files under DATA_DIR/subfolder."""
    folder = DATA_DIR / subfolder
    if not folder.exists():
        log.warning(f"Subfolder missing: {subfolder} — skipping.")
        return []
    files = sorted(folder.glob("*.jsonl"))
    if not files:
        log.warning(f"No .jsonl files found in {subfolder}")
    return files

# ─────────────────────────────────────────────────────────────────────────────
# BATCH EXECUTION
# ─────────────────────────────────────────────────────────────────────────────

def batch_insert(driver, query: str, rows: list[dict], description: str = "") -> int:
    """
    Execute a Cypher query in batches using UNWIND $rows.
    Returns total number of rows processed.
    """
    total = len(rows)
    processed = 0

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]

        def _tx(tx, batch_data):
            tx.run(query, rows=batch_data)

        with driver.session() as session:
            session.execute_write(_tx, batch)

        processed += len(batch)

    if description:
        log.info(f"  ✓ {description}: {processed} rows processed.")
    return processed

# ─────────────────────────────────────────────────────────────────────────────
# STEP 0: CONSTRAINTS & INDEXES
# ─────────────────────────────────────────────────────────────────────────────

def create_constraints(driver):
    """
    Create uniqueness constraints and performance indexes.
    Uses IF NOT EXISTS so this is fully idempotent.
    """
    log.info("Creating constraints & indexes ...")

    statements = [
        # ── Primary Key Constraints ──
        "CREATE CONSTRAINT customer_pk IF NOT EXISTS FOR (c:Customer) REQUIRE c.soldToParty IS UNIQUE",
        "CREATE CONSTRAINT sales_document_pk IF NOT EXISTS FOR (sd:SalesDocument) REQUIRE sd.documentId IS UNIQUE",
        "CREATE CONSTRAINT billing_document_pk IF NOT EXISTS FOR (bd:BillingDocument) REQUIRE bd.billingDocument IS UNIQUE",
        "CREATE CONSTRAINT product_pk IF NOT EXISTS FOR (p:Product) REQUIRE p.material IS UNIQUE",

        # ── Performance Indexes ──
        "CREATE INDEX idx_billing_date IF NOT EXISTS FOR (bd:BillingDocument) ON (bd.billingDocumentDate)",
        "CREATE INDEX idx_billing_cancelled IF NOT EXISTS FOR (bd:BillingDocument) ON (bd.billingDocumentIsCancelled)",
        "CREATE INDEX idx_billing_type IF NOT EXISTS FOR (bd:BillingDocument) ON (bd.billingDocumentType)",
        "CREATE INDEX idx_product_desc IF NOT EXISTS FOR (p:Product) ON (p.description)",
        "CREATE INDEX idx_sd_category IF NOT EXISTS FOR (sd:SalesDocument) ON (sd.sdDocumentCategory)",
        "CREATE INDEX idx_sd_creation IF NOT EXISTS FOR (sd:SalesDocument) ON (sd.creationDate)",
        "CREATE INDEX idx_customer_name IF NOT EXISTS FOR (c:Customer) ON (c.name)",
    ]

    with driver.session() as session:
        for stmt in statements:
            session.run(stmt)

    log.info(f"  ✓ {len(statements)} constraints/indexes ensured.")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: PRODUCTS
# ─────────────────────────────────────────────────────────────────────────────

def insert_products(driver):
    """
    Load Product nodes from products/*.jsonl
    SAP field mapping:
      product         → Product.material  (PRIMARY KEY)
      productType     → Product.productType
      productGroup    → Product.productGroup
      baseUnit        → Product.baseUnit
      division        → Product.division
      grossWeight     → Product.grossWeight  (toFloat)
      netWeight       → Product.netWeight    (toFloat)
      weightUnit      → Product.weightUnit
    """
    log.info("═══ STEP 1: Loading Products ═══")

    query = """
    UNWIND $rows AS row
    MERGE (p:Product {material: row.product})
      ON CREATE SET
        p.productType  = row.productType,
        p.productGroup = row.productGroup,
        p.baseUnit     = row.baseUnit,
        p.division     = row.division,
        p.grossWeight  = CASE WHEN row.grossWeight IS NOT NULL THEN toFloat(row.grossWeight) ELSE null END,
        p.netWeight    = CASE WHEN row.netWeight IS NOT NULL THEN toFloat(row.netWeight) ELSE null END,
        p.weightUnit   = row.weightUnit,
        p.creationDate = CASE WHEN row.creationDate IS NOT NULL THEN date(substring(row.creationDate, 0, 10)) ELSE null END
      ON MATCH SET
        p.productType  = row.productType,
        p.productGroup = row.productGroup
    """

    for f in get_jsonl_files("products"):
        rows = load_jsonl(f)
        batch_insert(driver, query, rows, f"Products from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: PRODUCT DESCRIPTIONS
# ─────────────────────────────────────────────────────────────────────────────

def insert_product_descriptions(driver):
    """
    Enrich existing Product nodes with English descriptions.
    SAP field mapping:
      product            → Product.material (join key)
      productDescription → Product.description
      language           → filter: only 'EN' rows
    """
    log.info("═══ STEP 2: Enriching Product Descriptions ═══")

    query = """
    UNWIND $rows AS row
    WITH row WHERE row.language = 'EN'
    MATCH (p:Product {material: row.product})
    SET p.description = row.productDescription
    """

    for f in get_jsonl_files("product_descriptions"):
        rows = load_jsonl(f)
        batch_insert(driver, query, rows, f"Descriptions from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: CUSTOMERS (Business Partners)
# ─────────────────────────────────────────────────────────────────────────────

def insert_customers(driver):
    """
    Load Customer nodes from business_partners/*.jsonl
    SAP field mapping:
      businessPartner         → Customer.soldToParty  (PRIMARY KEY)
        NOTE: businessPartner == customer == soldToParty in this dataset.
              We use soldToParty as PK because it's the join key across
              Sales Orders, Deliveries, and Billing Documents.
      businessPartnerName     → Customer.name
      businessPartnerFullName → Customer.fullName
      businessPartnerCategory → Customer.category
      businessPartnerGrouping → Customer.grouping
      businessPartnerIsBlocked → Customer.isBlocked
    """
    log.info("═══ STEP 3: Loading Customers ═══")

    query = """
    UNWIND $rows AS row
    MERGE (c:Customer {soldToParty: row.businessPartner})
      ON CREATE SET
        c.name      = row.businessPartnerName,
        c.fullName  = row.businessPartnerFullName,
        c.category  = row.businessPartnerCategory,
        c.grouping  = row.businessPartnerGrouping,
        c.isBlocked = row.businessPartnerIsBlocked
      ON MATCH SET
        c.name      = row.businessPartnerName,
        c.fullName  = row.businessPartnerFullName,
        c.isBlocked = row.businessPartnerIsBlocked
    """

    for f in get_jsonl_files("business_partners"):
        rows = load_jsonl(f)
        batch_insert(driver, query, rows, f"Customers from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: SALES ORDERS + PLACED relationship
# ─────────────────────────────────────────────────────────────────────────────

def insert_sales_orders(driver):
    """
    Load SalesDocument (ORDER) nodes from sales_order_headers/*.jsonl
    and create (Customer)-[:PLACED]->(SalesDocument) relationships.

    SAP field mapping:
      salesOrder            → SalesDocument.documentId  (PRIMARY KEY)
      salesOrderType        → SalesDocument.documentType
      soldToParty           → SalesDocument.soldToParty + Customer join key
      creationDate          → SalesDocument.creationDate (date)
      totalNetAmount        → SalesDocument.totalNetAmount (float)
      transactionCurrency   → SalesDocument.transactionCurrency
      overallDeliveryStatus → SalesDocument.deliveryStatus
      deliveryBlockReason   → SalesDocument.deliveryBlockReason
    """
    log.info("═══ STEP 4: Loading Sales Orders + PLACED relationships ═══")

    # Step 4a: Create SalesDocument nodes
    node_query = """
    UNWIND $rows AS row
    MERGE (sd:SalesDocument {documentId: row.salesOrder})
      ON CREATE SET
        sd.sdDocumentCategory  = 'ORDER',
        sd.documentType        = row.salesOrderType,
        sd.soldToParty         = row.soldToParty,
        sd.creationDate        = CASE WHEN row.creationDate IS NOT NULL THEN date(substring(row.creationDate, 0, 10)) ELSE null END,
        sd.totalNetAmount      = CASE WHEN row.totalNetAmount IS NOT NULL THEN toFloat(row.totalNetAmount) ELSE null END,
        sd.transactionCurrency = row.transactionCurrency,
        sd.deliveryStatus      = row.overallDeliveryStatus,
        sd.deliveryBlockReason = row.deliveryBlockReason
    """

    # Step 4b: Link Customer → SalesDocument
    # Uses MATCH on both sides (Customer nodes already loaded in Step 3)
    rel_query = """
    UNWIND $rows AS row
    MATCH (c:Customer {soldToParty: row.soldToParty})
    MATCH (sd:SalesDocument {documentId: row.salesOrder})
    MERGE (c)-[:PLACED]->(sd)
    """

    for f in get_jsonl_files("sales_order_headers"):
        rows = load_jsonl(f)
        batch_insert(driver, node_query, rows, f"SalesDocument(ORDER) from {f.name}")
        batch_insert(driver, rel_query, rows, f"PLACED rels from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: DELIVERY HEADERS (SalesDocument with category DELIVERY)
# ─────────────────────────────────────────────────────────────────────────────

def insert_deliveries(driver):
    """
    Load SalesDocument (DELIVERY) nodes from outbound_delivery_headers/*.jsonl

    SAP field mapping:
      deliveryDocument          → SalesDocument.documentId  (PRIMARY KEY)
      creationDate              → SalesDocument.creationDate (date)
      shippingPoint             → SalesDocument.shippingPoint
      overallGoodsMovementStatus → SalesDocument.goodsMovementStatus
      overallPickingStatus      → SalesDocument.pickingStatus
      actualGoodsMovementDate   → SalesDocument.actualGoodsMovementDate (date, nullable)
    """
    log.info("═══ STEP 5: Loading Delivery Headers ═══")

    query = """
    UNWIND $rows AS row
    MERGE (sd:SalesDocument {documentId: row.deliveryDocument})
      ON CREATE SET
        sd.sdDocumentCategory      = 'DELIVERY',
        sd.documentType            = 'DL',
        sd.creationDate            = CASE WHEN row.creationDate IS NOT NULL THEN date(substring(row.creationDate, 0, 10)) ELSE null END,
        sd.shippingPoint           = row.shippingPoint,
        sd.goodsMovementStatus     = row.overallGoodsMovementStatus,
        sd.pickingStatus           = row.overallPickingStatus,
        sd.actualGoodsMovementDate = CASE WHEN row.actualGoodsMovementDate IS NOT NULL THEN date(substring(row.actualGoodsMovementDate, 0, 10)) ELSE null END
    """

    for f in get_jsonl_files("outbound_delivery_headers"):
        rows = load_jsonl(f)
        batch_insert(driver, query, rows, f"SalesDocument(DELIVERY) from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: DELIVERY ITEMS → FLOWS_TO relationships
# ─────────────────────────────────────────────────────────────────────────────

def insert_delivery_relationships(driver):
    """
    Create (SalesDocument:ORDER)-[:FLOWS_TO]->(SalesDocument:DELIVERY) 
    from outbound_delivery_items/*.jsonl

    SAP field mapping:
      referenceSdDocument → the Sales Order documentId (ORDER side)
      deliveryDocument    → the Delivery documentId (DELIVERY side)

    The delivery items table links each delivery line back to its source
    sales order via referenceSdDocument. We deduplicate per order-delivery pair.

    Uses MATCH on both sides — both ORDER and DELIVERY nodes already exist.
    """
    log.info("═══ STEP 6: Creating FLOWS_TO relationships (Order → Delivery) ═══")

    query = """
    UNWIND $rows AS row
    MATCH (order:SalesDocument {documentId: row.referenceSdDocument})
    MATCH (delivery:SalesDocument {documentId: row.deliveryDocument})
    MERGE (order)-[:FLOWS_TO]->(delivery)
    """

    for f in get_jsonl_files("outbound_delivery_items"):
        rows = load_jsonl(f)

        # Deduplicate in Python to avoid redundant Cypher MERGE attempts.
        # Multiple delivery items for the same order-delivery pair would
        # create the same relationship — filter to unique pairs.
        seen_pairs = set()
        unique_rows = []
        for row in rows:
            ref = row.get("referenceSdDocument", "")
            dlv = row.get("deliveryDocument", "")
            if not ref or not dlv:
                continue
            pair = (ref, dlv)
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                unique_rows.append(row)

        batch_insert(driver, query, unique_rows, f"FLOWS_TO rels from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: BILLING DOCUMENT HEADERS
# ─────────────────────────────────────────────────────────────────────────────

def insert_billing_documents(driver):
    """
    Load BillingDocument nodes from:
      - billing_document_headers/*.jsonl     (F2 invoices + S1 reversals)
      - billing_document_cancellations/*.jsonl (cancelled F2s, same schema)

    MERGE on billingDocument deduplicates across files — some records
    appear in both headers and cancellations.

    SAP field mapping:
      billingDocument             → BillingDocument.billingDocument  (PRIMARY KEY)
      billingDocumentType         → BillingDocument.billingDocumentType  (F2=Invoice, S1=Reversal)
      totalNetAmount              → BillingDocument.totalNetAmount (float)
      transactionCurrency         → BillingDocument.transactionCurrency
      billingDocumentDate         → BillingDocument.billingDocumentDate (date)
      billingDocumentIsCancelled  → BillingDocument.billingDocumentIsCancelled (boolean)
      cancelledBillingDocument    → BillingDocument.cancelledBillingDocument (used for REVERSED_BY)
      companyCode                 → BillingDocument.companyCode
      fiscalYear                  → BillingDocument.fiscalYear
      creationDate                → BillingDocument.creationDate (date)
      soldToParty                 → BillingDocument.soldToParty
    """
    log.info("═══ STEP 7: Loading Billing Documents ═══")

    query = """
    UNWIND $rows AS row
    MERGE (bd:BillingDocument {billingDocument: row.billingDocument})
      ON CREATE SET
        bd.billingDocumentType       = row.billingDocumentType,
        bd.totalNetAmount            = CASE WHEN row.totalNetAmount IS NOT NULL THEN toFloat(row.totalNetAmount) ELSE null END,
        bd.transactionCurrency       = row.transactionCurrency,
        bd.billingDocumentDate       = CASE WHEN row.billingDocumentDate IS NOT NULL THEN date(substring(row.billingDocumentDate, 0, 10)) ELSE null END,
        bd.billingDocumentIsCancelled = row.billingDocumentIsCancelled,
        bd.cancelledBillingDocument  = row.cancelledBillingDocument,
        bd.companyCode               = row.companyCode,
        bd.fiscalYear                = row.fiscalYear,
        bd.creationDate              = CASE WHEN row.creationDate IS NOT NULL THEN date(substring(row.creationDate, 0, 10)) ELSE null END,
        bd.soldToParty               = row.soldToParty
      ON MATCH SET
        bd.billingDocumentIsCancelled = row.billingDocumentIsCancelled,
        bd.cancelledBillingDocument  = row.cancelledBillingDocument
    """

    # Load from both billing_document_headers AND billing_document_cancellations.
    # They share the same schema. MERGE deduplicates across them.
    for subfolder in ["billing_document_headers", "billing_document_cancellations"]:
        for f in get_jsonl_files(subfolder):
            rows = load_jsonl(f)
            batch_insert(driver, query, rows, f"BillingDocument from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: BILLING ITEMS → BILLED_AS + CONTAINS
# ─────────────────────────────────────────────────────────────────────────────

def insert_billing_items(driver):
    """
    Process billing_document_items/*.jsonl to create TWO relationship types:

    1. (SalesDocument)-[:BILLED_AS]->(BillingDocument)
       Links the upstream delivery to the billing document.
       SAP: referenceSdDocument → the Delivery document that generated this bill.

    2. (BillingDocument)-[:CONTAINS {item, qty, amount, currency}]->(Product)
       Line-item data stored as relationship properties (NO item nodes).
       Uniqueness: billingDocumentItem + material (composite on MERGE).

    SAP field mapping:
      billingDocument      → BillingDocument.billingDocument (join)
      billingDocumentItem  → CONTAINS.item  (string, relationship key)
      material             → Product.material (join)
      billingQuantity      → CONTAINS.qty (integer)
      netAmount            → CONTAINS.amount (float)
      transactionCurrency  → CONTAINS.currency
      billingQuantityUnit  → CONTAINS.unit
      referenceSdDocument  → SalesDocument.documentId (delivery that was billed)
    """
    log.info("═══ STEP 8: Loading Billing Items → BILLED_AS + CONTAINS ═══")

    # 8a: Create BILLED_AS relationships (Delivery → BillingDocument)
    # Deduplicate in Python: one BILLED_AS per (referenceSdDocument, billingDocument) pair
    billed_as_query = """
    UNWIND $rows AS row
    MATCH (sd:SalesDocument {documentId: row.referenceSdDocument})
    MATCH (bd:BillingDocument {billingDocument: row.billingDocument})
    MERGE (sd)-[:BILLED_AS]->(bd)
    """

    # 8b: Create CONTAINS relationships with line-item properties
    contains_query = """
    UNWIND $rows AS row
    MATCH (bd:BillingDocument {billingDocument: row.billingDocument})
    MATCH (p:Product {material: row.material})
    MERGE (bd)-[r:CONTAINS {item: row.billingDocumentItem, material: row.material}]->(p)
      ON CREATE SET
        r.qty      = CASE WHEN row.billingQuantity IS NOT NULL THEN toInteger(row.billingQuantity) ELSE null END,
        r.amount   = CASE WHEN row.netAmount IS NOT NULL THEN toFloat(row.netAmount) ELSE null END,
        r.currency = row.transactionCurrency,
        r.unit     = row.billingQuantityUnit
    """

    for f in get_jsonl_files("billing_document_items"):
        rows = load_jsonl(f)

        # ── BILLED_AS: deduplicate to unique (referenceSdDocument, billingDocument) pairs ──
        seen_pairs = set()
        billed_rows = []
        for row in rows:
            ref = row.get("referenceSdDocument", "")
            bd = row.get("billingDocument", "")
            if not ref or not bd:
                continue
            pair = (ref, bd)
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                billed_rows.append(row)

        batch_insert(driver, billed_as_query, billed_rows, f"BILLED_AS rels from {f.name}")

        # ── CONTAINS: filter rows with valid material ──
        contains_rows = [r for r in rows if r.get("material") and r.get("billingDocument")]
        batch_insert(driver, contains_query, contains_rows, f"CONTAINS rels from {f.name}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 9: CANCELLATIONS → REVERSED_BY
# ─────────────────────────────────────────────────────────────────────────────

def insert_cancellations(driver):
    """
    Create (BillingDocument:S1)-[:REVERSED_BY]->(BillingDocument:F2) relationships.

    In the SAP dataset:
      - S1 documents are reversal/cancellation documents.
      - S1.cancelledBillingDocument points to the original F2 invoice.
      - The original F2 has billingDocumentIsCancelled = true but its own
        cancelledBillingDocument field is empty.

    This step uses MATCH on BOTH sides — all BillingDocument nodes
    were already created in Step 7.
    """
    log.info("═══ STEP 9: Creating REVERSED_BY relationships ═══")

    query = """
    MATCH (reversal:BillingDocument)
      WHERE reversal.billingDocumentType = 'S1'
        AND reversal.cancelledBillingDocument IS NOT NULL
        AND reversal.cancelledBillingDocument <> ''
    MATCH (original:BillingDocument {billingDocument: reversal.cancelledBillingDocument})
    MERGE (reversal)-[:REVERSED_BY]->(original)
    """

    with driver.session() as session:
        result = session.execute_write(lambda tx: tx.run(query).consume())
        count = result.counters.relationships_created
        log.info(f"  ✓ REVERSED_BY: {count} reversal relationships created.")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 10: GRAPH VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def verify_graph(driver):
    """Print node and relationship counts for verification."""
    log.info("═══ VERIFICATION ═══")

    with driver.session() as session:
        # Node counts
        for label in ["Customer", "SalesDocument", "BillingDocument", "Product"]:
            result = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
            count = result.single()["cnt"]
            log.info(f"  {label:25s} → {count:>6} nodes")

        # SalesDocument category breakdown
        result = session.run("""
            MATCH (sd:SalesDocument)
            RETURN sd.sdDocumentCategory AS cat, count(sd) AS cnt
            ORDER BY cat
        """)
        for record in result:
            log.info(f"    └─ SalesDocument ({record['cat']:>10}) → {record['cnt']:>4}")

        # Relationship counts
        for rel_type in ["PLACED", "FLOWS_TO", "BILLED_AS", "CONTAINS", "REVERSED_BY"]:
            result = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS cnt")
            count = result.single()["cnt"]
            log.info(f"  {rel_type:25s} → {count:>6} relationships")

        # End-to-end path test
        result = session.run("""
            MATCH (c:Customer)-[:PLACED]->(o:SalesDocument)
                  -[:FLOWS_TO]->(d:SalesDocument)
                  -[:BILLED_AS]->(bd:BillingDocument)
                  -[:CONTAINS]->(p:Product)
            RETURN count(*) AS totalPaths
        """)
        paths = result.single()["totalPaths"]
        log.info(f"  {'E2E Paths (C→O→D→B→P)':25s} → {paths:>6} paths")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """
    Execute the full ingestion pipeline in dependency order:
      0. Constraints & Indexes
      1. Products
      2. Product Descriptions
      3. Customers
      4. Sales Orders + PLACED
      5. Delivery Headers
      6. Delivery Items → FLOWS_TO
      7. Billing Documents (Headers + Cancellations)
      8. Billing Items → BILLED_AS + CONTAINS
      9. Cancellations → REVERSED_BY
     10. Verification
    """
    start = time.time()
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║   SAP O2C → Neo4j Graph Ingestion Pipeline             ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    # Validate data directory
    if not DATA_DIR.exists():
        log.error(f"Data directory not found: {DATA_DIR}")
        sys.exit(1)

    driver = connect_to_neo4j()

    try:
        create_constraints(driver)       # Step 0
        insert_products(driver)          # Step 1
        insert_product_descriptions(driver)  # Step 2
        insert_customers(driver)         # Step 3
        insert_sales_orders(driver)      # Step 4
        insert_deliveries(driver)        # Step 5
        insert_delivery_relationships(driver)  # Step 6
        insert_billing_documents(driver)  # Step 7
        insert_billing_items(driver)     # Step 8
        insert_cancellations(driver)     # Step 9
        verify_graph(driver)             # Step 10

        elapsed = time.time() - start
        log.info(f"═══ Pipeline complete in {elapsed:.1f}s ═══")

    except Exception as e:
        log.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

    finally:
        driver.close()
        log.info("Neo4j connection closed.")


if __name__ == "__main__":
    main()
