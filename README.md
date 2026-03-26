# SAP O2C — Graph Query Interface

An interactive Streamlit application that lets you query a Neo4j graph database
containing SAP Order-to-Cash data using natural language. Powered by Google Gemini
for NL → Cypher translation and streamlit-agraph for interactive graph visualization.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  User Query  │────▶│  Gemini AI   │────▶│  Cypher Gen  │
│  (Natural    │     │  (gemini-2.0 │     │  (Read-Only) │
│   Language)  │     │   -flash)    │     │              │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │
                     ┌──────────────┐     ┌───────▼──────┐
                     │  3-Tab View  │◀────│   Neo4j DB   │
                     │  Text/Graph/ │     │  (bolt://    │
                     │  Table       │     │  localhost)  │
                     └──────────────┘     └──────────────┘
```

## Prerequisites

1. **Neo4j** — Running locally on `bolt://localhost:7687`
   - Credentials: `neo4j` / `password`
   - Graph must be populated using `ingest_o2c.py`

2. **Google Gemini API Key** — Get one at https://aistudio.google.com/apikey

3. **Python 3.10+**

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# (Optional) Set Gemini API key as environment variable
set GOOGLE_API_KEY=your-key-here

# Run the app
streamlit run app.py
```

The app will open at `http://localhost:8501`

## Graph Model

```
Customer ──PLACED──▶ SalesDocument (ORDER)
                          │
                       FLOWS_TO
                          │
                          ▼
                     SalesDocument (DELIVERY)
                          │
                       BILLED_AS
                          │
                          ▼
                     BillingDocument (F2) ◀──REVERSED_BY── BillingDocument (S1)
                          │
                       CONTAINS
                          │
                          ▼
                       Product
```

## Features

| Feature | Description |
|---------|-------------|
| 🤖 NL → Cypher | Converts plain English to Cypher via Gemini |
| 🔒 Read-Only | All write operations blocked at multiple layers |
| 🕸️ Interactive Graph | Click nodes to inspect properties in sidebar |
| 📊 Raw Data Table | Full tabular view of query results |
| 💬 NL Answers | Gemini generates factual answers from data |
| 📚 Query History | Last 5 queries saved with one-click replay |
| ⚡ Power Queries | 8 pre-built example queries included |

## Power Queries

Try these queries in the app to explore the dataset:

| # | Query | What it shows |
|---|-------|---------------|
| 1 | "Show me all customers and their total order values" | Customer overview with revenue |
| 2 | "Which products generated the most revenue?" | Revenue ranking by product |
| 3 | "Trace the full flow for sales order 740509" | End-to-end O2C path |
| 4 | "Show all cancelled billing documents and their reversals" | Cancellation audit |
| 5 | "What is the average lead time from order to billing?" | Lead-time analytics |
| 6 | "List the top 5 customers by number of invoices" | Customer activity ranking |
| 7 | "Show all products billed to customer 320000083" | Customer-product detail |
| 8 | "Which deliveries have not been billed yet?" | Open delivery detection |

## Node Color Legend

| Node Type | Color | Key Property |
|-----------|-------|-------------|
| Customer | 🔵 Blue | `soldToParty` |
| SalesDocument | 🟢 Green | `documentId` |
| BillingDocument | 🟠 Orange | `billingDocument` |
| Product | 🟣 Purple | `material` |

## Safety Guardrails

1. **Cypher validation** — Write keywords (CREATE, DELETE, SET, MERGE) are blocked
2. **Read-only routing** — Neo4j queries use `routing_="r"` (read mode)
3. **LIMIT enforcement** — Gemini is instructed to add LIMIT 50
4. **Cancellation filter** — Cancelled documents excluded by default
5. **Input sanitization** — Schema prompt prevents injection attacks

## File Structure

```
neo4j-schema/
├── app.py                 # Streamlit application (this app)
├── ingest_o2c.py          # ETL pipeline (run first to populate graph)
├── requirements.txt       # Python dependencies
├── README.md              # This file
├── 01_constraints_indexes.cypher
├── 02_ingest_products.cypher
├── 03_ingest_customers.cypher
├── 04_ingest_sales_orders.cypher
├── 05_ingest_deliveries.cypher
├── 06_ingest_billing.cypher
├── 07_sample_queries.cypher
└── file_mapping.json
```
