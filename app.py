"""
═══════════════════════════════════════════════════════════════════════════════
SAP O2C Graph Query Interface
═══════════════════════════════════════════════════════════════════════════════
A Streamlit application that accepts natural language questions, converts them
to Cypher via OpenAI ChatGPT, executes against a Neo4j graph database, and
displays results as: Natural Language Answer, Interactive Graph, Raw Data Table.

Graph Model:
  (Customer)-[:PLACED]->(SalesDocument:ORDER)
      -[:FLOWS_TO]->(SalesDocument:DELIVERY)
          -[:BILLED_AS]->(BillingDocument)
              -[:CONTAINS {item, qty, amount, currency}]->(Product)
  (BillingDocument)-[:REVERSED_BY]->(BillingDocument)

Configuration:
  API key is loaded from config/.env — set it once, never re-enter.
═══════════════════════════════════════════════════════════════════════════════
"""

import json
import os
import re
import streamlit as st
from openai import OpenAI
from neo4j import GraphDatabase
from streamlit_agraph import agraph, Node, Edge, Config
from dotenv import load_dotenv
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# LOAD API KEY FROM config/.env (persistent — set once, never re-enter)
# ─────────────────────────────────────────────────────────────────────────────

CONFIG_DIR = Path(__file__).parent / "config"
ENV_FILE = CONFIG_DIR / ".env"
load_dotenv(ENV_FILE)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="SAP O2C — Graph Query Interface",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS — Premium dark theme with glassmorphism
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }

    .subtitle {
        font-size: 1rem;
        color: #888;
        margin-bottom: 1.5rem;
    }

    .glass-card {
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 1.2rem;
        backdrop-filter: blur(12px);
        margin-bottom: 1rem;
    }

    .history-chip {
        display: inline-block;
        background: rgba(102, 126, 234, 0.15);
        border: 1px solid rgba(102, 126, 234, 0.3);
        border-radius: 8px;
        padding: 0.4rem 0.8rem;
        margin: 0.2rem;
        font-size: 0.82rem;
        color: #a0aeff;
    }

    .node-card {
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        padding: 1rem;
        margin-top: 0.5rem;
    }

    [data-testid="stMetricValue"] {
        font-size: 1.1rem !important;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

NODE_COLORS = {
    "Customer":        "#4F8CF7",
    "SalesDocument":   "#34D399",
    "BillingDocument": "#FB923C",
    "Product":         "#A78BFA",
}

NODE_SIZES = {
    "Customer":        30,
    "SalesDocument":   25,
    "BillingDocument": 25,
    "Product":         22,
}

# The full schema context given to ChatGPT for Cypher generation
SCHEMA_PROMPT = """
You are a Neo4j Cypher expert for an SAP Order-to-Cash (O2C) dataset.

═══ GRAPH SCHEMA ═══

NODES:
  (:Customer {soldToParty, name, fullName, category, grouping, isBlocked})
  (:SalesDocument {documentId, sdDocumentCategory, documentType, soldToParty,
                   creationDate, totalNetAmount, transactionCurrency,
                   deliveryStatus, deliveryBlockReason, shippingPoint,
                   goodsMovementStatus, pickingStatus, actualGoodsMovementDate})
  (:BillingDocument {billingDocument, billingDocumentType, totalNetAmount,
                     transactionCurrency, billingDocumentDate,
                     billingDocumentIsCancelled, cancelledBillingDocument,
                     companyCode, fiscalYear, creationDate, soldToParty})
  (:Product {material, productType, productGroup, baseUnit, division,
             description, grossWeight, netWeight, weightUnit})

RELATIONSHIPS:
  (Customer)-[:PLACED]->(SalesDocument)
  (SalesDocument)-[:FLOWS_TO]->(SalesDocument)
  (SalesDocument)-[:BILLED_AS]->(BillingDocument)
  (BillingDocument)-[:CONTAINS {item, qty, amount, currency, unit, material}]->(Product)
  (BillingDocument)-[:REVERSED_BY]->(BillingDocument)

═══ SAP BUSINESS LOGIC ═══
  • SalesDocument WHERE sdDocumentCategory = 'ORDER'    → Sales Order
  • SalesDocument WHERE sdDocumentCategory = 'DELIVERY'  → Outbound Delivery
  • BillingDocument WHERE billingDocumentType = 'F2'     → Invoice
  • BillingDocument WHERE billingDocumentType = 'S1'     → Reversal/Cancellation
  • Business flow: Customer → Order → Delivery → Invoice → Product

═══ TRAVERSAL PATH ═══
  Customer -[:PLACED]-> SalesDocument(ORDER)
           -[:FLOWS_TO]-> SalesDocument(DELIVERY)
           -[:BILLED_AS]-> BillingDocument
           -[:CONTAINS]-> Product

═══ RULES (STRICT) ═══
  1. Generate READ-ONLY Cypher. Use MATCH and RETURN only.
  2. NEVER use CREATE, DELETE, SET, MERGE, REMOVE, DROP, DETACH.
  3. Always include LIMIT 50 unless the query is an aggregation (COUNT, SUM, AVG).
  4. Always filter: bd.billingDocumentIsCancelled = false
     UNLESS the user explicitly asks about cancelled or reversed documents.
  5. Return ONLY valid Cypher. No explanations. No markdown. No code fences.
  6. If the question asks to modify, write, delete, or change data, return exactly:
     Safety Error: Read-only access only.
  7. If the question is unrelated to SAP O2C data, return exactly:
     This system only answers questions about the SAP dataset.
"""

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE INITIALIZATION
# ─────────────────────────────────────────────────────────────────────────────

if "query_history" not in st.session_state:
    st.session_state.query_history = []

if "selected_node" not in st.session_state:
    st.session_state.selected_node = None

if "last_result" not in st.session_state:
    st.session_state.last_result = None

if "last_cypher" not in st.session_state:
    st.session_state.last_cypher = None


# ─────────────────────────────────────────────────────────────────────────────
# OPENAI CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def get_openai_client(api_key: str) -> OpenAI:
    """Create an OpenAI client with the provided API key."""
    return OpenAI(api_key=api_key)


def get_api_key() -> str:
    """
    Resolve the OpenAI API key from multiple sources (priority order):
      1. config/.env file (persistent — recommended)
      2. Environment variable OPENAI_API_KEY
      3. Sidebar manual input (fallback)
    """
    # Already loaded from config/.env via dotenv
    key = os.environ.get("OPENAI_API_KEY", "")
    if key and key != "your-openai-api-key-here":
        return key
    return ""


def save_api_key(api_key: str):
    """Save the API key to config/.env for persistence across restarts."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.write(
            "# ═══════════════════════════════════════════════════════\n"
            "# SAP O2C Graph Query Interface — Configuration\n"
            "# ═══════════════════════════════════════════════════════\n"
            "# This file was auto-saved by the app.\n"
            "# ═══════════════════════════════════════════════════════\n\n"
            f"OPENAI_API_KEY={api_key}\n"
        )
    os.environ["OPENAI_API_KEY"] = api_key


# ─────────────────────────────────────────────────────────────────────────────
# CORE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def generate_cypher(client: OpenAI, question: str) -> str:
    """
    Convert a natural language question into a Cypher query using ChatGPT.
    Returns raw Cypher string or a safety/error message.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SCHEMA_PROMPT},
                {"role": "user", "content": question},
            ],
            temperature=0,
            max_tokens=1000,
        )
        cypher = response.choices[0].message.content.strip()

        # Strip markdown code fences if GPT wraps them
        cypher = re.sub(r"^```(?:cypher)?\s*\n?", "", cypher)
        cypher = re.sub(r"\n?```\s*$", "", cypher)
        cypher = cypher.strip()

        return cypher

    except Exception as e:
        return f"OpenAI Error: {str(e)}"


def validate_cypher(cypher: str) -> tuple[bool, str]:
    """
    Validate that the Cypher query is safe to execute.
    Returns (is_valid, error_message).
    """
    if cypher.startswith("Safety Error:"):
        return False, cypher
    if cypher.startswith("This system only"):
        return False, cypher
    if cypher.startswith("Error:") or cypher.startswith("OpenAI Error:"):
        return False, cypher

    write_keywords = ["CREATE", "DELETE", "SET ", "MERGE", "REMOVE", "DROP", "DETACH"]
    cypher_upper = cypher.upper()
    for kw in write_keywords:
        if re.search(r'\b' + kw.strip() + r'\b', cypher_upper):
            return False, f"Safety Error: Write operation '{kw.strip()}' detected. Read-only access only."

    if not cypher_upper.strip().startswith("MATCH") and not cypher_upper.strip().startswith("OPTIONAL"):
        if not cypher_upper.strip().startswith("CALL"):
            return False, "Safety Error: Query must start with MATCH or OPTIONAL MATCH."

    return True, ""


def connect_neo4j():
    """Create a Neo4j driver connection."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def run_query(cypher: str) -> tuple[list, list | None, str | None]:
    """
    Execute a read-only Cypher query against Neo4j.
    Returns (records, keys, error).
    """
    try:
        driver = connect_neo4j()
        records, summary, keys = driver.execute_query(
            cypher,
            database_="neo4j",
            routing_="r",
        )
        driver.close()
        return records, keys, None
    except Exception as e:
        return [], None, f"Neo4j Error: {str(e)}"


def parse_neo4j_results(records, keys) -> dict:
    """
    Convert Neo4j records into a serializable structure compatible with
    streamlit-agraph and table display.
    """
    nodes_map = {}
    edges_list = []
    edges_seen = set()
    table_data = []

    for record in records:
        row_dict = {}
        for key in keys:
            value = record[key]

            # ── Handle Neo4j Node objects ──
            if hasattr(value, "labels") and hasattr(value, "element_id"):
                node_id = value.element_id
                if node_id not in nodes_map:
                    labels = list(value.labels)
                    label = labels[0] if labels else "Unknown"
                    props = dict(value)
                    nodes_map[node_id] = {
                        "id": node_id,
                        "label": label,
                        "properties": props,
                        "display_name": _get_node_display(label, props),
                    }
                row_dict[key] = _get_node_display(
                    list(value.labels)[0] if value.labels else "?",
                    dict(value)
                )

            # ── Handle Neo4j Relationship objects ──
            elif hasattr(value, "type") and hasattr(value, "element_id"):
                edge_key = value.element_id
                if edge_key not in edges_seen:
                    edges_seen.add(edge_key)
                    edges_list.append({
                        "source": value.start_node.element_id,
                        "target": value.end_node.element_id,
                        "type": value.type,
                        "properties": dict(value),
                    })
                    for node in [value.start_node, value.end_node]:
                        nid = node.element_id
                        if nid not in nodes_map:
                            nlabels = list(node.labels)
                            nlabel = nlabels[0] if nlabels else "Unknown"
                            nprops = dict(node)
                            nodes_map[nid] = {
                                "id": nid,
                                "label": nlabel,
                                "properties": nprops,
                                "display_name": _get_node_display(nlabel, nprops),
                            }
                row_dict[key] = value.type

            # ── Handle Neo4j Path objects ──
            elif hasattr(value, "nodes") and hasattr(value, "relationships"):
                for node in value.nodes:
                    nid = node.element_id
                    if nid not in nodes_map:
                        nlabels = list(node.labels)
                        nlabel = nlabels[0] if nlabels else "Unknown"
                        nprops = dict(node)
                        nodes_map[nid] = {
                            "id": nid,
                            "label": nlabel,
                            "properties": nprops,
                            "display_name": _get_node_display(nlabel, nprops),
                        }
                for rel in value.relationships:
                    edge_key = rel.element_id
                    if edge_key not in edges_seen:
                        edges_seen.add(edge_key)
                        edges_list.append({
                            "source": rel.start_node.element_id,
                            "target": rel.end_node.element_id,
                            "type": rel.type,
                            "properties": dict(rel),
                        })
                row_dict[key] = f"Path({len(value.nodes)} nodes)"

            # ── Scalar values ──
            else:
                row_dict[key] = _serialize_value(value)

        table_data.append(row_dict)

    return {
        "nodes": list(nodes_map.values()),
        "edges": edges_list,
        "table": table_data,
    }


def _get_node_display(label: str, props: dict) -> str:
    """Generate a human-readable display name for a node."""
    if label == "Customer":
        return props.get("name", props.get("soldToParty", "Customer"))
    elif label == "SalesDocument":
        cat = props.get("sdDocumentCategory", "DOC")
        doc_id = props.get("documentId", "?")
        return f"{cat}:{doc_id}"
    elif label == "BillingDocument":
        return props.get("billingDocument", "BillingDoc")
    elif label == "Product":
        return props.get("description", props.get("material", "Product"))
    return str(props.get("id", label))


def _serialize_value(val):
    """Convert Neo4j-specific types to JSON-safe Python types."""
    if val is None:
        return None
    if hasattr(val, "iso_format"):
        return val.iso_format()
    if isinstance(val, (int, float, str, bool)):
        return val
    if isinstance(val, list):
        return [_serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {k: _serialize_value(v) for k, v in val.items()}
    return str(val)


def generate_answer(client: OpenAI, question: str, table_data: list) -> str:
    """
    Use ChatGPT to generate a natural language answer from the query results.
    Uses ONLY the returned data — no hallucinations.
    """
    if not table_data:
        return "No matching records found in the SAP dataset."

    data_str = json.dumps(table_data[:20], indent=2, default=str)

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a data analyst answering questions about SAP Order-to-Cash data.\n"
                        "RULES:\n"
                        "- Use ONLY the data provided. Do NOT make up information.\n"
                        "- Be concise, factual, and structured.\n"
                        "- Use bullet points for multiple items.\n"
                        "- Include specific numbers, amounts, and dates where available.\n"
                        "- If the data is insufficient, say so honestly."
                    ),
                },
                {
                    "role": "user",
                    "content": f"QUESTION: {question}\n\nQUERY RESULTS (JSON):\n{data_str}\n\nANSWER:",
                },
            ],
            temperature=0.2,
            max_tokens=800,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Could not generate answer: {str(e)}"


# ─────────────────────────────────────────────────────────────────────────────
# GRAPH VISUALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def render_graph(parsed_data: dict) -> str | None:
    """Render an interactive graph using streamlit-agraph."""
    if not parsed_data["nodes"] and not parsed_data["edges"]:
        st.info("No graph data to visualize. This happens with aggregation queries that return scalar values.")
        return None

    agraph_nodes = []
    for n in parsed_data["nodes"]:
        label = n["label"]
        color = NODE_COLORS.get(label, "#6B7280")
        size = NODE_SIZES.get(label, 20)
        agraph_nodes.append(Node(
            id=n["id"],
            label=n["display_name"],
            size=size,
            color=color,
            font={"color": "#ffffff", "size": 11},
            shape="dot",
            borderWidth=2,
            borderWidthSelected=4,
        ))

    agraph_edges = []
    for e in parsed_data["edges"]:
        agraph_edges.append(Edge(
            source=e["source"],
            target=e["target"],
            label=e["type"],
            color="#555555",
            font={"color": "#888888", "size": 9, "align": "top"},
            arrows="to",
            width=1.5,
        ))

    config = Config(
        width="100%",
        height=500,
        directed=True,
        physics=True,
        hierarchical=False,
        nodeHighlightBehavior=True,
        highlightColor="#F7DC6F",
        collapsible=False,
        node={"labelProperty": "label"},
        link={"labelProperty": "label", "renderLabel": True},
    )

    return agraph(nodes=agraph_nodes, edges=agraph_edges, config=config)


def show_node_details(parsed_data: dict, node_id: str):
    """Display full properties of a clicked node in the sidebar."""
    node = None
    for n in parsed_data["nodes"]:
        if n["id"] == node_id:
            node = n
            break
    if not node:
        return

    label = node["label"]
    props = node["properties"]
    color = NODE_COLORS.get(label, "#6B7280")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 Node Details")
    st.sidebar.markdown(
        f'<span style="background:{color}; color:white; padding:4px 10px; '
        f'border-radius:6px; font-weight:600;">{label}</span>',
        unsafe_allow_html=True,
    )

    if label == "BillingDocument":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("Document", props.get("billingDocument", "—"))
        with col2:
            st.metric("Type", props.get("billingDocumentType", "—"))
        col3, col4 = st.sidebar.columns(2)
        with col3:
            amount = props.get("totalNetAmount", "—")
            currency = props.get("transactionCurrency", "")
            st.metric("Net Amount", f"{amount} {currency}")
        with col4:
            st.metric("Date", str(props.get("billingDocumentDate", "—")))
        if props.get("billingDocumentIsCancelled"):
            st.sidebar.warning("⚠️ This document is cancelled.")

    elif label == "Product":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("Material", props.get("material", "—"))
        with col2:
            st.metric("Base Unit", props.get("baseUnit", "—"))
        if props.get("description"):
            st.sidebar.info(f"📦 {props['description']}")

    elif label == "Customer":
        st.metric("Sold-To Party", props.get("soldToParty", "—"))
        if props.get("name"):
            st.sidebar.info(f"🏢 {props['name']}")
        if props.get("isBlocked"):
            st.sidebar.error("🚫 Customer is BLOCKED.")

    elif label == "SalesDocument":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("Document ID", props.get("documentId", "—"))
        with col2:
            st.metric("Category", props.get("sdDocumentCategory", "—"))

    st.sidebar.markdown("**All Properties:**")
    st.sidebar.json(props)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar() -> str:
    """Render the sidebar with config, query history, and legend."""

    st.sidebar.markdown("## ⚙️ Configuration")

    # ── API Key Resolution ──
    saved_key = get_api_key()
    key_source = ""

    if saved_key:
        st.sidebar.success("✓ API key loaded from `config/.env`", icon="🔑")
        key_source = "file"
        api_key = saved_key

        # Option to update the saved key
        with st.sidebar.expander("🔄 Update API Key"):
            new_key = st.text_input("New OpenAI API Key", type="password", key="new_key_input")
            if st.button("💾 Save Key", key="save_new_key"):
                if new_key.strip():
                    save_api_key(new_key.strip())
                    st.success("✓ Key saved to config/.env")
                    st.rerun()
    else:
        st.sidebar.warning("⚠️ No API key found in `config/.env`")
        api_key = st.sidebar.text_input(
            "OpenAI API Key",
            type="password",
            placeholder="sk-...",
            help="Paste your OpenAI key here. Click Save to persist it.",
        )
        if api_key:
            if st.sidebar.button("💾 Save Key to config/.env", use_container_width=True):
                save_api_key(api_key.strip())
                st.sidebar.success("✓ Key saved! It will be loaded automatically next time.")
                st.rerun()

    st.sidebar.markdown("---")

    # ── Graph Legend ──
    st.sidebar.markdown("### 🎨 Node Legend")
    for label, color in NODE_COLORS.items():
        st.sidebar.markdown(
            f'<span style="display:inline-block;width:14px;height:14px;'
            f'background:{color};border-radius:50%;margin-right:8px;'
            f'vertical-align:middle;"></span>{label}',
            unsafe_allow_html=True,
        )

    st.sidebar.markdown("---")

    # ── Query History ──
    st.sidebar.markdown("### 📚 Query History")
    if st.session_state.query_history:
        for i, q in enumerate(reversed(st.session_state.query_history[-5:])):
            if st.sidebar.button(
                f"🔁 {q[:50]}{'...' if len(q) > 50 else ''}",
                key=f"history_{i}",
                use_container_width=True,
            ):
                st.session_state["rerun_query"] = q
                st.rerun()

        if st.sidebar.button("🗑️ Clear History", use_container_width=True):
            st.session_state.query_history = []
            st.session_state.last_result = None
            st.session_state.last_cypher = None
            st.session_state.selected_node = None
            st.rerun()
    else:
        st.sidebar.caption("No queries yet.")

    return api_key


# ─────────────────────────────────────────────────────────────────────────────
# MAIN APPLICATION
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Title ──
    st.markdown('<h1 class="main-title">🔍 Graph Query Interface</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Ask questions about the SAP Order-to-Cash dataset in plain English</p>', unsafe_allow_html=True)

    # ── Sidebar ──
    api_key = render_sidebar()

    # ── Check for re-run from history ──
    rerun_q = st.session_state.pop("rerun_query", None)

    # ── Input Area ──
    col_input, col_btn = st.columns([5, 1])
    with col_input:
        question = st.text_input(
            "Ask your question",
            value=rerun_q or "",
            placeholder="Ask your question...",
            label_visibility="collapsed",
            key="question_input",
        )
    with col_btn:
        submit = st.button("🚀 Submit", use_container_width=True, type="primary")

    # ── Power Query Examples ──
    with st.expander("💡 Power Queries — click to try"):
        examples = [
            "Show me all customers and their total order values",
            "Which products generated the most revenue?",
            "Trace the full flow for sales order 740509",
            "Show all cancelled billing documents and their reversals",
            "What is the average lead time from order to billing?",
            "List the top 5 customers by number of invoices",
            "Show all products billed to customer 320000083",
            "Which deliveries have not been billed yet?",
        ]
        example_cols = st.columns(2)
        for i, ex in enumerate(examples):
            with example_cols[i % 2]:
                if st.button(f"▶ {ex}", key=f"example_{i}", use_container_width=True):
                    st.session_state["rerun_query"] = ex
                    st.rerun()

    # ── Guard: need a question + API key ──
    if not submit and not rerun_q:
        if st.session_state.last_result:
            _display_results(
                st.session_state.last_result["question"],
                st.session_state.last_result["cypher"],
                st.session_state.last_result["parsed"],
                st.session_state.last_result["answer"],
            )
        return

    if not api_key:
        st.error("⚠️ Please enter your OpenAI API key in the sidebar, or save it to `config/.env`.")
        return

    query = question.strip() if question else ""
    if not query:
        return

    # ── Add to history ──
    if query not in st.session_state.query_history:
        st.session_state.query_history.append(query)
        if len(st.session_state.query_history) > 5:
            st.session_state.query_history.pop(0)

    # ── Create OpenAI client ──
    client = get_openai_client(api_key)

    # ── STEP 1: Generate Cypher ──
    with st.spinner("🤖 Generating Cypher query via ChatGPT..."):
        cypher = generate_cypher(client, query)

    is_valid, error_msg = validate_cypher(cypher)
    if not is_valid:
        st.error(f"❌ {error_msg}")
        return

    st.session_state.last_cypher = cypher

    # ── STEP 2: Execute Query ──
    with st.spinner("⚡ Executing query against Neo4j..."):
        records, result_keys, error = run_query(cypher)

    if error:
        st.error(f"❌ {error}")
        with st.expander("🔧 Generated Cypher (debug)"):
            st.code(cypher, language="cypher")
        return

    # ── STEP 3: Parse Results ──
    parsed = parse_neo4j_results(records, result_keys)

    if not parsed["table"]:
        st.warning("🔍 No matching records found in the SAP dataset.")
        with st.expander("🔧 Generated Cypher"):
            st.code(cypher, language="cypher")
        return

    # ── STEP 4: Generate NL Answer ──
    with st.spinner("📝 Generating natural language answer..."):
        answer = generate_answer(client, query, parsed["table"])

    # ── Cache results ──
    st.session_state.last_result = {
        "question": query,
        "cypher": cypher,
        "parsed": parsed,
        "answer": answer,
    }

    _display_results(query, cypher, parsed, answer)


def _display_results(question: str, cypher: str, parsed: dict, answer: str):
    """Render the three-tab result view."""

    with st.expander("🔧 Generated Cypher", expanded=False):
        st.code(cypher, language="cypher")

    stat_cols = st.columns(4)
    with stat_cols[0]:
        st.metric("Nodes", len(parsed["nodes"]))
    with stat_cols[1]:
        st.metric("Edges", len(parsed["edges"]))
    with stat_cols[2]:
        st.metric("Rows", len(parsed["table"]))
    with stat_cols[3]:
        st.metric("Status", "✓ OK")

    tab_text, tab_graph, tab_data = st.tabs(["💬 Text Answer", "🕸️ Visual Graph", "📊 Raw Data"])

    with tab_text:
        st.markdown(f"**Q:** {question}")
        st.markdown("---")
        st.markdown(answer)

    with tab_graph:
        if parsed["nodes"]:
            clicked_node = render_graph(parsed)
            if clicked_node:
                st.session_state.selected_node = clicked_node
                show_node_details(parsed, clicked_node)
        else:
            st.info("📊 This query returned scalar/aggregated data — no graph nodes to visualize. Check the **Raw Data** tab.")

    with tab_data:
        if parsed["table"]:
            st.dataframe(parsed["table"], use_container_width=True, hide_index=True)
        else:
            st.info("No tabular data to display.")


if __name__ == "__main__":
    main()
