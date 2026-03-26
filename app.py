"""
═══════════════════════════════════════════════════════════════════════════════
Mapping / Order to Cash — Optimized Graph Dashboard with Dodge AI
═══════════════════════════════════════════════════════════════════════════════
Optimizations:
  1. NODE_LIBRARY — Hardcoded schema, zero schema queries, minimal tokens
  2. Sliding Window — Only last 3 messages sent to Grok (low TPM)
  3. Pyvis — Full zoom/drag/pan interactivity via vis.js
  4. st.cache_resource — Neo4j driver cached, no reconnection per click
═══════════════════════════════════════════════════════════════════════════════
"""

import json, os, re, tempfile
import streamlit as st
import streamlit.components.v1 as components
from openai import OpenAI
from neo4j import GraphDatabase
from pyvis.network import Network
from dotenv import load_dotenv
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
CONFIG_DIR = Path(__file__).parent / "config"
ENV_FILE = CONFIG_DIR / ".env"
load_dotenv(ENV_FILE)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# ─────────────────────────────────────────────────────────────────────────────
# 1. NODE LIBRARY — Hardcoded schema (saves tokens, no schema queries)
#
# This is the ONLY schema reference sent to the LLM.
# It maps each Neo4j label to its queryable properties.
# ─────────────────────────────────────────────────────────────────────────────
NODE_LIBRARY = {
    "Customer": ["soldToParty", "name", "fullName", "isBlocked"],
    "SalesDocument": [
        "documentId", "sdDocumentCategory", "documentType", "soldToParty",
        "creationDate", "totalNetAmount", "transactionCurrency", "deliveryStatus",
    ],
    "BillingDocument": [
        "billingDocument", "billingDocumentType", "totalNetAmount",
        "transactionCurrency", "billingDocumentDate",
        "billingDocumentIsCancelled", "cancelledBillingDocument",
        "soldToParty", "fiscalYear",
    ],
    "Product": ["material", "productType", "productGroup", "baseUnit", "division", "description"],
}

RELATIONSHIPS = [
    "(Customer)-[:PLACED]->(SalesDocument)",
    "(SalesDocument)-[:FLOWS_TO]->(SalesDocument)",
    "(SalesDocument)-[:BILLED_AS]->(BillingDocument)",
    "(BillingDocument)-[:CONTAINS {item, qty, amount, currency}]->(Product)",
    "(BillingDocument)-[:REVERSED_BY]->(BillingDocument)",
]

# Build compact system prompt from NODE_LIBRARY (much smaller than full schema)
_lib_lines = "\n".join(f"  :{k} {{{', '.join(v)}}}" for k, v in NODE_LIBRARY.items())
_rel_lines = "\n".join(f"  {r}" for r in RELATIONSHIPS)

SYSTEM_PROMPT = f"""You are a Neo4j Cypher expert for SAP Order-to-Cash.

NODES:
{_lib_lines}

RELATIONSHIPS:
{_rel_lines}

LOGIC:
• sdDocumentCategory='ORDER' → Sales Order, 'DELIVERY' → Delivery
• billingDocumentType='F2' → Invoice, 'S1' → Reversal
• Flow: Customer → Order → Delivery → Billing → Product

RULES:
1. READ-ONLY. MATCH/RETURN only. NEVER CREATE/DELETE/SET/MERGE.
2. LIMIT 50 unless aggregation.
3. Filter billingDocumentIsCancelled=false unless asked about cancellations.
4. Return ONLY valid Cypher. No markdown, no explanation.
5. For write requests: Safety Error: Read-only access only.
6. For unrelated questions: This system only answers SAP O2C questions."""

# ─────────────────────────────────────────────────────────────────────────────
# NODE COLORS
# ─────────────────────────────────────────────────────────────────────────────
NODE_COLORS = {
    "Customer": "#4285f4",
    "SalesDocument": "#34a853",
    "BillingDocument": "#fbbc04",
    "Product": "#9c27b0",
}
DELIVERY_COLOR = "#00bcd4"
ANOMALY_COLOR = "#ea4335"
EDGE_COLOR = "#c6dafc"

LEGEND = [
    ("Customer", "#4285f4"), ("Sales Order", "#34a853"),
    ("Delivery", "#00bcd4"), ("Invoicing", "#fbbc04"),
    ("Product", "#9c27b0"), ("Anomaly", "#ea4335"),
]

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Mapping / Order to Cash",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: #f8f9fb; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 0.8rem; padding-bottom: 0; max-width: 100%; }

.breadcrumb {
    display: flex; align-items: center; gap: 8px;
    padding: 10px 20px; border-bottom: 1px solid #e8eaed;
    background: white; margin: -0.8rem -1rem 0.5rem -1rem;
    font-size: 14px; color: #5f6368;
}
.breadcrumb .current { color: #202124; font-weight: 600; }

.legend-bar { display: flex; gap: 14px; flex-wrap: wrap; padding: 4px 0; margin-bottom: 6px; }
.legend-item { display: flex; align-items: center; gap: 5px; font-size: 11.5px; color: #5f6368; }
.legend-dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }

.chat-header { padding: 12px 0 8px 0; border-bottom: 1px solid #f0f0f0; }
.chat-header h3 { font-size: 15px; font-weight: 600; color: #202124; margin: 0; }
.chat-header .sub { font-size: 12px; color: #9aa0a6; margin-top: 1px; }

.agent-card {
    display: flex; align-items: center; gap: 10px; padding: 10px 0;
}
.agent-avatar {
    width: 36px; height: 36px; border-radius: 10px;
    background: linear-gradient(135deg, #4285f4, #1a73e8);
    display: flex; align-items: center; justify-content: center;
    color: white; font-size: 16px; font-weight: 700;
}
.agent-info h4 { margin: 0; font-size: 13px; font-weight: 600; color: #202124; }
.agent-info .role { font-size: 11px; color: #9aa0a6; }

.status-bar {
    display: flex; align-items: center; gap: 6px;
    padding: 8px 0; font-size: 11.5px; color: #5f6368;
}
.status-dot { width: 7px; height: 7px; border-radius: 50%; background: #34a853; }

[data-testid="stMetricValue"] { font-size: 1rem !important; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────
DEFAULTS = {
    "messages": [{"role": "ai", "text": "Hi! I can help you analyze the **Order to Cash** process."}],
    "minimize": False,
    "hide_granular": False,
    "grok_ready": False,
    "graph_cache": None,
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────────────────────────────────────
# 4. CACHED NEO4J DRIVER (st.cache_resource — single connection, reused)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_driver():
    """Cached Neo4j driver — created once, reused across all reruns."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def neo4j_read(cypher: str, params: dict = None) -> tuple[list, list, str | None]:
    """Execute a read-only query using the cached driver."""
    try:
        driver = get_driver()
        records, summary, keys = driver.execute_query(
            cypher, parameters_=params or {}, database_="neo4j", routing_="r",
        )
        return records, keys, None
    except Exception as e:
        return [], [], str(e)

# ─────────────────────────────────────────────────────────────────────────────
# GROK (xAI) SETUP — uses OpenAI-compatible SDK with custom base_url
# ─────────────────────────────────────────────────────────────────────────────
XAI_BASE_URL = "https://api.x.ai/v1"
GROK_MODEL = "grok-3-mini-fast"

def get_api_key() -> str:
    key = os.environ.get("XAI_API_KEY", "")
    return key if key and key != "your-xai-api-key-here" else ""

def save_api_key(key: str):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(ENV_FILE, "w") as f:
        f.write(f"# Dodge AI config\nXAI_API_KEY={key}\n")
    os.environ["XAI_API_KEY"] = key

def init_grok(key: str):
    st.session_state.grok_client = OpenAI(api_key=key, base_url=XAI_BASE_URL)
    st.session_state.grok_ready = True

# ─────────────────────────────────────────────────────────────────────────────
# GRAPH DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
def _ser(val):
    if val is None: return None
    if hasattr(val, "iso_format"): return val.iso_format()
    if isinstance(val, (int, float, str, bool)): return val
    if isinstance(val, list): return [_ser(v) for v in val]
    if isinstance(val, dict): return {k: _ser(v) for k, v in val.items()}
    return str(val)

def _display(label, props):
    if label == "Customer": return props.get("name") or props.get("soldToParty", "")
    if label == "SalesDocument":
        cat = props.get("sdDocumentCategory", "")
        return f"{cat[:3]}:{props.get('documentId', '?')}"
    if label == "BillingDocument": return props.get("billingDocument", "")
    if label == "Product": return props.get("description") or props.get("material", "")
    return label

def load_graph() -> dict:
    """Fetch the full O2C graph from Neo4j."""
    cypher = """
    MATCH (n)
    WHERE n:Customer OR n:SalesDocument OR n:BillingDocument OR n:Product
    OPTIONAL MATCH (n)-[r]->(m)
    WHERE m:Customer OR m:SalesDocument OR m:BillingDocument OR m:Product
    RETURN n, r, m LIMIT 500
    """
    records, keys, error = neo4j_read(cypher)
    if error:
        return {"nodes": {}, "edges": [], "error": error}

    nodes = {}
    edges = []
    edge_ids = set()

    for rec in records:
        for key in keys:
            val = rec[key]
            if val is None:
                continue
            if hasattr(val, "labels") and hasattr(val, "element_id"):
                nid = val.element_id
                if nid not in nodes:
                    labels = list(val.labels)
                    lbl = labels[0] if labels else "Unknown"
                    props = {k: _ser(v) for k, v in dict(val).items()}
                    # Determine color
                    if lbl == "SalesDocument" and props.get("sdDocumentCategory") == "DELIVERY":
                        color = DELIVERY_COLOR
                    elif lbl == "BillingDocument" and props.get("billingDocumentIsCancelled"):
                        color = ANOMALY_COLOR
                    else:
                        color = NODE_COLORS.get(lbl, "#9aa0a6")
                    nodes[nid] = {
                        "id": nid, "label": lbl, "props": props,
                        "display": _display(lbl, props), "color": color,
                    }
            elif hasattr(val, "type") and hasattr(val, "element_id"):
                eid = val.element_id
                if eid not in edge_ids:
                    edge_ids.add(eid)
                    sid = val.start_node.element_id
                    tid = val.end_node.element_id
                    edges.append({"source": sid, "target": tid, "type": val.type})
                    # Register start/end nodes too
                    for n in (val.start_node, val.end_node):
                        nid2 = n.element_id
                        if nid2 not in nodes:
                            ls = list(n.labels)
                            lb = ls[0] if ls else "Unknown"
                            ps = {k: _ser(v) for k, v in dict(n).items()}
                            if lb == "SalesDocument" and ps.get("sdDocumentCategory") == "DELIVERY":
                                c = DELIVERY_COLOR
                            elif lb == "BillingDocument" and ps.get("billingDocumentIsCancelled"):
                                c = ANOMALY_COLOR
                            else:
                                c = NODE_COLORS.get(lb, "#9aa0a6")
                            nodes[nid2] = {
                                "id": nid2, "label": lb, "props": ps,
                                "display": _display(lb, ps), "color": c,
                            }

    return {"nodes": nodes, "edges": edges, "error": None}

# ─────────────────────────────────────────────────────────────────────────────
# 3. PYVIS GRAPH RENDERER (zoom, drag, pan — full interactivity)
# ─────────────────────────────────────────────────────────────────────────────
def render_pyvis(graph_data: dict) -> str | None:
    """Build and render a Pyvis graph. Returns clicked node ID via JS callback."""
    nodes = list(graph_data["nodes"].values())
    edges = graph_data["edges"]

    # Apply filters
    if st.session_state.minimize:
        nodes = [n for n in nodes if n["label"] in ("Customer", "SalesDocument")]
    if st.session_state.hide_granular:
        nodes = [n for n in nodes if n["label"] != "Product"]

    node_ids = {n["id"] for n in nodes}
    edges = [e for e in edges if e["source"] in node_ids and e["target"] in node_ids]

    if not nodes:
        st.info("No nodes with current filters.")
        return None

    # Build Pyvis network
    net = Network(
        height="620px", width="100%",
        bgcolor="#ffffff", font_color="#5f6368",
        directed=True, notebook=False,
        cdn_resources="remote",
    )

    # Physics config for good layout
    net.set_options("""
    {
        "physics": {
            "forceAtlas2Based": {
                "gravitationalConstant": -40,
                "centralGravity": 0.005,
                "springLength": 120,
                "springConstant": 0.04,
                "damping": 0.09
            },
            "solver": "forceAtlas2Based",
            "stabilization": {"iterations": 80}
        },
        "interaction": {
            "hover": true,
            "tooltipDelay": 100,
            "navigationButtons": true,
            "keyboard": true,
            "zoomView": true,
            "dragView": true
        },
        "edges": {
            "smooth": {"type": "continuous"},
            "arrows": {"to": {"enabled": true, "scaleFactor": 0.4}}
        },
        "nodes": {
            "font": {"size": 10, "color": "#5f6368"},
            "borderWidth": 1
        }
    }
    """)

    for n in nodes:
        # Build tooltip from top properties
        tip_lines = [f"<b>{n['label']}</b>"]
        for k, v in list(n["props"].items())[:6]:
            if v not in (None, "", []):
                tip_lines.append(f"{k}: {v}")
        tooltip = "<br>".join(tip_lines)

        size = 18 if n["label"] == "Customer" else 12
        net.add_node(
            n["id"], label=n["display"][:18],
            color=n["color"], size=size,
            title=tooltip, shape="dot",
            borderWidth=1,
        )

    for e in edges:
        net.add_edge(e["source"], e["target"], color=EDGE_COLOR, width=1)

    # ── Inject click handler that writes to a hidden div ──
    click_js = """
    <script>
    // After vis.js initializes, attach a click handler
    setTimeout(function() {
        try {
            var net = document.querySelector('.vis-network');
            if (!net) return;
            // Post clicked node id to Streamlit via window.parent
            var network = Object.values(net)[0];
        } catch(e) {}
    }, 1000);
    </script>
    """

    # Save to temp file and render
    tmp = Path(tempfile.gettempdir()) / "o2c_graph.html"
    net.save_graph(str(tmp))

    # Read and inject into Streamlit
    html_content = tmp.read_text(encoding="utf-8")
    components.html(html_content, height=640, scrolling=False)

    return None  # Pyvis click callbacks handled via next mechanism


# ─────────────────────────────────────────────────────────────────────────────
# 2. SLIDING WINDOW + NL → CYPHER (only last 3 messages to Grok)
# ─────────────────────────────────────────────────────────────────────────────
def nl_to_cypher(question: str) -> str:
    """Convert NL → Cypher using NODE_LIBRARY-based prompt + sliding window."""
    client = st.session_state.get("grok_client")
    if not client:
        return "Error: Grok not configured."

    # Build messages: system + last 3 user interactions (sliding window)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    recent = [m for m in st.session_state.messages[-3:] if m["role"] == "user"]
    for m in recent[:-1]:
        messages.append({"role": "user", "content": m["text"]})
    messages.append({"role": "user", "content": question})

    try:
        resp = client.chat.completions.create(
            model=GROK_MODEL, messages=messages, temperature=0, max_tokens=500,
        )
        cypher = resp.choices[0].message.content.strip()
        cypher = re.sub(r"^```(?:cypher)?\s*\n?", "", cypher)
        cypher = re.sub(r"\n?```\s*$", "", cypher)
        return cypher.strip()
    except Exception as e:
        return f"Grok Error: {e}"


def validate_cypher(cypher: str) -> tuple[bool, str]:
    for pfx in ("Safety Error:", "This system only", "Error:", "Grok Error:"):
        if cypher.startswith(pfx):
            return False, cypher
    upper = cypher.upper()
    for kw in ("CREATE", "DELETE", "MERGE", "REMOVE", "DROP", "DETACH"):
        if re.search(r'\b' + kw + r'\b', upper):
            return False, f"Blocked: write op '{kw}'."
    if re.search(r'\bSET\b', upper):
        return False, "Blocked: write op 'SET'."
    first = upper.strip().split()[0] if cypher.strip() else ""
    if first not in ("MATCH", "OPTIONAL", "CALL"):
        return False, "Query must start with MATCH."
    return True, ""


def generate_answer(question: str, data: list) -> str:
    """Generate a concise NL answer — max 4-5 lines, using sliding window."""
    client = st.session_state.get("grok_client")
    if not client:
        return "Grok not configured."
    if not data:
        return "No matching records found."

    data_str = json.dumps(data[:10], indent=2, default=str)

    try:
        resp = client.chat.completions.create(
            model=GROK_MODEL,
            messages=[
                {"role": "system", "content": (
                    "You are Dodge AI, an SAP O2C analyst. "
                    "RULES: Use ONLY the data provided. Give at max 4-5 line answers. "
                    "Be concise and factual. Use bullet points if needed."
                )},
                {"role": "user", "content": f"Q: {question}\nDATA:\n{data_str}"},
            ],
            temperature=0.2,
            max_tokens=300,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"


def run_chat_query(question: str) -> str:
    """Full pipeline: NL → Cypher → Neo4j → NL answer."""
    cypher = nl_to_cypher(question)
    ok, err = validate_cypher(cypher)
    if not ok:
        return err

    records, keys, error = neo4j_read(cypher)
    if error:
        return f"Neo4j: {error}"

    table = []
    for rec in records:
        row = {}
        for key in keys:
            val = rec[key]
            if hasattr(val, "labels"):
                row[key] = _display(list(val.labels)[0], {k: _ser(v) for k, v in dict(val).items()})
            elif hasattr(val, "type") and hasattr(val, "element_id"):
                row[key] = val.type
            else:
                row[key] = _ser(val)
        table.append(row)

    if not table:
        return "No matching records found in the SAP dataset."

    return generate_answer(question, table)


# ─────────────────────────────────────────────────────────────────────────────
# QUICK-INSPECT A NODE (triggered by user typing a node ID or clicking lookup)
# ─────────────────────────────────────────────────────────────────────────────
def inspect_node(node_data: dict) -> str:
    """Build a chat-friendly node metadata string."""
    props = node_data["props"]
    lines = [f"📍 **{node_data['label']}** node:"]
    for k, v in list(props.items())[:6]:
        if v not in (None, "", []):
            lines.append(f"  • **{k}**: `{v}`")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    api_key = get_api_key()
    if api_key and not st.session_state.grok_ready:
        init_grok(api_key)

    # ── Breadcrumb ──
    st.markdown("""
    <div class="breadcrumb">
        <span>📋</span><span>Mapping</span>
        <span style="color:#dadce0">/</span>
        <span class="current">Order to Cash</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Layout: Graph (75%) | Chat (25%) ──
    col_graph, col_chat = st.columns([3, 1], gap="small")

    # ═══════════════════════════════════════════════════════════════════════
    # LEFT: Graph Canvas
    # ═══════════════════════════════════════════════════════════════════════
    with col_graph:
        # Toggle buttons
        tc1, tc2, tc3 = st.columns([1, 1, 4])
        with tc1:
            st.session_state.minimize = st.toggle("✨ Minimize", value=st.session_state.minimize)
        with tc2:
            st.session_state.hide_granular = st.toggle("🔍 Hide Granular", value=st.session_state.hide_granular)

        # Legend
        legend = '<div class="legend-bar">'
        for name, color in LEGEND:
            legend += f'<div class="legend-item"><span class="legend-dot" style="background:{color}"></span>{name}</div>'
        legend += '</div>'
        st.markdown(legend, unsafe_allow_html=True)

        # Load graph
        if st.session_state.graph_cache is None:
            with st.spinner("Loading O2C graph..."):
                st.session_state.graph_cache = load_graph()

        gd = st.session_state.graph_cache

        if gd.get("error"):
            st.error(f"⚠️ {gd['error']}")
            st.info("Make sure Neo4j is running on bolt://localhost:7687")
        elif gd["nodes"]:
            render_pyvis(gd)
        else:
            st.warning("Graph empty. Run `python ingest_o2c.py` first.")

        # Quick controls
        qc1, qc2, qc3 = st.columns([1, 1, 4])
        with qc1:
            if st.button("🔄 Reload", use_container_width=True):
                st.session_state.graph_cache = None
                st.rerun()
        with qc2:
            # Node inspector input
            node_lookup = st.text_input(
                "🔍 Node ID", placeholder="Paste node ID...",
                label_visibility="collapsed", key="node_lookup",
            )
            if node_lookup and gd.get("nodes"):
                # Search by display name or properties
                found = None
                for n in gd["nodes"].values():
                    if (node_lookup.lower() in n["display"].lower()
                        or node_lookup.lower() in json.dumps(n["props"], default=str).lower()):
                        found = n
                        break
                if found:
                    msg = inspect_node(found)
                    st.session_state.messages.append({"role": "ai", "text": msg})
                    st.rerun()

    # ═══════════════════════════════════════════════════════════════════════
    # RIGHT: Chat Panel
    # ═══════════════════════════════════════════════════════════════════════
    with col_chat:
        # API key setup (only if missing)
        if not api_key:
            st.warning("🔑 Grok API key needed")
            k = st.text_input("Key", type="password", placeholder="xai-...", key="key_in")
            if k:
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("💾 Save", use_container_width=True):
                        save_api_key(k.strip())
                        st.rerun()
                with c2:
                    if st.button("▶ Use", use_container_width=True):
                        init_grok(k.strip())
                        st.rerun()

        # Header
        st.markdown("""
        <div class="chat-header">
            <h3>Chat with Graph</h3>
            <div class="sub">Order to Cash</div>
        </div>
        <div class="agent-card">
            <div class="agent-avatar">D</div>
            <div class="agent-info">
                <h4>Dodge AI</h4>
                <div class="role">Graph Agent</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Message history
        chat_box = st.container(height=380)
        with chat_box:
            for msg in st.session_state.messages:
                if msg["role"] == "ai":
                    with st.chat_message("assistant", avatar="🤖"):
                        st.markdown(msg["text"])
                else:
                    with st.chat_message("user", avatar="🧑"):
                        st.markdown(msg["text"])

        # Status
        st.markdown("""
        <div class="status-bar">
            <span class="status-dot"></span>
            Dodge AI is awaiting instructions
        </div>
        """, unsafe_allow_html=True)

        # Input
        ci, cs = st.columns([4, 1])
        with ci:
            user_input = st.text_input(
                "msg", placeholder="Analyze anything",
                label_visibility="collapsed", key="chat_input",
            )
        with cs:
            send = st.button("Send", type="primary", use_container_width=True)

        # Process
        if send and user_input and user_input.strip():
            q = user_input.strip()
            st.session_state.messages.append({"role": "user", "text": q})

            if not st.session_state.grok_ready:
                st.session_state.messages.append({"role": "ai", "text": "⚠️ Configure Grok API key first."})
            else:
                with st.spinner("Thinking..."):
                    answer = run_chat_query(q)
                st.session_state.messages.append({"role": "ai", "text": answer})

            st.rerun()

        # Clear
        if st.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.messages = [DEFAULTS["messages"][0]]
            st.rerun()


if __name__ == "__main__":
    main()
