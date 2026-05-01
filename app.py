import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import yaml
import time
import logging
from datetime import datetime
from io import StringIO

# Resolve project root
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import backend scripts
from scripts.extract import extract_all
from scripts.transform import transform_all
from scripts.validate import validate_all
from scripts.load import load_all

# Page Config
st.set_page_config(
    page_title="E-commerce ETL Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Premium Tech Look
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .main {
        background: radial-gradient(circle at top left, #0f172a, #020617);
        color: #f8fafc;
    }
    
    .stApp {
        background: radial-gradient(circle at top left, #0f172a, #020617);
    }

    .stButton>button {
        width: 100%;
        border-radius: 12px;
        height: 3.5em;
        background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
        color: white;
        font-weight: 800;
        border: none;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(99, 102, 241, 0.4);
        background: linear-gradient(135deg, #818cf8 0%, #6366f1 100%);
    }
    
    .card {
        background: rgba(30, 41, 59, 0.7);
        backdrop-filter: blur(12px);
        padding: 2rem;
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        margin-bottom: 1.5rem;
        transition: 0.3s;
    }
    
    .card:hover {
        border: 1px solid rgba(99, 102, 241, 0.5);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
    }
    
    .status-pass { color: #10b981; font-weight: 800; }
    .status-fail { color: #ef4444; font-weight: 800; }
    .status-warn { color: #f59e0b; font-weight: 800; }
    
    h1, h2, h3 {
        letter-spacing: -1px;
        font-weight: 800;
    }

    /* Metric Styling */
    [data-testid="stMetricValue"] {
        font-weight: 800;
        font-size: 2.5rem !important;
        color: #6366f1;
    }
    </style>
""", unsafe_allow_html=True)

# Helper: Load Config
def load_config():
    config_path = os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

config = load_config()

# Helper: Load Data
@st.cache_data
def get_table_data(table_name, folder="transformed"):
    path = os.path.join(PROJECT_ROOT, "data", folder, f"{table_name}.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    return None

# Helper: Get Quality Score
def get_quality_score():
    report_path = os.path.join(PROJECT_ROOT, "data", "validated", "validation_report.csv")
    if os.path.exists(report_path):
        df = pd.read_csv(report_path)
        passed = df[df["passed"] == True].shape[0]
        total = df.shape[0]
        return int((passed / total) * 100) if total > 0 else 0
    return 0

# Session State Initialization
if "pipeline_status" not in st.session_state:
    st.session_state.pipeline_status = "Idle"
if "last_run" not in st.session_state:
    st.session_state.last_run = "Never"
if "logs" not in st.session_state:
    st.session_state.logs = ""

# Sidebar Navigation
st.sidebar.title("🛒 ETL Command Center")
PAGES = ["Home", "Monitoring", "Upload Data", "Run Pipeline", "Data Preview", "Validation", "Analytics", "System Logs"]

if "current_page" not in st.session_state:
    st.session_state.current_page = "Home"

# Auto-switch to Run Pipeline if triggered
if st.session_state.get("run_triggered", False):
    st.session_state.current_page = "Run Pipeline"

page = st.sidebar.radio(
    "Navigation", 
    PAGES, 
    index=PAGES.index(st.session_state.current_page)
)
st.session_state.current_page = page

st.sidebar.divider()
st.sidebar.info(f"**Environment:** {config['environment']}")
st.sidebar.info(f"**Warehouse:** {config['load']['target'].upper()}")

# --- PAGE: HOME ---
if page == "Home":
    st.title("🚀 Data Pipeline Command Center")
    st.markdown("Monitor your e-commerce ETL ecosystem in real-time.")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # KPIs
    fact_orders = get_table_data("fact_orders")
    if fact_orders is not None:
        total_revenue = fact_orders["revenue"].sum()
        total_orders = fact_orders["order_id"].nunique()
        aov = total_revenue / total_orders if total_orders > 0 else 0
        total_customers = fact_orders["customer_id"].nunique()
    else:
        total_revenue, total_orders, aov, total_customers = 0, 0, 0, 0

    col1.metric("Total Revenue", f"${total_revenue:,.0f}")
    col2.metric("Total Orders", f"{total_orders:,}")
    col3.metric("Avg Order Value", f"${aov:,.2f}")
    col4.metric("Total Customers", f"{total_customers:,}")

    st.divider()

    col_main, col_side = st.columns([2, 1])
    
    with col_main:
        st.subheader("System Overview")
        q_score = get_quality_score()
        
        # Gauge Chart for Quality Score
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = q_score,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Data Quality Score", 'font': {'size': 24, 'color': 'white'}},
            gauge = {
                'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "white"},
                'bar': {'color': "#6366f1"},
                'bgcolor': "rgba(0,0,0,0)",
                'borderwidth': 2,
                'bordercolor': "white",
                'steps': [
                    {'range': [0, 50], 'color': '#ef4444'},
                    {'range': [50, 80], 'color': '#f59e0b'},
                    {'range': [80, 100], 'color': '#10b981'}],
            }
        ))
        fig_gauge.update_layout(paper_bgcolor="rgba(0,0,0,0)", font={'color': "white", 'family': "Arial"})
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_side:
        st.subheader("Last Execution")
        st.markdown(f"""
        <div class="card">
            <h3>Status: <span class="status-pass">{st.session_state.pipeline_status}</span></h3>
            <p><b>Time:</b> {st.session_state.last_run}</p>
            <p><b>Target:</b> {config['load']['target'].upper()}</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Trigger Pipeline Run"):
            st.session_state.run_triggered = True
            st.rerun()

# --- PAGE: MONITORING (3D Visualization) ---
elif page == "Monitoring":
    st.title("🌐 3D Pipeline Visualization")
    st.markdown("Visual flow of the ETL process from raw files to the analytical warehouse.")

    # Mock Data for Nodes
    nodes = [
        {"id": "Sources", "pos": [0, 0, 0], "color": [99, 102, 241]},
        {"id": "Extract", "pos": [2, 1, 1], "color": [16, 185, 129]},
        {"id": "Transform", "pos": [4, 0, 2], "color": [16, 185, 129]},
        {"id": "Validate", "pos": [6, -1, 1], "color": [245, 158, 11]},
        {"id": "Load", "pos": [8, 0, 0], "color": [99, 102, 241]},
        {"id": "Warehouse", "pos": [10, 0, 1], "color": [79, 70, 229]}
    ]
    
    # Create Lines (Edges)
    edges = []
    for i in range(len(nodes) - 1):
        edges.append({
            "source": nodes[i]["pos"],
            "target": nodes[i+1]["pos"]
        })

    import pydeck as pdk
    
    # Render with Pydeck
    view_state = pdk.ViewState(latitude=0, longitude=0, zoom=11, pitch=45, bearing=0)
    
    # Nodes as ScatterplotLayer
    node_layer = pdk.Layer(
        "ScatterplotLayer",
        pd.DataFrame([{"position": n["pos"], "color": n["color"], "name": n["id"]} for n in nodes]),
        get_position="position",
        get_fill_color="color",
        get_radius=500,
        pickable=True,
    )
    
    # Edges as ArcLayer
    edge_layer = pdk.Layer(
        "ArcLayer",
        pd.DataFrame([{"start": e["source"], "end": e["target"]} for e in edges]),
        get_source_position="start",
        get_target_position="end",
        get_source_color=[99, 102, 241, 150],
        get_target_color=[16, 185, 129, 150],
        get_width=5,
    )

    st.pydeck_chart(pdk.Deck(
        layers=[node_layer, edge_layer],
        initial_view_state=view_state,
        tooltip={"text": "{name}"}
    ))
    
    st.info("💡 Interactive 3D Node Graph: Drag to rotate, scroll to zoom.")

# --- PAGE: UPLOAD DATA ---
elif page == "Upload Data":
    st.title("📥 Data Ingestion")
    st.markdown("Upload and map source datasets to the target schema.")

    # Schema Definitions
    SCHEMAS = {
        "Orders": ["order_id", "customer_id", "product_id", "quantity", "order_date", "status"],
        "Customers": ["customer_id", "name", "email", "location"],
        "Products": ["product_id", "product_name", "category", "price"]
    }

    # Step 1: Selection
    col_u1, col_u2 = st.columns([1, 2])
    with col_u1:
        dataset_type = st.selectbox("Select Dataset Type", list(SCHEMAS.keys()))
    with col_u2:
        uploaded_file = st.file_uploader(f"Upload {dataset_type} CSV", type=["csv"])

    if uploaded_file is not None:
        df_uploaded = pd.read_csv(uploaded_file)
        
        # Step 2: Preview
        with st.expander("👀 Data Preview", expanded=True):
            st.dataframe(df_uploaded.head(10), use_container_width=True)
            st.info(f"Detected Columns: {', '.join(df_uploaded.columns.tolist())}")

        # Step 3: Mapping
        st.subheader("🗺️ Column Mapping")
        required_cols = SCHEMAS[dataset_type]
        mapping = {}
        
        col_m1, col_m2 = st.columns(2)
        for i, req_col in enumerate(required_cols):
            with col_m1 if i % 2 == 0 else col_m2:
                default_idx = 0
                if req_col in df_uploaded.columns:
                    default_idx = df_uploaded.columns.get_loc(req_col)
                
                mapping[req_col] = st.selectbox(
                    f"Required: {req_col}",
                    options=df_uploaded.columns,
                    index=default_idx,
                    key=f"map_{req_col}"
                )

        # Step 4: Validation & Save
        st.divider()
        mapped_values = list(mapping.values())
        has_duplicates = len(mapped_values) != len(set(mapped_values))
        
        if has_duplicates:
            st.error("❌ Duplicate mapping detected!")
        else:
            df_mapped = df_uploaded[mapped_values].copy()
            df_mapped.columns = list(mapping.keys())
            
            c1, c2 = st.columns(2)
            with c1:
                save_mode = st.radio("Save Mode", ["Replace existing data", "Append to existing data"], horizontal=True)
            with c2:
                if st.button("💾 Save to Raw Layer"):
                    target_filename = config['source_files'][dataset_type.lower()]
                    target_path = os.path.join(PROJECT_ROOT, config['paths']['raw_data'], target_filename)
                    
                    try:
                        if save_mode == "Replace existing data" or not os.path.exists(target_path):
                            df_mapped.to_csv(target_path, index=False)
                        else:
                            df_mapped.to_csv(target_path, mode='a', header=False, index=False)
                        
                        st.success(f"Saved to {target_filename}!")
                        st.balloons()
                    except Exception as e:
                        st.error(f"Error: {e}")

# --- PAGE: RUN PIPELINE ---
elif page == "Run Pipeline":
    st.title("⚙️ Pipeline Execution")
    
    auto_run = False
    if st.session_state.get("run_triggered", False):
        auto_run = True
        st.session_state.run_triggered = False
    
    if st.button("🚀 Start Production ETL Run") or auto_run:
        st.session_state.logs = ""
        with st.status("Executing ETL Pipeline Stages...", expanded=True) as status:
            st.write("Initializing...")
            time.sleep(1)
            
            # 1. Extract
            st.write("📥 Extracting data...")
            raw_data = extract_all()
            st.session_state.logs += f"[{datetime.now().strftime('%H:%M:%S')}] INFO: Extract Complete\n"
            
            # 2. Transform
            st.write("🔄 Transforming star schema...")
            star_schema = transform_all(raw_data)
            st.session_state.logs += f"[{datetime.now().strftime('%H:%M:%S')}] INFO: Transform Complete\n"
            
            # 3. Validate
            st.write("✅ Validating quality...")
            results = validate_all(star_schema)
            st.session_state.logs += f"[{datetime.now().strftime('%H:%M:%S')}] INFO: Validation Complete\n"
            
            # 4. Load
            st.write(f"📤 Loading into {config['load']['target']}...")
            load_all(star_schema)
            st.session_state.logs += f"[{datetime.now().strftime('%H:%M:%S')}] INFO: Load Complete\n"
            
            status.update(label="Pipeline Successful!", state="complete", expanded=False)
            
        st.session_state.pipeline_status = "Completed"
        st.session_state.last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.success("ETL Pipeline finished successfully!")
        st.balloons()

    st.subheader("Console Output")
    st.code(st.session_state.logs if st.session_state.logs else "No recent run data.")

# --- PAGE: DATA PREVIEW ---
elif page == "Data Preview":
    st.title("🔍 Data Explorer")
    layer = st.radio("Select Layer", ["Raw", "Transformed"], horizontal=True)
    
    if layer == "Raw":
        table = st.selectbox("Table", ["orders", "customers", "products"])
        df = get_table_data(table, folder="raw")
    else:
        table = st.selectbox("Table", ["fact_orders", "dim_customers", "dim_products", "dim_date"])
        df = get_table_data(table, folder="transformed")

    if df is not None:
        st.dataframe(df.head(100), use_container_width=True)
    else:
        st.warning("Data missing.")

# --- PAGE: VALIDATION ---
elif page == "Validation":
    st.title("🛡️ Quality Report")
    report_path = os.path.join(PROJECT_ROOT, "data", "validated", "validation_report.csv")
    if os.path.exists(report_path):
        report_df = pd.read_csv(report_path)
        c1, c2 = st.columns(2)
        c1.metric("Tests Passed", report_df[report_df["passed"] == True].shape[0])
        c2.metric("Tests Failed", report_df[report_df["passed"] == False].shape[0])
        
        for _, row in report_df.iterrows():
            status_class = "status-pass" if row["passed"] else "status-fail"
            st.markdown(f'<div class="card"><span class="{status_class}">{"✅" if row["passed"] else "❌"} {row["table"]}.{row["check"]}</span><br><small>{row["details"]}</small></div>', unsafe_allow_html=True)
    else:
        st.warning("No report found.")

# --- PAGE: ANALYTICS ---
elif page == "Analytics":
    st.title("📊 Business Intelligence")
    fact = get_table_data("fact_orders")
    dim_date = get_table_data("dim_date")
    dim_prod = get_table_data("dim_products")
    dim_cust = get_table_data("dim_customers")
    
    if fact is not None and dim_date is not None and dim_prod is not None:
        df = fact.merge(dim_date[["date_id", "month", "year", "month_name"]], on="date_id")
        df = df.merge(dim_prod[["product_id", "product_name", "category", "price"]], on="product_id")
        active = df[df["order_status"] != "cancelled"]

        t1, t2, t3 = st.tabs(["Revenue Analysis", "Product Insights", "Customer Value"])
        
        with t1:
            st.subheader("Monthly Revenue Trend")
            monthly = active.groupby(["year", "month", "month_name"])["revenue"].sum().reset_index()
            monthly["period"] = monthly["year"].astype(str) + "-" + monthly["month"].astype(str).str.zfill(2)
            fig_rev = px.line(monthly.sort_values("period"), x="period", y="revenue", markers=True)
            fig_rev.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_rev, use_container_width=True)

        with t2:
            st.subheader("3D Product Performance")
            # Revenue vs Price vs Quantity
            prod_perf = active.groupby("product_name").agg({"revenue": "sum", "price": "mean", "quantity": "sum"}).reset_index()
            fig_3d = px.scatter_3d(prod_perf, x='price', y='quantity', z='revenue', color='revenue', size_max=20, text='product_name')
            fig_3d.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_3d, use_container_width=True)

        with t3:
            st.subheader("Customer Lifetime Value (CLV)")
            if dim_cust is not None:
                clv = active.groupby("customer_id")["revenue"].sum().reset_index()
                clv = clv.merge(dim_cust[["customer_id", "name"]], on="customer_id")
                fig_clv = px.bar(clv.nlargest(15, "revenue"), x="revenue", y="name", orientation="h")
                fig_clv.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_clv, use_container_width=True)
    else:
        st.warning("Data missing.")

# --- PAGE: SYSTEM LOGS ---
elif page == "System Logs":
    st.title("📜 System Logs")
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    log_files = sorted([f for f in os.listdir(log_dir) if f.endswith(".log")], reverse=True)
    
    if log_files:
        selected_log = st.selectbox("Select Log File", log_files)
        with open(os.path.join(log_dir, selected_log), "r") as f:
            log_content = f.readlines()
        
        level_filter = st.multiselect("Filter by Level", ["INFO", "WARNING", "ERROR"], default=["INFO", "WARNING", "ERROR"])
        
        filtered_logs = [line for line in log_content if any(level in line for level in level_filter)]
        
        st.code("".join(filtered_logs))
    else:
        st.info("No log files found.")
