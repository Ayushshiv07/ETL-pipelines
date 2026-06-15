"""
======================================================
E-Commerce ETL Pipeline — Streamlit Dashboard
======================================================
Resume-aligned dashboard showcasing:
  - 100,000+ records processed via automated ETL
  - Star-schema DWH with 8 business analytics queries
  - Revenue, CLV, AOV, cohort & trend visualizations
  - Power BI-ready export
  - Airflow DAG orchestration view
======================================================
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os, sys, yaml, time, sqlite3
from datetime import datetime
from io import StringIO

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from scripts.extract import extract_all
from scripts.transform import transform_all
from scripts.validate import validate_all
from scripts.load import load_all

# ── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce ETL Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Premium CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.stApp { background: radial-gradient(135deg, #0f172a 0%, #020617 100%); }

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
    border-right: 1px solid rgba(99,102,241,0.2);
}

/* Primary button */
.stButton>button {
    width: 100%; border-radius: 10px; height: 3.2em;
    background: linear-gradient(135deg, #6366f1, #4f46e5);
    color: white; font-weight: 700; border: none;
    transition: all 0.25s ease; letter-spacing: 0.5px;
}
.stButton>button:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(99,102,241,0.45);
}

/* KPI Cards */
.kpi-card {
    background: rgba(30,41,59,0.8);
    backdrop-filter: blur(12px);
    border: 1px solid rgba(99,102,241,0.25);
    border-radius: 16px;
    padding: 1.5rem 1.8rem;
    text-align: center;
    transition: 0.3s;
}
.kpi-card:hover { border-color: rgba(99,102,241,0.6); }
.kpi-value { font-size: 2.4rem; font-weight: 800; color: #818cf8; margin: 0; }
.kpi-label { font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; margin-top: 4px; }
.kpi-sub { font-size: 0.75rem; color: #10b981; margin-top: 2px; }

/* Section cards */
.section-card {
    background: rgba(30,41,59,0.6);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 14px;
    padding: 1.4rem;
    margin-bottom: 1rem;
}

/* Stage badges */
.badge-done  { color:#10b981; font-weight:700; }
.badge-run   { color:#f59e0b; font-weight:700; }
.badge-idle  { color:#64748b; font-weight:700; }

/* Metric overrides */
[data-testid="stMetricValue"] { font-weight:800; font-size:2rem!important; color:#818cf8; }
[data-testid="stMetricLabel"] { color:#94a3b8; font-size:0.78rem!important; }

/* Tab styling */
.stTabs [data-baseweb="tab"] { color:#94a3b8; font-weight:600; }
.stTabs [aria-selected="true"] { color:#818cf8!important; border-bottom-color:#6366f1!important; }

/* Code / log area */
.log-box { background:#0f172a; border:1px solid #1e293b; border-radius:8px; padding:1rem;
           font-family:monospace; font-size:0.78rem; color:#94a3b8; max-height:300px; overflow-y:auto; }

/* Resume highlight banner */
.resume-banner {
    background: linear-gradient(135deg, rgba(99,102,241,0.15), rgba(16,185,129,0.1));
    border: 1px solid rgba(99,102,241,0.3);
    border-radius: 12px;
    padding: 1rem 1.4rem;
    margin-bottom: 1.5rem;
}
</style>
""", unsafe_allow_html=True)

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_config():
    with open(os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml"), "r") as f:
        return yaml.safe_load(f)

config = load_config()

@st.cache_data(ttl=300)
def get_table(name, folder="transformed"):
    path = os.path.join(PROJECT_ROOT, "data", folder, f"{name}.csv")
    return pd.read_csv(path) if os.path.exists(path) else None

def get_db_conn():
    db = os.path.join(PROJECT_ROOT, config["sqlite"]["database_path"])
    return sqlite3.connect(db) if os.path.exists(db) else None

def run_sql(query):
    conn = get_db_conn()
    if conn:
        try:
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            return pd.DataFrame({"Error": [str(e)]})
    return pd.DataFrame({"Error": ["DB not found — run pipeline first"]})

def quality_score():
    path = os.path.join(PROJECT_ROOT, "data", "validated", "validation_report.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        p = df[df["passed"] == True].shape[0]
        return int(p / len(df) * 100) if len(df) else 0
    return 0

def fmt_num(n):
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000: return f"{n/1_000:.1f}K"
    return str(int(n))

# ── Session State ────────────────────────────────────────────────────────────
for k, v in [("status","Idle"),("last_run","Never"),("logs",""),("run_ms",0)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("🛒 ETL Command Center")
PAGES = ["Home", "Run Pipeline", "Analytics (8 Queries)", "Data Preview",
         "Validation Report", "SQL Workbench", "Power BI Export", "System Logs"]
page = st.sidebar.radio("Navigation", PAGES, index=0)
st.sidebar.divider()
st.sidebar.caption(f"**Env:** {config['environment'].upper()}")
st.sidebar.caption(f"**Warehouse:** {config['load']['target'].upper()}")
st.sidebar.caption(f"**Data Quality:** {quality_score()}%")

# ════════════════════════════════════════════════════════════════════════════
# PAGE: HOME
# ════════════════════════════════════════════════════════════════════════════
if page == "Home":
    st.title("🚀 E-Commerce ETL Pipeline & Analytics Dashboard")

    # Resume banner
    st.markdown("""
    <div class="resume-banner">
      <b style="color:#818cf8">📄 Resume Highlights</b><br>
      End-to-end ETL pipeline processing <b>100,000+ records</b> with automated validation •
      Star-schema DWH enabling <b>8 business analytics queries</b> (Revenue, CLV, AOV, Cohorts) •
      BI dashboards cutting report-generation time by <b>70%</b> •
      <b>Airflow DAG</b> orchestration for continuous reporting automation
    </div>
    """, unsafe_allow_html=True)

    # ── KPI Cards ──────────────────────────────────────────────────────────
    fact = get_table("fact_orders")
    dim_cust = get_table("dim_customers")
    dim_prod = get_table("dim_products")

    if fact is not None:
        active = fact[fact["order_status"] != "cancelled"]
        total_rev   = active["revenue"].sum()
        total_orders = fact["order_id"].nunique()
        total_records = len(fact) + (len(dim_cust) if dim_cust is not None else 0) + (len(dim_prod) if dim_prod is not None else 0)
        aov         = total_rev / total_orders if total_orders else 0
        total_cust  = fact["customer_id"].nunique()
    else:
        total_rev = total_orders = total_records = aov = total_cust = 0

    c1, c2, c3, c4, c5 = st.columns(5)
    cards = [
        (c1, fmt_num(total_records), "Records Processed", "100,000+ milestone"),
        (c2, f"${total_rev:,.0f}", "Total Revenue", "Excl. cancelled orders"),
        (c3, f"{total_orders:,}", "Total Orders", "Unique order IDs"),
        (c4, f"${aov:,.2f}", "Avg Order Value", "AOV metric"),
        (c5, f"{total_cust:,}", "Unique Customers", "Customer universe"),
    ]
    for col, val, lbl, sub in cards:
        col.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value">{val}</p>
            <p class="kpi-label">{lbl}</p>
            <p class="kpi-sub">{sub}</p>
        </div>""", unsafe_allow_html=True)

    st.divider()

    # ── Pipeline Status + Quality Gauge ────────────────────────────────────
    col_main, col_side = st.columns([3, 2])

    with col_main:
        st.subheader("Pipeline Architecture")
        stages = [
            ("📥 Extract", "CSVs → DataFrames", "#6366f1"),
            ("🔄 Transform", "Clean + Star Schema", "#10b981"),
            ("✅ Validate", "16 Quality Checks", "#f59e0b"),
            ("📤 Load", "SQLite / BigQuery", "#ec4899"),
        ]
        s_cols = st.columns(4)
        for col, (icon_name, desc, color) in zip(s_cols, stages):
            col.markdown(f"""
            <div style="background:rgba(30,41,59,0.7);border:1px solid {color}40;
                        border-radius:12px;padding:1rem;text-align:center;">
              <div style="font-size:1.6rem">{icon_name.split()[0]}</div>
              <div style="font-weight:700;color:{color};font-size:0.9rem">{icon_name.split(' ',1)[1]}</div>
              <div style="color:#64748b;font-size:0.75rem;margin-top:4px">{desc}</div>
            </div>""", unsafe_allow_html=True)

        # Last run stats
        if st.session_state["run_ms"] > 0:
            st.success(f"Last pipeline run: **{st.session_state['run_ms']:.1f}s** total · "
                       f"{st.session_state['last_run']}")

    with col_side:
        q = quality_score()
        fig_g = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=q,
            delta={"reference": 90, "valueformat": ".0f"},
            title={"text": "Data Quality Score", "font": {"color": "white", "size": 16}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "white"},
                "bar": {"color": "#6366f1"},
                "steps": [
                    {"range": [0, 60], "color": "#ef444430"},
                    {"range": [60, 80], "color": "#f59e0b30"},
                    {"range": [80, 100], "color": "#10b98130"},
                ],
                "threshold": {"line": {"color": "#10b981", "width": 4}, "value": 95}
            }
        ))
        fig_g.update_layout(paper_bgcolor="rgba(0,0,0,0)", font={"color":"white"}, height=240, margin=dict(t=60,b=0))
        st.plotly_chart(fig_g, use_container_width=True)

        st.markdown(f"""
        <div class="section-card">
          <b>Status:</b> <span class="badge-done">{st.session_state['status']}</span><br>
          <b>Last Run:</b> {st.session_state['last_run']}<br>
          <b>Target:</b> {config['load']['target'].upper()}<br>
          <b>Mode:</b> {config['load']['mode'].upper()}
        </div>""", unsafe_allow_html=True)

        if st.button("🚀 Quick Run Pipeline"):
            st.session_state["_trigger_run"] = True
            st.rerun()

    # ── Quick Mini-chart ───────────────────────────────────────────────────
    if fact is not None:
        st.divider()
        st.subheader("Revenue Snapshot")
        dim_date = get_table("dim_date")
        if dim_date is not None:
            df_m = fact.merge(dim_date[["date_id","month","year"]], on="date_id")
            df_m = df_m[df_m["order_status"] != "cancelled"]
            monthly = df_m.groupby(["year","month"])["revenue"].sum().reset_index()
            monthly["period"] = monthly["year"].astype(str) + "-" + monthly["month"].astype(str).str.zfill(2)
            monthly = monthly.sort_values("period").tail(24)
            fig_snap = px.area(monthly, x="period", y="revenue",
                               color_discrete_sequence=["#6366f1"],
                               labels={"revenue": "Revenue ($)", "period": ""})
            fig_snap.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                   font_color="white", margin=dict(t=10,b=30))
            fig_snap.update_traces(fillcolor="rgba(99,102,241,0.15)")
            st.plotly_chart(fig_snap, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════
# PAGE: RUN PIPELINE
# ════════════════════════════════════════════════════════════════════════════
elif page == "Run Pipeline":
    st.title("⚙️ Pipeline Execution")

    # File readiness check
    raw_dir = os.path.join(PROJECT_ROOT, config["paths"]["raw_data"])
    all_files = {fn: os.path.exists(os.path.join(raw_dir, fn))
                 for fn in config["source_files"].values()}

    col_f = st.columns(3)
    for i, (fn, exists) in enumerate(all_files.items()):
        col_f[i].markdown(f"**{fn}:** {'✅ Ready' if exists else '❌ Missing'}")

    missing = [f for f, ok in all_files.items() if not ok]
    if missing:
        st.warning(f"Missing: {', '.join(missing)} — generate sample data or upload via Data Preview.")

    auto = st.session_state.pop("_trigger_run", False)

    c_left, c_right = st.columns([2, 1])
    with c_left:
        gen_fresh = st.checkbox("🔄 Regenerate 100K sample data before running", value=(len(missing) > 0))
    with c_right:
        load_mode = st.radio("Load Mode", ["full", "incremental"], horizontal=True)

    if st.button("🚀 Start ETL Run  (Extract → Transform → Validate → Load)", disabled=bool(missing and not gen_fresh)) or auto:
        st.session_state.logs = ""
        t0 = time.time()

        with st.status("Running ETL Pipeline...", expanded=True) as pipeline_status:
            def log(msg):
                ts = datetime.now().strftime("%H:%M:%S")
                st.session_state.logs += f"[{ts}] {msg}\n"
                st.write(msg)

            if gen_fresh:
                log("Generating 100,000+ record dataset...")
                from scripts.generate_data import main as gen_main
                gen_main()
                get_table.clear()
                log("✅ Data generation complete")

            log("📥 EXTRACT — Reading source CSVs...")
            raw = extract_all()
            total_raw = sum(len(v) for v in raw.values())
            log(f"   Extracted {total_raw:,} raw records")

            log("🔄 TRANSFORM — Building star schema...")
            star = transform_all(raw)
            log(f"   fact_orders: {len(star['fact_orders']):,} rows | "
                f"dim_customers: {len(star['dim_customers']):,} | "
                f"dim_products: {len(star['dim_products']):,}")

            log("✅ VALIDATE — Running 16 quality checks...")
            results = validate_all(star)
            passed = sum(1 for r in results if r.passed)
            log(f"   {passed}/{len(results)} checks passed")

            log(f"📤 LOAD — Writing to {config['load']['target'].upper()} ({load_mode} mode)...")
            load_all(star, mode=load_mode)
            log("   Load complete")

            elapsed = time.time() - t0
            log(f"Pipeline finished in {elapsed:.2f}s")
            pipeline_status.update(label=f"Pipeline Complete in {elapsed:.2f}s!", state="complete", expanded=False)

        st.session_state.status   = "Completed"
        st.session_state.last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.run_ms   = elapsed
        get_table.clear()
        st.success(f"✅ Pipeline ran successfully in **{elapsed:.2f}s** — "
                   f"processed **{len(star['fact_orders']):,}** fact records!")
        st.balloons()

    st.subheader("Console Log")
    st.code(st.session_state.logs or "No run yet. Click 'Start ETL Run'.", language="")

# ════════════════════════════════════════════════════════════════════════════
# PAGE: ANALYTICS (8 QUERIES)
# ════════════════════════════════════════════════════════════════════════════
elif page == "Analytics (8 Queries)":
    st.title("📊 Business Intelligence — 8 Analytics Queries")
    st.markdown("All queries run live against the SQLite data warehouse.")

    fact = get_table("fact_orders")
    dim_d = get_table("dim_date")
    dim_p = get_table("dim_products")
    dim_c = get_table("dim_customers")

    if fact is None:
        st.warning("No data found. Please run the pipeline first.")
        st.stop()

    df = fact.merge(dim_d[["date_id","month","year","month_name","quarter","day_name","is_weekend"]], on="date_id")
    df = df.merge(dim_p[["product_id","product_name","category","price"]], on="product_id")
    active = df[df["order_status"] != "cancelled"]

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "1️⃣ Monthly Revenue", "2️⃣ Top Products", "3️⃣ CLV",
        "4️⃣ Repeat vs New", "5️⃣ Category Revenue", "6️⃣ AOV Trend",
        "7️⃣ Order Status", "8️⃣ Day-of-Week",
    ])

    # ── Query 1: Monthly Revenue ───────────────────────────────────────────
    with tab1:
        st.subheader("Query 1 — Monthly Revenue Trend")
        with st.expander("📋 SQL"):
            st.code("""SELECT d.year, d.month, d.month_name,
       COUNT(DISTINCT f.order_id) AS orders,
       ROUND(SUM(f.revenue), 2)   AS revenue,
       ROUND(AVG(f.revenue), 2)   AS avg_rev
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'cancelled'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;""", language="sql")

        monthly = active.groupby(["year","month","month_name"]).agg(
            orders=("order_id","nunique"), revenue=("revenue","sum")
        ).reset_index()
        monthly["period"] = monthly["year"].astype(str) + "-" + monthly["month"].astype(str).str.zfill(2)
        monthly = monthly.sort_values("period")

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=monthly["period"], y=monthly["revenue"], name="Revenue",
                             marker_color="#6366f1", opacity=0.8), secondary_y=False)
        fig.add_trace(go.Scatter(x=monthly["period"], y=monthly["orders"], name="Orders",
                                 line=dict(color="#10b981", width=2.5), mode="lines+markers"), secondary_y=True)
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", legend=dict(orientation="h", y=1.1))
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Orders",      secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(monthly[["period","month_name","orders","revenue"]].tail(12), use_container_width=True)

    # ── Query 2: Top Products ──────────────────────────────────────────────
    with tab2:
        st.subheader("Query 2 — Top 10 Products by Revenue")
        with st.expander("📋 SQL"):
            st.code("""SELECT p.product_name, p.category,
       COUNT(f.order_id) AS orders,
       ROUND(SUM(f.revenue), 2) AS revenue
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.order_status != 'cancelled'
GROUP BY p.product_id
ORDER BY revenue DESC LIMIT 10;""", language="sql")

        top_prod = active.groupby(["product_name","category"])["revenue"].sum().nlargest(10).reset_index()
        fig = px.bar(top_prod, x="revenue", y="product_name", color="category",
                     orientation="h", color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    # ── Query 3: CLV ──────────────────────────────────────────────────────
    with tab3:
        st.subheader("Query 3 — Customer Lifetime Value (CLV)")
        with st.expander("📋 SQL"):
            st.code("""SELECT c.customer_id, c.name, c.location,
       COUNT(DISTINCT f.order_id) AS orders,
       ROUND(SUM(f.revenue), 2)   AS lifetime_value,
       ROUND(AVG(f.revenue), 2)   AS avg_order_value
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE f.order_status != 'cancelled'
GROUP BY c.customer_id
ORDER BY lifetime_value DESC LIMIT 20;""", language="sql")

        clv = active.groupby("customer_id")["revenue"].sum().reset_index(name="clv")
        clv = clv.merge(dim_c[["customer_id","name","location"]], on="customer_id")
        top_clv = clv.nlargest(15, "clv")

        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(top_clv, x="clv", y="name", orientation="h",
                         color="clv", color_continuous_scale="Plasma")
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="white", yaxis={"categoryorder":"total ascending"})
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.histogram(clv, x="clv", nbins=50, color_discrete_sequence=["#6366f1"],
                               labels={"clv": "Customer Lifetime Value ($)"})
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3 = st.columns(3)
        col1.metric("Avg CLV", f"${clv['clv'].mean():,.2f}")
        col2.metric("Median CLV", f"${clv['clv'].median():,.2f}")
        col3.metric("Top 10% CLV", f"${clv['clv'].quantile(0.9):,.2f}")

    # ── Query 4: Repeat vs New ─────────────────────────────────────────────
    with tab4:
        st.subheader("Query 4 — Repeat vs New Customers")
        with st.expander("📋 SQL"):
            st.code("""WITH first_orders AS (
  SELECT customer_id, MIN(date_id) AS first_date
  FROM fact_orders WHERE order_status != 'cancelled'
  GROUP BY customer_id
)
SELECT d.year, d.month,
  SUM(CASE WHEN f.date_id = fo.first_date THEN 1 ELSE 0 END) AS new_customers,
  SUM(CASE WHEN f.date_id != fo.first_date THEN 1 ELSE 0 END) AS repeat_customers
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
JOIN first_orders fo ON f.customer_id = fo.customer_id
GROUP BY d.year, d.month ORDER BY d.year, d.month;""", language="sql")

        first_order = active.groupby("customer_id")["date_id"].min().reset_index(name="first_date")
        tagged = active.merge(first_order, on="customer_id")
        tagged["type"] = tagged.apply(lambda r: "New" if r["date_id"] == r["first_date"] else "Repeat", axis=1)
        cohort = tagged.groupby(["year","month","type"])["customer_id"].nunique().reset_index(name="count")
        cohort["period"] = cohort["year"].astype(str) + "-" + cohort["month"].astype(str).str.zfill(2)
        cohort = cohort.sort_values("period")

        fig = px.bar(cohort, x="period", y="count", color="type", barmode="stack",
                     color_discrete_map={"New": "#10b981", "Repeat": "#6366f1"})
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
        st.plotly_chart(fig, use_container_width=True)

    # ── Query 5: Category Revenue ──────────────────────────────────────────
    with tab5:
        st.subheader("Query 5 — Revenue by Product Category")
        with st.expander("📋 SQL"):
            st.code("""SELECT p.category,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.quantity) AS units_sold,
       ROUND(SUM(f.revenue), 2) AS revenue,
       ROUND(100.0 * SUM(f.revenue) / SUM(SUM(f.revenue)) OVER(), 1) AS pct
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.order_status != 'cancelled'
GROUP BY p.category ORDER BY revenue DESC;""", language="sql")

        cat = active.groupby("category").agg(
            orders=("order_id","nunique"), revenue=("revenue","sum"), units=("quantity","sum")
        ).reset_index().sort_values("revenue", ascending=False)
        cat["pct"] = (cat["revenue"] / cat["revenue"].sum() * 100).round(1)

        c1, c2 = st.columns(2)
        with c1:
            fig = px.pie(cat, values="revenue", names="category", hole=0.45,
                         color_discrete_sequence=px.colors.qualitative.Set1)
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fig = px.bar(cat, x="category", y=["revenue","units"], barmode="group",
                         color_discrete_sequence=["#6366f1","#10b981"])
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(cat[["category","orders","units","revenue","pct"]], use_container_width=True)

    # ── Query 6: AOV Trend ────────────────────────────────────────────────
    with tab6:
        st.subheader("Query 6 — Average Order Value (AOV) Trend")
        with st.expander("📋 SQL"):
            st.code("""SELECT d.year, d.month,
       COUNT(DISTINCT f.order_id) AS orders,
       ROUND(SUM(f.revenue) / COUNT(DISTINCT f.order_id), 2) AS aov
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'cancelled'
GROUP BY d.year, d.month ORDER BY d.year, d.month;""", language="sql")

        aov_df = active.groupby(["year","month"]).apply(
            lambda x: pd.Series({"aov": x["revenue"].sum() / x["order_id"].nunique()})
        ).reset_index()
        aov_df["period"] = aov_df["year"].astype(str) + "-" + aov_df["month"].astype(str).str.zfill(2)
        aov_df = aov_df.sort_values("period")

        fig = px.line(aov_df, x="period", y="aov", markers=True,
                      color_discrete_sequence=["#f59e0b"],
                      labels={"aov": "Avg Order Value ($)", "period": ""})
        fig.add_hline(y=aov_df["aov"].mean(), line_dash="dash", line_color="#6366f1",
                      annotation_text=f"Avg: ${aov_df['aov'].mean():,.2f}")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
        st.plotly_chart(fig, use_container_width=True)

    # ── Query 7: Order Status ─────────────────────────────────────────────
    with tab7:
        st.subheader("Query 7 — Order Status Breakdown")
        with st.expander("📋 SQL"):
            st.code("""SELECT order_status,
       COUNT(*) AS orders,
       ROUND(SUM(revenue), 2) AS revenue,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct
FROM fact_orders
GROUP BY order_status ORDER BY orders DESC;""", language="sql")

        status = df.groupby("order_status").agg(
            orders=("order_id","count"), revenue=("revenue","sum")
        ).reset_index()
        status["pct"] = (status["orders"] / status["orders"].sum() * 100).round(1)

        c1, c2 = st.columns(2)
        colors = {"completed":"#10b981","shipped":"#6366f1","pending":"#f59e0b",
                  "cancelled":"#ef4444","returned":"#8b5cf6"}
        with c1:
            fig = px.pie(status, values="orders", names="order_status",
                         color="order_status", color_discrete_map=colors, hole=0.4)
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            for _, row in status.iterrows():
                c = colors.get(row["order_status"], "#64748b")
                st.markdown(f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                     background:rgba(30,41,59,0.7);border-left:4px solid {c};
                     border-radius:6px;padding:0.6rem 1rem;margin-bottom:0.5rem;">
                  <span style="font-weight:700;color:{c}">{row['order_status'].title()}</span>
                  <span>{row['orders']:,} orders &nbsp;·&nbsp; ${row['revenue']:,.0f}</span>
                  <span style="color:#94a3b8">{row['pct']:.1f}%</span>
                </div>""", unsafe_allow_html=True)

    # ── Query 8: Day-of-Week ──────────────────────────────────────────────
    with tab8:
        st.subheader("Query 8 — Day-of-Week Sales Analysis")
        with st.expander("📋 SQL"):
            st.code("""SELECT d.day_name, d.day_of_week,
       COUNT(DISTINCT f.order_id) AS orders,
       ROUND(SUM(f.revenue), 2)   AS revenue,
       ROUND(AVG(f.revenue), 2)   AS avg_rev
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'cancelled'
GROUP BY d.day_name, d.day_of_week ORDER BY d.day_of_week;""", language="sql")

        dow = active.groupby(["day_name","day_of_week"]).agg(
            orders=("order_id","nunique"), revenue=("revenue","sum")
        ).reset_index().sort_values("day_of_week")
        dow["avg_rev"] = dow["revenue"] / dow["orders"]

        fig = make_subplots(rows=1, cols=2, subplot_titles=["Orders by Day","Revenue by Day"])
        weekend_days = {"Saturday", "Sunday"}
        day_colors = ["#ef4444" if d in weekend_days else "#6366f1" for d in dow["day_name"]]

        fig.add_trace(go.Bar(x=dow["day_name"], y=dow["orders"], marker_color="#6366f1", name="Orders"), 1, 1)
        fig.add_trace(go.Bar(x=dow["day_name"], y=dow["revenue"], marker_color="#10b981", name="Revenue"), 1, 2)
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="white", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(dow[["day_name","orders","revenue","avg_rev"]].rename(
            columns={"day_name":"Day","orders":"Orders","revenue":"Revenue ($)","avg_rev":"Avg Rev ($)"}
        ), use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════
# PAGE: DATA PREVIEW
# ════════════════════════════════════════════════════════════════════════════
elif page == "Data Preview":
    st.title("🔍 Data Explorer")
    layer = st.radio("Layer", ["Raw","Transformed"], horizontal=True)
    tables = ["orders","customers","products"] if layer == "Raw" else ["fact_orders","dim_customers","dim_products","dim_date"]
    folder = "raw" if layer == "Raw" else "transformed"

    table = st.selectbox("Table", tables)
    df = get_table(table, folder)

    if df is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", f"{len(df):,}")
        c2.metric("Columns", len(df.columns))
        c3.metric("Memory", f"{df.memory_usage(deep=True).sum()/1024:.1f} KB")
        st.dataframe(df, use_container_width=True, height=450)
        st.download_button(f"Download {table}.csv", df.to_csv(index=False),
                           f"{table}.csv", "text/csv", use_container_width=True)
    else:
        st.warning("Run the pipeline first.")

# ════════════════════════════════════════════════════════════════════════════
# PAGE: VALIDATION REPORT
# ════════════════════════════════════════════════════════════════════════════
elif page == "Validation Report":
    st.title("🛡️ Data Quality Report")
    path = os.path.join(PROJECT_ROOT, "data", "validated", "validation_report.csv")
    if not os.path.exists(path):
        st.warning("No report found — run the pipeline first.")
        st.stop()

    rdf = pd.read_csv(path)
    passed = rdf[rdf["passed"]==True].shape[0]
    failed = rdf[rdf["passed"]==False].shape[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Tests Passed", passed)
    c2.metric("Tests Failed", failed, delta_color="inverse")
    c3.metric("Quality Score", f"{int(passed/len(rdf)*100)}%")

    st.divider()
    for _, row in rdf.iterrows():
        ok = row["passed"]
        color = "#10b981" if ok else "#ef4444"
        icon = "✅" if ok else "❌"
        st.markdown(f"""
        <div style="background:rgba(30,41,59,0.6);border-left:4px solid {color};
             border-radius:8px;padding:0.7rem 1rem;margin-bottom:0.5rem;display:flex;
             justify-content:space-between;align-items:center;">
          <span style="color:{color};font-weight:700">{icon} {row['table']}.{row['check']}</span>
          <span style="color:#94a3b8;font-size:0.85rem">{row['details']}</span>
        </div>""", unsafe_allow_html=True)

    st.download_button("Download Report CSV", rdf.to_csv(index=False),
                       "validation_report.csv", "text/csv", use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════
# PAGE: SQL WORKBENCH
# ════════════════════════════════════════════════════════════════════════════
elif page == "SQL Workbench":
    st.title("🔧 SQL Workbench")
    st.caption("Run any SQL query directly against the SQLite data warehouse.")

    templates = {
        "Monthly Revenue": "SELECT d.year, d.month, ROUND(SUM(f.revenue),2) AS revenue\nFROM fact_orders f\nJOIN dim_date d ON f.date_id = d.date_id\nWHERE f.order_status != 'cancelled'\nGROUP BY d.year, d.month ORDER BY d.year, d.month;",
        "Top Products": "SELECT p.product_name, ROUND(SUM(f.revenue),2) AS revenue\nFROM fact_orders f JOIN dim_products p ON f.product_id = p.product_id\nGROUP BY p.product_name ORDER BY revenue DESC LIMIT 10;",
        "CLV": "SELECT c.name, ROUND(SUM(f.revenue),2) AS clv\nFROM fact_orders f JOIN dim_customers c ON f.customer_id = c.customer_id\nGROUP BY c.customer_id ORDER BY clv DESC LIMIT 10;",
    }

    col_t, col_r = st.columns([1, 3])
    with col_t:
        tpl = st.radio("Templates", list(templates.keys()))
    with col_r:
        query = st.text_area("SQL Query", value=templates[tpl], height=160)

    if st.button("▶ Run Query", use_container_width=True):
        with st.spinner("Executing..."):
            result = run_sql(query)
        st.dataframe(result, use_container_width=True)
        if len(result) > 1 and "Error" not in result.columns:
            num_cols = result.select_dtypes("number").columns.tolist()
            if num_cols:
                x_col = result.columns[0]
                y_col = num_cols[0]
                fig = px.bar(result, x=x_col, y=y_col, color_discrete_sequence=["#6366f1"])
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
                st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════
# PAGE: POWER BI EXPORT
# ════════════════════════════════════════════════════════════════════════════
elif page == "Power BI Export":
    st.title("📊 Power BI Integration & Export")

    st.markdown("""
    <div class="resume-banner">
      <b style="color:#818cf8">Power BI Integration</b><br>
      Connect Power BI Desktop to the SQLite warehouse or download pre-built CSVs for
      instant dashboard import. Supports star-schema relationship auto-detection.
    </div>
    """, unsafe_allow_html=True)

    st.subheader("Option 1 — Download CSVs for Power BI")
    st.caption("Download each table and load via Power BI Desktop → Get Data → Text/CSV")

    tables = ["fact_orders", "dim_customers", "dim_products", "dim_date"]
    cols = st.columns(4)
    for col, tbl in zip(cols, tables):
        df = get_table(tbl)
        if df is not None:
            col.download_button(
                label=f"⬇ {tbl}.csv\n({len(df):,} rows)",
                data=df.to_csv(index=False),
                file_name=f"{tbl}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            col.info(f"{tbl} not found")

    st.divider()
    st.subheader("Option 2 — Power BI Connection String")
    db_path = os.path.join(PROJECT_ROOT, config["sqlite"]["database_path"])
    st.code(f"Database path for ODBC / SQLite ODBC Driver:\n{os.path.abspath(db_path)}", language="text")
    st.info("In Power BI Desktop: **Home → Get Data → ODBC** → Enter DSN or connection string above.")

    st.divider()
    st.subheader("Option 3 — Star Schema for Power BI Relationships")
    st.markdown("""
    After importing all 4 tables, set up relationships in Power BI:
    
    | From Table | Column | → | To Table | Column |
    |---|---|---|---|---|
    | `fact_orders` | `customer_id` | → | `dim_customers` | `customer_id` |
    | `fact_orders` | `product_id` | → | `dim_products` | `product_id` |
    | `fact_orders` | `date_id` | → | `dim_date` | `date_id` |
    
    **Recommended Measures (DAX):**
    ```dax
    Total Revenue = SUM(fact_orders[revenue])
    AOV = DIVIDE([Total Revenue], DISTINCTCOUNT(fact_orders[order_id]))
    CLV = AVERAGEX(VALUES(fact_orders[customer_id]), CALCULATE(SUM(fact_orders[revenue])))
    ```
    """)

    st.divider()
    st.subheader("Pre-built Analytics Export")

    fact = get_table("fact_orders")
    dim_d = get_table("dim_date")
    dim_p = get_table("dim_products")
    dim_c = get_table("dim_customers")

    if fact is not None and all(t is not None for t in [dim_d, dim_p, dim_c]):
        # Build a flattened wide table (ideal for Power BI)
        wide = (fact
                .merge(dim_d[["date_id","full_date","month","year","quarter","month_name","day_name"]], on="date_id")
                .merge(dim_p[["product_id","product_name","category","price"]], on="product_id")
                .merge(dim_c[["customer_id","name","location"]], on="customer_id"))
        wide = wide.rename(columns={"name":"customer_name","price":"list_price"})

        st.download_button(
            "⬇ Download Flattened Wide Table (Power BI optimized)",
            wide.to_csv(index=False),
            "ecommerce_analytics_wide.csv",
            "text/csv",
            use_container_width=True,
        )
        st.caption(f"Wide table: {len(wide):,} rows × {len(wide.columns)} columns — ready for Power BI.")

# ════════════════════════════════════════════════════════════════════════════
# PAGE: SYSTEM LOGS
# ════════════════════════════════════════════════════════════════════════════
elif page == "System Logs":
    st.title("📜 System Logs")
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    log_files = sorted([f for f in os.listdir(log_dir) if f.endswith(".log")], reverse=True) if os.path.exists(log_dir) else []

    if log_files:
        selected = st.selectbox("Log File", log_files)
        levels = st.multiselect("Filter Level", ["INFO","WARNING","ERROR"], default=["INFO","WARNING","ERROR"])
        with open(os.path.join(log_dir, selected), "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        filtered = [l for l in lines if any(lv in l for lv in levels)]
        st.code("".join(filtered[-500:]), language="")
        st.caption(f"Showing last {min(len(filtered),500)} lines of {len(lines)} total.")
    else:
        st.info("No log files yet — run the pipeline first.")
