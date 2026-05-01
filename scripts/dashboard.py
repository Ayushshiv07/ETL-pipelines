"""
======================================================
E-commerce ETL Pipeline — Interactive Dashboard
======================================================
Generates a Plotly HTML dashboard with key e-commerce KPIs.
Reads from the SQLite data warehouse or transformed CSVs.

Usage:
    python scripts/dashboard.py
    -> Opens data/validated/dashboard.html
======================================================
"""

import os, sys, logging
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_data():
    """Load star schema from transformed CSVs."""
    t_dir = os.path.join(PROJECT_ROOT, "data", "transformed")
    data = {}
    for tbl in ["fact_orders", "dim_customers", "dim_products", "dim_date"]:
        data[tbl] = pd.read_csv(os.path.join(t_dir, f"{tbl}.csv"))
    return data


def build_dashboard(data):
    """Build a multi-chart Plotly dashboard."""
    fact = data["fact_orders"]
    dim_date = data["dim_date"]
    dim_prod = data["dim_products"]
    dim_cust = data["dim_customers"]

    # Merge fact with dimensions
    df = fact.merge(dim_date[["date_id","month","year","month_name"]], on="date_id")
    df = df.merge(dim_prod[["product_id","product_name","category"]], on="product_id")

    # Filter out cancelled orders for revenue analysis
    active = df[df["order_status"] != "cancelled"]

    # ---- 1. Monthly Revenue Trend ----
    monthly = active.groupby(["year","month","month_name"]).agg(
        revenue=("revenue","sum"), orders=("order_id","nunique")
    ).reset_index().sort_values(["year","month"])
    monthly["period"] = monthly["year"].astype(str) + "-" + monthly["month"].astype(str).str.zfill(2)

    # ---- 2. Top Products ----
    top_products = active.groupby(["product_name","category"]).agg(
        revenue=("revenue","sum")
    ).reset_index().nlargest(10, "revenue")

    # ---- 3. Category Revenue ----
    category = active.groupby("category").agg(
        revenue=("revenue","sum")
    ).reset_index().sort_values("revenue", ascending=False)

    # ---- 4. CLV Distribution ----
    clv = active.groupby("customer_id").agg(
        lifetime_value=("revenue","sum")
    ).reset_index()

    # ---- 5. Order Status ----
    status = df.groupby("order_status").agg(
        count=("order_id","count")
    ).reset_index()

    # ---- Build Dashboard ----
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            "Monthly Revenue Trend",
            "Top 10 Products by Revenue",
            "Revenue by Category",
            "Customer Lifetime Value Distribution",
            "Order Status Breakdown",
            "Monthly Order Count Trend",
        ),
        specs=[
            [{"type": "scatter"}, {"type": "bar"}],
            [{"type": "bar"}, {"type": "histogram"}],
            [{"type": "pie"}, {"type": "scatter"}],
        ],
        vertical_spacing=0.1,
        horizontal_spacing=0.08,
    )

    # 1. Revenue trend line
    fig.add_trace(go.Scatter(
        x=monthly["period"], y=monthly["revenue"],
        mode="lines+markers", name="Revenue",
        line=dict(color="#6366f1", width=3),
        marker=dict(size=6),
    ), row=1, col=1)

    # 2. Top products bar
    fig.add_trace(go.Bar(
        y=top_products["product_name"], x=top_products["revenue"],
        orientation="h", name="Top Products",
        marker_color="#10b981",
    ), row=1, col=2)

    # 3. Category revenue bar
    fig.add_trace(go.Bar(
        x=category["category"], y=category["revenue"],
        name="Category Revenue",
        marker_color="#f59e0b",
    ), row=2, col=1)

    # 4. CLV histogram
    fig.add_trace(go.Histogram(
        x=clv["lifetime_value"], nbinsx=30,
        name="CLV Distribution",
        marker_color="#ef4444",
    ), row=2, col=2)

    # 5. Order status pie
    fig.add_trace(go.Pie(
        labels=status["order_status"], values=status["count"],
        name="Order Status",
        marker=dict(colors=["#10b981","#f59e0b","#6366f1","#ef4444","#8b5cf6"]),
    ), row=3, col=1)

    # 6. Monthly orders trend
    fig.add_trace(go.Scatter(
        x=monthly["period"], y=monthly["orders"],
        mode="lines+markers", name="Orders",
        line=dict(color="#ec4899", width=3),
        marker=dict(size=6),
    ), row=3, col=2)

    # ---- KPI Cards (as annotations) ----
    total_rev = active["revenue"].sum()
    total_orders = active["order_id"].nunique()
    aov = total_rev / total_orders if total_orders > 0 else 0
    total_customers = active["customer_id"].nunique()

    fig.update_layout(
        title=dict(
            text=(
                f"<b>E-Commerce Dashboard</b>  |  "
                f"Revenue: ${total_rev:,.0f}  |  "
                f"Orders: {total_orders:,}  |  "
                f"AOV: ${aov:,.2f}  |  "
                f"Customers: {total_customers:,}"
            ),
            font=dict(size=16),
        ),
        height=1100,
        showlegend=False,
        template="plotly_dark",
        font=dict(family="Inter, Arial, sans-serif"),
        paper_bgcolor="#0f172a",
        plot_bgcolor="#1e293b",
    )

    return fig


def main():
    logger.info("Building dashboard...")
    data = load_data()
    fig = build_dashboard(data)

    out_dir = os.path.join(PROJECT_ROOT, "data", "validated")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "dashboard.html")
    fig.write_html(out_path)
    logger.info(f"Dashboard saved: {out_path}")

    # Try to open in browser
    try:
        import webbrowser
        webbrowser.open(f"file://{os.path.abspath(out_path)}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
