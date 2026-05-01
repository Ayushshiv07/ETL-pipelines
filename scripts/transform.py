"""
======================================================
E-commerce ETL Pipeline — Transform Layer
======================================================
Cleans raw data, joins tables, engineers features,
and builds a star schema (fact + dimension tables).

Usage:
    from scripts.extract import extract_all
    from scripts.transform import transform_all
    star_schema = transform_all(extract_all())
======================================================
"""

import os, sys, logging
import pandas as pd
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

with open(os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml"), "r") as f:
    config = yaml.safe_load(f)

# ---- Cleaning Functions ----

def clean_orders(df):
    """Remove nulls in key columns, fix dtypes, deduplicate."""
    logger.info("Cleaning orders...")
    n = len(df)
    
    # Convert key columns to numeric, coercing errors to NaN
    for col in ["order_id", "customer_id", "product_id", "quantity"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    df = df.dropna(subset=["order_id", "customer_id", "product_id", "quantity"])
    
    # Ensure integer type
    df["order_id"] = df["order_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["product_id"] = df["product_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)
    
    df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce')
    df = df.dropna(subset=["order_date"])
    
    if "status" in df.columns:
        df["status"] = df["status"].fillna("completed").astype(str).str.strip().str.lower()
    else:
        df["status"] = "completed"
        
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    logger.info(f"  [OK] Orders: {n} -> {len(df)} rows ({n - len(df)} removed)")
    return df.reset_index(drop=True)


def clean_customers(df):
    """Remove null PKs, standardize text, deduplicate."""
    logger.info("Cleaning customers...")
    n = len(df)
    
    # Handle customer_id
    if "customer_id" in df.columns:
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors='coerce')
        df = df.dropna(subset=["customer_id"])
        df["customer_id"] = df["customer_id"].astype(int)
    else:
        logger.warning("  [WARN] customer_id column missing in customers data!")
        return pd.DataFrame(columns=["customer_id", "name", "email", "location", "signup_date"])
    
    # Safely handle other columns
    df["signup_date"] = pd.to_datetime(df.get("signup_date"), errors='coerce')
    
    if "name" in df.columns:
        df["name"] = df["name"].fillna("Unknown").astype(str).str.strip().str.title()
    else:
        df["name"] = "Unknown"
        
    if "email" in df.columns:
        df["email"] = df["email"].fillna("unknown@example.com").astype(str).str.strip().str.lower()
    else:
        df["email"] = "unknown@example.com"
        
    if "location" in df.columns:
        df["location"] = df["location"].fillna("Unknown").astype(str).str.strip()
    else:
        df["location"] = "Unknown"
    
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    logger.info(f"  [OK] Customers: {n} -> {len(df)} rows ({n - len(df)} removed)")
    return df.reset_index(drop=True)


def clean_products(df):
    """Remove null PKs, fill missing prices with category median, deduplicate."""
    logger.info("Cleaning products...")
    n = len(df)
    
    # Handle product_id
    if "product_id" in df.columns:
        df["product_id"] = pd.to_numeric(df["product_id"], errors='coerce')
        df = df.dropna(subset=["product_id"])
        df["product_id"] = df["product_id"].astype(int)
    else:
        logger.warning("  [WARN] product_id column missing in products data!")
        return pd.DataFrame(columns=["product_id", "product_name", "category", "price", "supplier"])
    
    # Handle price and category
    if "category" in df.columns and "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors='coerce')
        df["price"] = df.groupby("category")["price"].transform(lambda x: x.fillna(x.median()))
        df["price"] = df["price"].fillna(df["price"].median()).round(2)
    elif "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors='coerce').fillna(0).round(2)
    else:
        df["price"] = 0.0
    
    if "product_name" in df.columns:
        df["product_name"] = df["product_name"].fillna("Unknown Product").astype(str).str.strip()
    else:
        df["product_name"] = "Unknown Product"
        
    if "category" in df.columns:
        df["category"] = df["category"].fillna("General").astype(str).str.strip().str.title()
    else:
        df["category"] = "General"
        
    if "supplier" in df.columns:
        df["supplier"] = df["supplier"].fillna("Unknown").astype(str).str.strip()
    else:
        df["supplier"] = "Unknown"
        
    df = df.drop_duplicates(subset=["product_id"], keep="first")
    logger.info(f"  [OK] Products: {n} -> {len(df)} rows ({n - len(df)} removed)")
    return df.reset_index(drop=True)

# ---- Star Schema Builders ----

def build_dim_date(orders_df):
    """Build date dimension from unique order dates."""
    logger.info("Building dim_date...")
    dates = sorted(orders_df["order_date"].dt.date.unique())
    dim = pd.DataFrame({"full_date": pd.to_datetime(dates)})
    dim["date_id"] = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["day"] = dim["full_date"].dt.day
    dim["month"] = dim["full_date"].dt.month
    dim["year"] = dim["full_date"].dt.year
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["day_of_week"] = dim["full_date"].dt.dayofweek
    dim["day_name"] = dim["full_date"].dt.day_name()
    dim["month_name"] = dim["full_date"].dt.month_name()
    dim["is_weekend"] = dim["day_of_week"].isin([5, 6])
    dim = dim[["date_id","full_date","day","month","year","quarter",
               "day_of_week","day_name","month_name","is_weekend"]]
    logger.info(f"  [OK] dim_date: {len(dim)} unique dates")
    return dim


def build_dim_customers(df):
    """Build customer dimension."""
    logger.info("Building dim_customers...")
    out = df[["customer_id","name","email","location"]].copy()
    logger.info(f"  [OK] dim_customers: {len(out)} records")
    return out


def build_dim_products(df):
    """Build product dimension."""
    logger.info("Building dim_products...")
    out = df[["product_id","product_name","category","price","supplier"]].copy()
    logger.info(f"  [OK] dim_products: {len(out)} records")
    return out


def build_fact_orders(orders_df, products_df):
    """Join orders with products, calculate revenue, create date_id."""
    logger.info("Building fact_orders...")
    
    # Merge with products to get price
    fact = orders_df.merge(products_df[["product_id","price"]], on="product_id", how="left")
    
    # Handle missing price (e.g. if product_id not found)
    fact["price"] = fact["price"].fillna(0)
    fact["revenue"] = (fact["quantity"] * fact["price"]).round(2)
    
    # Create date_id
    fact["date_id"] = fact["order_date"].dt.strftime("%Y%m%d").astype(int)
    
    # Rename columns for fact table schema
    fact = fact.rename(columns={"price": "unit_price", "status": "order_status"})
    
    # Final column selection (ensure all exist)
    required_fact_cols = ["order_id","customer_id","product_id","date_id",
                         "quantity","unit_price","revenue","order_status"]
    
    # If any columns are missing, fill with defaults
    for col in required_fact_cols:
        if col not in fact.columns:
            fact[col] = 0 if "id" in col or col in ["quantity", "unit_price", "revenue"] else "unknown"

    fact = fact[required_fact_cols]
    logger.info(f"  [OK] fact_orders: {len(fact)} records  |  Revenue: ${fact['revenue'].sum():,.2f}")
    return fact

# ---- Main Orchestrator ----

def transform_all(raw_data):
    """Run full transformation: clean -> build star schema -> save CSVs."""
    logger.info("=" * 60)
    logger.info("TRANSFORM LAYER — Starting")
    logger.info("=" * 60)

    orders = clean_orders(raw_data["orders"])
    customers = clean_customers(raw_data["customers"])
    products = clean_products(raw_data["products"])

    star = {
        "dim_date": build_dim_date(orders),
        "dim_customers": build_dim_customers(customers),
        "dim_products": build_dim_products(products),
        "fact_orders": build_fact_orders(orders, products),
    }

    out_dir = os.path.join(PROJECT_ROOT, config["paths"]["transformed_data"])
    os.makedirs(out_dir, exist_ok=True)
    for name, df in star.items():
        path = os.path.join(out_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        logger.info(f"  -> Saved {path} ({len(df)} rows)")

    logger.info("Transformation complete [OK]")
    return star


if __name__ == "__main__":
    from scripts.extract import extract_all
    star = transform_all(extract_all())
    for name, df in star.items():
        print(f"\n--- {name.upper()} ---")
        print(df.head())
