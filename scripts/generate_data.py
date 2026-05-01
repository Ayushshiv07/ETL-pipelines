"""
======================================================
E-commerce ETL Pipeline — Sample Data Generator
======================================================
Generates realistic e-commerce datasets (orders, customers, products)
with intentional data quality issues (nulls, duplicates) for testing
the ETL cleaning logic.

Usage:
    python scripts/generate_data.py
======================================================
"""

import os
import sys
import random
import logging
from datetime import datetime, timedelta

import pandas as pd
import yaml
from faker import Faker

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# Resolve project root (parent of scripts/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Load configuration
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# Initialize Faker with seed for reproducibility
fake = Faker()
Faker.seed(config["data_generation"]["seed"])
random.seed(config["data_generation"]["seed"])


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_customers(num_customers: int) -> pd.DataFrame:
    """
    Generate a DataFrame of fake customers.

    Columns: customer_id, name, email, location, signup_date
    """
    logger.info(f"Generating {num_customers} customers...")

    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "location": fake.city() + ", " + fake.state_abbr(),
            "signup_date": fake.date_between(
                start_date="-3y", end_date="today"
            ).isoformat(),
        })

    df = pd.DataFrame(customers)
    logger.info(f"  [OK] Generated {len(df)} customer records")
    return df


def generate_products(num_products: int) -> pd.DataFrame:
    """
    Generate a DataFrame of fake products.

    Columns: product_id, product_name, category, price, supplier
    """
    logger.info(f"Generating {num_products} products...")

    categories = [
        "Electronics", "Clothing", "Home & Kitchen", "Books",
        "Sports & Outdoors", "Beauty", "Toys & Games", "Automotive",
        "Health", "Grocery",
    ]

    suppliers = [
        "GlobalTrade Inc.", "PrimeParts LLC", "DirectSource Co.",
        "MegaSupply Corp.", "QuickShip Logistics",
    ]

    products = []
    for i in range(1, num_products + 1):
        category = random.choice(categories)

        # Price ranges vary by category for realism
        price_ranges = {
            "Electronics": (49.99, 1299.99),
            "Clothing": (9.99, 199.99),
            "Home & Kitchen": (14.99, 499.99),
            "Books": (4.99, 59.99),
            "Sports & Outdoors": (19.99, 399.99),
            "Beauty": (7.99, 89.99),
            "Toys & Games": (9.99, 149.99),
            "Automotive": (24.99, 599.99),
            "Health": (5.99, 79.99),
            "Grocery": (1.99, 49.99),
        }
        low, high = price_ranges[category]

        products.append({
            "product_id": i,
            "product_name": fake.catch_phrase(),
            "category": category,
            "price": round(random.uniform(low, high), 2),
            "supplier": random.choice(suppliers),
        })

    df = pd.DataFrame(products)
    logger.info(f"  [OK] Generated {len(df)} product records")
    return df


def generate_orders(
    num_orders: int,
    num_customers: int,
    num_products: int,
) -> pd.DataFrame:
    """
    Generate a DataFrame of fake orders.

    Columns: order_id, customer_id, product_id, quantity, order_date, status
    """
    logger.info(f"Generating {num_orders} orders...")

    statuses = ["completed", "pending", "shipped", "cancelled", "returned"]
    status_weights = [0.60, 0.10, 0.15, 0.10, 0.05]

    orders = []
    for i in range(1, num_orders + 1):
        orders.append({
            "order_id": i,
            "customer_id": random.randint(1, num_customers),
            "product_id": random.randint(1, num_products),
            "quantity": random.randint(1, 10),
            "order_date": fake.date_between(
                start_date="-2y", end_date="today"
            ).isoformat(),
            "status": random.choices(statuses, weights=status_weights, k=1)[0],
        })

    df = pd.DataFrame(orders)
    logger.info(f"  [OK] Generated {len(df)} order records")
    return df


# ---------------------------------------------------------------------------
# Inject Data Quality Issues (for testing ETL cleaning)
# ---------------------------------------------------------------------------

def inject_nulls(df: pd.DataFrame, rate: float, columns: list) -> pd.DataFrame:
    """Randomly set values to NaN in specified columns."""
    df = df.copy()
    n_nulls = int(len(df) * rate)

    for col in columns:
        null_indices = random.sample(range(len(df)), min(n_nulls, len(df)))
        df.loc[null_indices, col] = None

    logger.info(f"  [WARN] Injected ~{rate*100:.0f}% nulls into columns: {columns}")
    return df


def inject_duplicates(df: pd.DataFrame, rate: float) -> pd.DataFrame:
    """Append duplicate rows to the DataFrame."""
    n_dupes = int(len(df) * rate)
    if n_dupes > 0:
        dupe_indices = random.sample(range(len(df)), min(n_dupes, len(df)))
        dupes = df.iloc[dupe_indices].copy()
        df = pd.concat([df, dupes], ignore_index=True)
        logger.info(f"  [WARN] Injected {n_dupes} duplicate rows")
    return df


# ---------------------------------------------------------------------------
# Main — Generate and Save
# ---------------------------------------------------------------------------

def main():
    """Generate all datasets and save to data/raw/."""
    logger.info("=" * 60)
    logger.info("E-commerce ETL Pipeline — Data Generation")
    logger.info("=" * 60)

    gen_cfg = config["data_generation"]
    raw_dir = os.path.join(PROJECT_ROOT, config["paths"]["raw_data"])
    os.makedirs(raw_dir, exist_ok=True)

    # --- Generate clean data ---
    customers_df = generate_customers(gen_cfg["num_customers"])
    products_df = generate_products(gen_cfg["num_products"])
    orders_df = generate_orders(
        gen_cfg["num_orders"],
        gen_cfg["num_customers"],
        gen_cfg["num_products"],
    )

    # --- Inject quality issues for testing ---
    null_rate = gen_cfg["null_rate"]
    dupe_rate = gen_cfg["duplicate_rate"]

    customers_df = inject_nulls(customers_df, null_rate, ["name", "email"])
    customers_df = inject_duplicates(customers_df, dupe_rate)

    products_df = inject_nulls(products_df, null_rate, ["product_name", "price"])
    products_df = inject_duplicates(products_df, dupe_rate)

    orders_df = inject_nulls(orders_df, null_rate, ["customer_id", "quantity"])
    orders_df = inject_duplicates(orders_df, dupe_rate)

    # --- Save to CSV ---
    customers_path = os.path.join(raw_dir, config["source_files"]["customers"])
    products_path = os.path.join(raw_dir, config["source_files"]["products"])
    orders_path = os.path.join(raw_dir, config["source_files"]["orders"])

    customers_df.to_csv(customers_path, index=False)
    products_df.to_csv(products_path, index=False)
    orders_df.to_csv(orders_path, index=False)

    logger.info("-" * 60)
    logger.info("Files saved:")
    logger.info(f"  -> {customers_path}  ({len(customers_df)} rows)")
    logger.info(f"  -> {products_path}   ({len(products_df)} rows)")
    logger.info(f"  -> {orders_path}     ({len(orders_df)} rows)")
    logger.info("=" * 60)
    logger.info("Data generation complete [OK]")


if __name__ == "__main__":
    main()
