"""
======================================================
E-commerce ETL Pipeline — Extract Layer
======================================================
Reads raw CSV files from the data/raw/ directory and returns
them as Pandas DataFrames for downstream transformation.

Responsibilities:
  - Validate that source files exist
  - Read CSVs with proper dtypes
  - Log row counts and basic stats
  - Return a dictionary of DataFrames

Usage:
    from scripts.extract import extract_all
    raw_data = extract_all()
======================================================
"""

import os
import sys
import logging
from typing import Dict, Optional

import pandas as pd
import yaml

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Extract Functions
# ---------------------------------------------------------------------------

def extract_csv(
    filepath: str,
    dtype_overrides: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Read a single CSV file into a Pandas DataFrame.

    Args:
        filepath: Absolute or relative path to the CSV file.
        dtype_overrides: Optional dict of column -> dtype mappings.

    Returns:
        DataFrame with the raw data.

    Raises:
        FileNotFoundError: If the file does not exist.
        pd.errors.EmptyDataError: If the file is empty.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Source file not found: {filepath}")

    logger.info(f"Extracting: {filepath}")

    df = pd.read_csv(filepath, dtype=dtype_overrides)

    # Log basic stats
    logger.info(f"  [OK] Rows: {len(df):,}  |  Columns: {len(df.columns)}")
    logger.info(f"  [OK] Columns: {list(df.columns)}")

    # Check for obvious issues
    null_counts = df.isnull().sum()
    if null_counts.any():
        cols_with_nulls = null_counts[null_counts > 0].to_dict()
        logger.warning(f"  [WARN] Nulls detected: {cols_with_nulls}")

    return df


def extract_orders() -> pd.DataFrame:
    """Extract the orders dataset."""
    filepath = os.path.join(
        PROJECT_ROOT,
        config["paths"]["raw_data"],
        config["source_files"]["orders"],
    )
    return extract_csv(filepath)


def extract_customers() -> pd.DataFrame:
    """Extract the customers dataset."""
    filepath = os.path.join(
        PROJECT_ROOT,
        config["paths"]["raw_data"],
        config["source_files"]["customers"],
    )
    return extract_csv(filepath)


def extract_products() -> pd.DataFrame:
    """Extract the products dataset."""
    filepath = os.path.join(
        PROJECT_ROOT,
        config["paths"]["raw_data"],
        config["source_files"]["products"],
    )
    return extract_csv(filepath)


def extract_all() -> Dict[str, pd.DataFrame]:
    """
    Extract all source datasets.

    Returns:
        Dictionary with keys 'orders', 'customers', 'products'
        mapped to their respective DataFrames.
    """
    logger.info("=" * 60)
    logger.info("EXTRACT LAYER — Starting extraction")
    logger.info("=" * 60)

    raw_data = {
        "orders": extract_orders(),
        "customers": extract_customers(),
        "products": extract_products(),
    }

    total_rows = sum(len(df) for df in raw_data.values())
    logger.info("-" * 60)
    logger.info(f"Extraction complete [OK]  |  Total rows: {total_rows:,}")
    logger.info("=" * 60)

    return raw_data


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data = extract_all()
    for name, df in data.items():
        print(f"\n--- {name.upper()} (first 5 rows) ---")
        print(df.head())
