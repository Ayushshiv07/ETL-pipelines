"""
======================================================
E-commerce ETL Pipeline — Data Profiling Layer
======================================================
Generates advanced statistical data profiling reports
on the star schema tables. Features:
  - Column-level metrics (Null%, unique count, min/max/mean/std)
  - Statistical outlier detection on numeric fields via IQR
  - Outputs a profile report to data/validated/profile_report.csv
======================================================
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

with open(os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml"), "r") as f:
    config = yaml.safe_load(f)


def calculate_iqr_outliers(df: pd.DataFrame, col: str) -> dict:
    """Calculate IQR-based outlier statistics for a numeric column."""
    if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
        return {"outliers_count": 0, "outliers_pct": 0.0, "lower_bound": 0.0, "upper_bound": 0.0}

    non_null = df[col].dropna()
    if len(non_null) == 0:
        return {"outliers_count": 0, "outliers_pct": 0.0, "lower_bound": 0.0, "upper_bound": 0.0}

    q1 = np.percentile(non_null, 25)
    q3 = np.percentile(non_null, 75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outliers = non_null[(non_null < lower_bound) | (non_null > upper_bound)]
    
    return {
        "outliers_count": len(outliers),
        "outliers_pct": round((len(outliers) / len(df)) * 100, 2),
        "lower_bound": round(float(lower_bound), 2),
        "upper_bound": round(float(upper_bound), 2),
    }


def profile_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Generate statistical profile for a given DataFrame."""
    logger.info(f"Profiling table: {table_name}")
    rows = []
    total_rows = len(df)

    for col in df.columns:
        nulls = df[col].isnull().sum()
        null_pct = round((nulls / total_rows) * 100, 2) if total_rows else 0.0
        uniques = df[col].nunique()
        dtype = str(df[col].dtype)

        min_val = max_val = mean_val = std_val = "N/A"
        outliers_count = 0
        outliers_pct = 0.0

        if pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_bool_dtype(df[col]):
            # Skip ID columns for numeric statistics
            if not any(x in col.lower() for x in ["id", "date_id"]):
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    min_val = round(float(non_null.min()), 2)
                    max_val = round(float(non_null.max()), 2)
                    mean_val = round(float(non_null.mean()), 2)
                    std_val = round(float(non_null.std()), 2)

                # If the column has meaningful outliers (revenue, price, quantity)
                if col in ["revenue", "unit_price", "price", "quantity"]:
                    outlier_stats = calculate_iqr_outliers(df, col)
                    outliers_count = outlier_stats["outliers_count"]
                    outliers_pct = outlier_stats["outliers_pct"]

        rows.append({
            "table_name": table_name,
            "column_name": col,
            "dtype": dtype,
            "nulls_count": int(nulls),
            "nulls_pct": null_pct,
            "unique_count": int(uniques),
            "min": min_val,
            "max": max_val,
            "mean": mean_val,
            "std": std_val,
            "outliers_count": int(outliers_count),
            "outliers_pct": outliers_pct
        })

    return pd.DataFrame(rows)


def run_profiling() -> pd.DataFrame:
    """Run profiling across all tables and save report."""
    logger.info("=" * 60)
    logger.info("PROFILING LAYER — Starting data profiling")
    logger.info("=" * 60)

    t_dir = os.path.join(PROJECT_ROOT, config["paths"]["transformed_data"])
    v_dir = os.path.join(PROJECT_ROOT, config["paths"]["validated_data"])
    os.makedirs(v_dir, exist_ok=True)

    tables = ["fact_orders", "dim_customers", "dim_products", "dim_date"]
    all_profiles = []

    for tbl in tables:
        path = os.path.join(t_dir, f"{tbl}.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            profile_df = profile_table(df, tbl)
            all_profiles.append(profile_df)
        else:
            logger.warning(f"Table not found for profiling: {tbl}")

    if not all_profiles:
        logger.error("No transformed tables found to profile.")
        return pd.DataFrame()

    final_report = pd.concat(all_profiles, ignore_index=True)
    report_path = os.path.join(v_dir, "profile_report.csv")
    final_report.to_csv(report_path, index=False)
    
    logger.info(f"Profiling complete. Saved report to: {report_path}")
    logger.info("=" * 60)
    
    return final_report


if __name__ == "__main__":
    run_profiling()
